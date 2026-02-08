"""
Quality Rules Engine + Check History + Dashboard + Alerts
"""
import json
import uuid
import logging
from typing import Optional, Dict

from fastapi import APIRouter, HTTPException, Query

from .metamgmt_shared import get_connection, _init, QualityRuleCreate, QualityAlertCreate

logger = logging.getLogger(__name__)

router = APIRouter()


# ══════════════════════════════════════════════
#  2. Quality Rules Engine
# ══════════════════════════════════════════════

@router.get("/quality-rules")
async def list_quality_rules(table_name: Optional[str] = None, severity: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if table_name:
            params.append(table_name)
            where.append(f"table_name = ${len(params)}")
        if severity:
            params.append(severity)
            where.append(f"severity = ${len(params)}")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"SELECT * FROM meta_quality_rule {w} ORDER BY table_name, column_name", *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/quality-rules")
async def create_quality_rule(body: QualityRuleCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO meta_quality_rule (table_name, column_name, rule_type, rule_name, condition_expr,
                normal_min, normal_max, error_min, error_max, enum_values, regex_pattern, severity, description, enabled)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14) RETURNING rule_id
        """, body.table_name, body.column_name, body.rule_type, body.rule_name, body.condition_expr,
            body.normal_min, body.normal_max, body.error_min, body.error_max,
            json.dumps(body.enum_values) if body.enum_values else None,
            body.regex_pattern, body.severity, body.description, body.enabled)
        return {"rule_id": row["rule_id"]}
    finally:
        await conn.close()


@router.put("/quality-rules/{rule_id}")
async def update_quality_rule(rule_id: int, body: QualityRuleCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE meta_quality_rule SET table_name=$2, column_name=$3, rule_type=$4, rule_name=$5,
                condition_expr=$6, normal_min=$7, normal_max=$8, error_min=$9, error_max=$10,
                enum_values=$11::jsonb, regex_pattern=$12, severity=$13, description=$14, enabled=$15, updated_at=NOW()
            WHERE rule_id=$1
        """, rule_id, body.table_name, body.column_name, body.rule_type, body.rule_name, body.condition_expr,
            body.normal_min, body.normal_max, body.error_min, body.error_max,
            json.dumps(body.enum_values) if body.enum_values else None,
            body.regex_pattern, body.severity, body.description, body.enabled)
        return {"ok": True}
    finally:
        await conn.close()


@router.delete("/quality-rules/{rule_id}")
async def delete_quality_rule(rule_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("DELETE FROM meta_quality_rule WHERE rule_id=$1", rule_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.post("/quality-rules/{rule_id}/check")
async def check_quality_rule(rule_id: int):
    """단일 품질 규칙 실행 + 결과 저장"""
    conn = await get_connection()
    try:
        await _init(conn)
        rule = await conn.fetchrow("SELECT * FROM meta_quality_rule WHERE rule_id=$1", rule_id)
        if not rule:
            raise HTTPException(404, "규칙을 찾을 수 없습니다")

        result = {"rule_id": rule_id, "status": "pass", "violations": 0, "total": 0, "details": []}
        table = rule["table_name"]
        column = rule["column_name"]

        try:
            total = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
            result["total"] = total

            if rule["rule_type"] == "not_null":
                violations = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
                result["violations"] = violations
            elif rule["rule_type"] == "range" and rule["normal_min"] is not None:
                violations = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL AND ({column} < $1 OR {column} > $2)",
                    rule["normal_min"], rule["normal_max"])
                result["violations"] = violations
            elif rule["rule_type"] == "enum" and rule["enum_values"]:
                vals = rule["enum_values"] if isinstance(rule["enum_values"], list) else json.loads(rule["enum_values"])
                placeholders = ",".join(f"${i+1}" for i in range(len(vals)))
                violations = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL AND {column}::text NOT IN ({placeholders})",
                    *[str(v) for v in vals])
                result["violations"] = violations
            elif rule["rule_type"] == "custom" and rule["condition_expr"]:
                violations = await conn.fetchval(
                    f"SELECT COUNT(*) FROM {table} WHERE NOT ({rule['condition_expr']})")
                result["violations"] = violations

            result["status"] = "pass" if result["violations"] == 0 else (
                "error" if result["violations"] / max(total, 1) > 0.01 else "warning")
            result["violation_rate"] = round(result["violations"] / max(total, 1) * 100, 4)

        except Exception as e:
            result["status"] = "error"
            result["details"].append(f"쿼리 실행 오류: {str(e)}")

        # Save result
        await conn.execute("""
            UPDATE meta_quality_rule SET last_checked=NOW(), last_result=$2::jsonb WHERE rule_id=$1
        """, rule_id, json.dumps(result))

        return result
    finally:
        await conn.close()


@router.post("/quality-rules/check-all")
async def check_all_quality_rules():
    """전체 활성 규칙 일괄 실행 + 이력 저장 + 알림 평가"""
    run_id = str(uuid.uuid4())[:12]
    conn = await get_connection()
    try:
        await _init(conn)
        rules = await conn.fetch("SELECT rule_id FROM meta_quality_rule WHERE enabled=TRUE ORDER BY rule_id")
        results = []
        for r in rules:
            try:
                result = await _check_single_rule(r["rule_id"])
                results.append(result)
            except Exception as e:
                results.append({"rule_id": r["rule_id"], "status": "error", "detail": str(e)})

        # Save history
        hist_conn = await get_connection()
        try:
            for res in results:
                await hist_conn.execute("""
                    INSERT INTO meta_quality_check_history
                        (run_id, rule_id, rule_name, table_name, column_name, rule_type, status,
                         total_rows, violations, violation_rate, detail)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                """, run_id,
                    res.get("rule_id"), res.get("rule_name", ""),
                    res.get("table", ""), res.get("column", ""), "",
                    res.get("status", "error"),
                    res.get("total", 0), res.get("violations", 0),
                    res.get("violation_rate", 0),
                    json.dumps(res.get("details", []), ensure_ascii=False) if res.get("details") else None)
        finally:
            await hist_conn.close()

        passed = sum(1 for r in results if r.get("status") == "pass")
        total = len(results)
        pass_rate = round(passed / max(total, 1) * 100, 1)

        # Evaluate alerts
        await _evaluate_alerts(run_id, results, pass_rate)

        return {
            "run_id": run_id,
            "total_rules": total, "passed": passed,
            "failed": total - passed,
            "pass_rate": pass_rate,
            "results": results,
        }
    finally:
        await conn.close()


async def _evaluate_alerts(run_id: str, results: list, pass_rate: float):
    """알림 조건 평가 -> 로그 기록"""
    conn = await get_connection()
    try:
        alerts = await conn.fetch("SELECT * FROM meta_quality_alert WHERE enabled=TRUE")
        for alert in alerts:
            triggered = False
            ct = alert["condition_type"]
            threshold = alert["threshold"]
            if ct == "rule_fail":
                triggered = any(r.get("status") not in ("pass",) for r in results)
            elif ct == "violation_threshold" and threshold is not None:
                triggered = any(r.get("violation_rate", 0) > threshold for r in results)
            elif ct == "batch_fail_rate" and threshold is not None:
                triggered = pass_rate < threshold
            if triggered:
                channel = alert["channel"]
                logger.warning(
                    "[QualityAlert] run=%s alert='%s' condition=%s channel=%s",
                    run_id, alert["name"], ct, channel)
    except Exception as e:
        logger.error("Alert evaluation error: %s", e)
    finally:
        await conn.close()


async def _check_single_rule(rule_id: int):
    conn = await get_connection()
    try:
        rule = await conn.fetchrow("SELECT * FROM meta_quality_rule WHERE rule_id=$1", rule_id)
        if not rule:
            return {"rule_id": rule_id, "status": "error", "detail": "not found"}
        result = {"rule_id": rule_id, "rule_name": rule["rule_name"], "table": rule["table_name"],
                  "column": rule["column_name"], "status": "pass", "violations": 0, "total": 0}
        table, column = rule["table_name"], rule["column_name"]
        total = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        result["total"] = total
        if rule["rule_type"] == "not_null":
            result["violations"] = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
        elif rule["rule_type"] == "range" and rule["normal_min"] is not None:
            result["violations"] = await conn.fetchval(
                f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL AND ({column} < $1 OR {column} > $2)",
                rule["normal_min"], rule["normal_max"])
        elif rule["rule_type"] == "enum" and rule["enum_values"]:
            vals = rule["enum_values"] if isinstance(rule["enum_values"], list) else json.loads(rule["enum_values"])
            phs = ",".join(f"${i+1}" for i in range(len(vals)))
            result["violations"] = await conn.fetchval(
                f"SELECT COUNT(*) FROM {table} WHERE {column} IS NOT NULL AND {column}::text NOT IN ({phs})",
                *[str(v) for v in vals])
        result["status"] = "pass" if result["violations"] == 0 else "fail"
        result["violation_rate"] = round(result["violations"] / max(total, 1) * 100, 4)
        await conn.execute("UPDATE meta_quality_rule SET last_checked=NOW(), last_result=$2::jsonb WHERE rule_id=$1",
                           rule_id, json.dumps(result))
        return result
    finally:
        await conn.close()


# ══════════════════════════════════════════════
#  2-B. Quality Check History & Dashboard & Alerts
# ══════════════════════════════════════════════

@router.get("/quality-check-history")
async def get_quality_check_history(
    run_id: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(200, ge=1, le=1000),
):
    """검증 이력 조회"""
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if run_id:
            params.append(run_id)
            where.append(f"run_id = ${len(params)}")
        if date_from:
            params.append(date_from)
            where.append(f"checked_at >= ${len(params)}::timestamptz")
        if date_to:
            params.append(date_to)
            where.append(f"checked_at <= ${len(params)}::timestamptz")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        params.append(limit)
        rows = await conn.fetch(
            f"SELECT * FROM meta_quality_check_history {w} ORDER BY checked_at DESC LIMIT ${len(params)}",
            *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.get("/quality-check-history/runs")
async def get_quality_check_runs(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """일괄 검증 실행 목록 (run_id별 요약)"""
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if date_from:
            params.append(date_from)
            where.append(f"checked_at >= ${len(params)}::timestamptz")
        if date_to:
            params.append(date_to)
            where.append(f"checked_at <= ${len(params)}::timestamptz")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"""
            SELECT run_id,
                   MIN(checked_at) AS checked_at,
                   COUNT(*) AS total_rules,
                   SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS passed,
                   SUM(CASE WHEN status != 'pass' THEN 1 ELSE 0 END) AS failed,
                   ROUND(SUM(CASE WHEN status = 'pass' THEN 1 ELSE 0 END)::numeric
                         / GREATEST(COUNT(*), 1) * 100, 1) AS pass_rate
            FROM meta_quality_check_history
            {w}
            GROUP BY run_id
            ORDER BY MIN(checked_at) DESC
            LIMIT 50
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.get("/quality-dashboard")
async def quality_dashboard():
    """품질 대시보드"""
    conn = await get_connection()
    try:
        await _init(conn)

        # Rules summary
        rules = await conn.fetch("SELECT * FROM meta_quality_rule ORDER BY rule_id")
        total_rules = len(rules)
        enabled_rules = sum(1 for r in rules if r["enabled"])
        by_severity: Dict[str, int] = {}
        by_rule_type: Dict[str, int] = {}
        for r in rules:
            sev = r["severity"]
            by_severity[sev] = by_severity.get(sev, 0) + 1
            rt = r["rule_type"]
            by_rule_type[rt] = by_rule_type.get(rt, 0) + 1

        # Last run
        last_run_row = await conn.fetchrow("""
            SELECT run_id, MIN(checked_at) AS checked_at,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status='pass' THEN 1 ELSE 0 END) AS passed,
                   SUM(CASE WHEN status!='pass' THEN 1 ELSE 0 END) AS failed
            FROM meta_quality_check_history
            WHERE run_id = (SELECT run_id FROM meta_quality_check_history ORDER BY checked_at DESC LIMIT 1)
            GROUP BY run_id
        """)
        last_run = None
        if last_run_row:
            total_lr = last_run_row["total"]
            passed_lr = last_run_row["passed"]
            last_run = {
                "run_id": last_run_row["run_id"],
                "checked_at": last_run_row["checked_at"].isoformat() if last_run_row["checked_at"] else None,
                "total": total_lr, "passed": passed_lr, "failed": last_run_row["failed"],
                "pass_rate": round(passed_lr / max(total_lr, 1) * 100, 1),
            }

        # Table scores (from last run)
        table_scores = []
        if last_run_row:
            ts_rows = await conn.fetch("""
                SELECT table_name,
                       COUNT(*) AS rule_count,
                       SUM(CASE WHEN status='pass' THEN 1 ELSE 0 END) AS pass_count,
                       SUM(CASE WHEN status!='pass' THEN 1 ELSE 0 END) AS fail_count,
                       ROUND(SUM(CASE WHEN status='pass' THEN 1 ELSE 0 END)::numeric
                             / GREATEST(COUNT(*),1) * 100, 1) AS score
                FROM meta_quality_check_history
                WHERE run_id = $1
                GROUP BY table_name ORDER BY table_name
            """, last_run_row["run_id"])
            table_scores = [dict(r) for r in ts_rows]

        # Trend (last 10 runs)
        trend_rows = await conn.fetch("""
            SELECT run_id, MIN(checked_at) AS checked_at,
                   ROUND(SUM(CASE WHEN status='pass' THEN 1 ELSE 0 END)::numeric
                         / GREATEST(COUNT(*),1) * 100, 1) AS pass_rate
            FROM meta_quality_check_history
            GROUP BY run_id
            ORDER BY MIN(checked_at) DESC
            LIMIT 10
        """)
        trend = [{"run_id": r["run_id"],
                   "checked_at": r["checked_at"].isoformat() if r["checked_at"] else None,
                   "pass_rate": float(r["pass_rate"])} for r in reversed(trend_rows)]

        # Active alerts count
        alerts_active = await conn.fetchval("SELECT COUNT(*) FROM meta_quality_alert WHERE enabled=TRUE")

        return {
            "rules_summary": {
                "total": total_rules, "enabled": enabled_rules,
                "by_severity": by_severity, "by_rule_type": by_rule_type,
            },
            "last_run": last_run,
            "table_scores": table_scores,
            "trend": trend,
            "alerts_active": alerts_active,
        }
    finally:
        await conn.close()


@router.get("/quality-alerts")
async def list_quality_alerts():
    conn = await get_connection()
    try:
        await _init(conn)
        rows = await conn.fetch("SELECT * FROM meta_quality_alert ORDER BY alert_id")
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/quality-alerts")
async def create_quality_alert(body: QualityAlertCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO meta_quality_alert (name, condition_type, threshold, target_rules, channel, channel_config, enabled)
            VALUES ($1,$2,$3,$4::jsonb,$5,$6::jsonb,$7) RETURNING alert_id
        """, body.name, body.condition_type, body.threshold,
            json.dumps(body.target_rules or []),
            body.channel, json.dumps(body.channel_config or {}), body.enabled)
        return {"alert_id": row["alert_id"]}
    finally:
        await conn.close()


@router.put("/quality-alerts/{alert_id}")
async def update_quality_alert(alert_id: int, body: QualityAlertCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE meta_quality_alert SET name=$2, condition_type=$3, threshold=$4,
                target_rules=$5::jsonb, channel=$6, channel_config=$7::jsonb, enabled=$8
            WHERE alert_id=$1
        """, alert_id, body.name, body.condition_type, body.threshold,
            json.dumps(body.target_rules or []),
            body.channel, json.dumps(body.channel_config or {}), body.enabled)
        return {"ok": True}
    finally:
        await conn.close()


@router.delete("/quality-alerts/{alert_id}")
async def delete_quality_alert(alert_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("DELETE FROM meta_quality_alert WHERE alert_id=$1", alert_id)
        return {"ok": True}
    finally:
        await conn.close()
