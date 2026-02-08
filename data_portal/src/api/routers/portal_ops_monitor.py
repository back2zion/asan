"""
DPR-005: 포털 운영 — 모니터링 & 로깅
접속 로그, 시스템 리소스, ETL 현황, 알림, 데이터 품질
"""
import time
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ._portal_ops_shared import (
    get_connection, portal_ops_init,
    AccessLogEntry, AlertCreate, AlertUpdate, QualityRuleCreate,
)

router = APIRouter(tags=["PortalOps-Monitor"])


# ── Access Logs ──

@router.post("/logs/access")
async def create_access_log(body: AccessLogEntry):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        row = await conn.fetchrow(
            "INSERT INTO po_access_log (user_id, user_name, department, action, resource, ip_address, user_agent, duration_ms, details) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb) RETURNING log_id, created_at",
            body.user_id, body.user_name, body.department, body.action, body.resource,
            body.ip_address, body.user_agent, body.duration_ms, json.dumps(body.details),
        )
        return {"log_id": row["log_id"], "created_at": row["created_at"].isoformat()}
    finally:
        await conn.close()


@router.get("/logs/access")
async def list_access_logs(
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    department: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    limit: int = Query(default=100, ge=1, le=500),
):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        conditions = []
        params = []
        idx = 1
        if user_id:
            conditions.append(f"user_id = ${idx}")
            params.append(user_id)
            idx += 1
        if action:
            conditions.append(f"action = ${idx}")
            params.append(action)
            idx += 1
        if department:
            conditions.append(f"department = ${idx}")
            params.append(department)
            idx += 1
        if date_from:
            conditions.append(f"created_at >= ${idx}::timestamptz")
            params.append(date_from)
            idx += 1
        if date_to:
            conditions.append(f"created_at <= ${idx}::timestamptz")
            params.append(date_to)
            idx += 1
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit)
        rows = await conn.fetch(
            f"SELECT * FROM po_access_log {where} ORDER BY created_at DESC LIMIT ${idx}",
            *params,
        )
        return [
            {
                "log_id": r["log_id"], "user_id": r["user_id"], "user_name": r["user_name"],
                "department": r["department"], "action": r["action"], "resource": r["resource"],
                "ip_address": r["ip_address"], "duration_ms": r["duration_ms"], "details": r["details"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.get("/logs/access/stats")
async def access_log_stats(
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """접속 로그 통계 (부서별, 사용자-행위 매트릭스 포함)"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        # Build date filter
        date_conds = []
        date_params = []
        idx = 1
        if date_from:
            date_conds.append(f"created_at >= ${idx}::timestamptz")
            date_params.append(date_from)
            idx += 1
        if date_to:
            date_conds.append(f"created_at <= ${idx}::timestamptz")
            date_params.append(date_to)
            idx += 1
        where = "WHERE " + " AND ".join(date_conds) if date_conds else ""

        total = await conn.fetchval(f"SELECT COUNT(*) FROM po_access_log {where}", *date_params)
        by_action = await conn.fetch(
            f"SELECT action, COUNT(*) AS cnt FROM po_access_log {where} GROUP BY action ORDER BY cnt DESC",
            *date_params,
        )
        by_user = await conn.fetch(
            f"SELECT user_id, user_name, department, COUNT(*) AS cnt FROM po_access_log {where} GROUP BY user_id, user_name, department ORDER BY cnt DESC LIMIT 10",
            *date_params,
        )
        today = await conn.fetchval(
            "SELECT COUNT(*) FROM po_access_log WHERE created_at >= CURRENT_DATE"
        )
        by_department = await conn.fetch(
            f"SELECT COALESCE(department, '미지정') AS dept, COUNT(*) AS cnt FROM po_access_log {where} GROUP BY department ORDER BY cnt DESC",
            *date_params,
        )
        user_action_matrix = await conn.fetch(
            f"SELECT user_id, user_name, action, COUNT(*) AS cnt FROM po_access_log {where} GROUP BY user_id, user_name, action ORDER BY user_id, action",
            *date_params,
        )
        # Transform matrix to {user_id, user_name, login: N, page_view: N, ...}
        matrix_map: dict = {}
        for r in user_action_matrix:
            uid = r["user_id"]
            if uid not in matrix_map:
                matrix_map[uid] = {"user_id": uid, "user_name": r["user_name"]}
            matrix_map[uid][r["action"]] = r["cnt"]

        return {
            "total_logs": total,
            "today_logs": today,
            "by_action": [{"action": r["action"], "count": r["cnt"]} for r in by_action],
            "top_users": [{"user_id": r["user_id"], "user_name": r["user_name"], "department": r["department"], "count": r["cnt"]} for r in by_user],
            "by_department": [{"department": r["dept"], "count": r["cnt"]} for r in by_department],
            "user_action_matrix": list(matrix_map.values()),
        }
    finally:
        await conn.close()


@router.get("/logs/access/anomalies")
async def detect_anomalies(days: int = Query(default=7, ge=1, le=30)):
    """이상 접속 탐지: 과다 다운로드, 반복 쿼리, 비정상 시간대 접속"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        anomalies = []

        # 1. 과다 다운로드 (하루 3건 이상)
        heavy_downloads = await conn.fetch(
            "SELECT user_id, user_name, department, DATE(created_at) AS day, COUNT(*) AS cnt "
            "FROM po_access_log WHERE action = 'data_download' AND created_at >= NOW() - ($1 || ' days')::interval "
            "GROUP BY user_id, user_name, department, DATE(created_at) HAVING COUNT(*) >= 3 ORDER BY cnt DESC",
            str(days),
        )
        for r in heavy_downloads:
            anomalies.append({
                "type": "excessive_download",
                "severity": "warning",
                "user_id": r["user_id"],
                "user_name": r["user_name"],
                "department": r["department"],
                "detail": f"{r['day']}에 {r['cnt']}건 데이터 다운로드",
                "count": r["cnt"],
                "date": str(r["day"]),
            })

        # 2. 동일 리소스 반복 쿼리 (5회 이상)
        repeat_queries = await conn.fetch(
            "SELECT user_id, user_name, department, resource, COUNT(*) AS cnt "
            "FROM po_access_log WHERE action = 'query_execute' AND created_at >= NOW() - ($1 || ' days')::interval "
            "GROUP BY user_id, user_name, department, resource HAVING COUNT(*) >= 5 ORDER BY cnt DESC",
            str(days),
        )
        for r in repeat_queries:
            anomalies.append({
                "type": "repeated_query",
                "severity": "info",
                "user_id": r["user_id"],
                "user_name": r["user_name"],
                "department": r["department"],
                "detail": f"'{r['resource']}'에 {r['cnt']}회 반복 쿼리 실행",
                "count": r["cnt"],
                "date": None,
            })

        # 3. 비정상 시간대 접속 (22:00~06:00)
        offhour = await conn.fetch(
            "SELECT user_id, user_name, department, COUNT(*) AS cnt "
            "FROM po_access_log WHERE (EXTRACT(HOUR FROM created_at) >= 22 OR EXTRACT(HOUR FROM created_at) < 6) "
            "AND created_at >= NOW() - ($1 || ' days')::interval "
            "GROUP BY user_id, user_name, department HAVING COUNT(*) >= 2 ORDER BY cnt DESC",
            str(days),
        )
        for r in offhour:
            anomalies.append({
                "type": "off_hours_access",
                "severity": "warning",
                "user_id": r["user_id"],
                "user_name": r["user_name"],
                "department": r["department"],
                "detail": f"비정상 시간대 (22~06시) {r['cnt']}회 접속",
                "count": r["cnt"],
                "date": None,
            })

        return {
            "period_days": days,
            "total_anomalies": len(anomalies),
            "anomalies": anomalies,
        }
    finally:
        await conn.close()


@router.get("/logs/access/trend")
async def access_trend(days: int = Query(default=7, ge=1, le=90)):
    """일별 접속 추이"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        rows = await conn.fetch(
            "SELECT DATE(created_at) AS day, action, COUNT(*) AS cnt "
            "FROM po_access_log WHERE created_at >= NOW() - ($1 || ' days')::interval "
            "GROUP BY DATE(created_at), action ORDER BY day",
            str(days),
        )
        # Pivot: {date, login, page_view, query_execute, data_download, export, total}
        day_map: dict = {}
        for r in rows:
            d = str(r["day"])
            if d not in day_map:
                day_map[d] = {"date": d, "total": 0}
            day_map[d][r["action"]] = r["cnt"]
            day_map[d]["total"] += r["cnt"]
        return {"days": days, "trend": sorted(day_map.values(), key=lambda x: x["date"])}
    finally:
        await conn.close()


# ── System Resources ──

@router.get("/system/resources")
async def get_system_resources():
    """시스템 리소스 현황"""
    import psutil
    cpu_percent = psutil.cpu_percent(interval=0.5)
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    # DB connection check
    db_status = "healthy"
    db_pool_info = {}
    conn = None
    try:
        conn = await get_connection()
        await portal_ops_init(conn)
        db_active = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = 'omop_cdm'"
        )
        db_size = await conn.fetchval(
            "SELECT pg_size_pretty(pg_database_size('omop_cdm'))"
        )
        db_pool_info = {"active_connections": db_active, "db_size": db_size}
    except Exception as e:
        db_status = f"error: {e}"
    finally:
        if conn:
            await conn.close()

    return {
        "cpu": {"percent": cpu_percent, "cores": psutil.cpu_count()},
        "memory": {
            "total_gb": round(mem.total / (1024**3), 1),
            "used_gb": round(mem.used / (1024**3), 1),
            "percent": mem.percent,
        },
        "disk": {
            "total_gb": round(disk.total / (1024**3), 1),
            "used_gb": round(disk.used / (1024**3), 1),
            "percent": round(disk.percent, 1),
        },
        "database": {"status": db_status, **db_pool_info},
    }


@router.get("/system/services")
async def get_service_status():
    """서비스 상태 확인"""
    import httpx
    import socket

    services = [
        {"name": "Backend API", "url": None, "port": 8000, "self": True},
        {"name": "Frontend (Vite)", "url": "http://localhost:5173", "port": 5173},
        {"name": "OMOP DB", "url": None, "port": 5436},
        {"name": "Qwen3 LLM", "url": "http://localhost:28888/health", "port": 28888},
        {"name": "Medical NER", "url": "http://localhost:28100/ner/health", "port": 28100},
        {"name": "Superset", "url": "http://localhost:18088/health", "port": 18088},
    ]

    results = []
    async with httpx.AsyncClient(timeout=3.0) as client:
        for svc in services:
            status = "unknown"
            latency_ms = None

            if svc.get("self"):
                # Backend API는 자기 자신이므로 항상 healthy
                status = "healthy"
                latency_ms = 0
            elif svc["url"]:
                try:
                    start = time.time()
                    resp = await client.get(svc["url"])
                    latency_ms = int((time.time() - start) * 1000)
                    status = "healthy" if resp.status_code < 500 else "degraded"
                except Exception:
                    status = "down"
            else:
                # Port-only check (DB)
                try:
                    start = time.time()
                    s = socket.create_connection(("localhost", svc["port"]), timeout=2)
                    s.close()
                    latency_ms = int((time.time() - start) * 1000)
                    status = "healthy"
                except Exception:
                    status = "down"

            results.append({
                "name": svc["name"],
                "port": svc["port"],
                "status": status,
                "latency_ms": latency_ms,
            })

    healthy = sum(1 for r in results if r["status"] == "healthy")
    return {"services": results, "healthy_count": healthy, "total_count": len(results)}


# ── Alerts ──

@router.post("/alerts")
async def create_alert(body: AlertCreate):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        row = await conn.fetchrow(
            "INSERT INTO po_alert (severity, source, message, details) "
            "VALUES ($1,$2,$3,$4::jsonb) RETURNING alert_id, created_at",
            body.severity, body.source, body.message, json.dumps(body.details),
        )
        return {"alert_id": row["alert_id"], "created_at": row["created_at"].isoformat()}
    finally:
        await conn.close()


@router.get("/alerts")
async def list_alerts(status: Optional[str] = None, severity: Optional[str] = None):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        conditions = []
        params = []
        idx = 1
        if status:
            conditions.append(f"status = ${idx}")
            params.append(status)
            idx += 1
        if severity:
            conditions.append(f"severity = ${idx}")
            params.append(severity)
            idx += 1
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        rows = await conn.fetch(
            f"SELECT * FROM po_alert {where} ORDER BY created_at DESC LIMIT 100", *params,
        )
        return [
            {
                "alert_id": r["alert_id"], "severity": r["severity"], "source": r["source"],
                "message": r["message"], "details": r["details"], "status": r["status"],
                "resolved_by": r["resolved_by"],
                "resolved_at": r["resolved_at"].isoformat() if r["resolved_at"] else None,
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.put("/alerts/{alert_id}")
async def update_alert(alert_id: int, body: AlertUpdate):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        extra = ""
        params = [body.status, alert_id]
        if body.status == "resolved":
            extra = ", resolved_by=$3, resolved_at=NOW()"
            params.insert(2, body.resolved_by or "admin")
        result = await conn.execute(
            f"UPDATE po_alert SET status=$1{extra} WHERE alert_id=$2", *params,
        )
        if "UPDATE 0" in result:
            raise HTTPException(status_code=404, detail="알림을 찾을 수 없습니다")
        return {"updated": True, "alert_id": alert_id}
    finally:
        await conn.close()


# ── Data Quality ──

@router.post("/quality/rules")
async def create_quality_rule(body: QualityRuleCreate):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        row = await conn.fetchrow(
            "INSERT INTO po_quality_rule (table_name, column_name, rule_type, rule_expr, threshold, description) "
            "VALUES ($1,$2,$3,$4,$5,$6) RETURNING rule_id",
            body.table_name, body.column_name, body.rule_type, body.rule_expr, body.threshold, body.description,
        )
        return {"rule_id": row["rule_id"]}
    finally:
        await conn.close()


@router.get("/quality/rules")
async def list_quality_rules():
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        rows = await conn.fetch("SELECT * FROM po_quality_rule ORDER BY table_name, rule_type")
        return [
            {
                "rule_id": r["rule_id"], "table_name": r["table_name"], "column_name": r["column_name"],
                "rule_type": r["rule_type"], "rule_expr": r["rule_expr"], "threshold": r["threshold"],
                "description": r["description"], "last_score": r["last_score"],
                "last_checked": r["last_checked"].isoformat() if r["last_checked"] else None,
                "status": r["status"],
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.post("/quality/check")
async def run_quality_check():
    """품질 규칙 전체 실행"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        rules = await conn.fetch("SELECT * FROM po_quality_rule WHERE status = 'active'")
        results = []
        for rule in rules:
            score = None
            error = None
            try:
                # For completeness/validity rules, execute rule_expr directly
                val = await conn.fetchval(f"SELECT ({rule['rule_expr']}) FROM {rule['table_name']}")
                score = float(val) if val is not None else 0.0
            except Exception as e:
                error = str(e)
                score = 0.0

            passed = (score >= rule["threshold"]) if rule["rule_type"] != "freshness" else (score <= rule["threshold"])

            await conn.execute(
                "UPDATE po_quality_rule SET last_score=$1, last_checked=NOW() WHERE rule_id=$2",
                score, rule["rule_id"],
            )
            await conn.execute(
                "INSERT INTO po_quality_history (rule_id, score) VALUES ($1,$2)",
                rule["rule_id"], score,
            )
            results.append({
                "rule_id": rule["rule_id"],
                "table_name": rule["table_name"],
                "column_name": rule["column_name"],
                "rule_type": rule["rule_type"],
                "score": round(score, 2),
                "threshold": rule["threshold"],
                "passed": passed,
                "error": error,
            })

        passed_cnt = sum(1 for r in results if r["passed"])
        return {
            "total_rules": len(results),
            "passed": passed_cnt,
            "failed": len(results) - passed_cnt,
            "overall_score": round(passed_cnt / len(results) * 100, 1) if results else 0,
            "results": results,
        }
    finally:
        await conn.close()


@router.get("/quality/summary")
async def quality_summary():
    """품질 요약"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        rules = await conn.fetch("SELECT * FROM po_quality_rule WHERE status = 'active'")
        total = len(rules)
        checked = sum(1 for r in rules if r["last_score"] is not None)
        passed = sum(1 for r in rules if r["last_score"] is not None and r["last_score"] >= r["threshold"])

        by_type = {}
        for r in rules:
            t = r["rule_type"]
            if t not in by_type:
                by_type[t] = {"total": 0, "passed": 0}
            by_type[t]["total"] += 1
            if r["last_score"] is not None and r["last_score"] >= r["threshold"]:
                by_type[t]["passed"] += 1

        return {
            "total_rules": total,
            "checked": checked,
            "passed": passed,
            "failed": checked - passed,
            "overall_score": round(passed / checked * 100, 1) if checked else None,
            "by_type": [{"type": k, **v} for k, v in by_type.items()],
        }
    finally:
        await conn.close()


# ── Log Retention (SER-007) ──

@router.get("/logs/retention")
async def list_retention_policies():
    """로그 보존 정책 목록"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        rows = await conn.fetch("SELECT * FROM po_log_retention_policy ORDER BY policy_id")
        policies = []
        for r in rows:
            # 각 테이블의 현재 로그 건수 조회
            row_count = 0
            oldest = None
            ts_col = r['ts_column']
            try:
                row_count = await conn.fetchval(f"SELECT COUNT(*) FROM {r['log_table']}")
                oldest_row = await conn.fetchval(f"SELECT MIN({ts_col}) FROM {r['log_table']}")
                oldest = oldest_row.isoformat() if oldest_row else None
            except Exception:
                pass
            policies.append({
                "policy_id": r["policy_id"],
                "log_table": r["log_table"],
                "display_name": r["display_name"],
                "retention_days": r["retention_days"],
                "enabled": r["enabled"],
                "current_rows": row_count,
                "oldest_record": oldest,
                "last_cleanup_at": r["last_cleanup_at"].isoformat() if r["last_cleanup_at"] else None,
                "rows_deleted_last": r["rows_deleted_last"],
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            })
        return {"policies": policies}
    finally:
        await conn.close()


@router.put("/logs/retention/{policy_id}")
async def update_retention_policy(
    policy_id: int,
    retention_days: int = Query(..., ge=30, le=3650),
    enabled: Optional[bool] = None,
):
    """보존 정책 수정"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        sets = ["retention_days = $1", "updated_at = NOW()"]
        params = [retention_days, policy_id]
        if enabled is not None:
            sets.append(f"enabled = ${len(params) + 1}")
            params.append(enabled)
        result = await conn.execute(
            f"UPDATE po_log_retention_policy SET {', '.join(sets)} WHERE policy_id = $2",
            *params,
        )
        if "UPDATE 0" in result:
            raise HTTPException(status_code=404, detail="정책을 찾을 수 없습니다")
        return {"updated": True, "policy_id": policy_id}
    finally:
        await conn.close()


@router.post("/logs/retention/cleanup")
async def run_retention_cleanup():
    """보존 기간 초과 로그 정리 실행"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        policies = await conn.fetch(
            "SELECT * FROM po_log_retention_policy WHERE enabled = TRUE"
        )
        results = []
        total_deleted = 0
        for p in policies:
            deleted = 0
            ts_col = p["ts_column"]
            try:
                result = await conn.execute(
                    f"DELETE FROM {p['log_table']} WHERE {ts_col} < NOW() - ($1 || ' days')::interval",
                    str(p["retention_days"]),
                )
                deleted = int(result.split()[-1]) if result else 0
                await conn.execute(
                    "UPDATE po_log_retention_policy SET last_cleanup_at = NOW(), rows_deleted_last = $1, updated_at = NOW() WHERE policy_id = $2",
                    deleted, p["policy_id"],
                )
            except Exception as e:
                results.append({
                    "log_table": p["log_table"],
                    "display_name": p["display_name"],
                    "error": str(e),
                    "deleted": 0,
                })
                continue
            total_deleted += deleted
            results.append({
                "log_table": p["log_table"],
                "display_name": p["display_name"],
                "retention_days": p["retention_days"],
                "deleted": deleted,
            })
        return {
            "total_deleted": total_deleted,
            "policies_processed": len(results),
            "results": results,
        }
    finally:
        await conn.close()
