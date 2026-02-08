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
            "INSERT INTO po_access_log (user_id, user_name, action, resource, ip_address, user_agent, duration_ms, details) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb) RETURNING log_id, created_at",
            body.user_id, body.user_name, body.action, body.resource,
            body.ip_address, body.user_agent, body.duration_ms, json.dumps(body.details),
        )
        return {"log_id": row["log_id"], "created_at": row["created_at"].isoformat()}
    finally:
        await conn.close()


@router.get("/logs/access")
async def list_access_logs(
    user_id: Optional[str] = None,
    action: Optional[str] = None,
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
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit)
        rows = await conn.fetch(
            f"SELECT * FROM po_access_log {where} ORDER BY created_at DESC LIMIT ${idx}",
            *params,
        )
        return [
            {
                "log_id": r["log_id"], "user_id": r["user_id"], "user_name": r["user_name"],
                "action": r["action"], "resource": r["resource"], "ip_address": r["ip_address"],
                "duration_ms": r["duration_ms"], "details": r["details"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.get("/logs/access/stats")
async def access_log_stats():
    """접속 로그 통계"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        total = await conn.fetchval("SELECT COUNT(*) FROM po_access_log")
        by_action = await conn.fetch(
            "SELECT action, COUNT(*) AS cnt FROM po_access_log GROUP BY action ORDER BY cnt DESC"
        )
        by_user = await conn.fetch(
            "SELECT user_id, user_name, COUNT(*) AS cnt FROM po_access_log GROUP BY user_id, user_name ORDER BY cnt DESC LIMIT 10"
        )
        today = await conn.fetchval(
            "SELECT COUNT(*) FROM po_access_log WHERE created_at >= CURRENT_DATE"
        )
        return {
            "total_logs": total,
            "today_logs": today,
            "by_action": [{"action": r["action"], "count": r["cnt"]} for r in by_action],
            "top_users": [{"user_id": r["user_id"], "user_name": r["user_name"], "count": r["cnt"]} for r in by_user],
        }
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
