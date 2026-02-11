"""
ETL Jobs Alerts: Execution logs, alert rules, alert history endpoints.
"""
import json
import asyncio
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
import asyncpg

from routers.etl_jobs_shared import (
    OMOP_DB_CONFIG, get_connection, release_connection, _ensure_tables, _ensure_seed_data,
    AlertRuleCreate,
)
from services.redis_cache import cached

router = APIRouter()


# ═══════════════════════════════════════════════════
#  Execution Logs
# ═══════════════════════════════════════════════════

@router.get("/logs")
@cached("etl-logs", ttl=60)
async def list_logs(
    job_id: Optional[int] = Query(None),
    status: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    limit: int = Query(50, le=200),
):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        query = """
            SELECT l.*, j.name AS job_name, j.job_type
            FROM etl_execution_log l
            LEFT JOIN etl_job j ON j.job_id = l.job_id
            WHERE 1=1
        """
        params = []
        idx = 1
        if job_id is not None:
            query += f" AND l.job_id = ${idx}"
            params.append(job_id)
            idx += 1
        if status:
            query += f" AND l.status = ${idx}"
            params.append(status)
            idx += 1
        if date_from:
            query += f" AND l.started_at >= ${idx}::timestamp"
            params.append(date_from)
            idx += 1
        if date_to:
            query += f" AND l.started_at <= ${idx}::timestamp"
            params.append(date_to)
            idx += 1
        query += f" ORDER BY l.log_id DESC LIMIT ${idx}"
        params.append(limit)

        rows = await conn.fetch(query, *params)

        # Stats
        stats = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status='success') AS success_count,
                COUNT(*) FILTER (WHERE status='failed') AS failed_count,
                COALESCE(AVG(EXTRACT(EPOCH FROM (ended_at - started_at)))::int, 0) AS avg_duration_sec,
                COALESCE(SUM(rows_processed), 0) AS total_rows
            FROM etl_execution_log
        """)

        return {
            "logs": [
                {
                    "log_id": r["log_id"],
                    "job_id": r["job_id"],
                    "job_name": r["job_name"],
                    "job_type": r["job_type"],
                    "run_id": r["run_id"],
                    "status": r["status"],
                    "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                    "ended_at": r["ended_at"].isoformat() if r["ended_at"] else None,
                    "duration_sec": int((r["ended_at"] - r["started_at"]).total_seconds()) if r["ended_at"] and r["started_at"] else None,
                    "rows_processed": r["rows_processed"],
                    "rows_failed": r["rows_failed"],
                    "error_message": r["error_message"],
                }
                for r in rows
            ],
            "stats": {
                "total": stats["total"],
                "success_count": stats["success_count"],
                "failed_count": stats["failed_count"],
                "success_rate": round(stats["success_count"] / max(stats["total"], 1) * 100, 1),
                "avg_duration_sec": stats["avg_duration_sec"],
                "total_rows": stats["total_rows"],
            },
        }
    finally:
        await release_connection(conn)


@router.get("/logs/stream")
async def stream_logs():
    """SSE 실시간 로그 스트리밍"""
    async def event_generator():
        last_id = 0
        while True:
            try:
                conn = await get_connection()
                try:
                    rows = await conn.fetch("""
                        SELECT l.log_id, l.job_id, l.status, l.started_at, l.rows_processed,
                               j.name AS job_name
                        FROM etl_execution_log l
                        LEFT JOIN etl_job j ON j.job_id = l.job_id
                        WHERE l.log_id > $1
                        ORDER BY l.log_id DESC LIMIT 10
                    """, last_id)
                    for r in reversed(rows):
                        last_id = max(last_id, r["log_id"])
                        payload = {
                            "log_id": r["log_id"],
                            "job_id": r["job_id"],
                            "job_name": r["job_name"],
                            "status": r["status"],
                            "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                            "rows_processed": r["rows_processed"],
                        }
                        yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"
                finally:
                    await release_connection(conn)
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e)}, ensure_ascii=False)}\n\n"
            await asyncio.sleep(2)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@router.get("/logs/{log_id}")
async def get_log_detail(log_id: int):
    conn = await get_connection()
    try:
        r = await conn.fetchrow("""
            SELECT l.*, j.name AS job_name, j.job_type
            FROM etl_execution_log l
            LEFT JOIN etl_job j ON j.job_id = l.job_id
            WHERE l.log_id = $1
        """, log_id)
        if not r:
            raise HTTPException(status_code=404, detail="로그를 찾을 수 없습니다")
        return {
            "log_id": r["log_id"],
            "job_id": r["job_id"],
            "job_name": r["job_name"],
            "job_type": r["job_type"],
            "run_id": r["run_id"],
            "status": r["status"],
            "started_at": r["started_at"].isoformat() if r["started_at"] else None,
            "ended_at": r["ended_at"].isoformat() if r["ended_at"] else None,
            "rows_processed": r["rows_processed"],
            "rows_failed": r["rows_failed"],
            "error_message": r["error_message"],
            "log_entries": json.loads(r["log_entries"]) if isinstance(r["log_entries"], str) else (r["log_entries"] or []),
        }
    finally:
        await release_connection(conn)


# ═══════════════════════════════════════════════════
#  Alert Rules & History
# ═══════════════════════════════════════════════════

@router.get("/alert-rules")
async def list_alert_rules():
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        rows = await conn.fetch("""
            SELECT r.*, j.name AS job_name
            FROM etl_alert_rule r
            LEFT JOIN etl_job j ON j.job_id = r.job_id
            ORDER BY r.rule_id
        """)
        return {
            "rules": [
                {
                    "rule_id": r["rule_id"],
                    "name": r["name"],
                    "condition_type": r["condition_type"],
                    "threshold": json.loads(r["threshold"]) if isinstance(r["threshold"], str) else (r["threshold"] or {}),
                    "job_id": r["job_id"],
                    "job_name": r["job_name"],
                    "channels": list(r["channels"]) if r["channels"] else [],
                    "webhook_url": r["webhook_url"],
                    "enabled": r["enabled"],
                }
                for r in rows
            ]
        }
    finally:
        await release_connection(conn)


@router.post("/alert-rules")
async def create_alert_rule(body: AlertRuleCreate):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        rid = await conn.fetchval("""
            INSERT INTO etl_alert_rule (name, condition_type, threshold, job_id, channels, webhook_url, enabled)
            VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7) RETURNING rule_id
        """, body.name, body.condition_type, json.dumps(body.threshold or {}),
             body.job_id, body.channels, body.webhook_url, body.enabled)
        return {"success": True, "rule_id": rid}
    finally:
        await release_connection(conn)


@router.put("/alert-rules/{rule_id}")
async def update_alert_rule(rule_id: int, body: AlertRuleCreate):
    conn = await get_connection()
    try:
        result = await conn.execute("""
            UPDATE etl_alert_rule SET name=$1, condition_type=$2, threshold=$3::jsonb,
                job_id=$4, channels=$5, webhook_url=$6, enabled=$7
            WHERE rule_id=$8
        """, body.name, body.condition_type, json.dumps(body.threshold or {}),
             body.job_id, body.channels, body.webhook_url, body.enabled, rule_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="규칙을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await release_connection(conn)


@router.delete("/alert-rules/{rule_id}")
async def delete_alert_rule(rule_id: int):
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM etl_alert_rule WHERE rule_id=$1", rule_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="규칙을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await release_connection(conn)


@router.get("/alerts")
async def list_alerts(
    severity: Optional[str] = Query(None),
    acknowledged: Optional[bool] = Query(None),
    limit: int = Query(50, le=200),
):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        query = """
            SELECT h.*, r.name AS rule_name, j.name AS job_name
            FROM etl_alert_history h
            LEFT JOIN etl_alert_rule r ON r.rule_id = h.rule_id
            LEFT JOIN etl_job j ON j.job_id = h.job_id
            WHERE 1=1
        """
        params = []
        idx = 1
        if severity:
            query += f" AND h.severity = ${idx}"
            params.append(severity)
            idx += 1
        if acknowledged is not None:
            query += f" AND h.acknowledged = ${idx}"
            params.append(acknowledged)
            idx += 1
        query += f" ORDER BY h.created_at DESC LIMIT ${idx}"
        params.append(limit)

        rows = await conn.fetch(query, *params)
        return {
            "alerts": [
                {
                    "alert_id": r["alert_id"],
                    "rule_id": r["rule_id"],
                    "rule_name": r["rule_name"],
                    "job_id": r["job_id"],
                    "job_name": r["job_name"],
                    "severity": r["severity"],
                    "title": r["title"],
                    "message": r["message"],
                    "acknowledged": r["acknowledged"],
                    "acknowledged_by": r["acknowledged_by"],
                    "acknowledged_at": r["acknowledged_at"].isoformat() if r["acknowledged_at"] else None,
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await release_connection(conn)


@router.put("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: int):
    conn = await get_connection()
    try:
        result = await conn.execute("""
            UPDATE etl_alert_history SET acknowledged=TRUE, acknowledged_by='admin', acknowledged_at=NOW()
            WHERE alert_id=$1
        """, alert_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="알림을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await release_connection(conn)


@router.get("/alerts/unread-count")
async def get_unread_alert_count():
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM etl_alert_history WHERE acknowledged = FALSE"
        )
        return {"unread_count": count}
    finally:
        await release_connection(conn)
