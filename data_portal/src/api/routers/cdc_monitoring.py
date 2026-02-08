"""
CDC 모니터링 - 서비스 현황, 이벤트 로그, 통계
"""
from typing import Optional

from fastapi import APIRouter, Query

from .cdc_shared import get_connection, ensure_tables, ensure_seed_data

router = APIRouter()


# ═══════════════════════════════════════════════════
#  Service Overview
# ═══════════════════════════════════════════════════

@router.get("/service-status")
async def get_service_overview():
    """전체 CDC 서비스 현황"""
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        await ensure_seed_data(conn)

        connectors = await conn.fetch("SELECT connector_id, name, db_type, status FROM cdc_connector ORDER BY connector_id")
        topics = await conn.fetch("""
            SELECT connector_id, COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE status='active') AS active,
                   COUNT(*) FILTER (WHERE status='error') AS error
            FROM cdc_topic GROUP BY connector_id
        """)
        topic_map = {t["connector_id"]: t for t in topics}

        # Recent event counts (last hour)
        events = await conn.fetch("""
            SELECT connector_id,
                   COUNT(*) AS total_events,
                   COUNT(*) FILTER (WHERE event_type='INSERT') AS inserts,
                   COUNT(*) FILTER (WHERE event_type='UPDATE') AS updates,
                   COUNT(*) FILTER (WHERE event_type='DELETE') AS deletes,
                   COUNT(*) FILTER (WHERE event_type='ERROR') AS errors,
                   COALESCE(SUM(row_count), 0) AS total_rows,
                   COALESCE(AVG(latency_ms)::int, 0) AS avg_latency_ms
            FROM cdc_event_log
            WHERE created_at > NOW() - INTERVAL '1 hour'
            GROUP BY connector_id
        """)
        event_map = {e["connector_id"]: e for e in events}

        result = []
        for c in connectors:
            t = topic_map.get(c["connector_id"], {})
            e = event_map.get(c["connector_id"], {})
            result.append({
                "connector_id": c["connector_id"],
                "name": c["name"],
                "db_type": c["db_type"],
                "status": c["status"],
                "topics_total": t.get("total", 0) if isinstance(t, dict) else 0,
                "topics_active": t.get("active", 0) if isinstance(t, dict) else 0,
                "topics_error": t.get("error", 0) if isinstance(t, dict) else 0,
                "events_1h": e.get("total_events", 0) if isinstance(e, dict) else 0,
                "inserts_1h": e.get("inserts", 0) if isinstance(e, dict) else 0,
                "updates_1h": e.get("updates", 0) if isinstance(e, dict) else 0,
                "deletes_1h": e.get("deletes", 0) if isinstance(e, dict) else 0,
                "errors_1h": e.get("errors", 0) if isinstance(e, dict) else 0,
                "total_rows_1h": e.get("total_rows", 0) if isinstance(e, dict) else 0,
                "avg_latency_ms": e.get("avg_latency_ms", 0) if isinstance(e, dict) else 0,
            })

        return {
            "services": result,
            "summary": {
                "total_connectors": len(connectors),
                "running": sum(1 for c in connectors if c["status"] == "running"),
                "stopped": sum(1 for c in connectors if c["status"] == "stopped"),
                "error": sum(1 for c in connectors if c["status"] == "error"),
            },
        }
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Event Log & Monitoring
# ═══════════════════════════════════════════════════

@router.get("/events")
async def list_events(
    connector_id: Optional[int] = Query(None),
    event_type: Optional[str] = Query(None),
    limit: int = Query(100, le=500),
):
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        await ensure_seed_data(conn)
        query = "SELECT * FROM cdc_event_log WHERE 1=1"
        params = []
        idx = 1
        if connector_id is not None:
            query += f" AND connector_id = ${idx}"
            params.append(connector_id)
            idx += 1
        if event_type:
            query += f" AND event_type = ${idx}"
            params.append(event_type)
            idx += 1
        query += f" ORDER BY event_id DESC LIMIT ${idx}"
        params.append(limit)
        rows = await conn.fetch(query, *params)
        return {
            "events": [
                {
                    "event_id": r["event_id"],
                    "connector_id": r["connector_id"],
                    "topic_name": r["topic_name"],
                    "event_type": r["event_type"],
                    "table_name": r["table_name"],
                    "row_count": r["row_count"],
                    "latency_ms": r["latency_ms"],
                    "error_message": r["error_message"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.get("/events/stats")
async def get_event_stats():
    """이벤트 통계 (최근 1시간/24시간)"""
    conn = await get_connection()
    try:
        await ensure_tables(conn)
        stats_1h = await conn.fetchrow("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE event_type='INSERT') AS inserts,
                   COUNT(*) FILTER (WHERE event_type='UPDATE') AS updates,
                   COUNT(*) FILTER (WHERE event_type='DELETE') AS deletes,
                   COUNT(*) FILTER (WHERE event_type='ERROR') AS errors,
                   COALESCE(SUM(row_count), 0) AS total_rows,
                   COALESCE(AVG(latency_ms)::int, 0) AS avg_latency,
                   COALESCE(MAX(latency_ms), 0) AS max_latency
            FROM cdc_event_log WHERE created_at > NOW() - INTERVAL '1 hour'
        """)
        stats_24h = await conn.fetchrow("""
            SELECT COUNT(*) AS total, COALESCE(SUM(row_count), 0) AS total_rows,
                   COUNT(*) FILTER (WHERE event_type='ERROR') AS errors
            FROM cdc_event_log WHERE created_at > NOW() - INTERVAL '24 hours'
        """)
        # Throughput per minute (last hour)
        throughput = await conn.fetch("""
            SELECT date_trunc('minute', created_at) AS minute,
                   COUNT(*) AS events, COALESCE(SUM(row_count), 0) AS rows
            FROM cdc_event_log
            WHERE created_at > NOW() - INTERVAL '1 hour'
            GROUP BY 1 ORDER BY 1
        """)
        return {
            "last_1h": {
                "total_events": stats_1h["total"],
                "inserts": stats_1h["inserts"],
                "updates": stats_1h["updates"],
                "deletes": stats_1h["deletes"],
                "errors": stats_1h["errors"],
                "total_rows": stats_1h["total_rows"],
                "avg_latency_ms": stats_1h["avg_latency"],
                "max_latency_ms": stats_1h["max_latency"],
            },
            "last_24h": {
                "total_events": stats_24h["total"],
                "total_rows": stats_24h["total_rows"],
                "errors": stats_24h["errors"],
            },
            "throughput": [
                {"minute": t["minute"].isoformat() if t["minute"] else None, "events": t["events"], "rows": t["rows"]}
                for t in throughput
            ],
        }
    finally:
        await conn.close()
