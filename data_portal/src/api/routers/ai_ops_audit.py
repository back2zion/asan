"""
AI Ops 감사 로그 엔드포인트
ai_ops.py 의 서브 라우터로 포함됨 (prefix 없음)
PostgreSQL-backed via _ai_ops_shared
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query

from ._ai_ops_shared import (
    db_load_audit_logs,
    db_audit_stats,
    db_export_audit_logs,
)

router = APIRouter()


# ═══════════════════════════════════════════════════════
#  감사 로그 (PostgreSQL)
# ═══════════════════════════════════════════════════════

@router.get("/audit-logs")
async def get_audit_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    model: Optional[str] = None,
    user: Optional[str] = None,
    query_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """감사 로그 조회 (PostgreSQL — 페이지네이션 + 필터)"""
    return await db_load_audit_logs(
        page=page,
        page_size=page_size,
        model=model,
        user=user,
        query_type=query_type,
        date_from=date_from,
        date_to=date_to,
    )


@router.get("/audit-logs/stats")
async def audit_log_stats():
    """감사 로그 통계 (PostgreSQL)"""
    return await db_audit_stats()


@router.get("/audit/export")
async def export_audit_log(
    model: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """감사 로그 CSV 내보내기 (PostgreSQL)"""
    from fastapi.responses import Response
    import csv
    import io

    logs = await db_export_audit_logs(model=model, date_from=date_from, date_to=date_to)

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Timestamp", "User", "Model", "Query Type", "Latency (ms)", "Tokens", "PII Count", "Hallucination", "Query"])
    for l in logs:
        writer.writerow([
            l.get("id", ""),
            l.get("timestamp", ""),
            l.get("user", ""),
            l.get("model", ""),
            l.get("query_type", ""),
            l.get("latency_ms", ""),
            l.get("tokens", ""),
            l.get("pii_count", 0),
            l.get("hallucination_status", ""),
            l.get("query", "")[:200],
        ])

    csv_content = output.getvalue()
    return Response(
        content=csv_content.encode("utf-8-sig"),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=audit_logs_{datetime.now().strftime('%Y%m%d')}.csv"},
    )
