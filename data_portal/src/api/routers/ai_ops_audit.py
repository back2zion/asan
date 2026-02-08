"""
AI Ops 감사 로그 엔드포인트
ai_ops.py 의 서브 라우터로 포함됨 (prefix 없음)
"""
from datetime import datetime
from typing import Optional, Dict

from fastapi import APIRouter, Query

from ._ai_ops_shared import _load_audit_log

router = APIRouter()


# ═══════════════════════════════════════════════════════
#  감사 로그
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
    """감사 로그 조회 (페이지네이션 + 필터)"""
    logs = _load_audit_log()
    logs.reverse()

    if model:
        logs = [l for l in logs if l.get("model") == model]
    if user:
        logs = [l for l in logs if l.get("user", "").lower().find(user.lower()) >= 0]
    if query_type:
        logs = [l for l in logs if l.get("query_type") == query_type]
    if date_from:
        logs = [l for l in logs if l.get("timestamp", "") >= date_from]
    if date_to:
        logs = [l for l in logs if l.get("timestamp", "") <= date_to + "T23:59:59"]

    total = len(logs)
    start = (page - 1) * page_size
    end = start + page_size

    return {
        "logs": logs[start:end],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


@router.get("/audit-logs/stats")
async def audit_log_stats():
    """감사 로그 통계 (실제 데이터만)"""
    logs = _load_audit_log()

    if not logs:
        return {
            "total_queries": 0,
            "avg_latency_ms": 0,
            "model_distribution": {},
            "query_type_distribution": {},
            "daily_counts": [],
            "note": "감사 로그가 없습니다. AI 대화를 사용하면 자동으로 기록됩니다.",
        }

    latencies = [l.get("latency_ms", 0) for l in logs if l.get("latency_ms")]

    model_dist: Dict[str, int] = {}
    for l in logs:
        m = l.get("model", "unknown")
        model_dist[m] = model_dist.get(m, 0) + 1

    qt_dist: Dict[str, int] = {}
    for l in logs:
        qt = l.get("query_type", "unknown")
        qt_dist[qt] = qt_dist.get(qt, 0) + 1

    daily: Dict[str, int] = {}
    for l in logs:
        day = l.get("timestamp", "")[:10]
        if day:
            daily[day] = daily.get(day, 0) + 1

    return {
        "total_queries": len(logs),
        "avg_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else 0,
        "model_distribution": model_dist,
        "query_type_distribution": qt_dist,
        "daily_counts": [{"date": k, "count": v} for k, v in sorted(daily.items())],
    }


@router.get("/audit/export")
async def export_audit_log(
    model: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """감사 로그 CSV 내보내기"""
    from fastapi.responses import Response
    import csv
    import io

    logs = _load_audit_log()
    logs.reverse()

    if model:
        logs = [l for l in logs if l.get("model") == model]
    if date_from:
        logs = [l for l in logs if l.get("timestamp", "") >= date_from]
    if date_to:
        logs = [l for l in logs if l.get("timestamp", "") <= date_to + "T23:59:59"]

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
