"""
Change Request Workflow + Overview endpoint
"""
import json
from typing import Optional

from fastapi import APIRouter

from .metamgmt_shared import get_connection, _init, ChangeRequestCreate

router = APIRouter()


# ══════════════════════════════════════════════
#  1. Change Request Workflow
# ══════════════════════════════════════════════

@router.get("/change-requests")
async def list_change_requests(status: Optional[str] = None, request_type: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if status:
            params.append(status)
            where.append(f"status = ${len(params)}")
        if request_type:
            params.append(request_type)
            where.append(f"request_type = ${len(params)}")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"SELECT * FROM meta_change_request {w} ORDER BY created_at DESC", *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/change-requests")
async def create_change_request(body: ChangeRequestCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO meta_change_request (request_type, target_table, target_column, title, description,
                change_detail, requester, priority, status)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,'draft') RETURNING request_id
        """, body.request_type, body.target_table, body.target_column, body.title, body.description,
            json.dumps(body.change_detail) if body.change_detail else None,
            body.requester, body.priority)
        return {"request_id": row["request_id"]}
    finally:
        await conn.close()


@router.put("/change-requests/{request_id}/submit")
async def submit_change_request(request_id: int):
    """draft -> submitted"""
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE meta_change_request SET status='submitted', updated_at=NOW()
            WHERE request_id=$1 AND status='draft'
        """, request_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.put("/change-requests/{request_id}/review")
async def review_change_request(request_id: int, reviewer: str = "검토자", comment: Optional[str] = None):
    """submitted -> reviewing"""
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE meta_change_request SET status='reviewing', reviewer=$2, review_comment=$3,
                reviewed_at=NOW(), updated_at=NOW()
            WHERE request_id=$1 AND status='submitted'
        """, request_id, reviewer, comment)
        return {"ok": True}
    finally:
        await conn.close()


@router.put("/change-requests/{request_id}/approve")
async def approve_change_request(request_id: int, approver: str = "승인자", comment: Optional[str] = None):
    """reviewing -> approved"""
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE meta_change_request SET status='approved', approver=$2, approval_comment=$3,
                approved_at=NOW(), updated_at=NOW()
            WHERE request_id=$1 AND status IN ('submitted','reviewing')
        """, request_id, approver, comment)
        return {"ok": True}
    finally:
        await conn.close()


@router.put("/change-requests/{request_id}/reject")
async def reject_change_request(request_id: int, approver: str = "승인자", comment: Optional[str] = None):
    """reviewing -> rejected"""
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE meta_change_request SET status='rejected', approver=$2, approval_comment=$3,
                approved_at=NOW(), updated_at=NOW()
            WHERE request_id=$1 AND status IN ('submitted','reviewing')
        """, request_id, approver, comment)
        return {"ok": True}
    finally:
        await conn.close()


@router.put("/change-requests/{request_id}/apply")
async def apply_change_request(request_id: int):
    """approved -> applied"""
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE meta_change_request SET status='applied', applied_at=NOW(), updated_at=NOW()
            WHERE request_id=$1 AND status='approved'
        """, request_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/change-requests/stats")
async def change_request_stats():
    conn = await get_connection()
    try:
        await _init(conn)
        rows = await conn.fetch("SELECT status, COUNT(*) cnt FROM meta_change_request GROUP BY status")
        return {r["status"]: r["cnt"] for r in rows}
    finally:
        await conn.close()


# ══════════════════════════════════════════════
#  Overview (cross-domain summary)
# ══════════════════════════════════════════════

@router.get("/overview")
async def get_overview():
    conn = await get_connection()
    try:
        await _init(conn)
        cr_stats = await conn.fetch("SELECT status, COUNT(*) cnt FROM meta_change_request GROUP BY status")
        qr_total = await conn.fetchval("SELECT COUNT(*) FROM meta_quality_rule WHERE enabled=TRUE")
        sm_total = await conn.fetchval("SELECT COUNT(*) FROM meta_source_mapping")
        sm_systems = await conn.fetchval("SELECT COUNT(DISTINCT source_system) FROM meta_source_mapping")
        comp_pass = await conn.fetchval("""
            SELECT COUNT(*) FROM meta_compliance_rule cr
            JOIN LATERAL (SELECT result FROM meta_compliance_status WHERE rule_id=cr.rule_id ORDER BY check_time DESC LIMIT 1) cs ON TRUE
            WHERE cr.enabled=TRUE AND cs.result='pass'
        """)
        comp_total = await conn.fetchval("SELECT COUNT(*) FROM meta_compliance_rule WHERE enabled=TRUE")
        pl_total = await conn.fetchval("SELECT COUNT(*) FROM meta_pipeline_biz")
        pl_running = await conn.fetchval("SELECT COUNT(*) FROM meta_pipeline_biz WHERE last_status='running'")

        return {
            "change_requests": {r["status"]: r["cnt"] for r in cr_stats},
            "quality_rules": qr_total,
            "source_mappings": sm_total,
            "source_systems": sm_systems,
            "compliance_pass": comp_pass or 0,
            "compliance_total": comp_total,
            "compliance_rate": round((comp_pass or 0) / max(comp_total, 1) * 100, 1),
            "pipelines_total": pl_total,
            "pipelines_running": pl_running,
        }
    finally:
        await conn.close()
