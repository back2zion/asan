"""
DGR-005: 재식별 허용, 사용자 속성, 접근 감사, 전체 현황
- Reidentification Requests (섹션 4)
- User Attributes (섹션 5)
- Access Audit Log (섹션 8)
- Overview (섹션 9)
"""
import json as _json
from datetime import datetime, timedelta
from typing import Optional
from fastapi import APIRouter, HTTPException, Query

from .secmgmt_shared import (
    get_conn, ensure_tables, ensure_seed,
    ReidRequestCreate, ReidScopeUpdate, UserAttributeCreate,
)

router = APIRouter()


# ═══════════════════════════════════════════════════════════
# 4. Reidentification Requests (재식별 허용 신청)
# ═══════════════════════════════════════════════════════════

@router.get("/reid-requests")
async def get_reid_requests(
    status: Optional[str] = None,
    department: Optional[str] = None,
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM sec_reid_request WHERE 1=1"
        args = []
        if status:
            args.append(status)
            q += f" AND status=${len(args)}"
        if department:
            args.append(department)
            q += f" AND department=${len(args)}"
        q += " ORDER BY created_at DESC"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.get("/reid-requests/{request_id}")
async def get_reid_request_detail(request_id: int):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("SELECT * FROM sec_reid_request WHERE request_id=$1", request_id)
        if not row:
            raise HTTPException(404, "Request not found")
        result = dict(row)
        # Include related access logs
        logs = await conn.fetch(
            "SELECT * FROM sec_access_log WHERE user_id IN (SELECT user_id FROM sec_user_attribute WHERE user_name=$1) ORDER BY created_at DESC LIMIT 10",
            row["requester"])
        result["recent_access_logs"] = [dict(l) for l in logs]
        return result
    finally:
        await conn.close()


@router.post("/reid-requests")
async def create_reid_request(body: ReidRequestCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO sec_reid_request (requester, department, purpose, justification, target_tables, target_columns, row_filter, duration_days, status)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'submitted') RETURNING *
        """, body.requester, body.department, body.purpose, body.justification, body.target_tables, body.target_columns, body.row_filter, body.duration_days)
        return dict(row)
    finally:
        await conn.close()


@router.put("/reid-requests/{request_id}/review")
async def review_reid_request(request_id: int, reviewer: str = "관리자"):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE sec_reid_request SET status='reviewing', reviewer=$2, updated_at=NOW()
            WHERE request_id=$1 AND status='submitted' RETURNING *
        """, request_id, reviewer)
        if not row:
            raise HTTPException(400, "Request not in submitted status")
        return dict(row)
    finally:
        await conn.close()


@router.put("/reid-requests/{request_id}/approve")
async def approve_reid_request(request_id: int, body: ReidScopeUpdate, reviewer_comment: Optional[str] = None):
    """승인 시 재식별 범위를 설정하여 처리"""
    conn = await get_conn()
    try:
        expires = None
        if body.expires_at:
            expires = datetime.fromisoformat(body.expires_at)
        else:
            req = await conn.fetchrow("SELECT duration_days FROM sec_reid_request WHERE request_id=$1", request_id)
            if req:
                expires = datetime.now() + timedelta(days=req["duration_days"])
        row = await conn.fetchrow("""
            UPDATE sec_reid_request SET status='approved', reviewer_comment=$2, approved_at=NOW(),
            expires_at=$3, scope_tables=$4, scope_columns=$5, scope_row_filter=$6, scope_max_rows=$7,
            updated_at=NOW()
            WHERE request_id=$1 AND status IN ('submitted','reviewing') RETURNING *
        """, request_id, reviewer_comment, expires, body.scope_tables, body.scope_columns, body.row_filter, body.max_rows)
        if not row:
            raise HTTPException(400, "Request not in submitted/reviewing status")
        return dict(row)
    finally:
        await conn.close()


@router.put("/reid-requests/{request_id}/reject")
async def reject_reid_request(request_id: int, reviewer_comment: str = "반려"):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE sec_reid_request SET status='rejected', reviewer_comment=$2, updated_at=NOW()
            WHERE request_id=$1 AND status IN ('submitted','reviewing') RETURNING *
        """, request_id, reviewer_comment)
        if not row:
            raise HTTPException(400, "Request not in submitted/reviewing status")
        return dict(row)
    finally:
        await conn.close()


@router.put("/reid-requests/{request_id}/revoke")
async def revoke_reid_request(request_id: int):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE sec_reid_request SET status='revoked', updated_at=NOW()
            WHERE request_id=$1 AND status='approved' RETURNING *
        """, request_id)
        if not row:
            raise HTTPException(400, "Request not in approved status")
        return dict(row)
    finally:
        await conn.close()


@router.get("/reid-requests/stats")
async def get_reid_request_stats():
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        stats = {}
        stats["by_status"] = [dict(r) for r in await conn.fetch(
            "SELECT status, COUNT(*) as cnt FROM sec_reid_request GROUP BY status ORDER BY cnt DESC")]
        stats["total"] = await conn.fetchval("SELECT COUNT(*) FROM sec_reid_request")
        stats["pending_count"] = await conn.fetchval(
            "SELECT COUNT(*) FROM sec_reid_request WHERE status IN ('submitted','reviewing')")
        stats["active_count"] = await conn.fetchval(
            "SELECT COUNT(*) FROM sec_reid_request WHERE status='approved' AND (expires_at IS NULL OR expires_at > NOW())")
        stats["expired_count"] = await conn.fetchval(
            "SELECT COUNT(*) FROM sec_reid_request WHERE status='approved' AND expires_at < NOW()")
        return stats
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 5. User Attributes (사용자 속성 관리)
# ═══════════════════════════════════════════════════════════

@router.get("/user-attributes")
async def get_user_attributes(
    department: Optional[str] = None,
    rank: Optional[str] = None,
    active: Optional[bool] = None,
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM sec_user_attribute WHERE 1=1"
        args = []
        if department:
            args.append(department)
            q += f" AND department=${len(args)}"
        if rank:
            args.append(rank)
            q += f" AND rank=${len(args)}"
        if active is not None:
            args.append(active)
            q += f" AND active=${len(args)}"
        q += " ORDER BY department, user_name"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/user-attributes")
async def create_user_attribute(body: UserAttributeCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO sec_user_attribute (user_id, user_name, department, research_field, rank, role_id, attributes)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *
        """, body.user_id, body.user_name, body.department, body.research_field, body.rank, body.role_id,
            _json.dumps(body.attributes or {}))
        return dict(row)
    finally:
        await conn.close()


@router.put("/user-attributes/{attr_id}")
async def update_user_attribute(attr_id: int, body: UserAttributeCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE sec_user_attribute SET user_id=$2, user_name=$3, department=$4, research_field=$5,
            rank=$6, role_id=$7, attributes=$8, updated_at=NOW()
            WHERE attr_id=$1 RETURNING *
        """, attr_id, body.user_id, body.user_name, body.department, body.research_field, body.rank,
            body.role_id, _json.dumps(body.attributes or {}))
        if not row:
            raise HTTPException(404, "User attribute not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/user-attributes/{attr_id}")
async def delete_user_attribute(attr_id: int):
    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM sec_user_attribute WHERE attr_id=$1", attr_id)
        return {"deleted": True}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 8. Access Audit Log (접근 감사 로그)
# ═══════════════════════════════════════════════════════════

@router.get("/access-logs")
async def get_access_logs(
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    result: Optional[str] = None,
    limit: int = Query(default=50, le=200),
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM sec_access_log WHERE 1=1"
        args = []
        if user_id:
            args.append(user_id)
            q += f" AND user_id=${len(args)}"
        if action:
            args.append(action)
            q += f" AND action=${len(args)}"
        if result:
            args.append(result)
            q += f" AND result=${len(args)}"
        args.append(limit)
        q += f" ORDER BY created_at DESC LIMIT ${len(args)}"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.get("/access-logs/stats")
async def get_access_log_stats():
    """접근 감사 통계"""
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        stats = {}
        stats["total"] = await conn.fetchval("SELECT COUNT(*) FROM sec_access_log")
        stats["by_result"] = [dict(r) for r in await conn.fetch(
            "SELECT result, COUNT(*) as cnt FROM sec_access_log GROUP BY result ORDER BY cnt DESC")]
        stats["by_action"] = [dict(r) for r in await conn.fetch(
            "SELECT action, COUNT(*) as cnt FROM sec_access_log GROUP BY action ORDER BY cnt DESC")]
        stats["by_user"] = [dict(r) for r in await conn.fetch(
            "SELECT user_name, COUNT(*) as cnt, COUNT(*) FILTER (WHERE result='denied') as denied_cnt FROM sec_access_log GROUP BY user_name ORDER BY cnt DESC")]
        stats["denied_count"] = await conn.fetchval("SELECT COUNT(*) FROM sec_access_log WHERE result='denied'")
        stats["recent_denied"] = [dict(r) for r in await conn.fetch(
            "SELECT * FROM sec_access_log WHERE result='denied' ORDER BY created_at DESC LIMIT 5")]
        return stats
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 9. Overview (전체 보안 현황)
# ═══════════════════════════════════════════════════════════

@router.get("/overview")
async def get_security_overview():
    """DGR-005 전체 보안 관리 현황 대시보드"""
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        overview = {}
        overview["policies"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM sec_policy"),
            "critical": await conn.fetchval("SELECT COUNT(*) FROM sec_policy WHERE security_level='극비'"),
            "sensitive": await conn.fetchval("SELECT COUNT(*) FROM sec_policy WHERE security_level='민감'"),
        }
        overview["term_rules"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM sec_term_rule"),
            "auto_propagate": await conn.fetchval("SELECT COUNT(*) FROM sec_term_rule WHERE auto_propagate=TRUE"),
        }
        overview["biz_security"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM sec_biz_meta"),
            "deident_targets": await conn.fetchval("SELECT COUNT(*) FROM sec_biz_meta WHERE deident_target=TRUE"),
        }
        overview["reid_requests"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM sec_reid_request"),
            "pending": await conn.fetchval("SELECT COUNT(*) FROM sec_reid_request WHERE status IN ('submitted','reviewing')"),
            "active": await conn.fetchval("SELECT COUNT(*) FROM sec_reid_request WHERE status='approved' AND (expires_at IS NULL OR expires_at > NOW())"),
        }
        overview["users"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM sec_user_attribute"),
            "active": await conn.fetchval("SELECT COUNT(*) FROM sec_user_attribute WHERE active=TRUE"),
        }
        overview["dynamic_policies"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM sec_dynamic_policy"),
            "enabled": await conn.fetchval("SELECT COUNT(*) FROM sec_dynamic_policy WHERE enabled=TRUE"),
            "by_type": [dict(r) for r in await conn.fetch(
                "SELECT policy_type, COUNT(*) as cnt FROM sec_dynamic_policy GROUP BY policy_type")],
        }
        overview["masking_rules"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM sec_masking_rule"),
            "by_level": [dict(r) for r in await conn.fetch(
                "SELECT masking_level, COUNT(*) as cnt FROM sec_masking_rule GROUP BY masking_level")],
        }
        overview["access_logs"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM sec_access_log"),
            "denied": await conn.fetchval("SELECT COUNT(*) FROM sec_access_log WHERE result='denied'"),
            "masked": await conn.fetchval("SELECT COUNT(*) FROM sec_access_log WHERE result='masked'"),
        }
        return overview
    finally:
        await conn.close()
