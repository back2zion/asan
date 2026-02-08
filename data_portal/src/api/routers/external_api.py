"""
바-3: 외부 연동 API 게이트웨이 — API 키 인증 + 사용량 추적 + 표준 인터페이스
RFP 요구: 빅데이터 분석지원 포털 연계, 외부 다기관 데이터 표준화
"""
import os
import hmac
import hashlib
import secrets
import time
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Header, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/external", tags=["ExternalAPI"])

async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)

# ── 모델 ──

class PartnerCreate(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    organization: str = Field(..., max_length=200)
    contact_email: str = Field(..., max_length=200)
    purpose: str = Field(..., max_length=500)
    allowed_resources: list = Field(default_factory=lambda: ["Patient", "Condition", "Observation"])
    rate_limit_per_hour: int = Field(default=1000, ge=10, le=100000)

class PartnerUpdate(BaseModel):
    is_active: Optional[bool] = None
    rate_limit_per_hour: Optional[int] = Field(None, ge=10, le=100000)
    allowed_resources: Optional[list] = None

# ── 테이블 ──

_tbl_ok = False
async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS ext_partner (
            partner_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            organization VARCHAR(200) NOT NULL,
            contact_email VARCHAR(200) NOT NULL,
            purpose TEXT,
            api_key_hash VARCHAR(200) NOT NULL,
            api_key_prefix VARCHAR(10) NOT NULL,
            allowed_resources JSONB DEFAULT '["Patient","Condition","Observation"]',
            rate_limit_per_hour INTEGER DEFAULT 1000,
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            last_used_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS ext_api_usage (
            usage_id BIGSERIAL PRIMARY KEY,
            partner_id INTEGER REFERENCES ext_partner(partner_id),
            endpoint VARCHAR(200) NOT NULL,
            method VARCHAR(10) NOT NULL,
            status_code INTEGER,
            response_time_ms DOUBLE PRECISION,
            ip_address VARCHAR(45),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_ext_usage_partner ON ext_api_usage(partner_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_ext_usage_time ON ext_api_usage(created_at);
    """)
    _tbl_ok = True

def _hash_key(key: str) -> str:
    return hashlib.sha256(key.encode()).hexdigest()


# ══════════════════════════════════════════
# 파트너 관리 (관리자용)
# ══════════════════════════════════════════

@router.post("/partners")
async def create_partner(body: PartnerCreate):
    """외부 파트너 등록 + API 키 발급"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        api_key = f"asan_{secrets.token_hex(24)}"
        prefix = api_key[:10]
        hashed = _hash_key(api_key)
        import json
        pid = await conn.fetchval("""
            INSERT INTO ext_partner (name, organization, contact_email, purpose, api_key_hash, api_key_prefix, allowed_resources, rate_limit_per_hour)
            VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8) RETURNING partner_id
        """, body.name, body.organization, body.contact_email, body.purpose,
            hashed, prefix, json.dumps(body.allowed_resources), body.rate_limit_per_hour)
        return {
            "partner_id": pid,
            "api_key": api_key,
            "message": "API 키를 안전하게 보관하세요. 다시 조회할 수 없습니다."
        }
    finally:
        await _rel(conn)

@router.get("/partners")
async def list_partners():
    """파트너 목록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("""
            SELECT partner_id, name, organization, contact_email, api_key_prefix,
                   allowed_resources, rate_limit_per_hour, is_active, created_at, last_used_at
            FROM ext_partner ORDER BY created_at DESC
        """)
        return [dict(r) for r in rows]
    finally:
        await _rel(conn)

@router.put("/partners/{partner_id}")
async def update_partner(partner_id: int, body: PartnerUpdate):
    """파트너 설정 변경"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        existing = await conn.fetchrow("SELECT * FROM ext_partner WHERE partner_id=$1", partner_id)
        if not existing:
            raise HTTPException(404, "파트너를 찾을 수 없습니다")
        if body.is_active is not None:
            await conn.execute("UPDATE ext_partner SET is_active=$1 WHERE partner_id=$2", body.is_active, partner_id)
        if body.rate_limit_per_hour is not None:
            await conn.execute("UPDATE ext_partner SET rate_limit_per_hour=$1 WHERE partner_id=$2", body.rate_limit_per_hour, partner_id)
        if body.allowed_resources is not None:
            import json
            await conn.execute("UPDATE ext_partner SET allowed_resources=$1::jsonb WHERE partner_id=$2", json.dumps(body.allowed_resources), partner_id)
        return {"partner_id": partner_id, "updated": True}
    finally:
        await _rel(conn)

@router.post("/partners/{partner_id}/rotate-key")
async def rotate_api_key(partner_id: int):
    """API 키 재발급"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        existing = await conn.fetchrow("SELECT * FROM ext_partner WHERE partner_id=$1", partner_id)
        if not existing:
            raise HTTPException(404, "파트너를 찾을 수 없습니다")
        new_key = f"asan_{secrets.token_hex(24)}"
        prefix = new_key[:10]
        hashed = _hash_key(new_key)
        await conn.execute("UPDATE ext_partner SET api_key_hash=$1, api_key_prefix=$2 WHERE partner_id=$3", hashed, prefix, partner_id)
        return {"partner_id": partner_id, "api_key": new_key, "message": "새 API 키가 발급되었습니다."}
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# API 키 검증 유틸
# ══════════════════════════════════════════

async def _verify_api_key(conn, api_key: str) -> dict:
    """API 키 검증 + rate limit 체크"""
    hashed = _hash_key(api_key)
    partner = await conn.fetchrow("SELECT * FROM ext_partner WHERE api_key_hash=$1", hashed)
    if not partner:
        raise HTTPException(401, "유효하지 않은 API 키")
    if not partner["is_active"]:
        raise HTTPException(403, "비활성화된 파트너 계정")
    # rate limit
    hour_ago = datetime.utcnow().replace(second=0, microsecond=0)
    usage_count = await conn.fetchval(
        "SELECT COUNT(*) FROM ext_api_usage WHERE partner_id=$1 AND created_at > NOW() - INTERVAL '1 hour'",
        partner["partner_id"])
    if usage_count >= partner["rate_limit_per_hour"]:
        raise HTTPException(429, f"시간당 요청 한도 초과: {usage_count}/{partner['rate_limit_per_hour']}")
    await conn.execute("UPDATE ext_partner SET last_used_at=NOW() WHERE partner_id=$1", partner["partner_id"])
    return dict(partner)


# ══════════════════════════════════════════
# 외부 API 엔드포인트 (API 키 인증)
# ══════════════════════════════════════════

@router.get("/v1/patients")
async def ext_patients(
    x_api_key: str = Header(..., alias="X-API-Key"),
    gender: Optional[str] = None,
    _count: int = Query(20, le=100, alias="_count"),
):
    """외부 파트너용 환자 목록 (API 키 인증)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        t0 = time.time()
        partner = await _verify_api_key(conn, x_api_key)
        if "Patient" not in (partner.get("allowed_resources") or []):
            raise HTTPException(403, "Patient 리소스 접근 권한 없음")
        q = "SELECT person_id, gender_source_value, year_of_birth FROM person WHERE 1=1"
        params, idx = [], 1
        if gender:
            g = {"male": "M", "female": "F"}.get(gender, gender)
            q += f" AND gender_source_value=${idx}"; params.append(g); idx += 1
        q += f" ORDER BY person_id LIMIT ${idx}"; params.append(_count)
        rows = await conn.fetch(q, *params)
        ms = (time.time()-t0)*1000
        await conn.execute("INSERT INTO ext_api_usage (partner_id, endpoint, method, status_code, response_time_ms) VALUES ($1,$2,$3,$4,$5)",
            partner["partner_id"], "/external/v1/patients", "GET", 200, ms)
        return {"total": len(rows), "data": [dict(r) for r in rows]}
    finally:
        await _rel(conn)

@router.get("/v1/conditions")
async def ext_conditions(
    x_api_key: str = Header(..., alias="X-API-Key"),
    patient: Optional[int] = None,
    _count: int = Query(20, le=100, alias="_count"),
):
    """외부 파트너용 진단 정보"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        t0 = time.time()
        partner = await _verify_api_key(conn, x_api_key)
        if "Condition" not in (partner.get("allowed_resources") or []):
            raise HTTPException(403, "Condition 리소스 접근 권한 없음")
        q = "SELECT condition_occurrence_id, person_id, condition_source_value, condition_start_date FROM condition_occurrence WHERE 1=1"
        params, idx = [], 1
        if patient:
            q += f" AND person_id=${idx}"; params.append(patient); idx += 1
        q += f" ORDER BY condition_start_date DESC LIMIT ${idx}"; params.append(_count)
        rows = await conn.fetch(q, *params)
        ms = (time.time()-t0)*1000
        await conn.execute("INSERT INTO ext_api_usage (partner_id, endpoint, method, status_code, response_time_ms) VALUES ($1,$2,$3,$4,$5)",
            partner["partner_id"], "/external/v1/conditions", "GET", 200, ms)
        return {"total": len(rows), "data": [dict(r) for r in rows]}
    finally:
        await _rel(conn)

@router.get("/v1/observations")
async def ext_observations(
    x_api_key: str = Header(..., alias="X-API-Key"),
    patient: Optional[int] = None,
    _count: int = Query(20, le=50, alias="_count"),
):
    """외부 파트너용 검사 결과"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        t0 = time.time()
        partner = await _verify_api_key(conn, x_api_key)
        if "Observation" not in (partner.get("allowed_resources") or []):
            raise HTTPException(403, "Observation 리소스 접근 권한 없음")
        q = "SELECT measurement_id, person_id, measurement_source_value, measurement_date, value_as_number, unit_source_value FROM measurement WHERE 1=1"
        params, idx = [], 1
        if patient:
            q += f" AND person_id=${idx}"; params.append(patient); idx += 1
        q += f" ORDER BY measurement_date DESC LIMIT ${idx}"; params.append(_count)
        rows = await conn.fetch(q, *params)
        ms = (time.time()-t0)*1000
        await conn.execute("INSERT INTO ext_api_usage (partner_id, endpoint, method, status_code, response_time_ms) VALUES ($1,$2,$3,$4,$5)",
            partner["partner_id"], "/external/v1/observations", "GET", 200, ms)
        return {"total": len(rows), "data": [dict(r) for r in rows]}
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# 사용량 통계
# ══════════════════════════════════════════

@router.get("/usage")
async def api_usage_stats(partner_id: Optional[int] = None, days: int = Query(7, ge=1, le=90)):
    """API 사용량 통계"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        base = "SELECT * FROM ext_api_usage WHERE created_at > NOW() - ($1 || ' days')::interval"
        params = [str(days)]
        if partner_id:
            base += " AND partner_id = $2"
            params.append(partner_id)
        base += " ORDER BY created_at DESC LIMIT 500"
        rows = await conn.fetch(base, *params)

        # 집계
        by_partner = {}
        by_endpoint = {}
        for r in rows:
            pid = r["partner_id"]
            ep = r["endpoint"]
            by_partner[pid] = by_partner.get(pid, 0) + 1
            by_endpoint[ep] = by_endpoint.get(ep, 0) + 1

        return {
            "period_days": days,
            "total_requests": len(rows),
            "by_partner": by_partner,
            "by_endpoint": by_endpoint,
            "recent": [dict(r) for r in rows[:20]]
        }
    finally:
        await _rel(conn)
