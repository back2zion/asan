"""
SER-010: 개인정보 수집·이용 동의 관리 API
RFP 요구: 동의 절차 구현, 동의 내용 변경, 처리 기록 저장/모니터링, 보유기간 관리
"""
from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/consent", tags=["ConsentMgmt"])


# ── DB 연결 ──
async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)


# ── Pydantic 모델 ──
class ConsentPolicyCreate(BaseModel):
    policy_code: str = Field(..., max_length=50, description="정책 코드 (예: RESEARCH_DATA)")
    title: str = Field(..., max_length=200)
    description: str = Field(..., max_length=5000)
    purpose: str = Field(..., max_length=500, description="수집·이용 목적")
    data_items: str = Field(..., max_length=2000, description="수집 항목")
    retention_days: int = Field(365, ge=1, le=36500, description="보유 기간(일)")
    is_required: bool = Field(True, description="필수 동의 여부")
    version: str = Field("1.0", max_length=20)

class ConsentPolicyUpdate(BaseModel):
    title: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=5000)
    purpose: Optional[str] = Field(None, max_length=500)
    data_items: Optional[str] = Field(None, max_length=2000)
    retention_days: Optional[int] = Field(None, ge=1, le=36500)

class ConsentRecordCreate(BaseModel):
    user_id: str = Field(..., max_length=50)
    policy_id: int
    agreed: bool
    ip_address: Optional[str] = Field(None, max_length=45)

class ConsentWithdraw(BaseModel):
    user_id: str = Field(..., max_length=50)
    policy_id: int
    reason: Optional[str] = Field(None, max_length=500)


# ── 테이블 초기화 ──
_tbl_ok = False

async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS consent_policy (
            policy_id SERIAL PRIMARY KEY,
            policy_code VARCHAR(50) NOT NULL UNIQUE,
            title VARCHAR(200) NOT NULL,
            description TEXT NOT NULL,
            purpose VARCHAR(500) NOT NULL,
            data_items TEXT NOT NULL,
            retention_days INTEGER DEFAULT 365,
            is_required BOOLEAN DEFAULT TRUE,
            version VARCHAR(20) DEFAULT '1.0',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS consent_record (
            record_id SERIAL PRIMARY KEY,
            user_id VARCHAR(50) NOT NULL,
            policy_id INTEGER NOT NULL REFERENCES consent_policy(policy_id),
            agreed BOOLEAN NOT NULL,
            ip_address VARCHAR(45),
            agreed_at TIMESTAMPTZ DEFAULT NOW(),
            expires_at TIMESTAMPTZ,
            withdrawn_at TIMESTAMPTZ,
            withdraw_reason VARCHAR(500),
            UNIQUE(user_id, policy_id)
        );

        CREATE TABLE IF NOT EXISTS consent_audit_log (
            log_id SERIAL PRIMARY KEY,
            user_id VARCHAR(50) NOT NULL,
            policy_id INTEGER,
            action VARCHAR(30) NOT NULL,
            detail TEXT,
            ip_address VARCHAR(45),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_consent_record_user ON consent_record(user_id);
        CREATE INDEX IF NOT EXISTS idx_consent_audit_user ON consent_audit_log(user_id);
    """)
    # 시드 데이터
    cnt = await conn.fetchval("SELECT COUNT(*) FROM consent_policy")
    if cnt == 0:
        seeds = [
            ("RESEARCH_DATA", "연구용 데이터 활용 동의",
             "통합 데이터 플랫폼에서 연구 목적으로 수집되는 의료 데이터의 활용에 대한 동의입니다.",
             "의료 연구 및 데이터 분석을 위한 가명처리된 데이터 활용",
             "진단정보, 처방정보, 검사결과, 영상정보(가명처리)", 1095, True, "1.0"),
            ("PLATFORM_USAGE", "플랫폼 이용 동의",
             "통합 데이터 플랫폼 서비스 이용을 위한 개인정보 수집·이용 동의입니다.",
             "플랫폼 로그인, 접속 이력 관리, 서비스 제공",
             "사번, 성명, 부서, 직급, 접속IP, 접속일시", 365, True, "1.0"),
            ("DATA_EXPORT", "데이터 반출 동의",
             "데이터 반출 시 개인정보 보호 의무에 대한 동의입니다.",
             "연구 목적 데이터 반출 및 외부 분석 환경 이용",
             "반출 데이터 항목, 반출 사유, IRB 승인 정보", 730, True, "1.0"),
            ("AI_ANALYSIS", "AI 분석 동의",
             "AI 기반 분석 서비스 이용 시 데이터 처리에 대한 동의입니다.",
             "AI 모델 학습 및 추론을 위한 가명처리된 데이터 활용",
             "진료기록 텍스트(가명처리), 검사결과, AI 분석 결과", 365, False, "1.0"),
        ]
        for code, title, desc, purpose, items, ret, req, ver in seeds:
            await conn.execute("""
                INSERT INTO consent_policy (policy_code, title, description, purpose, data_items, retention_days, is_required, version)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING
            """, code, title, desc, purpose, items, ret, req, ver)
    _tbl_ok = True


async def _log_audit(conn, user_id: str, policy_id, action: str, detail: str = None, ip: str = None):
    await conn.execute("""
        INSERT INTO consent_audit_log (user_id, policy_id, action, detail, ip_address)
        VALUES ($1,$2,$3,$4,$5)
    """, user_id, policy_id, action, detail, ip)


# ══════════════════════════════════════════
# 1. 동의 정책 관리
# ══════════════════════════════════════════

@router.get("/policies")
async def list_policies(active_only: bool = True):
    """동의 정책 목록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT * FROM consent_policy"
        if active_only:
            q += " WHERE is_active = TRUE"
        q += " ORDER BY policy_id"
        rows = await conn.fetch(q)
        return [dict(r) for r in rows]
    finally:
        await _rel(conn)


@router.get("/policies/{policy_id}")
async def get_policy(policy_id: int):
    """동의 정책 상세"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow("SELECT * FROM consent_policy WHERE policy_id=$1", policy_id)
        if not row:
            raise HTTPException(404, "정책을 찾을 수 없습니다")
        result = dict(row)
        result["agreed_count"] = await conn.fetchval(
            "SELECT COUNT(*) FROM consent_record WHERE policy_id=$1 AND agreed=TRUE AND withdrawn_at IS NULL",
            policy_id)
        return result
    finally:
        await _rel(conn)


@router.post("/policies")
async def create_policy(body: ConsentPolicyCreate):
    """동의 정책 생성"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow("""
            INSERT INTO consent_policy (policy_code, title, description, purpose, data_items, retention_days, is_required, version)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *
        """, body.policy_code, body.title, body.description, body.purpose,
            body.data_items, body.retention_days, body.is_required, body.version)
        return dict(row)
    finally:
        await _rel(conn)


@router.put("/policies/{policy_id}")
async def update_policy(policy_id: int, body: ConsentPolicyUpdate):
    """동의 정책 수정 (내용 변경 시 버전 자동 증가)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        existing = await conn.fetchrow("SELECT * FROM consent_policy WHERE policy_id=$1", policy_id)
        if not existing:
            raise HTTPException(404, "정책을 찾을 수 없습니다")

        updates = {}
        if body.title is not None:
            updates["title"] = body.title
        if body.description is not None:
            updates["description"] = body.description
        if body.purpose is not None:
            updates["purpose"] = body.purpose
        if body.data_items is not None:
            updates["data_items"] = body.data_items
        if body.retention_days is not None:
            updates["retention_days"] = body.retention_days

        if not updates:
            return dict(existing)

        # 버전 자동 증가
        ver = existing["version"]
        try:
            major, minor = ver.split(".")
            new_ver = f"{major}.{int(minor) + 1}"
        except Exception:
            new_ver = ver + ".1"

        set_clause = ", ".join(f"{k}=${i+2}" for i, k in enumerate(updates.keys()))
        vals = list(updates.values())
        row = await conn.fetchrow(
            f"UPDATE consent_policy SET {set_clause}, version=${len(vals)+2}, updated_at=NOW() WHERE policy_id=$1 RETURNING *",
            policy_id, *vals, new_ver)
        return dict(row)
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# 2. 동의 기록 (사용자 동의/철회)
# ══════════════════════════════════════════

@router.get("/user/{user_id}")
async def get_user_consents(user_id: str):
    """사용자의 전체 동의 현황"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("""
            SELECT cr.*, cp.policy_code, cp.title, cp.purpose, cp.is_required, cp.version as policy_version
            FROM consent_record cr
            JOIN consent_policy cp ON cr.policy_id = cp.policy_id
            WHERE cr.user_id = $1
            ORDER BY cr.agreed_at DESC
        """, user_id)
        policies = await conn.fetch("SELECT * FROM consent_policy WHERE is_active=TRUE ORDER BY policy_id")
        agreed_ids = {r["policy_id"] for r in rows if r["agreed"] and not r["withdrawn_at"]}
        return {
            "user_id": user_id,
            "consents": [dict(r) for r in rows],
            "policies": [{**dict(p), "user_agreed": p["policy_id"] in agreed_ids} for p in policies],
            "all_required_agreed": all(
                p["policy_id"] in agreed_ids
                for p in policies if p["is_required"]
            ),
        }
    finally:
        await _rel(conn)


class ConsentBatchAgree(BaseModel):
    user_id: str = Field(..., max_length=50)
    policy_ids: List[int]


@router.post("/agree-batch")
async def agree_batch(body: ConsentBatchAgree):
    """일괄 동의 등록 (한 번의 요청으로 여러 정책 동의)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        results = []
        for pid in body.policy_ids:
            policy = await conn.fetchrow("SELECT * FROM consent_policy WHERE policy_id=$1 AND is_active=TRUE", pid)
            if not policy:
                continue
            expires = datetime.utcnow() + timedelta(days=policy["retention_days"])
            row = await conn.fetchrow("""
                INSERT INTO consent_record (user_id, policy_id, agreed, ip_address, expires_at)
                VALUES ($1,$2,TRUE,NULL,$3)
                ON CONFLICT (user_id, policy_id) DO UPDATE SET
                    agreed=TRUE, agreed_at=NOW(), expires_at=$3, withdrawn_at=NULL, withdraw_reason=NULL
                RETURNING *
            """, body.user_id, pid, expires)
            await _log_audit(conn, body.user_id, pid, "AGREE", f"정책: {policy['title']}")
            results.append(dict(row))
        return {"agreed_count": len(results), "records": results}
    finally:
        await _rel(conn)


@router.post("/agree")
async def agree_consent(body: ConsentRecordCreate):
    """동의 등록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        policy = await conn.fetchrow("SELECT * FROM consent_policy WHERE policy_id=$1 AND is_active=TRUE", body.policy_id)
        if not policy:
            raise HTTPException(404, "유효한 동의 정책이 아닙니다")

        expires = datetime.utcnow() + timedelta(days=policy["retention_days"])
        row = await conn.fetchrow("""
            INSERT INTO consent_record (user_id, policy_id, agreed, ip_address, expires_at)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (user_id, policy_id) DO UPDATE SET
                agreed=$3, ip_address=$4, agreed_at=NOW(), expires_at=$5, withdrawn_at=NULL, withdraw_reason=NULL
            RETURNING *
        """, body.user_id, body.policy_id, body.agreed, body.ip_address, expires)
        await _log_audit(conn, body.user_id, body.policy_id, "AGREE" if body.agreed else "DISAGREE",
                         f"정책: {policy['title']}", body.ip_address)
        return dict(row)
    finally:
        await _rel(conn)


@router.post("/withdraw")
async def withdraw_consent(body: ConsentWithdraw):
    """동의 철회"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        existing = await conn.fetchrow(
            "SELECT * FROM consent_record WHERE user_id=$1 AND policy_id=$2 AND agreed=TRUE AND withdrawn_at IS NULL",
            body.user_id, body.policy_id)
        if not existing:
            raise HTTPException(404, "유효한 동의 기록이 없습니다")

        policy = await conn.fetchrow("SELECT * FROM consent_policy WHERE policy_id=$1", body.policy_id)
        if policy and policy["is_required"]:
            raise HTTPException(400, "필수 동의는 철회할 수 없습니다. 서비스 탈퇴를 통해 처리하세요.")

        row = await conn.fetchrow("""
            UPDATE consent_record SET withdrawn_at=NOW(), withdraw_reason=$3
            WHERE user_id=$1 AND policy_id=$2 RETURNING *
        """, body.user_id, body.policy_id, body.reason)
        await _log_audit(conn, body.user_id, body.policy_id, "WITHDRAW",
                         f"사유: {body.reason or '미기재'}")
        return dict(row)
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# 3. 감사/모니터링
# ══════════════════════════════════════════

@router.get("/audit-log")
async def get_audit_log(
    user_id: Optional[str] = None,
    action: Optional[str] = None,
    limit: int = Query(50, le=500),
):
    """동의 처리 기록 조회"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT * FROM consent_audit_log WHERE 1=1"
        args = []
        if user_id:
            args.append(user_id)
            q += f" AND user_id=${len(args)}"
        if action:
            args.append(action)
            q += f" AND action=${len(args)}"
        q += f" ORDER BY created_at DESC LIMIT {limit}"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await _rel(conn)


@router.get("/stats")
async def consent_stats():
    """동의 현황 통계"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        policies = await conn.fetch("SELECT * FROM consent_policy WHERE is_active=TRUE ORDER BY policy_id")
        result = []
        for p in policies:
            agreed = await conn.fetchval(
                "SELECT COUNT(*) FROM consent_record WHERE policy_id=$1 AND agreed=TRUE AND withdrawn_at IS NULL",
                p["policy_id"])
            withdrawn = await conn.fetchval(
                "SELECT COUNT(*) FROM consent_record WHERE policy_id=$1 AND withdrawn_at IS NOT NULL",
                p["policy_id"])
            expired = await conn.fetchval(
                "SELECT COUNT(*) FROM consent_record WHERE policy_id=$1 AND expires_at < NOW() AND withdrawn_at IS NULL",
                p["policy_id"])
            result.append({
                "policy_id": p["policy_id"],
                "policy_code": p["policy_code"],
                "title": p["title"],
                "is_required": p["is_required"],
                "agreed_count": agreed,
                "withdrawn_count": withdrawn,
                "expired_count": expired,
            })
        total_users = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM consent_record")
        return {
            "total_users": total_users,
            "policies": result,
            "audit_total": await conn.fetchval("SELECT COUNT(*) FROM consent_audit_log"),
        }
    finally:
        await _rel(conn)


@router.get("/expiring")
async def get_expiring_consents(days: int = Query(30, ge=1, le=365)):
    """만료 임박 동의 목록 (보유기간 도래)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("""
            SELECT cr.*, cp.title, cp.policy_code
            FROM consent_record cr
            JOIN consent_policy cp ON cr.policy_id = cp.policy_id
            WHERE cr.agreed = TRUE
              AND cr.withdrawn_at IS NULL
              AND cr.expires_at IS NOT NULL
              AND cr.expires_at < NOW() + INTERVAL '1 day' * $1
            ORDER BY cr.expires_at ASC
        """, days)
        return [dict(r) for r in rows]
    finally:
        await _rel(conn)
