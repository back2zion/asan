"""
SER-002: 관리자 API — 계정 CRUD, IP 화이트리스트, 로그인 이력
관리자(admin) 역할만 접근 가능.
"""
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query, Depends

from .auth_shared import (
    get_connection, ensure_auth_tables, ensure_seed_users,
    hash_password, validate_password, require_admin,
    UserCreateRequest, UserUpdateRequest, IpWhitelistCreate,
    PASSWORD_MAX_AGE_DAYS, PASSWORD_HISTORY_COUNT,
)

router = APIRouter()


# ══════════════════════════════════════════════════════════════════════════════
# 계정 관리
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/users")
async def list_users(
    role: str = Query(None),
    is_active: bool = Query(None),
    limit: int = Query(100),
    admin: dict = Depends(require_admin),
):
    """계정 목록 조회"""
    await ensure_auth_tables()
    await ensure_seed_users()
    conn = await get_connection()
    try:
        sql = "SELECT user_id, username, display_name, email, role, department, is_active, is_locked, last_login, created_at FROM auth_user WHERE 1=1"
        params = []
        idx = 1
        if role:
            sql += f" AND role = ${idx}"
            params.append(role)
            idx += 1
        if is_active is not None:
            sql += f" AND is_active = ${idx}"
            params.append(is_active)
            idx += 1
        sql += f" ORDER BY user_id LIMIT ${idx}"
        params.append(limit)

        rows = await conn.fetch(sql, *params)
        return {
            "success": True,
            "total": len(rows),
            "users": [dict(r) for r in rows],
        }
    finally:
        await conn.close()


@router.post("/users")
async def create_user(body: UserCreateRequest, admin: dict = Depends(require_admin)):
    """계정 생성"""
    # 식별자 제한: admin, root, administrator 등 금지
    forbidden_names = {"root", "admin", "administrator", "superuser", "sa"}
    if body.username.lower() in forbidden_names:
        raise HTTPException(status_code=400, detail="추측 가능한 식별자는 사용할 수 없습니다 (root, admin 등)")

    valid, msg = validate_password(body.password)
    if not valid:
        raise HTTPException(status_code=400, detail=msg)

    conn = await get_connection()
    try:
        await ensure_auth_tables(conn)
        exists = await conn.fetchval(
            "SELECT 1 FROM auth_user WHERE username = $1", body.username
        )
        if exists:
            raise HTTPException(status_code=409, detail="이미 존재하는 사용자명입니다")

        pw_hash = hash_password(body.password)
        uid = await conn.fetchval("""
            INSERT INTO auth_user (username, password_hash, display_name, email, role, department)
            VALUES ($1, $2, $3, $4, $5, $6) RETURNING user_id
        """, body.username, pw_hash, body.display_name, body.email, body.role, body.department)

        await conn.execute(
            "INSERT INTO auth_password_history (user_id, password_hash) VALUES ($1, $2)",
            uid, pw_hash,
        )

        return {"success": True, "user_id": uid, "message": f"계정 '{body.username}' 생성 완료"}
    finally:
        await conn.close()


@router.put("/users/{user_id}")
async def update_user(user_id: int, body: UserUpdateRequest, admin: dict = Depends(require_admin)):
    """계정 수정 (역할/부서/활성화 등)"""
    conn = await get_connection()
    try:
        user = await conn.fetchrow("SELECT * FROM auth_user WHERE user_id = $1", user_id)
        if not user:
            raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")

        updates = {}
        if body.display_name is not None:
            updates["display_name"] = body.display_name
        if body.email is not None:
            updates["email"] = body.email
        if body.role is not None:
            updates["role"] = body.role
        if body.department is not None:
            updates["department"] = body.department
        if body.is_active is not None:
            updates["is_active"] = body.is_active

        if not updates:
            return {"success": True, "message": "변경사항 없음"}

        set_clauses = ", ".join(f"{k} = ${i+1}" for i, k in enumerate(updates.keys()))
        params = list(updates.values())
        params.append(user_id)

        await conn.execute(
            f"UPDATE auth_user SET {set_clauses}, updated_at=NOW() WHERE user_id = ${len(params)}",
            *params,
        )
        return {"success": True, "message": f"계정 '{user['username']}' 수정 완료"}
    finally:
        await conn.close()


@router.delete("/users/{user_id}")
async def deactivate_user(user_id: int, admin: dict = Depends(require_admin)):
    """계정 비활성화 (소프트 삭제)"""
    conn = await get_connection()
    try:
        user = await conn.fetchrow("SELECT username FROM auth_user WHERE user_id = $1", user_id)
        if not user:
            raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")

        await conn.execute(
            "UPDATE auth_user SET is_active=FALSE, updated_at=NOW() WHERE user_id=$1", user_id
        )
        return {"success": True, "message": f"계정 '{user['username']}' 비활성화 완료"}
    finally:
        await conn.close()


@router.post("/users/{user_id}/unlock")
async def unlock_user(user_id: int, admin: dict = Depends(require_admin)):
    """잠금 해제"""
    conn = await get_connection()
    try:
        await conn.execute(
            "UPDATE auth_user SET is_locked=FALSE, failed_login_count=0, locked_until=NULL, updated_at=NOW() WHERE user_id=$1",
            user_id,
        )
        return {"success": True, "message": "계정 잠금이 해제되었습니다"}
    finally:
        await conn.close()


@router.post("/users/{user_id}/reset-password")
async def reset_password(user_id: int, body: dict, admin: dict = Depends(require_admin)):
    """관리자 비밀번호 초기화"""
    new_password = body.get("new_password", "")
    valid, msg = validate_password(new_password)
    if not valid:
        raise HTTPException(status_code=400, detail=msg)

    conn = await get_connection()
    try:
        user = await conn.fetchrow("SELECT * FROM auth_user WHERE user_id = $1", user_id)
        if not user:
            raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")

        pw_hash = hash_password(new_password)
        expires_at = datetime.utcnow() + timedelta(days=PASSWORD_MAX_AGE_DAYS)
        await conn.execute(
            "UPDATE auth_user SET password_hash=$1, password_changed_at=NOW(), password_expires_at=$2, failed_login_count=0, is_locked=FALSE, locked_until=NULL, updated_at=NOW() WHERE user_id=$3",
            pw_hash, expires_at, user_id,
        )
        await conn.execute(
            "INSERT INTO auth_password_history (user_id, password_hash) VALUES ($1, $2)",
            user_id, pw_hash,
        )
        return {"success": True, "message": f"계정 '{user['username']}' 비밀번호 초기화 완료"}
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# 로그인 시도 이력
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/login-attempts")
async def list_login_attempts(
    username: str = Query(None),
    success: bool = Query(None),
    limit: int = Query(100),
    admin: dict = Depends(require_admin),
):
    """로그인 시도 이력 조회"""
    conn = await get_connection()
    try:
        await ensure_auth_tables(conn)
        sql = "SELECT * FROM auth_login_attempt WHERE 1=1"
        params = []
        idx = 1
        if username:
            sql += f" AND username = ${idx}"
            params.append(username)
            idx += 1
        if success is not None:
            sql += f" AND success = ${idx}"
            params.append(success)
            idx += 1
        sql += f" ORDER BY created_at DESC LIMIT ${idx}"
        params.append(limit)

        rows = await conn.fetch(sql, *params)
        return {
            "success": True,
            "total": len(rows),
            "attempts": [dict(r) for r in rows],
        }
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# IP 화이트리스트
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/ip-whitelist")
async def list_ip_whitelist(admin: dict = Depends(require_admin)):
    """IP 화이트리스트 조회"""
    conn = await get_connection()
    try:
        await ensure_auth_tables(conn)
        rows = await conn.fetch("SELECT * FROM auth_ip_whitelist ORDER BY whitelist_id")
        return {"success": True, "total": len(rows), "whitelist": [dict(r) for r in rows]}
    finally:
        await conn.close()


@router.post("/ip-whitelist")
async def add_ip_whitelist(body: IpWhitelistCreate, admin: dict = Depends(require_admin)):
    """IP 화이트리스트 추가"""
    conn = await get_connection()
    try:
        await ensure_auth_tables(conn)
        wid = await conn.fetchval(
            "INSERT INTO auth_ip_whitelist (ip_pattern, description, scope) VALUES ($1,$2,$3) RETURNING whitelist_id",
            body.ip_pattern, body.description, body.scope,
        )
        return {"success": True, "whitelist_id": wid}
    finally:
        await conn.close()


@router.delete("/ip-whitelist/{whitelist_id}")
async def delete_ip_whitelist(whitelist_id: int, admin: dict = Depends(require_admin)):
    """IP 화이트리스트 삭제"""
    conn = await get_connection()
    try:
        await conn.execute("DELETE FROM auth_ip_whitelist WHERE whitelist_id = $1", whitelist_id)
        return {"success": True, "message": "삭제 완료"}
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# 보안 통계
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/security-stats")
async def security_stats(admin: dict = Depends(require_admin)):
    """보안 통계 대시보드"""
    conn = await get_connection()
    try:
        await ensure_auth_tables(conn)
        total_users = await conn.fetchval("SELECT COUNT(*) FROM auth_user")
        active_users = await conn.fetchval("SELECT COUNT(*) FROM auth_user WHERE is_active=TRUE")
        locked_users = await conn.fetchval("SELECT COUNT(*) FROM auth_user WHERE is_locked=TRUE")
        expired_pw = await conn.fetchval(
            "SELECT COUNT(*) FROM auth_user WHERE password_expires_at < NOW() AND is_active=TRUE"
        )
        today_logins = await conn.fetchval(
            "SELECT COUNT(*) FROM auth_login_attempt WHERE success=TRUE AND created_at > NOW() - INTERVAL '24 hours'"
        )
        today_failures = await conn.fetchval(
            "SELECT COUNT(*) FROM auth_login_attempt WHERE success=FALSE AND created_at > NOW() - INTERVAL '24 hours'"
        )
        blacklisted = await conn.fetchval("SELECT COUNT(*) FROM auth_token_blacklist")
        ip_rules = await conn.fetchval("SELECT COUNT(*) FROM auth_ip_whitelist WHERE enabled=TRUE")

        role_dist = await conn.fetch(
            "SELECT role, COUNT(*) AS cnt FROM auth_user WHERE is_active=TRUE GROUP BY role ORDER BY cnt DESC"
        )

        return {
            "success": True,
            "stats": {
                "total_users": total_users,
                "active_users": active_users,
                "locked_users": locked_users,
                "expired_passwords": expired_pw,
                "today_successful_logins": today_logins,
                "today_failed_logins": today_failures,
                "blacklisted_tokens": blacklisted,
                "ip_whitelist_rules": ip_rules,
                "role_distribution": [dict(r) for r in role_dist],
            },
        }
    finally:
        await conn.close()
