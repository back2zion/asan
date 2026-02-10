"""
SER-002: 인증 API — 로그인, 로그아웃, 토큰 갱신, 내 정보, 비밀번호 변경
"""
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from .auth_shared import (
    get_connection, ensure_auth_tables, ensure_seed_users,
    hash_password, verify_password, validate_password,
    create_jwt_token, decode_jwt_token,
    get_current_user, bearer_scheme,
    LoginRequest, LoginResponse, RefreshRequest, ChangePasswordRequest,
    ACCESS_TOKEN_EXPIRE_MINUTES, REFRESH_TOKEN_EXPIRE_DAYS,
    MAX_LOGIN_ATTEMPTS, ACCOUNT_LOCK_MINUTES, PASSWORD_HISTORY_COUNT,
    PASSWORD_MAX_AGE_DAYS,
)

router = APIRouter()


@router.post("/login", response_model=LoginResponse)
async def login(request: Request, body: LoginRequest):
    """로그인 — JWT 토큰 발급

    - 5회 실패 시 30분간 계정 잠금
    - 비밀번호 만료 경고 (7일 이내)
    """
    await ensure_auth_tables()
    await ensure_seed_users()

    conn = await get_connection()
    try:
        user = await conn.fetchrow(
            "SELECT * FROM auth_user WHERE username = $1", body.username
        )
        client_ip = request.client.host if request.client else "unknown"
        user_agent = (request.headers.get("user-agent") or "")[:500]

        # 사용자 없음
        if not user:
            await conn.execute(
                "INSERT INTO auth_login_attempt (username,ip_address,user_agent,success,failure_reason) VALUES ($1,$2,$3,false,'user_not_found')",
                body.username, client_ip, user_agent,
            )
            raise HTTPException(status_code=401, detail="사용자명 또는 비밀번호가 잘못되었습니다")

        # 비활성화
        if not user["is_active"]:
            raise HTTPException(status_code=403, detail="비활성화된 계정입니다. 관리자에게 문의하세요")

        # 잠금
        if user["is_locked"]:
            if user["locked_until"] and datetime.utcnow() < user["locked_until"]:
                mins = int((user["locked_until"] - datetime.utcnow()).total_seconds() / 60) + 1
                raise HTTPException(status_code=423, detail=f"계정이 잠겼습니다. {mins}분 후 다시 시도하세요")
            # 잠금 시간 경과 → 해제
            await conn.execute(
                "UPDATE auth_user SET is_locked=FALSE, failed_login_count=0, locked_until=NULL WHERE user_id=$1",
                user["user_id"],
            )

        # 비밀번호 확인
        if not verify_password(body.password, user["password_hash"]):
            fail_cnt = user["failed_login_count"] + 1
            if fail_cnt >= MAX_LOGIN_ATTEMPTS:
                locked_until = datetime.utcnow() + timedelta(minutes=ACCOUNT_LOCK_MINUTES)
                await conn.execute(
                    "UPDATE auth_user SET failed_login_count=$1, is_locked=TRUE, locked_until=$2 WHERE user_id=$3",
                    fail_cnt, locked_until, user["user_id"],
                )
                await conn.execute(
                    "INSERT INTO auth_login_attempt (username,ip_address,user_agent,success,failure_reason) VALUES ($1,$2,$3,false,'account_locked')",
                    body.username, client_ip, user_agent,
                )
                raise HTTPException(
                    status_code=423,
                    detail=f"로그인 {MAX_LOGIN_ATTEMPTS}회 실패로 계정이 {ACCOUNT_LOCK_MINUTES}분간 잠겼습니다",
                )
            await conn.execute(
                "UPDATE auth_user SET failed_login_count=$1 WHERE user_id=$2",
                fail_cnt, user["user_id"],
            )
            await conn.execute(
                "INSERT INTO auth_login_attempt (username,ip_address,user_agent,success,failure_reason) VALUES ($1,$2,$3,false,'wrong_password')",
                body.username, client_ip, user_agent,
            )
            remaining = MAX_LOGIN_ATTEMPTS - fail_cnt
            raise HTTPException(
                status_code=401,
                detail=f"사용자명 또는 비밀번호가 잘못되었습니다 (남은 시도: {remaining}회)",
            )

        # 로그인 성공
        await conn.execute(
            "UPDATE auth_user SET failed_login_count=0, is_locked=FALSE, locked_until=NULL, last_login=NOW() WHERE user_id=$1",
            user["user_id"],
        )
        await conn.execute(
            "INSERT INTO auth_login_attempt (username,ip_address,user_agent,success) VALUES ($1,$2,$3,true)",
            body.username, client_ip, user_agent,
        )

        # JWT 발급
        token_data = {
            "sub": user["user_id"],
            "username": user["username"],
            "role": user["role"],
            "display_name": user["display_name"],
        }
        access_token = create_jwt_token(
            token_data, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        refresh_token = create_jwt_token(
            {**token_data, "type": "refresh"},
            expires_delta=timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        )

        # 비밀번호 만료 경고
        pw_warning = None
        if user["password_expires_at"]:
            if datetime.utcnow() > user["password_expires_at"]:
                pw_warning = "비밀번호가 만료되었습니다. 변경해 주세요."
            else:
                days_left = (user["password_expires_at"] - datetime.utcnow()).days
                if days_left <= 7:
                    pw_warning = f"비밀번호가 {days_left}일 후 만료됩니다."

        return LoginResponse(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            user={
                "user_id": user["user_id"],
                "username": user["username"],
                "display_name": user["display_name"],
                "role": user["role"],
                "department": user["department"],
                "password_warning": pw_warning,
            },
        )
    finally:
        await conn.close()


@router.post("/logout")
async def logout(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
):
    """로그아웃 — 토큰 블랙리스트 등록"""
    if not credentials:
        return {"success": True, "message": "이미 로그아웃 상태입니다"}

    try:
        payload = decode_jwt_token(credentials.credentials)
    except ValueError:
        return {"success": True, "message": "토큰이 이미 만료되었습니다"}

    jti = payload.get("jti")
    exp = payload.get("exp")
    if jti and exp:
        conn = await get_connection()
        try:
            await ensure_auth_tables(conn)
            await conn.execute(
                "INSERT INTO auth_token_blacklist (jti, expires_at) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                jti, datetime.utcfromtimestamp(exp),
            )
        finally:
            await conn.close()

    return {"success": True, "message": "로그아웃되었습니다"}


@router.post("/refresh", response_model=LoginResponse)
async def refresh_token(body: RefreshRequest):
    """리프레시 토큰으로 새 액세스 토큰 발급"""
    try:
        payload = decode_jwt_token(body.refresh_token)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=f"리프레시 토큰 오류: {e}")

    if payload.get("type") != "refresh":
        raise HTTPException(status_code=401, detail="유효한 리프레시 토큰이 아닙니다")

    # 사용자 최신 정보 조회
    conn = await get_connection()
    try:
        user = await conn.fetchrow(
            "SELECT * FROM auth_user WHERE user_id = $1 AND is_active = TRUE",
            payload["sub"],
        )
        if not user:
            raise HTTPException(status_code=401, detail="사용자가 존재하지 않거나 비활성화되었습니다")

        token_data = {
            "sub": user["user_id"],
            "username": user["username"],
            "role": user["role"],
            "display_name": user["display_name"],
        }
        access_token = create_jwt_token(
            token_data, expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        )
        new_refresh = create_jwt_token(
            {**token_data, "type": "refresh"},
            expires_delta=timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        )

        return LoginResponse(
            access_token=access_token,
            refresh_token=new_refresh,
            expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
            user={
                "user_id": user["user_id"],
                "username": user["username"],
                "display_name": user["display_name"],
                "role": user["role"],
                "department": user["department"],
            },
        )
    finally:
        await conn.close()


@router.get("/me")
async def get_me(user: dict = Depends(get_current_user)):
    """현재 로그인된 사용자 정보"""
    return {"success": True, "user": user}


@router.post("/change-password")
async def change_password(
    body: ChangePasswordRequest,
    user: dict = Depends(get_current_user),
):
    """비밀번호 변경 — 정책 검증 + 이력 확인"""
    # 정책 검증
    valid, msg = validate_password(body.new_password)
    if not valid:
        raise HTTPException(status_code=400, detail=msg)

    conn = await get_connection()
    try:
        db_user = await conn.fetchrow(
            "SELECT * FROM auth_user WHERE user_id = $1", user["user_id"]
        )
        if not db_user:
            raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다")

        # 현재 비밀번호 확인
        if not verify_password(body.current_password, db_user["password_hash"]):
            raise HTTPException(status_code=401, detail="현재 비밀번호가 잘못되었습니다")

        # 이전 비밀번호와 동일 여부 확인
        history = await conn.fetch(
            "SELECT password_hash FROM auth_password_history WHERE user_id=$1 ORDER BY created_at DESC LIMIT $2",
            user["user_id"], PASSWORD_HISTORY_COUNT,
        )
        for h in history:
            if verify_password(body.new_password, h["password_hash"]):
                raise HTTPException(
                    status_code=400,
                    detail=f"최근 {PASSWORD_HISTORY_COUNT}회 이내 사용한 비밀번호는 재사용할 수 없습니다",
                )

        # 변경
        new_hash = hash_password(body.new_password)
        expires_at = datetime.utcnow() + timedelta(days=PASSWORD_MAX_AGE_DAYS)
        await conn.execute(
            "UPDATE auth_user SET password_hash=$1, password_changed_at=NOW(), password_expires_at=$2, updated_at=NOW() WHERE user_id=$3",
            new_hash, expires_at, user["user_id"],
        )
        await conn.execute(
            "INSERT INTO auth_password_history (user_id, password_hash) VALUES ($1, $2)",
            user["user_id"], new_hash,
        )

        return {"success": True, "message": "비밀번호가 변경되었습니다"}
    finally:
        await conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# KeyCloak SSO (OIDC)
# ══════════════════════════════════════════════════════════════════════════════


@router.get("/sso/login")
async def sso_login(redirect_uri: str = "http://localhost:5173/auth/callback"):
    """KeyCloak SSO 로그인 — OIDC Authorization URL 반환"""
    from core.config import settings

    auth_url = (
        f"{settings.KEYCLOAK_URL}/realms/{settings.KEYCLOAK_REALM}"
        f"/protocol/openid-connect/auth"
        f"?client_id={settings.KEYCLOAK_CLIENT_ID}"
        f"&response_type=code"
        f"&scope=openid profile email"
        f"&redirect_uri={redirect_uri}"
    )
    return {"auth_url": auth_url}


@router.post("/sso/callback")
async def sso_callback(
    code: str,
    redirect_uri: str = "http://localhost:5173/auth/callback",
):
    """KeyCloak SSO 콜백 — authorization code -> JWT 토큰 교환"""
    import httpx
    from core.config import settings

    token_url = (
        f"{settings.KEYCLOAK_URL}/realms/{settings.KEYCLOAK_REALM}"
        f"/protocol/openid-connect/token"
    )

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(token_url, data={
            "grant_type": "authorization_code",
            "client_id": settings.KEYCLOAK_CLIENT_ID,
            "client_secret": settings.KEYCLOAK_CLIENT_SECRET,
            "code": code,
            "redirect_uri": redirect_uri,
        })

    if resp.status_code != 200:
        raise HTTPException(
            status_code=401,
            detail=f"KeyCloak token exchange failed: {resp.text[:200]}",
        )

    kc_tokens = resp.json()

    # KeyCloak userinfo
    userinfo_url = (
        f"{settings.KEYCLOAK_URL}/realms/{settings.KEYCLOAK_REALM}"
        f"/protocol/openid-connect/userinfo"
    )
    async with httpx.AsyncClient(timeout=10) as client:
        ui_resp = await client.get(
            userinfo_url,
            headers={"Authorization": f"Bearer {kc_tokens['access_token']}"},
        )

    if ui_resp.status_code != 200:
        raise HTTPException(status_code=401, detail="KeyCloak userinfo failed")

    userinfo = ui_resp.json()

    # Issue our own JWT (so existing auth middleware works seamlessly)
    username = userinfo.get("preferred_username", userinfo.get("sub", "sso_user"))

    # Ensure user exists in our DB
    await ensure_auth_tables()
    conn = await get_connection()
    try:
        user = await conn.fetchrow(
            "SELECT * FROM auth_user WHERE username = $1", username
        )
        if not user:
            # Auto-create SSO user
            await conn.execute(
                "INSERT INTO auth_user "
                "(username, password_hash, display_name, email, role, auth_provider) "
                "VALUES ($1, $2, $3, $4, 'viewer', 'keycloak')",
                username,
                "SSO_NO_PASSWORD",
                userinfo.get("name", username),
                userinfo.get("email", ""),
            )
            user = await conn.fetchrow(
                "SELECT * FROM auth_user WHERE username = $1", username
            )
    finally:
        await conn.close()

    token_data = {
        "sub": user["user_id"],
        "username": user["username"],
        "role": user["role"],
        "display_name": user["display_name"],
        "provider": "keycloak",
    }
    access_token = create_jwt_token(
        token_data,
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    )
    refresh_token = create_jwt_token(
        {**token_data, "type": "refresh"},
        expires_delta=timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
    )

    return LoginResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
        user={
            "user_id": user["user_id"],
            "username": user["username"],
            "display_name": user["display_name"],
            "role": user["role"],
            "department": user.get("department"),
        },
    )


@router.get("/sso/status")
async def sso_status():
    """KeyCloak SSO 연결 상태 확인"""
    import httpx
    from core.config import settings

    try:
        well_known_url = (
            f"{settings.KEYCLOAK_URL}/realms/{settings.KEYCLOAK_REALM}"
            f"/.well-known/openid-configuration"
        )
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(well_known_url)
        if resp.status_code == 200:
            config = resp.json()
            return {
                "status": "connected",
                "issuer": config.get("issuer"),
                "realm": settings.KEYCLOAK_REALM,
            }
        # Realm may not exist — check server connectivity
        master_url = f"{settings.KEYCLOAK_URL}/realms/master/.well-known/openid-configuration"
        async with httpx.AsyncClient(timeout=5) as client:
            master_resp = await client.get(master_url)
        if master_resp.status_code == 200:
            return {
                "status": "realm_not_found",
                "detail": f"KeyCloak 서버 연결됨, '{settings.KEYCLOAK_REALM}' realm 미생성",
                "keycloak_url": settings.KEYCLOAK_URL,
            }
        return {"status": "error", "http_status": resp.status_code}
    except Exception as e:
        return {"status": "unreachable", "error": str(e)}
