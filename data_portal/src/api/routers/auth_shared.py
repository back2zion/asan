"""
SER-002: 접근통제 — 인증/인가 공유 모듈
DB 스키마, Pydantic 모델, JWT/패스워드 유틸, FastAPI 의존성, 시드 데이터.
"""
import os
import re
import uuid
import json
import hmac
import hashlib
import base64
from datetime import datetime, timedelta
from typing import Optional, List
from ipaddress import ip_address, ip_network

from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
import asyncpg

# ── Config ──

JWT_SECRET = os.getenv("JWT_SECRET", "asan-idp-secret-key-change-in-production-2026")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MIN", "30"))
REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("REFRESH_TOKEN_EXPIRE_DAYS", "7"))

PASSWORD_MIN_LENGTH = 8
PASSWORD_MAX_AGE_DAYS = 90
PASSWORD_HISTORY_COUNT = 5
MAX_LOGIN_ATTEMPTS = 5
ACCOUNT_LOCK_MINUTES = 30

AUTH_REQUIRED = os.getenv("AUTH_REQUIRED", "false").lower() == "true"

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}


async def get_connection():
    try:
        return await asyncpg.connect(**OMOP_DB_CONFIG)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Password Hashing (PBKDF2-SHA256 — stdlib, 외부 의존성 없음)
# ══════════════════════════════════════════════════════════════════════════════

def hash_password(password: str) -> str:
    salt = os.urandom(32)
    key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
    return salt.hex() + ":" + key.hex()


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        salt_hex, key_hex = stored_hash.split(":")
        salt = bytes.fromhex(salt_hex)
        key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 100_000)
        return hmac.compare_digest(key.hex(), key_hex)
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# Password Policy
# ══════════════════════════════════════════════════════════════════════════════

def validate_password(password: str) -> tuple:
    """비밀번호 정책: 8자+, 대문자, 소문자, 숫자, 특수문자"""
    if len(password) < PASSWORD_MIN_LENGTH:
        return False, f"비밀번호는 최소 {PASSWORD_MIN_LENGTH}자 이상이어야 합니다"
    if not re.search(r"[A-Z]", password):
        return False, "비밀번호에 대문자가 포함되어야 합니다"
    if not re.search(r"[a-z]", password):
        return False, "비밀번호에 소문자가 포함되어야 합니다"
    if not re.search(r"\d", password):
        return False, "비밀번호에 숫자가 포함되어야 합니다"
    if not re.search(r"[!@#$%^&*(),.?\":{}|<>\-_=+\[\]~`]", password):
        return False, "비밀번호에 특수문자가 포함되어야 합니다"
    forbidden = ["password", "12345678", "qwerty", "admin", "root", "administrator"]
    if password.lower() in forbidden:
        return False, "추측 가능한 비밀번호는 사용할 수 없습니다"
    return True, ""


# ══════════════════════════════════════════════════════════════════════════════
# JWT Token (stdlib hmac — PyJWT 불필요)
# ══════════════════════════════════════════════════════════════════════════════

def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


def create_jwt_token(payload: dict, expires_delta: timedelta = None) -> str:
    header = {"alg": JWT_ALGORITHM, "typ": "JWT"}
    now = datetime.utcnow()
    payload = {
        **payload,
        "iat": int(now.timestamp()),
        "jti": payload.get("jti", str(uuid.uuid4())),
    }
    if expires_delta:
        payload["exp"] = int((now + expires_delta).timestamp())

    h = _b64url_encode(json.dumps(header, separators=(",", ":")).encode())
    p = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode())
    sig = hmac.new(JWT_SECRET.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest()
    return f"{h}.{p}.{_b64url_encode(sig)}"


def decode_jwt_token(token: str) -> dict:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("잘못된 토큰 형식")
        h, p, s = parts
        expected = hmac.new(JWT_SECRET.encode(), f"{h}.{p}".encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected, _b64url_decode(s)):
            raise ValueError("토큰 서명이 유효하지 않습니다")
        payload = json.loads(_b64url_decode(p))
        if "exp" in payload and datetime.utcnow().timestamp() > payload["exp"]:
            raise ValueError("토큰이 만료되었습니다")
        return payload
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"토큰 디코딩 오류: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# IP Whitelist Check
# ══════════════════════════════════════════════════════════════════════════════

def check_ip_allowed(client_ip: str, whitelist: list) -> bool:
    if not whitelist:
        return True
    try:
        addr = ip_address(client_ip)
        for pattern in whitelist:
            try:
                if "/" in pattern:
                    if addr in ip_network(pattern, strict=False):
                        return True
                elif addr == ip_address(pattern):
                    return True
            except ValueError:
                continue
        return False
    except ValueError:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# FastAPI Dependencies
# ══════════════════════════════════════════════════════════════════════════════

bearer_scheme = HTTPBearer(auto_error=False)


async def get_current_user(
    request: Request,
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> dict:
    """현재 인증된 사용자. AUTH_REQUIRED=false 이면 데모 사용자 반환."""
    if not AUTH_REQUIRED:
        return {
            "user_id": 0,
            "username": "demo",
            "role": "admin",
            "display_name": "데모 사용자",
        }

    if not credentials:
        raise HTTPException(status_code=401, detail="인증 토큰이 필요합니다")

    try:
        payload = decode_jwt_token(credentials.credentials)
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))

    # 블랙리스트 확인
    jti = payload.get("jti")
    if jti:
        conn = await get_connection()
        try:
            blacklisted = await conn.fetchval(
                "SELECT 1 FROM auth_token_blacklist WHERE jti = $1", jti
            )
            if blacklisted:
                raise HTTPException(status_code=401, detail="토큰이 무효화되었습니다")
        finally:
            await conn.close()

    return {
        "user_id": payload.get("sub"),
        "username": payload.get("username"),
        "role": payload.get("role"),
        "display_name": payload.get("display_name"),
    }


async def require_admin(user: dict = Depends(get_current_user)) -> dict:
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다")
    return user


# ══════════════════════════════════════════════════════════════════════════════
# Pydantic Models
# ══════════════════════════════════════════════════════════════════════════════

class LoginRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=50)
    password: str = Field(..., min_length=1)


class LoginResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int
    user: dict


class RefreshRequest(BaseModel):
    refresh_token: str


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(..., min_length=8)


class UserCreateRequest(BaseModel):
    username: str = Field(..., min_length=2, max_length=50, pattern=r"^[a-zA-Z0-9._-]+$")
    password: str = Field(..., min_length=8)
    display_name: str = Field(..., max_length=100)
    email: Optional[str] = Field(None, max_length=200)
    role: str = Field(default="viewer", pattern=r"^(admin|researcher|clinician|analyst|viewer)$")
    department: Optional[str] = Field(None, max_length=100)


class UserUpdateRequest(BaseModel):
    display_name: Optional[str] = Field(None, max_length=100)
    email: Optional[str] = Field(None, max_length=200)
    role: Optional[str] = Field(None, pattern=r"^(admin|researcher|clinician|analyst|viewer)$")
    department: Optional[str] = Field(None, max_length=100)
    is_active: Optional[bool] = None


class IpWhitelistCreate(BaseModel):
    ip_pattern: str = Field(..., max_length=50)
    description: Optional[str] = Field(None, max_length=200)
    scope: str = Field(default="admin", pattern=r"^(admin|all|api)$")


# ══════════════════════════════════════════════════════════════════════════════
# DB Schema
# ══════════════════════════════════════════════════════════════════════════════

_tables_ensured = False


async def ensure_auth_tables(conn=None):
    global _tables_ensured
    if _tables_ensured:
        return
    close = conn is None
    if close:
        conn = await get_connection()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_user (
                user_id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(500) NOT NULL,
                display_name VARCHAR(100) NOT NULL DEFAULT '',
                email VARCHAR(200),
                role VARCHAR(30) NOT NULL DEFAULT 'viewer'
                    CHECK (role IN ('admin','researcher','clinician','analyst','viewer')),
                department VARCHAR(100),
                is_active BOOLEAN DEFAULT TRUE,
                is_locked BOOLEAN DEFAULT FALSE,
                locked_until TIMESTAMP,
                failed_login_count INT DEFAULT 0,
                last_login TIMESTAMP,
                password_changed_at TIMESTAMP DEFAULT NOW(),
                password_expires_at TIMESTAMP DEFAULT NOW() + INTERVAL '90 days',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_login_attempt (
                attempt_id BIGSERIAL PRIMARY KEY,
                username VARCHAR(50) NOT NULL,
                ip_address VARCHAR(45),
                user_agent TEXT,
                success BOOLEAN DEFAULT FALSE,
                failure_reason VARCHAR(100),
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_attempt_user ON auth_login_attempt(username, created_at)"
        )
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_token_blacklist (
                token_id SERIAL PRIMARY KEY,
                jti VARCHAR(100) UNIQUE NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_blacklist_jti ON auth_token_blacklist(jti)"
        )
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_password_history (
                history_id SERIAL PRIMARY KEY,
                user_id INT REFERENCES auth_user(user_id) ON DELETE CASCADE,
                password_hash VARCHAR(500) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_ip_whitelist (
                whitelist_id SERIAL PRIMARY KEY,
                ip_pattern VARCHAR(50) NOT NULL,
                description VARCHAR(200),
                scope VARCHAR(20) DEFAULT 'admin'
                    CHECK (scope IN ('admin','all','api')),
                enabled BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        _tables_ensured = True
    finally:
        if close:
            await conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# Seed Data
# ══════════════════════════════════════════════════════════════════════════════

_seed_done = False


async def ensure_seed_users():
    global _seed_done
    if _seed_done:
        return
    conn = await get_connection()
    try:
        await ensure_auth_tables(conn)
        count = await conn.fetchval("SELECT COUNT(*) FROM auth_user")
        if count > 0:
            _seed_done = True
            return

        accounts = [
            ("idp_admin", "Admin1234!", "시스템 관리자", "admin@asan.org", "admin", "정보전산팀"),
            ("researcher01", "Research1234!", "연구자", "researcher@asan.org", "researcher", "임상연구센터"),
            ("clinician01", "Clinic1234!", "임상의", "clinician@asan.org", "clinician", "내과"),
            ("analyst01", "Analyst1234!", "분석가", "analyst@asan.org", "analyst", "데이터분석팀"),
            ("viewer01", "Viewer1234!", "열람자", "viewer@asan.org", "viewer", "일반"),
        ]
        for uname, pwd, dname, email, role, dept in accounts:
            pw_hash = hash_password(pwd)
            uid = await conn.fetchval("""
                INSERT INTO auth_user (username, password_hash, display_name, email, role, department)
                VALUES ($1, $2, $3, $4, $5, $6) RETURNING user_id
            """, uname, pw_hash, dname, email, role, dept)
            await conn.execute(
                "INSERT INTO auth_password_history (user_id, password_hash) VALUES ($1, $2)",
                uid, pw_hash,
            )

        # 기본 IP 화이트리스트 (사설 네트워크)
        await conn.execute("""
            INSERT INTO auth_ip_whitelist (ip_pattern, description, scope) VALUES
                ('127.0.0.1', '로컬호스트', 'all'),
                ('::1', '로컬호스트 IPv6', 'all'),
                ('10.0.0.0/8', '사설 네트워크 A', 'admin'),
                ('172.16.0.0/12', '사설 네트워크 B', 'admin'),
                ('192.168.0.0/16', '사설 네트워크 C', 'admin')
        """)
        _seed_done = True
    finally:
        await conn.close()
