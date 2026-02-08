"""
SER-004: CSRF 토큰 생성/검증 미들웨어
- GET/HEAD/OPTIONS: CSRF 토큰을 쿠키로 발급
- POST/PUT/DELETE: X-CSRF-Token 헤더 검증
"""
import os
import hmac
import hashlib
import secrets
import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

CSRF_SECRET = os.getenv("CSRF_SECRET", "csrf-" + os.getenv("JWT_SECRET", "dev-csrf-key")[:16])
CSRF_COOKIE = "csrf_token"
CSRF_HEADER = "X-CSRF-Token"
TOKEN_MAX_AGE = 3600  # 1시간

# CSRF 검증 제외 경로
EXEMPT_PATHS = {
    "/api/v1/health",
    "/api/v1/metrics",
    "/api/v1/auth/login",
    "/api/v1/auth/refresh",
    "/api/docs",
    "/api/redoc",
    "/api/openapi.json",
}
EXEMPT_PREFIXES = ("/api/v1/health",)

SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}


def _generate_token() -> str:
    """타임스탬프 + 랜덤 + HMAC 서명으로 CSRF 토큰 생성"""
    ts = str(int(time.time()))
    nonce = secrets.token_hex(16)
    payload = f"{ts}:{nonce}"
    sig = hmac.new(CSRF_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:16]
    return f"{payload}:{sig}"


def _validate_token(token: str) -> bool:
    """CSRF 토큰 서명 + 만료 검증"""
    try:
        parts = token.split(":")
        if len(parts) != 3:
            return False
        ts, nonce, sig = parts
        payload = f"{ts}:{nonce}"
        expected = hmac.new(CSRF_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:16]
        if not hmac.compare_digest(sig, expected):
            return False
        if time.time() - int(ts) > TOKEN_MAX_AGE:
            return False
        return True
    except Exception:
        return False


class CSRFMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # 제외 경로
        if path in EXEMPT_PATHS or any(path.startswith(p) for p in EXEMPT_PREFIXES):
            return await call_next(request)

        # 안전한 메서드: 토큰 발급
        if request.method in SAFE_METHODS:
            response = await call_next(request)
            token = _generate_token()
            response.set_cookie(
                CSRF_COOKIE,
                token,
                max_age=TOKEN_MAX_AGE,
                httponly=False,  # JS에서 읽어야 함
                samesite="strict",
                secure=request.url.scheme == "https",
                path="/",
            )
            return response

        # 상태 변경 메서드: 토큰 검증
        header_token = request.headers.get(CSRF_HEADER, "")
        cookie_token = request.cookies.get(CSRF_COOKIE, "")

        if not header_token or not cookie_token:
            return Response(
                content='{"detail":"CSRF 토큰이 필요합니다"}',
                status_code=403,
                media_type="application/json",
            )

        if not hmac.compare_digest(header_token, cookie_token):
            return Response(
                content='{"detail":"CSRF 토큰이 일치하지 않습니다"}',
                status_code=403,
                media_type="application/json",
            )

        if not _validate_token(header_token):
            return Response(
                content='{"detail":"CSRF 토큰이 만료되었거나 유효하지 않습니다"}',
                status_code=403,
                media_type="application/json",
            )

        return await call_next(request)
