"""
SER-004: Rate Limiting 미들웨어
순수 Python dict 기반 토큰 버킷 — 외부 의존성 없음
"""
import time
import logging
from collections import defaultdict
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

logger = logging.getLogger("rate_limit")

# 면제 경로
EXEMPT_PATHS = {"/api/v1/health", "/api/v1/metrics", "/api/docs", "/api/redoc", "/api/openapi.json"}
EXEMPT_PREFIXES = ("/api/v1/ai-architecture/health",)


class RateLimitMiddleware(BaseHTTPMiddleware):
    """IP별 토큰 버킷 Rate Limiter"""

    def __init__(self, app, per_minute: int = 100, per_hour: int = 1000):
        super().__init__(app)
        self.per_minute = per_minute
        self.per_hour = per_hour
        # {ip: [(timestamp, ...), ...]}
        self._minute_buckets: dict = defaultdict(list)
        self._hour_buckets: dict = defaultdict(list)
        self._last_cleanup = time.monotonic()

    def _cleanup(self, now: float):
        """오래된 엔트리 정리 (5분마다)"""
        if now - self._last_cleanup < 300:
            return
        cutoff_min = now - 60
        cutoff_hr = now - 3600
        for ip in list(self._minute_buckets.keys()):
            self._minute_buckets[ip] = [t for t in self._minute_buckets[ip] if t > cutoff_min]
            if not self._minute_buckets[ip]:
                del self._minute_buckets[ip]
        for ip in list(self._hour_buckets.keys()):
            self._hour_buckets[ip] = [t for t in self._hour_buckets[ip] if t > cutoff_hr]
            if not self._hour_buckets[ip]:
                del self._hour_buckets[ip]
        self._last_cleanup = now

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if path in EXEMPT_PATHS or path.startswith(EXEMPT_PREFIXES):
            return await call_next(request)

        ip = request.headers.get("X-Real-IP", request.client.host if request.client else "unknown")
        now = time.monotonic()

        self._cleanup(now)

        # 분당 체크
        cutoff_min = now - 60
        self._minute_buckets[ip] = [t for t in self._minute_buckets[ip] if t > cutoff_min]
        if len(self._minute_buckets[ip]) >= self.per_minute:
            logger.warning(f"Rate limit (per-min) exceeded: {ip}")
            return JSONResponse(
                status_code=429,
                content={"detail": "요청 제한 초과 (분당)", "retry_after": 60},
                headers={"Retry-After": "60"},
            )

        # 시간당 체크
        cutoff_hr = now - 3600
        self._hour_buckets[ip] = [t for t in self._hour_buckets[ip] if t > cutoff_hr]
        if len(self._hour_buckets[ip]) >= self.per_hour:
            logger.warning(f"Rate limit (per-hour) exceeded: {ip}")
            return JSONResponse(
                status_code=429,
                content={"detail": "요청 제한 초과 (시간당)", "retry_after": 3600},
                headers={"Retry-After": "3600"},
            )

        self._minute_buckets[ip].append(now)
        self._hour_buckets[ip].append(now)

        return await call_next(request)
