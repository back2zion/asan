"""
SER-004: 감사 로그 미들웨어
- 관리자 작업 (POST/PUT/DELETE) 자동 기록
- SQL 실행 이력 기록
- po_audit_log 테이블에 저장
"""
import time
import logging
import json
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = logging.getLogger("audit")

# 감사 대상 경로 패턴
AUDIT_PATHS = {
    "/api/v1/auth/",
    "/api/v1/portal-ops/",
    "/api/v1/governance/",
    "/api/v1/security/",
    "/api/v1/permission/",
    "/api/v1/cohort/",
    "/api/v1/text2sql/",
    "/api/v1/bi/",
}

# 제외 (읽기 전용 / 내부)
EXCLUDE_PATHS = {"/api/v1/health", "/api/v1/metrics"}

_table_ensured = False


async def _ensure_audit_table():
    """감사 로그 테이블 생성 (최초 1회)"""
    global _table_ensured
    if _table_ensured:
        return
    try:
        from services.db_pool import get_pool
        pool = await get_pool()
        if pool is None:
            return
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS po_audit_log (
                    audit_id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ DEFAULT NOW(),
                    user_id VARCHAR(50),
                    username VARCHAR(100),
                    role VARCHAR(30),
                    method VARCHAR(10) NOT NULL,
                    path VARCHAR(500) NOT NULL,
                    status_code INTEGER,
                    duration_ms DOUBLE PRECISION,
                    ip_address VARCHAR(45),
                    user_agent VARCHAR(500),
                    request_body TEXT,
                    response_summary TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_po_audit_ts ON po_audit_log(timestamp);
                CREATE INDEX IF NOT EXISTS idx_po_audit_user ON po_audit_log(user_id);
                CREATE INDEX IF NOT EXISTS idx_po_audit_path ON po_audit_log(path);
            """)
        _table_ensured = True
    except Exception as e:
        logger.debug(f"Audit table creation skipped: {e}")


async def _write_audit(
    method: str, path: str, status_code: int, duration_ms: float,
    ip_address: str, user_agent: str, user_id: str = None,
    username: str = None, role: str = None,
    request_body: str = None, response_summary: str = None,
):
    """감사 로그 DB 기록 (비동기, 실패해도 요청 차단하지 않음)"""
    try:
        from services.db_pool import get_pool
        pool = await get_pool()
        if pool is None:
            return
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO po_audit_log
                    (user_id, username, role, method, path, status_code,
                     duration_ms, ip_address, user_agent, request_body, response_summary)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            """,
                user_id, username, role, method, path, status_code,
                duration_ms, ip_address, user_agent,
                request_body[:2000] if request_body else None,
                response_summary[:500] if response_summary else None,
            )
    except Exception as e:
        logger.debug(f"Audit write failed: {e}")


class AuditMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # 제외 경로
        if path in EXCLUDE_PATHS:
            return await call_next(request)

        # 감사 대상 판별
        is_audit_target = any(path.startswith(p) for p in AUDIT_PATHS)
        is_write_op = request.method in ("POST", "PUT", "DELETE", "PATCH")

        if not (is_audit_target and is_write_op):
            return await call_next(request)

        # 테이블 초기화
        await _ensure_audit_table()

        # 요청 본문 캡처 (민감정보 마스킹)
        body_text = None
        try:
            body = await request.body()
            if body:
                body_dict = json.loads(body)
                # 비밀번호 필드 마스킹
                for key in ("password", "current_password", "new_password", "secret"):
                    if key in body_dict:
                        body_dict[key] = "***"
                body_text = json.dumps(body_dict, ensure_ascii=False)
        except Exception:
            pass

        # 사용자 정보 추출 (JWT에서)
        user_id = username = role = None
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            try:
                from routers.auth_shared import decode_jwt_token
                payload = decode_jwt_token(auth_header[7:])
                user_id = str(payload.get("sub", ""))
                username = payload.get("username", "")
                role = payload.get("role", "")
            except Exception:
                pass

        start = time.monotonic()
        response = await call_next(request)
        duration = (time.monotonic() - start) * 1000

        # 비동기 감사 기록
        ip = request.headers.get("X-Real-IP", request.client.host if request.client else "unknown")
        ua = request.headers.get("User-Agent", "")[:500]

        await _write_audit(
            method=request.method,
            path=path,
            status_code=response.status_code,
            duration_ms=round(duration, 2),
            ip_address=ip,
            user_agent=ua,
            user_id=user_id,
            username=username,
            role=role,
            request_body=body_text,
            response_summary=f"status={response.status_code}",
        )

        # 구조화된 텍스트 로그
        logger.info(
            f"AUDIT | {request.method} {path} | user={username or 'anon'} "
            f"| role={role or '-'} | status={response.status_code} | {duration:.0f}ms | ip={ip}"
        )

        return response
