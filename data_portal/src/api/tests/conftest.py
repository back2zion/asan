"""
테스트 공유 fixtures — lifespan 우회 + 외부 서비스 mock
"""
import os
import sys
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import httpx

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── FastAPI TestApp (lifespan 우회) ──────────────────────────────────────────

@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


@pytest.fixture(scope="session")
def event_loop():
    """세션 범위 이벤트 루프"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture()
def app():
    """lifespan 우회한 FastAPI 앱 (DB/S3/RAG 초기화 skip)"""
    # lifespan에서 사용하는 서비스들을 mock
    with patch("services.db_pool.init_pool", new_callable=AsyncMock), \
         patch("services.db_pool.close_pool", new_callable=AsyncMock), \
         patch("services.db_pool.get_pool", new_callable=AsyncMock), \
         patch("services.redis_cache.close_redis", new_callable=AsyncMock), \
         patch("services.s3_service.ensure_buckets"):
        from main import app as _app
        yield _app


@pytest.fixture()
async def client(app):
    """httpx AsyncClient — ASGI transport로 직접 연결"""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as ac:
        yield ac


# ── CSRF 토큰 fixture ───────────────────────────────────────────────────────

@pytest.fixture()
def csrf_token():
    """유효한 CSRF 토큰 생성"""
    from middleware.csrf import _generate_token
    return _generate_token()


@pytest.fixture()
def csrf_headers(csrf_token):
    """CSRF 헤더 + 쿠키 셋"""
    return {
        "X-CSRF-Token": csrf_token,
        "Cookie": f"csrf_token={csrf_token}",
    }


# ── JWT 인증 fixture ────────────────────────────────────────────────────────

@pytest.fixture()
def auth_token():
    """유효한 JWT 토큰"""
    from routers.auth_shared import create_jwt_token
    from datetime import timedelta
    return create_jwt_token(
        {
            "sub": 1,
            "username": "test_admin",
            "role": "admin",
            "display_name": "테스트 관리자",
        },
        expires_delta=timedelta(hours=1),
    )


@pytest.fixture()
def auth_headers(auth_token):
    """Authorization Bearer 헤더"""
    return {"Authorization": f"Bearer {auth_token}"}


@pytest.fixture()
def authenticated_headers(auth_headers, csrf_headers):
    """인증 + CSRF 헤더 결합"""
    return {**auth_headers, **csrf_headers}


# ── Mock DB 연결 ─────────────────────────────────────────────────────────────

@pytest.fixture()
def mock_db_conn():
    """AsyncMock asyncpg connection"""
    conn = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetchval = AsyncMock(return_value=None)
    conn.execute = AsyncMock()
    conn.close = AsyncMock()
    return conn


@pytest.fixture()
def mock_db_pool(mock_db_conn):
    """AsyncMock asyncpg pool"""
    pool = AsyncMock()
    pool.acquire = AsyncMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_db_conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
    return pool


# ── Mock Redis ───────────────────────────────────────────────────────────────

@pytest.fixture()
def mock_redis():
    """AsyncMock Redis client"""
    r = AsyncMock()
    r.get = AsyncMock(return_value=None)
    r.set = AsyncMock(return_value=True)
    r.delete = AsyncMock(return_value=True)
    r.aclose = AsyncMock()
    return r


# ── Mock LLM ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def mock_llm_service():
    """LLM 서비스 mock"""
    service = AsyncMock()
    service.extract_intent = AsyncMock()
    service._call_llm = AsyncMock(return_value="mock response")
    return service


# ── Mock httpx ───────────────────────────────────────────────────────────────

@pytest.fixture()
def mock_httpx_client():
    """외부 서비스 httpx mock"""
    client = AsyncMock()
    client.post = AsyncMock()
    client.get = AsyncMock()
    return client
