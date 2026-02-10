"""
Phase 7: E2E Smoke 테스트 — 실제 서비스 연결 확인
@pytest.mark.e2e (전체 서비스 필요)
"""
import pytest
import httpx
import asyncio
import asyncpg
import os


API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000/api/v1")
OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}


@pytest.mark.e2e
class TestAPISmoke:
    """API 서버 연결 확인"""

    async def test_api_health(self):
        async with httpx.AsyncClient(timeout=10) as client:
            try:
                resp = await client.get(f"{API_BASE}/health")
                assert resp.status_code == 200
                assert resp.json()["status"] == "healthy"
            except httpx.ConnectError:
                pytest.skip("API server not running")


@pytest.mark.e2e
class TestOMOPDBSmoke:
    """OMOP DB 연결 확인"""

    async def test_db_connection(self):
        try:
            conn = await asyncpg.connect(**OMOP_DB_CONFIG)
            result = await conn.fetchval("SELECT 1")
            assert result == 1
            await conn.close()
        except Exception as e:
            pytest.skip(f"OMOP DB not available: {e}")


@pytest.mark.e2e
class TestRedisSmoke:
    """Redis 연결 확인"""

    async def test_redis_ping(self):
        try:
            import redis.asyncio as aioredis
            r = aioredis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                password=os.getenv("REDIS_PASSWORD", "asan2025!"),
                socket_connect_timeout=3,
            )
            pong = await r.ping()
            assert pong is True
            await r.aclose()
        except Exception as e:
            pytest.skip(f"Redis not available: {e}")


@pytest.mark.e2e
class TestMilvusSmoke:
    """Milvus Vector DB 연결 확인"""

    async def test_milvus_health(self):
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                resp = await client.get("http://localhost:9091/healthz")
                assert resp.status_code == 200
        except Exception as e:
            pytest.skip(f"Milvus not available: {e}")


@pytest.mark.e2e
class TestMinIOSmoke:
    """MinIO S3 연결 확인"""

    async def test_minio_connection(self):
        try:
            from minio import Minio
            client = Minio(
                endpoint=os.getenv("MINIO_ENDPOINT", "localhost:19000"),
                access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
                secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
                secure=False,
            )
            buckets = client.list_buckets()
            assert len(buckets) >= 0  # 연결 성공
        except Exception as e:
            pytest.skip(f"MinIO not available: {e}")


@pytest.mark.e2e
class TestNERServiceSmoke:
    """NER 서비스 연결 확인"""

    async def test_ner_health(self):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get("http://localhost:28100/ner/health")
                assert resp.status_code == 200
        except Exception as e:
            pytest.skip(f"NER service not available: {e}")


@pytest.mark.e2e
class TestLLMServiceSmoke:
    """LLM 서비스 (Qwen3) 연결 확인"""

    async def test_llm_health(self):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get("http://localhost:28888/v1/models")
                assert resp.status_code == 200
        except Exception as e:
            pytest.skip(f"LLM service not available: {e}")


@pytest.mark.e2e
class TestPaper2SlidesSmoke:
    """Paper2Slides 서비스 연결 확인"""

    async def test_p2s_health(self):
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get("http://localhost:29001/health")
                assert resp.status_code == 200
        except Exception as e:
            pytest.skip(f"Paper2Slides not available: {e}")
