"""
Phase 4: Datamart 라우터 API 테스트
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from contextlib import asynccontextmanager


def _make_mock_pool(mock_conn):
    """mock pool that supports pool.acquire() → conn"""
    pool = AsyncMock()

    @asynccontextmanager
    async def acquire():
        yield mock_conn

    pool.acquire = acquire
    pool.release = AsyncMock()
    return pool


class TestDatamartTables:
    """GET /api/v1/datamart/tables"""

    async def test_list_tables(self, client, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(side_effect=[
            [{"table_name": "person", "row_count": 76074}],
            [{"table_name": "person", "col_count": 18}],
        ])
        pool = _make_mock_pool(mock_db_conn)
        with patch("services.db_pool.get_pool", return_value=pool):
            resp = await client.get("/api/v1/datamart/tables")
        assert resp.status_code == 200
        data = resp.json()
        assert "tables" in data
        assert "total_tables" in data


class TestDatamartSchema:
    """GET /api/v1/datamart/tables/{table_name}/schema"""

    async def test_get_schema_success(self, client, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[
            {
                "column_name": "person_id",
                "data_type": "bigint",
                "character_maximum_length": None,
                "is_nullable": "NO",
                "column_default": None,
                "ordinal_position": 1,
            }
        ])
        pool = _make_mock_pool(mock_db_conn)
        with patch("services.db_pool.get_pool", return_value=pool):
            resp = await client.get("/api/v1/datamart/tables/person/schema")
        assert resp.status_code == 200

    async def test_get_schema_invalid_table(self, client):
        """SQL injection 방지 — 잘못된 테이블명 → 404"""
        resp = await client.get("/api/v1/datamart/tables/evil_table/schema")
        assert resp.status_code == 404


class TestDatamartSample:
    """GET /api/v1/datamart/tables/{table_name}/sample"""

    async def test_get_sample_success(self, client, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[
            MagicMock(**{"keys.return_value": ["person_id", "gender_source_value"],
                         "__getitem__": lambda self, k: {"person_id": 1, "gender_source_value": "M"}[k]})
        ])
        pool = _make_mock_pool(mock_db_conn)
        with patch("services.db_pool.get_pool", return_value=pool):
            resp = await client.get("/api/v1/datamart/tables/person/sample?limit=2")
        assert resp.status_code == 200


class TestDatamartHealth:
    """GET /api/v1/datamart/health"""

    async def test_datamart_health(self, client, mock_db_conn):
        mock_db_conn.fetchval = AsyncMock(side_effect=[
            "PostgreSQL 13.0",  # version()
            18,  # table count
        ])
        pool = _make_mock_pool(mock_db_conn)

        async def mock_get_pool():
            return pool

        with patch("routers._datamart_shared.get_pool", side_effect=mock_get_pool):
            resp = await client.get("/api/v1/datamart/health")
        assert resp.status_code == 200


class TestDatamartCacheClear:
    """POST /api/v1/datamart/cache-clear"""

    async def test_cache_clear(self, client, csrf_headers):
        with patch("services.redis_cache.cache_delete", new_callable=AsyncMock, return_value=True), \
             patch("services.redis_cache.get_redis", new_callable=AsyncMock):
            resp = await client.post("/api/v1/datamart/cache-clear", headers=csrf_headers)
        assert resp.status_code != 403
