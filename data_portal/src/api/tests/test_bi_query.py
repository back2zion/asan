"""
BI Query API 테스트 — /api/v1/bi/query/* 엔드포인트
Mock 패턴 A: _bi_shared.get_connection() + bi_init()
"""
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock

import pytest


# ══════════════════════════════════════════════════════════
#  POST /bi/query/execute
# ══════════════════════════════════════════════════════════

class TestBiQueryExecute:

    async def test_execute_valid_sql(self, client, csrf_headers, mock_db_conn):
        mock_row = MagicMock()
        mock_row.keys.return_value = ["gender", "count"]
        mock_row.__getitem__ = lambda s, k: {"gender": "M", "count": 100}[k]
        mock_db_conn.fetch = AsyncMock(return_value=[mock_row])
        mock_db_conn.execute = AsyncMock()

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.post(
                "/api/v1/bi/query/execute",
                json={"sql": "SELECT gender, count(*) FROM person GROUP BY gender"},
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "columns" in data
        assert "rows" in data
        assert data["row_count"] >= 1

    async def test_execute_invalid_sql_rejected(self, client, csrf_headers):
        with patch("routers.bi_query.get_connection", AsyncMock()), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.post(
                "/api/v1/bi/query/execute",
                json={"sql": "DROP TABLE person"},
                headers=csrf_headers,
            )
        assert resp.status_code == 400

    async def test_execute_empty_result(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[])
        mock_db_conn.execute = AsyncMock()

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.post(
                "/api/v1/bi/query/execute",
                json={"sql": "SELECT * FROM person WHERE 1=0"},
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        assert resp.json()["row_count"] == 0


# ══════════════════════════════════════════════════════════
#  POST /bi/query/validate
# ══════════════════════════════════════════════════════════

class TestBiQueryValidate:

    async def test_validate_valid_sql(self, client, csrf_headers):
        resp = await client.post(
            "/api/v1/bi/query/validate",
            json={"sql": "SELECT * FROM person LIMIT 10"},
            headers=csrf_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is True

    async def test_validate_dangerous_sql(self, client, csrf_headers):
        resp = await client.post(
            "/api/v1/bi/query/validate",
            json={"sql": "DELETE FROM person"},
            headers=csrf_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False


# ══════════════════════════════════════════════════════════
#  GET /bi/query/history
# ══════════════════════════════════════════════════════════

class TestBiQueryHistory:

    async def test_history_returns_list(self, client, csrf_headers, mock_db_conn):
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda s, k: {
            "id": 1, "sql_text": "SELECT 1", "status": "success",
            "row_count": 1, "execution_time_ms": 5, "columns": [],
            "error_message": None, "created_at": datetime.now(),
        }[k]
        mock_db_conn.fetch = AsyncMock(return_value=[mock_row])

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.get("/api/v1/bi/query/history", headers=csrf_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)


# ══════════════════════════════════════════════════════════
#  GET /bi/query/tables
# ══════════════════════════════════════════════════════════

class TestBiQueryTables:

    async def test_tables_returns_list(self, client, csrf_headers, mock_db_conn):
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda s, k: {"table_name": "person", "row_count": 76074}[k]
        mock_db_conn.fetch = AsyncMock(return_value=[mock_row])

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.get("/api/v1/bi/query/tables", headers=csrf_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert data[0]["table_name"] == "person"


# ══════════════════════════════════════════════════════════
#  GET /bi/query/columns/{table}
# ══════════════════════════════════════════════════════════

class TestBiQueryColumns:

    async def test_columns_valid_table(self, client, csrf_headers, mock_db_conn):
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda s, k: {
            "column_name": "person_id", "data_type": "bigint", "is_nullable": "NO",
        }[k]
        mock_db_conn.fetch = AsyncMock(return_value=[mock_row])

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.get("/api/v1/bi/query/columns/person", headers=csrf_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert data[0]["column_name"] == "person_id"

    async def test_columns_invalid_table_name(self, client, csrf_headers):
        with patch("routers.bi_query.get_connection", AsyncMock()), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.get(
                "/api/v1/bi/query/columns/drop%20table",
                headers=csrf_headers,
            )
        assert resp.status_code == 400

    async def test_columns_not_found(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[])

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.get(
                "/api/v1/bi/query/columns/nonexistent",
                headers=csrf_headers,
            )
        assert resp.status_code == 404


# ══════════════════════════════════════════════════════════
#  POST /bi/query/save, GET /saved, DELETE /saved/{id}
# ══════════════════════════════════════════════════════════

class TestBiQuerySave:

    async def test_save_query(self, client, csrf_headers, mock_db_conn):
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda s, k: {"query_id": 1, "created_at": datetime.now()}[k]
        mock_db_conn.fetchrow = AsyncMock(return_value=mock_row)

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.post(
                "/api/v1/bi/query/save",
                json={
                    "name": "테스트 쿼리",
                    "sql_text": "SELECT * FROM person",
                    "tags": ["test"],
                },
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        assert "query_id" in resp.json()

    async def test_list_saved_queries(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[])

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.get("/api/v1/bi/query/saved", headers=csrf_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_delete_saved_query(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="DELETE 1")

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.delete("/api/v1/bi/query/saved/1", headers=csrf_headers)
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    async def test_delete_saved_query_not_found(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="DELETE 0")

        with patch("routers.bi_query.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_query.bi_init", AsyncMock()):
            resp = await client.delete("/api/v1/bi/query/saved/999", headers=csrf_headers)
        assert resp.status_code == 404
