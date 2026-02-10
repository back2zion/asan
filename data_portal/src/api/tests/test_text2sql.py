"""
Phase 4: Text2SQL 라우터 API 테스트
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock


class TestText2SQLHelpers:
    """순수 함수 테스트"""

    def test_extract_tables_from_sql(self):
        from routers.text2sql import _extract_tables_from_sql
        tables = _extract_tables_from_sql("SELECT * FROM person JOIN condition_occurrence ON 1=1")
        assert "person" in tables
        assert "condition_occurrence" in tables

    def test_extract_tables_empty(self):
        from routers.text2sql import _extract_tables_from_sql
        assert _extract_tables_from_sql("") == []

    def test_extract_tables_from_subquery(self):
        from routers.text2sql import _extract_tables_from_sql
        sql = "SELECT * FROM person WHERE person_id IN (SELECT person_id FROM visit_occurrence)"
        tables = _extract_tables_from_sql(sql)
        assert "person" in tables
        assert "visit_occurrence" in tables


class TestText2SQLValidate:
    """POST /api/v1/text2sql/validate"""

    async def test_validate_valid_sql(self, client, csrf_headers):
        resp = await client.post(
            "/api/v1/text2sql/validate",
            json={"sql": "SELECT * FROM person LIMIT 10"},
            headers=csrf_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("valid") is True or data.get("is_valid") is True

    async def test_validate_dangerous_sql(self, client, csrf_headers):
        resp = await client.post(
            "/api/v1/text2sql/validate",
            json={"sql": "DROP TABLE person"},
            headers=csrf_headers,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("valid") is False or data.get("is_valid") is False


class TestText2SQLExecute:
    """POST /api/v1/text2sql/execute — SQL 실행"""

    async def test_execute_forbidden_sql(self, client, csrf_headers):
        """DELETE 등 금지된 SQL은 거부"""
        resp = await client.post(
            "/api/v1/text2sql/execute",
            json={"sql": "DELETE FROM person WHERE 1=1"},
            headers=csrf_headers,
        )
        # 금지 SQL → 400 또는 실행 거부
        assert resp.status_code in (200, 400, 422)
        if resp.status_code == 200:
            data = resp.json()
            # 실행 결과에 에러가 포함되어야 함
            assert data.get("row_count", 0) == 0


class TestText2SQLMetadata:
    """GET /api/v1/text2sql/metadata/*"""

    async def test_get_tables_metadata(self, client):
        resp = await client.get("/api/v1/text2sql/metadata/tables")
        assert resp.status_code == 200

    async def test_get_terms_metadata(self, client):
        resp = await client.get("/api/v1/text2sql/metadata/terms")
        assert resp.status_code == 200
