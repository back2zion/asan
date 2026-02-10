"""
거버넌스 API 테스트 — /api/v1/governance/sensitivity, /api/v1/governance/roles
Mock 패턴 C: gov_shared.get_connection()
"""
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock

import pytest


class _DictRow(dict):
    """asyncpg Record처럼 dict(row) 가능한 mock row"""

    def __getitem__(self, key):
        return super().__getitem__(key)


def _column_row(table, column):
    return _DictRow(table_name=table, column_name=column)


def _role_row(role_id=1, role_name="관리자"):
    return _DictRow(
        role_id=role_id,
        role_name=role_name,
        description="시스템 전체 관리",
        access_scope="전체 접근",
        allowed_tables=["person", "visit_occurrence"],
        security_level="Row/Column/Cell",
    )


# ══════════════════════════════════════════════════════════
#  GET /governance/sensitivity
# ══════════════════════════════════════════════════════════

class TestGovernanceSensitivity:

    async def test_sensitivity_returns_three_groups(self, client, csrf_headers, mock_db_conn):
        cols = [
            _column_row("person", "person_id"),
            _column_row("person", "gender_source_value"),
            _column_row("person", "year_of_birth"),
        ]
        mock_db_conn.fetch = AsyncMock(side_effect=[
            cols,   # information_schema.columns
            [],     # sensitivity_override (empty)
        ])
        mock_db_conn.execute = AsyncMock()

        with patch("routers.gov_sensitivity.get_connection", AsyncMock(return_value=mock_db_conn)):
            resp = await client.get("/api/v1/governance/sensitivity", headers=csrf_headers)

        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) == 3
        levels = {item["level"] for item in data}
        assert levels == {"극비", "민감", "일반"}

    async def test_sensitivity_has_color_and_count(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(side_effect=[[], []])
        mock_db_conn.execute = AsyncMock()

        with patch("routers.gov_sensitivity.get_connection", AsyncMock(return_value=mock_db_conn)):
            resp = await client.get("/api/v1/governance/sensitivity", headers=csrf_headers)

        assert resp.status_code == 200
        for item in resp.json():
            assert "color" in item
            assert "count" in item
            assert "columns" in item


# ══════════════════════════════════════════════════════════
#  PUT /governance/sensitivity
# ══════════════════════════════════════════════════════════

class TestGovernanceSensitivityUpdate:

    async def test_update_sensitivity(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock()

        with patch("routers.gov_sensitivity.get_connection", AsyncMock(return_value=mock_db_conn)):
            resp = await client.put(
                "/api/v1/governance/sensitivity",
                json={
                    "table_name": "person",
                    "column_name": "person_id",
                    "level": "극비",
                    "reason": "환자 식별자",
                },
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    async def test_update_sensitivity_invalid_level(self, client, csrf_headers):
        resp = await client.put(
            "/api/v1/governance/sensitivity",
            json={
                "table_name": "person",
                "column_name": "person_id",
                "level": "invalid",
            },
            headers=csrf_headers,
        )
        assert resp.status_code == 422


# ══════════════════════════════════════════════════════════
#  RBAC Roles CRUD
# ══════════════════════════════════════════════════════════

class TestGovernanceRoles:

    async def test_list_roles(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock()
        mock_db_conn.fetchval = AsyncMock(return_value=4)  # count > 0 → skip seed
        mock_db_conn.fetch = AsyncMock(return_value=[_role_row(1, "관리자"), _role_row(2, "연구자")])

        with patch("routers.gov_rbac.get_connection", AsyncMock(return_value=mock_db_conn)):
            resp = await client.get("/api/v1/governance/roles", headers=csrf_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)
        assert len(data) >= 1
        assert "role_id" in data[0]
        assert "table_count" in data[0]

    async def test_create_role(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchval = AsyncMock(return_value=5)

        with patch("routers.gov_rbac.get_connection", AsyncMock(return_value=mock_db_conn)):
            resp = await client.post(
                "/api/v1/governance/roles",
                json={
                    "role_name": "외부연구자",
                    "description": "외부 공동연구자",
                    "access_scope": "비식별 데이터",
                    "allowed_tables": ["person", "condition_occurrence"],
                    "security_level": "Row",
                },
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    async def test_update_role(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="UPDATE 1")

        with patch("routers.gov_rbac.get_connection", AsyncMock(return_value=mock_db_conn)):
            resp = await client.put(
                "/api/v1/governance/roles/1",
                json={
                    "role_name": "수정된 역할",
                    "description": "수정된 설명",
                    "access_scope": "제한 접근",
                    "allowed_tables": ["person"],
                    "security_level": "Table",
                },
                headers=csrf_headers,
            )
        assert resp.status_code == 200

    async def test_update_role_not_found(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="UPDATE 0")

        with patch("routers.gov_rbac.get_connection", AsyncMock(return_value=mock_db_conn)):
            resp = await client.put(
                "/api/v1/governance/roles/999",
                json={
                    "role_name": "없는 역할",
                    "access_scope": "none",
                    "security_level": "None",
                },
                headers=csrf_headers,
            )
        assert resp.status_code == 404

    async def test_delete_role(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="DELETE 1")

        with patch("routers.gov_rbac.get_connection", AsyncMock(return_value=mock_db_conn)):
            resp = await client.delete("/api/v1/governance/roles/1", headers=csrf_headers)
        assert resp.status_code == 200
        assert resp.json()["success"] is True

    async def test_delete_role_not_found(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="DELETE 0")

        with patch("routers.gov_rbac.get_connection", AsyncMock(return_value=mock_db_conn)):
            resp = await client.delete("/api/v1/governance/roles/999", headers=csrf_headers)
        assert resp.status_code == 404
