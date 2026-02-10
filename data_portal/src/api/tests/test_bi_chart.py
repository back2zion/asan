"""
BI Chart & Dashboard API 테스트 — /api/v1/bi/charts/*, /api/v1/bi/dashboards/*, /api/v1/bi/overview
Mock 패턴 A: _bi_shared.get_connection() + bi_init()
"""
import json
from datetime import datetime
from unittest.mock import AsyncMock, patch, MagicMock

import pytest


def _mock_chart_row(chart_id=1, chart_type="bar"):
    """차트 DB row mock 헬퍼"""
    row = MagicMock()
    data = {
        "chart_id": chart_id,
        "name": "테스트 차트",
        "chart_type": chart_type,
        "sql_query": "SELECT 1 AS val",
        "config": {},
        "description": "테스트",
        "creator": "admin",
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    row.__getitem__ = lambda s, k: data[k]
    row.keys = lambda: data.keys()
    return row


def _mock_dashboard_row(dashboard_id=1):
    """대시보드 DB row mock 헬퍼"""
    row = MagicMock()
    data = {
        "dashboard_id": dashboard_id,
        "name": "테스트 대시보드",
        "description": "대시보드 설명",
        "layout": {"columns": 2},
        "chart_ids": [1, 2],
        "creator": "admin",
        "shared": True,
        "created_at": datetime.now(),
        "updated_at": datetime.now(),
    }
    row.__getitem__ = lambda s, k: data[k]
    row.keys = lambda: data.keys()
    return row


# ══════════════════════════════════════════════════════════
#  Charts CRUD
# ══════════════════════════════════════════════════════════

class TestBiChartCrud:

    async def test_create_chart(self, client, csrf_headers, mock_db_conn):
        ret_row = MagicMock()
        ret_row.__getitem__ = lambda s, k: {"chart_id": 1, "created_at": datetime.now()}[k]
        mock_db_conn.fetchrow = AsyncMock(return_value=ret_row)

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.post(
                "/api/v1/bi/charts",
                json={
                    "name": "성별 분포",
                    "chart_type": "pie",
                    "sql_query": "SELECT gender_source_value, COUNT(*) FROM person GROUP BY 1",
                },
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        assert "chart_id" in resp.json()

    async def test_create_chart_invalid_type_rejected(self, client, csrf_headers):
        resp = await client.post(
            "/api/v1/bi/charts",
            json={
                "name": "차트",
                "chart_type": "invalid_type",
                "sql_query": "SELECT 1",
            },
            headers=csrf_headers,
        )
        assert resp.status_code == 422

    async def test_create_chart_dangerous_sql_rejected(self, client, csrf_headers, mock_db_conn):
        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.post(
                "/api/v1/bi/charts",
                json={
                    "name": "악성 차트",
                    "chart_type": "bar",
                    "sql_query": "DROP TABLE person",
                },
                headers=csrf_headers,
            )
        assert resp.status_code == 400

    async def test_list_charts(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[_mock_chart_row()])

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.get("/api/v1/bi/charts", headers=csrf_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_get_chart_with_data(self, client, csrf_headers, mock_db_conn):
        chart = _mock_chart_row()
        mock_db_conn.fetchrow = AsyncMock(return_value=chart)
        mock_db_conn.fetch = AsyncMock(return_value=[])

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.get("/api/v1/bi/charts/1", headers=csrf_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["chart_id"] == 1
        assert "data" in data

    async def test_get_chart_not_found(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchrow = AsyncMock(return_value=None)

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.get("/api/v1/bi/charts/999", headers=csrf_headers)
        assert resp.status_code == 404

    async def test_delete_chart(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="DELETE 1")

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.delete("/api/v1/bi/charts/1", headers=csrf_headers)
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    async def test_delete_chart_not_found(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="DELETE 0")

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.delete("/api/v1/bi/charts/999", headers=csrf_headers)
        assert resp.status_code == 404


# ══════════════════════════════════════════════════════════
#  Dashboards CRUD
# ══════════════════════════════════════════════════════════

class TestBiDashboardCrud:

    async def test_create_dashboard(self, client, csrf_headers, mock_db_conn):
        ret_row = MagicMock()
        ret_row.__getitem__ = lambda s, k: {"dashboard_id": 1, "created_at": datetime.now()}[k]
        mock_db_conn.fetchrow = AsyncMock(return_value=ret_row)

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.post(
                "/api/v1/bi/dashboards",
                json={
                    "name": "테스트 대시보드",
                    "chart_ids": [1, 2],
                    "layout": {"columns": 2},
                },
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        assert "dashboard_id" in resp.json()

    async def test_list_dashboards(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[_mock_dashboard_row()])

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.get("/api/v1/bi/dashboards", headers=csrf_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_delete_dashboard(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="DELETE 1")

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.delete("/api/v1/bi/dashboards/1", headers=csrf_headers)
        assert resp.status_code == 200

    async def test_delete_dashboard_not_found(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.execute = AsyncMock(return_value="DELETE 0")

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.delete("/api/v1/bi/dashboards/999", headers=csrf_headers)
        assert resp.status_code == 404


# ══════════════════════════════════════════════════════════
#  GET /bi/overview
# ══════════════════════════════════════════════════════════

class TestBiOverview:

    async def test_overview(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchval = AsyncMock(return_value=5)
        mock_db_conn.fetch = AsyncMock(return_value=[])

        with patch("routers.bi_chart.get_connection", AsyncMock(return_value=mock_db_conn)), \
             patch("routers.bi_chart.bi_init", AsyncMock()):
            resp = await client.get("/api/v1/bi/overview", headers=csrf_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "chart_count" in data
        assert "dashboard_count" in data
        assert "recent_queries" in data
