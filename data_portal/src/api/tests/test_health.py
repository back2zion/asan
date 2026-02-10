"""
Phase 1: Health + Root 엔드포인트 테스트
"""
import pytest


class TestHealthEndpoint:
    """GET /api/v1/health"""

    async def test_health_returns_200(self, client):
        resp = await client.get("/api/v1/health")
        assert resp.status_code == 200

    async def test_health_has_status_healthy(self, client):
        resp = await client.get("/api/v1/health")
        data = resp.json()
        assert data["status"] == "healthy"

    async def test_health_has_required_fields(self, client):
        resp = await client.get("/api/v1/health")
        data = resp.json()
        assert "timestamp" in data
        assert "service" in data
        assert "version" in data

    async def test_health_version_format(self, client):
        resp = await client.get("/api/v1/health")
        data = resp.json()
        assert data["version"] == "1.0.0"


class TestRootEndpoint:
    """GET /"""

    async def test_root_returns_200(self, client):
        resp = await client.get("/")
        assert resp.status_code == 200

    async def test_root_has_message(self, client):
        resp = await client.get("/")
        data = resp.json()
        assert "message" in data
        assert "IDP" in data["message"]

    async def test_root_has_version(self, client):
        resp = await client.get("/")
        data = resp.json()
        assert data["version"] == "1.0.0"


class TestMetricsEndpoint:
    """GET /api/v1/metrics"""

    async def test_metrics_returns_200(self, client):
        resp = await client.get("/api/v1/metrics")
        assert resp.status_code == 200

    async def test_metrics_content_type(self, client):
        resp = await client.get("/api/v1/metrics")
        assert "text/plain" in resp.headers.get("content-type", "")


class TestNotFoundRoute:
    """존재하지 않는 경로"""

    async def test_404_on_unknown_route(self, client):
        resp = await client.get("/api/v1/nonexistent-route-xyz")
        assert resp.status_code == 404
