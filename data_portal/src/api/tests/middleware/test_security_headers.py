"""
Phase 2: 보안 헤더 미들웨어 테스트
"""
import pytest


EXPECTED_HEADERS = {
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "strict-transport-security": "max-age=31536000; includeSubDomains",
    "x-xss-protection": "1; mode=block",
    "referrer-policy": "strict-origin-when-cross-origin",
    "permissions-policy": "camera=(), microphone=(), geolocation=()",
}


class TestSecurityHeaders:
    """모든 응답에 보안 헤더 6개 확인"""

    async def test_health_has_security_headers(self, client):
        resp = await client.get("/api/v1/health")
        for header, value in EXPECTED_HEADERS.items():
            assert resp.headers.get(header) == value, f"Missing or wrong header: {header}"

    async def test_root_has_security_headers(self, client):
        resp = await client.get("/")
        for header, value in EXPECTED_HEADERS.items():
            assert resp.headers.get(header) == value, f"Missing or wrong header: {header}"

    async def test_404_has_security_headers(self, client):
        resp = await client.get("/nonexistent")
        for header, value in EXPECTED_HEADERS.items():
            assert resp.headers.get(header) == value, f"Missing or wrong header: {header}"

    async def test_metrics_has_security_headers(self, client):
        resp = await client.get("/api/v1/metrics")
        for header, value in EXPECTED_HEADERS.items():
            assert resp.headers.get(header) == value, f"Missing or wrong header: {header}"
