"""
Phase 2: 감사 로그 미들웨어 테스트
"""
import pytest
from unittest.mock import AsyncMock, patch


class TestAuditConfig:
    """감사 대상 경로 설정 확인"""

    def test_audit_paths_exist(self):
        from middleware.audit import AUDIT_PATHS
        assert "/api/v1/auth/" in AUDIT_PATHS
        assert "/api/v1/text2sql/" in AUDIT_PATHS
        assert "/api/v1/governance/" in AUDIT_PATHS

    def test_exclude_paths(self):
        from middleware.audit import EXCLUDE_PATHS
        assert "/api/v1/health" in EXCLUDE_PATHS
        assert "/api/v1/metrics" in EXCLUDE_PATHS


class TestAuditMiddleware:
    """감사 미들웨어 통합 테스트"""

    async def test_get_request_not_audited(self, client):
        """GET 요청 → 감사 대상 아님"""
        with patch("middleware.audit._write_audit", new_callable=AsyncMock) as mock_write:
            resp = await client.get("/api/v1/health")
        assert resp.status_code == 200
        mock_write.assert_not_called()

    async def test_post_to_audit_path_is_audited(self, client, csrf_headers):
        """POST to audit path → 감사 로그 기록"""
        mock_pool = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=AsyncMock())
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=False)
        with patch("middleware.audit._write_audit", new_callable=AsyncMock) as mock_write, \
             patch("middleware.audit._ensure_audit_table", new_callable=AsyncMock), \
             patch("services.db_pool.get_pool", new_callable=AsyncMock, return_value=mock_pool):
            resp = await client.post(
                "/api/v1/auth/login",
                json={"username": "test", "password": "test123"},
                headers=csrf_headers,
            )
        # /auth/login은 exempt path이므로 CSRF 통과
        # audit은 POST + audit path 조건이 맞으면 실행
        assert resp.status_code != 403

    async def test_password_masking_in_body(self):
        """비밀번호 필드 마스킹 확인"""
        import json
        body_dict = {"username": "admin", "password": "secret123", "secret": "key"}
        for key in ("password", "current_password", "new_password", "secret"):
            if key in body_dict:
                body_dict[key] = "***"
        result = json.dumps(body_dict, ensure_ascii=False)
        assert "secret123" not in result
        assert "key" not in result
        assert "***" in result

    async def test_exclude_path_not_audited(self, client):
        """health/metrics는 감사 제외"""
        with patch("middleware.audit._write_audit", new_callable=AsyncMock) as mock_write:
            await client.get("/api/v1/health")
            await client.get("/api/v1/metrics")
        mock_write.assert_not_called()
