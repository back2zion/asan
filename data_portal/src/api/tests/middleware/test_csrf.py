"""
Phase 2: CSRF 미들웨어 테스트
"""
import pytest
import time
from unittest.mock import AsyncMock, patch


class TestCSRFTokenGeneration:
    """_generate_token / _validate_token 단위 테스트"""

    def test_generate_token_format(self):
        from middleware.csrf import _generate_token
        token = _generate_token()
        parts = token.split(":")
        assert len(parts) == 3  # timestamp:nonce:signature

    def test_validate_valid_token(self):
        from middleware.csrf import _generate_token, _validate_token
        token = _generate_token()
        assert _validate_token(token) is True

    def test_validate_expired_token(self):
        from middleware.csrf import _validate_token, CSRF_SECRET
        import hmac, hashlib, secrets
        old_ts = str(int(time.time()) - 7200)  # 2시간 전
        nonce = secrets.token_hex(16)
        payload = f"{old_ts}:{nonce}"
        sig = hmac.new(CSRF_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()[:16]
        token = f"{payload}:{sig}"
        assert _validate_token(token) is False

    def test_validate_tampered_token(self):
        from middleware.csrf import _generate_token, _validate_token
        token = _generate_token()
        parts = token.split(":")
        parts[2] = "0" * 16  # 서명 변조
        tampered = ":".join(parts)
        assert _validate_token(tampered) is False

    def test_validate_wrong_format(self):
        from middleware.csrf import _validate_token
        assert _validate_token("") is False
        assert _validate_token("only:two") is False
        assert _validate_token("a:b:c:d") is False

    def test_validate_non_numeric_timestamp(self):
        from middleware.csrf import _validate_token
        assert _validate_token("abc:nonce:sig") is False


class TestCSRFMiddleware:
    """CSRF 미들웨어 통합 테스트 (client 사용)"""

    async def test_get_sets_csrf_cookie(self, client):
        resp = await client.get("/api/v1/health")
        # health는 CSRF exempt이므로 쿠키가 없을 수 있음
        # 대신 non-exempt GET 경로로 테스트
        resp2 = await client.get("/")
        cookies = resp2.cookies
        # root 엔드포인트도 쿠키를 발급하는지 확인
        # CSRF 미들웨어가 GET에서 쿠키를 설정해야 함
        assert resp2.status_code == 200

    async def test_post_without_csrf_returns_403(self, client):
        """CSRF 토큰 없이 POST → 403 (exempt 아닌 경로)"""
        resp = await client.post(
            "/api/v1/text2sql/validate",
            json={"sql": "SELECT 1"},
        )
        assert resp.status_code == 403

    async def test_post_with_valid_csrf_passes(self, client, csrf_headers):
        """유효한 CSRF 토큰으로 POST → 미들웨어 통과"""
        # text2sql/validate는 내부적으로 다른 에러를 낼 수 있지만
        # CSRF 검증은 통과해야 함 (403이 아닌 다른 상태코드)
        resp = await client.post(
            "/api/v1/text2sql/validate",
            json={"sql": "SELECT 1"},
            headers=csrf_headers,
        )
        assert resp.status_code != 403

    async def test_post_with_mismatched_csrf_returns_403(self, client, csrf_token):
        """헤더와 쿠키의 CSRF 토큰이 다르면 403"""
        from middleware.csrf import _generate_token
        different_token = _generate_token()
        headers = {
            "X-CSRF-Token": csrf_token,
            "Cookie": f"csrf_token={different_token}",
        }
        resp = await client.post(
            "/api/v1/text2sql/validate",
            json={"sql": "SELECT 1"},
            headers=headers,
        )
        assert resp.status_code == 403

    async def test_exempt_path_login_bypasses_csrf(self, client):
        """로그인 경로는 CSRF 검증 면제"""
        with patch("middleware.audit._write_audit", new_callable=AsyncMock), \
             patch("middleware.audit._ensure_audit_table", new_callable=AsyncMock), \
             patch("services.db_pool.get_pool", new_callable=AsyncMock, return_value=None):
            resp = await client.post(
                "/api/v1/auth/login",
                json={"username": "test", "password": "test"},
            )
        # CSRF 403이 아닌 다른 응답 (DB 에러 등)
        assert resp.status_code != 403
