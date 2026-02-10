"""
Phase 1: Auth 엔드포인트 테스트 (DB mock)
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timedelta


class TestAuthSharedUtils:
    """auth_shared.py 순수 함수 테스트"""

    def test_hash_and_verify_password(self):
        from routers.auth_shared import hash_password, verify_password
        pw = "TestPass123!"
        hashed = hash_password(pw)
        assert verify_password(pw, hashed)

    def test_verify_wrong_password(self):
        from routers.auth_shared import hash_password, verify_password
        hashed = hash_password("Correct123!")
        assert not verify_password("Wrong123!", hashed)

    def test_verify_malformed_hash(self):
        from routers.auth_shared import verify_password
        assert not verify_password("any", "malformed-hash")

    def test_validate_password_ok(self):
        from routers.auth_shared import validate_password
        ok, msg = validate_password("StrongP@ss1")
        assert ok
        assert msg == ""

    def test_validate_password_too_short(self):
        from routers.auth_shared import validate_password
        ok, msg = validate_password("Sh1!")
        assert not ok
        assert "최소" in msg

    def test_validate_password_no_uppercase(self):
        from routers.auth_shared import validate_password
        ok, msg = validate_password("nouppercase1!")
        assert not ok
        assert "대문자" in msg

    def test_validate_password_no_lowercase(self):
        from routers.auth_shared import validate_password
        ok, msg = validate_password("NOLOWERCASE1!")
        assert not ok
        assert "소문자" in msg

    def test_validate_password_no_digit(self):
        from routers.auth_shared import validate_password
        ok, msg = validate_password("NoDigitHere!")
        assert not ok
        assert "숫자" in msg

    def test_validate_password_no_special(self):
        from routers.auth_shared import validate_password
        ok, msg = validate_password("NoSpecial123")
        assert not ok
        assert "특수문자" in msg

    def test_validate_password_forbidden(self):
        from routers.auth_shared import validate_password
        ok, msg = validate_password("password")
        assert not ok

    def test_create_and_decode_jwt(self):
        from routers.auth_shared import create_jwt_token, decode_jwt_token
        payload = {"sub": 1, "username": "test", "role": "admin", "display_name": "테스트"}
        token = create_jwt_token(payload, expires_delta=timedelta(hours=1))
        decoded = decode_jwt_token(token)
        assert decoded["sub"] == 1
        assert decoded["username"] == "test"

    def test_jwt_expired(self):
        from routers.auth_shared import create_jwt_token, decode_jwt_token
        payload = {"sub": 1, "username": "test", "role": "admin", "display_name": "테스트"}
        token = create_jwt_token(payload, expires_delta=timedelta(seconds=-1))
        with pytest.raises(ValueError, match="만료"):
            decode_jwt_token(token)

    def test_jwt_invalid_format(self):
        from routers.auth_shared import decode_jwt_token
        with pytest.raises(ValueError):
            decode_jwt_token("not.a.valid.token.format")

    def test_jwt_tampered_signature(self):
        from routers.auth_shared import create_jwt_token, decode_jwt_token
        payload = {"sub": 1, "username": "test", "role": "admin", "display_name": "테스트"}
        token = create_jwt_token(payload, expires_delta=timedelta(hours=1))
        parts = token.split(".")
        parts[2] = parts[2][::-1]  # 서명 변조
        tampered = ".".join(parts)
        with pytest.raises(ValueError):
            decode_jwt_token(tampered)


class TestIpWhitelist:
    """IP 화이트리스트 검증"""

    def test_empty_whitelist_allows_all(self):
        from routers.auth_shared import check_ip_allowed
        assert check_ip_allowed("1.2.3.4", [])

    def test_exact_ip_match(self):
        from routers.auth_shared import check_ip_allowed
        assert check_ip_allowed("192.168.1.1", ["192.168.1.1"])

    def test_cidr_match(self):
        from routers.auth_shared import check_ip_allowed
        assert check_ip_allowed("10.0.5.100", ["10.0.0.0/8"])

    def test_ip_not_in_whitelist(self):
        from routers.auth_shared import check_ip_allowed
        assert not check_ip_allowed("8.8.8.8", ["192.168.0.0/16"])

    def test_invalid_ip_returns_false(self):
        from routers.auth_shared import check_ip_allowed
        assert not check_ip_allowed("not-an-ip", ["192.168.0.0/16"])


class TestLoginEndpoint:
    """POST /api/v1/auth/login — DB mocked"""

    @pytest.fixture()
    def mock_user_row(self):
        """성공적인 로그인을 위한 mock 사용자 레코드"""
        from routers.auth_shared import hash_password
        return {
            "user_id": 1,
            "username": "test_admin",
            "password_hash": hash_password("Admin1234!"),
            "display_name": "테스트 관리자",
            "role": "admin",
            "department": "테스트팀",
            "email": "admin@test.org",
            "is_active": True,
            "is_locked": False,
            "locked_until": None,
            "failed_login_count": 0,
            "last_login": None,
            "password_changed_at": datetime.utcnow(),
            "password_expires_at": datetime.utcnow() + timedelta(days=90),
        }

    async def test_login_success(self, client, csrf_headers, mock_db_conn, mock_user_row):
        mock_db_conn.fetchrow = AsyncMock(return_value=mock_user_row)
        with patch("routers.auth_core.get_connection", return_value=mock_db_conn), \
             patch("routers.auth_core.ensure_auth_tables", new_callable=AsyncMock), \
             patch("routers.auth_core.ensure_seed_users", new_callable=AsyncMock):
            resp = await client.post(
                "/api/v1/auth/login",
                json={"username": "test_admin", "password": "Admin1234!"},
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "access_token" in data
        assert "refresh_token" in data
        assert data["user"]["username"] == "test_admin"

    async def test_login_user_not_found(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchrow = AsyncMock(return_value=None)
        with patch("routers.auth_core.get_connection", return_value=mock_db_conn), \
             patch("routers.auth_core.ensure_auth_tables", new_callable=AsyncMock), \
             patch("routers.auth_core.ensure_seed_users", new_callable=AsyncMock):
            resp = await client.post(
                "/api/v1/auth/login",
                json={"username": "nouser", "password": "any"},
                headers=csrf_headers,
            )
        assert resp.status_code == 401

    async def test_login_wrong_password(self, client, csrf_headers, mock_db_conn, mock_user_row):
        mock_db_conn.fetchrow = AsyncMock(return_value=mock_user_row)
        with patch("routers.auth_core.get_connection", return_value=mock_db_conn), \
             patch("routers.auth_core.ensure_auth_tables", new_callable=AsyncMock), \
             patch("routers.auth_core.ensure_seed_users", new_callable=AsyncMock):
            resp = await client.post(
                "/api/v1/auth/login",
                json={"username": "test_admin", "password": "WrongPass1!"},
                headers=csrf_headers,
            )
        assert resp.status_code == 401

    async def test_login_locked_account(self, client, csrf_headers, mock_db_conn, mock_user_row):
        mock_user_row["is_locked"] = True
        mock_user_row["locked_until"] = datetime.utcnow() + timedelta(minutes=25)
        mock_db_conn.fetchrow = AsyncMock(return_value=mock_user_row)
        with patch("routers.auth_core.get_connection", return_value=mock_db_conn), \
             patch("routers.auth_core.ensure_auth_tables", new_callable=AsyncMock), \
             patch("routers.auth_core.ensure_seed_users", new_callable=AsyncMock):
            resp = await client.post(
                "/api/v1/auth/login",
                json={"username": "test_admin", "password": "Admin1234!"},
                headers=csrf_headers,
            )
        assert resp.status_code == 423


class TestMeEndpoint:
    """GET /api/v1/auth/me"""

    async def test_me_with_valid_token(self, client, auth_headers):
        with patch("routers.auth_shared.AUTH_REQUIRED", False):
            resp = await client.get("/api/v1/auth/me", headers=auth_headers)
        assert resp.status_code == 200

    async def test_me_without_token(self, client):
        with patch("routers.auth_shared.AUTH_REQUIRED", True):
            resp = await client.get("/api/v1/auth/me")
        # 401 when no credentials and AUTH_REQUIRED=true
        assert resp.status_code in (401, 403)


class TestLogoutEndpoint:
    """POST /api/v1/auth/logout"""

    async def test_logout_success(self, client, auth_headers, csrf_headers, mock_db_conn):
        headers = {**auth_headers, **csrf_headers}
        mock_db_conn.fetchval = AsyncMock(return_value=None)
        with patch("routers.auth_core.get_connection", return_value=mock_db_conn), \
             patch("routers.auth_core.ensure_auth_tables", new_callable=AsyncMock):
            resp = await client.post("/api/v1/auth/logout", headers=headers)
        assert resp.status_code == 200
        assert resp.json()["success"] is True
