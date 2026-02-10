"""
동의 관리 API 테스트 — /api/v1/consent/*
Mock 패턴 B: consent_mgmt._get_conn() / _rel() / _tbl_ok
"""
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest


class _DictRow(dict):
    """asyncpg Record처럼 dict(row) 가능한 mock row"""

    def __getitem__(self, key):
        return super().__getitem__(key)


def _policy_row(policy_id=1, is_required=True):
    return _DictRow(
        policy_id=policy_id,
        policy_code="RESEARCH_DATA",
        title="연구용 데이터 활용 동의",
        description="연구 목적 데이터 활용 동의",
        purpose="의료 연구",
        data_items="진단정보, 처방정보",
        retention_days=1095,
        is_required=is_required,
        version="1.0",
        is_active=True,
        created_at=datetime.now(),
        updated_at=datetime.now(),
    )


def _record_row(user_id="user1", policy_id=1, agreed=True):
    return _DictRow(
        record_id=1,
        user_id=user_id,
        policy_id=policy_id,
        agreed=agreed,
        ip_address="127.0.0.1",
        agreed_at=datetime.now(),
        expires_at=datetime.now() + timedelta(days=365),
        withdrawn_at=None,
        withdraw_reason=None,
    )


def _consent_record_with_policy(user_id="user1", policy_id=1, agreed=True):
    return _DictRow(
        record_id=1,
        user_id=user_id,
        policy_id=policy_id,
        agreed=agreed,
        ip_address="127.0.0.1",
        agreed_at=datetime.now(),
        expires_at=datetime.now() + timedelta(days=365),
        withdrawn_at=None,
        withdraw_reason=None,
        policy_code="RESEARCH_DATA",
        title="연구 동의",
        purpose="연구",
        is_required=True,
        policy_version="1.0",
    )


def _consent_patches(mock_conn):
    return (
        patch("routers.consent_mgmt._get_conn", AsyncMock(return_value=mock_conn)),
        patch("routers.consent_mgmt._rel", AsyncMock()),
        patch("routers.consent_mgmt._tbl_ok", True),
    )


# ══════════════════════════════════════════════════════════
#  GET /consent/policies
# ══════════════════════════════════════════════════════════

class TestConsentPolicies:

    async def test_list_policies(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[_policy_row()])

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.get("/api/v1/consent/policies", headers=csrf_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_list_policies_active_only_false(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[])

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.get(
                "/api/v1/consent/policies?active_only=false",
                headers=csrf_headers,
            )
        assert resp.status_code == 200

    async def test_get_policy_by_id(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchrow = AsyncMock(return_value=_policy_row())
        mock_db_conn.fetchval = AsyncMock(return_value=10)

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.get("/api/v1/consent/policies/1", headers=csrf_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["agreed_count"] == 10

    async def test_get_policy_not_found(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchrow = AsyncMock(return_value=None)

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.get("/api/v1/consent/policies/999", headers=csrf_headers)
        assert resp.status_code == 404


# ══════════════════════════════════════════════════════════
#  POST /consent/agree
# ══════════════════════════════════════════════════════════

class TestConsentAgree:

    async def test_agree_success(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchrow = AsyncMock(side_effect=[
            _policy_row(),    # policy lookup
            _record_row(),    # INSERT RETURNING
        ])
        mock_db_conn.execute = AsyncMock()

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.post(
                "/api/v1/consent/agree",
                json={"user_id": "user1", "policy_id": 1, "agreed": True},
                headers=csrf_headers,
            )
        assert resp.status_code == 200

    async def test_agree_invalid_policy(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchrow = AsyncMock(return_value=None)

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.post(
                "/api/v1/consent/agree",
                json={"user_id": "user1", "policy_id": 999, "agreed": True},
                headers=csrf_headers,
            )
        assert resp.status_code == 404


# ══════════════════════════════════════════════════════════
#  POST /consent/agree-batch
# ══════════════════════════════════════════════════════════

class TestConsentAgreeBatch:

    async def test_batch_agree(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchrow = AsyncMock(side_effect=[
            _policy_row(1),
            _record_row("user1", 1),
            _policy_row(2),
            _record_row("user1", 2),
        ])
        mock_db_conn.execute = AsyncMock()

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.post(
                "/api/v1/consent/agree-batch",
                json={"user_id": "user1", "policy_ids": [1, 2]},
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["agreed_count"] == 2


# ══════════════════════════════════════════════════════════
#  POST /consent/withdraw
# ══════════════════════════════════════════════════════════

class TestConsentWithdraw:

    async def test_withdraw_optional_policy(self, client, csrf_headers, mock_db_conn):
        record = _record_row()
        policy = _policy_row(is_required=False)
        withdrawn_row = _record_row()

        mock_db_conn.fetchrow = AsyncMock(side_effect=[record, policy, withdrawn_row])
        mock_db_conn.execute = AsyncMock()

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.post(
                "/api/v1/consent/withdraw",
                json={"user_id": "user1", "policy_id": 1, "reason": "불필요"},
                headers=csrf_headers,
            )
        assert resp.status_code == 200

    async def test_withdraw_required_policy_blocked(self, client, csrf_headers, mock_db_conn):
        record = _record_row()
        policy = _policy_row(is_required=True)

        mock_db_conn.fetchrow = AsyncMock(side_effect=[record, policy])

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.post(
                "/api/v1/consent/withdraw",
                json={"user_id": "user1", "policy_id": 1},
                headers=csrf_headers,
            )
        assert resp.status_code == 400
        assert "필수" in resp.json()["detail"]

    async def test_withdraw_no_existing_record(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetchrow = AsyncMock(return_value=None)

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.post(
                "/api/v1/consent/withdraw",
                json={"user_id": "user1", "policy_id": 1},
                headers=csrf_headers,
            )
        assert resp.status_code == 404


# ══════════════════════════════════════════════════════════
#  GET /consent/user/{user_id}
# ══════════════════════════════════════════════════════════

class TestConsentUser:

    async def test_get_user_consents(self, client, csrf_headers, mock_db_conn):
        consent_row = _consent_record_with_policy()
        policy_row = _policy_row()

        mock_db_conn.fetch = AsyncMock(side_effect=[
            [consent_row],   # consent records
            [policy_row],    # active policies
        ])

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.get("/api/v1/consent/user/user1", headers=csrf_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["user_id"] == "user1"
        assert "consents" in data
        assert "policies" in data


# ══════════════════════════════════════════════════════════
#  GET /consent/stats, /consent/audit-log
# ══════════════════════════════════════════════════════════

class TestConsentStats:

    async def test_stats(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[_policy_row()])
        mock_db_conn.fetchval = AsyncMock(return_value=0)

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.get("/api/v1/consent/stats", headers=csrf_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert "total_users" in data
        assert "policies" in data

    async def test_audit_log(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[])

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.get("/api/v1/consent/audit-log", headers=csrf_headers)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    async def test_audit_log_with_filter(self, client, csrf_headers, mock_db_conn):
        mock_db_conn.fetch = AsyncMock(return_value=[])

        p1, p2, p3 = _consent_patches(mock_db_conn)
        with p1, p2, p3:
            resp = await client.get(
                "/api/v1/consent/audit-log?user_id=user1&action=AGREE",
                headers=csrf_headers,
            )
        assert resp.status_code == 200
