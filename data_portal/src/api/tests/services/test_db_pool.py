"""
Phase 3: DB Connection Pool 단위 테스트
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock


class TestInitPool:
    """init_pool — 연결 풀 초기화"""

    async def test_init_creates_pool(self):
        mock_pool = AsyncMock()
        with patch("asyncpg.create_pool", new_callable=AsyncMock, return_value=mock_pool):
            import services.db_pool as mod
            mod._pool = None
            result = await mod.init_pool()
        assert result is mock_pool

    async def test_init_idempotent(self):
        """이미 풀이 있으면 재생성하지 않음"""
        existing_pool = AsyncMock()
        import services.db_pool as mod
        mod._pool = existing_pool
        result = await mod.init_pool()
        assert result is existing_pool
        mod._pool = None  # cleanup


class TestGetPool:
    """get_pool — lazy init"""

    async def test_get_pool_lazy_init(self):
        mock_pool = AsyncMock()
        import services.db_pool as mod
        mod._pool = None
        with patch.object(mod, "init_pool", new_callable=AsyncMock) as mock_init:
            mock_init.return_value = mock_pool
            # init_pool sets _pool, so simulate that
            async def side_effect():
                mod._pool = mock_pool
                return mock_pool
            mock_init.side_effect = side_effect
            result = await mod.get_pool()
        assert result is mock_pool
        mod._pool = None  # cleanup

    async def test_get_pool_returns_existing(self):
        existing = AsyncMock()
        import services.db_pool as mod
        mod._pool = existing
        result = await mod.get_pool()
        assert result is existing
        mod._pool = None  # cleanup


class TestClosePool:
    """close_pool — 종료"""

    async def test_close_pool(self):
        mock_pool = AsyncMock()
        import services.db_pool as mod
        mod._pool = mock_pool
        await mod.close_pool()
        mock_pool.close.assert_called_once()
        assert mod._pool is None

    async def test_close_pool_when_none(self):
        import services.db_pool as mod
        mod._pool = None
        await mod.close_pool()  # 크래시 없이 완료
        assert mod._pool is None
