"""
Phase 3: Redis Cache 서비스 단위 테스트
"""
import pytest
from unittest.mock import AsyncMock, patch
import json


class TestCacheGet:
    """cache_get — Redis 조회"""

    async def test_get_existing_key(self, mock_redis):
        mock_redis.get = AsyncMock(return_value=json.dumps({"count": 42}))
        with patch("services.redis_cache._pool", mock_redis), \
             patch("services.redis_cache.get_redis", return_value=mock_redis):
            from services.redis_cache import cache_get
            result = await cache_get("test-key")
        assert result == {"count": 42}

    async def test_get_missing_key(self, mock_redis):
        mock_redis.get = AsyncMock(return_value=None)
        with patch("services.redis_cache._pool", mock_redis), \
             patch("services.redis_cache.get_redis", return_value=mock_redis):
            from services.redis_cache import cache_get
            result = await cache_get("nonexistent")
        assert result is None

    async def test_get_connection_failure_returns_none(self):
        """Redis 연결 실패 시 graceful degradation — None 반환, 크래시 없음"""
        mock_r = AsyncMock()
        mock_r.get = AsyncMock(side_effect=ConnectionError("Redis down"))
        with patch("services.redis_cache.get_redis", return_value=mock_r):
            from services.redis_cache import cache_get
            result = await cache_get("any")
        assert result is None


class TestCacheSet:
    """cache_set — Redis 저장"""

    async def test_set_value(self, mock_redis):
        with patch("services.redis_cache.get_redis", return_value=mock_redis):
            from services.redis_cache import cache_set
            result = await cache_set("key", {"data": "value"}, ttl=60)
        assert result is True
        mock_redis.set.assert_called_once()

    async def test_set_connection_failure_returns_false(self):
        """Redis 연결 실패 시 graceful degradation"""
        mock_r = AsyncMock()
        mock_r.set = AsyncMock(side_effect=ConnectionError("Redis down"))
        with patch("services.redis_cache.get_redis", return_value=mock_r):
            from services.redis_cache import cache_set
            result = await cache_set("key", "val")
        assert result is False


class TestCacheDelete:
    """cache_delete — Redis 삭제"""

    async def test_delete_key(self, mock_redis):
        with patch("services.redis_cache.get_redis", return_value=mock_redis):
            from services.redis_cache import cache_delete
            result = await cache_delete("key")
        assert result is True
        mock_redis.delete.assert_called_once_with("key")

    async def test_delete_failure_returns_false(self):
        """Redis 연결 실패 시 graceful degradation"""
        mock_r = AsyncMock()
        mock_r.delete = AsyncMock(side_effect=ConnectionError("Redis down"))
        with patch("services.redis_cache.get_redis", return_value=mock_r):
            from services.redis_cache import cache_delete
            result = await cache_delete("key")
        assert result is False


class TestCloseRedis:
    """close_redis — 종료 시 정리"""

    async def test_close_when_pool_exists(self, mock_redis):
        import services.redis_cache as mod
        mod._pool = mock_redis
        await mod.close_redis()
        mock_redis.aclose.assert_called_once()
        assert mod._pool is None

    async def test_close_when_pool_none(self):
        import services.redis_cache as mod
        mod._pool = None
        await mod.close_redis()  # 크래시 없이 완료
        assert mod._pool is None
