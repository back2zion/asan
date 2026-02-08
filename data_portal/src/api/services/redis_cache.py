"""
Redis Cache Service — 공유 캐시 (멀티 워커 호환, 재시작 시 유지)
"""
import json
import time
import logging
from typing import Optional, Any

import redis.asyncio as aioredis

from core.config import settings

logger = logging.getLogger("redis_cache")

_pool: Optional[aioredis.Redis] = None


async def get_redis() -> aioredis.Redis:
    """Redis 연결 풀 (lazy singleton)"""
    global _pool
    if _pool is None:
        _pool = aioredis.Redis(
            host=settings.REDIS_HOST,
            port=settings.REDIS_PORT,
            password=settings.REDIS_PASSWORD,
            db=0,
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
            retry_on_timeout=True,
        )
    return _pool


async def cache_get(key: str) -> Optional[dict]:
    """캐시 조회 — 없으면 None"""
    try:
        r = await get_redis()
        raw = await r.get(key)
        if raw is None:
            return None
        return json.loads(raw)
    except Exception as e:
        logger.warning("redis cache_get(%s) failed: %s", key, e)
        return None


async def cache_set(key: str, value: Any, ttl: int = 300) -> bool:
    """캐시 저장 (TTL 초 단위)"""
    try:
        r = await get_redis()
        await r.set(key, json.dumps(value, default=str), ex=ttl)
        return True
    except Exception as e:
        logger.warning("redis cache_set(%s) failed: %s", key, e)
        return False


async def cache_delete(key: str) -> bool:
    """캐시 삭제"""
    try:
        r = await get_redis()
        await r.delete(key)
        return True
    except Exception as e:
        logger.warning("redis cache_delete(%s) failed: %s", key, e)
        return False


async def close_redis():
    """종료 시 정리"""
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None
