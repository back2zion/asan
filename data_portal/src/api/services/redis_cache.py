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


def cached(key_prefix: str, ttl: int = 300):
    """Redis 캐시 데코레이터 — FastAPI GET 엔드포인트용.

    사용법::

        @router.get("/stats")
        @cached("my-stats", ttl=300)
        async def get_stats():
            ...

    쿼리 파라미터가 있으면 캐시 키에 자동 포함.
    Redis 장애 시 캐시 없이 원본 함수 실행.
    """
    import functools
    import inspect

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # 캐시 키 생성: prefix + 파라미터
            parts = [key_prefix]
            if kwargs:
                sorted_params = sorted(
                    (k, str(v)) for k, v in kwargs.items()
                    if v is not None and k not in ("request", "response")
                )
                if sorted_params:
                    parts.append(":".join(f"{k}={v}" for k, v in sorted_params))
            key = ":".join(parts)

            # 캐시 조회
            hit = await cache_get(key)
            if hit is not None:
                return hit

            # 원본 실행
            result = await func(*args, **kwargs)

            # 결과 캐시 (dict / list / Pydantic 등)
            try:
                data = result
                if hasattr(result, "model_dump"):
                    data = result.model_dump()
                elif hasattr(result, "dict"):
                    data = result.dict()
                await cache_set(key, data, ttl)
            except Exception:
                pass  # 캐시 실패 무시

            return result
        return wrapper
    return decorator


async def close_redis():
    """종료 시 정리"""
    global _pool
    if _pool is not None:
        await _pool.aclose()
        _pool = None
