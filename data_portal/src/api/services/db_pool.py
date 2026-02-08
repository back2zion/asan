"""
asyncpg Connection Pool — OMOP CDM DB 연결 풀
모든 라우터에서 공유. 서버 시작/종료 시 자동 관리.
"""
import os
import logging
from typing import Optional

import asyncpg

logger = logging.getLogger("db_pool")

_pool: Optional[asyncpg.Pool] = None

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}


async def init_pool() -> asyncpg.Pool:
    """연결 풀 초기화 (lifespan startup에서 호출)"""
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            **OMOP_DB_CONFIG,
            min_size=2,
            max_size=10,
            max_inactive_connection_lifetime=300,
            command_timeout=30,
        )
        logger.info("OMOP DB connection pool created (min=2, max=10)")
    return _pool


async def get_pool() -> asyncpg.Pool:
    """연결 풀 가져오기 (lazy init)"""
    global _pool
    if _pool is None:
        await init_pool()
    return _pool


async def close_pool():
    """연결 풀 종료 (lifespan shutdown에서 호출)"""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("OMOP DB connection pool closed")
