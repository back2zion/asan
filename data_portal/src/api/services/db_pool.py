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


async def execute_with_logging(conn, query: str, *args, timeout: float = 30.0):
    """슬로우 쿼리 로깅 래퍼 — threshold 1000ms"""
    import time as _time
    start = _time.monotonic()
    try:
        result = await conn.fetch(query, *args, timeout=timeout)
        elapsed_ms = (_time.monotonic() - start) * 1000
        if elapsed_ms > 1000:
            logger.warning(f"SLOW QUERY ({elapsed_ms:.0f}ms): {query[:200]}")
            try:
                await conn.execute(
                    """INSERT INTO slow_query_log (query_text, duration_ms, params_summary, created_at)
                       VALUES ($1, $2, $3, NOW())""",
                    query[:2000], elapsed_ms, str(args)[:500],
                )
            except Exception:
                pass  # 테이블 미존재 시 무시
        return result
    except Exception as e:
        elapsed_ms = (_time.monotonic() - start) * 1000
        logger.error(f"QUERY ERROR ({elapsed_ms:.0f}ms): {query[:200]} — {e}")
        raise


async def ensure_slow_query_table():
    """slow_query_log 테이블 생성 (lifespan에서 호출)"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS slow_query_log (
                id SERIAL PRIMARY KEY,
                query_text TEXT NOT NULL,
                duration_ms DOUBLE PRECISION NOT NULL,
                params_summary TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_slow_query_ts ON slow_query_log(created_at);
        """)
