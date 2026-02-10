"""
Lakehouse helpers — constants, DB helpers, DuckDB singleton, Pydantic models, cache.
Extracted from lakehouse.py to keep only @router handlers in the main file.
"""
import os
import json
import time
import asyncio as _asyncio
from typing import Optional
from pathlib import Path

from pydantic import BaseModel, Field


# ── 경로 설정 ──
LAKEHOUSE_ROOT = Path(os.getenv("LAKEHOUSE_ROOT", "/tmp/idp_lakehouse"))
PARQUET_DIR = LAKEHOUSE_ROOT / "parquet"
SNAPSHOT_DIR = LAKEHOUSE_ROOT / "snapshots"
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


# ── DB 연결 ──
async def _pg_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()


async def _pg_rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)


# ── DuckDB 싱글톤 ──
_duckdb = None


def _get_duckdb():
    global _duckdb
    if _duckdb is None:
        try:
            import duckdb
            _duckdb = duckdb.connect(str(LAKEHOUSE_ROOT / "olap.duckdb"))
            _duckdb.execute("INSTALL postgres; LOAD postgres;")
            _duckdb.execute("INSTALL parquet; LOAD parquet;")
        except ImportError:
            return None
        except Exception:
            try:
                import duckdb
                _duckdb = duckdb.connect(str(LAKEHOUSE_ROOT / "olap.duckdb"))
            except Exception:
                return None
        # ATTACH OMOP CDM PostgreSQL database
        if _duckdb is not None:
            try:
                _duckdb.execute(
                    "ATTACH 'dbname=omop_cdm user=omopuser password=omop host=localhost port=5436' "
                    "AS omop (TYPE POSTGRES, READ_ONLY);"
                )
            except Exception:
                # Already attached or connection issue — non-fatal
                pass
    return _duckdb


# ── 테이블 초기화 ──
_tbl_ok = False


async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS lh_table_version (
            version_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            version_num INTEGER NOT NULL DEFAULT 1,
            snapshot_path VARCHAR(500),
            row_count BIGINT,
            size_bytes BIGINT,
            schema_hash VARCHAR(64),
            created_by VARCHAR(50) DEFAULT 'system',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            metadata JSONB DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_lh_ver_table ON lh_table_version(table_name, version_num);

        CREATE TABLE IF NOT EXISTS lh_parquet_file (
            file_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            file_path VARCHAR(500) NOT NULL,
            file_size BIGINT,
            row_count BIGINT,
            row_group_count INTEGER,
            compression VARCHAR(20) DEFAULT 'snappy',
            format_version VARCHAR(10) DEFAULT 'parquet-v2',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS lh_olap_query_log (
            query_id SERIAL PRIMARY KEY,
            engine VARCHAR(20) DEFAULT 'duckdb',
            sql_query TEXT NOT NULL,
            execution_ms DOUBLE PRECISION,
            row_count INTEGER,
            status VARCHAR(20) DEFAULT 'success',
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    _tbl_ok = True


# ── Pydantic ──

class OlapQueryRequest(BaseModel):
    sql: str = Field(..., min_length=5, max_length=5000)
    engine: str = Field(default="duckdb", pattern=r"^(duckdb|postgres)$")


class ParquetExportRequest(BaseModel):
    table_name: str = Field(..., pattern=r"^[a-z_][a-z0-9_]{0,99}$")
    sql: Optional[str] = None
    compression: str = Field(default="snappy", pattern=r"^(snappy|gzip|zstd|none)$")


class SnapshotRequest(BaseModel):
    table_name: str = Field(..., pattern=r"^[a-z_][a-z0-9_]{0,99}$")
    comment: Optional[str] = Field(None, max_length=500)


class DuckDBQueryRequest(BaseModel):
    sql: str = Field(..., description="DuckDB SQL query (read-only)")


# ── Overview 캐시 (11초 → 즉시) ──
_overview_cache: dict = {"data": None, "ts": 0}
_OVERVIEW_TTL = 300  # 5분


async def _fetch_overview_data():
    """실제 overview 데이터 조회 (내부용)"""
    conn = await _pg_conn()
    try:
        await _ensure_tables(conn)

        # OMOP CDM 테이블 통계
        omop_stats = await conn.fetch("""
            SELECT relname as table_name, n_live_tup as row_count
            FROM pg_stat_user_tables
            WHERE schemaname='public' AND n_live_tup > 0
            ORDER BY n_live_tup DESC LIMIT 20
        """)
        total_rows = sum(r["row_count"] for r in omop_stats)

        # Parquet 파일 통계
        pq_count = await conn.fetchval("SELECT COUNT(*) FROM lh_parquet_file") or 0
        pq_size = await conn.fetchval("SELECT COALESCE(SUM(file_size),0) FROM lh_parquet_file") or 0

        # 스냅샷 통계
        snap_count = await conn.fetchval("SELECT COUNT(*) FROM lh_table_version") or 0
        tables_versioned = await conn.fetchval("SELECT COUNT(DISTINCT table_name) FROM lh_table_version") or 0

        # OLAP 쿼리 통계
        olap_total = await conn.fetchval("SELECT COUNT(*) FROM lh_olap_query_log") or 0
        olap_avg_ms = await conn.fetchval("SELECT AVG(execution_ms) FROM lh_olap_query_log WHERE status='success'")

        # DuckDB 상태 (이미 초기화된 경우만 — 초기화 블로킹 방지)
        duckdb_ok = _duckdb is not None

        return {
            "architecture": "Lakehouse (PostgreSQL OLTP + DuckDB OLAP + Parquet Storage)",
            "omop_cdm": {
                "total_rows": total_rows,
                "table_count": len(omop_stats),
                "tables": [dict(r) for r in omop_stats[:10]],
            },
            "parquet_storage": {
                "file_count": pq_count,
                "total_size_mb": round(pq_size / 1024 / 1024, 2),
            },
            "table_versioning": {
                "snapshot_count": snap_count,
                "tables_versioned": tables_versioned,
            },
            "olap_engine": {
                "duckdb_available": duckdb_ok,
                "total_queries": olap_total,
                "avg_execution_ms": round(olap_avg_ms, 2) if olap_avg_ms else 0,
            },
        }
    finally:
        await _pg_rel(conn)


async def _refresh_overview_cache():
    """백그라운드에서 overview 캐시 갱신"""
    try:
        data = await _fetch_overview_data()
        _overview_cache["data"] = data
        _overview_cache["ts"] = time.time()
    except Exception as e:
        print(f"[lakehouse-overview] refresh failed: {e}")
