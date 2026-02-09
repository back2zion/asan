"""
가. 데이터 레이크하우스 아키텍처 — DuckDB OLAP + Parquet 스토리지 + 테이블 버전 관리
RFP 요구: 레이크하우스 아키텍처 기반 통합 데이터 플랫폼
- DuckDB: In-process OLAP 엔진 (분석 쿼리 가속)
- Parquet: 컬럼형 스토리지 (데이터 반출/아카이브)
- 테이블 버전: 스냅샷 기반 시간여행 쿼리
"""
import os
import json
import time
import tempfile
from datetime import datetime
from typing import Optional, List
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/lakehouse", tags=["Lakehouse"])

# Sub-router: Lakehouse Quality (DQ + 스키마 진화)
from .lakehouse_quality import router as quality_router
router.include_router(quality_router)

# ── Overview 캐시 (11초 → 즉시) ──
import asyncio as _asyncio
_overview_cache: dict = {"data": None, "ts": 0}
_OVERVIEW_TTL = 300  # 5분

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


# ══════════════════════════════════════════
# DuckDB OLAP 쿼리
# ══════════════════════════════════════════

@router.post("/olap/query")
async def olap_query(body: OlapQueryRequest):
    """DuckDB OLAP 엔진으로 분석 쿼리 실행 (읽기 전용)"""
    sql_upper = body.sql.upper().strip()
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        raise HTTPException(400, "SELECT/WITH 쿼리만 허용됩니다")
    for kw in ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]:
        import re
        if re.search(rf'\b{kw}\b', sql_upper):
            raise HTTPException(400, f"금지 키워드: {kw}")

    conn = await _pg_conn()
    try:
        await _ensure_tables(conn)
        t0 = time.time()

        if body.engine == "duckdb":
            db = _get_duckdb()
            if db is None:
                raise HTTPException(503, "DuckDB 엔진을 사용할 수 없습니다 (pip install duckdb)")
            try:
                result = db.execute(body.sql).fetchall()
                columns = [desc[0] for desc in db.description]
                rows = [list(r) for r in result[:1000]]
            except Exception as e:
                ms = (time.time() - t0) * 1000
                await conn.execute(
                    "INSERT INTO lh_olap_query_log (engine,sql_query,execution_ms,status,error_message) VALUES ($1,$2,$3,$4,$5)",
                    "duckdb", body.sql, ms, "error", str(e))
                raise HTTPException(400, f"DuckDB 쿼리 오류: {e}")
        else:
            rows_raw = await conn.fetch(body.sql + (" LIMIT 1000" if "LIMIT" not in sql_upper else ""))
            columns = list(rows_raw[0].keys()) if rows_raw else []
            rows = [list(r.values()) for r in rows_raw]

        ms = (time.time() - t0) * 1000
        await conn.execute(
            "INSERT INTO lh_olap_query_log (engine,sql_query,execution_ms,row_count,status) VALUES ($1,$2,$3,$4,$5)",
            body.engine, body.sql, ms, len(rows), "success")

        return {
            "engine": body.engine,
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
            "execution_ms": round(ms, 2),
        }
    finally:
        await _pg_rel(conn)


@router.get("/olap/status")
async def olap_status():
    """OLAP 엔진 상태"""
    db = _get_duckdb()
    duckdb_ok = db is not None
    duckdb_version = None
    duckdb_tables = []
    if duckdb_ok:
        try:
            duckdb_version = db.execute("SELECT version()").fetchone()[0]
            tbls = db.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='main'").fetchall()
            duckdb_tables = [t[0] for t in tbls]
        except Exception:
            pass

    parquet_files = list(PARQUET_DIR.glob("*.parquet"))
    return {
        "duckdb": {"available": duckdb_ok, "version": duckdb_version, "tables": duckdb_tables},
        "parquet": {"directory": str(PARQUET_DIR), "file_count": len(parquet_files),
                    "total_size_mb": round(sum(f.stat().st_size for f in parquet_files) / 1024 / 1024, 2)},
        "lakehouse_root": str(LAKEHOUSE_ROOT),
    }


# ══════════════════════════════════════════
# Parquet 스토리지
# ══════════════════════════════════════════

@router.post("/parquet/export")
async def export_to_parquet(body: ParquetExportRequest):
    """OMOP CDM 테이블을 Parquet 파일로 변환 저장"""
    conn = await _pg_conn()
    try:
        await _ensure_tables(conn)
        db = _get_duckdb()

        sql = body.sql or f"SELECT * FROM {body.table_name}"
        sql_upper = sql.upper()
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            raise HTTPException(400, "SELECT 쿼리만 허용됩니다")

        # 행 수 확인
        count = await conn.fetchval(f"SELECT COUNT(*) FROM ({sql} LIMIT 500000) t")

        fname = f"{body.table_name}_{int(time.time())}.parquet"
        fpath = PARQUET_DIR / fname

        if db:
            # DuckDB로 직접 Parquet 생성 (PostgreSQL 연동)
            pg_port = int(os.getenv("OMOP_DB_PORT", "5436"))
            pg_user = os.getenv("OMOP_DB_USER", "omopuser")
            pg_pass = os.getenv("OMOP_DB_PASSWORD", "omop")
            pg_db = os.getenv("OMOP_DB_NAME", "omop_cdm")
            try:
                db.execute(f"ATTACH IF NOT EXISTS 'dbname={pg_db} host=localhost port={pg_port} user={pg_user} password={pg_pass}' AS pg (TYPE POSTGRES, READ_ONLY)")
            except Exception:
                pass
            try:
                db.execute(f"COPY ({sql}) TO '{fpath}' (FORMAT PARQUET, COMPRESSION '{body.compression}')")
            except Exception as e:
                # DuckDB postgres 연동 실패 시 fallback
                raise HTTPException(400, f"Parquet 변환 실패: {e}")
        else:
            # DuckDB 없으면 Python으로 Parquet 생성
            try:
                import pyarrow as pa
                import pyarrow.parquet as pq
                rows = await conn.fetch(sql + " LIMIT 500000")
                if not rows:
                    raise HTTPException(404, "데이터 없음")
                columns = list(rows[0].keys())
                data = {col: [r[col] for r in rows] for col in columns}
                table = pa.table(data)
                pq.write_table(table, str(fpath), compression=body.compression if body.compression != "none" else None)
            except ImportError:
                raise HTTPException(503, "pyarrow 또는 duckdb가 필요합니다")

        file_size = fpath.stat().st_size
        await conn.execute("""
            INSERT INTO lh_parquet_file (table_name, file_path, file_size, row_count, compression)
            VALUES ($1,$2,$3,$4,$5)
        """, body.table_name, str(fpath), file_size, count, body.compression)

        return {
            "table_name": body.table_name,
            "file_path": str(fpath),
            "file_size_mb": round(file_size / 1024 / 1024, 2),
            "row_count": count,
            "compression": body.compression,
        }
    finally:
        await _pg_rel(conn)


@router.get("/parquet/files")
async def list_parquet_files():
    """Parquet 파일 목록"""
    conn = await _pg_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("SELECT * FROM lh_parquet_file ORDER BY created_at DESC LIMIT 100")
        return [dict(r) for r in rows]
    finally:
        await _pg_rel(conn)


@router.post("/parquet/register-in-duckdb")
async def register_parquet_in_duckdb(file_id: int):
    """Parquet 파일을 DuckDB 테이블로 등록"""
    conn = await _pg_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow("SELECT * FROM lh_parquet_file WHERE file_id=$1", file_id)
        if not row:
            raise HTTPException(404, "파일을 찾을 수 없습니다")
        db = _get_duckdb()
        if not db:
            raise HTTPException(503, "DuckDB 사용 불가")
        tname = f"pq_{row['table_name']}"
        db.execute(f"CREATE OR REPLACE TABLE {tname} AS SELECT * FROM read_parquet('{row['file_path']}')")
        cnt = db.execute(f"SELECT COUNT(*) FROM {tname}").fetchone()[0]
        return {"duckdb_table": tname, "row_count": cnt, "source_file": row["file_path"]}
    finally:
        await _pg_rel(conn)


# ══════════════════════════════════════════
# 테이블 버전 (스냅샷)
# ══════════════════════════════════════════

@router.post("/snapshots")
async def create_snapshot(body: SnapshotRequest):
    """OMOP CDM 테이블 스냅샷 생성 (Parquet 아카이브)"""
    conn = await _pg_conn()
    try:
        await _ensure_tables(conn)
        # 현재 버전 확인
        cur_ver = await conn.fetchval(
            "SELECT COALESCE(MAX(version_num), 0) FROM lh_table_version WHERE table_name=$1",
            body.table_name)
        new_ver = cur_ver + 1

        # 행 수
        try:
            row_count = await conn.fetchval(f"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname=$1", body.table_name)
        except Exception:
            row_count = 0

        # 스키마 해시
        import hashlib
        cols = await conn.fetch(
            "SELECT column_name, data_type FROM information_schema.columns WHERE table_name=$1 ORDER BY ordinal_position",
            body.table_name)
        schema_str = json.dumps([(r["column_name"], r["data_type"]) for r in cols])
        schema_hash = hashlib.sha256(schema_str.encode()).hexdigest()[:16]

        # Parquet 스냅샷 저장
        snap_path = str(SNAPSHOT_DIR / f"{body.table_name}_v{new_ver}_{int(time.time())}.parquet")
        db = _get_duckdb()
        size_bytes = 0
        if db:
            try:
                pg_port = int(os.getenv("OMOP_DB_PORT", "5436"))
                pg_user = os.getenv("OMOP_DB_USER", "omopuser")
                pg_pass = os.getenv("OMOP_DB_PASSWORD", "omop")
                pg_db = os.getenv("OMOP_DB_NAME", "omop_cdm")
                db.execute(f"ATTACH IF NOT EXISTS 'dbname={pg_db} host=localhost port={pg_port} user={pg_user} password={pg_pass}' AS pg (TYPE POSTGRES, READ_ONLY)")
                sample_sql = f"SELECT * FROM pg.public.{body.table_name} LIMIT 100000"
                db.execute(f"COPY ({sample_sql}) TO '{snap_path}' (FORMAT PARQUET, COMPRESSION 'zstd')")
                size_bytes = Path(snap_path).stat().st_size
            except Exception:
                snap_path = None
        else:
            snap_path = None

        meta = {"comment": body.comment, "schema": schema_str[:500]}
        vid = await conn.fetchval("""
            INSERT INTO lh_table_version (table_name, version_num, snapshot_path, row_count, size_bytes, schema_hash, metadata)
            VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb) RETURNING version_id
        """, body.table_name, new_ver, snap_path, row_count, size_bytes, schema_hash, json.dumps(meta))

        return {
            "version_id": vid,
            "table_name": body.table_name,
            "version": new_ver,
            "row_count": row_count,
            "schema_hash": schema_hash,
            "snapshot_path": snap_path,
            "size_mb": round(size_bytes / 1024 / 1024, 2) if size_bytes else 0,
        }
    finally:
        await _pg_rel(conn)


@router.get("/snapshots")
async def list_snapshots(table_name: Optional[str] = None):
    """테이블 스냅샷 목록"""
    conn = await _pg_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT * FROM lh_table_version"
        params = []
        if table_name:
            q += " WHERE table_name = $1"
            params.append(table_name)
        q += " ORDER BY table_name, version_num DESC"
        rows = await conn.fetch(q, *params)
        return [dict(r) for r in rows]
    finally:
        await _pg_rel(conn)


@router.get("/snapshots/{table_name}/diff")
async def snapshot_diff(table_name: str, v1: int = Query(...), v2: int = Query(...)):
    """두 스냅샷 간 스키마 diff"""
    conn = await _pg_conn()
    try:
        await _ensure_tables(conn)
        snap1 = await conn.fetchrow(
            "SELECT * FROM lh_table_version WHERE table_name=$1 AND version_num=$2", table_name, v1)
        snap2 = await conn.fetchrow(
            "SELECT * FROM lh_table_version WHERE table_name=$1 AND version_num=$2", table_name, v2)
        if not snap1 or not snap2:
            raise HTTPException(404, "스냅샷을 찾을 수 없습니다")

        schema_changed = snap1["schema_hash"] != snap2["schema_hash"]
        row_diff = (snap2["row_count"] or 0) - (snap1["row_count"] or 0)

        return {
            "table_name": table_name,
            "v1": v1, "v2": v2,
            "schema_changed": schema_changed,
            "v1_schema_hash": snap1["schema_hash"],
            "v2_schema_hash": snap2["schema_hash"],
            "v1_rows": snap1["row_count"],
            "v2_rows": snap2["row_count"],
            "row_diff": row_diff,
        }
    finally:
        await _pg_rel(conn)


# ══════════════════════════════════════════
# 레이크하우스 전체 통계
# ══════════════════════════════════════════

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


@router.get("/overview")
async def lakehouse_overview():
    """레이크하우스 전체 현황 (5분 캐시, stale-while-revalidate)"""
    now = time.time()
    cached = _overview_cache["data"]
    cached_ts = _overview_cache["ts"]

    # 1) 캐시 fresh → 즉시 반환
    if cached and (now - cached_ts) < _OVERVIEW_TTL:
        return cached

    # 2) 캐시 stale → 기존 반환 + 백그라운드 갱신
    if cached:
        _asyncio.create_task(_refresh_overview_cache())
        return cached

    # 3) 최초 호출 → 동기 fetch
    data = await _fetch_overview_data()
    _overview_cache["data"] = data
    _overview_cache["ts"] = now
    return data
