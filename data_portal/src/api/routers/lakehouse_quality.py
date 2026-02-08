"""
레이크하우스 DQ + 스키마 진화
테이블 프로파일링, 스키마 변경 이력, DQ 검증
"""
import json
import hashlib
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/quality", tags=["LakehouseQuality"])

async def _get_conn():
    from services.db_pool import get_pool
    return await (await get_pool()).acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    await (await get_pool()).release(conn)

_tbl_ok = False

async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS lh_data_profile (
            profile_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            column_name VARCHAR(100),
            row_count BIGINT,
            null_count BIGINT,
            null_percent DOUBLE PRECISION,
            distinct_count BIGINT,
            min_value TEXT,
            max_value TEXT,
            avg_value DOUBLE PRECISION,
            data_type VARCHAR(50),
            profile_metadata JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS lh_schema_evolution (
            evolution_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            change_type VARCHAR(30) NOT NULL,
            column_name VARCHAR(100),
            old_definition TEXT,
            new_definition TEXT,
            is_compatible BOOLEAN DEFAULT TRUE,
            applied BOOLEAN DEFAULT FALSE,
            applied_at TIMESTAMPTZ,
            created_by VARCHAR(50) DEFAULT 'system',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_lh_profile_table ON lh_data_profile(table_name);
        CREATE INDEX IF NOT EXISTS idx_lh_schema_table ON lh_schema_evolution(table_name);
    """)
    _tbl_ok = True


class SchemaEvolveRequest(BaseModel):
    table_name: str = Field(..., pattern=r"^[a-z_][a-z0-9_]{0,99}$")
    change_type: str = Field(..., pattern=r"^(add_column|rename_column|change_type|drop_column)$")
    column_name: str = Field(..., pattern=r"^[a-z_][a-z0-9_]{0,99}$")
    new_definition: Optional[str] = Field(None, max_length=500)
    old_definition: Optional[str] = Field(None, max_length=500)


# OMOP CDM 테이블 목록 (프로파일링 허용)
ALLOWED_TABLES = {
    "person", "condition_occurrence", "drug_exposure", "measurement",
    "visit_occurrence", "procedure_occurrence", "observation", "death",
    "condition_era", "drug_era", "cost", "payer_plan_period",
    "device_exposure", "observation_period", "care_site", "provider", "location",
}


@router.post("/profile/{table}")
async def profile_table(table: str):
    """테이블 프로파일링 (null%, distinct, min/max, type)"""
    if table not in ALLOWED_TABLES:
        raise HTTPException(400, f"프로파일링 허용 테이블: {', '.join(sorted(ALLOWED_TABLES))}")

    conn = await _get_conn()
    try:
        await _ensure_tables(conn)

        # 컬럼 정보
        cols = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name=$1 AND table_schema='public'
            ORDER BY ordinal_position
        """, table)
        if not cols:
            raise HTTPException(404, f"테이블 '{table}'을 찾을 수 없습니다")

        total_rows = await conn.fetchval(
            "SELECT n_live_tup FROM pg_stat_user_tables WHERE relname=$1", table) or 0

        profiles = []
        for col in cols[:30]:  # 최대 30 컬럼
            cname = col["column_name"]
            dtype = col["data_type"]

            null_count = await conn.fetchval(
                f"SELECT COUNT(*) FROM {table} WHERE {cname} IS NULL") or 0
            distinct_count = await conn.fetchval(
                f"SELECT COUNT(DISTINCT {cname}) FROM (SELECT {cname} FROM {table} LIMIT 100000) t") or 0

            min_val = max_val = avg_val = None
            if dtype in ("integer", "bigint", "numeric", "double precision", "real", "smallint"):
                row = await conn.fetchrow(
                    f"SELECT MIN({cname})::text, MAX({cname})::text, AVG({cname}::numeric) FROM {table}")
                if row:
                    min_val, max_val, avg_val = row[0], row[1], float(row[2]) if row[2] else None
            elif dtype in ("date", "timestamp without time zone", "timestamp with time zone"):
                row = await conn.fetchrow(
                    f"SELECT MIN({cname})::text, MAX({cname})::text FROM {table}")
                if row:
                    min_val, max_val = row[0], row[1]

            null_pct = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0

            pid = await conn.fetchval("""
                INSERT INTO lh_data_profile (table_name, column_name, row_count, null_count, null_percent,
                    distinct_count, min_value, max_value, avg_value, data_type)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING profile_id
            """, table, cname, total_rows, null_count, null_pct,
               distinct_count, min_val, max_val, avg_val, dtype)

            profiles.append({
                "profile_id": pid,
                "column": cname,
                "type": dtype,
                "null_percent": null_pct,
                "distinct_count": distinct_count,
                "min": min_val,
                "max": max_val,
                "avg": round(avg_val, 4) if avg_val else None,
            })

        return {
            "table": table,
            "total_rows": total_rows,
            "column_count": len(profiles),
            "profiles": profiles,
        }
    finally:
        await _rel(conn)


@router.get("/profiles")
async def list_profiles(table_name: Optional[str] = None, limit: int = Query(50, le=200)):
    """프로파일 이력"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT DISTINCT ON (table_name, column_name) * FROM lh_data_profile"
        params, idx = [], 1
        if table_name:
            q += f" WHERE table_name = ${idx}"; params.append(table_name); idx += 1
        q += " ORDER BY table_name, column_name, created_at DESC"
        q = f"SELECT * FROM ({q}) t LIMIT ${idx}"
        params.append(limit)
        rows = await conn.fetch(q, *params)
        return {"profiles": [dict(r) for r in rows], "total": len(rows)}
    finally:
        await _rel(conn)


@router.post("/schema/evolve")
async def evolve_schema(body: SchemaEvolveRequest):
    """스키마 진화 (호환성 체크 + 기록)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)

        # 현재 컬럼 확인
        existing = await conn.fetch("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_name=$1 AND table_schema='public'
        """, body.table_name)
        existing_cols = {r["column_name"]: r["data_type"] for r in existing}

        is_compatible = True
        detail = ""

        if body.change_type == "add_column":
            if body.column_name in existing_cols:
                is_compatible = False
                detail = f"컬럼 '{body.column_name}'이 이미 존재합니다"
            else:
                detail = f"새 컬럼 '{body.column_name}' 추가 가능 (호환)"
        elif body.change_type == "drop_column":
            if body.column_name not in existing_cols:
                is_compatible = False
                detail = f"컬럼 '{body.column_name}'이 존재하지 않습니다"
            else:
                is_compatible = False  # 컬럼 삭제는 비호환 변경
                detail = f"컬럼 삭제는 비호환 변경입니다 (downstream 영향 확인 필요)"
        elif body.change_type == "rename_column":
            if body.column_name not in existing_cols:
                is_compatible = False
                detail = f"컬럼 '{body.column_name}'이 존재하지 않습니다"
            else:
                is_compatible = False
                detail = f"컬럼 이름 변경은 비호환 변경입니다"
        elif body.change_type == "change_type":
            if body.column_name not in existing_cols:
                is_compatible = False
                detail = f"컬럼 '{body.column_name}'이 존재하지 않습니다"
            else:
                current_type = existing_cols[body.column_name]
                widening = {
                    ("integer", "bigint"), ("smallint", "integer"), ("real", "double precision"),
                    ("varchar", "text"), ("character varying", "text"),
                }
                if (current_type, body.new_definition) in widening:
                    detail = f"타입 확장 ({current_type} → {body.new_definition}) — 호환"
                else:
                    is_compatible = False
                    detail = f"타입 변경 ({current_type} → {body.new_definition}) — 비호환 (데이터 손실 가능)"

        eid = await conn.fetchval("""
            INSERT INTO lh_schema_evolution (table_name, change_type, column_name, old_definition, new_definition, is_compatible)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING evolution_id
        """, body.table_name, body.change_type, body.column_name, body.old_definition, body.new_definition, is_compatible)

        return {
            "evolution_id": eid,
            "table_name": body.table_name,
            "change_type": body.change_type,
            "is_compatible": is_compatible,
            "detail": detail,
        }
    finally:
        await _rel(conn)


@router.get("/schema/history/{table}")
async def schema_history(table: str, limit: int = Query(20, le=100)):
    """스키마 변경 이력"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("""
            SELECT * FROM lh_schema_evolution WHERE table_name=$1
            ORDER BY created_at DESC LIMIT $2
        """, table, limit)
        return {"table": table, "history": [dict(r) for r in rows], "total": len(rows)}
    finally:
        await _rel(conn)


@router.post("/validate-export")
async def validate_export(table: str = Query(..., pattern=r"^[a-z_][a-z0-9_]{0,99}$")):
    """Parquet 내보내기 전 DQ 검증"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        if table not in ALLOWED_TABLES:
            raise HTTPException(400, f"허용 테이블: {', '.join(sorted(ALLOWED_TABLES))}")

        total = await conn.fetchval(f"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname=$1", table) or 0
        cols = await conn.fetch("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_name=$1 AND table_schema='public' ORDER BY ordinal_position
        """, table)

        checks = []
        # null check on PK
        pk_col = cols[0]["column_name"] if cols else None
        if pk_col:
            pk_nulls = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {pk_col} IS NULL") or 0
            checks.append({"check": "pk_not_null", "column": pk_col, "pass": pk_nulls == 0, "null_count": pk_nulls})

        # 전체 null 비율
        high_null_cols = []
        for col in cols[:20]:
            cn = col["column_name"]
            nc = await conn.fetchval(f"SELECT COUNT(*) FROM {table} WHERE {cn} IS NULL") or 0
            pct = round(nc / total * 100, 1) if total > 0 else 0
            if pct > 90:
                high_null_cols.append({"column": cn, "null_percent": pct})

        all_pass = all(c["pass"] for c in checks) and len(high_null_cols) == 0

        return {
            "table": table,
            "total_rows": total,
            "column_count": len(cols),
            "dq_passed": all_pass,
            "checks": checks,
            "high_null_columns": high_null_cols,
            "export_ready": all_pass,
        }
    finally:
        await _rel(conn)


@router.get("/dashboard")
async def lakehouse_quality_dashboard():
    """Lakehouse DQ 대시보드"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        # 프로파일 통계
        profile_count = await conn.fetchval("SELECT COUNT(*) FROM lh_data_profile") or 0
        profiled_tables = await conn.fetchval("SELECT COUNT(DISTINCT table_name) FROM lh_data_profile") or 0

        # 스키마 진화 통계
        evolution_count = await conn.fetchval("SELECT COUNT(*) FROM lh_schema_evolution") or 0
        compatible_count = await conn.fetchval("SELECT COUNT(*) FROM lh_schema_evolution WHERE is_compatible=TRUE") or 0
        incompatible_count = await conn.fetchval("SELECT COUNT(*) FROM lh_schema_evolution WHERE is_compatible=FALSE") or 0

        # 높은 null 비율 컬럼
        high_null = await conn.fetch("""
            SELECT DISTINCT ON (table_name, column_name) table_name, column_name, null_percent
            FROM lh_data_profile WHERE null_percent > 80
            ORDER BY table_name, column_name, created_at DESC
            LIMIT 20
        """)

        return {
            "profiling": {
                "total_profiles": profile_count,
                "tables_profiled": profiled_tables,
            },
            "schema_evolution": {
                "total_changes": evolution_count,
                "compatible": compatible_count,
                "incompatible": incompatible_count,
            },
            "data_quality_alerts": {
                "high_null_columns": [dict(r) for r in high_null],
            },
        }
    finally:
        await _rel(conn)
