"""
데이터마트 API - OMOP CDM 데이터베이스 연동
OMOP CDM V5.4 테이블 목록, 스키마, 샘플 데이터 제공
"""
import time
import csv
import io

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from services.redis_cache import cache_set
from ._datamart_shared import (
    OMOP_DB_CONFIG,
    TABLE_DESCRIPTIONS, TABLE_CATEGORIES, ALLOWED_TABLES,
    get_connection, release_connection, validate_table_name, _serialize_value,
)
from . import datamart_analytics

router = APIRouter(prefix="/datamart", tags=["DataMart"])

# Analytics 서브라우터 포함 (CDM summary, dashboard stats)
router.include_router(datamart_analytics.router)


# ═══════════════════════════════════════════════════
#  Table Introspection Endpoints
# ═══════════════════════════════════════════════════

_tables_cache: dict = {"data": None, "ts": 0}
_TABLES_CACHE_TTL = 600  # 10분

@router.get("/tables")
async def list_tables():
    """OMOP CDM 전체 테이블 목록 (행 수, 컬럼 수 포함)"""
    now = time.time()
    if _tables_cache["data"] and now - _tables_cache["ts"] < _TABLES_CACHE_TTL:
        return _tables_cache["data"]

    conn = await get_connection()
    try:
        # 테이블별 행 수 조회
        row_counts = await conn.fetch("""
            SELECT c.relname as table_name, c.reltuples::bigint as row_count
            FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'r' AND n.nspname = 'public'
            ORDER BY c.relname
        """)
        count_map = {r["table_name"]: r["row_count"] for r in row_counts}

        # 테이블별 컬럼 수 조회
        col_counts = await conn.fetch("""
            SELECT table_name, COUNT(*) as col_count
            FROM information_schema.columns
            WHERE table_schema = 'public'
            GROUP BY table_name
            ORDER BY table_name
        """)
        col_map = {r["table_name"]: r["col_count"] for r in col_counts}

        # 카테고리 역매핑
        table_to_category = {}
        for cat, tables in TABLE_CATEGORIES.items():
            for t in tables:
                table_to_category[t] = cat

        tables = []
        for table_name in sorted(ALLOWED_TABLES):
            if table_name in col_map:
                tables.append({
                    "name": table_name,
                    "description": TABLE_DESCRIPTIONS.get(table_name, ""),
                    "category": table_to_category.get(table_name, "Other"),
                    "row_count": count_map.get(table_name, 0),
                    "column_count": col_map.get(table_name, 0),
                })

        result = {
            "tables": tables,
            "total_tables": len(tables),
            "database": "OMOP CDM V5.4",
            "source": "CMS Synthetic Data",
        }
        _tables_cache["data"] = result
        _tables_cache["ts"] = time.time()
        return result
    finally:
        await release_connection(conn)


@router.get("/tables/{table_name}/schema")
async def get_table_schema(table_name: str):
    """테이블 컬럼 스키마 정보"""
    validate_table_name(table_name)
    conn = await get_connection()
    try:
        columns = await conn.fetch("""
            SELECT
                column_name,
                data_type,
                character_maximum_length,
                is_nullable,
                column_default,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position
        """, table_name)

        return {
            "table_name": table_name,
            "description": TABLE_DESCRIPTIONS.get(table_name, ""),
            "columns": [
                {
                    "name": col["column_name"],
                    "type": col["data_type"].upper() + (
                        f"({col['character_maximum_length']})" if col["character_maximum_length"] else ""
                    ),
                    "nullable": col["is_nullable"] == "YES",
                    "default": col["column_default"],
                    "position": col["ordinal_position"],
                }
                for col in columns
            ],
        }
    finally:
        await release_connection(conn)


@router.get("/tables/{table_name}/sample")
async def get_sample_data(
    table_name: str,
    limit: int = Query(default=5, ge=1, le=50),
):
    """테이블 샘플 데이터 조회 (최대 50행)"""
    validate_table_name(table_name)
    conn = await get_connection()
    try:
        # 안전한 쿼리: table_name은 이미 validate_table_name으로 검증됨
        rows = await conn.fetch(f'SELECT * FROM "{table_name}" LIMIT $1', limit)

        if not rows:
            return {"table_name": table_name, "columns": [], "rows": [], "total_sampled": 0}

        columns = list(rows[0].keys())
        data = []
        for row in rows:
            data.append({col: _serialize_value(row[col]) for col in columns})

        return {
            "table_name": table_name,
            "columns": columns,
            "rows": data,
            "total_sampled": len(data),
        }
    finally:
        await release_connection(conn)


@router.get("/tables/{table_name}/stats")
async def get_table_stats(table_name: str):
    """테이블 기본 통계"""
    validate_table_name(table_name)
    conn = await get_connection()
    try:
        row_count = await conn.fetchval(f'SELECT COUNT(*) FROM "{table_name}"')

        col_info = await conn.fetch("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position
        """, table_name)

        return {
            "table_name": table_name,
            "row_count": row_count,
            "column_count": len(col_info),
            "columns": [{"name": c["column_name"], "type": c["data_type"]} for c in col_info],
        }
    finally:
        await release_connection(conn)


# ═══════════════════════════════════════════════════
#  Health Check
# ═══════════════════════════════════════════════════

@router.get("/health")
async def health_check():
    """OMOP CDM DB 연결 상태 확인"""
    try:
        conn = await get_connection()
        try:
            version = await conn.fetchval("SELECT version()")
            table_count = await conn.fetchval(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'"
            )
        finally:
            await release_connection(conn)
        return {
            "status": "healthy",
            "database": "omop_cdm",
            "tables": table_count,
            "version": version,
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


# ═══════════════════════════════════════════════════
#  CDM Mapping Examples
# ═══════════════════════════════════════════════════

_mapping_examples_cache: dict = {"data": None, "ts": 0}

@router.get("/cdm-mapping-examples")
async def get_cdm_mapping_examples():
    """OMOP CDM 매핑 예시 — 실제 DB 컬럼 기반 동적 생성 (10분 캐시)"""
    import time as _time
    if _mapping_examples_cache["data"] and _time.time() - _mapping_examples_cache["ts"] < 600:
        return _mapping_examples_cache["data"]

    conn = await get_connection()
    try:
        # 실제 테이블에서 대표 소스값 추출
        examples = []

        # 성별
        try:
            gender_vals = await conn.fetch(
                "SELECT DISTINCT gender_source_value FROM person WHERE gender_source_value IS NOT NULL LIMIT 5"
            )
            gv = " / ".join(r["gender_source_value"] for r in gender_vals)
            examples.append({
                "source": f'환자 성별 "{gv}"',
                "sourceField": "gender",
                "cdmTable": "person",
                "cdmField": "gender_source_value",
                "cdmValue": gv,
                "standard": "OMOP Gender",
            })
        except Exception:
            pass

        # 진단 코드 (TOP 1)
        try:
            cond = await conn.fetchrow(
                "SELECT condition_source_value, COUNT(*) AS cnt FROM condition_occurrence "
                "WHERE condition_source_value IS NOT NULL GROUP BY 1 ORDER BY cnt DESC LIMIT 1"
            )
            if cond:
                examples.append({
                    "source": f'진단 코드 "{cond["condition_source_value"]}"',
                    "sourceField": "diagnosis_code",
                    "cdmTable": "condition_occurrence",
                    "cdmField": "condition_source_value",
                    "cdmValue": cond["condition_source_value"],
                    "standard": "SNOMED CT",
                })
        except Exception:
            pass

        # 약물
        try:
            drug = await conn.fetchrow(
                "SELECT drug_source_value, COUNT(*) AS cnt FROM drug_exposure "
                "WHERE drug_source_value IS NOT NULL GROUP BY 1 ORDER BY cnt DESC LIMIT 1"
            )
            if drug:
                examples.append({
                    "source": f'약물 "{drug["drug_source_value"]}"',
                    "sourceField": "drug_name",
                    "cdmTable": "drug_exposure",
                    "cdmField": "drug_source_value",
                    "cdmValue": drug["drug_source_value"],
                    "standard": "RxNorm",
                })
        except Exception:
            pass

        # 검사
        try:
            meas = await conn.fetchrow(
                "SELECT measurement_source_value, COUNT(*) AS cnt FROM measurement "
                "WHERE measurement_source_value IS NOT NULL GROUP BY 1 ORDER BY cnt DESC LIMIT 1"
            )
            if meas:
                examples.append({
                    "source": f'검사 "{meas["measurement_source_value"]}"',
                    "sourceField": "lab_code",
                    "cdmTable": "measurement",
                    "cdmField": "measurement_source_value",
                    "cdmValue": meas["measurement_source_value"],
                    "standard": "LOINC",
                })
        except Exception:
            pass

        # 내원 유형
        try:
            visit = await conn.fetchrow(
                "SELECT visit_concept_id, COUNT(*) AS cnt FROM visit_occurrence "
                "GROUP BY 1 ORDER BY cnt DESC LIMIT 1"
            )
            if visit:
                visit_names = {9201: "입원", 9202: "외래", 9203: "응급"}
                vname = visit_names.get(visit["visit_concept_id"], str(visit["visit_concept_id"]))
                examples.append({
                    "source": f'내원 "{vname}"',
                    "sourceField": "visit_type",
                    "cdmTable": "visit_occurrence",
                    "cdmField": "visit_concept_id",
                    "cdmValue": str(visit["visit_concept_id"]),
                    "standard": "OMOP Visit",
                })
        except Exception:
            pass

        # 관찰
        try:
            obs = await conn.fetchrow(
                "SELECT observation_source_value, COUNT(*) AS cnt FROM observation "
                "WHERE observation_source_value IS NOT NULL GROUP BY 1 ORDER BY cnt DESC LIMIT 1"
            )
            if obs:
                examples.append({
                    "source": f'관찰 "{obs["observation_source_value"]}"',
                    "sourceField": "observation_code",
                    "cdmTable": "observation",
                    "cdmField": "observation_source_value",
                    "cdmValue": obs["observation_source_value"],
                    "standard": "SNOMED CT",
                })
        except Exception:
            pass

        result = {"examples": examples}
        _mapping_examples_cache["data"] = result
        _mapping_examples_cache["ts"] = _time.time()
        return result
    finally:
        await release_connection(conn)


# ═══════════════════════════════════════════════════
#  Management Endpoints
# ═══════════════════════════════════════════════════

class TableDescriptionUpdate(BaseModel):
    description: str = Field(..., max_length=500)


@router.put("/tables/{table_name}/description")
async def update_table_description(table_name: str, body: TableDescriptionUpdate):
    """테이블 설명 수정"""
    validate_table_name(table_name)
    TABLE_DESCRIPTIONS[table_name] = body.description
    # 캐시 무효화
    datamart_analytics._cdm_summary_cache.clear()
    datamart_analytics._dashboard_cache.clear()
    _mapping_examples_cache["data"] = None
    return {"success": True, "table_name": table_name, "description": body.description}


@router.post("/cache-clear")
async def clear_all_caches():
    """모든 데이터마트 캐시 초기화"""
    datamart_analytics._cdm_summary_cache.clear()
    datamart_analytics._dashboard_cache.clear()
    _mapping_examples_cache["data"] = None
    _mapping_examples_cache["ts"] = 0
    _tables_cache["data"] = None
    _tables_cache["ts"] = 0
    # Redis 캐시도 삭제
    await cache_set(datamart_analytics._REDIS_CDM_KEY, None, 1)
    await cache_set(datamart_analytics._REDIS_DASH_KEY, None, 1)
    return {"success": True, "message": "모든 캐시가 초기화되었습니다"}


@router.get("/tables/{table_name}/export-csv")
async def export_table_csv(
    table_name: str,
    limit: int = Query(default=1000, ge=1, le=50000),
):
    """테이블 데이터 CSV 내보내기 (최대 50,000행) — 청크 스트리밍"""
    validate_table_name(table_name)

    async def csv_stream():
        conn = await get_connection()
        try:
            rows = await conn.fetch(f'SELECT * FROM "{table_name}" LIMIT $1', limit)
            if not rows:
                return
            columns = list(rows[0].keys())
            # BOM + 헤더
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            yield b"\xef\xbb\xbf" + buf.getvalue().encode("utf-8")
            # 1000행씩 청크 전송
            chunk_buf = io.StringIO()
            chunk_writer = csv.writer(chunk_buf)
            for i, row in enumerate(rows):
                chunk_writer.writerow([_serialize_value(row[col]) for col in columns])
                if (i + 1) % 1000 == 0:
                    yield chunk_buf.getvalue().encode("utf-8")
                    chunk_buf = io.StringIO()
                    chunk_writer = csv.writer(chunk_buf)
            rest = chunk_buf.getvalue()
            if rest:
                yield rest.encode("utf-8")
        finally:
            await release_connection(conn)

    return StreamingResponse(
        csv_stream(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{table_name}.csv"'},
    )
