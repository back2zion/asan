"""
데이터마트 API - OMOP CDM 데이터베이스 연동
OMOP CDM V6.0 테이블 목록, 스키마, 샘플 데이터 제공
"""
import os
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
import asyncpg

router = APIRouter(prefix="/datamart", tags=["DataMart"])

# OMOP CDM DB 연결 설정
OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}

# OMOP CDM 테이블 설명 (한국어)
TABLE_DESCRIPTIONS = {
    "person": "환자 인구통계학적 정보 (성별, 생년, 인종 등)",
    "visit_occurrence": "내원/입퇴원 이력 정보",
    "visit_detail": "내원 세부 정보 (병동 이동, 진료과 등)",
    "condition_occurrence": "진단/상병 발생 기록",
    "condition_era": "진단 기간 요약 (연속된 진단의 집약)",
    "drug_exposure": "약물 처방/투약 기록",
    "drug_era": "약물 투여 기간 요약",
    "procedure_occurrence": "시술/수술/검사 수행 기록",
    "measurement": "검사 결과 (Lab, Vital signs 등)",
    "observation": "관찰 기록 (진단 외 임상 소견)",
    "observation_period": "환자 관찰 기간 (데이터 수집 기간)",
    "device_exposure": "의료기기 사용 기록",
    "care_site": "진료 장소 (병원, 병동, 외래 등)",
    "provider": "의료진 정보",
    "location": "지역/주소 정보",
    "location_history": "지역 변경 이력",
    "cost": "의료비용 정보",
    "payer_plan_period": "보험 가입 기간 정보",
    "note": "임상 노트/기록 텍스트",
    "note_nlp": "임상 노트 NLP 처리 결과",
    "specimen_id": "검체 정보",
    "survey_conduct": "설문 수행 기록",
    "imaging_study": "영상 검사 기록 (CT, MRI 등)",
}

# OMOP CDM 테이블 카테고리
TABLE_CATEGORIES = {
    "Clinical Data": ["person", "visit_occurrence", "visit_detail", "condition_occurrence",
                      "drug_exposure", "procedure_occurrence", "measurement", "observation",
                      "device_exposure", "imaging_study"],
    "Health System": ["care_site", "provider", "location", "location_history"],
    "Derived": ["condition_era", "drug_era", "observation_period"],
    "Cost & Payer": ["cost", "payer_plan_period"],
    "Unstructured": ["note", "note_nlp"],
    "Other": ["specimen_id", "survey_conduct"],
}

# 허용된 테이블 이름 (SQL injection 방지)
ALLOWED_TABLES = set(TABLE_DESCRIPTIONS.keys())


async def get_connection():
    """OMOP CDM DB 연결"""
    try:
        return await asyncpg.connect(**OMOP_DB_CONFIG)
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"OMOP CDM 데이터베이스 연결 실패: {str(e)}",
        )


def validate_table_name(table_name: str) -> str:
    """테이블 이름 검증 (SQL injection 방지)"""
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"테이블을 찾을 수 없습니다: {table_name}")
    return table_name


@router.get("/tables")
async def list_tables():
    """OMOP CDM 전체 테이블 목록 (행 수, 컬럼 수 포함)"""
    conn = await get_connection()
    try:
        # 테이블별 행 수 조회
        row_counts = await conn.fetch("""
            SELECT relname as table_name, n_live_tup as row_count
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
            ORDER BY relname
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

        return {
            "tables": tables,
            "total_tables": len(tables),
            "database": "OMOP CDM V6.0",
            "source": "CMS Synthetic Data",
        }
    finally:
        await conn.close()


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
        await conn.close()


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
        await conn.close()


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
        await conn.close()


@router.get("/health")
async def health_check():
    """OMOP CDM DB 연결 상태 확인"""
    try:
        conn = await asyncpg.connect(**OMOP_DB_CONFIG)
        version = await conn.fetchval("SELECT version()")
        table_count = await conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'"
        )
        await conn.close()
        return {
            "status": "healthy",
            "database": "omop_cdm",
            "tables": table_count,
            "version": version,
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


def _serialize_value(val):
    """DB 값을 JSON-serializable 형태로 변환"""
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    # datetime, date, Decimal 등
    return str(val)
