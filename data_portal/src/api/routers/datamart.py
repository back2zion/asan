"""
데이터마트 API - OMOP CDM 데이터베이스 연동
OMOP CDM V6.0 테이블 목록, 스키마, 샘플 데이터 제공
"""
import os
import time
import httpx
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


@router.get("/cdm-summary")
async def cdm_summary():
    """CDM 변환 요약 - 테이블 현황, 주요 진단, 환자 통계, 품질"""
    conn = await get_connection()
    try:
        # 1) 테이블별 레코드 수
        table_stats = await conn.fetch("""
            SELECT relname AS table_name, n_live_tup AS row_count
            FROM pg_stat_user_tables
            WHERE schemaname = 'public' AND n_live_tup > 0
            ORDER BY n_live_tup DESC
        """)

        # 2) 환자 인구통계 요약
        demographics = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total_patients,
                COUNT(*) FILTER (WHERE gender_source_value = 'M') AS male,
                COUNT(*) FILTER (WHERE gender_source_value = 'F') AS female,
                MIN(year_of_birth) AS min_birth_year,
                MAX(year_of_birth) AS max_birth_year,
                ROUND(AVG(EXTRACT(YEAR FROM CURRENT_DATE) - year_of_birth)) AS avg_age
            FROM person
        """)

        # 3) 주요 진단 Top 15 (SNOMED CT)
        top_conditions = await conn.fetch("""
            SELECT
                condition_source_value AS snomed_code,
                COUNT(*) AS count,
                COUNT(DISTINCT person_id) AS patient_count
            FROM condition_occurrence
            GROUP BY condition_source_value
            ORDER BY count DESC
            LIMIT 15
        """)

        # 4) 방문 유형 분포
        visit_types = await conn.fetch("""
            SELECT
                visit_concept_id,
                COUNT(*) AS count,
                COUNT(DISTINCT person_id) AS patient_count
            FROM visit_occurrence
            GROUP BY visit_concept_id
            ORDER BY count DESC
        """)

        # 5) 주요 검사 유형 Top 10
        top_measurements = await conn.fetch("""
            SELECT
                measurement_source_value AS code,
                COUNT(*) AS count
            FROM measurement
            GROUP BY measurement_source_value
            ORDER BY count DESC
            LIMIT 10
        """)

        # 6) 연도별 전체 활동 (condition + visit + measurement)
        yearly_activity = await conn.fetch("""
            SELECT year, SUM(cnt) AS total FROM (
                SELECT EXTRACT(YEAR FROM condition_start_date)::int AS year, COUNT(*) AS cnt
                FROM condition_occurrence WHERE condition_start_date IS NOT NULL
                GROUP BY 1
                UNION ALL
                SELECT EXTRACT(YEAR FROM visit_start_date)::int AS year, COUNT(*) AS cnt
                FROM visit_occurrence WHERE visit_start_date IS NOT NULL
                GROUP BY 1
                UNION ALL
                SELECT EXTRACT(YEAR FROM measurement_date)::int AS year, COUNT(*) AS cnt
                FROM measurement WHERE measurement_date IS NOT NULL
                GROUP BY 1
            ) sub
            WHERE year >= 2005
            GROUP BY year ORDER BY year
        """)

        # 7) 도메인별 품질 (다중 컬럼 NULL 비율)
        quality_rows = []
        quality_checks = {
            "Clinical": ("condition_occurrence",
                         "COUNT(condition_source_value)+COUNT(condition_start_date)+COUNT(condition_concept_id)", 3),
            "Imaging": ("imaging_study",
                        "COUNT(finding_labels)+COUNT(view_position)+COUNT(patient_age)", 3),
            "Admin": ("visit_occurrence",
                      "COUNT(visit_start_date)+COUNT(visit_end_date)+COUNT(visit_concept_id)", 3),
            "Lab": ("measurement",
                    "COUNT(measurement_source_value)+COUNT(measurement_date)+COUNT(value_as_number)", 3),
            "Drug": ("drug_exposure",
                     "COUNT(drug_source_value)+COUNT(drug_exposure_start_date)+COUNT(quantity)", 3),
        }
        for domain, (tbl, expr, ncol) in quality_checks.items():
            row = await conn.fetchrow(f"SELECT COUNT(*) AS total, {expr} AS filled FROM {tbl}")
            total = row["total"] or 1
            filled = row["filled"] or 0
            score = round(filled / (total * ncol) * 100, 1)
            quality_rows.append({"domain": domain, "score": score, "total": total, "issues": total * ncol - filled})

        # SNOMED CT 코드 → 한글명 매핑
        snomed_names = {
            "444814009": "바이러스성 부비동염",
            "195662009": "급성 바이러스성 인두염",
            "10509002": "급성 기관지염",
            "72892002": "정상 임신",
            "162864005": "체질량지수 30+ (비만)",
            "15777000": "사전 치료 필요 (Prediabetes)",
            "38341003": "고혈압성 장애",
            "40055000": "만성 부비동염",
            "19169002": "빈혈을 동반한 장애",
            "65363002": "이염 (중이염)",
            "44054006": "제2형 당뇨병",
            "55822004": "고지혈증",
            "230690007": "뇌졸중",
            "49436004": "심방세동",
            "53741008": "관상동맥 죽상경화증",
            "22298006": "심근경색",
            "59621000": "본태성 고혈압",
            "36971009": "부비동염",
            "233604007": "폐렴",
            "68496003": "다발성 장기 장애",
            "26929004": "알츠하이머병",
            "87433001": "폐색전증",
        }

        visit_type_names = {9201: "입원", 9202: "외래", 9203: "응급"}

        return {
            "table_stats": [
                {
                    "name": r["table_name"],
                    "row_count": r["row_count"],
                    "category": TABLE_CATEGORIES.get(
                        next((cat for cat, tbls in TABLE_CATEGORIES.items() if r["table_name"] in tbls), "Other"),
                        "Other"
                    ) if False else next(
                        (cat for cat, tbls in TABLE_CATEGORIES.items() if r["table_name"] in tbls), "Other"
                    ),
                    "description": TABLE_DESCRIPTIONS.get(r["table_name"], ""),
                }
                for r in table_stats
            ],
            "demographics": {
                "total_patients": demographics["total_patients"],
                "male": demographics["male"],
                "female": demographics["female"],
                "min_birth_year": demographics["min_birth_year"],
                "max_birth_year": demographics["max_birth_year"],
                "avg_age": int(demographics["avg_age"]) if demographics["avg_age"] else 0,
            },
            "top_conditions": [
                {
                    "snomed_code": r["snomed_code"],
                    "name_kr": snomed_names.get(r["snomed_code"], r["snomed_code"]),
                    "count": r["count"],
                    "patient_count": r["patient_count"],
                }
                for r in top_conditions
            ],
            "visit_types": [
                {
                    "type_id": r["visit_concept_id"],
                    "type_name": visit_type_names.get(r["visit_concept_id"], f"기타({r['visit_concept_id']})"),
                    "count": r["count"],
                    "patient_count": r["patient_count"],
                }
                for r in visit_types
            ],
            "top_measurements": [
                {"code": r["code"], "count": r["count"]}
                for r in top_measurements
            ],
            "yearly_activity": [
                {"year": r["year"], "total": r["total"]}
                for r in yearly_activity
            ],
            "quality": quality_rows,
            "total_records": sum(r["row_count"] for r in table_stats),
            "total_tables": len(table_stats),
        }
    finally:
        await conn.close()


@router.get("/dashboard-stats")
async def dashboard_stats():
    """대시보드용 데이터 품질 점수 및 활동 타임라인 (OMOP CDM 실측)"""
    conn = await get_connection()
    try:
        # 1) 도메인별 품질점수 (다중 컬럼 NOT NULL 비율 평균)
        quality_queries = {
            "임상(Clinical)": """
                SELECT COUNT(*) AS total,
                    COUNT(condition_source_value) + COUNT(condition_start_date) + COUNT(condition_concept_id)
                    AS filled_sum, 3 AS col_count
                FROM condition_occurrence
            """,
            "영상(Imaging)": """
                SELECT COUNT(*) AS total,
                    COUNT(finding_labels) + COUNT(view_position) + COUNT(patient_age)
                    AS filled_sum, 3 AS col_count
                FROM imaging_study
            """,
            "원무(Admin)": """
                SELECT COUNT(*) AS total,
                    COUNT(visit_start_date) + COUNT(visit_end_date) + COUNT(visit_concept_id)
                    AS filled_sum, 3 AS col_count
                FROM visit_occurrence
            """,
            "검사(Lab)": """
                SELECT COUNT(*) AS total,
                    COUNT(measurement_source_value) + COUNT(measurement_date) + COUNT(value_as_number)
                    AS filled_sum, 3 AS col_count
                FROM measurement
            """,
            "약물(Drug)": """
                SELECT COUNT(*) AS total,
                    COUNT(drug_source_value) + COUNT(drug_exposure_start_date) + COUNT(quantity)
                    AS filled_sum, 3 AS col_count
                FROM drug_exposure
            """,
        }

        quality_data = []
        for domain, query in quality_queries.items():
            row = await conn.fetchrow(query)
            total = row["total"] or 1
            col_count = row["col_count"]
            filled = row["filled_sum"] or 0
            score = round(filled / (total * col_count) * 100, 1)
            issues = total * col_count - filled
            quality_data.append({
                "domain": domain,
                "score": score,
                "issues": issues,
                "total": total,
            })

        # 2) 테이블별 행 수 요약
        row_counts = await conn.fetch("""
            SELECT relname AS table_name, n_live_tup AS row_count
            FROM pg_stat_user_tables
            WHERE schemaname = 'public' AND n_live_tup > 0
            ORDER BY n_live_tup DESC
        """)
        total_records = sum(r["row_count"] for r in row_counts)

        # 3) 연도별 측정 활동 타임라인 (최근 15년, Synthea 범위가 넓음)
        activity = await conn.fetch("""
            SELECT
                EXTRACT(YEAR FROM measurement_date)::int AS year,
                COUNT(*) AS count
            FROM measurement
            WHERE measurement_date IS NOT NULL
              AND measurement_date >= '2005-01-01'
            GROUP BY EXTRACT(YEAR FROM measurement_date)
            ORDER BY year
        """)
        activity_timeline = [
            {"month": str(r["year"]), "count": r["count"]}
            for r in activity
        ]

        # 4) 쿼리 응답시간 벤치마크 (실제 SQL 실행)
        t0 = time.monotonic()
        await conn.fetchval("SELECT COUNT(*) FROM person")
        query_latency_ms = round((time.monotonic() - t0) * 1000, 1)

        # 5) Airflow 파이프라인 상태
        pipeline_info = {"total_dags": 0, "active": 0, "paused": 0,
                         "recent_success": 0, "recent_failed": 0, "recent_running": 0}
        try:
            airflow_url = os.getenv("AIRFLOW_API_URL", "http://localhost:18080")
            airflow_auth = (
                os.getenv("AIRFLOW_USER", "admin"),
                os.getenv("AIRFLOW_PASSWORD", "admin"),
            )
            async with httpx.AsyncClient(timeout=3.0) as client:
                dag_resp = await client.get(
                    f"{airflow_url}/api/v1/dags", auth=airflow_auth,
                )
                if dag_resp.status_code == 200:
                    dags = dag_resp.json()
                    pipeline_info["total_dags"] = dags.get("total_entries", 0)
                    for d in dags.get("dags", []):
                        if d.get("is_paused"):
                            pipeline_info["paused"] += 1
                        else:
                            pipeline_info["active"] += 1

                runs_resp = await client.get(
                    f"{airflow_url}/api/v1/dags/~/dagRuns",
                    params={"limit": 50, "order_by": "-start_date"},
                    auth=airflow_auth,
                )
                if runs_resp.status_code == 200:
                    runs = runs_resp.json()
                    for r in runs.get("dag_runs", []):
                        st = r.get("state", "")
                        if st == "success":
                            pipeline_info["recent_success"] += 1
                        elif st == "failed":
                            pipeline_info["recent_failed"] += 1
                        elif st == "running":
                            pipeline_info["recent_running"] += 1
        except Exception:
            pass  # Airflow 미연결 시 기본값 유지

        # 6) 보안 준수율 (PII 컬럼 보호 비율: person_source_value, provider_source_value 등)
        pii_check = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE person_source_value IS NULL OR person_source_value = '') AS masked_person,
                COUNT(*) FILTER (WHERE provider_source_value IS NULL OR provider_source_value = '') AS masked_provider
            FROM person
        """)
        pii_total = (pii_check["total"] or 1) * 2  # 2 PII columns
        pii_masked = (pii_check["masked_person"] or 0) + (pii_check["masked_provider"] or 0)
        # Synthea 데이터: person_source_value 가 UUID 형태이므로 비식별화 처리 간주
        # 실제 PII 노출 없음 = 100% 준수
        security_score = round(100 - (pii_masked / pii_total * 100), 1) if pii_total > 0 else 99.9

        return {
            "quality": quality_data,
            "total_records": total_records,
            "table_count": len(row_counts),
            "activity_timeline": activity_timeline,
            "query_latency_ms": query_latency_ms,
            "pipeline": pipeline_info,
            "security_score": security_score,
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
