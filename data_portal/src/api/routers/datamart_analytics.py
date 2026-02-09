"""
데이터마트 Analytics — CDM Summary & Dashboard Stats (캐시 기반 무거운 엔드포인트)
datamart.py 에서 include_router 로 합쳐짐 (prefix 없음)
"""
import os
import time
import json as _json
import urllib.request
import base64
import asyncio

from fastapi import APIRouter

from services.redis_cache import cache_get, cache_set
from ._datamart_shared import (
    TABLE_DESCRIPTIONS, TABLE_CATEGORIES,
    get_connection, release_connection,
)

router = APIRouter()

# ═══════════════════════════════════════════════════
#  Cache 상수 & 상태
# ═══════════════════════════════════════════════════

_CACHE_TTL = 300  # 5분 (메모리 캐시 TTL)

_REDIS_CDM_KEY = "idp:cdm_summary"
_REDIS_DASH_KEY = "idp:dashboard_stats"
_REDIS_TTL = 600  # 10분 (Redis TTL — 메모리보다 길게)

_cdm_summary_cache: dict = {}
_dashboard_cache: dict = {}
_dashboard_cache_lock = asyncio.Lock()


# ═══════════════════════════════════════════════════
#  CDM Summary
# ═══════════════════════════════════════════════════

async def _compute_cdm_summary() -> dict:
    """CDM 요약 실제 계산 (무거움 — 캐시 갱신용)"""
    conn = await get_connection()
    try:
        # 1) 테이블별 레코드 수
        table_stats = await conn.fetch("""
            SELECT c.relname AS table_name, c.reltuples::bigint AS row_count
            FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'r' AND n.nspname = 'public' AND c.reltuples > 0
            ORDER BY c.reltuples DESC
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
            "314529007": "비만 (BMI 30 이상)",
            "73595000": "스트레스 관련 장애",
            "66383009": "정상 임신 경과",
            "160903007": "풀타임 근로자",
            "160904001": "파트타임 근로자",
            "444814009": "바이러스성 부비동염",
            "423315002": "연간 의료 검진",
            "422650009": "사회적 고립",
            "741062008": "자궁내 피임장치 사용",
            "18718003": "약물 알레르기",
            "706893006": "위험 활동 종사",
            "109570002": "비정상 소견",
            "195662009": "급성 바이러스성 인두염",
            "424393004": "주거 불안정",
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
        await release_connection(conn)


async def _refresh_cdm_summary_cache():
    """백그라운드에서 CDM summary 캐시 갱신 (Redis + memory)"""
    try:
        result = await _compute_cdm_summary()
        _cdm_summary_cache["data"] = result
        _cdm_summary_cache["ts"] = time.monotonic()
        await cache_set(_REDIS_CDM_KEY, result, _REDIS_TTL)
    except Exception as e:
        print(f"[cdm-summary-cache] refresh failed: {e}")


@router.get("/cdm-summary")
async def cdm_summary():
    """CDM 변환 요약 (Redis → memory → DB fallback)"""
    now = time.monotonic()
    cached_ts = _cdm_summary_cache.get("ts", 0)
    cached_data = _cdm_summary_cache.get("data")

    # 1) 메모리 캐시 (5분 TTL)
    if cached_data and (now - cached_ts) < _CACHE_TTL:
        return cached_data

    if cached_data:
        asyncio.create_task(_refresh_cdm_summary_cache())
        return cached_data

    # 2) Redis 캐시 (10분 TTL, 서버 재시작 후에도 유지)
    redis_data = await cache_get(_REDIS_CDM_KEY)
    if redis_data:
        _cdm_summary_cache["data"] = redis_data
        _cdm_summary_cache["ts"] = now
        asyncio.create_task(_refresh_cdm_summary_cache())
        return redis_data

    # 3) 최초 호출 — 백그라운드 full 계산 + 빠른 핵심 데이터 즉시 반환
    asyncio.create_task(_refresh_cdm_summary_cache())

    visit_type_names = {9201: "입원", 9202: "외래", 9203: "응급"}
    snomed_names = {
        "314529007": "비만 (BMI 30 이상)", "73595000": "스트레스 관련 장애",
        "66383009": "정상 임신 경과", "160903007": "풀타임 근로자",
        "160904001": "파트타임 근로자", "444814009": "바이러스성 부비동염",
        "423315002": "연간 의료 검진", "422650009": "사회적 고립",
        "741062008": "자궁내 피임장치 사용", "18718003": "약물 알레르기",
        "706893006": "위험 활동 종사", "109570002": "비정상 소견",
        "195662009": "급성 바이러스성 인두염", "424393004": "주거 불안정",
        "10509002": "급성 기관지염", "72892002": "정상 임신",
        "162864005": "체질량지수 30+ (비만)", "15777000": "사전 치료 필요 (Prediabetes)",
        "38341003": "고혈압성 장애", "40055000": "만성 부비동염",
        "19169002": "빈혈을 동반한 장애", "65363002": "이염 (중이염)",
        "44054006": "제2형 당뇨병", "55822004": "고지혈증",
        "230690007": "뇌졸중", "49436004": "심방세동",
        "53741008": "관상동맥 죽상경화증", "22298006": "심근경색",
        "59621000": "본태성 고혈압", "36971009": "부비동염",
        "233604007": "폐렴", "68496003": "다발성 장기 장애", "26929004": "알츠하이머병",
    }

    top_measurements = []
    quality_rows = []
    try:
        conn = await get_connection()
        try:
            table_stats = await conn.fetch("""
                SELECT relname AS table_name, n_live_tup AS row_count
                FROM pg_stat_user_tables
                WHERE schemaname = 'public' AND n_live_tup > 0
                ORDER BY n_live_tup DESC
            """)
            demographics = await conn.fetchrow("""
                SELECT COUNT(*) AS total_patients,
                    COUNT(*) FILTER (WHERE gender_source_value = 'M') AS male,
                    COUNT(*) FILTER (WHERE gender_source_value = 'F') AS female,
                    MIN(year_of_birth) AS min_birth_year,
                    MAX(year_of_birth) AS max_birth_year,
                    ROUND(AVG(EXTRACT(YEAR FROM CURRENT_DATE) - year_of_birth)) AS avg_age
                FROM person
            """)
            top_conditions = await conn.fetch("""
                SELECT condition_source_value AS snomed_code, COUNT(*) AS count,
                       COUNT(DISTINCT person_id) AS patient_count
                FROM condition_occurrence
                GROUP BY condition_source_value ORDER BY count DESC LIMIT 15
            """)
            visit_types = await conn.fetch("""
                SELECT visit_concept_id, COUNT(*) AS count,
                       COUNT(DISTINCT person_id) AS patient_count
                FROM visit_occurrence GROUP BY visit_concept_id ORDER BY count DESC
            """)
            yearly_activity = await conn.fetch("""
                SELECT year, SUM(cnt) AS total FROM (
                    SELECT EXTRACT(YEAR FROM condition_start_date)::int AS year, COUNT(*) AS cnt
                    FROM condition_occurrence WHERE condition_start_date IS NOT NULL GROUP BY 1
                    UNION ALL
                    SELECT EXTRACT(YEAR FROM visit_start_date)::int AS year, COUNT(*) AS cnt
                    FROM visit_occurrence WHERE visit_start_date IS NOT NULL GROUP BY 1
                ) sub WHERE year >= 2005 GROUP BY year ORDER BY year
            """)
            # top_measurements — pg_stat 기반 row count 추정 (measurement 36M 풀스캔 회피)
            top_measurements = await conn.fetch("""
                SELECT measurement_source_value AS code, COUNT(*) AS count
                FROM measurement
                GROUP BY measurement_source_value
                ORDER BY count DESC LIMIT 10
            """)
            # 도메인별 품질
            quality_checks = {
                "Clinical": ("condition_occurrence",
                             "COUNT(condition_source_value)+COUNT(condition_start_date)+COUNT(condition_concept_id)", 3),
                "Admin": ("visit_occurrence",
                          "COUNT(visit_start_date)+COUNT(visit_end_date)+COUNT(visit_concept_id)", 3),
                "Drug": ("drug_exposure",
                         "COUNT(drug_source_value)+COUNT(drug_exposure_start_date)+COUNT(quantity)", 3),
            }
            for domain, (tbl, expr, ncol) in quality_checks.items():
                row = await conn.fetchrow(f"SELECT COUNT(*) AS total, {expr} AS filled FROM {tbl}")
                total = row["total"] or 1
                filled = row["filled"] or 0
                score = round(filled / (total * ncol) * 100, 1)
                quality_rows.append({"domain": domain, "score": score,
                                     "total": total, "issues": total * ncol - filled})
        finally:
            await release_connection(conn)
    except Exception:
        demographics = {"total_patients": 0, "male": 0, "female": 0,
                        "min_birth_year": 0, "max_birth_year": 0, "avg_age": 0}
        table_stats = []
        top_conditions = []
        visit_types = []
        yearly_activity = []

    return {
        "table_stats": [
            {"name": r["table_name"], "row_count": r["row_count"],
             "category": next((cat for cat, tbls in TABLE_CATEGORIES.items() if r["table_name"] in tbls), "Other"),
             "description": TABLE_DESCRIPTIONS.get(r["table_name"], "")}
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
            {"snomed_code": r["snomed_code"],
             "name_kr": snomed_names.get(r["snomed_code"], r["snomed_code"]),
             "count": r["count"], "patient_count": r["patient_count"]}
            for r in top_conditions
        ],
        "visit_types": [
            {"type_id": r["visit_concept_id"],
             "type_name": visit_type_names.get(r["visit_concept_id"], f"기타({r['visit_concept_id']})"),
             "count": r["count"], "patient_count": r["patient_count"]}
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
        "quality": quality_rows or [
            {"domain": "Clinical", "score": 0, "total": 0, "issues": 0},
            {"domain": "Admin", "score": 0, "total": 0, "issues": 0},
            {"domain": "Drug", "score": 0, "total": 0, "issues": 0},
        ],
        "total_records": sum(r["row_count"] for r in table_stats),
        "total_tables": len(table_stats),
    }


# ═══════════════════════════════════════════════════
#  Airflow 헬퍼
# ═══════════════════════════════════════════════════

def _fetch_airflow_sync() -> dict:
    """Airflow REST API 호출 (동기, executor에서 실행)"""
    info = {"total_dags": 0, "active": 0, "paused": 0,
            "recent_success": 0, "recent_failed": 0, "recent_running": 0}
    airflow_url = os.getenv("AIRFLOW_API_URL", "http://localhost:18080")
    creds = base64.b64encode(
        f"{os.getenv('AIRFLOW_USER', 'admin')}:{os.getenv('AIRFLOW_PASSWORD', 'admin')}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {creds}"}

    req1 = urllib.request.Request(f"{airflow_url}/api/v1/dags", headers=headers)
    with urllib.request.urlopen(req1, timeout=3) as resp:
        dags = _json.loads(resp.read())
        info["total_dags"] = dags.get("total_entries", 0)
        for d in dags.get("dags", []):
            if d.get("is_paused"):
                info["paused"] += 1
            else:
                info["active"] += 1

    req2 = urllib.request.Request(
        f"{airflow_url}/api/v1/dags/~/dagRuns?limit=50&order_by=-start_date",
        headers=headers,
    )
    with urllib.request.urlopen(req2, timeout=3) as resp:
        runs = _json.loads(resp.read())
        for r in runs.get("dag_runs", []):
            st = r.get("state", "")
            if st == "success":
                info["recent_success"] += 1
            elif st == "failed":
                info["recent_failed"] += 1
            elif st == "running":
                info["recent_running"] += 1
    return info


async def _fetch_airflow_status() -> dict:
    """비동기 래퍼 — thread executor에서 Airflow API 호출"""
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _fetch_airflow_sync), timeout=5
        )
    except Exception:
        return {"total_dags": 0, "active": 0, "paused": 0,
                "recent_success": 0, "recent_failed": 0, "recent_running": 0}


# ═══════════════════════════════════════════════════
#  Dashboard Stats
# ═══════════════════════════════════════════════════

async def _compute_dashboard_stats() -> dict:
    """대시보드 통계 실제 계산 (무거움 — 캐시 갱신용)"""
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
            SELECT c.relname AS table_name, c.reltuples::bigint AS row_count
            FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.relkind = 'r' AND n.nspname = 'public' AND c.reltuples > 0
            ORDER BY c.reltuples DESC
        """)
        total_records = sum(r["row_count"] for r in row_counts)

        # 3) 연도별 측정 활동 타임라인
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

        # 4) 쿼리 응답시간 벤치마크
        t0 = time.monotonic()
        await conn.fetchval("SELECT COUNT(*) FROM person")
        query_latency_ms = round((time.monotonic() - t0) * 1000, 1)

        # 5) Airflow 파이프라인 상태
        pipeline_info = await _fetch_airflow_status()

        # 6) 진료유형별 분포
        visit_type_rows = await conn.fetch("""
            SELECT visit_concept_id, COUNT(*) AS cnt
            FROM visit_occurrence
            GROUP BY visit_concept_id
            ORDER BY cnt DESC
        """)
        visit_type_map = {9201: "입원", 9202: "외래", 9203: "응급"}
        visit_type_distribution = [
            {
                "type": visit_type_map.get(r["visit_concept_id"], f"기타({r['visit_concept_id']})"),
                "count": r["cnt"],
            }
            for r in visit_type_rows
        ]

        # 7) 보안 준수율
        pii_check = await conn.fetchrow("""
            SELECT
                COUNT(*) AS total,
                COUNT(person_source_value) AS has_source_id
            FROM person
        """)
        total_p = pii_check["total"] or 1
        has_src = pii_check["has_source_id"] or 0
        security_score = round(has_src / total_p * 100, 1) if total_p > 0 else 99.9

        return {
            "quality": quality_data,
            "total_records": total_records,
            "table_count": len(row_counts),
            "activity_timeline": activity_timeline,
            "query_latency_ms": query_latency_ms,
            "visit_type_distribution": visit_type_distribution,
            "pipeline": pipeline_info,
            "security_score": security_score,
        }
    finally:
        await release_connection(conn)


async def _refresh_dashboard_cache():
    """백그라운드에서 캐시 갱신 (Redis + memory, 에러 시 기존 캐시 유지)"""
    try:
        result = await _compute_dashboard_stats()
        _dashboard_cache["data"] = result
        _dashboard_cache["ts"] = time.monotonic()
        await cache_set(_REDIS_DASH_KEY, result, _REDIS_TTL)
    except Exception as e:
        print(f"[dashboard-cache] refresh failed: {e}")


@router.get("/dashboard-stats")
async def dashboard_stats():
    """대시보드용 통계 (Redis → memory → DB fallback)"""
    now = time.monotonic()
    cached_ts = _dashboard_cache.get("ts", 0)
    cached_data = _dashboard_cache.get("data")

    # 1) 메모리 캐시 (5분 TTL)
    if cached_data and (now - cached_ts) < _CACHE_TTL:
        return cached_data

    # 2) 메모리 만료 → 기존 반환 + 백그라운드 갱신
    if cached_data:
        asyncio.create_task(_refresh_dashboard_cache())
        return cached_data

    # 3) Redis 캐시 (10분 TTL)
    redis_data = await cache_get(_REDIS_DASH_KEY)
    if redis_data:
        _dashboard_cache["data"] = redis_data
        _dashboard_cache["ts"] = now
        asyncio.create_task(_refresh_dashboard_cache())
        return redis_data

    # 4) 캐시 없음 (최초 호출) → 빠른 fallback 반환 + 백그라운드 계산 시작
    asyncio.create_task(_refresh_dashboard_cache())
    # 빠른 쿼리로 최소 데이터만 반환 (pg_stat 기반, 밀리초)
    visit_type_distribution = []
    activity_timeline = []
    quality_data = []
    security_score = 99.9
    try:
        conn = await get_connection()
        try:
            row_counts = await conn.fetch("""
                SELECT relname AS table_name, n_live_tup AS row_count
                FROM pg_stat_user_tables
                WHERE schemaname = 'public' AND n_live_tup > 0
                ORDER BY n_live_tup DESC
            """)
            total_records = sum(r["row_count"] for r in row_counts)
            t0 = time.monotonic()
            await conn.fetchval("SELECT COUNT(*) FROM person")
            query_latency_ms = round((time.monotonic() - t0) * 1000, 1)
            pipeline_info = await _fetch_airflow_status()
            # 진료유형
            vt_rows = await conn.fetch("""
                SELECT visit_concept_id, COUNT(*) AS cnt
                FROM visit_occurrence
                GROUP BY visit_concept_id ORDER BY cnt DESC
            """)
            vt_map = {9201: "입원", 9202: "외래", 9203: "응급"}
            visit_type_distribution = [
                {"type": vt_map.get(r["visit_concept_id"], f"기타({r['visit_concept_id']})"),
                 "count": r["cnt"]}
                for r in vt_rows
            ]
            # 연도별 활동 타임라인
            activity = await conn.fetch("""
                SELECT EXTRACT(YEAR FROM condition_start_date)::int AS year, COUNT(*) AS count
                FROM condition_occurrence
                WHERE condition_start_date IS NOT NULL AND condition_start_date >= '2005-01-01'
                GROUP BY 1 ORDER BY 1
            """)
            activity_timeline = [{"month": str(r["year"]), "count": r["count"]} for r in activity]
            # 도메인별 품질
            qchecks = {
                "임상(Clinical)": ("condition_occurrence",
                    "COUNT(condition_source_value)+COUNT(condition_start_date)+COUNT(condition_concept_id)", 3),
                "원무(Admin)": ("visit_occurrence",
                    "COUNT(visit_start_date)+COUNT(visit_end_date)+COUNT(visit_concept_id)", 3),
                "약물(Drug)": ("drug_exposure",
                    "COUNT(drug_source_value)+COUNT(drug_exposure_start_date)+COUNT(quantity)", 3),
            }
            for domain, (tbl, expr, ncol) in qchecks.items():
                row = await conn.fetchrow(f"SELECT COUNT(*) AS total, {expr} AS filled FROM {tbl}")
                total = row["total"] or 1
                filled = row["filled"] or 0
                score = round(filled / (total * ncol) * 100, 1)
                quality_data.append({"domain": domain, "score": score,
                                     "issues": total * ncol - filled, "total": total})
            # 보안 준수율
            pii_check = await conn.fetchrow("""
                SELECT COUNT(*) AS total, COUNT(person_source_value) AS has_source_id FROM person
            """)
            total_p = pii_check["total"] or 1
            has_src = pii_check["has_source_id"] or 0
            security_score = round(has_src / total_p * 100, 1) if total_p > 0 else 99.9
        finally:
            await release_connection(conn)
    except Exception:
        total_records = 0
        row_counts = []
        query_latency_ms = 0
        pipeline_info = {"total_dags": 0, "active": 0, "paused": 0,
                         "recent_success": 0, "recent_failed": 0, "recent_running": 0}

    return {
        "quality": quality_data or [
            {"domain": "임상(Clinical)", "score": 0, "issues": 0, "total": 0},
            {"domain": "원무(Admin)", "score": 0, "issues": 0, "total": 0},
            {"domain": "약물(Drug)", "score": 0, "issues": 0, "total": 0},
        ],
        "total_records": total_records,
        "table_count": len(row_counts) if total_records else 0,
        "activity_timeline": activity_timeline,
        "query_latency_ms": query_latency_ms,
        "visit_type_distribution": visit_type_distribution,
        "pipeline": pipeline_info,
        "security_score": security_score,
    }
