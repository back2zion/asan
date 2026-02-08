"""
코호트 리뷰 API — /drill-down, /patient/{id}/timeline, /summary-stats
"""
import logging
from fastapi import APIRouter, HTTPException

from services.sql_executor import sql_executor
from .cohort_shared import (
    DrillDownRequest, SummaryStatsRequest,
    criteria_to_person_sql,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/drill-down")
async def drill_down(req: DrillDownRequest):
    """코호트 기준에 해당하는 환자 목록 조회"""
    person_sql = criteria_to_person_sql(req.criteria)
    sql = (
        f"SELECT p.person_id, p.gender_source_value AS gender, "
        f"p.year_of_birth AS birth_year, (2026 - p.year_of_birth) AS age "
        f"FROM person p "
        f"WHERE p.person_id IN ({person_sql}) "
        f"ORDER BY p.person_id "
        f"LIMIT {req.limit} OFFSET {req.offset}"
    )
    result = await sql_executor.execute(sql)

    if result.natural_language_explanation and "오류" in result.natural_language_explanation:
        raise HTTPException(status_code=500, detail=result.natural_language_explanation)

    patients = []
    for row in result.results:
        patients.append({
            "person_id": row[0],
            "gender": row[1],
            "birth_year": row[2],
            "age": row[3],
        })

    return {"patients": patients, "count": len(patients)}


@router.get("/patient/{person_id}/timeline")
async def patient_timeline(person_id: int):
    """환자 개별 타임라인 — 6개 테이블 UNION ALL"""
    # 기본 정보
    info_sql = (
        f"SELECT person_id, gender_source_value, year_of_birth, "
        f"(2026 - year_of_birth) AS age "
        f"FROM person WHERE person_id = {person_id}"
    )
    info_result = await sql_executor.execute(info_sql)
    if not info_result.results:
        raise HTTPException(status_code=404, detail="환자를 찾을 수 없습니다")

    info_row = info_result.results[0]
    patient_info = {
        "person_id": info_row[0],
        "gender": info_row[1],
        "birth_year": info_row[2],
        "age": info_row[3],
    }

    # 타임라인 이벤트 (measurement/observation은 서브쿼리 LIMIT 50)
    timeline_sql = f"""
SELECT * FROM (
  SELECT 'condition' AS domain,
         condition_start_date::text AS event_date,
         condition_source_value AS code,
         '' AS value
  FROM condition_occurrence WHERE person_id = {person_id}
  UNION ALL
  SELECT 'drug' AS domain,
         drug_exposure_start_date::text AS event_date,
         drug_source_value AS code,
         '' AS value
  FROM drug_exposure WHERE person_id = {person_id}
  UNION ALL
  SELECT 'visit' AS domain,
         visit_start_date::text AS event_date,
         visit_concept_id::text AS code,
         '' AS value
  FROM visit_occurrence WHERE person_id = {person_id}
  UNION ALL
  SELECT * FROM (
    SELECT 'measurement' AS domain,
           measurement_date::text AS event_date,
           measurement_source_value AS code,
           COALESCE(value_as_number::text, '') AS value
    FROM measurement WHERE person_id = {person_id}
    LIMIT 50
  ) m
  UNION ALL
  SELECT 'procedure' AS domain,
         procedure_date::text AS event_date,
         procedure_source_value AS code,
         '' AS value
  FROM procedure_occurrence WHERE person_id = {person_id}
  UNION ALL
  SELECT * FROM (
    SELECT 'observation' AS domain,
           observation_date::text AS event_date,
           observation_source_value AS code,
           COALESCE(value_as_string, '') AS value
    FROM observation WHERE person_id = {person_id}
    LIMIT 50
  ) o
) events
ORDER BY event_date DESC
"""
    timeline_result = await sql_executor.execute(timeline_sql)

    events = []
    for row in timeline_result.results:
        events.append({
            "domain": row[0],
            "event_date": row[1],
            "code": row[2],
            "value": row[3] if len(row) > 3 else "",
        })

    return {"patient": patient_info, "events": events}


@router.post("/summary-stats")
async def summary_stats(req: SummaryStatsRequest):
    """코호트 요약 통계 — 성별, 연령, 진단 Top10, 방문유형"""
    person_sql = criteria_to_person_sql(req.criteria)

    # 성별 분포
    gender_sql = (
        f"SELECT gender_source_value, COUNT(*) "
        f"FROM person WHERE person_id IN ({person_sql}) "
        f"GROUP BY gender_source_value"
    )
    gender_result = await sql_executor.execute(gender_sql)
    gender_dist = [
        {"gender": row[0], "count": row[1]}
        for row in gender_result.results
    ]

    # 연령 통계
    age_sql = (
        f"SELECT MIN(2026 - year_of_birth) AS min_age, "
        f"MAX(2026 - year_of_birth) AS max_age, "
        f"AVG(2026 - year_of_birth)::int AS mean_age, "
        f"PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY 2026 - year_of_birth)::int AS median_age "
        f"FROM person WHERE person_id IN ({person_sql})"
    )
    age_result = await sql_executor.execute(age_sql)
    age_row = age_result.results[0] if age_result.results else [0, 0, 0, 0]
    age_stats = {
        "min": age_row[0], "max": age_row[1],
        "mean": age_row[2], "median": age_row[3],
    }

    # 연령대 분포 (10년 단위)
    age_dist_sql = (
        f"SELECT (((2026 - year_of_birth) / 10) * 10)::text || 's' AS age_group, COUNT(*) "
        f"FROM person WHERE person_id IN ({person_sql}) "
        f"GROUP BY age_group ORDER BY age_group"
    )
    age_dist_result = await sql_executor.execute(age_dist_sql)
    age_dist = [
        {"age_group": row[0], "count": row[1]}
        for row in age_dist_result.results
    ]

    # 진단 Top 10
    cond_sql = (
        f"SELECT condition_source_value, COUNT(DISTINCT person_id) AS cnt "
        f"FROM condition_occurrence "
        f"WHERE person_id IN ({person_sql}) "
        f"GROUP BY condition_source_value ORDER BY cnt DESC LIMIT 10"
    )
    cond_result = await sql_executor.execute(cond_sql)
    condition_top10 = [
        {"code": row[0], "count": row[1]}
        for row in cond_result.results
    ]

    # 방문 유형 분포
    visit_sql = (
        f"SELECT visit_concept_id, COUNT(*) "
        f"FROM visit_occurrence "
        f"WHERE person_id IN ({person_sql}) "
        f"GROUP BY visit_concept_id ORDER BY COUNT(*) DESC"
    )
    visit_result = await sql_executor.execute(visit_sql)
    visit_labels = {9201: "입원", 9202: "외래", 9203: "응급"}
    visit_type_dist = [
        {"visit_type": visit_labels.get(row[0], str(row[0])), "count": row[1]}
        for row in visit_result.results
    ]

    return {
        "gender_dist": gender_dist,
        "age_stats": age_stats,
        "age_dist": age_dist,
        "condition_top10": condition_top10,
        "visit_type_dist": visit_type_dist,
    }
