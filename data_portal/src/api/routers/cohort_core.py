"""
코호트 빌더 핵심 API — /count, /execute-flow, /set-operation
"""
import logging
from fastapi import APIRouter, HTTPException

from services.sql_executor import sql_executor
from .cohort_shared import (
    CountRequest, ExecuteFlowRequest, SetOperationRequest,
    criterion_to_subquery, criteria_to_person_sql,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/count")
async def cohort_count(req: CountRequest):
    """단일 criterion의 환자 수 카운트"""
    sub = criterion_to_subquery(req.criterion)
    sql = f"SELECT COUNT(DISTINCT person_id) FROM ({sub}) t"
    result = await sql_executor.execute(sql)

    if result.natural_language_explanation and "오류" in result.natural_language_explanation:
        raise HTTPException(status_code=500, detail=result.natural_language_explanation)

    count = result.results[0][0] if result.results else 0
    return {
        "count": count,
        "sql_used": sql,
        "execution_time_ms": result.execution_time_ms,
    }


@router.post("/execute-flow")
async def execute_flow(req: ExecuteFlowRequest):
    """CONSORT 흐름 실행 — 순차적 inclusion/exclusion 적용"""
    if not req.steps:
        raise HTTPException(status_code=400, detail="steps가 비어있습니다")

    # 전체 모수
    total_sql = "SELECT COUNT(DISTINCT person_id) FROM person"
    total_result = await sql_executor.execute(total_sql)
    total_population = total_result.results[0][0] if total_result.results else 0

    steps_result = []
    inclusion_criteria = []
    exclusion_subqueries = []

    for step in req.steps:
        c = step.criterion
        label = step.label or c.label

        if step.step_type == "inclusion":
            inclusion_criteria.append(c)
            # 현재까지의 inclusion intersect
            person_sql = criteria_to_person_sql(inclusion_criteria)
        else:
            # exclusion: 기존 inclusion 집합에서 제외
            exclusion_subqueries.append(criterion_to_subquery(c))
            person_sql = criteria_to_person_sql(inclusion_criteria)

        # exclusion 적용
        final_sql = person_sql
        for ex_sub in exclusion_subqueries:
            final_sql = f"SELECT person_id FROM ({final_sql}) t WHERE person_id NOT IN ({ex_sub})"

        count_sql = f"SELECT COUNT(DISTINCT person_id) FROM ({final_sql}) t"
        result = await sql_executor.execute(count_sql)

        if result.natural_language_explanation and "오류" in result.natural_language_explanation:
            raise HTTPException(status_code=500, detail=result.natural_language_explanation)

        remaining = result.results[0][0] if result.results else 0
        prev_count = steps_result[-1]["remaining_count"] if steps_result else total_population
        excluded = prev_count - remaining

        steps_result.append({
            "step_type": step.step_type,
            "label": label,
            "remaining_count": remaining,
            "excluded_count": excluded,
        })

    return {
        "total_population": total_population,
        "steps": steps_result,
        "final_count": steps_result[-1]["remaining_count"] if steps_result else total_population,
    }


@router.post("/set-operation")
async def set_operation(req: SetOperationRequest):
    """두 코호트 그룹 간 집합 연산 (intersection/union/difference)"""
    sql_a = criteria_to_person_sql(req.group_a)
    sql_b = criteria_to_person_sql(req.group_b)

    # |A|
    count_a_sql = f"SELECT COUNT(DISTINCT person_id) FROM ({sql_a}) t"
    # |B|
    count_b_sql = f"SELECT COUNT(DISTINCT person_id) FROM ({sql_b}) t"
    # |A ∩ B|
    overlap_sql = (
        f"SELECT COUNT(DISTINCT person_id) FROM ({sql_a}) t "
        f"WHERE person_id IN ({sql_b})"
    )

    res_a = await sql_executor.execute(count_a_sql)
    res_b = await sql_executor.execute(count_b_sql)
    res_overlap = await sql_executor.execute(overlap_sql)

    count_a = res_a.results[0][0] if res_a.results else 0
    count_b = res_b.results[0][0] if res_b.results else 0
    count_overlap = res_overlap.results[0][0] if res_overlap.results else 0
    count_a_only = count_a - count_overlap
    count_b_only = count_b - count_overlap

    if req.operation == "intersection":
        count_result = count_overlap
    elif req.operation == "union":
        count_result = count_a + count_b - count_overlap
    else:  # difference
        count_result = count_a_only

    return {
        "count_a": count_a,
        "count_b": count_b,
        "count_overlap": count_overlap,
        "count_a_only": count_a_only,
        "count_b_only": count_b_only,
        "count_result": count_result,
        "operation": req.operation,
    }
