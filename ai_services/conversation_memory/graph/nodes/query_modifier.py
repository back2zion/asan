"""
쿼리 수정 노드

이전 컨텍스트를 기반으로 SQL 쿼리를 수정합니다.
AND 조건 추가, 서브쿼리 필터링 등을 처리합니다.
"""

from typing import Optional

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
    ReferenceType,
)
from ai_services.conversation_memory.utils.sql_utils import (
    add_where_condition,
    create_subquery_filter,
    create_cte_with_previous_result,
    merge_sql_conditions,
)


def query_modifier_node(state: ConversationState) -> dict:
    """SQL 쿼리를 수정하는 노드.

    sql_generator 이후에 실행됩니다.
    참조가 탐지된 경우, 이전 쿼리의 결과를 서브쿼리로 사용하여
    현재 SQL의 person_id를 필터링합니다.

    서브쿼리 방식을 사용하여 이전 SQL의 JOIN과 조건이 자체적으로
    포함되므로, 테이블 별칭 누락 문제가 발생하지 않습니다.

    Args:
        state: 현재 대화 상태

    Returns:
        업데이트할 상태 필드 딕셔너리:
        - modified_sql: 수정된 SQL
        - added_conditions: 추가된 조건 목록
    """
    base_sql = state.get("base_sql")
    has_references = state.get("has_references", False)

    if not base_sql or not has_references:
        return {
            "modified_sql": base_sql,
            "added_conditions": [],
        }

    detected_refs = state.get("detected_references", [])

    # sql_generator는 더 이상 query_history에 추가하지 않으므로,
    # query_history[-1]이 실제 이전 턴의 컨텍스트임
    query_history = state.get("query_history", [])
    previous_context = query_history[-1] if query_history else None

    if not previous_context:
        return {
            "modified_sql": base_sql,
            "added_conditions": [],
        }

    # 이전 턴의 최종 SQL (수정된 SQL 우선)
    previous_sql = previous_context.executed_sql or previous_context.generated_sql
    if not previous_sql:
        return {
            "modified_sql": base_sql,
            "added_conditions": [],
        }

    modified_sql = base_sql
    added_conditions = []
    ref_types = {ref.ref_type for ref in detected_refs}

    # 모든 참조 유형에서 서브쿼리 방식 사용
    # 이전 SQL 전체를 서브쿼리로 감싸서 person_id로 필터링
    # 이렇게 하면 이전 SQL의 JOIN/WHERE가 자체 포함되어 별칭 문제 없음
    if ReferenceType.ENTITY in ref_types:
        modified_sql = create_subquery_filter(
            modified_sql, "p.person_id", previous_sql, "person_id"
        )
        added_conditions.append("person_id IN (이전 쿼리의 환자 집합)")

    elif ReferenceType.RESULT in ref_types or ReferenceType.TEMPORAL in ref_types:
        modified_sql = create_subquery_filter(
            modified_sql, "p.person_id", previous_sql, "person_id"
        )
        added_conditions.append("person_id IN (이전 결과 필터)")

    elif ReferenceType.CONDITION in ref_types:
        modified_sql = create_subquery_filter(
            modified_sql, "p.person_id", previous_sql, "person_id"
        )
        added_conditions.append("person_id IN (이전 조건 유지)")

    return {
        "modified_sql": modified_sql,
        "added_conditions": added_conditions,
    }


def _apply_entity_filter(
    sql: str,
    resolved_value: dict,
    last_context: Optional["QueryContext"],
) -> tuple[str, Optional[str]]:
    """엔티티 참조 필터를 적용합니다.

    Args:
        sql: 기본 SQL
        resolved_value: 해석된 참조 값
        last_context: 이전 쿼리 컨텍스트

    Returns:
        (수정된 SQL, 추가된 조건)
    """
    if resolved_value.get("use_subquery") and last_context:
        # 많은 ID는 서브쿼리로 처리
        previous_sql = last_context.executed_sql or last_context.generated_sql
        if previous_sql:
            modified = create_subquery_filter(
                sql,
                "person_id",
                previous_sql,
                "person_id",
            )
            return modified, "person_id IN (subquery)"

    patient_ids = resolved_value.get("ids", [])
    if patient_ids:
        if len(patient_ids) <= 100:
            ids_str = ", ".join(f"'{pid}'" for pid in patient_ids)
            condition = f"person_id IN ({ids_str})"
            modified = add_where_condition(sql, condition)
            return modified, condition

    return sql, None


def _apply_result_filter(
    sql: str,
    resolved_value: dict,
    last_context: Optional["QueryContext"],
) -> tuple[str, Optional[str]]:
    """결과 참조 필터를 적용합니다.

    Args:
        sql: 기본 SQL
        resolved_value: 해석된 참조 값
        last_context: 이전 쿼리 컨텍스트

    Returns:
        (수정된 SQL, 추가된 조건)
    """
    if not last_context:
        return sql, None

    previous_sql = resolved_value.get("previous_sql")
    if not previous_sql:
        previous_sql = last_context.executed_sql or last_context.generated_sql

    if previous_sql:
        # CTE를 사용하여 이전 결과와 조인
        modified = create_cte_with_previous_result(
            sql,
            previous_sql,
            cte_name="prev_result",
            join_column="person_id",
        )
        return modified, "person_id IN (SELECT person_id FROM prev_result)"

    # CTE 없이 이전 조건만 적용
    previous_conditions = resolved_value.get("previous_conditions", [])
    if previous_conditions:
        modified = merge_sql_conditions(sql, previous_conditions)
        return modified, f"conditions: {', '.join(previous_conditions)}"

    return sql, None


def apply_incremental_filter(
    current_sql: str,
    previous_context: "QueryContext",
    new_condition: str,
) -> str:
    """점진적 필터링을 적용합니다.

    이전 쿼리 결과에서 추가 조건으로 필터링하는 패턴입니다.
    예: "당뇨 환자" → "그 중 남성만" → "65세 이상만"

    Args:
        current_sql: 현재 SQL
        previous_context: 이전 쿼리 컨텍스트
        new_condition: 새로 추가할 조건

    Returns:
        수정된 SQL
    """
    # 이전 쿼리의 모든 조건을 현재 쿼리에 적용
    modified = current_sql
    for condition in previous_context.conditions:
        modified = add_where_condition(modified, condition)

    # 새 조건 추가
    modified = add_where_condition(modified, new_condition)

    return modified


def build_modified_query_summary(
    original_sql: str,
    modified_sql: str,
    added_conditions: list[str],
) -> dict:
    """수정된 쿼리 요약 정보를 생성합니다.

    Args:
        original_sql: 원본 SQL
        modified_sql: 수정된 SQL
        added_conditions: 추가된 조건 목록

    Returns:
        요약 정보 딕셔너리
    """
    return {
        "original_sql": original_sql,
        "modified_sql": modified_sql,
        "is_modified": original_sql != modified_sql,
        "added_conditions": added_conditions,
        "condition_count": len(added_conditions),
        "modification_type": _determine_modification_type(added_conditions),
    }


def _determine_modification_type(conditions: list[str]) -> str:
    """수정 유형을 판단합니다."""
    if not conditions:
        return "none"

    if any("subquery" in c.lower() or "prev_result" in c.lower() for c in conditions):
        return "subquery_filter"

    if any("person_id IN" in c for c in conditions):
        return "id_filter"

    return "condition_filter"
