"""
컨텍스트 해석 노드

탐지된 참조 표현을 이전 쿼리 컨텍스트를 기반으로 해석합니다.
"""

from typing import Any, Optional

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
    QueryContext,
    ReferenceInfo,
    ReferenceType,
)


def context_resolver_node(state: ConversationState) -> dict:
    """참조 표현을 컨텍스트 기반으로 해석하는 노드.

    탐지된 참조 표현("방금", "그 환자", "그 중에서" 등)을
    이전 쿼리 컨텍스트를 참조하여 구체적인 값으로 해석합니다.

    Args:
        state: 현재 대화 상태

    Returns:
        업데이트할 상태 필드 딕셔너리:
        - detected_references: resolved_value가 채워진 참조 목록
        - added_conditions: 추가할 WHERE 조건 목록
    """
    detected_refs = state.get("detected_references", [])
    last_context = state.get("last_query_context")

    if not detected_refs or not last_context:
        return {
            "detected_references": detected_refs,
            "added_conditions": [],
        }

    resolved_refs = []
    added_conditions = []

    for ref in detected_refs:
        resolved_ref, condition = _resolve_reference(ref, last_context, state)
        resolved_refs.append(resolved_ref)
        if condition:
            added_conditions.append(condition)

    return {
        "detected_references": resolved_refs,
        "added_conditions": added_conditions,
    }


def _resolve_reference(
    ref: ReferenceInfo,
    last_context: QueryContext,
    state: ConversationState,
) -> tuple[ReferenceInfo, Optional[str]]:
    """개별 참조 표현을 해석합니다.

    Args:
        ref: 참조 표현 정보
        last_context: 이전 쿼리 컨텍스트
        state: 전체 대화 상태

    Returns:
        (해석된 참조 정보, 추가할 WHERE 조건)
    """
    condition = None

    if ref.ref_type == ReferenceType.TEMPORAL:
        # 시간적 참조: 이전 쿼리 결과 전체 참조
        ref.resolved_value = {
            "type": "previous_query",
            "turn_number": last_context.turn_number,
            "sql": last_context.executed_sql or last_context.generated_sql,
            "result_count": last_context.result_count,
        }

    elif ref.ref_type == ReferenceType.ENTITY:
        # 엔티티 참조: 이전 쿼리의 환자/대상 ID 목록
        patient_ids = last_context.patient_ids
        if patient_ids:
            ref.resolved_value = {
                "type": "patient_ids",
                "ids": patient_ids,
                "count": len(patient_ids),
            }
            # 환자 ID 필터 조건 생성
            if len(patient_ids) <= 100:
                ids_str = ", ".join(f"'{pid}'" for pid in patient_ids)
                condition = f"person_id IN ({ids_str})"
            else:
                # 많은 ID는 서브쿼리로 처리
                ref.resolved_value["use_subquery"] = True

    elif ref.ref_type == ReferenceType.RESULT:
        # 결과 참조: 이전 쿼리 결과 집합에서 필터링
        ref.resolved_value = {
            "type": "filter_from_result",
            "previous_sql": last_context.executed_sql or last_context.generated_sql,
            "previous_conditions": last_context.conditions,
        }

    elif ref.ref_type == ReferenceType.CONDITION:
        # 조건 추가: 기존 조건 유지하며 새 조건 추가
        ref.resolved_value = {
            "type": "add_conditions",
            "existing_conditions": last_context.conditions,
        }
        # 기존 조건들을 모두 추가
        for cond in last_context.conditions:
            if cond not in (state.get("added_conditions") or []):
                condition = cond
                break  # 하나씩 추가 (중복 방지)

    return ref, condition


def extract_intent_from_references(
    query: str,
    refs: list[ReferenceInfo],
    last_context: Optional[QueryContext],
) -> dict[str, Any]:
    """참조 표현에서 사용자 의도를 추출합니다.

    Args:
        query: 사용자 질의
        refs: 해석된 참조 표현 목록
        last_context: 이전 쿼리 컨텍스트

    Returns:
        추출된 의도 정보:
        - action: 수행할 동작 (filter, aggregate, compare 등)
        - target: 대상 (이전 결과, 특정 환자군 등)
        - new_conditions: 추가할 조건들
    """
    intent = {
        "action": "query",  # 기본 동작
        "target": "new",    # 기본 대상
        "new_conditions": [],
        "use_previous_result": False,
    }

    ref_types = {ref.ref_type for ref in refs}

    # 참조 유형에 따른 의도 해석
    if ReferenceType.RESULT in ref_types or ReferenceType.TEMPORAL in ref_types:
        intent["use_previous_result"] = True
        intent["target"] = "previous_result"

    if ReferenceType.ENTITY in ref_types:
        intent["target"] = "previous_patients"
        intent["use_previous_result"] = True

    if ReferenceType.CONDITION in ref_types:
        intent["action"] = "filter"
        if last_context:
            intent["new_conditions"] = list(last_context.conditions)

    # 질의에서 추가 조건 키워드 탐지
    filter_keywords = {
        "남성": "sex_cd = 'M'",
        "여성": "sex_cd = 'F'",
        "65세 이상": "age >= 65",
        "70세 이상": "age >= 70",
        "성인": "age >= 18",
        "소아": "age < 18",
    }

    for keyword, condition in filter_keywords.items():
        if keyword in query:
            intent["new_conditions"].append(condition)
            intent["action"] = "filter"

    return intent


def build_context_prompt(
    query: str,
    refs: list[ReferenceInfo],
    last_context: Optional[QueryContext],
) -> str:
    """LLM에 전달할 컨텍스트 프롬프트를 생성합니다.

    Args:
        query: 사용자 질의
        refs: 해석된 참조 표현 목록
        last_context: 이전 쿼리 컨텍스트

    Returns:
        컨텍스트가 포함된 프롬프트 문자열
    """
    prompt_parts = []

    if last_context:
        prompt_parts.append("## 이전 대화 컨텍스트")
        prompt_parts.append(f"- 이전 질의: {last_context.original_query}")
        prompt_parts.append(f"- 실행된 SQL: {last_context.executed_sql or last_context.generated_sql}")
        prompt_parts.append(f"- 결과 건수: {last_context.result_count}건")

        if last_context.conditions:
            prompt_parts.append(f"- 적용된 조건: {', '.join(last_context.conditions)}")

    if refs:
        prompt_parts.append("\n## 탐지된 참조 표현")
        for ref in refs:
            prompt_parts.append(f"- '{ref.original_text}' ({ref.ref_type.value})")
            if ref.resolved_value:
                prompt_parts.append(f"  → 해석: {ref.resolved_value}")

    prompt_parts.append(f"\n## 현재 질의\n{query}")
    prompt_parts.append("\n위 컨텍스트를 참고하여 현재 질의를 해석하고 적절한 SQL을 생성해주세요.")

    return "\n".join(prompt_parts)
