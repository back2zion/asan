"""
쿼리 컨텍스트 강화 노드

NL2SQL 서비스(XiYan SQL 등)에 전달할 enriched_context를 구성합니다.
ConversationMemory는 SQL을 생성하지 않으며,
이전 대화 컨텍스트와 참조 해석 결과를 정리하여 전달하는 역할만 합니다.
"""

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
    QueryContext,
    ReferenceInfo,
    ReferenceType,
)


def query_enricher_node(state: ConversationState) -> dict:
    """NL2SQL 서비스에 전달할 컨텍스트를 구성하는 노드.

    이전 대화 히스토리, 참조 해석 결과, 현재 질의를 종합하여
    XiYan SQL 등의 NL2SQL 서비스가 효율적인 SQL을 생성할 수 있도록
    enriched_context를 구성합니다.

    Args:
        state: 현재 대화 상태

    Returns:
        업데이트할 상태 필드 딕셔너리:
        - enriched_context: NL2SQL 서비스에 전달할 컨텍스트
        - turn_count: 증가된 턴 수
    """
    current_query = state.get("current_query", "")
    has_references = state.get("has_references", False)
    detected_refs = state.get("detected_references", [])
    last_context = state.get("last_query_context")
    turn_count = state.get("turn_count", 0) + 1

    enriched = {
        "original_query": current_query,
        "is_follow_up": has_references,
        "turn_number": turn_count,
        "previous_context": None,
        "resolved_references": [],
        "context_prompt": "",
    }

    # 이전 컨텍스트 포함
    if has_references and last_context:
        enriched["previous_context"] = {
            "query": last_context.original_query,
            "sql": last_context.executed_sql or last_context.generated_sql,
            "conditions": last_context.conditions,
            "tables_used": last_context.tables_used,
            "result_count": last_context.result_count,
        }

        # 참조 해석 정보
        enriched["resolved_references"] = [
            {
                "type": ref.ref_type.value,
                "text": ref.original_text,
                "resolved_value": ref.resolved_value,
            }
            for ref in detected_refs
        ]

    # NL2SQL 서비스용 프롬프트 구성
    enriched["context_prompt"] = _build_context_prompt(
        current_query, has_references, last_context, detected_refs
    )

    return {
        "enriched_context": enriched,
        "turn_count": turn_count,
    }


def _build_context_prompt(
    query: str,
    has_references: bool,
    last_context: "QueryContext | None",
    refs: list[ReferenceInfo],
) -> str:
    """NL2SQL 서비스에 전달할 컨텍스트 프롬프트를 생성합니다.

    Args:
        query: 현재 사용자 질의
        has_references: 참조 표현 존재 여부
        last_context: 이전 쿼리 컨텍스트
        refs: 탐지된 참조 표현 목록

    Returns:
        컨텍스트가 포함된 프롬프트 문자열
    """
    if not has_references or not last_context:
        return query

    parts = []

    # 이전 대화 컨텍스트
    parts.append("## 이전 대화 컨텍스트")
    parts.append(f"- 이전 질의: {last_context.original_query}")

    previous_sql = last_context.executed_sql or last_context.generated_sql
    if previous_sql:
        parts.append(f"- 실행된 SQL:\n```sql\n{previous_sql}\n```")

    if last_context.result_count is not None:
        parts.append(f"- 결과 건수: {last_context.result_count}건")

    if last_context.conditions:
        parts.append(f"- 적용된 조건: {', '.join(last_context.conditions)}")

    if last_context.tables_used:
        parts.append(f"- 사용된 테이블: {', '.join(last_context.tables_used)}")

    # 참조 해석 정보
    if refs:
        parts.append("")
        parts.append("## 참조 해석")
        for ref in refs:
            ref_desc = _describe_reference(ref)
            parts.append(f"- \"{ref.original_text}\" → {ref_desc}")

    # 현재 질의
    parts.append("")
    parts.append("## 현재 질의")
    parts.append(query)

    # 지시사항
    parts.append("")
    parts.append("위 이전 대화 컨텍스트를 참고하여, 현재 질의에 맞는 SQL을 생성해주세요.")
    parts.append("이전 쿼리의 조건을 유지하면서 새로운 조건을 추가해야 합니다.")
    parts.append("JOIN, WHERE 등을 하나의 효율적인 쿼리로 작성해주세요.")

    return "\n".join(parts)


def _describe_reference(ref: ReferenceInfo) -> str:
    """참조 표현을 설명 문자열로 변환합니다."""
    descriptions = {
        ReferenceType.TEMPORAL: "이전 쿼리 결과 참조 (시간적)",
        ReferenceType.ENTITY: "이전 쿼리의 환자/대상 참조",
        ReferenceType.RESULT: "이전 결과 집합에서 필터링",
        ReferenceType.CONDITION: "이전 조건 유지 + 새 조건 추가",
    }
    return descriptions.get(ref.ref_type, "알 수 없는 참조")
