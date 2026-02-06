"""
참조 탐지 노드

사용자 질의에서 한국어 참조 표현("방금", "이전", "그 환자" 등)을 탐지합니다.
"""

from ai_services.conversation_memory.state.conversation_state import ConversationState
from ai_services.conversation_memory.utils.korean_patterns import (
    detect_korean_references,
)


def reference_detector_node(state: ConversationState) -> dict:
    """참조 표현을 탐지하는 노드.

    사용자의 현재 질의에서 한국어 참조 표현을 탐지하고,
    탐지 결과에 따라 워크플로우 분기를 결정합니다.

    Args:
        state: 현재 대화 상태

    Returns:
        업데이트할 상태 필드 딕셔너리:
        - detected_references: 탐지된 참조 표현 목록
        - has_references: 참조 표현 존재 여부 (분기 조건)
    """
    current_query = state.get("current_query", "")

    if not current_query:
        return {
            "detected_references": [],
            "has_references": False,
        }

    # 한국어 참조 표현 탐지
    detected_refs = detect_korean_references(current_query)

    # 참조가 있더라도 이전 컨텍스트가 없으면 참조 해석 불가
    has_previous_context = state.get("last_query_context") is not None

    has_references = len(detected_refs) > 0 and has_previous_context

    return {
        "detected_references": detected_refs,
        "has_references": has_references,
    }


def should_resolve_context(state: ConversationState) -> str:
    """워크플로우 분기 조건 함수.

    has_references 값에 따라 다음 노드를 결정합니다.

    Args:
        state: 현재 대화 상태

    Returns:
        다음 노드 이름:
        - "context_resolver": 참조 표현이 있는 경우
        - "query_enricher": 참조 표현이 없는 경우
    """
    if state.get("has_references", False):
        return "context_resolver"
    return "query_enricher"
