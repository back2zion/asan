"""
LangGraph State Management 기반 ConversationMemory 모듈

서울아산병원 통합데이터플랫폼(IDP)의 다중 턴 대화 상태 관리 시스템.
"방금", "이전", "그 환자" 등의 한국어 참조 표현을 이해하고 문맥을 유지합니다.
"""

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
    QueryContext,
    ReferenceInfo,
    ReferenceType,
)
from ai_services.conversation_memory.graph.workflow import (
    create_conversation_workflow,
    record_query_result,
)

__all__ = [
    "ConversationState",
    "QueryContext",
    "ReferenceInfo",
    "ReferenceType",
    "create_conversation_workflow",
    "record_query_result",
]

__version__ = "0.1.0"
