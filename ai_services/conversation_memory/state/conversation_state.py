"""
ConversationState 정의

LangGraph State Management 기반 대화 상태 관리를 위한 TypedDict 및 데이터 클래스 정의.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Annotated, Any, Optional
from typing_extensions import TypedDict

from langchain_core.messages import BaseMessage


def add_messages(left: list[BaseMessage], right: list[BaseMessage]) -> list[BaseMessage]:
    """메시지 리스트를 자동으로 축적하는 리듀서 함수.

    LangGraph의 add_messages 동작을 구현합니다.
    동일한 ID를 가진 메시지는 업데이트하고, 새로운 메시지는 추가합니다.
    """
    # ID로 기존 메시지 인덱싱
    left_idx = {m.id: idx for idx, m in enumerate(left) if hasattr(m, 'id') and m.id}

    merged = list(left)
    for message in right:
        if hasattr(message, 'id') and message.id and message.id in left_idx:
            # 기존 메시지 업데이트
            merged[left_idx[message.id]] = message
        else:
            # 새 메시지 추가
            merged.append(message)

    return merged


class ReferenceType(str, Enum):
    """참조 표현 유형"""
    TEMPORAL = "temporal"      # 시간적 참조: "방금", "이전", "아까", "직전"
    ENTITY = "entity"          # 엔티티 참조: "그 환자", "해당 환자", "위 환자"
    RESULT = "result"          # 결과 참조: "그 중", "거기서", "위 결과"
    CONDITION = "condition"    # 조건 추가: "추가로", "더 좁혀", "그리고"


@dataclass
class ReferenceInfo:
    """탐지된 참조 표현 정보"""
    ref_type: ReferenceType
    pattern: str               # 매칭된 패턴
    position: int              # 문자열 내 위치
    original_text: str         # 원본 텍스트
    resolved_value: Optional[Any] = None  # 해석된 값 (예: 이전 쿼리 결과)


@dataclass
class QueryContext:
    """쿼리 컨텍스트 정보 - 각 턴의 쿼리 실행 결과를 저장"""
    turn_number: int
    original_query: str        # 사용자의 원본 질의
    enhanced_query: Optional[str] = None  # Prompt Enhancement 후 질의
    generated_sql: Optional[str] = None   # 생성된 SQL
    executed_sql: Optional[str] = None    # 실제 실행된 SQL (수정된 경우)
    result_count: Optional[int] = None    # 조회 결과 건수
    result_summary: Optional[dict] = None  # 결과 요약 (컬럼명, 샘플 데이터 등)
    patient_ids: Optional[list[str]] = None  # 조회된 환자 ID 목록 (필터링용)
    conditions: list[str] = field(default_factory=list)  # 적용된 WHERE 조건들
    tables_used: list[str] = field(default_factory=list)  # 사용된 테이블 목록
    timestamp: datetime = field(default_factory=datetime.now)
    execution_time_ms: Optional[float] = None  # 쿼리 실행 시간


class ConversationState(TypedDict):
    """LangGraph 대화 상태

    세션별 대화 컨텍스트를 관리하며, add_messages를 통해 메시지가 자동 축적됩니다.
    ConversationMemory는 SQL을 생성하지 않으며, NL2SQL 서비스(XiYan SQL 등)에
    전달할 enriched_context를 구성하는 것이 핵심 역할입니다.

    Attributes:
        thread_id: 대화 스레드 고유 ID
        user_id: 사용자 ID
        messages: 대화 메시지 목록 (자동 축적)
        current_query: 현재 처리 중인 사용자 질의
        last_query_context: 직전 턴의 쿼리 컨텍스트 (참조 해석에 사용)
        query_history: 전체 쿼리 히스토리
        detected_references: 현재 질의에서 탐지된 참조 표현 목록
        has_references: 참조 표현 존재 여부 (워크플로우 분기용)
        enriched_context: NL2SQL 서비스에 전달할 컨텍스트 정보
        turn_count: 현재 대화 턴 수
        session_metadata: 세션 메타데이터 (생성 시간, 만료 시간 등)
    """
    thread_id: str
    user_id: str
    messages: Annotated[list[BaseMessage], add_messages]
    current_query: str
    last_query_context: Optional[QueryContext]
    query_history: list[QueryContext]
    detected_references: list[ReferenceInfo]
    has_references: bool
    enriched_context: Optional[dict]
    turn_count: int
    session_metadata: dict


def create_initial_state(
    thread_id: str,
    user_id: str,
    initial_query: str = "",
) -> ConversationState:
    """초기 대화 상태 생성

    Args:
        thread_id: 대화 스레드 ID
        user_id: 사용자 ID
        initial_query: 초기 질의 (선택)

    Returns:
        초기화된 ConversationState
    """
    return ConversationState(
        thread_id=thread_id,
        user_id=user_id,
        messages=[],
        current_query=initial_query,
        last_query_context=None,
        query_history=[],
        detected_references=[],
        has_references=False,
        enriched_context=None,
        turn_count=0,
        session_metadata={
            "created_at": datetime.now().isoformat(),
            "expires_at": None,  # 30분 비활성 시 설정
            "max_turns": 50,
        },
    )
