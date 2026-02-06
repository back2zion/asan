"""
ConversationState 테스트
"""

import pytest
from datetime import datetime

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
    QueryContext,
    ReferenceInfo,
    ReferenceType,
    add_messages,
    create_initial_state,
)

try:
    from langchain_core.messages import HumanMessage, AIMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


class TestQueryContext:
    """QueryContext 테스트"""

    def test_create_query_context(self):
        """QueryContext 생성 테스트"""
        ctx = QueryContext(
            turn_number=1,
            original_query="당뇨 환자 몇 명?",
            generated_sql="SELECT COUNT(*) FROM condition_occurrence WHERE condition_concept_id = 201826",
            result_count=1247,
            conditions=["condition_concept_id = 201826"],
            tables_used=["condition_occurrence"],
        )

        assert ctx.turn_number == 1
        assert ctx.original_query == "당뇨 환자 몇 명?"
        assert ctx.result_count == 1247
        assert len(ctx.conditions) == 1
        assert "condition_occurrence" in ctx.tables_used

    def test_query_context_with_patient_ids(self):
        """환자 ID 목록 포함 테스트"""
        ctx = QueryContext(
            turn_number=2,
            original_query="그 중 남성만",
            patient_ids=["P001", "P002", "P003"],
            conditions=["sex_cd = 'M'"],
        )

        assert ctx.patient_ids == ["P001", "P002", "P003"]
        assert len(ctx.patient_ids) == 3


class TestReferenceInfo:
    """ReferenceInfo 테스트"""

    def test_create_reference_info(self):
        """ReferenceInfo 생성 테스트"""
        ref = ReferenceInfo(
            ref_type=ReferenceType.TEMPORAL,
            pattern=r"방금",
            position=0,
            original_text="방금",
        )

        assert ref.ref_type == ReferenceType.TEMPORAL
        assert ref.original_text == "방금"
        assert ref.resolved_value is None

    def test_reference_with_resolved_value(self):
        """해석된 값 포함 테스트"""
        ref = ReferenceInfo(
            ref_type=ReferenceType.ENTITY,
            pattern=r"그\s*환자",
            position=0,
            original_text="그 환자",
            resolved_value={
                "type": "patient_ids",
                "ids": ["P001", "P002"],
            },
        )

        assert ref.resolved_value is not None
        assert ref.resolved_value["type"] == "patient_ids"


class TestConversationState:
    """ConversationState 테스트"""

    def test_create_initial_state(self):
        """초기 상태 생성 테스트"""
        state = create_initial_state(
            thread_id="test-thread-001",
            user_id="user-001",
            initial_query="당뇨 환자 조회",
        )

        assert state["thread_id"] == "test-thread-001"
        assert state["user_id"] == "user-001"
        assert state["current_query"] == "당뇨 환자 조회"
        assert state["turn_count"] == 0
        assert state["messages"] == []
        assert state["has_references"] is False
        assert state["enriched_context"] is None

    def test_initial_state_metadata(self):
        """초기 상태 메타데이터 테스트"""
        state = create_initial_state(
            thread_id="test-thread-001",
            user_id="user-001",
        )

        assert "created_at" in state["session_metadata"]
        assert state["session_metadata"]["max_turns"] == 50


@pytest.mark.skipif(not LANGCHAIN_AVAILABLE, reason="langchain not installed")
class TestAddMessages:
    """add_messages 리듀서 테스트"""

    def test_add_new_messages(self):
        """새 메시지 추가 테스트"""
        left = [HumanMessage(content="Hello")]
        right = [AIMessage(content="Hi there")]

        result = add_messages(left, right)

        assert len(result) == 2
        assert result[0].content == "Hello"
        assert result[1].content == "Hi there"

    def test_add_messages_empty_left(self):
        """빈 목록에 메시지 추가 테스트"""
        left = []
        right = [HumanMessage(content="First message")]

        result = add_messages(left, right)

        assert len(result) == 1
        assert result[0].content == "First message"

    def test_add_messages_empty_right(self):
        """빈 목록 추가 테스트"""
        left = [HumanMessage(content="Existing")]
        right = []

        result = add_messages(left, right)

        assert len(result) == 1
        assert result[0].content == "Existing"

    def test_message_update_by_id(self):
        """ID로 메시지 업데이트 테스트"""
        msg1 = HumanMessage(content="Original", id="msg-1")
        msg2 = HumanMessage(content="Updated", id="msg-1")

        left = [msg1]
        right = [msg2]

        result = add_messages(left, right)

        # 같은 ID면 업데이트
        assert len(result) == 1
        assert result[0].content == "Updated"


class TestReferenceType:
    """ReferenceType Enum 테스트"""

    def test_reference_type_values(self):
        """참조 유형 값 테스트"""
        assert ReferenceType.TEMPORAL.value == "temporal"
        assert ReferenceType.ENTITY.value == "entity"
        assert ReferenceType.RESULT.value == "result"
        assert ReferenceType.CONDITION.value == "condition"

    def test_reference_type_from_string(self):
        """문자열에서 참조 유형 변환 테스트"""
        assert ReferenceType("temporal") == ReferenceType.TEMPORAL
        assert ReferenceType("entity") == ReferenceType.ENTITY


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
