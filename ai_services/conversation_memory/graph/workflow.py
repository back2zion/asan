"""
LangGraph 워크플로우 정의

ConversationMemory의 핵심 워크플로우를 정의합니다.
SQL 생성은 하지 않으며, NL2SQL 서비스에 전달할 컨텍스트를 구성합니다.

워크플로우 흐름:
START → reference_detector → [분기]
                             ├─ has_references → context_resolver ─┐
                             └─ no_references ─────────────────────┤
                                                                    ↓
                                                             query_enricher
                                                                    ↓
                                                             state_updater → END
"""

from typing import Any, Optional

from langgraph.graph import END, StateGraph

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
    QueryContext,
    create_initial_state,
)
from ai_services.conversation_memory.graph.nodes.reference_detector import (
    reference_detector_node,
    should_resolve_context,
)
from ai_services.conversation_memory.graph.nodes.context_resolver import (
    context_resolver_node,
)
from ai_services.conversation_memory.graph.nodes.query_enricher import (
    query_enricher_node,
)


def state_updater_node(state: ConversationState) -> dict:
    """상태 업데이트 노드.

    워크플로우 완료 후 최종 상태를 정리합니다.
    QueryContext를 생성하고 query_history에 추가합니다.
    SQL 정보는 아직 없으므로, 호출측에서 record_query_result()로 채워야 합니다.

    Args:
        state: 현재 대화 상태

    Returns:
        업데이트할 상태 필드 딕셔너리
    """
    from datetime import datetime, timedelta
    from langchain_core.messages import HumanMessage

    current_query = state.get("current_query", "")
    turn_count = state.get("turn_count", 0)

    # 사용자 메시지 추가
    messages_to_add = []
    if current_query:
        messages_to_add.append(HumanMessage(content=current_query))

    # QueryContext 생성 (SQL 없이 — record_query_result에서 채워짐)
    query_context = QueryContext(
        turn_number=turn_count,
        original_query=current_query,
        generated_sql=None,
        executed_sql=None,
        result_count=None,
        conditions=[],
        tables_used=[],
        timestamp=datetime.now(),
    )

    query_history = list(state.get("query_history", []))
    query_history.append(query_context)

    # 최대 50개 턴 유지
    max_turns = state.get("session_metadata", {}).get("max_turns", 50)
    if len(query_history) > max_turns:
        query_history = query_history[-max_turns:]

    # 세션 메타데이터 업데이트
    session_metadata = dict(state.get("session_metadata", {}))
    session_metadata["last_updated"] = datetime.now().isoformat()
    session_metadata["expires_at"] = (datetime.now() + timedelta(minutes=30)).isoformat()

    return {
        "messages": messages_to_add,
        "session_metadata": session_metadata,
        "last_query_context": query_context,
        "query_history": query_history,
    }


def create_conversation_workflow() -> StateGraph:
    """ConversationMemory 워크플로우를 생성합니다.

    4개 노드:
    - reference_detector: 한국어 참조 표현 탐지
    - context_resolver: 참조 해석 (분기)
    - query_enricher: NL2SQL용 컨텍스트 구성
    - state_updater: 상태 정리 및 히스토리 업데이트

    Returns:
        컴파일 가능한 StateGraph
    """
    workflow = StateGraph(ConversationState)

    # 노드 추가
    workflow.add_node("reference_detector", reference_detector_node)
    workflow.add_node("context_resolver", context_resolver_node)
    workflow.add_node("query_enricher", query_enricher_node)
    workflow.add_node("state_updater", state_updater_node)

    # 엔트리 포인트
    workflow.set_entry_point("reference_detector")

    # 조건부 분기
    workflow.add_conditional_edges(
        "reference_detector",
        should_resolve_context,
        {
            "context_resolver": "context_resolver",
            "query_enricher": "query_enricher",
        },
    )

    # 참조 있는 경로: context_resolver → query_enricher (합류)
    workflow.add_edge("context_resolver", "query_enricher")

    # 공통 경로: query_enricher → state_updater → END
    workflow.add_edge("query_enricher", "state_updater")
    workflow.add_edge("state_updater", END)

    return workflow


def compile_workflow(
    workflow: Optional[StateGraph] = None,
    checkpointer: Optional[Any] = None,
) -> Any:
    """워크플로우를 컴파일합니다.

    Args:
        workflow: StateGraph 인스턴스 (미지정 시 새로 생성)
        checkpointer: 체크포인터 인스턴스 (선택)

    Returns:
        컴파일된 워크플로우 앱
    """
    if workflow is None:
        workflow = create_conversation_workflow()

    if checkpointer:
        return workflow.compile(checkpointer=checkpointer)

    return workflow.compile()


async def run_conversation_turn(
    app: Any,
    thread_id: str,
    user_id: str,
    query: str,
    previous_state: Optional[ConversationState] = None,
) -> ConversationState:
    """대화 턴을 실행합니다 (비동기).

    ConversationMemory 워크플로우를 실행하여 enriched_context를 생성합니다.
    SQL 생성은 하지 않으며, 반환된 enriched_context를 NL2SQL 서비스에 전달해야 합니다.

    Args:
        app: 컴파일된 워크플로우 앱
        thread_id: 대화 스레드 ID
        user_id: 사용자 ID
        query: 사용자 질의
        previous_state: 이전 대화 상태 (선택)

    Returns:
        업데이트된 대화 상태 (enriched_context 포함)
    """
    if previous_state:
        state = dict(previous_state)
        state["current_query"] = query
        state["detected_references"] = []
        state["has_references"] = False
        state["enriched_context"] = None
    else:
        state = create_initial_state(thread_id, user_id, query)

    config = {"configurable": {"thread_id": thread_id}}
    result = await app.ainvoke(state, config)

    return result


def run_conversation_turn_sync(
    app: Any,
    thread_id: str,
    user_id: str,
    query: str,
    previous_state: Optional[ConversationState] = None,
) -> ConversationState:
    """대화 턴을 동기적으로 실행합니다.

    Args:
        app: 컴파일된 워크플로우 앱
        thread_id: 대화 스레드 ID
        user_id: 사용자 ID
        query: 사용자 질의
        previous_state: 이전 대화 상태 (선택)

    Returns:
        업데이트된 대화 상태 (enriched_context 포함)
    """
    if previous_state:
        state = dict(previous_state)
        state["current_query"] = query
        state["detected_references"] = []
        state["has_references"] = False
        state["enriched_context"] = None
    else:
        state = create_initial_state(thread_id, user_id, query)

    config = {"configurable": {"thread_id": thread_id}}
    result = app.invoke(state, config)

    return result


def record_query_result(
    state: dict,
    generated_sql: str,
    executed_sql: Optional[str] = None,
    result_count: Optional[int] = None,
    patient_ids: Optional[list[str]] = None,
    conditions: Optional[list[str]] = None,
    tables_used: Optional[list[str]] = None,
) -> dict:
    """NL2SQL 서비스 실행 결과를 상태에 기록합니다.

    ConversationMemory 워크플로우 실행 후, 호출측에서 XiYan SQL 등의
    NL2SQL 서비스를 통해 SQL을 생성/실행한 뒤, 그 결과를 상태에 기록합니다.
    이 정보는 다음 턴에서 이전 컨텍스트로 참조됩니다.

    Args:
        state: 워크플로우 실행 결과 상태
        generated_sql: NL2SQL 서비스가 생성한 SQL
        executed_sql: 실제 실행된 SQL (미지정 시 generated_sql 사용)
        result_count: 쿼리 결과 건수
        patient_ids: 조회된 환자 ID 목록 (선택)
        conditions: 적용된 WHERE 조건 목록 (선택)
        tables_used: 사용된 테이블 목록 (선택)

    Returns:
        SQL 결과가 기록된 상태

    Example:
        >>> result = run_conversation_turn_sync(app, thread_id, user_id, query)
        >>> # XiYan SQL로 SQL 생성
        >>> sql = xiyan_sql.generate(result["enriched_context"]["context_prompt"])
        >>> # 결과 기록
        >>> result = record_query_result(result, sql, result_count=1247)
    """
    from ai_services.conversation_memory.utils.sql_utils import extract_table_names

    final_sql = executed_sql or generated_sql
    final_tables = tables_used or (extract_table_names(final_sql) if final_sql else [])

    # last_query_context 업데이트
    last_ctx = state.get("last_query_context")
    if last_ctx:
        updated_ctx = QueryContext(
            turn_number=last_ctx.turn_number,
            original_query=last_ctx.original_query,
            generated_sql=generated_sql,
            executed_sql=final_sql,
            result_count=result_count,
            patient_ids=patient_ids,
            conditions=conditions or [],
            tables_used=final_tables,
            timestamp=last_ctx.timestamp,
        )
        state["last_query_context"] = updated_ctx

        # query_history의 마지막 항목도 업데이트
        query_history = state.get("query_history", [])
        if query_history:
            query_history[-1] = updated_ctx
            state["query_history"] = query_history

    return state


def get_workflow_diagram() -> str:
    """워크플로우 다이어그램을 Mermaid 형식으로 반환합니다.

    Returns:
        Mermaid 다이어그램 문자열
    """
    return """
```mermaid
graph TD
    START((Start)) --> A[reference_detector]
    A --> B{has_references?}
    B -->|Yes| C[context_resolver]
    B -->|No| D[query_enricher]
    C --> D
    D --> E[state_updater]
    E --> END((End))

    style A fill:#e1f5fe
    style C fill:#fff3e0
    style D fill:#e8f5e9
    style E fill:#fce4ec
```
"""
