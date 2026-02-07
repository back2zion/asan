"""
Conversation Memory API Router

LangGraph State Management 기반 다중 턴 대화 관리 API.
"""

import os
from typing import Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

# ConversationMemory 모듈 임포트
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../../.."))

from ai_services.conversation_memory import (
    ConversationState,
    create_conversation_workflow,
)
from ai_services.conversation_memory.state.conversation_state import (
    create_initial_state,
    QueryContext,
)
from ai_services.conversation_memory.graph.workflow import (
    compile_workflow,
    run_conversation_turn_sync,
)
from ai_services.conversation_memory.checkpointer.sqlite_checkpointer import (
    SQLiteCheckpointer,
)
from ai_services.conversation_memory.graph.workflow import record_query_result
from ai_services.xiyan_sql import XiYanSQLService, XIYAN_SQL_API_URL, XIYAN_SQL_MODEL
from services.sql_executor import sql_executor


router = APIRouter()

# XiYan SQL 서비스 초기화
xiyan_sql_service = XiYanSQLService(
    api_url=XIYAN_SQL_API_URL,
    model_name=XIYAN_SQL_MODEL,
)

# 체크포인터 초기화 (개발 환경: SQLite)
DB_PATH = os.getenv("CONVERSATION_MEMORY_DB", "conversation_memory.db")
checkpointer = SQLiteCheckpointer(DB_PATH)

# 워크플로우 컴파일
workflow = create_conversation_workflow()
app_workflow = compile_workflow(workflow)


# =============================================================================
# Request/Response Models
# =============================================================================

class StartConversationRequest(BaseModel):
    """대화 시작 요청"""
    user_id: str = Field(..., description="사용자 ID")
    initial_query: Optional[str] = Field(None, description="초기 질의 (선택)")


class StartConversationResponse(BaseModel):
    """대화 시작 응답"""
    thread_id: str
    user_id: str
    message: str


class ConversationQueryRequest(BaseModel):
    """대화 쿼리 요청"""
    thread_id: str = Field(..., description="대화 스레드 ID")
    query: str = Field(..., description="사용자 질의")


class ConversationQueryResponse(BaseModel):
    """대화 쿼리 응답"""
    thread_id: str
    turn_number: int
    original_query: str
    has_references: bool
    detected_references: list[dict]
    generated_sql: Optional[str]
    modified_sql: Optional[str]
    added_conditions: list[str]
    result_count: Optional[int]
    execution_results: Optional[list] = None
    execution_columns: Optional[list] = None
    execution_error: Optional[str] = None


class ConversationHistoryResponse(BaseModel):
    """대화 히스토리 응답"""
    thread_id: str
    user_id: str
    turn_count: int
    history: list[dict]


class ThreadListResponse(BaseModel):
    """스레드 목록 응답"""
    user_id: str
    threads: list[dict]


class RestoreResponse(BaseModel):
    """복원 응답"""
    thread_id: str
    restored_turn: int
    current_query: str
    message: str


# =============================================================================
# API Endpoints
# =============================================================================

@router.post("/conversation/start", response_model=StartConversationResponse)
async def start_conversation(request: StartConversationRequest):
    """새 대화 스레드를 시작합니다.

    Args:
        request: 대화 시작 요청 (user_id 필수)

    Returns:
        생성된 스레드 정보
    """
    try:
        thread_id = str(uuid4())

        # 체크포인터에 스레드 생성
        checkpointer.create_thread(request.user_id, thread_id)

        # 초기 상태 저장 (process_query에서 load_checkpoint 가능하도록)
        initial_state = create_initial_state(
            thread_id=thread_id,
            user_id=request.user_id,
            initial_query="",
        )
        checkpointer.save_checkpoint(thread_id, initial_state)

        # 초기 질의가 있으면 첫 턴 실행
        if request.initial_query:
            initial_state = create_initial_state(
                thread_id=thread_id,
                user_id=request.user_id,
                initial_query=request.initial_query,
            )
            result = run_conversation_turn_sync(
                app_workflow,
                thread_id,
                request.user_id,
                request.initial_query,
                None,
            )
            checkpointer.save_checkpoint(thread_id, result)

            if result.get("last_query_context"):
                checkpointer.save_query_context(thread_id, result["last_query_context"])

        return StartConversationResponse(
            thread_id=thread_id,
            user_id=request.user_id,
            message="대화가 시작되었습니다." + (
                f" 첫 번째 쿼리가 처리되었습니다." if request.initial_query else ""
            ),
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/conversation/query", response_model=ConversationQueryResponse)
async def process_query(request: ConversationQueryRequest):
    """대화 컨텍스트를 유지하며 쿼리를 처리합니다.

    "방금", "이전", "그 환자" 등의 참조 표현을 이해하고
    이전 쿼리 결과를 기반으로 SQL을 수정합니다.

    Args:
        request: 쿼리 요청 (thread_id, query)

    Returns:
        처리 결과 (생성된 SQL, 추가된 조건 등)
    """
    try:
        thread_id = request.thread_id
        query = request.query

        # 이전 상태 로드
        previous_state = checkpointer.load_checkpoint(thread_id)

        if previous_state is None:
            # 스레드가 없으면 에러
            raise HTTPException(
                status_code=404,
                detail=f"Thread not found: {thread_id}. Start a new conversation first.",
            )

        # 워크플로우 실행
        result = run_conversation_turn_sync(
            app_workflow,
            thread_id,
            previous_state.get("user_id", "unknown"),
            query,
            previous_state,
        )

        # 체크포인트 저장
        checkpointer.save_checkpoint(thread_id, result)

        # 쿼리 컨텍스트 저장
        if result.get("last_query_context"):
            checkpointer.save_query_context(thread_id, result["last_query_context"])

        # XiYan SQL로 SQL 생성
        generated_sql = None
        execution_results = None
        execution_columns = None
        execution_error = None
        result_count = None

        enriched_context = result.get("enriched_context")
        if enriched_context:
            try:
                generated_sql = await xiyan_sql_service.generate_sql_with_context(
                    enriched_context
                )

                # SQL 실행
                if generated_sql:
                    is_valid, error_msg = sql_executor.validate_sql(generated_sql)
                    if is_valid:
                        exec_result = await sql_executor.execute(generated_sql)
                        if exec_result.results:
                            execution_results = exec_result.results
                            execution_columns = exec_result.columns
                            result_count = exec_result.row_count
                        elif exec_result.natural_language_explanation:
                            execution_error = exec_result.natural_language_explanation
                    else:
                        execution_error = f"SQL 검증 실패: {error_msg}"

                # 생성된 SQL과 결과를 상태에 기록 (다음 턴 참조용)
                result = record_query_result(
                    result,
                    generated_sql=generated_sql,
                    result_count=result_count,
                )
                # 업데이트된 상태 저장
                checkpointer.save_checkpoint(thread_id, result)
                if result.get("last_query_context"):
                    checkpointer.save_query_context(thread_id, result["last_query_context"])
            except Exception as e:
                print(f"XiYan SQL generation/execution failed: {e}")
                execution_error = str(e)

        # 응답 생성
        detected_refs = [
            {
                "type": ref.ref_type.value,
                "text": ref.original_text,
                "resolved": ref.resolved_value,
            }
            for ref in result.get("detected_references", [])
        ]

        return ConversationQueryResponse(
            thread_id=thread_id,
            turn_number=result.get("turn_count", 0),
            original_query=query,
            has_references=result.get("has_references", False),
            detected_references=detected_refs,
            generated_sql=generated_sql,
            modified_sql=result.get("modified_sql"),
            added_conditions=result.get("added_conditions", []),
            result_count=result_count,
            execution_results=execution_results,
            execution_columns=execution_columns,
            execution_error=execution_error,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/conversation/{thread_id}/history", response_model=ConversationHistoryResponse)
async def get_conversation_history(thread_id: str, limit: int = 50):
    """대화 히스토리를 조회합니다.

    Args:
        thread_id: 대화 스레드 ID
        limit: 최대 조회 건수

    Returns:
        대화 히스토리
    """
    try:
        # 현재 상태 로드
        state = checkpointer.load_checkpoint(thread_id)

        if state is None:
            raise HTTPException(
                status_code=404,
                detail=f"Thread not found: {thread_id}",
            )

        # 쿼리 히스토리 조회
        history = checkpointer.get_query_history(thread_id, limit)

        history_data = [
            {
                "turn_number": ctx.turn_number,
                "original_query": ctx.original_query,
                "generated_sql": ctx.generated_sql,
                "executed_sql": ctx.executed_sql,
                "result_count": ctx.result_count,
                "conditions": ctx.conditions,
                "timestamp": ctx.timestamp.isoformat() if ctx.timestamp else None,
            }
            for ctx in history
        ]

        return ConversationHistoryResponse(
            thread_id=thread_id,
            user_id=state.get("user_id", "unknown"),
            turn_count=state.get("turn_count", 0),
            history=history_data,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/conversation/{thread_id}/restore/{turn}", response_model=RestoreResponse)
async def restore_to_turn(thread_id: str, turn: int):
    """특정 턴으로 대화 상태를 복원합니다.

    Args:
        thread_id: 대화 스레드 ID
        turn: 복원할 턴 번호

    Returns:
        복원 결과
    """
    try:
        # 해당 버전의 체크포인트 로드
        state = checkpointer.load_checkpoint(thread_id, checkpoint_version=turn)

        if state is None:
            raise HTTPException(
                status_code=404,
                detail=f"Checkpoint not found: thread={thread_id}, turn={turn}",
            )

        # 복원된 상태를 최신 체크포인트로 저장
        checkpointer.save_checkpoint(thread_id, state)

        return RestoreResponse(
            thread_id=thread_id,
            restored_turn=turn,
            current_query=state.get("current_query", ""),
            message=f"Turn {turn}로 복원되었습니다.",
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/conversation/threads", response_model=ThreadListResponse)
async def list_threads(user_id: str, limit: int = 20):
    """사용자의 대화 스레드 목록을 조회합니다.

    Args:
        user_id: 사용자 ID
        limit: 최대 조회 건수

    Returns:
        스레드 목록
    """
    try:
        threads = checkpointer.list_threads(user_id, limit)

        return ThreadListResponse(
            user_id=user_id,
            threads=threads,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/conversation/{thread_id}")
async def delete_thread(thread_id: str):
    """대화 스레드를 삭제합니다.

    Args:
        thread_id: 삭제할 스레드 ID

    Returns:
        삭제 결과
    """
    try:
        success = checkpointer.delete_thread(thread_id)

        if not success:
            raise HTTPException(
                status_code=404,
                detail=f"Thread not found: {thread_id}",
            )

        return {
            "success": True,
            "message": f"Thread {thread_id} deleted.",
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Health Check
# =============================================================================

@router.get("/conversation/health")
async def conversation_health():
    """Conversation Memory 서비스 상태를 확인합니다."""
    return {
        "status": "healthy",
        "service": "ConversationMemory",
        "checkpointer": "SQLite",
        "db_path": DB_PATH,
    }
