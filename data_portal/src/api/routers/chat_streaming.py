"""
AI Assistant Chat API — SSE streaming endpoint

PRD AAR-001 1-1: Natural Language Interface
- /chat/stream — SSE streaming chat
"""
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from typing import Optional, List, Dict, Any, AsyncGenerator
from datetime import datetime
import uuid
import httpx
import json

from core.config import settings

# Import shared state and helpers from chat_core
from routers.chat_core import (
    ChatRequest,
    sessions,
    detect_and_handle_schema_query,
    detect_and_query_imaging,
    extract_and_execute_sql,
    generate_fallback_response,
    _post_process_response,
    PROMPT_ENHANCEMENT_ENABLED,
    prompt_enhancement_service,
    save_chat_message,
)
import asyncio

router = APIRouter()


async def call_llm_streaming(
    message: str,
    history: List[Dict],
    context: Optional[Dict] = None,
    original_query: Optional[str] = None,
) -> AsyncGenerator[str, None]:
    """LLM streaming call - yield tokens"""
    enhancement_note = ""
    if original_query:
        enhancement_note = f"""
참고: 사용자의 원본 입력 "{original_query}"가 다음과 같이 자동 확장되었습니다:
"{message}"
확장된 질의를 기반으로 답변하되, 사용자에게 "(AI가 질의를 강화하고 있다)" 형태로 확장 과정을 먼저 알려주세요.
"""
    system_prompt = f"""당신은 서울아산병원 통합 데이터 플랫폼(IDP)의 AI 어시스턴트입니다.
사용자의 자연어 질문을 SQL로 변환하고 실행하여 결과를 알려줍니다.

## 중요: SQL 생성 규칙
- 데이터 질문에는 반드시 실행 가능한 PostgreSQL SQL을 ```sql 블록으로 작성하세요
- SQL은 시스템이 자동 실행하여 결과를 사용자에게 보여줍니다
- concept 테이블은 존재하지 않습니다. 절대 JOIN하지 마세요
- 진단 필터링: condition_occurrence.condition_source_value = 'SNOMED코드' 사용
- 성별 필터링: person.gender_source_value = 'M' 또는 'F' 사용
- 컬럼 별칭(alias)은 반드시 영문으로 작성하세요 (예: AS patient_count). 한글 별칭 금지

## 데이터베이스 스키마 (OMOP CDM, PostgreSQL)
### person (환자)
person_id BIGINT PK, gender_concept_id BIGINT, year_of_birth INT, month_of_birth INT, day_of_birth INT,
gender_source_value VARCHAR(50) -- 'M' 또는 'F'
### condition_occurrence (진단)
condition_occurrence_id BIGINT PK, person_id BIGINT FK, condition_concept_id BIGINT,
condition_start_date DATE, condition_end_date DATE, condition_source_value VARCHAR(50) -- SNOMED CT 코드
### visit_occurrence (방문)
visit_occurrence_id BIGINT PK, person_id BIGINT FK, visit_concept_id BIGINT,
visit_start_date DATE, visit_end_date DATE
### drug_exposure (약물)
drug_exposure_id BIGINT PK, person_id BIGINT FK, drug_concept_id BIGINT,
drug_exposure_start_date DATE, drug_exposure_end_date DATE, drug_source_value VARCHAR(100), quantity NUMERIC, days_supply INT
### measurement (검사)
measurement_id BIGINT PK, person_id BIGINT FK, measurement_concept_id BIGINT,
measurement_date DATE, value_as_number NUMERIC, measurement_source_value VARCHAR(100), unit_source_value VARCHAR(50)
### imaging_study (흉부X-ray)
imaging_study_id SERIAL PK, person_id INT FK, image_filename VARCHAR(200),
finding_labels VARCHAR(500), view_position VARCHAR(10), patient_age INT, patient_gender VARCHAR(2), image_url VARCHAR(500)

## SNOMED CT 코드 매핑
당뇨=44054006, 고혈압=38341003, 심방세동=49436004, 심근경색=22298006, 뇌졸중=230690007

## 이미지 표시
imaging_study 조회 시 마크다운 이미지: ![소견](image_url값)

{enhancement_note}
답변은 항상 한국어로 해주세요."""

    messages_payload = [{"role": "system", "content": system_prompt}]
    for msg in history[-10:]:
        messages_payload.append({"role": msg["role"], "content": msg["content"]})
    messages_payload.append({"role": "user", "content": message})

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            async with client.stream(
                "POST",
                f"{settings.LLM_API_URL}/chat/completions",
                json={
                    "model": settings.LLM_MODEL,
                    "messages": messages_payload,
                    "max_tokens": 2048,
                    "temperature": 0.7,
                    "stream": True,
                },
                headers=(
                    {"Authorization": f"Bearer {settings.OPENAI_API_KEY}"}
                    if settings.OPENAI_API_KEY
                    else {}
                ),
            ) as response:
                if response.status_code != 200:
                    yield generate_fallback_response(message)
                    return
                async for line in response.aiter_lines():
                    if not line.startswith("data: "):
                        continue
                    data_str = line[6:]
                    if data_str.strip() == "[DONE]":
                        break
                    try:
                        data = json.loads(data_str)
                        delta = data["choices"][0].get("delta", {})
                        content = delta.get("content", "")
                        if content:
                            yield content
                    except (json.JSONDecodeError, KeyError, IndexError):
                        pass
    except Exception:
        yield generate_fallback_response(message)


# ===== Endpoint =====

@router.post("/stream")
async def stream_message(request: ChatRequest):
    """SSE streaming AI chat endpoint"""

    async def event_generator() -> AsyncGenerator[str, None]:
        session_id = request.session_id or str(uuid.uuid4())
        if session_id not in sessions:
            sessions[session_id] = {
                "id": session_id,
                "user_id": request.user_id,
                "messages": [],
                "created_at": datetime.utcnow().isoformat(),
            }

        def sse(event_type: str, data: dict) -> str:
            payload = {
                "event_type": event_type,
                "data": data,
                "timestamp": datetime.utcnow().isoformat(),
                "session_id": session_id,
            }
            return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

        yield sse("session_start", {"session_id": session_id})

        # Prompt Enhancement
        original_query = request.message
        enhanced_query = request.message
        enhancement_applied = False

        if PROMPT_ENHANCEMENT_ENABLED and prompt_enhancement_service:
            try:
                prev_messages = sessions[session_id]["messages"][-5:]
                context_str = "\n".join(
                    [f"{m['role']}: {m['content']}" for m in prev_messages]
                ) if prev_messages else None
                enhancement_result = await prompt_enhancement_service.enhance(
                    query=request.message, context=context_str
                )
                enhanced_query = enhancement_result.enhanced_query
                enhancement_applied = enhancement_result.enhancement_applied
            except Exception:
                enhanced_query = original_query

        # Save user message
        message_id = str(uuid.uuid4())
        sessions[session_id]["messages"].append({
            "id": message_id,
            "role": "user",
            "content": request.message,
            "enhanced_content": enhanced_query if enhancement_applied else None,
            "timestamp": datetime.utcnow().isoformat(),
        })
        # DB 영속화 (user message)
        asyncio.ensure_future(save_chat_message(
            session_id, request.user_id or "anonymous", message_id,
            "user", request.message,
            enhanced_content=enhanced_query if enhancement_applied else None,
        ))

        yield sse("step_update", {"step": "질의 분석 중..."})

        # Schema query detection
        schema_result = detect_and_handle_schema_query(enhanced_query)
        tool_results: List[Dict[str, Any]] = []

        if schema_result:
            yield sse("token", {"content": schema_result})
            final_content = schema_result
        elif (imaging_result := await detect_and_query_imaging(enhanced_query)):
            yield sse("token", {"content": imaging_result["message"]})
            final_content = imaging_result["message"]
            tool_results = imaging_result["tool_results"]
        else:
            yield sse("step_update", {"step": "LLM 응답 생성 중..."})
            final_content = ""
            try:
                async for token in call_llm_streaming(
                    message=enhanced_query,
                    history=sessions[session_id]["messages"][:-1],
                    context=request.context,
                    original_query=original_query if enhancement_applied else None,
                ):
                    final_content += token
                    yield sse("token", {"content": token})

                # SQL auto-execute after streaming complete
                sql_result = await extract_and_execute_sql(final_content)
                if sql_result:
                    if "error" in sql_result:
                        extra = f"\n\n**SQL 실행 오류**: {sql_result['error']}"
                        final_content += extra
                        yield sse("token", {"content": extra})
                    elif sql_result.get("results"):
                        tool_results.append({
                            "columns": sql_result["columns"],
                            "results": sql_result["results"],
                        })
                        row_count = sql_result["row_count"]
                        if row_count == 1 and len(sql_result.get("columns", [])) == 1:
                            val = sql_result["results"][0][0]
                            prefix = f"\n\n**조회 결과: {val}**"
                        elif sql_result.get("auto_limited") and row_count >= 100:
                            prefix = f"\n\n**조회 결과: 상위 {row_count}건 표시** (더 많은 결과가 있을 수 있습니다)"
                        else:
                            prefix = f"\n\n**조회 결과: {row_count}건**"
                        final_content += prefix
                        yield sse("step_update", {"step": "SQL 실행 완료", "content": prefix})
                    elif sql_result.get("row_count") == 0:
                        extra = "\n\n*조회 결과가 없습니다.*"
                        final_content += extra
                        yield sse("token", {"content": extra})
            except Exception as e:
                error_msg = f"죄송합니다. 일시적인 오류가 발생했습니다: {str(e)}"
                final_content = error_msg
                yield sse("token", {"content": error_msg})

        # AI Ops post-processing (after streaming complete)
        sql_data = tool_results[0].get("results") if tool_results else None
        final_content = _post_process_response(
            response_text=final_content,
            user_id=request.user_id or "anonymous",
            query=request.message,
            query_type="sql" if sql_data else "chat",
            latency_ms=int((datetime.utcnow() - datetime.utcnow()).total_seconds() * 1000),
            sql_results=sql_data,
        )

        # Save assistant message
        assistant_msg_id = str(uuid.uuid4())
        sessions[session_id]["messages"].append({
            "id": assistant_msg_id,
            "role": "assistant",
            "content": final_content,
            "timestamp": datetime.utcnow().isoformat(),
        })
        # DB 영속화 (assistant message)
        asyncio.ensure_future(save_chat_message(
            session_id, request.user_id or "anonymous", assistant_msg_id,
            "assistant", final_content,
            tool_results=tool_results,
        ))

        yield sse("completion", {"tool_results": tool_results})

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
