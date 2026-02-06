"""
AI Assistant Chat API

PRD AAR-001 1-1: Natural Language Interface
- Step 1: 불완전한 자연어 입력
- Step 2: Prompt Enhancement (자동확장)
- Step 3: SQL 생성
- Step 4~6: 실행 및 결과 표시
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import httpx
import sys

from core.config import settings

# Prompt Enhancement 모듈 import
sys.path.insert(0, "/home/babelai/datastreams-work/datastreams/asan")
try:
    from ai_services.prompt_enhancement import prompt_enhancement_service
    PROMPT_ENHANCEMENT_ENABLED = True
except ImportError:
    PROMPT_ENHANCEMENT_ENABLED = False
    prompt_enhancement_service = None

router = APIRouter()

# In-memory session storage (production에서는 Redis 사용)
sessions: Dict[str, Dict] = {}


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None
    user_id: Optional[str] = "anonymous"
    context: Optional[Dict[str, Any]] = None


class ChatResponse(BaseModel):
    session_id: str
    message_id: str
    assistant_message: str
    tool_results: List[Dict[str, Any]] = []
    suggested_actions: List[Dict[str, Any]] = []
    processing_time_ms: int
    # Prompt Enhancement 결과 (PRD AAR-001 Step 2)
    original_query: Optional[str] = None
    enhanced_query: Optional[str] = None
    enhancement_applied: bool = False
    enhancement_confidence: Optional[float] = None


@router.post("/chat", response_model=ChatResponse)
async def send_message(request: ChatRequest):
    """AI 어시스턴트에게 메시지 전송

    PRD AAR-001 1-1 워크플로우:
    Step 1: 불완전한 자연어 입력 수신
    Step 2: Prompt Enhancement (자동확장)
    Step 3~6: SQL 생성 및 실행 (추후 연동)
    """
    start_time = datetime.utcnow()

    # 세션 생성 또는 조회
    session_id = request.session_id or str(uuid.uuid4())
    if session_id not in sessions:
        sessions[session_id] = {
            "id": session_id,
            "user_id": request.user_id,
            "messages": [],
            "created_at": datetime.utcnow().isoformat(),
        }

    # ===== Step 2: Prompt Enhancement =====
    original_query = request.message
    enhanced_query = request.message
    enhancement_applied = False
    enhancement_confidence = None

    if PROMPT_ENHANCEMENT_ENABLED and prompt_enhancement_service:
        try:
            # 이전 대화 컨텍스트 구성
            prev_messages = sessions[session_id]["messages"][-5:]
            context_str = "\n".join([
                f"{m['role']}: {m['content']}"
                for m in prev_messages
            ]) if prev_messages else None

            # Prompt Enhancement 실행
            enhancement_result = await prompt_enhancement_service.enhance(
                query=request.message,
                context=context_str
            )

            enhanced_query = enhancement_result.enhanced_query
            enhancement_applied = enhancement_result.enhancement_applied
            enhancement_confidence = enhancement_result.confidence

            # 로깅
            if enhancement_applied:
                print(f"[Prompt Enhancement] '{original_query}' → '{enhanced_query}' (conf: {enhancement_confidence:.2f})")
        except Exception as e:
            print(f"[Prompt Enhancement Error] {e}")
            # 오류 시 원본 사용
            enhanced_query = original_query

    # 메시지 저장 (확장된 쿼리도 함께)
    message_id = str(uuid.uuid4())
    sessions[session_id]["messages"].append({
        "id": message_id,
        "role": "user",
        "content": request.message,
        "enhanced_content": enhanced_query if enhancement_applied else None,
        "timestamp": datetime.utcnow().isoformat(),
    })

    # LLM 호출 (확장된 쿼리 사용)
    try:
        assistant_message = await call_llm(
            message=enhanced_query,  # 확장된 쿼리 사용
            history=sessions[session_id]["messages"][:-1],
            context=request.context,
            original_query=original_query if enhancement_applied else None
        )
    except Exception as e:
        assistant_message = f"죄송합니다. 일시적인 오류가 발생했습니다: {str(e)}"

    # 응답 저장
    sessions[session_id]["messages"].append({
        "id": str(uuid.uuid4()),
        "role": "assistant",
        "content": assistant_message,
        "timestamp": datetime.utcnow().isoformat(),
    })

    processing_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

    return ChatResponse(
        session_id=session_id,
        message_id=message_id,
        assistant_message=assistant_message,
        tool_results=[],
        suggested_actions=get_suggested_actions(request.message),
        processing_time_ms=processing_time,
        # Prompt Enhancement 결과
        original_query=original_query,
        enhanced_query=enhanced_query if enhancement_applied else None,
        enhancement_applied=enhancement_applied,
        enhancement_confidence=enhancement_confidence,
    )


@router.get("/chat/sessions")
async def get_sessions(user_id: str = "anonymous"):
    """사용자의 세션 목록 조회"""
    user_sessions = [
        {"id": s["id"], "created_at": s["created_at"], "message_count": len(s["messages"])}
        for s in sessions.values()
        if s["user_id"] == user_id
    ]
    return {"sessions": user_sessions}


@router.get("/chat/sessions/{session_id}")
async def get_session(session_id: str):
    """세션 상세 조회"""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    return sessions[session_id]


@router.get("/chat/sessions/{session_id}/timeline")
async def get_timeline(session_id: str):
    """세션 타임라인 조회"""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    return {"timeline": sessions[session_id]["messages"]}


@router.post("/chat/sessions/{session_id}/restore/{message_id}")
async def restore_state(session_id: str, message_id: str):
    """특정 메시지 시점으로 상태 복원"""
    if session_id not in sessions:
        raise HTTPException(status_code=404, detail="Session not found")

    messages = sessions[session_id]["messages"]
    restored_messages = []
    for msg in messages:
        restored_messages.append(msg)
        if msg["id"] == message_id:
            break

    sessions[session_id]["messages"] = restored_messages
    return {"status": "restored", "message_count": len(restored_messages)}


async def call_llm(
    message: str,
    history: List[Dict],
    context: Optional[Dict] = None,
    original_query: Optional[str] = None
) -> str:
    """LLM API 호출

    Args:
        message: 사용자 메시지 (확장된 쿼리일 수 있음)
        history: 대화 기록
        context: 추가 컨텍스트
        original_query: 원본 쿼리 (확장된 경우)
    """
    # 시스템 프롬프트에 확장 정보 포함
    enhancement_note = ""
    if original_query:
        enhancement_note = f"""
참고: 사용자의 원본 입력 "{original_query}"가 다음과 같이 자동 확장되었습니다:
"{message}"

확장된 질의를 기반으로 답변하되, 사용자에게 "(AI가 질의를 강화하고 있다)" 형태로 확장 과정을 먼저 알려주세요.
"""

    system_prompt = f"""당신은 서울아산병원 통합 데이터 플랫폼(IDP)의 AI 어시스턴트입니다.
데이터 분석, SQL 쿼리 작성, 데이터 카탈로그 검색을 도와드립니다.

주요 기능:
1. 자연어를 SQL로 변환 (Text2SQL)
2. 데이터 카탈로그 검색
3. 데이터 분석 지원
4. ETL 파이프라인 모니터링
{enhancement_note}
답변은 항상 한국어로 해주세요."""

    messages = [{"role": "system", "content": system_prompt}]

    # 대화 기록 추가
    for msg in history[-10:]:  # 최근 10개만
        messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({"role": "user", "content": message})

    # OpenAI 호환 API 호출
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{settings.LLM_API_URL}/chat/completions",
                json={
                    "model": settings.LLM_MODEL,
                    "messages": messages,
                    "max_tokens": 2048,
                    "temperature": 0.7,
                },
                headers={"Authorization": f"Bearer {settings.OPENAI_API_KEY}"} if settings.OPENAI_API_KEY else {}
            )

            if response.status_code == 200:
                data = response.json()
                return data["choices"][0]["message"]["content"]
            else:
                # Fallback 응답
                return generate_fallback_response(message)
    except Exception:
        return generate_fallback_response(message)


def generate_fallback_response(message: str) -> str:
    """LLM 연결 실패 시 기본 응답"""
    message_lower = message.lower()

    if "sql" in message_lower or "쿼리" in message_lower:
        return """SQL 쿼리 작성을 도와드리겠습니다.

데이터 카탈로그에서 원하시는 테이블을 검색하시거나,
구체적인 요구사항을 말씀해 주시면 SQL을 생성해 드릴 수 있습니다.

예시:
- "환자 테이블에서 최근 1개월 입원 환자 조회"
- "진료과별 외래 환자 수 통계"
"""

    if "카탈로그" in message_lower or "검색" in message_lower or "테이블" in message_lower:
        return """데이터 카탈로그 검색을 도와드리겠습니다.

좌측 메뉴의 '데이터 카탈로그'에서 테이블과 컬럼을 검색하실 수 있습니다.
또는 여기서 직접 검색어를 입력해 주세요.

예시:
- "환자 관련 테이블"
- "진료 데이터"
- "검사 결과"
"""

    return """안녕하세요! 서울아산병원 IDP AI 어시스턴트입니다.

도움이 필요하신 작업을 말씀해 주세요:

1. **SQL 쿼리 작성** - 자연어로 원하는 데이터를 설명해 주세요
2. **데이터 검색** - 테이블이나 컬럼을 검색해 드립니다
3. **분석 지원** - 데이터 분석 방법을 안내해 드립니다

무엇을 도와드릴까요?"""


def get_suggested_actions(message: str) -> List[Dict[str, Any]]:
    """컨텍스트 기반 추천 액션"""
    actions = []

    if "sql" in message.lower() or "쿼리" in message.lower():
        actions.append({
            "type": "navigate",
            "label": "SQL 편집기 열기",
            "target": "/olap"
        })

    if "카탈로그" in message.lower() or "테이블" in message.lower():
        actions.append({
            "type": "navigate",
            "label": "데이터 카탈로그 열기",
            "target": "/catalog"
        })

    if "대시보드" in message.lower() or "시각화" in message.lower():
        actions.append({
            "type": "navigate",
            "label": "BI 대시보드 열기",
            "target": "/bi"
        })

    return actions
