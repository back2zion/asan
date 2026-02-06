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
import asyncio
import os
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

    # 이미징 질의 감지 → DB 직접 조회
    imaging_result = await detect_and_query_imaging(enhanced_query)
    tool_results: List[Dict[str, Any]] = []

    if imaging_result:
        assistant_message = imaging_result["message"]
        tool_results = imaging_result["tool_results"]
    else:
        # LLM 호출 (확장된 쿼리 사용)
        try:
            assistant_message = await call_llm(
                message=enhanced_query,
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
        tool_results=tool_results,
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


IMAGING_KEYWORDS = ["영상", "이미지", "x-ray", "xray", "촬영", "흉부", "chest", "방사선", "엑스레이"]
OMOP_CONTAINER = os.getenv("OMOP_CONTAINER", "infra-omop-db-1")
OMOP_USER = os.getenv("OMOP_USER", "omopuser")
OMOP_DB = os.getenv("OMOP_DB", "omop_cdm")


async def detect_and_query_imaging(message: str) -> Optional[Dict[str, Any]]:
    """이미징 관련 질의 감지 시 imaging_study 테이블 직접 조회"""
    msg_lower = message.lower()
    if not any(kw in msg_lower for kw in IMAGING_KEYWORDS):
        return None

    # finding 필터 추출
    finding_filter = ""
    finding_keywords = {
        "심비대": "Cardiomegaly", "cardiomegaly": "Cardiomegaly",
        "폐기종": "Emphysema", "emphysema": "Emphysema",
        "침윤": "Infiltration", "infiltration": "Infiltration",
        "흉수": "Effusion", "effusion": "Effusion",
        "무기폐": "Atelectasis", "atelectasis": "Atelectasis",
        "기흉": "Pneumothorax", "pneumothorax": "Pneumothorax",
        "종괴": "Mass", "mass": "Mass",
        "결절": "Nodule", "nodule": "Nodule",
        "경화": "Consolidation", "consolidation": "Consolidation",
        "부종": "Edema", "edema": "Edema",
        "섬유화": "Fibrosis", "fibrosis": "Fibrosis",
        "폐렴": "Pneumonia", "pneumonia": "Pneumonia",
    }
    for kor, eng in finding_keywords.items():
        if kor in msg_lower:
            finding_filter = f"WHERE i.finding_labels ILIKE '%{eng}%'"
            break

    # 전체 건수 조회
    count_sql = f"SELECT COUNT(*) FROM imaging_study i {finding_filter};"
    # 데이터 조회 (최대 50건)
    sql = f"""
    SELECT i.imaging_study_id, i.person_id, i.image_filename, i.finding_labels,
           i.view_position, i.patient_age, i.patient_gender, i.image_url
    FROM imaging_study i
    {finding_filter}
    ORDER BY i.imaging_study_id
    LIMIT 50;
    """

    try:
        # 전체 건수 조회
        count_cmd = [
            "docker", "exec", OMOP_CONTAINER,
            "psql", "-U", OMOP_USER, "-d", OMOP_DB,
            "-t", "-A", "-c", count_sql
        ]
        count_proc = await asyncio.create_subprocess_exec(
            *count_cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        count_out, _ = await asyncio.wait_for(count_proc.communicate(), timeout=10.0)
        total_count = int(count_out.decode().strip()) if count_proc.returncode == 0 else 0

        # 데이터 조회
        cmd = [
            "docker", "exec", OMOP_CONTAINER,
            "psql", "-U", OMOP_USER, "-d", OMOP_DB,
            "-t", "-A", "-F", "\t", "-c", sql
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10.0)

        if process.returncode != 0:
            return None

        output = stdout.decode().strip()
        if not output:
            return None

        columns = ["imaging_study_id", "person_id", "image_filename",
                    "finding_labels", "view_position", "patient_age",
                    "patient_gender", "image_url"]
        rows = []
        for line in output.split("\n"):
            if line.strip():
                rows.append(line.split("\t"))

        # 마크다운 응답 생성
        fetched = len(rows)
        md_lines = [f"**흉부 X-ray 영상 조회 결과** (전체 {total_count:,}건 중 {fetched}건 표시)\n"]

        for row in rows[:4]:
            findings = row[3] if len(row) > 3 else ""
            view = row[4] if len(row) > 4 else ""
            age = row[5] if len(row) > 5 else ""
            gender = row[6] if len(row) > 6 else ""
            url = row[7] if len(row) > 7 else ""
            md_lines.append(f"**환자 {row[1]}** | {age}세 {gender} | {findings} ({view})")
            md_lines.append(f"![{findings}]({url})\n")

        if fetched > 4:
            md_lines.append(f"... 외 {fetched - 4}건이 더 있습니다.")

        md_lines.append(f"\n좌측 **CDW 연구지원** 메뉴에서 자연어 질의로 전체 {total_count:,}건을 조회할 수 있습니다.")

        return {
            "message": "\n".join(md_lines),
            "tool_results": [{
                "columns": columns,
                "results": rows,
            }],
        }

    except Exception as e:
        print(f"[Imaging Query Error] {e}")
        return None


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
5. 흉부 X-ray 영상 조회 지원

## 사용 가능한 데이터베이스 테이블:
- person: 환자 정보 (person_id, year_of_birth, gender_concept_id 등)
- condition_occurrence: 진단 기록
- visit_occurrence: 방문 기록
- drug_exposure: 약물 처방
- measurement: 검사 결과
- observation: 관찰 기록
- imaging_study: 흉부 X-ray 영상 (imaging_study_id, person_id, image_filename, finding_labels, view_position, patient_age, patient_gender, image_url)

## 이미지 표시 방법:
imaging_study 테이블의 image_url 컬럼에는 이미지 경로가 저장되어 있습니다.
영상/이미지 관련 질문에는 마크다운 이미지 문법으로 이미지를 포함해주세요:
![설명](/api/v1/imaging/images/파일명.png)

예시 - 사용자가 "흉부 X-ray 환자 보여줘"라고 하면:
1. imaging_study 테이블에서 조회할 SQL을 안내하고
2. 대표 이미지를 마크다운으로 표시: ![Cardiomegaly PA](/api/v1/imaging/images/00000001_000.png)
3. "CDW 연구지원 페이지에서 자연어 질의로 전체 목록을 조회할 수 있습니다"라고 안내

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
    msg_lower = message.lower()

    if "sql" in msg_lower or "쿼리" in msg_lower:
        actions.append({
            "type": "navigate",
            "label": "SQL 편집기 열기",
            "target": "/governance"
        })

    if "카탈로그" in msg_lower or "테이블" in msg_lower:
        actions.append({
            "type": "navigate",
            "label": "데이터 카탈로그 열기",
            "target": "/catalog"
        })

    if "대시보드" in msg_lower or "시각화" in msg_lower:
        actions.append({
            "type": "navigate",
            "label": "BI 대시보드 열기",
            "target": "/bi"
        })

    # 이미징 관련 키워드
    imaging_keywords = ["영상", "이미지", "x-ray", "촬영", "흉부", "chest", "방사선", "xray"]
    if any(kw in msg_lower for kw in imaging_keywords):
        actions.append({
            "type": "navigate",
            "label": "흉부 X-ray 영상 조회",
            "target": "/cdw"
        })

    return actions
