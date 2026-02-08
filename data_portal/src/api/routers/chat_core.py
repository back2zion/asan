"""
AI Assistant Chat API — Core chat endpoint & shared state

PRD AAR-001 1-1: Natural Language Interface
- /chat — synchronous chat
- Shared: sessions dict, models, LLM helpers, SQL execution
"""
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime
import uuid
import httpx
import asyncio
import json
import os
import sys

from core.config import settings

# AI Ops: PII masking + audit log
try:
    from routers.ai_ops import mask_pii, verify_hallucination, append_audit_log
    AI_OPS_ENABLED = True
except ImportError:
    AI_OPS_ENABLED = False

# AAR-001: Auto query logging
try:
    from routers.catalog_analytics import log_query_to_catalog
    QUERY_LOG_ENABLED = True
except ImportError:
    QUERY_LOG_ENABLED = False

# Prompt Enhancement module import
sys.path.insert(0, "/home/babelai/datastreams-work/datastreams/asan")
try:
    from ai_services.prompt_enhancement import prompt_enhancement_service
    PROMPT_ENHANCEMENT_ENABLED = True
except ImportError:
    PROMPT_ENHANCEMENT_ENABLED = False
    prompt_enhancement_service = None

router = APIRouter()

# In-memory session storage (production: Redis)
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
    # Prompt Enhancement results (PRD AAR-001 Step 2)
    original_query: Optional[str] = None
    enhanced_query: Optional[str] = None
    enhancement_applied: bool = False
    enhancement_confidence: Optional[float] = None


# ===== Schema discovery detection =====

SCHEMA_DISCOVERY_KEYWORDS = ["테이블", "스키마", "컬럼", "필드", "구조"]
SCHEMA_ACTION_KEYWORDS = ["찾아", "찾기", "보여", "조회", "알려", "뭐가", "뭐야", "있나", "있어", "어디", "어떤"]

_SCHEMA_KEYWORD_MAP = {
    "진단": "condition_occurrence",
    "질병": "condition_occurrence",
    "질환": "condition_occurrence",
    "환자": "person",
    "방문": "visit_occurrence",
    "입원": "visit_occurrence",
    "외래": "visit_occurrence",
    "응급": "visit_occurrence",
    "약물": "drug_exposure",
    "처방": "drug_exposure",
    "투약": "drug_exposure",
    "검사": "measurement",
    "혈액": "measurement",
    "관찰": "observation",
    "시술": "procedure_occurrence",
    "수술": "procedure_occurrence",
    "영상": "imaging_study",
    "이미지": "imaging_study",
    "x-ray": "imaging_study",
    "xray": "imaging_study",
    "흉부": "imaging_study",
}


def detect_and_handle_schema_query(message: str) -> Optional[str]:
    """Detect schema/table search queries and return table metadata directly"""
    msg_lower = message.lower().replace(" ", "")

    has_schema_kw = any(kw in message for kw in SCHEMA_DISCOVERY_KEYWORDS)
    has_action_kw = any(kw in message for kw in SCHEMA_ACTION_KEYWORDS)

    if not (has_schema_kw and has_action_kw):
        return None

    matched_tables = set()
    for kw, table_name in _SCHEMA_KEYWORD_MAP.items():
        if kw in message.lower():
            matched_tables.add(table_name)

    try:
        from ai_services.xiyan_sql.schema import SAMPLE_TABLES
    except ImportError:
        return None

    if not matched_tables:
        md = ["**OMOP CDM 데이터베이스 테이블 목록**\n"]
        md.append("| 테이블명 | 한글명 | 설명 | 도메인 | 컬럼 수 |")
        md.append("|---------|-------|------|--------|--------|")
        for t in SAMPLE_TABLES:
            md.append(f"| `{t['physical_name']}` | {t['business_name']} | {t['description']} | {t['domain']} | {len(t['columns'])}개 |")
        md.append(f"\n총 **{len(SAMPLE_TABLES)}개** 테이블이 있습니다. 특정 테이블에 대해 자세히 알고 싶으시면 '진단 테이블 구조 보여줘'처럼 질문해 주세요.")
        return "\n".join(md)

    md = []
    for table_name in matched_tables:
        table_meta = next((t for t in SAMPLE_TABLES if t["physical_name"] == table_name), None)
        if not table_meta:
            continue
        md.append(f"### {table_meta['business_name']} (`{table_meta['physical_name']}`)")
        md.append(f"- **설명**: {table_meta['description']}")
        md.append(f"- **도메인**: {table_meta['domain']}")
        md.append(f"- **컬럼 수**: {len(table_meta['columns'])}개\n")
        md.append("| 컬럼명 | 한글명 | 타입 | PK | 설명 |")
        md.append("|--------|-------|------|-----|------|")
        for col in table_meta["columns"]:
            pk = "PK" if col.get("is_pk") else ""
            md.append(f"| `{col['physical_name']}` | {col['business_name']} | {col['data_type']} | {pk} | {col['description']} |")
        md.append("")

    if not md:
        return None

    header = f"**검색 결과: {len(matched_tables)}개 테이블**\n"
    return header + "\n".join(md)


IMAGING_KEYWORDS = ["영상", "이미지", "x-ray", "xray", "촬영", "흉부", "chest", "방사선", "엑스레이"]
OMOP_CONTAINER = os.getenv("OMOP_CONTAINER", "infra-omop-db-1")
OMOP_USER = os.getenv("OMOP_USER", "omopuser")
OMOP_DB = os.getenv("OMOP_DB", "omop_cdm")

# SQL forbidden keywords (read-only guarantee)
SQL_FORBIDDEN = ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
                 "GRANT", "REVOKE", "EXECUTE", "EXEC", "MERGE", "REPLACE"]


async def detect_and_query_imaging(message: str) -> Optional[Dict[str, Any]]:
    """Detect imaging queries and directly query imaging_study table"""
    msg_lower = message.lower()
    if not any(kw in msg_lower for kw in IMAGING_KEYWORDS):
        return None

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

    count_sql = f"SELECT COUNT(*) FROM imaging_study i {finding_filter};"
    sql = f"""
    SELECT i.imaging_study_id, i.person_id, i.image_filename, i.finding_labels,
           i.view_position, i.patient_age, i.patient_gender, i.image_url
    FROM imaging_study i
    {finding_filter}
    ORDER BY i.imaging_study_id
    LIMIT 50;
    """

    try:
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


async def extract_and_execute_sql(llm_response: str) -> Optional[Dict[str, Any]]:
    """Detect SQL in LLM response and auto-execute"""
    import re as _re

    sql = None
    for pattern in [
        r'```sql\s*\n(.*?)```',
        r'```\s*\n(SELECT.*?)```',
        r'(SELECT\s+[\s\S]+?;)',
    ]:
        match = _re.search(pattern, llm_response, _re.DOTALL | _re.IGNORECASE)
        if match:
            sql = match.group(1).strip()
            break

    if not sql:
        for line in llm_response.split('\n'):
            stripped = line.strip()
            if stripped.upper().startswith('SELECT ') or stripped.upper().startswith('WITH '):
                idx = llm_response.index(line)
                remaining = llm_response[idx:]
                sql_lines = []
                for sl in remaining.split('\n'):
                    s = sl.strip()
                    if not s and sql_lines:
                        break
                    if s:
                        sql_lines.append(sl)
                sql = '\n'.join(sql_lines).strip()
                break

    if not sql:
        return None

    sql_upper = sql.upper().strip().rstrip(';')
    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return None
    for kw in SQL_FORBIDDEN:
        if _re.search(rf'\b{kw}\b', sql_upper):
            return None

    sql = sql.strip().rstrip(';')
    if "LIMIT" not in sql.upper():
        sql += "\nLIMIT 100"

    try:
        col_sql = _re.sub(r'LIMIT\s+\d+', 'LIMIT 0', sql, flags=_re.IGNORECASE)
        col_cmd = [
            "docker", "exec", OMOP_CONTAINER,
            "psql", "-U", OMOP_USER, "-d", OMOP_DB, "-c", col_sql
        ]
        col_proc = await asyncio.create_subprocess_exec(
            *col_cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        col_out, _ = await asyncio.wait_for(col_proc.communicate(), timeout=5.0)
        columns = []
        if col_proc.returncode == 0 and col_out:
            first_line = col_out.decode('utf-8').strip().split('\n')[0]
            columns = [c.strip() for c in first_line.split('|') if c.strip()]

        cmd = [
            "docker", "exec", OMOP_CONTAINER,
            "psql", "-U", OMOP_USER, "-d", OMOP_DB,
            "-t", "-A", "-F", "\t", "-c", sql
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10.0)

        if process.returncode != 0:
            error = stderr.decode('utf-8').strip()
            return {"error": error, "sql": sql}

        output = stdout.decode('utf-8').strip()
        if not output:
            return {"results": [], "columns": columns, "sql": sql, "row_count": 0}

        rows = []
        for line in output.split('\n'):
            if line.strip():
                row = line.split('\t')
                parsed = []
                for val in row:
                    try:
                        parsed.append(float(val) if '.' in val else int(val))
                    except ValueError:
                        parsed.append(val if val else None)
                rows.append(parsed)

        return {
            "results": rows,
            "columns": columns,
            "sql": sql,
            "row_count": len(rows),
        }
    except asyncio.TimeoutError:
        return {"error": "SQL 실행 시간 초과 (10초)", "sql": sql}
    except Exception as e:
        print(f"[SQL Auto-Execute Error] {e}")
        return None


async def call_llm(
    message: str,
    history: List[Dict],
    context: Optional[Dict] = None,
    original_query: Optional[str] = None
) -> str:
    """LLM API call"""
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

### person (환자 1,130명)
person_id BIGINT PK, gender_concept_id BIGINT, year_of_birth INT, month_of_birth INT, day_of_birth INT,
gender_source_value VARCHAR(50) -- 'M' 또는 'F'

### condition_occurrence (진단 7,900건)
condition_occurrence_id BIGINT PK, person_id BIGINT FK, condition_concept_id BIGINT,
condition_start_date DATE, condition_end_date DATE,
condition_source_value VARCHAR(50) -- SNOMED CT 코드 (아래 참조)

### visit_occurrence (방문 32,153건)
visit_occurrence_id BIGINT PK, person_id BIGINT FK, visit_concept_id BIGINT,
visit_start_date DATE, visit_end_date DATE

### drug_exposure (약물 13,799건)
drug_exposure_id BIGINT PK, person_id BIGINT FK, drug_concept_id BIGINT,
drug_exposure_start_date DATE, drug_exposure_end_date DATE,
drug_source_value VARCHAR(100), quantity NUMERIC, days_supply INT

### measurement (검사 170,043건)
measurement_id BIGINT PK, person_id BIGINT FK, measurement_concept_id BIGINT,
measurement_date DATE, value_as_number NUMERIC,
measurement_source_value VARCHAR(100), unit_source_value VARCHAR(50)

### observation (관찰 7,899건)
observation_id BIGINT PK, person_id BIGINT FK, observation_concept_id BIGINT,
observation_date DATE, observation_source_value VARCHAR(50)

### imaging_study (흉부X-ray 112,120건)
imaging_study_id SERIAL PK, person_id INT FK, image_filename VARCHAR(200),
finding_labels VARCHAR(500), view_position VARCHAR(10), patient_age INT,
patient_gender VARCHAR(2), image_url VARCHAR(500)

## SNOMED CT 코드 매핑 (condition_source_value 값)
- 당뇨: '44054006'
- 고혈압: '38341003'
- 심방세동: '49436004'
- 심근경색: '22298006'
- 뇌졸중: '230690007'
- 관상동맥질환: '53741008'
- 천식: '195967001'

## 테이블 조인
- 모든 테이블은 person_id로 person 테이블과 조인

## 이미지 표시
imaging_study 조회 시 마크다운 이미지: ![소견](image_url값)

{enhancement_note}
답변은 항상 한국어로 해주세요."""

    messages = [{"role": "system", "content": system_prompt}]

    for msg in history[-10:]:
        messages.append({"role": msg["role"], "content": msg["content"]})

    messages.append({"role": "user", "content": message})

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
                return generate_fallback_response(message)
    except Exception:
        return generate_fallback_response(message)


def _post_process_response(
    response_text: str,
    user_id: str,
    query: str,
    model: str = "qwen3-32b",
    query_type: str = "chat",
    latency_ms: int = 0,
    sql_results: Optional[List] = None,
) -> str:
    """Post-process: PII masking + hallucination check + audit log"""
    if not AI_OPS_ENABLED:
        return response_text

    masked_text, pii_count = mask_pii(response_text)

    hall_result = verify_hallucination(masked_text, sql_results)
    hall_status = hall_result.get("status", "skipped")

    try:
        token_estimate = len(masked_text) // 4
        append_audit_log({
            "user": user_id,
            "model": model,
            "query_type": query_type,
            "query": query[:500],
            "latency_ms": latency_ms,
            "tokens": token_estimate,
            "pii_count": pii_count,
            "hallucination_status": hall_status,
        })
    except Exception:
        pass

    return masked_text


def generate_fallback_response(message: str) -> str:
    """Default response when LLM connection fails"""
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
    """Context-based suggested actions"""
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

    imaging_keywords = ["영상", "이미지", "x-ray", "촬영", "흉부", "chest", "방사선", "xray"]
    if any(kw in msg_lower for kw in imaging_keywords):
        actions.append({
            "type": "navigate",
            "label": "흉부 X-ray 영상 조회",
            "target": "/cdw"
        })

    return actions


# ===== Endpoints =====

@router.post("/", response_model=ChatResponse)
async def send_message(request: ChatRequest):
    """AI 어시스턴트에게 메시지 전송

    PRD AAR-001 1-1 워크플로우:
    Step 1: 불완전한 자연어 입력 수신
    Step 2: Prompt Enhancement (자동확장)
    Step 3~6: SQL 생성 및 실행 (추후 연동)
    """
    start_time = datetime.utcnow()

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
            prev_messages = sessions[session_id]["messages"][-5:]
            context_str = "\n".join([
                f"{m['role']}: {m['content']}"
                for m in prev_messages
            ]) if prev_messages else None

            enhancement_result = await prompt_enhancement_service.enhance(
                query=request.message,
                context=context_str
            )

            enhanced_query = enhancement_result.enhanced_query
            enhancement_applied = enhancement_result.enhancement_applied
            enhancement_confidence = enhancement_result.confidence

            if enhancement_applied:
                print(f"[Prompt Enhancement] '{original_query}' -> '{enhanced_query}' (conf: {enhancement_confidence:.2f})")
        except Exception as e:
            print(f"[Prompt Enhancement Error] {e}")
            enhanced_query = original_query

    message_id = str(uuid.uuid4())
    sessions[session_id]["messages"].append({
        "id": message_id,
        "role": "user",
        "content": request.message,
        "enhanced_content": enhanced_query if enhancement_applied else None,
        "timestamp": datetime.utcnow().isoformat(),
    })

    schema_result = detect_and_handle_schema_query(enhanced_query)
    tool_results: List[Dict[str, Any]] = []

    if schema_result:
        assistant_message = schema_result
    elif (imaging_result := await detect_and_query_imaging(enhanced_query)):
        assistant_message = imaging_result["message"]
        tool_results = imaging_result["tool_results"]
    else:
        try:
            assistant_message = await call_llm(
                message=enhanced_query,
                history=sessions[session_id]["messages"][:-1],
                context=request.context,
                original_query=original_query if enhancement_applied else None
            )
        except Exception as e:
            assistant_message = f"죄송합니다. 일시적인 오류가 발생했습니다: {str(e)}"

        sql_result = await extract_and_execute_sql(assistant_message)
        if sql_result:
            if "error" in sql_result:
                assistant_message += f"\n\n**SQL 실행 오류**: {sql_result['error']}"
            elif sql_result.get("results"):
                tool_results.append({
                    "columns": sql_result["columns"],
                    "results": sql_result["results"],
                })
                row_count = sql_result["row_count"]
                if row_count == 1 and len(sql_result.get("columns", [])) == 1:
                    val = sql_result["results"][0][0]
                    assistant_message = f"**조회 결과: {val}**\n\n{assistant_message}"
                else:
                    assistant_message = f"**조회 결과: {row_count}건**\n\n{assistant_message}"
            elif sql_result.get("row_count") == 0:
                assistant_message += "\n\n*조회 결과가 없습니다.*"

    processing_time = int((datetime.utcnow() - start_time).total_seconds() * 1000)

    # AAR-001: Auto query logging to catalog_query_log
    if QUERY_LOG_ENABLED and tool_results:
        try:
            sql_executed = None
            row_count = 0
            for tr in tool_results:
                if tr.get("results"):
                    row_count = len(tr["results"])
                    break
            # Extract SQL from LLM response if present
            import re as _re_log
            sql_match = _re_log.search(r'```sql\s*\n(.*?)```', assistant_message, _re_log.DOTALL)
            sql_executed = sql_match.group(1).strip() if sql_match else None
            # Extract table names from SQL
            tables = []
            if sql_executed:
                table_pattern = _re_log.findall(r'\bFROM\s+(\w+)|\bJOIN\s+(\w+)', sql_executed, _re_log.IGNORECASE)
                tables = list(set(t for pair in table_pattern for t in pair if t))
            asyncio.ensure_future(log_query_to_catalog(
                user_id=request.user_id or "anonymous",
                query_text=request.message,
                query_type="chat_sql",
                tables_accessed=tables,
                response_time_ms=processing_time,
                result_count=row_count,
            ))
        except Exception:
            pass

    # AI Ops post-processing
    sql_data = tool_results[0].get("results") if tool_results else None
    assistant_message = _post_process_response(
        response_text=assistant_message,
        user_id=request.user_id or "anonymous",
        query=request.message,
        query_type="sql" if sql_data else "chat",
        latency_ms=processing_time,
        sql_results=sql_data,
    )

    sessions[session_id]["messages"].append({
        "id": str(uuid.uuid4()),
        "role": "assistant",
        "content": assistant_message,
        "timestamp": datetime.utcnow().isoformat(),
    })

    return ChatResponse(
        session_id=session_id,
        message_id=message_id,
        assistant_message=assistant_message,
        tool_results=tool_results,
        suggested_actions=get_suggested_actions(request.message),
        processing_time_ms=processing_time,
        original_query=original_query,
        enhanced_query=enhanced_query if enhancement_applied else None,
        enhancement_applied=enhancement_applied,
        enhancement_confidence=enhancement_confidence,
    )
