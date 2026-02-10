"""
비정형 데이터 처리 파이프라인 — 헬퍼 함수 및 모델
(unstructured.py 에서 추출)
"""
import io
import json
import logging
from typing import List, Optional

import httpx
from pydantic import BaseModel

from core.config import settings

logger = logging.getLogger(__name__)

# ── 외부 서비스 URL ──
NER_SERVICE_URL = "http://localhost:28100"

# ── Pydantic 모델 ──


class TextProcessRequest(BaseModel):
    text: str
    source_type: Optional[str] = "경과기록"
    person_id: Optional[int] = None


class JobSummary(BaseModel):
    job_id: int
    job_type: str
    source_type: Optional[str]
    status: str
    input_summary: Optional[str]
    result_count: int
    omop_records_created: int
    processing_time_ms: Optional[int]
    created_at: Optional[str]


# ── DB helpers ──

_tbl_ok = False


async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()


async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)


async def _ensure_tables():
    global _tbl_ok
    if _tbl_ok:
        return
    conn = await _get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS unstructured_job (
                job_id SERIAL PRIMARY KEY,
                job_type VARCHAR(30) NOT NULL,
                source_type VARCHAR(50),
                status VARCHAR(20) DEFAULT 'pending',
                input_summary VARCHAR(500),
                s3_key VARCHAR(500),
                result_count INT DEFAULT 0,
                result_json JSONB,
                omop_records_created INT DEFAULT 0,
                processing_time_ms INT,
                error_message TEXT,
                created_by VARCHAR(100) DEFAULT 'system',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            );
            CREATE INDEX IF NOT EXISTS idx_unstructured_job_type ON unstructured_job(job_type);
            CREATE INDEX IF NOT EXISTS idx_unstructured_job_status ON unstructured_job(status);
        """)
        _tbl_ok = True
    finally:
        await _rel(conn)


# ── Internal helper functions ──


async def _llm_section_split(text: str) -> dict:
    """Qwen3 LLM으로 임상노트 섹션 분리"""
    llm_base = settings.LLM_API_URL.rstrip("/")
    url = f"{llm_base}/chat/completions" if llm_base.endswith("/v1") else f"{llm_base}/v1/chat/completions"

    prompt = f"""다음 임상노트 텍스트를 섹션별로 분리하세요.

텍스트:
{text}

다음 JSON 형식으로 응답하세요 (JSON만, 다른 텍스트 없이):
{{
  "chief_complaint": "주소/주호소",
  "present_illness": "현병력",
  "findings": "소견/검사결과",
  "assessment": "평가/진단",
  "plan": "계획/처방"
}}
값이 없는 섹션은 빈 문자열로 남겨두세요."""

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                url,
                json={
                    "model": settings.LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": "/no_think"},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.2,
                    "max_tokens": 1000,
                },
            )
            if resp.status_code == 200:
                import re
                content = resp.json()["choices"][0]["message"]["content"]
                match = re.search(r'\{[\s\S]*\}', content)
                if match:
                    return json.loads(match.group())
    except Exception as e:
        logger.warning(f"LLM section split failed: {e}")
    return {"raw": text}


async def _call_ner(text: str) -> List[dict]:
    """BioClinicalBERT NER 호출"""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{NER_SERVICE_URL}/ner/analyze",
                json={"text": text},
            )
            if resp.status_code == 200:
                return resp.json().get("entities", [])
    except Exception as e:
        logger.warning(f"NER call failed: {e}")
    return []


async def _save_to_s3(key: str, data: bytes, content_type: str = "text/plain") -> bool:
    """MinIO에 파일 저장"""
    try:
        from services.s3_service import get_s3_client
        s3 = get_s3_client()
        s3.put_object("idp-documents", key, io.BytesIO(data), len(data), content_type=content_type)
        return True
    except Exception as e:
        logger.warning(f"S3 save failed: {e}")
        return False


async def _insert_omop_note(conn, text: str, source_type: str, person_id: Optional[int], sections: dict) -> int:
    """OMOP note 테이블에 적재 -- 기존 OMOP CDM 스키마 사용. 반환: note_id"""
    # note_type_concept_id 매핑
    type_map = {
        "경과기록": 44814637,
        "병리보고서": 44814638,
        "영상소견": 44814639,
        "퇴원요약": 44814640,
        "수술기록": 44814641,
    }
    type_id = type_map.get(source_type, 0)

    # note_id: 기존 테이블에 시퀀스 없음 -> COALESCE(MAX+1, 1) 사용
    next_id = await conn.fetchval("SELECT COALESCE(MAX(note_id), 0) + 1 FROM note")

    # FK defaults: person=1, provider=1, visit_occurrence=1, visit_detail=1
    await conn.execute(
        """INSERT INTO note
           (note_id, note_event_id, note_event_field_concept_id,
            note_date, note_datetime, note_type_concept_id, note_class_concept_id,
            note_title, note_text,
            encoding_concept_id, language_concept_id, note_source_value,
            person_person_id, provider_provider_id,
            visit_occurrence_visit_occurrence_id, visit_detail_visit_detail_id)
           VALUES ($1, 0, 0,
                   CURRENT_DATE, NOW(), $2, 0,
                   $3, $4,
                   0, 0, $5,
                   $6, 1, 1, 1)""",
        next_id, type_id,
        source_type, text[:800],
        json.dumps(sections, ensure_ascii=False)[:50],
        person_id or 1,
    )
    return next_id


async def _insert_omop_note_nlp(conn, note_id: int, entities: List[dict]) -> int:
    """OMOP note_nlp 테이블에 NER 결과 적재 -- 기존 OMOP CDM 스키마 사용. 반환: 적재 건수"""
    # note_nlp_id: 시퀀스 없음 -> COALESCE(MAX+1, 1)
    base_id = await conn.fetchval("SELECT COALESCE(MAX(note_nlp_id), 0) FROM note_nlp")
    count = 0
    for ent in entities:
        base_id += 1
        await conn.execute(
            """INSERT INTO note_nlp
               (note_nlp_id, note_note_id, section_concept_id,
                snippet, "offset", lexical_variant,
                note_nlp_concept_id, note_nlp_source_concept_id,
                nlp_system, nlp_date, nlp_datetime,
                term_exists, term_temporal, term_modifiers)
               VALUES ($1, $2, 0,
                       $3, $4, $5,
                       0, 0,
                       $6, CURRENT_DATE, NOW(),
                       'Y', NULL, $7)""",
            base_id, note_id,
            ent.get("text", "")[:250],
            f"{ent.get('start', 0)}-{ent.get('end', 0)}",
            ent.get("omopConcept", "")[:250],
            "BioClinicalBERT",
            json.dumps({
                "type": ent.get("type"),
                "standardCode": ent.get("standardCode"),
                "codeSystem": ent.get("codeSystem"),
                "confidence": ent.get("confidence"),
            }, ensure_ascii=False)[:2000],
        )
        count += 1
    return count
