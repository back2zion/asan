"""
비정형 데이터 처리 파이프라인 라우터
- 임상노트 텍스트 구조화 (LLM 섹션분리 + NER + OMOP 적재)
- DICOM 메타데이터 추출 + OMOP 적재
- 처리 작업 관리 및 통계
"""
import io
import json
import logging
import time as _time
from datetime import datetime, timezone
from typing import List, Optional

import httpx
from fastapi import APIRouter, File, HTTPException, UploadFile
from pydantic import BaseModel

from core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/unstructured", tags=["Unstructured"])

# ── 외부 서비스 URL ──
NER_SERVICE_URL = "http://localhost:28100"

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


# ── 1. Health ──

@router.get("/health")
async def pipeline_health():
    """비정형 처리 파이프라인 상태 확인 (NER, LLM, MinIO, OMOP)"""
    checks = {}

    # NER
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{NER_SERVICE_URL}/ner/health")
            checks["ner"] = "healthy" if r.status_code == 200 else "unhealthy"
    except Exception:
        checks["ner"] = "unhealthy"

    # LLM (Qwen3)
    try:
        llm_base = settings.LLM_API_URL.rstrip("/")
        url = f"{llm_base}/models" if llm_base.endswith("/v1") else f"{llm_base}/v1/models"
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(url)
            checks["llm"] = "healthy" if r.status_code == 200 else "unhealthy"
    except Exception:
        checks["llm"] = "unhealthy"

    # MinIO
    try:
        from services.s3_service import get_s3_client
        s3 = get_s3_client()
        s3.bucket_exists("idp-documents")
        checks["minio"] = "healthy"
    except Exception:
        checks["minio"] = "unhealthy"

    # OMOP DB
    try:
        conn = await _get_conn()
        try:
            await conn.fetchval("SELECT 1")
            checks["omop_db"] = "healthy"
        finally:
            await _rel(conn)
    except Exception:
        checks["omop_db"] = "unhealthy"

    # pydicom
    try:
        import pydicom  # noqa: F401
        checks["pydicom"] = "installed"
    except ImportError:
        checks["pydicom"] = "not_installed"

    overall = "healthy" if all(v in ("healthy", "installed") for v in checks.values()) else "degraded"
    return {"status": overall, "checks": checks}


# ── 2. Jobs 목록 ──

@router.get("/jobs")
async def list_jobs(limit: int = 50, offset: int = 0, status: Optional[str] = None):
    """처리 작업 목록"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        if status:
            rows = await conn.fetch(
                """SELECT job_id, job_type, source_type, status, input_summary,
                          result_count, omop_records_created, processing_time_ms, created_at
                   FROM unstructured_job
                   WHERE status = $1
                   ORDER BY created_at DESC LIMIT $2 OFFSET $3""",
                status, limit, offset,
            )
        else:
            rows = await conn.fetch(
                """SELECT job_id, job_type, source_type, status, input_summary,
                          result_count, omop_records_created, processing_time_ms, created_at
                   FROM unstructured_job
                   ORDER BY created_at DESC LIMIT $1 OFFSET $2""",
                limit, offset,
            )
        return [
            {
                "job_id": r["job_id"],
                "job_type": r["job_type"],
                "source_type": r["source_type"],
                "status": r["status"],
                "input_summary": r["input_summary"],
                "result_count": r["result_count"],
                "omop_records_created": r["omop_records_created"],
                "processing_time_ms": r["processing_time_ms"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    finally:
        await _rel(conn)


# ── 3. Job 상세 ──

@router.get("/jobs/{job_id}")
async def get_job(job_id: int):
    """처리 작업 상세 결과"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        r = await conn.fetchrow(
            "SELECT * FROM unstructured_job WHERE job_id = $1", job_id,
        )
        if not r:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        return {
            "job_id": r["job_id"],
            "job_type": r["job_type"],
            "source_type": r["source_type"],
            "status": r["status"],
            "input_summary": r["input_summary"],
            "s3_key": r["s3_key"],
            "result_count": r["result_count"],
            "result_json": json.loads(r["result_json"]) if r["result_json"] else None,
            "omop_records_created": r["omop_records_created"],
            "processing_time_ms": r["processing_time_ms"],
            "error_message": r["error_message"],
            "created_by": r["created_by"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
        }
    finally:
        await _rel(conn)


# ── 4. 텍스트 구조화 ──

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
    """OMOP note 테이블에 적재 — 기존 OMOP CDM 스키마 사용. 반환: note_id"""
    # note_type_concept_id 매핑
    type_map = {
        "경과기록": 44814637,
        "병리보고서": 44814638,
        "영상소견": 44814639,
        "퇴원요약": 44814640,
        "수술기록": 44814641,
    }
    type_id = type_map.get(source_type, 0)

    # note_id: 기존 테이블에 시퀀스 없음 → COALESCE(MAX+1, 1) 사용
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
    """OMOP note_nlp 테이블에 NER 결과 적재 — 기존 OMOP CDM 스키마 사용. 반환: 적재 건수"""
    # note_nlp_id: 시퀀스 없음 → COALESCE(MAX+1, 1)
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


@router.post("/process/text")
async def process_text(req: TextProcessRequest):
    """임상노트 텍스트 구조화 (LLM 섹션분리 + NER + OMOP 적재)"""
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="텍스트를 입력하세요")

    await _ensure_tables()
    start = _time.monotonic()
    conn = await _get_conn()

    try:
        # 1. job 생성
        job_id = await conn.fetchval(
            """INSERT INTO unstructured_job (job_type, source_type, status, input_summary)
               VALUES ('text', $1, 'processing', $2)
               RETURNING job_id""",
            req.source_type, req.text[:500],
        )

        # 2. S3 저장
        s3_key = f"clinical-notes/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/job_{job_id}.txt"
        await _save_to_s3(s3_key, req.text.encode("utf-8"))

        # 3. LLM 섹션 분리
        sections = await _llm_section_split(req.text)

        # 4. NER 엔티티 추출
        entities = await _call_ner(req.text)

        # 5. OMOP note 적재
        note_id = await _insert_omop_note(conn, req.text, req.source_type or "경과기록", req.person_id, sections)

        # 6. OMOP note_nlp 적재
        nlp_count = await _insert_omop_note_nlp(conn, note_id, entities)

        elapsed_ms = int((_time.monotonic() - start) * 1000)

        # 7. job 업데이트
        result = {
            "sections": sections,
            "entities": entities,
            "note_id": note_id,
            "nlp_records": nlp_count,
        }
        await conn.execute(
            """UPDATE unstructured_job
               SET status='completed', s3_key=$1, result_count=$2, result_json=$3::jsonb,
                   omop_records_created=$4, processing_time_ms=$5, completed_at=NOW()
               WHERE job_id=$6""",
            s3_key, len(entities), json.dumps(result, ensure_ascii=False, default=str),
            1 + nlp_count, elapsed_ms, job_id,
        )

        return {
            "job_id": job_id,
            "status": "completed",
            "sections": sections,
            "entities": entities,
            "omop": {
                "note_id": note_id,
                "note_nlp_count": nlp_count,
            },
            "s3_key": s3_key,
            "processing_time_ms": elapsed_ms,
        }

    except HTTPException:
        raise
    except Exception as e:
        elapsed_ms = int((_time.monotonic() - start) * 1000)
        logger.error(f"Text processing failed: {e}")
        # job 실패 기록
        try:
            await conn.execute(
                """UPDATE unstructured_job
                   SET status='failed', error_message=$1, processing_time_ms=$2, completed_at=NOW()
                   WHERE job_id=$3""",
                str(e)[:500], elapsed_ms, job_id,
            )
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"텍스트 처리 실패: {str(e)}")
    finally:
        await _rel(conn)


# ── 5. DICOM 처리 ──

@router.post("/process/dicom")
async def process_dicom(file: UploadFile = File(...)):
    """DICOM 파일 업로드 + 메타데이터 추출 + OMOP 적재"""
    try:
        import pydicom
    except ImportError:
        raise HTTPException(status_code=501, detail="pydicom 패키지가 설치되어 있지 않습니다. pip install pydicom")

    if not file.filename:
        raise HTTPException(status_code=400, detail="파일을 업로드하세요")

    await _ensure_tables()
    start = _time.monotonic()
    conn = await _get_conn()

    try:
        # 파일 읽기
        content = await file.read()

        # 1. job 생성
        job_id = await conn.fetchval(
            """INSERT INTO unstructured_job (job_type, source_type, status, input_summary)
               VALUES ('dicom', 'DICOM', 'processing', $1)
               RETURNING job_id""",
            (file.filename or "unknown")[:500],
        )

        # 2. S3 저장
        s3_key = f"dicom/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}/job_{job_id}_{file.filename}"
        await _save_to_s3(s3_key, content, content_type="application/dicom")

        # 3. pydicom 파싱
        ds = pydicom.dcmread(io.BytesIO(content), force=True)

        def safe_str(tag_name: str) -> str:
            val = getattr(ds, tag_name, None)
            return str(val).strip() if val is not None else ""

        meta = {
            "PatientID": safe_str("PatientID"),
            "PatientName": safe_str("PatientName"),
            "StudyDate": safe_str("StudyDate"),
            "Modality": safe_str("Modality"),
            "BodyPartExamined": safe_str("BodyPartExamined"),
            "StudyDescription": safe_str("StudyDescription"),
            "SeriesDescription": safe_str("SeriesDescription"),
            "Rows": int(ds.Rows) if hasattr(ds, "Rows") and ds.Rows else None,
            "Columns": int(ds.Columns) if hasattr(ds, "Columns") and ds.Columns else None,
            "BitsAllocated": int(ds.BitsAllocated) if hasattr(ds, "BitsAllocated") and ds.BitsAllocated else None,
            "Manufacturer": safe_str("Manufacturer"),
            "InstitutionName": safe_str("InstitutionName"),
        }

        # 4. OMOP imaging_study 테이블 — DICOM 컬럼 확장 (idempotent)
        for col_ddl in [
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS modality VARCHAR(20)",
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS body_part VARCHAR(100)",
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS study_description VARCHAR(500)",
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS study_date DATE",
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS s3_key VARCHAR(500)",
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS dicom_meta JSONB",
        ]:
            try:
                await conn.execute(col_ddl)
            except Exception:
                pass  # column already exists or other benign error

        # StudyDate 파싱 (YYYYMMDD → DATE)
        study_date = None
        raw_date = meta.get("StudyDate", "")
        if raw_date and len(raw_date) >= 8:
            try:
                study_date = datetime.strptime(raw_date[:8], "%Y%m%d").date()
            except ValueError:
                pass

        img_id = await conn.fetchval(
            """INSERT INTO imaging_study
               (modality, body_part, study_description, study_date, s3_key, dicom_meta,
                image_filename, image_url)
               VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)
               RETURNING imaging_study_id""",
            meta["Modality"][:20] if meta["Modality"] else None,
            meta["BodyPartExamined"][:100] if meta["BodyPartExamined"] else None,
            meta["StudyDescription"][:500] if meta["StudyDescription"] else None,
            study_date,
            s3_key,
            json.dumps(meta, ensure_ascii=False, default=str),
            (file.filename or "unknown")[:200],
            s3_key[:500],
        )

        result_count = len([v for v in meta.values() if v])
        elapsed_ms = int((_time.monotonic() - start) * 1000)

        result = {
            "dicom_meta": meta,
            "imaging_study_id": img_id,
        }
        await conn.execute(
            """UPDATE unstructured_job
               SET status='completed', s3_key=$1, result_count=$2, result_json=$3::jsonb,
                   omop_records_created=1, processing_time_ms=$4, completed_at=NOW()
               WHERE job_id=$5""",
            s3_key, result_count, json.dumps(result, ensure_ascii=False, default=str),
            elapsed_ms, job_id,
        )

        return {
            "job_id": job_id,
            "status": "completed",
            "filename": file.filename,
            "dicom_meta": meta,
            "omop": {
                "imaging_study_id": img_id,
            },
            "s3_key": s3_key,
            "processing_time_ms": elapsed_ms,
        }

    except HTTPException:
        raise
    except Exception as e:
        elapsed_ms = int((_time.monotonic() - start) * 1000)
        logger.error(f"DICOM processing failed: {e}")
        try:
            await conn.execute(
                """UPDATE unstructured_job
                   SET status='failed', error_message=$1, processing_time_ms=$2, completed_at=NOW()
                   WHERE job_id=$3""",
                str(e)[:500], elapsed_ms, job_id,
            )
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"DICOM 처리 실패: {str(e)}")
    finally:
        await _rel(conn)


# ── 6. 통계 ──

@router.get("/stats")
async def get_stats():
    """비정형 처리 통계"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        # Job 통계
        total = await conn.fetchval("SELECT COUNT(*) FROM unstructured_job") or 0

        type_rows = await conn.fetch(
            "SELECT job_type, COUNT(*) AS cnt FROM unstructured_job GROUP BY job_type"
        )
        by_type = {r["job_type"]: r["cnt"] for r in type_rows}

        status_rows = await conn.fetch(
            "SELECT status, COUNT(*) AS cnt FROM unstructured_job GROUP BY status"
        )
        by_status = {r["status"]: r["cnt"] for r in status_rows}

        # OMOP 테이블 row counts (안전하게 — 테이블 없으면 0)
        omop_counts = {}
        for table in ("note", "note_nlp", "imaging_study"):
            try:
                cnt = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")  # noqa: S608
                omop_counts[table] = cnt or 0
            except Exception:
                omop_counts[table] = 0

        # 최근 작업 5건
        recent_rows = await conn.fetch(
            """SELECT job_id, job_type, source_type, status, input_summary,
                      result_count, processing_time_ms, created_at
               FROM unstructured_job ORDER BY created_at DESC LIMIT 5"""
        )
        recent = [
            {
                "job_id": r["job_id"],
                "job_type": r["job_type"],
                "source_type": r["source_type"],
                "status": r["status"],
                "input_summary": r["input_summary"],
                "result_count": r["result_count"],
                "processing_time_ms": r["processing_time_ms"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in recent_rows
        ]

        return {
            "total_jobs": total,
            "by_type": by_type,
            "by_status": by_status,
            "omop_records": omop_counts,
            "recent_jobs": recent,
        }
    finally:
        await _rel(conn)
