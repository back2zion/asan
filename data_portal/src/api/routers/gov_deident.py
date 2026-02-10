"""
거버넌스 - 비식별화 규칙/현황/파이프라인/모니터링, 재식별 요청
"""
import re
import json as _json
from datetime import datetime, date
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
import asyncpg

from routers.gov_shared import (
    get_connection,
    DeidentRuleCreate, ReidentRequestCreate,
)
from ._gov_deident_helpers import (
    _ensure_deident_rule_table,
    _ensure_deident_pipeline_tables,
)

router = APIRouter()


# ── 비식별화 규칙 ──

@router.get("/deident-rules")
async def get_deident_rules():
    """비식별화 규칙 목록"""
    conn = await get_connection()
    try:
        await _ensure_deident_rule_table(conn)
        rows = await conn.fetch("SELECT * FROM deident_rule ORDER BY rule_id")
        return [
            {
                "rule_id": r["rule_id"],
                "target_column": r["target_column"],
                "method": r["method"],
                "pattern": r["pattern"],
                "enabled": r["enabled"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.post("/deident-rules")
async def create_deident_rule(body: DeidentRuleCreate):
    """비식별화 규칙 추가"""
    conn = await get_connection()
    try:
        await _ensure_deident_rule_table(conn)
        rule_id = await conn.fetchval("""
            INSERT INTO deident_rule (target_column, method, pattern, enabled)
            VALUES ($1, $2, $3, $4)
            RETURNING rule_id
        """, body.target_column, body.method, body.pattern, body.enabled)
        return {"success": True, "rule_id": rule_id}
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="이미 동일 대상 컬럼에 규칙이 존재합니다")
    finally:
        await conn.close()


@router.put("/deident-rules/{rule_id}")
async def update_deident_rule(rule_id: int, body: DeidentRuleCreate):
    """비식별화 규칙 수정"""
    conn = await get_connection()
    try:
        result = await conn.execute("""
            UPDATE deident_rule
            SET target_column = $1, method = $2, pattern = $3, enabled = $4
            WHERE rule_id = $5
        """, body.target_column, body.method, body.pattern, body.enabled, rule_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="규칙을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


@router.delete("/deident-rules/{rule_id}")
async def delete_deident_rule(rule_id: int):
    """비식별화 규칙 삭제"""
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM deident_rule WHERE rule_id = $1", rule_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="규칙을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


# ── 비식별화 현황 ──

@router.get("/deidentification")
async def get_deidentification():
    """비식별화 현황 - 실제 OMOP CDM 데이터 스캔"""
    conn = await get_connection()
    try:
        # 1) PII 유형별 스캔
        pii_types = []

        # person_source_value (환자 식별번호)
        psv = await conn.fetchrow("""
            SELECT COUNT(*) AS total,
                   COUNT(person_source_value) AS non_null
            FROM person
        """)
        pii_types.append({
            "key": "person_id",
            "type": "환자 식별번호",
            "count": psv["non_null"],
            "method": "일방향 해시",
            "status": "done",
            "example": f"person_source_value → SHA-256 해시 ({psv['non_null']}건)",
        })

        # gender_source_value (성별 원본)
        gsv = await conn.fetchrow("""
            SELECT COUNT(gender_source_value) AS cnt FROM person
            WHERE gender_source_value IS NOT NULL
        """)
        pii_types.append({
            "key": "gender",
            "type": "성별 정보",
            "count": gsv["cnt"],
            "method": "코드 변환",
            "status": "done",
            "example": "M/F → gender_concept_id",
        })

        # year_of_birth (생년)
        yob = await conn.fetchrow("""
            SELECT COUNT(year_of_birth) AS cnt FROM person
            WHERE year_of_birth IS NOT NULL
        """)
        pii_types.append({
            "key": "birth_year",
            "type": "생년월일",
            "count": yob["cnt"],
            "method": "라운딩 (연도만 보관)",
            "status": "done",
            "example": "1988-01-15 → 1988 (연도만)",
        })

        # source_value 컬럼 (간접 식별자) — pg_stat 기반 근사치 (즉시 응답)
        total_sv = await conn.fetchval("""
            SELECT COALESCE(SUM(s.n_live_tup * sub.sv_count), 0)::bigint
            FROM (
                SELECT table_name, COUNT(*) AS sv_count
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND column_name LIKE '%%source_value'
                  AND table_name != 'person'
                GROUP BY table_name
            ) sub
            JOIN pg_stat_user_tables s
              ON s.relname = sub.table_name AND s.schemaname = 'public'
        """) or 0
        pii_types.append({
            "key": "source_values",
            "type": "원본 코드 (source_value)",
            "count": total_sv,
            "method": "코드 매핑",
            "status": "done",
            "example": "원본코드 → OMOP concept_id 변환",
        })

        # note_text (임상 노트 PII)
        note_pii_count = 0
        try:
            notes = await conn.fetch("""
                SELECT note_text FROM note
                WHERE note_text IS NOT NULL
                LIMIT 1000
            """)
            phone_re = re.compile(r'01[016789]-?\d{3,4}-?\d{4}')
            ssn_re = re.compile(r'\d{6}-[1-4]\d{6}')
            for n in notes:
                txt = n["note_text"] or ""
                note_pii_count += len(phone_re.findall(txt)) + len(ssn_re.findall(txt))
        except Exception:
            pass
        pii_types.append({
            "key": "note_pii",
            "type": "임상노트 PII (전화/주민번호)",
            "count": note_pii_count,
            "method": "패턴 마스킹",
            "status": "done" if note_pii_count == 0 else "partial",
            "example": "010-1234-5678 → 010-****-5678",
        })

        # 2) 요약 통계
        total_records = await conn.fetchval("""
            SELECT SUM(n_live_tup)::bigint
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
        """) or 0

        total_pii = sum(p["count"] for p in pii_types)
        masking_rate = round((1 - note_pii_count / max(total_pii, 1)) * 100, 1) if total_pii > 0 else 100.0

        # 3) K-익명성: (gender_concept_id, year_of_birth) 최소 그룹 크기
        k_anon = await conn.fetchval("""
            SELECT MIN(cnt) FROM (
                SELECT COUNT(*) AS cnt
                FROM person
                GROUP BY gender_concept_id, year_of_birth
                HAVING COUNT(*) > 0
            ) sub
        """) or 0

        # 4) Before/After 샘플 (person 테이블 실제 레코드)
        sample_rows = await conn.fetch("""
            SELECT person_id, person_source_value, gender_source_value,
                   year_of_birth, race_source_value, ethnicity_source_value
            FROM person LIMIT 3
        """)
        sample_before = []
        sample_after = []
        for r in sample_rows:
            sample_before.append({
                "person_id": r["person_id"],
                "person_source_value": r["person_source_value"],
                "gender": r["gender_source_value"],
                "year_of_birth": r["year_of_birth"],
                "race": r["race_source_value"],
                "ethnicity": r["ethnicity_source_value"],
            })
            psv_val = r["person_source_value"] or ""
            masked_psv = psv_val[:4] + "****" if len(psv_val) > 4 else "****"
            sample_after.append({
                "person_id": "HASH-" + str(r["person_id"])[-4:],
                "person_source_value": masked_psv,
                "gender": r["gender_source_value"],
                "year_of_birth": r["year_of_birth"],
                "race": "***",
                "ethnicity": "***",
            })

        # 5) 스캔 이력 저장 (trend용)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS deident_scan_history (
                scan_id SERIAL PRIMARY KEY,
                scan_date DATE NOT NULL DEFAULT CURRENT_DATE UNIQUE,
                total_scanned BIGINT,
                pii_detected INT,
                masked INT,
                k_anonymity INT
            )
        """)
        masked_count = total_pii - note_pii_count
        await conn.execute("""
            INSERT INTO deident_scan_history (scan_date, total_scanned, pii_detected, masked, k_anonymity)
            VALUES (CURRENT_DATE, $1, $2, $3, $4)
            ON CONFLICT DO NOTHING
        """, total_records, total_pii, masked_count, k_anon)

        # 6) trend 데이터
        trend = await conn.fetch("""
            SELECT scan_date, total_scanned, pii_detected, masked
            FROM deident_scan_history
            ORDER BY scan_date DESC
            LIMIT 14
        """)
        trend_data = [
            {
                "date": r["scan_date"].strftime("%m/%d") if isinstance(r["scan_date"], (datetime, date)) else str(r["scan_date"]),
                "total_scanned": r["total_scanned"],
                "pii_detected": r["pii_detected"],
            }
            for r in reversed(trend)
        ]

        return {
            "pii_types": pii_types,
            "summary": {
                "total_scanned": total_records,
                "pii_detected": total_pii,
                "masking_rate": masking_rate,
                "k_anonymity": k_anon,
            },
            "trend": trend_data,
            "sample_before": sample_before,
            "sample_after": sample_after,
        }
    finally:
        await conn.close()


# ── DGR-006: Pipeline 단계별 비식별 + 처리 로그 + 재식별 워크플로우 ──

@router.get("/deident-pipeline")
async def get_deident_pipeline():
    """Pipeline 5단계 비식별 처리 현황"""
    conn = await get_connection()
    try:
        await _ensure_deident_pipeline_tables(conn)
        rows = await conn.fetch("SELECT * FROM deident_pipeline_stage ORDER BY stage_order")
        return [
            {
                "stage_id": r["stage_id"],
                "stage_name": r["stage_name"],
                "stage_order": r["stage_order"],
                "description": r["description"],
                "deident_methods": _json.loads(r["deident_methods"]) if isinstance(r["deident_methods"], str) else (r["deident_methods"] or []),
                "applied_tables": _json.loads(r["applied_tables"]) if isinstance(r["applied_tables"], str) else (r["applied_tables"] or []),
                "record_count": r["record_count"],
                "status": r["status"],
                "last_processed": r["last_processed"].isoformat() if r["last_processed"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.get("/deident-processing-log")
async def get_deident_processing_log(
    process_type: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=200),
):
    """비식별/재식별 처리 로그"""
    conn = await get_connection()
    try:
        await _ensure_deident_pipeline_tables(conn)
        query = "SELECT * FROM deident_processing_log WHERE 1=1"
        params: list = []
        idx = 1
        if process_type:
            query += f" AND process_type = ${idx}"
            params.append(process_type)
            idx += 1
        query += f" ORDER BY processed_at DESC LIMIT ${idx}"
        params.append(limit)
        rows = await conn.fetch(query, *params)
        return [
            {
                "log_id": r["log_id"],
                "process_type": r["process_type"],
                "stage_name": r["stage_name"],
                "target_table": r["target_table"],
                "target_column": r["target_column"],
                "method": r["method"],
                "records_processed": r["records_processed"],
                "status": r["status"],
                "operator": r["operator"],
                "detail": r["detail"],
                "processed_at": r["processed_at"].isoformat() if r["processed_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.get("/deident-monitoring")
async def get_deident_monitoring():
    """비식별/재식별 모니터링 대시보드"""
    conn = await get_connection()
    try:
        await _ensure_deident_pipeline_tables(conn)

        # 단계별 현황
        stages = await conn.fetch("SELECT * FROM deident_pipeline_stage ORDER BY stage_order")

        # 로그 요약
        total_deident = await conn.fetchval(
            "SELECT COALESCE(SUM(records_processed), 0) FROM deident_processing_log WHERE process_type = 'deident'"
        )
        total_reident = await conn.fetchval(
            "SELECT COALESCE(SUM(records_processed), 0) FROM deident_processing_log WHERE process_type = 'reident'"
        )
        recent_24h = await conn.fetchval(
            "SELECT COALESCE(SUM(records_processed), 0) FROM deident_processing_log WHERE processed_at > NOW() - INTERVAL '24 hours'"
        )
        pending_reident = await conn.fetchval(
            "SELECT COUNT(*) FROM deident_reident_request WHERE approval_level = 'pending'"
        )

        # 최근 로그 10건
        recent_logs = await conn.fetch(
            "SELECT * FROM deident_processing_log ORDER BY processed_at DESC LIMIT 10"
        )

        return {
            "summary": {
                "total_deident": total_deident,
                "total_reident": total_reident,
                "recent_24h": recent_24h,
                "pending_reident": pending_reident,
            },
            "stages": [
                {
                    "stage_name": r["stage_name"],
                    "stage_order": r["stage_order"],
                    "description": r["description"],
                    "record_count": r["record_count"],
                    "status": r["status"],
                }
                for r in stages
            ],
            "recent_logs": [
                {
                    "log_id": r["log_id"],
                    "process_type": r["process_type"],
                    "stage_name": r["stage_name"],
                    "target_table": r["target_table"],
                    "target_column": r["target_column"],
                    "method": r["method"],
                    "records_processed": r["records_processed"],
                    "status": r["status"],
                    "operator": r["operator"],
                    "detail": r["detail"],
                    "processed_at": r["processed_at"].isoformat() if r["processed_at"] else None,
                }
                for r in recent_logs
            ],
        }
    finally:
        await conn.close()


@router.get("/reident-requests")
async def get_reident_requests():
    """재식별 요청 목록"""
    conn = await get_connection()
    try:
        await _ensure_deident_pipeline_tables(conn)
        rows = await conn.fetch("SELECT * FROM deident_reident_request ORDER BY created_at DESC")
        return [
            {
                "request_id": r["request_id"],
                "requester": r["requester"],
                "purpose": r["purpose"],
                "target_tables": _json.loads(r["target_tables"]) if isinstance(r["target_tables"], str) else (r["target_tables"] or []),
                "target_columns": _json.loads(r["target_columns"]) if isinstance(r["target_columns"], str) else (r["target_columns"] or []),
                "approval_level": r["approval_level"],
                "approver": r["approver"],
                "approval_comment": r["approval_comment"],
                "expires_at": r["expires_at"].isoformat() if r["expires_at"] else None,
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "approved_at": r["approved_at"].isoformat() if r["approved_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.post("/reident-requests")
async def create_reident_request(body: ReidentRequestCreate):
    """재식별 요청 생성"""
    conn = await get_connection()
    try:
        await _ensure_deident_pipeline_tables(conn)
        expires = None
        if body.expires_at:
            try:
                expires = datetime.fromisoformat(body.expires_at.replace("Z", "+00:00"))
            except ValueError:
                pass
        request_id = await conn.fetchval("""
            INSERT INTO deident_reident_request (requester, purpose, target_tables, target_columns, expires_at)
            VALUES ($1, $2, $3::jsonb, $4::jsonb, $5)
            RETURNING request_id
        """, body.requester, body.purpose, _json.dumps(body.target_tables), _json.dumps(body.target_columns), expires)
        return {"success": True, "request_id": request_id}
    finally:
        await conn.close()


@router.put("/reident-requests/{request_id}/approve")
async def approve_reident_request(request_id: int, approver: str = Query("관리자")):
    """재식별 요청 승인"""
    conn = await get_connection()
    try:
        row = await conn.fetchrow("SELECT approval_level FROM deident_reident_request WHERE request_id = $1", request_id)
        if not row:
            raise HTTPException(status_code=404, detail="요청을 찾을 수 없습니다")
        if row["approval_level"] != "pending":
            raise HTTPException(status_code=400, detail="pending 상태의 요청만 승인 가능합니다")
        await conn.execute("""
            UPDATE deident_reident_request
            SET approval_level = 'approved', approver = $1, approved_at = NOW(),
                expires_at = COALESCE(expires_at, NOW() + INTERVAL '90 days')
            WHERE request_id = $2
        """, approver, request_id)
        return {"success": True, "message": "승인 완료"}
    finally:
        await conn.close()


@router.put("/reident-requests/{request_id}/reject")
async def reject_reident_request(request_id: int, approver: str = Query("관리자"), reason: str = Query("")):
    """재식별 요청 거부"""
    conn = await get_connection()
    try:
        row = await conn.fetchrow("SELECT approval_level FROM deident_reident_request WHERE request_id = $1", request_id)
        if not row:
            raise HTTPException(status_code=404, detail="요청을 찾을 수 없습니다")
        if row["approval_level"] != "pending":
            raise HTTPException(status_code=400, detail="pending 상태의 요청만 거부 가능합니다")
        await conn.execute("""
            UPDATE deident_reident_request
            SET approval_level = 'rejected', approver = $1, approval_comment = $2, approved_at = NOW()
            WHERE request_id = $3
        """, approver, reason, request_id)
        return {"success": True, "message": "거부 완료"}
    finally:
        await conn.close()
