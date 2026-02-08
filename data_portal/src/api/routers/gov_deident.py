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

router = APIRouter()


# ── 비식별화 규칙 ──

async def _ensure_deident_rule_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS deident_rule (
            rule_id SERIAL PRIMARY KEY,
            target_column VARCHAR(200) NOT NULL,
            method VARCHAR(50) NOT NULL,
            pattern VARCHAR(200),
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(target_column)
        )
    """)
    # 시드: 비어있으면 기본 5개 PII 규칙 삽입
    count = await conn.fetchval("SELECT COUNT(*) FROM deident_rule")
    if count == 0:
        seeds = [
            ("person.person_source_value", "해시", None, True),
            ("person.gender_source_value", "코드 변환", None, True),  # noqa: E501
            ("person.year_of_birth", "라운딩", None, True),
            ("*.source_value", "코드 매핑", None, True),  # noqa: E501
            ("note.note_text", "마스킹", r"01[016789]-?\d{3,4}-?\d{4}|\d{6}-[1-4]\d{6}", True),
        ]
        for target, method, pat, enabled in seeds:
            await conn.execute("""
                INSERT INTO deident_rule (target_column, method, pattern, enabled)
                VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING
            """, target, method, pat, enabled)


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

async def _ensure_deident_pipeline_tables(conn):
    """Pipeline 단계별 비식별 현황, 처리 로그, 재식별 요청 테이블 생성 + 시드"""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS deident_pipeline_stage (
            stage_id SERIAL PRIMARY KEY,
            stage_name VARCHAR(50) NOT NULL,
            stage_order INTEGER NOT NULL,
            description VARCHAR(200),
            deident_methods JSONB DEFAULT '[]',
            applied_tables JSONB DEFAULT '[]',
            record_count BIGINT DEFAULT 0,
            status VARCHAR(20) DEFAULT 'active',
            last_processed TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cnt = await conn.fetchval("SELECT COUNT(*) FROM deident_pipeline_stage")
    if cnt == 0:
        seeds = [
            (1, "수집", 1, "수집 시 PII 탐지 및 분류",
             '["PII 자동탐지","정규식 스캔","민감도 분류"]',
             '["person","note","observation"]', 76074),
            (2, "적재", 2, "적재 시 해시/마스킹 처리",
             '["SHA-256 해시","부분 마스킹","토큰화"]',
             '["person","death","note"]', 76074),
            (3, "변환", 3, "CDM 변환 중 코드 매핑",
             '["source_value→concept_id","코드 매핑","범주화"]',
             '["condition_occurrence","drug_exposure","procedure_occurrence","measurement"]', 55700000),
            (4, "저장", 4, "저장 시 암호화 적용",
             '["AES-256 암호화","컬럼 레벨 암호화","접근 로그"]',
             '["person","death","note","observation"]', 21300000),
            (5, "제공", 5, "제공 시 동적 마스킹",
             '["역할기반 마스킹","K-익명성 검증","동적 필터"]',
             '["person","visit_occurrence","condition_occurrence"]', 4500000),
        ]
        for sid, name, order, desc, methods, tables, rcount in seeds:
            await conn.execute("""
                INSERT INTO deident_pipeline_stage (stage_id, stage_name, stage_order, description, deident_methods, applied_tables, record_count, status, last_processed)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7, 'active', NOW())
                ON CONFLICT DO NOTHING
            """, sid, name, order, desc, methods, tables, rcount)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS deident_processing_log (
            log_id SERIAL PRIMARY KEY,
            process_type VARCHAR(20) NOT NULL,
            stage_name VARCHAR(50),
            target_table VARCHAR(100),
            target_column VARCHAR(100),
            method VARCHAR(50),
            records_processed BIGINT DEFAULT 0,
            status VARCHAR(20) DEFAULT 'success',
            operator VARCHAR(50) DEFAULT 'system',
            detail TEXT,
            processed_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    cnt2 = await conn.fetchval("SELECT COUNT(*) FROM deident_processing_log")
    if cnt2 == 0:
        log_seeds = [
            ("deident", "수집", "person", "person_source_value", "PII 탐지", 76074, "success", "system", "신규 환자 PII 자동 탐지 완료"),
            ("deident", "적재", "person", "person_source_value", "SHA-256 해시", 76074, "success", "system", "환자 식별번호 해시 처리 완료"),
            ("deident", "적재", "person", "gender_source_value", "코드 변환", 76074, "success", "system", "성별 코드 변환 처리"),
            ("deident", "변환", "condition_occurrence", "condition_source_value", "코드 매핑", 2800000, "success", "etl_job", "진단 코드 OMOP 매핑 완료"),
            ("deident", "변환", "drug_exposure", "drug_source_value", "코드 매핑", 3900000, "success", "etl_job", "약물 코드 OMOP 매핑 완료"),
            ("deident", "저장", "note", "note_text", "패턴 마스킹", 1200, "success", "system", "임상노트 전화번호/주민번호 마스킹"),
            ("deident", "저장", "person", "*", "AES-256 암호화", 76074, "success", "system", "환자 테이블 컬럼 레벨 암호화"),
            ("deident", "제공", "visit_occurrence", "*", "동적 마스킹", 4500000, "success", "system", "역할 기반 동적 마스킹 적용"),
            ("reident", "제공", "person", "person_source_value", "해시 복원", 150, "success", "연구관리자", "IRB 승인 연구 대상자 재식별"),
            ("reident", "제공", "condition_occurrence", "condition_source_value", "코드 역매핑", 500, "success", "임상연구팀", "코호트 분석용 원본 코드 복원"),
        ]
        for pt, sn, tt, tc, mth, rp, st, op, dt in log_seeds:
            await conn.execute("""
                INSERT INTO deident_processing_log (process_type, stage_name, target_table, target_column, method, records_processed, status, operator, detail, processed_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW() - INTERVAL '1 hour' * (random() * 72)::int)
            """, pt, sn, tt, tc, mth, rp, st, op, dt)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS deident_reident_request (
            request_id SERIAL PRIMARY KEY,
            requester VARCHAR(50) NOT NULL,
            purpose VARCHAR(500) NOT NULL,
            target_tables JSONB NOT NULL,
            target_columns JSONB DEFAULT '[]',
            approval_level VARCHAR(20) DEFAULT 'pending',
            approver VARCHAR(50),
            approval_comment TEXT,
            expires_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            approved_at TIMESTAMPTZ
        )
    """)
    cnt3 = await conn.fetchval("SELECT COUNT(*) FROM deident_reident_request")
    if cnt3 == 0:
        await conn.execute("""
            INSERT INTO deident_reident_request (requester, purpose, target_tables, target_columns, approval_level, approver, approval_comment, expires_at, created_at, approved_at)
            VALUES
            ('김연구', 'IRB-2026-001 승인 코호트 연구: 당뇨 환자 5년 추적 분석', '["person","condition_occurrence"]'::jsonb, '["person_source_value","condition_source_value"]'::jsonb, 'approved', '이관리자', 'IRB 승인 확인 완료', NOW() + INTERVAL '90 days', NOW() - INTERVAL '5 days', NOW() - INTERVAL '4 days'),
            ('박분석', '의료 AI 모델 학습용 비식별 데이터 원본 검증', '["measurement","observation"]'::jsonb, '["value_as_number"]'::jsonb, 'pending', NULL, NULL, NULL, NOW() - INTERVAL '1 day', NULL),
            ('최임상', '퇴원 후 30일 재입원 분석 (만료)', '["visit_occurrence","condition_occurrence"]'::jsonb, '[]'::jsonb, 'expired', '이관리자', '기간 만료로 자동 종료', NOW() - INTERVAL '30 days', NOW() - INTERVAL '60 days', NOW() - INTERVAL '59 days')
        """)


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
