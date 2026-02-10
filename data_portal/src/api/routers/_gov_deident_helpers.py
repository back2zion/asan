"""
gov_deident.py helper -- DDL + seed functions extracted for readability.
"""
import json as _json


async def _ensure_deident_rule_table(conn):
    """비식별화 규칙 테이블 DDL + 기본 PII 규칙 시드"""
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
