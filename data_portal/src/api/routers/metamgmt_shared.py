"""
Shared state for metadata-mgmt sub-routers:
  - DB config, connection helper
  - Pydantic models
  - Table DDL + seed data
"""
import os
import json
import logging
from typing import List, Optional, Dict, Any

from fastapi import HTTPException
from pydantic import BaseModel, Field
import asyncpg

logger = logging.getLogger(__name__)

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}


async def get_connection():
    try:
        return await asyncpg.connect(**OMOP_DB_CONFIG)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")


# ── Pydantic Models ──

class ChangeRequestCreate(BaseModel):
    request_type: str = Field(..., pattern=r"^(schema_change|column_add|column_remove|column_modify|table_add|metadata_update)$")
    target_table: str = Field(..., max_length=100)
    target_column: Optional[str] = Field(None, max_length=100)
    title: str = Field(..., max_length=200)
    description: str = Field(..., max_length=2000)
    change_detail: Optional[Dict[str, Any]] = None
    requester: str = Field(default="사용자", max_length=50)
    priority: str = Field(default="normal", pattern=r"^(low|normal|high|critical)$")


class QualityRuleCreate(BaseModel):
    table_name: str = Field(..., max_length=100)
    column_name: str = Field(..., max_length=100)
    rule_type: str = Field(..., pattern=r"^(not_null|range|enum|regex|unique|referential|custom)$")
    rule_name: str = Field(..., max_length=100)
    condition_expr: Optional[str] = Field(None, max_length=500)
    normal_min: Optional[float] = None
    normal_max: Optional[float] = None
    error_min: Optional[float] = None
    error_max: Optional[float] = None
    enum_values: Optional[List[str]] = None
    regex_pattern: Optional[str] = Field(None, max_length=300)
    severity: str = Field(default="warning", pattern=r"^(info|warning|error|critical)$")
    description: Optional[str] = Field(None, max_length=500)
    enabled: bool = True


class SourceMappingCreate(BaseModel):
    source_system: str = Field(..., max_length=100)
    source_table: str = Field(..., max_length=100)
    source_column: str = Field(..., max_length=100)
    source_type: str = Field(..., max_length=50)
    target_table: str = Field(..., max_length=100)
    target_column: str = Field(..., max_length=100)
    target_type: str = Field(..., max_length=50)
    transform_rule: Optional[str] = Field(None, max_length=500)
    description: Optional[str] = Field(None, max_length=500)


class ComplianceRuleCreate(BaseModel):
    rule_name: str = Field(..., max_length=100)
    regulation: str = Field(..., max_length=100)
    category: str = Field(default="privacy", pattern=r"^(privacy|security|quality|retention|access|audit)$")
    description: str = Field(..., max_length=1000)
    check_query: Optional[str] = Field(None, max_length=2000)
    threshold: Optional[float] = None
    severity: str = Field(default="warning", pattern=r"^(info|warning|error|critical)$")
    enabled: bool = True


class PipelineBizCreate(BaseModel):
    pipeline_name: str = Field(..., max_length=100)
    job_type: str = Field(default="batch", pattern=r"^(batch|stream|cdc)$")
    biz_metadata_ids: List[str] = Field(default_factory=list)
    source_tables: List[str] = Field(default_factory=list)
    target_table: str = Field(..., max_length=100)
    schedule: Optional[str] = Field(None, max_length=100)
    owner: Optional[str] = Field(None, max_length=50)
    description: Optional[str] = Field(None, max_length=500)


class QualityAlertCreate(BaseModel):
    name: str = Field(..., max_length=100)
    condition_type: str = Field(..., pattern=r"^(rule_fail|violation_threshold|batch_fail_rate)$")
    threshold: Optional[float] = None
    target_rules: Optional[List[int]] = Field(default_factory=list)
    channel: str = Field(default="log", pattern=r"^(log|webhook|email)$")
    channel_config: Optional[Dict[str, Any]] = Field(default_factory=dict)
    enabled: bool = True


# ── Table Setup ──

async def _ensure_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS meta_change_request (
            request_id SERIAL PRIMARY KEY,
            request_type VARCHAR(30) NOT NULL,
            target_table VARCHAR(100) NOT NULL,
            target_column VARCHAR(100),
            title VARCHAR(200) NOT NULL,
            description TEXT NOT NULL,
            change_detail JSONB,
            requester VARCHAR(50) DEFAULT '사용자',
            priority VARCHAR(20) DEFAULT 'normal',
            status VARCHAR(20) DEFAULT 'draft',
            reviewer VARCHAR(50),
            review_comment TEXT,
            reviewed_at TIMESTAMPTZ,
            approver VARCHAR(50),
            approval_comment TEXT,
            approved_at TIMESTAMPTZ,
            applied_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS meta_quality_rule (
            rule_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            column_name VARCHAR(100) NOT NULL,
            rule_type VARCHAR(20) NOT NULL,
            rule_name VARCHAR(100) NOT NULL,
            condition_expr VARCHAR(500),
            normal_min DOUBLE PRECISION,
            normal_max DOUBLE PRECISION,
            error_min DOUBLE PRECISION,
            error_max DOUBLE PRECISION,
            enum_values JSONB,
            regex_pattern VARCHAR(300),
            severity VARCHAR(20) DEFAULT 'warning',
            description TEXT,
            enabled BOOLEAN DEFAULT TRUE,
            last_checked TIMESTAMPTZ,
            last_result JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS meta_source_mapping (
            mapping_id SERIAL PRIMARY KEY,
            source_system VARCHAR(100) NOT NULL,
            source_table VARCHAR(100) NOT NULL,
            source_column VARCHAR(100) NOT NULL,
            source_type VARCHAR(50) NOT NULL,
            target_table VARCHAR(100) NOT NULL,
            target_column VARCHAR(100) NOT NULL,
            target_type VARCHAR(50) NOT NULL,
            transform_rule VARCHAR(500),
            mapping_status VARCHAR(20) DEFAULT 'active',
            description TEXT,
            verified_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS meta_compliance_rule (
            rule_id SERIAL PRIMARY KEY,
            rule_name VARCHAR(100) NOT NULL,
            regulation VARCHAR(100) NOT NULL,
            category VARCHAR(20) DEFAULT 'privacy',
            description TEXT NOT NULL,
            check_query TEXT,
            threshold DOUBLE PRECISION,
            severity VARCHAR(20) DEFAULT 'warning',
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS meta_compliance_status (
            status_id SERIAL PRIMARY KEY,
            rule_id INTEGER REFERENCES meta_compliance_rule(rule_id) ON DELETE CASCADE,
            check_time TIMESTAMPTZ DEFAULT NOW(),
            result VARCHAR(20) DEFAULT 'pass',
            actual_value DOUBLE PRECISION,
            detail TEXT,
            checked_by VARCHAR(50) DEFAULT 'system'
        );

        CREATE TABLE IF NOT EXISTS meta_pipeline_biz (
            link_id SERIAL PRIMARY KEY,
            pipeline_name VARCHAR(100) NOT NULL,
            job_type VARCHAR(20) DEFAULT 'batch',
            biz_metadata_ids JSONB DEFAULT '[]',
            source_tables JSONB DEFAULT '[]',
            target_table VARCHAR(100) NOT NULL,
            schedule VARCHAR(100),
            owner VARCHAR(50),
            description TEXT,
            last_run TIMESTAMPTZ,
            last_status VARCHAR(20) DEFAULT 'idle',
            row_count BIGINT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS meta_quality_check_history (
            history_id SERIAL PRIMARY KEY,
            run_id VARCHAR(50) NOT NULL,
            rule_id INTEGER,
            rule_name VARCHAR(100),
            table_name VARCHAR(100),
            column_name VARCHAR(100),
            rule_type VARCHAR(20),
            status VARCHAR(20),
            total_rows BIGINT DEFAULT 0,
            violations BIGINT DEFAULT 0,
            violation_rate DOUBLE PRECISION DEFAULT 0,
            detail TEXT,
            checked_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS meta_quality_alert (
            alert_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            condition_type VARCHAR(30) NOT NULL,
            threshold DOUBLE PRECISION,
            target_rules JSONB DEFAULT '[]',
            channel VARCHAR(20) DEFAULT 'log',
            channel_config JSONB DEFAULT '{}',
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)


_SEEDED = False


async def _ensure_seed(conn):
    global _SEEDED
    if _SEEDED:
        return
    cnt = await conn.fetchval("SELECT COUNT(*) FROM meta_change_request")
    if cnt > 0:
        _SEEDED = True
        return

    # -- Change Requests --
    requests = [
        ("schema_change", "measurement", "value_as_number", "검사 결과 컬럼 타입 변경",
         "value_as_number를 numeric(10,2)에서 double precision으로 변경. 소수점 정밀도 향상 필요.",
         '{"old_type":"numeric(10,2)","new_type":"double precision"}', "김연구원", "high", "approved",
         "이정훈", "타입 변경 시 기존 데이터 호환성 확인 완료", "박승인자", "승인. 다음 배치 시 적용 바람."),
        ("column_add", "person", None, "보험유형 컬럼 추가",
         "person 테이블에 insurance_type 컬럼 추가. 건강보험/의료급여/산재/자동차 구분.",
         '{"column":"insurance_type","type":"varchar(20)","default":"건강보험"}', "최분석가", "normal", "applied",
         "이정훈", "환자 코호트 분석에 필요한 컬럼", "박승인자", "승인."),
        ("metadata_update", "condition_occurrence", None, "진단 테이블 비즈 메타데이터 갱신",
         "condition_occurrence 비즈명을 '진단/상병 발생 기록'에서 '진단 이력 관리 테이블'로 변경. 도메인 태그 추가.",
         '{"old_biz_name":"진단/상병 발생 기록","new_biz_name":"진단 이력 관리 테이블","add_tags":["진단","상병","ICD-10"]}',
         "박메타", "low", "submitted", None, None, None, None),
        ("column_remove", "drug_exposure", "stop_reason", "중단사유 컬럼 삭제 요청",
         "stop_reason 컬럼 사용률 0.1% 미만으로 삭제 검토 요청. 영향도 분석 필요.",
         '{"column":"stop_reason","usage_rate":0.001,"dependent_marts":["약물 처방 마트"]}',
         "이DBA", "normal", "reviewing", "김검토자", None, None, None),
        ("table_add", None, None, "AI 추론 결과 테이블 신규 생성",
         "AI/ML 모델 추론 결과를 저장하는 ai_prediction 테이블 신규 생성 요청.",
         '{"table":"ai_prediction","columns":["prediction_id","person_id","model_name","prediction_date","result","confidence","features_json"]}',
         "정AI연구원", "high", "draft", None, None, None, None),
    ]
    for r in requests:
        await conn.execute("""
            INSERT INTO meta_change_request (request_type, target_table, target_column, title, description,
                change_detail, requester, priority, status, reviewer, review_comment, approver, approval_comment)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11,$12,$13)
        """, *r)

    # -- Quality Rules --
    rules = [
        ("person", "year_of_birth", "range", "출생연도 범위", None, 1900, 2026, 1850, 2030, None, None,
         "error", "출생연도는 1900~2026 범위여야 함. 1850~1900 또는 2026~2030은 경고."),
        ("person", "gender_source_value", "enum", "성별 코드", None, None, None, None, None,
         '["M","F"]', None, "error", "성별은 M 또는 F만 허용."),
        ("measurement", "value_as_number", "range", "검사 수치 범위", None, 0, 100000, -1, 999999, None, None,
         "warning", "검사 수치 합리적 범위. 0~100000 정상, 음수 또는 극단값은 오류."),
        ("visit_occurrence", "visit_start_date", "not_null", "방문시작일 필수", "visit_start_date IS NOT NULL",
         None, None, None, None, None, None, "critical", "방문 시작일은 필수값."),
        ("condition_occurrence", "condition_source_value", "not_null", "진단코드 필수",
         "condition_source_value IS NOT NULL", None, None, None, None, None, None,
         "error", "진단 코드(원천값)는 필수."),
        ("drug_exposure", "drug_exposure_start_date", "not_null", "처방시작일 필수",
         "drug_exposure_start_date IS NOT NULL", None, None, None, None, None, None,
         "critical", "약물 처방 시작일 필수값."),
        ("person", "person_source_value", "regex", "환자ID 형식", None, None, None, None, None,
         None, r"^[A-Z0-9]{6,20}$", "warning", "환자 원천 ID는 영대문자/숫자 6~20자리."),
        ("measurement", "measurement_source_value", "not_null", "검사코드 필수",
         "measurement_source_value IS NOT NULL", None, None, None, None, None, None,
         "warning", "검사 원천 코드 필수."),
        ("visit_occurrence", "visit_concept_id", "enum", "방문유형 코드", None, None, None, None, None,
         '[9201,9202,9203]', None, "error", "방문 유형: 9201(입원), 9202(외래), 9203(응급)만 허용."),
        ("observation_period", "observation_period_start_date", "custom", "관찰기간 정합성",
         "observation_period_start_date <= observation_period_end_date",
         None, None, None, None, None, None, "error", "관찰 시작일이 종료일 이전이어야 함."),
    ]
    for r in rules:
        await conn.execute("""
            INSERT INTO meta_quality_rule (table_name, column_name, rule_type, rule_name, condition_expr,
                normal_min, normal_max, error_min, error_max, enum_values, regex_pattern, severity, description)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13)
        """, *r)

    # -- Source Mappings --
    mappings = [
        ("아산 EHR", "ehr_patient", "patient_id", "varchar(20)", "person", "person_source_value", "varchar(50)",
         "TRIM(UPPER(patient_id))", "환자 ID 직접 매핑 (대문자 변환)"),
        ("아산 EHR", "ehr_patient", "sex_cd", "char(1)", "person", "gender_source_value", "varchar(50)",
         "CASE sex_cd WHEN '1' THEN 'M' WHEN '2' THEN 'F' END", "성별 코드 1→M, 2→F 변환"),
        ("아산 EHR", "ehr_patient", "birth_ymd", "varchar(8)", "person", "year_of_birth", "integer",
         "CAST(SUBSTRING(birth_ymd,1,4) AS INTEGER)", "생년월일 8자리에서 연도 4자리 추출"),
        ("아산 EHR", "ehr_diagnosis", "diag_cd", "varchar(10)", "condition_occurrence", "condition_source_value", "varchar(50)",
         "REPLACE(diag_cd, '.', '')", "ICD-10 코드 점(.) 제거"),
        ("아산 EHR", "ehr_diagnosis", "diag_date", "varchar(8)", "condition_occurrence", "condition_start_date", "date",
         "TO_DATE(diag_date, 'YYYYMMDD')", "날짜 문자열→DATE 변환"),
        ("Lab 시스템", "lab_result", "test_cd", "varchar(20)", "measurement", "measurement_source_value", "varchar(50)",
         "TRIM(test_cd)", "검사 코드 직접 매핑"),
        ("Lab 시스템", "lab_result", "result_val", "varchar(50)", "measurement", "value_as_number", "double precision",
         "CASE WHEN result_val ~ '^[0-9.]+$' THEN CAST(result_val AS double precision) ELSE NULL END",
         "숫자형 결과만 변환, 문자형은 NULL"),
        ("Lab 시스템", "lab_result", "result_unit", "varchar(20)", "measurement", "unit_source_value", "varchar(50)",
         "TRIM(LOWER(result_unit))", "단위 소문자 정규화"),
        ("처방 시스템", "rx_order", "drug_cd", "varchar(15)", "drug_exposure", "drug_source_value", "varchar(50)",
         "TRIM(drug_cd)", "약물 코드 직접 매핑"),
        ("처방 시스템", "rx_order", "start_dt", "datetime", "drug_exposure", "drug_exposure_start_date", "date",
         "CAST(start_dt AS DATE)", "datetime→date 변환"),
        ("처방 시스템", "rx_order", "dose_qty", "decimal", "drug_exposure", "quantity", "double precision",
         "CAST(dose_qty AS double precision)", "투약량 타입 변환"),
        ("수술 시스템", "op_record", "proc_cd", "varchar(10)", "procedure_occurrence", "procedure_source_value", "varchar(50)",
         "TRIM(proc_cd)", "시술 코드 직접 매핑"),
        ("영상 시스템", "pacs_study", "study_uid", "varchar(64)", "imaging_study", "study_instance_uid", "varchar(100)",
         "study_uid", "DICOM Study UID 직접 매핑"),
        ("비정형-병리", "path_report", "report_text", "text", "note", "note_text", "text",
         "regexp_replace(report_text, '\\d{6}-\\d{7}', '[MASKED]')", "병리보고서 텍스트 — 개인정보 마스킹 후 적재"),
        ("비정형-경과기록", "progress_note", "note_content", "text", "note", "note_text", "text",
         "regexp_replace(note_content, '(\\d{3}[-.]?\\d{4}[-.]?\\d{4})', '[TEL_MASKED]')", "경과기록 — 전화번호 마스킹"),
    ]
    for m in mappings:
        await conn.execute("""
            INSERT INTO meta_source_mapping (source_system, source_table, source_column, source_type,
                target_table, target_column, target_type, transform_rule, description)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        """, *m)

    # -- Compliance Rules --
    comp_rules = [
        ("개인정보 비식별화", "개인정보보호법", "privacy",
         "person_source_value, gender_source_value 등 직접 식별자는 비식별 처리 후 제공되어야 한다.",
         "SELECT COUNT(*) FROM person WHERE person_source_value IS NOT NULL AND LENGTH(person_source_value) > 0", 0, "critical"),
        ("최소 K-익명성", "개인정보보호법", "privacy",
         "준식별자(성별, 출생연도) 조합의 최소 그룹 크기가 5 이상이어야 한다.",
         "SELECT MIN(cnt) FROM (SELECT COUNT(*) cnt FROM person GROUP BY gender_source_value, year_of_birth) t", 5, "error"),
        ("데이터 보존 기간", "의료법", "retention",
         "진료 기록은 최소 10년간 보존해야 한다. observation_period 기준 10년 이내 데이터 삭제 금지.",
         None, None, "warning"),
        ("접근 로그 기록", "개인정보보호법", "audit",
         "개인정보 포함 테이블(person, note) 접근 시 로그가 기록되어야 한다.",
         None, None, "error"),
        ("검사 결과 범위 검증", "의료 데이터 품질 가이드", "quality",
         "measurement.value_as_number의 이상치(상위/하위 0.1%) 비율이 1% 미만이어야 한다.",
         None, 1.0, "warning"),
        ("필수 컬럼 완전성", "OMOP CDM v5.4", "quality",
         "NOT NULL 필수 컬럼(person_id, visit_start_date 등)의 NULL 비율이 0%여야 한다.",
         "SELECT COUNT(*) FROM visit_occurrence WHERE visit_start_date IS NULL", 0, "critical"),
        ("표준코드 매핑률", "OMOP CDM v5.4", "quality",
         "condition_concept_id, drug_concept_id 등 표준코드 매핑률이 80% 이상이어야 한다.",
         None, 80, "warning"),
        ("암호화 저장", "개인정보보호법", "security",
         "개인정보(person_source_value)는 암호화하여 저장해야 한다.",
         None, None, "critical"),
    ]
    for c in comp_rules:
        await conn.execute("""
            INSERT INTO meta_compliance_rule (rule_name, regulation, category, description, check_query, threshold, severity)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
        """, *c)

    # Compliance check results (seed)
    comp_statuses = [
        (1, "pass", 0, "person_source_value는 Synthea 합성 데이터 — 실 환자 ID 아님"),
        (2, "pass", 12, "최소 그룹 크기 12 (gender_source_value='M', year_of_birth=1990)"),
        (3, "pass", None, "observation_period 기준 2014~2025 데이터 보존 확인"),
        (4, "warning", None, "접근 로그 시스템 미구축 — 구현 예정"),
        (5, "pass", 0.3, "이상치 비율 0.3% — 기준(1%) 이내"),
        (6, "pass", 0, "visit_start_date NULL 건수: 0"),
        (7, "warning", 65, "표준코드 매핑률 65% — 기준(80%) 미달, 개선 진행 중"),
        (8, "warning", None, "DB 수준 암호화 미적용 — TDE 도입 검토 중"),
    ]
    for s in comp_statuses:
        await conn.execute("""
            INSERT INTO meta_compliance_status (rule_id, result, actual_value, detail)
            VALUES ($1,$2,$3,$4)
        """, *s)

    # -- Pipeline Biz Metadata --
    pipelines = [
        ("OMOP_person_ETL", "batch", '["환자 인구통계","보험 정보"]',
         '["ehr_patient","insurance_info"]', "person", "0 2 * * *", "data_team",
         "EHR 환자 정보 → OMOP person 변환. Biz 메타: 환자 인구통계, 보험 정보.", "success", 76074),
        ("OMOP_condition_CDC", "cdc", '["진단 이력","상병 코드"]',
         '["ehr_diagnosis"]', "condition_occurrence", "실시간", "clinical_team",
         "EHR 진단 → OMOP condition_occurrence CDC. Biz 메타: 진단 이력, 상병 코드.", "running", 2800000),
        ("OMOP_measurement_batch", "batch", '["검사 결과","Lab 데이터"]',
         '["lab_result","vital_sign"]', "measurement", "0 1 * * *", "lab_team",
         "Lab/Vital → OMOP measurement 배치. Biz 메타: 검사 결과, Lab 데이터.", "success", 36600000),
        ("OMOP_drug_ETL", "batch", '["약물 처방","투약 기록"]',
         '["rx_order"]', "drug_exposure", "0 3 * * *", "pharmacy_team",
         "처방 → OMOP drug_exposure 변환. Biz 메타: 약물 처방, 투약 기록.", "success", 3900000),
        ("OMOP_visit_stream", "stream", '["방문 이력","입퇴원 정보"]',
         '["ehr_visit","admit_discharge"]', "visit_occurrence", "5분 간격", "data_team",
         "방문/입퇴원 → OMOP visit_occurrence 스트림. Biz 메타: 방문 이력, 입퇴원 정보.", "running", 4500000),
        ("pathology_NLP", "batch", '["병리보고서","비정형→정형"]',
         '["path_report"]', "note", "0 6 * * *", "ai_team",
         "병리보고서 비정형 텍스트 NLP 구조화. Biz 메타: 병리보고서.", "success", 150000),
        ("imaging_DICOM", "batch", '["영상 메타데이터","DICOM"]',
         '["pacs_study"]', "imaging_study", "0 4 * * *", "imaging_team",
         "PACS DICOM → OMOP imaging_study. Biz 메타: 영상 메타데이터.", "idle", 0),
        ("patient_mart_refresh", "batch", '["환자 코호트","보험 통합"]',
         '["person","payer_plan_period","observation_period"]', "dm_patient_mart", "0 5 * * *", "bi_team",
         "환자 기본 마트 갱신. Biz 메타: 환자 코호트, 보험 통합.", "success", 76074),
    ]
    for p in pipelines:
        await conn.execute("""
            INSERT INTO meta_pipeline_biz (pipeline_name, job_type, biz_metadata_ids, source_tables,
                target_table, schedule, owner, description, last_status, row_count)
            VALUES ($1,$2,$3::jsonb,$4::jsonb,$5,$6,$7,$8,$9,$10)
        """, *p)

    # -- Quality Alerts (seed) --
    alert_cnt = await conn.fetchval("SELECT COUNT(*) FROM meta_quality_alert")
    if alert_cnt == 0:
        alerts_seed = [
            ("critical 규칙 실패", "rule_fail", None, "[]", "log", "{}"),
            ("위반율 1% 초과", "violation_threshold", 1.0, "[]", "log", "{}"),
            ("일괄검증 통과율 80% 미만", "batch_fail_rate", 80.0, "[]", "log", "{}"),
            ("NULL 비율 10% 초과", "violation_threshold", 10.0, "[]", "webhook", '{"url":"https://hooks.example.com/quality"}'),
        ]
        for a in alerts_seed:
            await conn.execute("""
                INSERT INTO meta_quality_alert (name, condition_type, threshold, target_rules, channel, channel_config)
                VALUES ($1,$2,$3,$4::jsonb,$5,$6::jsonb)
            """, *a)

    _SEEDED = True


async def _init(conn):
    await _ensure_tables(conn)
    await _ensure_seed(conn)
