"""
데이터 카탈로그 - 공유 DB 설정, 연결, 테이블 초기화
"""
import os
import json
import hashlib
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import HTTPException
from pydantic import BaseModel, Field
import asyncpg


# ── DB Config ──

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

class CatalogEntryCreate(BaseModel):
    entry_name: str = Field(..., max_length=100)
    entry_type: str = Field(..., pattern=r"^(table|view|api|sql_query|json_schema|xml_schema|file|stream)$")
    physical_name: Optional[str] = Field(None, max_length=100)
    business_name: str = Field(..., max_length=200)
    domain: str = Field(default="기타", max_length=50)
    description: Optional[str] = Field(None, max_length=2000)
    source_system: Optional[str] = Field(None, max_length=100)
    owner_dept: Optional[str] = Field(None, max_length=100)
    owner_person: Optional[str] = Field(None, max_length=50)
    tags: List[str] = Field(default_factory=list)
    access_level: str = Field(default="internal", pattern=r"^(public|internal|restricted|confidential)$")
    usage_guide: Optional[str] = Field(None, max_length=2000)
    policy_notes: Optional[str] = Field(None, max_length=1000)
    connection_info: Optional[Dict[str, Any]] = None
    schema_definition: Optional[Dict[str, Any]] = None


class OwnershipCreate(BaseModel):
    entry_id: int
    owner_type: str = Field(..., pattern=r"^(primary|steward|consumer|shared)$")
    dept: str = Field(..., max_length=100)
    person: Optional[str] = Field(None, max_length=50)
    access_scope: str = Field(default="read", pattern=r"^(read|write|admin|full)$")
    share_purpose: Optional[str] = Field(None, max_length=500)
    expiry_date: Optional[str] = None


class RelationCreate(BaseModel):
    source_entry_id: int
    target_entry_id: int
    relation_type: str = Field(..., pattern=r"^(derives_from|feeds_into|references|same_entity|aggregates|supplements)$")
    description: Optional[str] = Field(None, max_length=500)


class WorkHistoryCreate(BaseModel):
    entry_id: int
    action_type: str = Field(..., pattern=r"^(create|update|schema_change|quality_check|etl_run|cohort_extract|query|export|share|archive)$")
    action_detail: Optional[Dict[str, Any]] = None
    actor: str = Field(default="system", max_length=50)
    job_id: Optional[str] = Field(None, max_length=100)
    cohort_id: Optional[str] = Field(None, max_length=100)
    row_count: Optional[int] = None


class ResourceCreate(BaseModel):
    entry_id: int
    resource_type: str = Field(..., pattern=r"^(rest_api|graphql|sql_view|stored_proc|json_file|xml_file|csv_file|parquet_file|stream_topic)$")
    endpoint_url: Optional[str] = Field(None, max_length=500)
    method: Optional[str] = Field(None, max_length=10)
    request_schema: Optional[Dict[str, Any]] = None
    response_schema: Optional[Dict[str, Any]] = None
    query_text: Optional[str] = Field(None, max_length=5000)
    file_path: Optional[str] = Field(None, max_length=500)
    format_spec: Optional[Dict[str, Any]] = None
    description: Optional[str] = Field(None, max_length=500)


# ── Table Setup ──

async def _ensure_tables(conn):
    try:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS catalog_entry (
            entry_id SERIAL PRIMARY KEY,
            entry_name VARCHAR(100) NOT NULL,
            entry_type VARCHAR(20) NOT NULL,
            physical_name VARCHAR(100),
            business_name VARCHAR(200) NOT NULL,
            domain VARCHAR(50) DEFAULT '기타',
            description TEXT,
            source_system VARCHAR(100),
            owner_dept VARCHAR(100),
            owner_person VARCHAR(50),
            tags JSONB DEFAULT '[]',
            access_level VARCHAR(20) DEFAULT 'internal',
            usage_guide TEXT,
            policy_notes TEXT,
            connection_info JSONB,
            schema_definition JSONB,
            schema_hash VARCHAR(64),
            row_count BIGINT DEFAULT 0,
            quality_score DOUBLE PRECISION,
            last_synced TIMESTAMPTZ,
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS catalog_ownership (
            ownership_id SERIAL PRIMARY KEY,
            entry_id INTEGER REFERENCES catalog_entry(entry_id) ON DELETE CASCADE,
            owner_type VARCHAR(20) NOT NULL,
            dept VARCHAR(100) NOT NULL,
            person VARCHAR(50),
            access_scope VARCHAR(20) DEFAULT 'read',
            share_purpose TEXT,
            expiry_date DATE,
            granted_by VARCHAR(50) DEFAULT 'system',
            granted_at TIMESTAMPTZ DEFAULT NOW(),
            status VARCHAR(20) DEFAULT 'active'
        );

        CREATE TABLE IF NOT EXISTS catalog_relation (
            relation_id SERIAL PRIMARY KEY,
            source_entry_id INTEGER REFERENCES catalog_entry(entry_id) ON DELETE CASCADE,
            target_entry_id INTEGER REFERENCES catalog_entry(entry_id) ON DELETE CASCADE,
            relation_type VARCHAR(30) NOT NULL,
            description TEXT,
            strength DOUBLE PRECISION DEFAULT 1.0,
            auto_detected BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(source_entry_id, target_entry_id, relation_type)
        );

        CREATE TABLE IF NOT EXISTS catalog_work_history (
            history_id BIGSERIAL PRIMARY KEY,
            entry_id INTEGER REFERENCES catalog_entry(entry_id) ON DELETE CASCADE,
            action_type VARCHAR(30) NOT NULL,
            action_detail JSONB,
            actor VARCHAR(50) DEFAULT 'system',
            job_id VARCHAR(100),
            cohort_id VARCHAR(100),
            row_count BIGINT,
            duration_ms BIGINT,
            status VARCHAR(20) DEFAULT 'success',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS catalog_resource (
            resource_id SERIAL PRIMARY KEY,
            entry_id INTEGER REFERENCES catalog_entry(entry_id) ON DELETE CASCADE,
            resource_type VARCHAR(20) NOT NULL,
            endpoint_url VARCHAR(500),
            method VARCHAR(10),
            request_schema JSONB,
            response_schema JSONB,
            query_text TEXT,
            file_path VARCHAR(500),
            format_spec JSONB,
            description TEXT,
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS catalog_sync_log (
            sync_id SERIAL PRIMARY KEY,
            entry_id INTEGER,
            sync_type VARCHAR(30) NOT NULL,
            changes_detected JSONB,
            changes_applied JSONB,
            status VARCHAR(20) DEFAULT 'success',
            error_message TEXT,
            synced_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    except asyncpg.exceptions.UniqueViolationError:
        pass  # tables/sequences already exist


_SEEDED = False


async def _ensure_seed(conn):
    global _SEEDED
    if _SEEDED:
        return
    cnt = await conn.fetchval("SELECT COUNT(*) FROM catalog_entry")
    if cnt > 0:
        _SEEDED = True
        return

    # -- Catalog Entries (tables + non-table resources) --
    entries = [
        # OMOP CDM Tables
        ("person", "table", "person", "환자 기본 정보", "환자", "환자 인구통계학적 정보 (성별, 생년, 인종 등). OMOP CDM 핵심 테이블.",
         "아산 EHR", "의료정보팀", "김데이터", '["환자","인구통계","PHI"]', "restricted",
         "개인정보 포함. 연구 목적 사용 시 IRB 승인 필요. person_source_value는 비식별화 후 제공.",
         "개인정보보호법 적용. 10년 보존 의무. 비식별화 후 연구 제공 가능.", 76074, 95.2),
        ("visit_occurrence", "table", "visit_occurrence", "방문/내원 이력", "진료", "내원/입퇴원 이력. visit_concept_id: 9201(입원), 9202(외래), 9203(응급).",
         "아산 EHR", "진료정보팀", "이진료", '["방문","입원","외래","응급"]', "internal",
         "방문 유형별 필터링 필수. visit_start_date 기준 조회 권장. 입원/외래/응급 분리 분석.",
         "진료 기록 10년 보존. 연구 시 환자 동의 필요.", 4500000, 92.8),
        ("condition_occurrence", "table", "condition_occurrence", "진단/상병 기록", "진단", "진단/상병 발생 기록. SNOMED-CT 기반 condition_concept_id.",
         "아산 EHR", "임상연구팀", "박연구", '["진단","상병","ICD-10","SNOMED"]', "internal",
         "condition_source_value로 원천 코드 확인. 당뇨=44054006, 고혈압=38341003.",
         None, 2800000, 88.5),
        ("measurement", "table", "measurement", "검사 결과", "검사", "Lab/Vital 검사 결과. LOINC 기반. 36.6M 행 대용량 테이블.",
         "Lab 시스템", "검사정보팀", "최검사", '["검사","Lab","Vital","LOINC"]', "internal",
         "대용량 주의: 전체 스캔 금지. 반드시 WHERE 조건 사용. MV 활용 권장.",
         None, 36600000, 90.1),
        ("drug_exposure", "table", "drug_exposure", "약물 처방/투약", "처방", "약물 처방/투여 기록. RxNorm 기반.",
         "처방 시스템", "약제팀", "정약사", '["약물","처방","RxNorm"]', "internal",
         "drug_source_value로 원천 약품코드 확인. drug_era 테이블과 결합하여 투약 기간 분석.",
         None, 3900000, 91.3),
        ("procedure_occurrence", "table", "procedure_occurrence", "시술/수술 기록", "시술", "시술/수술/검사 수행 기록. CPT-4/HCPCS 기반.",
         "수술 시스템", "수술정보팀", "한수술", '["시술","수술","CPT-4"]', "internal",
         None, None, 12400000, 89.7),

        # Non-table resources
        ("omop_rest_api", "api", None, "OMOP CDM REST API", "플랫폼", "IDP 플랫폼 데이터마트 REST API. 테이블 목록, 스키마, 샘플 데이터 조회.",
         "IDP", "플랫폼팀", "김개발", '["API","REST","데이터마트"]', "internal",
         "GET /api/v1/datamart/tables — 테이블 목록. GET /api/v1/datamart/table/{name}/schema — 스키마.",
         None, 0, None),
        ("text2sql_api", "api", None, "Text-to-SQL API", "AI", "자연어 질의를 SQL로 변환하는 AI API.",
         "IDP", "AI팀", "정AI", '["API","AI","Text2SQL","NLP"]', "internal",
         "POST /api/v1/text2sql/generate — 자연어→SQL. POST /api/v1/text2sql/execute — SQL 실행.",
         None, 0, None),
        ("patient_cohort_view", "view", "v_patient_cohort", "환자 코호트 뷰", "연구", "환자 기본 + 방문 + 진단 통합 뷰. 연구용 코호트 추출에 사용.",
         "IDP", "임상연구팀", "박연구", '["뷰","코호트","연구"]', "restricted",
         "SELECT * FROM v_patient_cohort WHERE condition_code = '44054006' — 당뇨 코호트.",
         "IRB 승인 후 사용. 비식별화 적용.", 0, None),
        ("synthea_csv", "file", None, "Synthea 합성 데이터 CSV", "원천", "Synthea 합성 환자 데이터 CSV 파일 세트.",
         "Synthea", "데이터팀", "김데이터", '["CSV","합성데이터","Synthea"]', "public",
         "synthea/csv/ 디렉토리의 CSV 파일들. ETL을 통해 OMOP CDM으로 변환.",
         None, 0, None),
        ("lab_hl7_stream", "stream", None, "Lab HL7 실시간 스트림", "검사", "Lab 시스템 HL7 메시지 실시간 스트림. Kafka 토픽으로 수신.",
         "Lab 시스템", "검사정보팀", "최검사", '["스트림","HL7","Kafka","실시간"]', "internal",
         "Kafka 토픽: lab-hl7-raw. HL7 v2.5 포맷. CDC 커넥터로 measurement 테이블에 적재.",
         None, 0, None),
        ("diagnosis_json_schema", "json_schema", None, "진단 JSON 스키마", "진단", "EHR→OMOP 진단 데이터 교환용 JSON 스키마 정의.",
         "IDP", "연동팀", "이연동", '["JSON","스키마","진단","교환"]', "public",
         '{"type":"object","properties":{"diag_cd":{"type":"string"},"diag_date":{"type":"string","format":"date"}}}',
         None, 0, None),
    ]
    for e in entries:
        schema_hash = hashlib.sha256(e[0].encode()).hexdigest()[:16]
        await conn.execute("""
            INSERT INTO catalog_entry (entry_name, entry_type, physical_name, business_name, domain,
                description, source_system, owner_dept, owner_person, tags, access_level,
                usage_guide, policy_notes, row_count, quality_score, schema_hash)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14,$15,$16)
        """, e[0], e[1], e[2], e[3], e[4], e[5], e[6], e[7], e[8], e[9], e[10], e[11], e[12], e[13], e[14], schema_hash)

    # -- Ownership --
    ownerships = [
        (1, "primary", "의료정보팀", "김데이터", "admin", None),
        (1, "steward", "데이터거버넌스팀", "이거버넌스", "write", None),
        (1, "consumer", "임상연구팀", "박연구", "read", "연구용 코호트 추출"),
        (1, "shared", "AI팀", "정AI", "read", "AI 모델 학습용 비식별 데이터"),
        (2, "primary", "진료정보팀", "이진료", "admin", None),
        (2, "consumer", "BI팀", "강분석", "read", "방문 패턴 BI 리포트"),
        (3, "primary", "임상연구팀", "박연구", "admin", None),
        (4, "primary", "검사정보팀", "최검사", "admin", None),
        (4, "shared", "AI팀", "정AI", "read", "이상치 탐지 모델"),
        (5, "primary", "약제팀", "정약사", "admin", None),
        (7, "primary", "플랫폼팀", "김개발", "admin", None),
        (7, "shared", "전체부서", None, "read", "플랫폼 API 일반 사용"),
    ]
    for o in ownerships:
        await conn.execute("""
            INSERT INTO catalog_ownership (entry_id, owner_type, dept, person, access_scope, share_purpose)
            VALUES ($1,$2,$3,$4,$5,$6)
        """, *o)

    # -- Relations --
    relations = [
        (1, 2, "feeds_into", "person → visit_occurrence (FK: person_id)", False),
        (1, 3, "feeds_into", "person → condition_occurrence (FK: person_id)", False),
        (1, 4, "feeds_into", "person → measurement (FK: person_id)", False),
        (1, 5, "feeds_into", "person → drug_exposure (FK: person_id)", False),
        (2, 6, "feeds_into", "visit → procedure (FK: visit_occurrence_id)", False),
        (3, 5, "references", "condition ↔ drug (동일 환자 진단-처방 연관)", True),
        (4, 3, "supplements", "measurement 검사 결과가 condition 진단을 보충", True),
        (10, 1, "derives_from", "Synthea CSV → person ETL 원천", False),
        (10, 2, "derives_from", "Synthea CSV → visit ETL 원천", False),
        (9, 1, "aggregates", "코호트 뷰가 person 테이블 집계", False),
        (9, 2, "aggregates", "코호트 뷰가 visit 테이블 집계", False),
        (9, 3, "aggregates", "코호트 뷰가 condition 테이블 집계", False),
        (11, 4, "feeds_into", "HL7 스트림 → measurement CDC 적재", False),
        (8, 4, "references", "Text2SQL API가 measurement 테이블 조회", True),
    ]
    for r in relations:
        await conn.execute("""
            INSERT INTO catalog_relation (source_entry_id, target_entry_id, relation_type, description, auto_detected)
            VALUES ($1,$2,$3,$4,$5)
        """, *r)

    # -- Work History --
    histories = [
        (1, "create", '{"source":"Synthea ETL","version":"v1.0"}', "system", None, None, 76074, 120000, "success"),
        (1, "schema_change", '{"change":"add column insurance_type"}', "김데이터", None, None, None, None, "success"),
        (1, "quality_check", '{"completeness":95.2,"null_rate":4.8}', "system", "qc-20260207", None, 76074, 5000, "success"),
        (2, "etl_run", '{"source":"ehr_visit","mode":"incremental"}', "system", "etl-visit-001", None, 4500000, 180000, "success"),
        (3, "cohort_extract", '{"condition":"당뇨(44054006)","count":12500}', "박연구", None, "cohort-dm-001", 12500, 30000, "success"),
        (3, "cohort_extract", '{"condition":"고혈압(38341003)","count":18200}', "박연구", None, "cohort-ht-001", 18200, 45000, "success"),
        (4, "etl_run", '{"source":"lab_result","mode":"full"}', "system", "etl-lab-daily", None, 36600000, 720000, "success"),
        (4, "export", '{"format":"parquet","dest":"research_lake","requester":"AI팀"}', "정AI", "export-lab-001", None, 1000000, 60000, "success"),
        (5, "etl_run", '{"source":"rx_order","mode":"incremental"}', "system", "etl-drug-001", None, 3900000, 240000, "success"),
        (7, "update", '{"change":"API 엔드포인트 추가: /datamart/table/{name}/sample"}', "김개발", None, None, None, None, "success"),
        (9, "create", '{"sql":"CREATE VIEW v_patient_cohort AS SELECT ..."}', "박연구", None, None, None, 2000, "success"),
        (11, "create", '{"topic":"lab-hl7-raw","connector":"debezium-lab"}', "최검사", None, None, None, None, "success"),
    ]
    for h in histories:
        await conn.execute("""
            INSERT INTO catalog_work_history (entry_id, action_type, action_detail, actor, job_id, cohort_id,
                row_count, duration_ms, status)
            VALUES ($1,$2,$3::jsonb,$4,$5,$6,$7,$8,$9)
        """, *h)

    # -- Resources --
    resources = [
        (7, "rest_api", "/api/v1/datamart/tables", "GET", None,
         '{"type":"array","items":{"type":"object","properties":{"table_name":{},"row_count":{}}}}',
         None, None, '{"content_type":"application/json"}', "OMOP CDM 테이블 목록 조회"),
        (7, "rest_api", "/api/v1/datamart/table/{name}/schema", "GET", None,
         '{"type":"object","properties":{"columns":{"type":"array"}}}',
         None, None, '{"content_type":"application/json"}', "테이블 스키마 조회"),
        (7, "rest_api", "/api/v1/datamart/table/{name}/sample", "GET", None,
         '{"type":"array"}',
         None, None, '{"content_type":"application/json","max_rows":100}', "샘플 데이터 조회"),
        (8, "rest_api", "/api/v1/text2sql/generate", "POST",
         '{"type":"object","properties":{"question":{"type":"string"}}}',
         '{"type":"object","properties":{"sql":{"type":"string"},"explanation":{"type":"string"}}}',
         None, None, '{"content_type":"application/json"}', "자연어→SQL 변환"),
        (9, "sql_view", None, None, None, None,
         "CREATE VIEW v_patient_cohort AS SELECT p.person_id, p.gender_source_value, p.year_of_birth, v.visit_concept_id, c.condition_source_value FROM person p JOIN visit_occurrence v ON v.person_id = p.person_id LEFT JOIN condition_occurrence c ON c.person_id = p.person_id",
         None, None, "환자 코호트 분석용 통합 뷰"),
        (10, "csv_file", None, None, None, None, None,
         "synthea/csv/patients.csv", '{"delimiter":",","encoding":"UTF-8","header":true}', "환자 원천 CSV"),
        (10, "csv_file", None, None, None, None, None,
         "synthea/csv/conditions.csv", '{"delimiter":",","encoding":"UTF-8","header":true}', "진단 원천 CSV"),
        (11, "stream_topic", "kafka://localhost:9092/lab-hl7-raw", None, None,
         '{"type":"HL7v2.5","segments":["MSH","PID","OBX"]}',
         None, None, '{"format":"HL7","partition_key":"patient_id"}', "Lab HL7 메시지 Kafka 토픽"),
        (12, "json_file", None, None,
         '{"type":"object","required":["diag_cd","diag_date"],"properties":{"diag_cd":{"type":"string","pattern":"^[A-Z]\\\\d{2}"},"diag_date":{"type":"string","format":"date"},"severity":{"type":"string","enum":["mild","moderate","severe"]}}}',
         None, None, "schemas/diagnosis.json",
         '{"json_schema_version":"draft-07"}', "EHR→OMOP 진단 교환 JSON 스키마"),
    ]
    for r in resources:
        await conn.execute("""
            INSERT INTO catalog_resource (entry_id, resource_type, endpoint_url, method, request_schema,
                response_schema, query_text, file_path, format_spec, description)
            VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9::jsonb,$10)
        """, *r)

    _SEEDED = True


async def _init(conn):
    await _ensure_tables(conn)
    await _ensure_seed(conn)
