"""
DGR-005: 데이터 보안 관리 - 공유 상수, DB 헬퍼, Pydantic 모델
"""
import json as _json
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
import asyncpg


OMOP_DB_CONFIG = {
    "host": "localhost", "port": 5436,
    "user": "omopuser", "password": "omop", "database": "omop_cdm",
}


# ── DB Helpers ──

async def get_conn():
    return await asyncpg.connect(**OMOP_DB_CONFIG)


async def ensure_tables():
    conn = await get_conn()
    try:
        await conn.execute("""
        -- 1) Security Policies (테이블+컬럼 단위 보안)
        CREATE TABLE IF NOT EXISTS sec_policy (
            policy_id SERIAL PRIMARY KEY,
            target_type VARCHAR(10) NOT NULL CHECK (target_type IN ('table','column')),
            target_table VARCHAR(100) NOT NULL,
            target_column VARCHAR(100),
            security_type VARCHAR(50) NOT NULL,
            security_level VARCHAR(10) NOT NULL CHECK (security_level IN ('극비','민감','일반','공개')),
            legal_basis VARCHAR(200),
            deident_method VARCHAR(50),
            deident_timing VARCHAR(50),
            description TEXT,
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 2) Term-based Security Rules (표준 용어 기반 보안)
        CREATE TABLE IF NOT EXISTS sec_term_rule (
            rule_id SERIAL PRIMARY KEY,
            term_name VARCHAR(100) NOT NULL,
            security_type VARCHAR(50) NOT NULL,
            security_level VARCHAR(10) NOT NULL CHECK (security_level IN ('극비','민감','일반','공개')),
            deident_method VARCHAR(50),
            auto_propagate BOOLEAN DEFAULT TRUE,
            applied_count INTEGER DEFAULT 0,
            description TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 3) Biz Metadata Security Attributes
        CREATE TABLE IF NOT EXISTS sec_biz_meta (
            meta_id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            column_name VARCHAR(100),
            security_type VARCHAR(50) NOT NULL,
            deident_target BOOLEAN DEFAULT FALSE,
            deident_timing VARCHAR(50),
            deident_level VARCHAR(50),
            deident_method VARCHAR(50),
            retention_period VARCHAR(50),
            access_scope VARCHAR(100),
            description TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 4) Reidentification Requests
        CREATE TABLE IF NOT EXISTS sec_reid_request (
            request_id SERIAL PRIMARY KEY,
            requester VARCHAR(100) NOT NULL,
            department VARCHAR(100) NOT NULL,
            purpose TEXT NOT NULL,
            justification TEXT NOT NULL,
            target_tables TEXT[] NOT NULL,
            target_columns TEXT[] NOT NULL,
            row_filter TEXT,
            duration_days INTEGER NOT NULL,
            status VARCHAR(20) DEFAULT 'submitted' CHECK (status IN ('draft','submitted','reviewing','approved','rejected','expired','revoked')),
            reviewer VARCHAR(100),
            reviewer_comment TEXT,
            approved_at TIMESTAMP,
            expires_at TIMESTAMP,
            scope_tables TEXT[],
            scope_columns TEXT[],
            scope_row_filter TEXT,
            scope_max_rows INTEGER,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 5) User Attributes (사용자 속성)
        CREATE TABLE IF NOT EXISTS sec_user_attribute (
            attr_id SERIAL PRIMARY KEY,
            user_id VARCHAR(100) NOT NULL UNIQUE,
            user_name VARCHAR(100) NOT NULL,
            department VARCHAR(100) NOT NULL,
            research_field VARCHAR(100),
            rank VARCHAR(50) NOT NULL,
            role_id INTEGER,
            attributes JSONB DEFAULT '{}',
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 6) Dynamic Security Policies
        CREATE TABLE IF NOT EXISTS sec_dynamic_policy (
            dp_id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            policy_type VARCHAR(20) NOT NULL CHECK (policy_type IN ('row_level','column_level','cell_level','context_based')),
            condition JSONB NOT NULL,
            action JSONB NOT NULL,
            priority INTEGER DEFAULT 100,
            target_tables TEXT[],
            enabled BOOLEAN DEFAULT TRUE,
            description TEXT,
            evaluated_count INTEGER DEFAULT 0,
            last_evaluated_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 7) Masking Rules (Row/Column/Cell 3단계)
        CREATE TABLE IF NOT EXISTS sec_masking_rule (
            rule_id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            masking_level VARCHAR(10) NOT NULL CHECK (masking_level IN ('row','column','cell')),
            target_table VARCHAR(100) NOT NULL,
            target_column VARCHAR(100),
            masking_method VARCHAR(50) NOT NULL,
            condition JSONB,
            parameters JSONB,
            enabled BOOLEAN DEFAULT TRUE,
            applied_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 8) Access Audit Log
        CREATE TABLE IF NOT EXISTS sec_access_log (
            log_id BIGSERIAL PRIMARY KEY,
            user_id VARCHAR(100),
            user_name VARCHAR(100),
            action VARCHAR(50) NOT NULL,
            target_table VARCHAR(100),
            target_column VARCHAR(100),
            policy_ids INTEGER[],
            result VARCHAR(20) DEFAULT 'allowed' CHECK (result IN ('allowed','denied','masked','filtered')),
            context JSONB,
            ip_address VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        );
        """)
    finally:
        await conn.close()


async def ensure_seed():
    conn = await get_conn()
    try:
        cnt = await conn.fetchval("SELECT COUNT(*) FROM sec_policy")
        if cnt > 0:
            return
        # Security Policies
        await conn.execute("""
        INSERT INTO sec_policy (target_type, target_table, target_column, security_type, security_level, legal_basis, deident_method, deident_timing, description) VALUES
        ('column','person','person_source_value','PII','극비','개인정보보호법 제23조','SHA-256 해시','수집 즉시','주민등록번호 등 직접식별자'),
        ('column','person','gender_source_value','PII','민감','개인정보보호법 제23조','코드변환','수집 즉시','성별 코드'),
        ('column','person','year_of_birth','준식별자','민감','개인정보보호법 제23조','라운딩(5년 단위)','분석 시','출생연도'),
        ('table','death','','민감의료정보','극비','의료법 제21조','접근제한','상시','사망 기록 전체'),
        ('column','note_nlp','note_text','민감의료정보','극비','의료법 제21조','패턴마스킹','조회 시','자유텍스트 진료기록'),
        ('column','condition_occurrence','condition_source_value','진단정보','민감','개인정보보호법 제23조','코드변환','분석 시','진단코드 원문'),
        ('column','drug_exposure','drug_source_value','처방정보','민감','약사법 제21조','코드변환','분석 시','약품코드 원문'),
        ('column','measurement','value_as_number','검사결과','민감','의료법 제21조','범위변환','분석 시','검사 수치'),
        ('table','visit_occurrence','','방문기록','민감','개인정보보호법 제17조','Row-level 필터링','조회 시','진료 방문 기록'),
        ('column','observation','value_as_string','관찰기록','민감','의료법 제21조','패턴마스킹','조회 시','관찰값 문자열')
        """)
        # Term-based Security Rules
        await conn.execute("""
        INSERT INTO sec_term_rule (term_name, security_type, security_level, deident_method, auto_propagate, description) VALUES
        ('환자','PII','극비','SHA-256 해시',TRUE,'환자 식별 관련 컬럼 → 극비 + 해시'),
        ('진단','진단정보','민감','코드변환',TRUE,'진단 관련 컬럼 → 민감 + 코드변환'),
        ('처방','처방정보','민감','코드변환',TRUE,'처방 관련 컬럼 → 민감 + 코드변환'),
        ('검사','검사결과','민감','범위변환',TRUE,'검사 관련 컬럼 → 민감 + 범위변환'),
        ('수술','시술정보','민감','코드변환',TRUE,'수술/시술 관련 컬럼 → 민감 + 코드변환'),
        ('진료','방문기록','민감','Row-level 필터링',TRUE,'진료 관련 컬럼 → 민감 + 행 필터링'),
        ('투약','약물정보','민감','코드변환',TRUE,'투약 관련 컬럼 → 민감 + 코드변환'),
        ('활력징후','검사결과','일반','범위변환',FALSE,'활력징후 → 일반 (집계용)'),
        ('비식별화','거버넌스','공개',NULL,FALSE,'비식별 관련 메타데이터 → 공개')
        """)
        # Biz Metadata Security
        await conn.execute("""
        INSERT INTO sec_biz_meta (table_name, column_name, security_type, deident_target, deident_timing, deident_level, deident_method, retention_period, access_scope) VALUES
        ('person','person_source_value','PII',TRUE,'수집 즉시','완전비식별','SHA-256 해시','영구보존','관리자'),
        ('person','gender_source_value','준식별자',TRUE,'수집 즉시','부분비식별','코드변환','영구보존','연구자+'),
        ('person','year_of_birth','준식별자',TRUE,'분석 시','부분비식별','라운딩(5년)','영구보존','연구자+'),
        ('death','cause_source_value','민감의료정보',TRUE,'상시','접근제한','접근통제','10년','관리자'),
        ('condition_occurrence','condition_source_value','진단정보',TRUE,'분석 시','부분비식별','코드변환','10년','임상의+'),
        ('drug_exposure','drug_source_value','처방정보',TRUE,'분석 시','부분비식별','코드변환','10년','임상의+'),
        ('measurement','value_as_number','검사결과',FALSE,'해당없음','해당없음','해당없음','10년','연구자+'),
        ('visit_occurrence','visit_start_date','방문기록',TRUE,'분석 시','부분비식별','날짜 이동','10년','임상의+'),
        ('note_nlp','note_text','자유텍스트',TRUE,'조회 시','완전비식별','패턴마스킹','5년','관리자'),
        ('observation','value_as_string','관찰기록',TRUE,'조회 시','부분비식별','패턴마스킹','10년','연구자+')
        """)
        # Reid Requests (sample)
        await conn.execute("""
        INSERT INTO sec_reid_request (requester, department, purpose, justification, target_tables, target_columns, row_filter, duration_days, status, reviewer, reviewer_comment, approved_at, expires_at, scope_tables, scope_columns) VALUES
        ('김연구','의학연구센터','당뇨 코호트 임상시험 환자 매칭','IRB 승인번호 2026-0142, 당뇨 환자 200명 대상 임상시험을 위해 person_source_value 재식별 필요','{person,condition_occurrence}','{person_source_value,condition_source_value}','condition_concept_id = 44054006',30,'approved','박보안','IRB 승인 확인, 200명 제한 조건부 승인','2026-02-01','2026-03-03','{person,condition_occurrence}','{person_source_value}'),
        ('이분석','데이터분석팀','약물 부작용 모니터링 검증','식약처 보고 의무 약물 부작용 케이스 확인을 위해 원본 약품코드 필요','{drug_exposure,person}','{drug_source_value,person_source_value}','drug_concept_id IN (1,2,3)',14,'reviewing','박보안',NULL,NULL,NULL,NULL,NULL),
        ('최임상','순환기내과','심부전 환자 추적관찰 데이터 검증','기존 코호트 10명에 대한 투약 이력 재확인','{person,drug_exposure,visit_occurrence}','{person_source_value,drug_source_value,visit_start_date}',NULL,7,'submitted',NULL,NULL,NULL,NULL,NULL,NULL),
        ('정관리','정보보안팀','보안 감사 목적 데이터 무결성 검증','연 1회 보안 감사 시 원본 데이터 무결성 확인 필요','{person}','{person_source_value}',NULL,3,'rejected','박보안','보안 감사 시 비식별 상태에서 해시 비교로 충분',NULL,NULL,NULL,NULL)
        """)
        # User Attributes
        await conn.execute("""
        INSERT INTO sec_user_attribute (user_id, user_name, department, research_field, rank, role_id, attributes) VALUES
        ('admin01','박보안','정보보안팀',NULL,'팀장',1,'{"clearance":"top_secret","audit_role":true}'),
        ('res01','김연구','의학연구센터','당뇨/내분비','선임연구원',2,'{"irb_certified":true,"research_projects":["PRJ-2026-01"]}'),
        ('doc01','최임상','순환기내과','심부전','부교수',3,'{"license":"의사","specialty":"심장내과","patients":[1001,1002]}'),
        ('ana01','이분석','데이터분석팀','약물역학','분석가',4,'{"tools":["Python","R","SAS"],"clearance":"sensitive"}'),
        ('doc02','한진료','소아청소년과','소아혈액종양','조교수',3,'{"license":"의사","specialty":"소아과"}'),
        ('res02','오연구','임상약리학과','약물유전체','연구교수',2,'{"irb_certified":true}'),
        ('nurse01','송간호','간호부','중환자간호','수간호사',3,'{"license":"간호사","ward":"ICU"}'),
        ('admin02','윤관리','의료정보팀',NULL,'대리',1,'{"system_admin":true}')
        """)
        # Dynamic Policies
        await conn.execute("""
        INSERT INTO sec_dynamic_policy (name, policy_type, condition, action, priority, target_tables, enabled, description) VALUES
        ('연구자 진단데이터 Row 필터링','row_level','{"user_role":"연구자","has_irb":true}','{"filter":"person_id IN (SELECT person_id FROM research_cohort WHERE project_id = :user_project)","description":"IRB 승인 코호트 환자만 접근"}',10,'{condition_occurrence,drug_exposure,measurement}',TRUE,'IRB 승인된 연구자는 자신의 코호트 환자 데이터만 조회 가능'),
        ('임상의 담당환자 Row 필터링','row_level','{"user_role":"임상의"}','{"filter":"person_id IN (:assigned_patients)","description":"담당 환자만 접근"}',20,'{person,visit_occurrence,condition_occurrence}',TRUE,'임상의는 담당 환자 데이터만 조회 가능'),
        ('분석가 식별자 Column 마스킹','column_level','{"user_role":"분석가"}','{"mask_columns":["person_source_value","gender_source_value"],"method":"SHA-256"}',30,'{person}',TRUE,'분석가는 직접식별자 컬럼이 해시로 마스킹됨'),
        ('민감 텍스트 Cell 마스킹','cell_level','{"column_type":"text","security_level":"극비"}','{"regex_patterns":["\\\\d{6}-\\\\d{7}","\\\\d{3}-\\\\d{4}-\\\\d{4}","[가-힣]{2,4}\\\\s*님"],"replacement":"[MASKED]"}',40,'{note_nlp,observation}',TRUE,'자유텍스트 내 주민번호, 전화번호, 이름 패턴 실시간 마스킹'),
        ('업무시간 외 접근 제한','context_based','{"time_range":"outside_business_hours","business_hours":{"start":"08:00","end":"18:00"}}','{"restrict":"deny_write","allow":"read_only","alert":true}',50,NULL,TRUE,'업무시간(08-18시) 외 쓰기 차단, 읽기만 허용'),
        ('외부 네트워크 접근 제한','context_based','{"network":"external","ip_not_in":["10.0.0.0/8","172.16.0.0/12"]}','{"restrict":"deny_all","alert":true,"description":"외부 네트워크에서 접근 차단"}',5,NULL,TRUE,'병원 내부 네트워크가 아닌 경우 접근 차단'),
        ('연구목적 시 비식별 완화','context_based','{"purpose":"research","has_irb":true,"approved_reid":true}','{"override_masking":["condition_source_value","drug_source_value"],"log_level":"detailed"}',60,'{condition_occurrence,drug_exposure}',TRUE,'IRB + 재식별 승인 시 특정 컬럼 마스킹 해제')
        """)
        # Masking Rules
        await conn.execute("""
        INSERT INTO sec_masking_rule (name, masking_level, target_table, target_column, masking_method, condition, parameters) VALUES
        ('주민번호 해시','column','person','person_source_value','hash',NULL,'{"algorithm":"SHA-256","salt":"asan_2026"}'),
        ('성별코드 코드변환','column','person','gender_source_value','code_map',NULL,'{"mapping":{"M":"G1","F":"G2"}}'),
        ('출생연도 라운딩','column','person','year_of_birth','rounding',NULL,'{"unit":5}'),
        ('진단코드 부분마스킹','column','condition_occurrence','condition_source_value','partial_mask',NULL,'{"keep_prefix":3,"mask_char":"*"}'),
        ('자유텍스트 패턴마스킹','cell','note_nlp','note_text','regex_mask','{"content_contains":"\\\\d{6}"}','{"patterns":["\\\\d{6}-\\\\d{7}","\\\\d{3}-\\\\d{3,4}-\\\\d{4}"],"replacement":"[MASKED]"}'),
        ('검사수치 범위변환','cell','measurement','value_as_number','range_bucket','{"value_gt":1000}','{"buckets":[0,100,500,1000,5000,99999],"labels":["매우낮음","낮음","보통","높음","매우높음"]}'),
        ('사망기록 Row 제한','row','death',NULL,'row_filter',NULL,'{"require_role":["관리자"],"deny_message":"사망 기록은 관리자만 접근 가능합니다"}'),
        ('방문기록 담당환자 필터','row','visit_occurrence',NULL,'row_filter','{"user_role":"임상의"}','{"filter_column":"person_id","filter_source":"user.assigned_patients"}'),
        ('약품코드 연구자 제한','column','drug_exposure','drug_source_value','partial_mask','{"user_role_not":"임상의"}','{"keep_prefix":2,"mask_char":"*"}')
        """)
        # Access Audit Log (sample)
        await conn.execute("""
        INSERT INTO sec_access_log (user_id, user_name, action, target_table, target_column, policy_ids, result, context, ip_address, created_at) VALUES
        ('res01','김연구','SELECT','condition_occurrence','condition_source_value','{1,6}','masked','{"purpose":"research","session":"sess-001"}','10.0.1.50',NOW() - INTERVAL '2 hours'),
        ('doc01','최임상','SELECT','visit_occurrence',NULL,'{9}','filtered','{"purpose":"clinical","patients_count":10}','10.0.2.30',NOW() - INTERVAL '1 hour'),
        ('ana01','이분석','SELECT','person','person_source_value','{1}','denied','{"purpose":"analysis","reason":"no_reid_approval"}','10.0.3.20',NOW() - INTERVAL '30 minutes'),
        ('admin01','박보안','UPDATE','sec_policy',NULL,'{}','allowed','{"purpose":"admin","action":"policy_update"}','10.0.0.10',NOW() - INTERVAL '15 minutes'),
        ('res01','김연구','SELECT','drug_exposure','drug_source_value','{7}','masked','{"purpose":"research","reid_approved":true}','10.0.1.50',NOW() - INTERVAL '10 minutes'),
        ('doc02','한진료','SELECT','person','person_source_value','{1}','denied','{"purpose":"clinical","reason":"not_assigned_patient"}','10.0.2.31',NOW() - INTERVAL '5 minutes'),
        ('nurse01','송간호','SELECT','measurement','value_as_number','{8}','allowed','{"purpose":"clinical","ward":"ICU"}','10.0.2.40',NOW()),
        ('admin02','윤관리','EXPORT','person',NULL,'{1,2,3}','denied','{"purpose":"export","reason":"bulk_export_restricted"}','192.168.1.100',NOW())
        """)
    finally:
        await conn.close()


# ── Pydantic Models ──

class SecurityPolicyCreate(BaseModel):
    target_type: str = Field(..., pattern=r"^(table|column)$")
    target_table: str = Field(..., max_length=100)
    target_column: Optional[str] = Field(None, max_length=100)
    security_type: str = Field(..., max_length=50)
    security_level: str = Field(..., pattern=r"^(극비|민감|일반|공개)$")
    legal_basis: Optional[str] = Field(None, max_length=200)
    deident_method: Optional[str] = Field(None, max_length=50)
    deident_timing: Optional[str] = Field(None, max_length=50)
    description: Optional[str] = Field(None, max_length=500)
    enabled: bool = True


class TermSecurityRuleCreate(BaseModel):
    term_name: str = Field(..., max_length=100)
    security_type: str = Field(..., max_length=50)
    security_level: str = Field(..., pattern=r"^(극비|민감|일반|공개)$")
    deident_method: Optional[str] = Field(None, max_length=50)
    auto_propagate: bool = True
    description: Optional[str] = Field(None, max_length=300)


class BizSecurityMetaCreate(BaseModel):
    table_name: str = Field(..., max_length=100)
    column_name: Optional[str] = Field(None, max_length=100)
    security_type: str = Field(..., max_length=50)
    deident_target: bool = False
    deident_timing: Optional[str] = Field(None, max_length=50)
    deident_level: Optional[str] = Field(None, max_length=50)
    deident_method: Optional[str] = Field(None, max_length=50)
    retention_period: Optional[str] = Field(None, max_length=50)
    access_scope: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = Field(None, max_length=500)


class ReidRequestCreate(BaseModel):
    requester: str = Field(..., max_length=100)
    department: str = Field(..., max_length=100)
    purpose: str = Field(..., max_length=500)
    justification: str = Field(..., max_length=1000)
    target_tables: List[str]
    target_columns: List[str]
    row_filter: Optional[str] = Field(None, max_length=500)
    duration_days: int = Field(..., ge=1, le=365)


class ReidScopeUpdate(BaseModel):
    scope_tables: List[str]
    scope_columns: List[str]
    row_filter: Optional[str] = Field(None, max_length=500)
    max_rows: Optional[int] = Field(None, ge=1)
    expires_at: Optional[str] = None


class UserAttributeCreate(BaseModel):
    user_id: str = Field(..., max_length=100)
    user_name: str = Field(..., max_length=100)
    department: str = Field(..., max_length=100)
    research_field: Optional[str] = Field(None, max_length=100)
    rank: str = Field(..., max_length=50)
    role_id: Optional[int] = None
    attributes: Optional[Dict[str, Any]] = None


class DynamicPolicyCreate(BaseModel):
    name: str = Field(..., max_length=200)
    policy_type: str = Field(..., pattern=r"^(row_level|column_level|cell_level|context_based)$")
    condition: Dict[str, Any]
    action: Dict[str, Any]
    priority: int = Field(default=100, ge=1, le=1000)
    target_tables: Optional[List[str]] = None
    enabled: bool = True
    description: Optional[str] = Field(None, max_length=500)


class MaskingRuleCreate(BaseModel):
    name: str = Field(..., max_length=200)
    masking_level: str = Field(..., pattern=r"^(row|column|cell)$")
    target_table: str = Field(..., max_length=100)
    target_column: Optional[str] = Field(None, max_length=100)
    masking_method: str = Field(..., max_length=50)
    condition: Optional[Dict[str, Any]] = None
    parameters: Optional[Dict[str, Any]] = None
    enabled: bool = True
