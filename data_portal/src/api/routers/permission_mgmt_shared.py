"""
DGR-007 권한 관리 — 공유 모듈
Pydantic 모델, DB 설정, 테이블 생성 / 시드 헬퍼
"""
import json as _json
from datetime import datetime
from typing import List, Optional, Dict, Any

import asyncpg
from pydantic import BaseModel, Field


# ── DB Config ──

OMOP_DB_CONFIG = {
    "host": "localhost", "port": 5436,
    "user": "omopuser", "password": "omop", "database": "omop_cdm",
}


# ── Pydantic Models ──

class DatasetCreate(BaseModel):
    name: str = Field(..., max_length=200)
    description: Optional[str] = Field(None, max_length=500)
    dataset_type: str = Field(..., pattern=r"^(table_set|column_set|row_filtered|composite)$")
    tables: List[str]
    columns: Optional[Dict[str, List[str]]] = None
    row_filter: Optional[str] = Field(None, max_length=500)
    owner: Optional[str] = Field(None, max_length=100)
    classification: Optional[str] = Field(None, max_length=50)


class PermGrantCreate(BaseModel):
    dataset_id: Optional[int] = None
    target_table: Optional[str] = Field(None, max_length=100)
    target_columns: Optional[List[str]] = None
    row_filter: Optional[str] = Field(None, max_length=500)
    grant_type: str = Field(..., pattern=r"^(select|insert|update|delete|execute|reidentify|export|admin)$")
    grantee_type: str = Field(..., pattern=r"^(role|user|role_assignment)$")
    grantee_id: str = Field(..., max_length=100)
    source_role_id: Optional[int] = None
    granted_by: Optional[str] = Field(None, max_length=100)
    expires_at: Optional[str] = None
    condition: Optional[Dict[str, Any]] = None


class RoleAssignmentCreate(BaseModel):
    user_id: str = Field(..., max_length=100)
    role_id: int
    assignment_type: str = Field(default="primary", pattern=r"^(primary|secondary|temporary|delegated)$")
    priority: int = Field(default=100, ge=1, le=1000)
    parameters: Optional[Dict[str, Any]] = None
    expires_at: Optional[str] = None
    granted_by: Optional[str] = Field(None, max_length=100)


class RoleParamDefCreate(BaseModel):
    role_id: int
    param_name: str = Field(..., max_length=100)
    param_type: str = Field(..., pattern=r"^(string|integer|boolean|date|list|json)$")
    description: Optional[str] = Field(None, max_length=300)
    default_value: Optional[str] = Field(None, max_length=500)
    validation_rule: Optional[str] = Field(None, max_length=500)
    required: bool = False


class EamMappingCreate(BaseModel):
    eam_role_code: str = Field(..., max_length=50)
    eam_role_name: str = Field(..., max_length=200)
    eam_department: Optional[str] = Field(None, max_length=100)
    platform_role_id: int
    mapping_rule: Optional[Dict[str, Any]] = None
    auto_sync: bool = True


class EdwMigrationCreate(BaseModel):
    edw_source: str = Field(..., max_length=100)
    edw_permission_type: str = Field(..., max_length=50)
    edw_object_name: str = Field(..., max_length=200)
    edw_grantee: str = Field(..., max_length=100)
    platform_dataset_id: Optional[int] = None
    platform_role_id: Optional[int] = None
    platform_grant_type: Optional[str] = Field(None, max_length=50)


# ── DB Helpers ──

async def get_conn():
    return await asyncpg.connect(**OMOP_DB_CONFIG)


async def ensure_tables():
    conn = await get_conn()
    try:
        await conn.execute("""
        -- 1) Dataset (데이터세트 추상화)
        CREATE TABLE IF NOT EXISTS perm_dataset (
            dataset_id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            dataset_type VARCHAR(20) NOT NULL CHECK (dataset_type IN ('table_set','column_set','row_filtered','composite')),
            tables TEXT[] NOT NULL,
            columns JSONB,
            row_filter TEXT,
            owner VARCHAR(100),
            classification VARCHAR(50),
            active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 2) Permission Grant (테이블/컬럼/로우 권한 부여)
        CREATE TABLE IF NOT EXISTS perm_grant (
            grant_id SERIAL PRIMARY KEY,
            dataset_id INTEGER REFERENCES perm_dataset(dataset_id) ON DELETE SET NULL,
            target_table VARCHAR(100),
            target_columns TEXT[],
            row_filter TEXT,
            grant_type VARCHAR(20) NOT NULL CHECK (grant_type IN ('select','insert','update','delete','execute','reidentify','export','admin')),
            grantee_type VARCHAR(20) NOT NULL CHECK (grantee_type IN ('role','user','role_assignment')),
            grantee_id VARCHAR(100) NOT NULL,
            source_role_id INTEGER,
            granted_by VARCHAR(100),
            active BOOLEAN DEFAULT TRUE,
            expires_at TIMESTAMP,
            condition JSONB,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 3) Role Assignment (복수 역할 할당)
        CREATE TABLE IF NOT EXISTS perm_role_assignment (
            assignment_id SERIAL PRIMARY KEY,
            user_id VARCHAR(100) NOT NULL,
            role_id INTEGER NOT NULL,
            assignment_type VARCHAR(20) DEFAULT 'primary' CHECK (assignment_type IN ('primary','secondary','temporary','delegated')),
            priority INTEGER DEFAULT 100,
            parameters JSONB DEFAULT '{}',
            active BOOLEAN DEFAULT TRUE,
            expires_at TIMESTAMP,
            granted_by VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(user_id, role_id)
        );

        -- 4) Role Parameter Definition (역할 파라미터 정의)
        CREATE TABLE IF NOT EXISTS perm_role_param_def (
            param_def_id SERIAL PRIMARY KEY,
            role_id INTEGER NOT NULL,
            param_name VARCHAR(100) NOT NULL,
            param_type VARCHAR(20) NOT NULL CHECK (param_type IN ('string','integer','boolean','date','list','json')),
            description TEXT,
            default_value TEXT,
            validation_rule TEXT,
            required BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(role_id, param_name)
        );

        -- 5) EAM Role Mapping (AMIS 3.0 EAM 연계)
        CREATE TABLE IF NOT EXISTS perm_eam_mapping (
            mapping_id SERIAL PRIMARY KEY,
            eam_role_code VARCHAR(50) NOT NULL UNIQUE,
            eam_role_name VARCHAR(200) NOT NULL,
            eam_department VARCHAR(100),
            platform_role_id INTEGER NOT NULL,
            mapping_rule JSONB,
            auto_sync BOOLEAN DEFAULT TRUE,
            last_synced_at TIMESTAMP,
            sync_status VARCHAR(20) DEFAULT 'pending' CHECK (sync_status IN ('pending','synced','error','disabled')),
            sync_count INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );

        -- 6) EDW Migration (EDW 권한 이관)
        CREATE TABLE IF NOT EXISTS perm_edw_migration (
            migration_id SERIAL PRIMARY KEY,
            edw_source VARCHAR(100) NOT NULL,
            edw_permission_type VARCHAR(50) NOT NULL,
            edw_object_name VARCHAR(200) NOT NULL,
            edw_grantee VARCHAR(100) NOT NULL,
            platform_dataset_id INTEGER REFERENCES perm_dataset(dataset_id) ON DELETE SET NULL,
            platform_role_id INTEGER,
            platform_grant_type VARCHAR(50),
            status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending','mapped','migrated','verified','failed','skipped')),
            migration_note TEXT,
            migrated_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        -- 7) Permission Audit (권한 감사)
        CREATE TABLE IF NOT EXISTS perm_audit (
            audit_id BIGSERIAL PRIMARY KEY,
            action VARCHAR(50) NOT NULL,
            actor VARCHAR(100),
            target_type VARCHAR(50),
            target_id VARCHAR(100),
            details JSONB,
            source_roles INTEGER[],
            effective_grants TEXT[],
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)
    finally:
        await conn.close()


async def ensure_seed():
    conn = await get_conn()
    try:
        cnt = await conn.fetchval("SELECT COUNT(*) FROM perm_dataset")
        if cnt > 0:
            return

        # Datasets
        await conn.execute("""
        INSERT INTO perm_dataset (name, description, dataset_type, tables, columns, row_filter, owner, classification) VALUES
        ('OMOP CDM 임상 전체','OMOP CDM 전체 임상 테이블셋','table_set','{person,visit_occurrence,condition_occurrence,drug_exposure,measurement,observation,procedure_occurrence,death,note_nlp}',NULL,NULL,'정보보안팀','극비'),
        ('환자 기본정보','person 테이블 비식별 컬럼셋','column_set','{person}','{"person":["person_id","gender_concept_id","year_of_birth","race_concept_id"]}',NULL,'정보보안팀','민감'),
        ('당뇨 코호트','당뇨 환자 condition 데이터','row_filtered','{condition_occurrence,person}',NULL,'condition_concept_id = 44054006','의학연구센터','민감'),
        ('외래 방문기록','외래 방문 row-filtered 데이터셋','row_filtered','{visit_occurrence}',NULL,'visit_concept_id = 9202','진료지원팀','일반'),
        ('약물 부작용 모니터','약물 노출 + 상태 변화 복합셋','composite','{drug_exposure,condition_occurrence,person}','{"person":["person_id","year_of_birth"]}','drug_concept_id IS NOT NULL','약무팀','민감'),
        ('검사결과 집계','measurement 집계 분석용','table_set','{measurement}',NULL,NULL,'데이터분석팀','일반'),
        ('진료노트 비식별','note_nlp 비식별 처리 데이터','column_set','{note_nlp}','{"note_nlp":["note_id","note_type_concept_id","note_date"]}',NULL,'정보보안팀','극비'),
        ('EDW 통계 마트','EDW 이관 통계 데이터셋','table_set','{visit_occurrence,condition_occurrence,measurement}',NULL,NULL,'데이터분석팀','일반')
        """)

        # Role Assignments (복수 역할)
        await conn.execute("""
        INSERT INTO perm_role_assignment (user_id, role_id, assignment_type, priority, parameters, granted_by) VALUES
        ('admin01',1,'primary',10,'{"scope":"full_system","audit_access":true}','system'),
        ('res01',2,'primary',10,'{"irb_number":"2026-0142","cohort_id":"DM-001","max_patients":200}','admin01'),
        ('res01',4,'secondary',50,'{"tool_access":["python","r"],"export_limit":1000}','admin01'),
        ('doc01',3,'primary',10,'{"department":"순환기내과","specialty":"심장내과","patient_list":[1001,1002,1003]}','admin01'),
        ('doc01',2,'secondary',50,'{"irb_number":"2026-0155","cohort_id":"HF-001"}','admin01'),
        ('ana01',4,'primary',10,'{"tool_access":["python","r","sas"],"export_limit":5000}','admin01'),
        ('doc02',3,'primary',10,'{"department":"소아청소년과","specialty":"소아과"}','admin01'),
        ('res02',2,'primary',10,'{"irb_number":"2026-0168","cohort_id":"PGx-001"}','admin01'),
        ('nurse01',3,'primary',10,'{"department":"간호부","ward":"ICU","read_only":true}','admin01'),
        ('admin02',1,'secondary',20,'{"scope":"data_management"}','admin01')
        """)

        # Permission Grants (데이터세트/테이블/컬럼/로우 단위)
        await conn.execute("""
        INSERT INTO perm_grant (dataset_id, target_table, target_columns, row_filter, grant_type, grantee_type, grantee_id, source_role_id, granted_by) VALUES
        (1,NULL,NULL,NULL,'admin','role','1',1,'system'),
        (2,NULL,NULL,NULL,'select','role','2',2,'admin01'),
        (3,NULL,NULL,NULL,'select','role','2',2,'admin01'),
        (3,NULL,NULL,NULL,'reidentify','user','res01',2,'admin01'),
        (6,NULL,NULL,NULL,'select','role','4',4,'admin01'),
        (6,NULL,NULL,NULL,'export','role','4',4,'admin01'),
        (NULL,'person','{"person_id","gender_concept_id","year_of_birth"}',NULL,'select','role','3',3,'admin01'),
        (NULL,'visit_occurrence',NULL,'person_id IN (:assigned_patients)','select','role','3',3,'admin01'),
        (NULL,'condition_occurrence',NULL,'person_id IN (:assigned_patients)','select','role','3',3,'admin01'),
        (4,NULL,NULL,NULL,'select','role','3',3,'admin01'),
        (5,NULL,NULL,NULL,'select','role','2',2,'admin01'),
        (7,NULL,NULL,NULL,'select','role','1',1,'system'),
        (8,NULL,NULL,NULL,'select','role','4',4,'admin01'),
        (NULL,'measurement',NULL,NULL,'select','role','3',3,'admin01')
        """)

        # Role Parameter Definitions
        await conn.execute("""
        INSERT INTO perm_role_param_def (role_id, param_name, param_type, description, default_value, validation_rule, required) VALUES
        (1,'scope','string','관리 범위','full_system','IN(full_system,data_management,security_only)',TRUE),
        (1,'audit_access','boolean','감사 로그 접근 여부','true',NULL,FALSE),
        (2,'irb_number','string','IRB 승인 번호',NULL,'REGEX(^\\d{4}-\\d{4}$)',TRUE),
        (2,'cohort_id','string','코호트 식별자',NULL,NULL,FALSE),
        (2,'max_patients','integer','최대 환자 수 제한','100','RANGE(1,10000)',FALSE),
        (3,'department','string','소속 진료과',NULL,NULL,TRUE),
        (3,'specialty','string','전문 분야',NULL,NULL,FALSE),
        (3,'patient_list','list','담당 환자 목록',NULL,NULL,FALSE),
        (3,'read_only','boolean','읽기 전용 여부','false',NULL,FALSE),
        (4,'tool_access','list','허용 분석 도구','["python"]',NULL,FALSE),
        (4,'export_limit','integer','내보내기 행 제한','1000','RANGE(100,100000)',FALSE),
        (2,'research_period','date','연구 기간 (만료일)',NULL,NULL,FALSE)
        """)

        # EAM Mappings (AMIS 3.0 연계)
        await conn.execute("""
        INSERT INTO perm_eam_mapping (eam_role_code, eam_role_name, eam_department, platform_role_id, mapping_rule, auto_sync, sync_status) VALUES
        ('EAM-ADM-001','시스템관리자','정보전략실',1,'{"privilege_level":"admin","sync_params":{"scope":"full_system"}}',TRUE,'synced'),
        ('EAM-DOC-001','전문의','진료부',3,'{"privilege_level":"clinical","sync_params":{"read_only":false}}',TRUE,'synced'),
        ('EAM-DOC-002','전공의','진료부',3,'{"privilege_level":"clinical","sync_params":{"read_only":true}}',TRUE,'synced'),
        ('EAM-RES-001','임상연구원','연구지원부',2,'{"privilege_level":"research","sync_params":{"max_patients":200}}',TRUE,'synced'),
        ('EAM-RES-002','연구교수','연구지원부',2,'{"privilege_level":"research","sync_params":{"max_patients":500}}',TRUE,'synced'),
        ('EAM-NRS-001','간호사','간호부',3,'{"privilege_level":"clinical","sync_params":{"read_only":true}}',TRUE,'synced'),
        ('EAM-ANA-001','데이터분석가','경영지원부',4,'{"privilege_level":"analyst","sync_params":{"export_limit":5000}}',TRUE,'synced'),
        ('EAM-ANA-002','통계분석원','경영지원부',4,'{"privilege_level":"analyst","sync_params":{"export_limit":1000}}',TRUE,'pending'),
        ('EAM-PHR-001','약사','약무부',3,'{"privilege_level":"clinical","sync_params":{"department":"약무부","specialty":"약학"}}',TRUE,'synced'),
        ('EAM-SEC-001','정보보안담당','정보보안팀',1,'{"privilege_level":"security","sync_params":{"scope":"security_only"}}',TRUE,'synced')
        """)

        # EDW Migration Records
        await conn.execute("""
        INSERT INTO perm_edw_migration (edw_source, edw_permission_type, edw_object_name, edw_grantee, platform_dataset_id, platform_role_id, platform_grant_type, status, migration_note) VALUES
        ('EDW_STAT','SELECT','DW_진료통계_월별','통계분석팀',8,4,'select','migrated','월별 진료 통계 → OMOP measurement 집계 매핑'),
        ('EDW_STAT','SELECT','DW_입퇴원통계','경영기획팀',4,4,'select','migrated','입퇴원 통계 → visit_occurrence 외래 필터 매핑'),
        ('EDW_STAT','SELECT','DW_질병통계_ICD','의학연구센터',3,2,'select','migrated','ICD 질병 통계 → condition_occurrence 당뇨 코호트 매핑'),
        ('EDW_STAT','EXECUTE','SP_환자코호트추출','임상시험센터',NULL,2,'execute','mapped','코호트 추출 SP → 플랫폼 코호트 도구로 전환 필요'),
        ('EDW_REID','REIDENTIFY','TB_환자마스터','IRB연구팀',1,2,'reidentify','migrated','환자 마스터 재식별 → sec_reid_request 워크플로우 연계'),
        ('EDW_REID','REIDENTIFY','TB_진단이력','약물감시팀',NULL,2,'reidentify','pending','진단 이력 재식별 → 매핑 대상 데이터셋 미결정'),
        ('EDW_STAT','SELECT','DW_약품처방통계','약무팀',5,3,'select','verified','약품 처방 통계 → drug_exposure 매핑 검증 완료'),
        ('EDW_STAT','SELECT','DW_검사결과통계','진단검사의학과',6,4,'select','migrated','검사 결과 → measurement 매핑'),
        ('EDW_STAT','EXPORT','DW_전체통계_익명','대외협력팀',NULL,4,'export','pending','익명 통계 내보내기 → 데이터셋 미생성'),
        ('EDW_REID','REIDENTIFY','TB_사망기록','의무기록팀',NULL,1,'reidentify','skipped','사망 기록 재식별 → 관리자 직접 접근으로 대체')
        """)

        # Audit Log (seed)
        await conn.execute("""
        INSERT INTO perm_audit (action, actor, target_type, target_id, details, source_roles, effective_grants) VALUES
        ('GRANT','admin01','dataset','1','{"grant_type":"admin","grantee":"role:1"}','{1}','{admin:dataset:1}'),
        ('GRANT','admin01','dataset','2','{"grant_type":"select","grantee":"role:2"}','{1}','{select:dataset:2}'),
        ('ASSIGN_ROLE','admin01','user','res01','{"role_id":2,"type":"primary","irb":"2026-0142"}','{1}','{role:2:primary}'),
        ('ASSIGN_ROLE','admin01','user','res01','{"role_id":4,"type":"secondary"}','{1}','{role:4:secondary}'),
        ('GRANT_REIDENTIFY','admin01','dataset','3','{"grantee":"res01","duration_days":30}','{1}','{reidentify:dataset:3}'),
        ('EAM_SYNC','system','eam_mapping','EAM-DOC-001','{"synced_users":45,"status":"synced"}',NULL,NULL),
        ('EDW_MIGRATE','admin01','edw_permission','DW_진료통계_월별','{"from":"EDW_STAT","to":"dataset:8","status":"migrated"}','{1}','{select:dataset:8}'),
        ('REVOKE','admin01','grant','5','{"reason":"연구 기간 종료","grant_type":"select"}','{1}','{revoke:grant:5}')
        """)
    finally:
        await conn.close()
