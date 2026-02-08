"""
Shared utilities for data_mart_ops sub-modules.
DB config, connection helper, table setup, seed data, and Pydantic models.
"""
import os
import json
import hashlib
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import HTTPException
from pydantic import BaseModel, Field
import asyncpg

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

class MartCreate(BaseModel):
    mart_name: str = Field(..., max_length=100)
    description: Optional[str] = Field(None, max_length=1000)
    purpose: str = Field(..., max_length=200)
    zone: str = Field(default="gold", pattern=r"^(silver|gold|mart)$")
    source_tables: List[str] = Field(default_factory=list)
    target_schema: Optional[Dict[str, Any]] = None
    owner: Optional[str] = Field(None, max_length=50)
    refresh_schedule: Optional[str] = Field(None, max_length=100)
    retention_days: int = Field(default=365)


class DimensionCreate(BaseModel):
    dimension_name: str = Field(..., max_length=100)
    logical_name: str = Field(..., max_length=100)
    hierarchy_levels: List[Dict[str, str]] = Field(default_factory=list)
    attributes: List[Dict[str, str]] = Field(default_factory=list)
    description: Optional[str] = Field(None, max_length=500)
    mart_ids: List[int] = Field(default_factory=list)


class StandardMetricCreate(BaseModel):
    metric_name: str = Field(..., max_length=100)
    logical_name: str = Field(..., max_length=100)
    formula: str = Field(..., max_length=500)
    unit: Optional[str] = Field(None, max_length=50)
    dimension_ids: List[int] = Field(default_factory=list)
    mart_id: Optional[int] = None
    category: str = Field(default="clinical", pattern=r"^(clinical|operational|financial|quality|research)$")
    description: Optional[str] = Field(None, max_length=500)
    catalog_visible: bool = True


class FlowStageCreate(BaseModel):
    stage_name: str = Field(..., max_length=100)
    stage_order: int = Field(ge=0)
    stage_type: str = Field(..., pattern=r"^(ingest|cleanse|transform|enrich|serve)$")
    storage_type: str = Field(default="object_storage")
    file_format: str = Field(default="parquet")
    processing_rules: Optional[Dict[str, Any]] = None
    meta_config: Optional[Dict[str, Any]] = None
    description: Optional[str] = Field(None, max_length=500)


class OptimizationCreate(BaseModel):
    mart_id: int
    opt_type: str = Field(..., pattern=r"^(materialized_view|index|partition|denormalize|cache)$")
    config: Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = Field(None, max_length=500)


# ── Table Setup ──

async def _ensure_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS dm_mart_registry (
            mart_id SERIAL PRIMARY KEY,
            mart_name VARCHAR(100) NOT NULL,
            description TEXT,
            purpose VARCHAR(200) NOT NULL,
            zone VARCHAR(20) NOT NULL DEFAULT 'gold',
            source_tables JSONB DEFAULT '[]',
            target_schema JSONB,
            schema_hash VARCHAR(64),
            owner VARCHAR(50),
            refresh_schedule VARCHAR(100),
            retention_days INTEGER DEFAULT 365,
            status VARCHAR(20) DEFAULT 'active',
            row_count BIGINT DEFAULT 0,
            last_refreshed TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS dm_schema_change (
            change_id SERIAL PRIMARY KEY,
            mart_id INTEGER REFERENCES dm_mart_registry(mart_id) ON DELETE CASCADE,
            change_type VARCHAR(30) NOT NULL,
            old_schema JSONB,
            new_schema JSONB,
            columns_added JSONB DEFAULT '[]',
            columns_removed JSONB DEFAULT '[]',
            columns_modified JSONB DEFAULT '[]',
            impact_summary TEXT,
            dependent_marts JSONB DEFAULT '[]',
            status VARCHAR(20) DEFAULT 'pending',
            applied_by VARCHAR(50),
            applied_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS dm_dimension (
            dim_id SERIAL PRIMARY KEY,
            dimension_name VARCHAR(100) NOT NULL,
            logical_name VARCHAR(100) NOT NULL,
            hierarchy_levels JSONB DEFAULT '[]',
            attributes JSONB DEFAULT '[]',
            description TEXT,
            mart_ids JSONB DEFAULT '[]',
            record_count BIGINT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS dm_standard_metric (
            metric_id SERIAL PRIMARY KEY,
            metric_name VARCHAR(100) NOT NULL,
            logical_name VARCHAR(100) NOT NULL,
            formula VARCHAR(500) NOT NULL,
            unit VARCHAR(50),
            dimension_ids JSONB DEFAULT '[]',
            mart_id INTEGER,
            category VARCHAR(30) DEFAULT 'clinical',
            description TEXT,
            catalog_visible BOOLEAN DEFAULT TRUE,
            last_calculated TIMESTAMPTZ,
            last_value DOUBLE PRECISION,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS dm_flow_stage (
            stage_id SERIAL PRIMARY KEY,
            stage_name VARCHAR(100) NOT NULL,
            stage_order INTEGER NOT NULL DEFAULT 0,
            stage_type VARCHAR(30) NOT NULL,
            storage_type VARCHAR(50) DEFAULT 'object_storage',
            file_format VARCHAR(30) DEFAULT 'parquet',
            processing_rules JSONB,
            meta_config JSONB,
            description TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS dm_connection_optimization (
            opt_id SERIAL PRIMARY KEY,
            mart_id INTEGER REFERENCES dm_mart_registry(mart_id) ON DELETE CASCADE,
            opt_type VARCHAR(30) NOT NULL,
            config JSONB DEFAULT '{}',
            description TEXT,
            status VARCHAR(20) DEFAULT 'proposed',
            performance_before JSONB,
            performance_after JSONB,
            applied_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)


_SEEDED = False


async def _ensure_seed(conn):
    global _SEEDED
    if _SEEDED:
        return
    cnt = await conn.fetchval("SELECT COUNT(*) FROM dm_mart_registry")
    if cnt > 0:
        _SEEDED = True
        return

    # -- Mart Registry --
    marts = [
        ("환자 기본 마트", "환자 인구통계 + 보험 정보 통합 마트", "환자 코호트 분석", "gold",
         '["person","payer_plan_period","observation_period"]',
         '{"columns":["person_id","gender","birth_year","insurance_type","obs_start","obs_end"]}',
         "data_team", "0 2 * * *", 730),
        ("진단 분석 마트", "진단/상병 코드 기반 분석 마트", "질환별 환자 분포 분석", "gold",
         '["condition_occurrence","condition_era","person"]',
         '{"columns":["person_id","condition_code","condition_name","start_date","end_date","duration_days"]}',
         "clinical_team", "0 3 * * *", 365),
        ("약물 처방 마트", "약물 처방/투여 패턴 분석 마트", "약물 사용 패턴 및 상호작용 분석", "gold",
         '["drug_exposure","drug_era","person"]',
         '{"columns":["person_id","drug_code","drug_name","start_date","quantity","days_supply"]}',
         "pharmacy_team", "0 4 * * *", 365),
        ("검사 결과 마트", "Lab/Vital 검사 결과 통합 마트", "검사 추이 및 이상치 분석", "gold",
         '["measurement","person","visit_occurrence"]',
         '{"columns":["person_id","visit_id","measurement_code","value","unit","range_low","range_high","date"]}',
         "lab_team", "0 1 * * *", 180),
        ("방문 통계 마트", "외래/입원/응급 방문 통계 마트", "의료 이용 패턴 분석", "mart",
         '["visit_occurrence","visit_detail","person","cost"]',
         '{"columns":["person_id","visit_type","visit_date","duration","cost","department"]}',
         "bi_team", "30 1 * * *", 365),
        ("진료비 분석 마트", "진료비/보험 청구 분석 마트", "수익성 및 원가 분석", "mart",
         '["cost","visit_occurrence","payer_plan_period","person"]',
         '{"columns":["person_id","visit_id","total_cost","paid_by_payer","paid_by_patient","plan_type"]}',
         "finance_team", "0 5 * * 1", 730),
    ]
    for m in marts:
        schema_hash = hashlib.sha256(m[5].encode()).hexdigest()[:16]
        await conn.execute("""
            INSERT INTO dm_mart_registry (mart_name, description, purpose, zone, source_tables, target_schema,
                                          schema_hash, owner, refresh_schedule, retention_days, row_count)
            VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9,$10, $11)
        """, m[0], m[1], m[2], m[3], m[4], m[5], schema_hash, m[6], m[7], m[8],
            [76074, 280000, 390000, 3660000, 450000, 440000][marts.index(m)])

    # -- Schema Changes --
    changes = [
        (1, "column_add", '[]', '["insurance_type"]', '[]', '["insurance_type"]', '[]',
         "보험 유형 컬럼 추가로 환자 마트 확장", '[]', "applied"),
        (4, "column_modify", '[]', '[]', '[]', '[]',
         '[{"column":"value","old_type":"numeric(10,2)","new_type":"double precision"}]',
         "검사 결과 정밀도 향상을 위한 타입 변경", '[5]', "applied"),
        (2, "column_add", '[]', '["severity","onset_type"]', '[]', '["severity","onset_type"]', '[]',
         "진단 심각도/발생유형 추가 — 방문 통계 마트에 영향", '[5]', "pending"),
    ]
    for c in changes:
        await conn.execute("""
            INSERT INTO dm_schema_change (mart_id, change_type, old_schema, new_schema,
                                          columns_added, columns_removed, columns_modified,
                                          impact_summary, dependent_marts, status)
            VALUES ($1,$2,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8,$9::jsonb,$10)
        """, *c)

    # -- Dimensions --
    dims = [
        ("dim_time", "시간 Dimension", '[{"level":"year","key":"year"},{"level":"quarter","key":"quarter"},{"level":"month","key":"month"},{"level":"day","key":"day"}]',
         '[{"name":"is_weekend","type":"boolean"},{"name":"fiscal_quarter","type":"varchar"}]',
         "연/분기/월/일 계층 시간 차원", "[1,2,3,4,5,6]"),
        ("dim_patient", "환자 Dimension", '[{"level":"gender","key":"gender_source_value"},{"level":"age_group","key":"age_group"},{"level":"person_id","key":"person_id"}]',
         '[{"name":"birth_year","type":"integer"},{"name":"insurance_type","type":"varchar"}]',
         "성별/연령대/환자ID 계층", "[1,2,3,4,5,6]"),
        ("dim_diagnosis", "진단 Dimension", '[{"level":"category","key":"condition_category"},{"level":"group","key":"condition_group"},{"level":"code","key":"condition_source_value"}]',
         '[{"name":"snomed_code","type":"varchar"},{"name":"icd_code","type":"varchar"}]',
         "진단 카테고리/그룹/코드 계층 (SNOMED-CT 기반)", "[2]"),
        ("dim_drug", "약물 Dimension", '[{"level":"class","key":"drug_class"},{"level":"ingredient","key":"ingredient"},{"level":"product","key":"drug_source_value"}]',
         '[{"name":"atc_code","type":"varchar"},{"name":"route","type":"varchar"}]',
         "약물 계열/성분/제품 계층 (ATC 기반)", "[3]"),
        ("dim_visit", "방문유형 Dimension", '[{"level":"type","key":"visit_concept_id"},{"level":"department","key":"care_site"}]',
         '[{"name":"type_name","type":"varchar"}]',
         "방문유형(입원/외래/응급)/진료과 계층", "[5,6]"),
    ]
    for d in dims:
        await conn.execute("""
            INSERT INTO dm_dimension (dimension_name, logical_name, hierarchy_levels, attributes, description, mart_ids)
            VALUES ($1,$2,$3::jsonb,$4::jsonb,$5,$6::jsonb)
        """, *d)

    # -- Standard Metrics --
    metrics = [
        ("patient_count", "환자 수", "COUNT(DISTINCT person_id)", "명", "[1,2]", 1, "clinical", "고유 환자 수", True),
        ("visit_count", "방문 건수", "COUNT(visit_occurrence_id)", "건", "[2,5]", 5, "operational", "총 방문 건수", True),
        ("avg_los", "평균 재원일수", "AVG(EXTRACT(DAY FROM visit_end_date - visit_start_date))", "일", "[2,5]", 5, "operational", "입원 환자 평균 재원 기간", True),
        ("condition_prevalence", "유병률", "COUNT(DISTINCT person_id) / total_patients * 100", "%", "[1,3]", 2, "clinical", "특정 진단의 유병률", True),
        ("drug_utilization", "약물 사용률", "COUNT(DISTINCT person_id with drug) / total_patients * 100", "%", "[1,4]", 3, "clinical", "특정 약물 사용 비율", True),
        ("avg_cost_per_visit", "방문당 평균 진료비", "SUM(total_cost) / COUNT(visit_id)", "원", "[2,5]", 6, "financial", "방문 건당 평균 진료비", True),
        ("readmission_rate", "재입원율", "COUNT(readmission) / COUNT(admission) * 100", "%", "[2,5]", 5, "quality", "30일 내 재입원율", True),
        ("lab_abnormal_rate", "검사 이상률", "COUNT(abnormal) / COUNT(total_tests) * 100", "%", "[1,2]", 4, "clinical", "검사 결과 이상치 비율", True),
    ]
    for m in metrics:
        await conn.execute("""
            INSERT INTO dm_standard_metric (metric_name, logical_name, formula, unit, dimension_ids,
                                            mart_id, category, description, catalog_visible)
            VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9)
        """, *m)

    # -- Flow Stages --
    stages = [
        ("원천 수집", 0, "ingest", "object_storage", "csv",
         '{"dedup":false,"validation":"schema_only"}',
         '{"capture_source_meta":true,"record_lineage":true}',
         "EHR/EMR/Lab 등 원천 시스템에서 데이터 수집. 원본 형태 보존."),
        ("정제/클렌징", 1, "cleanse", "object_storage", "parquet",
         '{"null_handling":"flag","type_casting":true,"dedup":true}',
         '{"quality_score":true,"error_log":true}',
         "비표준→표준 용어 매핑, 결측치 처리, 중복 제거, 타입 변환."),
        ("변환/표준화", 2, "transform", "rdbms", "parquet",
         '{"omop_mapping":true,"concept_mapping":true,"unit_normalize":true}',
         '{"column_lineage":true,"transform_rules":true}',
         "OMOP CDM 표준 모델로 변환. 비정형→정형 구조화 수행."),
        ("통합/강화", 3, "enrich", "rdbms", "parquet",
         '{"join_dimensions":true,"calculate_metrics":true,"aggregate":true}',
         '{"dimension_snapshot":true,"metric_cache":true}',
         "Dimension 결합, 표준 지표 산출, 집계 테이블 생성."),
        ("서빙/제공", 4, "serve", "rdbms", "delta",
         '{"materialized_view":true,"index_optimize":true,"cache_policy":"5min"}',
         '{"catalog_register":true,"access_control":true}',
         "데이터 마트 서빙. MV/인덱스 최적화, 카탈로그 등록, 접근 제어."),
    ]
    for s in stages:
        await conn.execute("""
            INSERT INTO dm_flow_stage (stage_name, stage_order, stage_type, storage_type,
                                       file_format, processing_rules, meta_config, description)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8)
        """, *s)

    # -- Connection Optimizations --
    opts = [
        (4, "materialized_view", '{"view_name":"mv_measurement_daily","refresh":"REFRESH MATERIALIZED VIEW CONCURRENTLY","schedule":"0 */6 * * *","query":"SELECT person_id,measurement_date::date,measurement_source_value,AVG(value_as_number) FROM measurement GROUP BY 1,2,3"}',
         "검사 결과 일별 집계 MV — 36M 행 → ~500K 행으로 축소", "applied"),
        (5, "index", '{"index_name":"idx_visit_type_date","columns":["visit_concept_id","visit_start_date"],"type":"btree"}',
         "방문유형+일자 복합 인덱스 — 방문 패턴 쿼리 10x 향상", "applied"),
        (2, "partition", '{"strategy":"range","column":"condition_start_date","interval":"yearly","partitions":["2018","2019","2020","2021","2022","2023","2024","2025"]}',
         "진단 마트 연도별 파티셔닝 — 연간 조회 시 스캔 범위 축소", "proposed"),
        (6, "denormalize", '{"embed_fields":["visit_type_name","payer_name"],"source_joins":["visit_occurrence","payer_plan_period"]}',
         "진료비 마트에 방문유형명/보험자명 비정규화 내장 — JOIN 제거", "proposed"),
        (1, "cache", '{"cache_type":"redis","ttl_seconds":300,"key_pattern":"patient_mart:{person_id}","warmup":"top_1000_patients"}',
         "환자 기본 마트 Redis 캐시 — 빈번 조회 환자 300초 TTL", "proposed"),
    ]
    for o in opts:
        await conn.execute("""
            INSERT INTO dm_connection_optimization (mart_id, opt_type, config, description, status)
            VALUES ($1,$2,$3::jsonb,$4,$5)
        """, *o)

    _SEEDED = True


async def _init(conn):
    await _ensure_tables(conn)
    await _ensure_seed(conn)
