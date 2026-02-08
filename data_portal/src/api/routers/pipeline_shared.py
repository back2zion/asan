"""
Pipeline 공유 모듈: DB 설정, 연결 헬퍼, 테이블 생성, 시드 데이터, Pydantic 모델
"""
import os
import json
import random
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from pathlib import Path

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

# ── File Paths ──

DATA_DIR = Path(__file__).parent.parent / "etl_data"
DATA_DIR.mkdir(exist_ok=True)
LZ_TEMPLATES_FILE = DATA_DIR / "landing_zone_templates.json"
EXPORT_PIPELINES_FILE = DATA_DIR / "export_pipelines.json"


# ── DB Helpers ──

async def get_conn():
    try:
        return await asyncpg.connect(**OMOP_DB_CONFIG)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")


async def ensure_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_execution (
            exec_id BIGSERIAL PRIMARY KEY,
            pipeline_id VARCHAR(100),
            pipeline_type VARCHAR(50),
            pipeline_name VARCHAR(200),
            status VARCHAR(30) DEFAULT 'running',
            started_at TIMESTAMP DEFAULT NOW(),
            ended_at TIMESTAMP,
            rows_processed BIGINT DEFAULT 0,
            error_message TEXT
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cdw_query_history (
            query_id BIGSERIAL PRIMARY KEY,
            query_text TEXT,
            tables_accessed TEXT[],
            columns_accessed TEXT[],
            join_tables TEXT[],
            filter_columns TEXT[],
            execution_count INT DEFAULT 1,
            total_time_ms BIGINT DEFAULT 0,
            first_seen TIMESTAMP DEFAULT NOW(),
            last_seen TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_mart_design (
            mart_id SERIAL PRIMARY KEY,
            name VARCHAR(200),
            description TEXT,
            source_tables TEXT[],
            columns JSONB DEFAULT '[]',
            indexes JSONB DEFAULT '[]',
            estimated_rows BIGINT DEFAULT 0,
            status VARCHAR(30) DEFAULT 'draft',
            build_sql TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)


async def seed_query_history(conn):
    """시뮬레이션 CDW 조회 이력 40건 삽입 (최초 1회)"""
    count = await conn.fetchval("SELECT COUNT(*) FROM cdw_query_history")
    if count > 0:
        return

    seed_queries = [
        # 고빈도: person + visit_occurrence 조인
        {"query": "SELECT p.person_id, p.gender_source_value, v.visit_start_date FROM person p JOIN visit_occurrence v ON p.person_id = v.person_id WHERE v.visit_concept_id = 9202",
         "tables": ["person", "visit_occurrence"], "columns": ["person_id", "gender_source_value", "visit_start_date", "visit_concept_id"],
         "joins": ["person-visit_occurrence"], "filters": ["visit_concept_id"], "count": 185, "time": 12500},
        {"query": "SELECT p.person_id, p.year_of_birth, v.visit_start_date, v.visit_end_date FROM person p JOIN visit_occurrence v ON p.person_id = v.person_id WHERE p.gender_source_value = 'M'",
         "tables": ["person", "visit_occurrence"], "columns": ["person_id", "year_of_birth", "visit_start_date", "visit_end_date", "gender_source_value"],
         "joins": ["person-visit_occurrence"], "filters": ["gender_source_value"], "count": 142, "time": 9800},
        {"query": "SELECT COUNT(*) FROM visit_occurrence WHERE visit_concept_id = 9201 AND visit_start_date >= '2020-01-01'",
         "tables": ["visit_occurrence"], "columns": ["visit_concept_id", "visit_start_date"],
         "joins": [], "filters": ["visit_concept_id", "visit_start_date"], "count": 210, "time": 3400},
        # 고빈도: condition_occurrence 조인
        {"query": "SELECT p.person_id, c.condition_concept_id, c.condition_start_date FROM person p JOIN condition_occurrence c ON p.person_id = c.person_id WHERE c.condition_concept_id = 44054006",
         "tables": ["person", "condition_occurrence"], "columns": ["person_id", "condition_concept_id", "condition_start_date"],
         "joins": ["person-condition_occurrence"], "filters": ["condition_concept_id"], "count": 167, "time": 18200},
        {"query": "SELECT v.visit_occurrence_id, c.condition_concept_id FROM visit_occurrence v JOIN condition_occurrence c ON v.visit_occurrence_id = c.visit_occurrence_id WHERE v.visit_concept_id = 9203",
         "tables": ["visit_occurrence", "condition_occurrence"], "columns": ["visit_occurrence_id", "condition_concept_id", "visit_concept_id"],
         "joins": ["visit_occurrence-condition_occurrence"], "filters": ["visit_concept_id"], "count": 98, "time": 22100},
        # 고빈도: drug_exposure
        {"query": "SELECT p.person_id, d.drug_concept_id, d.drug_exposure_start_date FROM person p JOIN drug_exposure d ON p.person_id = d.person_id",
         "tables": ["person", "drug_exposure"], "columns": ["person_id", "drug_concept_id", "drug_exposure_start_date"],
         "joins": ["person-drug_exposure"], "filters": [], "count": 134, "time": 25300},
        {"query": "SELECT d.drug_concept_id, COUNT(*) FROM drug_exposure d JOIN visit_occurrence v ON d.visit_occurrence_id = v.visit_occurrence_id GROUP BY d.drug_concept_id",
         "tables": ["drug_exposure", "visit_occurrence"], "columns": ["drug_concept_id", "visit_occurrence_id"],
         "joins": ["drug_exposure-visit_occurrence"], "filters": [], "count": 89, "time": 31200},
        # measurement
        {"query": "SELECT p.person_id, m.measurement_concept_id, m.value_as_number FROM person p JOIN measurement m ON p.person_id = m.person_id WHERE m.measurement_concept_id = 3004249",
         "tables": ["person", "measurement"], "columns": ["person_id", "measurement_concept_id", "value_as_number"],
         "joins": ["person-measurement"], "filters": ["measurement_concept_id"], "count": 156, "time": 95000},
        {"query": "SELECT m.measurement_concept_id, AVG(m.value_as_number) FROM measurement m WHERE m.value_as_number IS NOT NULL GROUP BY m.measurement_concept_id LIMIT 100",
         "tables": ["measurement"], "columns": ["measurement_concept_id", "value_as_number"],
         "joins": [], "filters": ["value_as_number"], "count": 78, "time": 120000},
        {"query": "SELECT m.person_id, m.measurement_date, m.value_as_number FROM measurement m JOIN visit_occurrence v ON m.visit_occurrence_id = v.visit_occurrence_id WHERE v.visit_concept_id = 9201",
         "tables": ["measurement", "visit_occurrence"], "columns": ["person_id", "measurement_date", "value_as_number", "visit_occurrence_id", "visit_concept_id"],
         "joins": ["measurement-visit_occurrence"], "filters": ["visit_concept_id"], "count": 112, "time": 85000},
        # procedure_occurrence
        {"query": "SELECT p.person_id, pr.procedure_concept_id FROM person p JOIN procedure_occurrence pr ON p.person_id = pr.person_id",
         "tables": ["person", "procedure_occurrence"], "columns": ["person_id", "procedure_concept_id"],
         "joins": ["person-procedure_occurrence"], "filters": [], "count": 95, "time": 45000},
        {"query": "SELECT pr.procedure_concept_id, COUNT(*) FROM procedure_occurrence pr GROUP BY pr.procedure_concept_id ORDER BY COUNT(*) DESC LIMIT 20",
         "tables": ["procedure_occurrence"], "columns": ["procedure_concept_id"],
         "joins": [], "filters": [], "count": 67, "time": 38000},
        # observation
        {"query": "SELECT o.observation_concept_id, o.value_as_string FROM observation o WHERE o.person_id = 12345",
         "tables": ["observation"], "columns": ["observation_concept_id", "value_as_string", "person_id"],
         "joins": [], "filters": ["person_id"], "count": 45, "time": 52000},
        {"query": "SELECT p.person_id, o.observation_concept_id FROM person p JOIN observation o ON p.person_id = o.person_id WHERE o.observation_date >= '2022-01-01'",
         "tables": ["person", "observation"], "columns": ["person_id", "observation_concept_id", "observation_date"],
         "joins": ["person-observation"], "filters": ["observation_date"], "count": 73, "time": 68000},
        # condition_era
        {"query": "SELECT ce.person_id, ce.condition_concept_id, ce.condition_era_start_date FROM condition_era ce WHERE ce.condition_concept_id = 38341003",
         "tables": ["condition_era"], "columns": ["person_id", "condition_concept_id", "condition_era_start_date"],
         "joins": [], "filters": ["condition_concept_id"], "count": 88, "time": 15000},
        # 3-table 조인
        {"query": "SELECT p.person_id, v.visit_start_date, c.condition_concept_id FROM person p JOIN visit_occurrence v ON p.person_id = v.person_id JOIN condition_occurrence c ON v.visit_occurrence_id = c.visit_occurrence_id",
         "tables": ["person", "visit_occurrence", "condition_occurrence"], "columns": ["person_id", "visit_start_date", "condition_concept_id", "visit_occurrence_id"],
         "joins": ["person-visit_occurrence", "visit_occurrence-condition_occurrence"], "filters": [], "count": 124, "time": 42000},
        {"query": "SELECT p.person_id, v.visit_start_date, d.drug_concept_id FROM person p JOIN visit_occurrence v ON p.person_id = v.person_id JOIN drug_exposure d ON v.visit_occurrence_id = d.visit_occurrence_id",
         "tables": ["person", "visit_occurrence", "drug_exposure"], "columns": ["person_id", "visit_start_date", "drug_concept_id", "visit_occurrence_id"],
         "joins": ["person-visit_occurrence", "visit_occurrence-drug_exposure"], "filters": [], "count": 108, "time": 55000},
        {"query": "SELECT p.person_id, c.condition_concept_id, d.drug_concept_id FROM person p JOIN condition_occurrence c ON p.person_id = c.person_id JOIN drug_exposure d ON p.person_id = d.person_id WHERE c.condition_concept_id = 44054006",
         "tables": ["person", "condition_occurrence", "drug_exposure"], "columns": ["person_id", "condition_concept_id", "drug_concept_id"],
         "joins": ["person-condition_occurrence", "person-drug_exposure"], "filters": ["condition_concept_id"], "count": 93, "time": 72000},
        {"query": "SELECT p.person_id, v.visit_start_date, m.value_as_number FROM person p JOIN visit_occurrence v ON p.person_id = v.person_id JOIN measurement m ON v.visit_occurrence_id = m.visit_occurrence_id WHERE m.measurement_concept_id = 3004249",
         "tables": ["person", "visit_occurrence", "measurement"], "columns": ["person_id", "visit_start_date", "value_as_number", "visit_occurrence_id", "measurement_concept_id"],
         "joins": ["person-visit_occurrence", "visit_occurrence-measurement"], "filters": ["measurement_concept_id"], "count": 87, "time": 110000},
        # cost 조인
        {"query": "SELECT v.visit_occurrence_id, co.total_charge FROM visit_occurrence v JOIN cost co ON v.visit_occurrence_id = co.cost_event_id WHERE co.total_charge > 1000",
         "tables": ["visit_occurrence", "cost"], "columns": ["visit_occurrence_id", "total_charge", "cost_event_id"],
         "joins": ["visit_occurrence-cost"], "filters": ["total_charge"], "count": 76, "time": 28000},
        # payer_plan_period
        {"query": "SELECT p.person_id, pp.payer_source_value FROM person p JOIN payer_plan_period pp ON p.person_id = pp.person_id",
         "tables": ["person", "payer_plan_period"], "columns": ["person_id", "payer_source_value"],
         "joins": ["person-payer_plan_period"], "filters": [], "count": 55, "time": 18000},
        # 추가 고빈도 쿼리들
        {"query": "SELECT person_id, year_of_birth, gender_source_value FROM person WHERE year_of_birth >= 1950",
         "tables": ["person"], "columns": ["person_id", "year_of_birth", "gender_source_value"],
         "joins": [], "filters": ["year_of_birth"], "count": 230, "time": 800},
        {"query": "SELECT visit_occurrence_id, person_id, visit_concept_id, visit_start_date FROM visit_occurrence WHERE visit_start_date BETWEEN '2023-01-01' AND '2023-12-31'",
         "tables": ["visit_occurrence"], "columns": ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date"],
         "joins": [], "filters": ["visit_start_date"], "count": 198, "time": 5600},
        {"query": "SELECT condition_concept_id, COUNT(DISTINCT person_id) FROM condition_occurrence GROUP BY condition_concept_id ORDER BY COUNT(DISTINCT person_id) DESC LIMIT 50",
         "tables": ["condition_occurrence"], "columns": ["condition_concept_id", "person_id"],
         "joins": [], "filters": [], "count": 145, "time": 22000},
        {"query": "SELECT drug_concept_id, COUNT(DISTINCT person_id) FROM drug_exposure GROUP BY drug_concept_id ORDER BY COUNT(DISTINCT person_id) DESC LIMIT 50",
         "tables": ["drug_exposure"], "columns": ["drug_concept_id", "person_id"],
         "joins": [], "filters": [], "count": 120, "time": 19000},
        {"query": "SELECT person_id, condition_concept_id, condition_start_date, condition_end_date FROM condition_occurrence WHERE condition_start_date >= '2023-01-01'",
         "tables": ["condition_occurrence"], "columns": ["person_id", "condition_concept_id", "condition_start_date", "condition_end_date"],
         "joins": [], "filters": ["condition_start_date"], "count": 132, "time": 16000},
        {"query": "SELECT p.person_id, p.gender_source_value, ce.condition_concept_id, ce.condition_era_start_date FROM person p JOIN condition_era ce ON p.person_id = ce.person_id",
         "tables": ["person", "condition_era"], "columns": ["person_id", "gender_source_value", "condition_concept_id", "condition_era_start_date"],
         "joins": ["person-condition_era"], "filters": [], "count": 71, "time": 12000},
        {"query": "SELECT v.visit_concept_id, COUNT(*), AVG(EXTRACT(EPOCH FROM (v.visit_end_date - v.visit_start_date))/86400) FROM visit_occurrence v WHERE v.visit_end_date IS NOT NULL GROUP BY v.visit_concept_id",
         "tables": ["visit_occurrence"], "columns": ["visit_concept_id", "visit_start_date", "visit_end_date"],
         "joins": [], "filters": ["visit_end_date"], "count": 92, "time": 7800},
        {"query": "SELECT m.measurement_concept_id, m.unit_source_value, COUNT(*) FROM measurement m GROUP BY m.measurement_concept_id, m.unit_source_value HAVING COUNT(*) > 1000",
         "tables": ["measurement"], "columns": ["measurement_concept_id", "unit_source_value"],
         "joins": [], "filters": [], "count": 64, "time": 135000},
        {"query": "SELECT pr.procedure_concept_id, v.visit_concept_id, COUNT(*) FROM procedure_occurrence pr JOIN visit_occurrence v ON pr.visit_occurrence_id = v.visit_occurrence_id GROUP BY pr.procedure_concept_id, v.visit_concept_id",
         "tables": ["procedure_occurrence", "visit_occurrence"], "columns": ["procedure_concept_id", "visit_concept_id", "visit_occurrence_id"],
         "joins": ["procedure_occurrence-visit_occurrence"], "filters": [], "count": 58, "time": 62000},
        # 코호트 쿼리
        {"query": "SELECT DISTINCT p.person_id FROM person p JOIN condition_occurrence c ON p.person_id = c.person_id JOIN drug_exposure d ON p.person_id = d.person_id WHERE c.condition_concept_id = 44054006 AND d.drug_concept_id IN (21600381, 21600382)",
         "tables": ["person", "condition_occurrence", "drug_exposure"], "columns": ["person_id", "condition_concept_id", "drug_concept_id"],
         "joins": ["person-condition_occurrence", "person-drug_exposure"], "filters": ["condition_concept_id", "drug_concept_id"], "count": 82, "time": 95000},
        {"query": "SELECT DISTINCT p.person_id FROM person p JOIN condition_occurrence c ON p.person_id = c.person_id WHERE c.condition_concept_id = 38341003 AND p.year_of_birth <= 1960",
         "tables": ["person", "condition_occurrence"], "columns": ["person_id", "condition_concept_id", "year_of_birth"],
         "joins": ["person-condition_occurrence"], "filters": ["condition_concept_id", "year_of_birth"], "count": 115, "time": 28000},
        # 비용 분석
        {"query": "SELECT p.person_id, SUM(co.total_charge) FROM person p JOIN visit_occurrence v ON p.person_id = v.person_id JOIN cost co ON v.visit_occurrence_id = co.cost_event_id GROUP BY p.person_id HAVING SUM(co.total_charge) > 10000",
         "tables": ["person", "visit_occurrence", "cost"], "columns": ["person_id", "total_charge", "visit_occurrence_id", "cost_event_id"],
         "joins": ["person-visit_occurrence", "visit_occurrence-cost"], "filters": ["total_charge"], "count": 68, "time": 48000},
        {"query": "SELECT v.visit_concept_id, AVG(co.total_charge), MAX(co.total_charge) FROM visit_occurrence v JOIN cost co ON v.visit_occurrence_id = co.cost_event_id GROUP BY v.visit_concept_id",
         "tables": ["visit_occurrence", "cost"], "columns": ["visit_concept_id", "total_charge", "visit_occurrence_id", "cost_event_id"],
         "joins": ["visit_occurrence-cost"], "filters": [], "count": 54, "time": 32000},
        # 시계열 분석
        {"query": "SELECT DATE_TRUNC('month', v.visit_start_date), COUNT(*) FROM visit_occurrence v GROUP BY 1 ORDER BY 1",
         "tables": ["visit_occurrence"], "columns": ["visit_start_date"],
         "joins": [], "filters": [], "count": 175, "time": 4200},
        {"query": "SELECT DATE_TRUNC('month', c.condition_start_date), c.condition_concept_id, COUNT(*) FROM condition_occurrence c WHERE c.condition_concept_id IN (44054006, 38341003) GROUP BY 1, 2 ORDER BY 1",
         "tables": ["condition_occurrence"], "columns": ["condition_start_date", "condition_concept_id"],
         "joins": [], "filters": ["condition_concept_id"], "count": 138, "time": 18000},
        {"query": "SELECT DATE_TRUNC('month', d.drug_exposure_start_date), COUNT(DISTINCT d.person_id) FROM drug_exposure d GROUP BY 1 ORDER BY 1",
         "tables": ["drug_exposure"], "columns": ["drug_exposure_start_date", "person_id"],
         "joins": [], "filters": [], "count": 99, "time": 24000},
        # 4-table 조인
        {"query": "SELECT p.person_id, v.visit_start_date, c.condition_concept_id, d.drug_concept_id FROM person p JOIN visit_occurrence v ON p.person_id = v.person_id JOIN condition_occurrence c ON v.visit_occurrence_id = c.visit_occurrence_id JOIN drug_exposure d ON v.visit_occurrence_id = d.visit_occurrence_id WHERE v.visit_concept_id = 9201",
         "tables": ["person", "visit_occurrence", "condition_occurrence", "drug_exposure"], "columns": ["person_id", "visit_start_date", "condition_concept_id", "drug_concept_id", "visit_occurrence_id", "visit_concept_id"],
         "joins": ["person-visit_occurrence", "visit_occurrence-condition_occurrence", "visit_occurrence-drug_exposure"], "filters": ["visit_concept_id"], "count": 65, "time": 150000},
        {"query": "SELECT p.person_id, v.visit_start_date, m.value_as_number, c.condition_concept_id FROM person p JOIN visit_occurrence v ON p.person_id = v.person_id JOIN measurement m ON v.visit_occurrence_id = m.visit_occurrence_id JOIN condition_occurrence c ON p.person_id = c.person_id WHERE c.condition_concept_id = 44054006",
         "tables": ["person", "visit_occurrence", "measurement", "condition_occurrence"], "columns": ["person_id", "visit_start_date", "value_as_number", "condition_concept_id", "visit_occurrence_id", "measurement_concept_id"],
         "joins": ["person-visit_occurrence", "visit_occurrence-measurement", "person-condition_occurrence"], "filters": ["condition_concept_id"], "count": 57, "time": 180000},
        {"query": "SELECT p.person_id, p.gender_source_value, p.year_of_birth FROM person p WHERE p.person_id IN (SELECT DISTINCT person_id FROM condition_occurrence WHERE condition_concept_id = 44054006)",
         "tables": ["person", "condition_occurrence"], "columns": ["person_id", "gender_source_value", "year_of_birth", "condition_concept_id"],
         "joins": ["person-condition_occurrence"], "filters": ["condition_concept_id"], "count": 102, "time": 35000},
    ]

    for q in seed_queries:
        await conn.execute("""
            INSERT INTO cdw_query_history (query_text, tables_accessed, columns_accessed, join_tables, filter_columns, execution_count, total_time_ms, first_seen, last_seen)
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW() - INTERVAL '30 days' * RANDOM(), NOW() - INTERVAL '1 day' * RANDOM())
        """, q["query"], q["tables"], q["columns"], q["joins"], q["filters"], q["count"], q["time"])


async def seed_executions(conn):
    """시뮬레이션 실행 이력 생성"""
    count = await conn.fetchval("SELECT COUNT(*) FROM pipeline_execution")
    if count > 0:
        return
    types = [
        ("lz-person", "landing_zone", "Person Landing Zone"),
        ("lz-visit", "landing_zone", "Visit Landing Zone"),
        ("lz-condition", "landing_zone", "Condition Landing Zone"),
        ("lz-drug", "landing_zone", "Drug Landing Zone"),
        ("lz-measurement", "landing_zone", "Measurement Landing Zone"),
        ("exp-cdm-atlas", "export", "CDM ATLAS Export"),
        ("exp-pms", "export", "PMS 정밀의료 Export"),
        ("exp-redcap", "export", "REDCap e-CRF Export"),
        ("exp-research", "export", "연구 코호트 Export"),
    ]
    statuses = ["completed", "completed", "completed", "completed", "failed", "running"]
    for i in range(30):
        t = types[i % len(types)]
        st = random.choice(statuses)
        started = datetime.now() - timedelta(hours=random.randint(1, 720))
        ended = started + timedelta(seconds=random.randint(10, 600)) if st != "running" else None
        rows = random.randint(1000, 500000) if st == "completed" else 0
        err = "Connection timeout" if st == "failed" else None
        await conn.execute("""
            INSERT INTO pipeline_execution (pipeline_id, pipeline_type, pipeline_name, status, started_at, ended_at, rows_processed, error_message)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        """, t[0], t[1], t[2], st, started, ended, rows, err)


# ── Pydantic Models ──

class LandingZoneTemplate(BaseModel):
    name: str
    source_system: str = "OMOP CDM"
    target_tables: List[str] = []
    cdc_config: Dict[str, Any] = {}
    landing_schema: Dict[str, Any] = {}
    schedule: str = "0 */6 * * *"
    validation_rules: List[Dict[str, Any]] = []
    enabled: bool = True

class ExportPipelineCreate(BaseModel):
    name: str
    target_system: str
    source_tables: List[str] = []
    format: str = "sql"
    deidentify: bool = False
    config: Dict[str, Any] = {}
    schedule: Optional[str] = None
    enabled: bool = True

class MartDesignCreate(BaseModel):
    name: str
    description: str = ""
    source_tables: List[str] = []
    columns: List[Dict[str, Any]] = []
    indexes: List[Dict[str, Any]] = []
    estimated_rows: int = 0
    build_sql: str = ""


# ── Built-in Templates ──

def builtin_lz_templates() -> List[Dict[str, Any]]:
    return [
        {"id": "lz-person", "name": "Person Landing Zone", "source_system": "OMOP CDM", "target_tables": ["person"],
         "cdc_config": {"capture_mode": "full_load", "watermark_column": "person_id", "batch_interval": "6h"},
         "landing_schema": {"columns": [
             {"source": "person_id", "target": "person_id", "type": "BIGINT", "transform": "none"},
             {"source": "gender_source_value", "target": "gender", "type": "VARCHAR(10)", "transform": "none"},
             {"source": "year_of_birth", "target": "birth_year", "type": "INT", "transform": "none"},
             {"source": "race_source_value", "target": "race", "type": "VARCHAR(50)", "transform": "none"},
         ]},
         "schedule": "0 */6 * * *", "validation_rules": [{"type": "not_null", "column": "person_id"}, {"type": "range", "column": "year_of_birth", "min": 1900, "max": 2030}], "enabled": True, "captured_count": 76074},
        {"id": "lz-visit", "name": "Visit Occurrence Landing Zone", "source_system": "OMOP CDM", "target_tables": ["visit_occurrence"],
         "cdc_config": {"capture_mode": "incremental", "watermark_column": "visit_start_date", "batch_interval": "1h"},
         "landing_schema": {"columns": [
             {"source": "visit_occurrence_id", "target": "visit_id", "type": "BIGINT", "transform": "none"},
             {"source": "person_id", "target": "patient_id", "type": "BIGINT", "transform": "none"},
             {"source": "visit_concept_id", "target": "visit_type", "type": "INT", "transform": "concept_map"},
             {"source": "visit_start_date", "target": "start_date", "type": "DATE", "transform": "none"},
             {"source": "visit_end_date", "target": "end_date", "type": "DATE", "transform": "none"},
         ]},
         "schedule": "0 * * * *", "validation_rules": [{"type": "not_null", "column": "visit_occurrence_id"}, {"type": "not_null", "column": "person_id"}, {"type": "date_range", "column": "visit_start_date", "min": "2000-01-01"}], "enabled": True, "captured_count": 4512345},
        {"id": "lz-condition", "name": "Condition Occurrence Landing Zone", "source_system": "OMOP CDM", "target_tables": ["condition_occurrence"],
         "cdc_config": {"capture_mode": "incremental", "watermark_column": "condition_start_date", "batch_interval": "1h"},
         "landing_schema": {"columns": [
             {"source": "condition_occurrence_id", "target": "condition_id", "type": "BIGINT", "transform": "none"},
             {"source": "person_id", "target": "patient_id", "type": "BIGINT", "transform": "none"},
             {"source": "condition_concept_id", "target": "condition_code", "type": "INT", "transform": "snomed_map"},
             {"source": "condition_start_date", "target": "onset_date", "type": "DATE", "transform": "none"},
         ]},
         "schedule": "0 * * * *", "validation_rules": [{"type": "not_null", "column": "condition_occurrence_id"}], "enabled": True, "captured_count": 2834567},
        {"id": "lz-drug", "name": "Drug Exposure Landing Zone", "source_system": "OMOP CDM", "target_tables": ["drug_exposure"],
         "cdc_config": {"capture_mode": "incremental", "watermark_column": "drug_exposure_start_date", "batch_interval": "2h"},
         "landing_schema": {"columns": [
             {"source": "drug_exposure_id", "target": "drug_id", "type": "BIGINT", "transform": "none"},
             {"source": "person_id", "target": "patient_id", "type": "BIGINT", "transform": "none"},
             {"source": "drug_concept_id", "target": "drug_code", "type": "INT", "transform": "rxnorm_map"},
             {"source": "drug_exposure_start_date", "target": "start_date", "type": "DATE", "transform": "none"},
             {"source": "quantity", "target": "quantity", "type": "NUMERIC", "transform": "none"},
         ]},
         "schedule": "0 */2 * * *", "validation_rules": [{"type": "not_null", "column": "drug_exposure_id"}, {"type": "positive", "column": "quantity"}], "enabled": True, "captured_count": 3912345},
        {"id": "lz-measurement", "name": "Measurement Landing Zone", "source_system": "OMOP CDM", "target_tables": ["measurement"],
         "cdc_config": {"capture_mode": "incremental", "watermark_column": "measurement_date", "batch_interval": "30m"},
         "landing_schema": {"columns": [
             {"source": "measurement_id", "target": "measurement_id", "type": "BIGINT", "transform": "none"},
             {"source": "person_id", "target": "patient_id", "type": "BIGINT", "transform": "none"},
             {"source": "measurement_concept_id", "target": "test_code", "type": "INT", "transform": "loinc_map"},
             {"source": "value_as_number", "target": "result_value", "type": "NUMERIC", "transform": "none"},
             {"source": "unit_source_value", "target": "result_unit", "type": "VARCHAR(50)", "transform": "ucum_map"},
             {"source": "measurement_date", "target": "test_date", "type": "DATE", "transform": "none"},
         ]},
         "schedule": "*/30 * * * *", "validation_rules": [{"type": "not_null", "column": "measurement_id"}, {"type": "range", "column": "value_as_number", "min": -1000, "max": 100000}], "enabled": True, "captured_count": 36623456},
        {"id": "lz-procedure", "name": "Procedure Occurrence Landing Zone", "source_system": "OMOP CDM", "target_tables": ["procedure_occurrence"],
         "cdc_config": {"capture_mode": "incremental", "watermark_column": "procedure_date", "batch_interval": "2h"},
         "landing_schema": {"columns": [
             {"source": "procedure_occurrence_id", "target": "procedure_id", "type": "BIGINT", "transform": "none"},
             {"source": "person_id", "target": "patient_id", "type": "BIGINT", "transform": "none"},
             {"source": "procedure_concept_id", "target": "procedure_code", "type": "INT", "transform": "snomed_map"},
             {"source": "procedure_date", "target": "procedure_date", "type": "DATE", "transform": "none"},
         ]},
         "schedule": "0 */2 * * *", "validation_rules": [{"type": "not_null", "column": "procedure_occurrence_id"}], "enabled": True, "captured_count": 12456789},
        {"id": "lz-observation", "name": "Observation Landing Zone", "source_system": "OMOP CDM", "target_tables": ["observation"],
         "cdc_config": {"capture_mode": "incremental", "watermark_column": "observation_date", "batch_interval": "2h"},
         "landing_schema": {"columns": [
             {"source": "observation_id", "target": "observation_id", "type": "BIGINT", "transform": "none"},
             {"source": "person_id", "target": "patient_id", "type": "BIGINT", "transform": "none"},
             {"source": "observation_concept_id", "target": "observation_code", "type": "INT", "transform": "none"},
             {"source": "value_as_string", "target": "value", "type": "TEXT", "transform": "none"},
             {"source": "observation_date", "target": "obs_date", "type": "DATE", "transform": "none"},
         ]},
         "schedule": "0 */2 * * *", "validation_rules": [{"type": "not_null", "column": "observation_id"}], "enabled": True, "captured_count": 21345678},
        {"id": "lz-payer", "name": "Payer Plan Period Landing Zone", "source_system": "OMOP CDM", "target_tables": ["payer_plan_period"],
         "cdc_config": {"capture_mode": "full_load", "watermark_column": "payer_plan_period_id", "batch_interval": "24h"},
         "landing_schema": {"columns": [
             {"source": "payer_plan_period_id", "target": "payer_id", "type": "BIGINT", "transform": "none"},
             {"source": "person_id", "target": "patient_id", "type": "BIGINT", "transform": "none"},
             {"source": "payer_source_value", "target": "payer_name", "type": "VARCHAR(100)", "transform": "none"},
             {"source": "payer_plan_period_start_date", "target": "start_date", "type": "DATE", "transform": "none"},
             {"source": "payer_plan_period_end_date", "target": "end_date", "type": "DATE", "transform": "none"},
         ]},
         "schedule": "0 0 * * *", "validation_rules": [{"type": "not_null", "column": "payer_plan_period_id"}], "enabled": True, "captured_count": 2812345},
    ]


def builtin_export_pipelines() -> List[Dict[str, Any]]:
    return [
        {"id": "exp-cdm-atlas", "name": "CDM ATLAS Export", "target_system": "CDM ATLAS",
         "source_tables": ["person", "visit_occurrence", "condition_occurrence", "drug_exposure", "measurement", "procedure_occurrence", "observation"],
         "format": "sql", "deidentify": False, "schedule": "0 2 * * *", "enabled": True,
         "config": {"export_type": "full_cdm", "target_schema": "cdm_export", "include_era_tables": True, "atlas_version": "2.13"},
         "description": "OHDSI ATLAS 분석용 CDM 전체 테이블 SQL Export"},
        {"id": "exp-pms", "name": "PMS 정밀의료 Export", "target_system": "PMS",
         "source_tables": ["person", "condition_occurrence", "drug_exposure", "measurement"],
         "format": "csv", "deidentify": True, "schedule": "0 3 * * 1", "enabled": True,
         "config": {"delimiter": ",", "encoding": "utf-8", "deidentify_fields": ["person_id", "year_of_birth"], "hash_algorithm": "sha256", "precision_medicine_fields": ["condition_concept_id", "drug_concept_id", "measurement_concept_id"]},
         "description": "정밀의료 시스템용 비식별화 CSV Export (매주 월요일)"},
        {"id": "exp-redcap", "name": "REDCap e-CRF Export", "target_system": "REDCap",
         "source_tables": ["person", "visit_occurrence", "condition_occurrence", "measurement"],
         "format": "api", "deidentify": True, "schedule": "manual", "enabled": True,
         "config": {"api_url": "https://redcap.amc.seoul.kr/api/", "project_id": 142, "event_name": "baseline_arm_1", "deidentify_fields": ["person_id"], "field_mapping": {"person_id": "record_id", "condition_concept_id": "diagnosis", "measurement_concept_id": "lab_test"}},
         "description": "REDCap 전자 증례기록서 API 연동 (수동 실행)"},
        {"id": "exp-research", "name": "연구 코호트 Export", "target_system": "Research",
         "source_tables": ["person", "condition_occurrence", "drug_exposure", "measurement", "visit_occurrence"],
         "format": "parquet", "deidentify": True, "schedule": "manual", "enabled": True,
         "config": {"compression": "snappy", "partition_by": ["condition_concept_id"], "deidentify_fields": ["person_id", "year_of_birth"], "cohort_criteria": "condition_concept_id IN (44054006, 38341003)"},
         "description": "연구 목적 코호트 데이터 Parquet Export (비식별화)"},
        {"id": "exp-achilles", "name": "CDM Achilles Export", "target_system": "CDM Achilles",
         "source_tables": ["person", "visit_occurrence", "condition_occurrence", "drug_exposure", "measurement", "procedure_occurrence", "observation", "condition_era"],
         "format": "sql", "deidentify": False, "schedule": "0 1 * * 0", "enabled": True,
         "config": {"achilles_version": "1.7.2", "generate_heel": True, "output_schema": "achilles_results", "parallel_threads": 4},
         "description": "OHDSI Achilles 데이터 품질 분석용 SQL Export (매주 일요일)"},
    ]


# ── File I/O Helpers ──

def load_custom_templates() -> List[Dict[str, Any]]:
    if LZ_TEMPLATES_FILE.exists():
        return json.loads(LZ_TEMPLATES_FILE.read_text())
    return []

def save_custom_templates(templates: List[Dict[str, Any]]):
    LZ_TEMPLATES_FILE.write_text(json.dumps(templates, ensure_ascii=False, indent=2, default=str))

def load_custom_exports() -> List[Dict[str, Any]]:
    if EXPORT_PIPELINES_FILE.exists():
        return json.loads(EXPORT_PIPELINES_FILE.read_text())
    return []

def save_custom_exports(exports: List[Dict[str, Any]]):
    EXPORT_PIPELINES_FILE.write_text(json.dumps(exports, ensure_ascii=False, indent=2, default=str))
