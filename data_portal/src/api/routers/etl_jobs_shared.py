"""
ETL Jobs shared module: DB config, connection helper, Pydantic models, schema setup, seed data.
Used by etl_jobs_core, etl_jobs_deps, etl_jobs_alerts.
"""
import os
import json
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


_pool: asyncpg.Pool | None = None
_tables_ensured = False
_seed_ensured = False


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None or _pool._closed:
        try:
            _pool = await asyncpg.create_pool(**OMOP_DB_CONFIG, min_size=2, max_size=8)
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")
    return _pool


async def get_connection():
    """커넥션 풀에서 연결 획득 (backward compat — conn.close()가 풀에 반환)"""
    pool = await _get_pool()
    try:
        return await pool.acquire()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")


async def release_connection(conn):
    """커넥션을 풀에 반환"""
    pool = await _get_pool()
    await pool.release(conn)


# ── Pydantic Models ──

class JobGroupCreate(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = Field(None, max_length=500)
    priority: int = Field(default=0)
    enabled: bool = True


class JobCreate(BaseModel):
    group_id: int
    name: str = Field(..., max_length=200)
    job_type: str = Field(..., pattern=r"^(batch|stream|cdc)$")
    source_id: Optional[str] = Field(None, max_length=100)
    target_table: Optional[str] = Field(None, max_length=100)
    schedule: Optional[str] = Field(None, max_length=100)
    dag_id: Optional[str] = Field(None, max_length=200)
    config: Optional[Dict[str, Any]] = None
    enabled: bool = True
    sort_order: int = Field(default=0)


class JobUpdate(BaseModel):
    group_id: Optional[int] = None
    name: Optional[str] = Field(None, max_length=200)
    job_type: Optional[str] = Field(None, pattern=r"^(batch|stream|cdc)$")
    source_id: Optional[str] = Field(None, max_length=100)
    target_table: Optional[str] = Field(None, max_length=100)
    schedule: Optional[str] = Field(None, max_length=100)
    dag_id: Optional[str] = Field(None, max_length=200)
    config: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None
    sort_order: Optional[int] = None


class DependencyCreate(BaseModel):
    source_table: str = Field(..., max_length=100)
    target_table: str = Field(..., max_length=100)
    relationship: Optional[str] = Field(None, max_length=200)
    dep_type: str = Field(default="manual", pattern=r"^(fk|manual|derived)$")


class AlertRuleCreate(BaseModel):
    name: str = Field(..., max_length=200)
    condition_type: str = Field(..., pattern=r"^(error_count|duration_exceeded|row_anomaly|job_failure)$")
    threshold: Optional[Dict[str, Any]] = None
    job_id: Optional[int] = None
    channels: List[str] = []
    webhook_url: Optional[str] = Field(None, max_length=500)
    enabled: bool = True


class ReorderBody(BaseModel):
    sort_order: int


# ── DB Schema ──

async def _ensure_tables(conn):
    global _tables_ensured
    if _tables_ensured:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_job_group (
            group_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            description VARCHAR(500),
            priority INT DEFAULT 0,
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_job (
            job_id SERIAL PRIMARY KEY,
            group_id INT REFERENCES etl_job_group(group_id) ON DELETE CASCADE,
            name VARCHAR(200) NOT NULL,
            job_type VARCHAR(20) NOT NULL CHECK (job_type IN ('batch', 'stream', 'cdc')),
            source_id VARCHAR(100),
            target_table VARCHAR(100),
            schedule VARCHAR(100),
            dag_id VARCHAR(200),
            config JSONB DEFAULT '{}',
            enabled BOOLEAN DEFAULT TRUE,
            sort_order INT DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_table_dependency (
            dep_id SERIAL PRIMARY KEY,
            source_table VARCHAR(100) NOT NULL,
            target_table VARCHAR(100) NOT NULL,
            relationship VARCHAR(200),
            dep_type VARCHAR(20) NOT NULL DEFAULT 'manual' CHECK (dep_type IN ('fk', 'manual', 'derived')),
            UNIQUE(source_table, target_table)
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_execution_log (
            log_id BIGSERIAL PRIMARY KEY,
            job_id INT REFERENCES etl_job(job_id) ON DELETE SET NULL,
            run_id VARCHAR(200),
            status VARCHAR(20) NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'success', 'failed', 'warning')),
            started_at TIMESTAMP DEFAULT NOW(),
            ended_at TIMESTAMP,
            rows_processed BIGINT DEFAULT 0,
            rows_failed BIGINT DEFAULT 0,
            error_message TEXT,
            log_entries JSONB DEFAULT '[]'
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_alert_rule (
            rule_id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            condition_type VARCHAR(30) NOT NULL CHECK (condition_type IN ('error_count', 'duration_exceeded', 'row_anomaly', 'job_failure')),
            threshold JSONB DEFAULT '{}',
            job_id INT REFERENCES etl_job(job_id) ON DELETE SET NULL,
            channels TEXT[] DEFAULT '{}',
            webhook_url VARCHAR(500),
            enabled BOOLEAN DEFAULT TRUE
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS etl_alert_history (
            alert_id BIGSERIAL PRIMARY KEY,
            rule_id INT REFERENCES etl_alert_rule(rule_id) ON DELETE SET NULL,
            job_id INT,
            severity VARCHAR(20) DEFAULT 'info',
            title VARCHAR(300),
            message TEXT,
            acknowledged BOOLEAN DEFAULT FALSE,
            acknowledged_by VARCHAR(100),
            acknowledged_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    _tables_ensured = True


async def _ensure_seed_data(conn):
    """첫 호출 시 시드 데이터 삽입"""
    global _seed_ensured
    if _seed_ensured:
        return
    count = await conn.fetchval("SELECT COUNT(*) FROM etl_job_group")
    _seed_ensured = True
    if count > 0:
        return

    # Groups
    g1 = await conn.fetchval("""
        INSERT INTO etl_job_group (name, description, priority, enabled)
        VALUES ('OMOP CDM 적재', 'OMOP CDM 표준 테이블 적재 파이프라인', 1, true) RETURNING group_id
    """)
    g2 = await conn.fetchval("""
        INSERT INTO etl_job_group (name, description, priority, enabled)
        VALUES ('이기종 소스 수집', '외부 시스템 데이터 수집 파이프라인', 2, true) RETURNING group_id
    """)
    g3 = await conn.fetchval("""
        INSERT INTO etl_job_group (name, description, priority, enabled)
        VALUES ('데이터 품질 검증', '적재 후 품질 검증 파이프라인', 3, true) RETURNING group_id
    """)

    # Jobs
    jobs = [
        (g1, 'measurement CDC 적재', 'cdc', 'omop-cdc', 'measurement', '실시간', 'omop_cdc_measurement', json.dumps({"buffer_size": 1000, "flush_interval_sec": 5}), 1),
        (g1, 'condition_occurrence CDC 적재', 'cdc', 'omop-cdc', 'condition_occurrence', '실시간', 'omop_cdc_condition', json.dumps({"buffer_size": 500}), 2),
        (g2, 'Synthea CSV 배치 적재', 'batch', 'synthea-csv', 'person', '0 2 * * *', 'synthea_csv_batch', json.dumps({"file_pattern": "*.csv", "delimiter": ","}), 1),
        (g2, 'EHR Oracle 배치 동기화', 'batch', 'ehr-oracle', 'visit_occurrence', '0 3 * * *', 'ehr_oracle_sync', json.dumps({"schema": "EMR", "incremental": True}), 2),
        (g2, 'Lab HL7 스트림 수신', 'stream', 'hl7-fhir', 'measurement', '상시', 'lab_hl7_stream', json.dumps({"port": 2575, "protocol": "MLLP"}), 3),
    ]
    job_ids = []
    for group_id, name, jtype, src, tgt, sched, dag, cfg, order in jobs:
        jid = await conn.fetchval("""
            INSERT INTO etl_job (group_id, name, job_type, source_id, target_table, schedule, dag_id, config, sort_order)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9) RETURNING job_id
        """, group_id, name, jtype, src, tgt, sched, dag, cfg, order)
        job_ids.append(jid)

    # Execution logs (sample)
    for i, jid in enumerate(job_ids):
        for status in ['success', 'success', 'failed' if i == 2 else 'success']:
            await conn.execute("""
                INSERT INTO etl_execution_log (job_id, run_id, status, started_at, ended_at, rows_processed, rows_failed, error_message)
                VALUES ($1, $2, $3, NOW() - INTERVAL '1 hour' * $4, NOW() - INTERVAL '1 hour' * $4 + INTERVAL '5 minutes', $5, $6, $7)
            """, jid, f"run-{jid}-{status}-{i}", status, (i + 1) * 2,
                 10000 + i * 5000 if status == 'success' else 3000,
                 0 if status == 'success' else 150,
                 None if status == 'success' else 'Connection timeout to source database')

    # Alert rules
    await conn.execute("""
        INSERT INTO etl_alert_rule (name, condition_type, threshold, channels, webhook_url, enabled)
        VALUES
            ('Job 실패 알림', 'job_failure', '{"max_failures": 1}'::jsonb, ARRAY['email', 'webhook'], NULL, true),
            ('실행 시간 초과', 'duration_exceeded', '{"max_minutes": 30}'::jsonb, ARRAY['email'], NULL, true),
            ('처리 건수 이상', 'row_anomaly', '{"deviation_pct": 50}'::jsonb, ARRAY['webhook'], NULL, true)
    """)

    # Alert history (sample)
    await conn.execute("""
        INSERT INTO etl_alert_history (rule_id, job_id, severity, title, message, acknowledged)
        VALUES
            (1, $1, 'critical', 'Synthea CSV 배치 실패', 'Job synthea_csv_batch 실패: Connection timeout to source database', false),
            (2, $2, 'warning', 'measurement CDC 실행시간 초과', 'Job measurement CDC가 30분을 초과하여 실행 중입니다', false),
            (1, $3, 'info', 'EHR Oracle 동기화 완료', 'Job ehr_oracle_sync 정상 완료 (15,000 rows)', true)
    """, job_ids[2], job_ids[0], job_ids[3])

    # Table dependencies (OMOP CDM core FK relationships)
    deps = [
        ("person", "visit_occurrence", "person_id → person_id", "fk"),
        ("person", "condition_occurrence", "person_id → person_id", "fk"),
        ("person", "drug_exposure", "person_id → person_id", "fk"),
        ("person", "measurement", "person_id → person_id", "fk"),
        ("person", "observation", "person_id → person_id", "fk"),
        ("person", "procedure_occurrence", "person_id → person_id", "fk"),
        ("person", "device_exposure", "person_id → person_id", "fk"),
        ("person", "death", "person_id → person_id", "fk"),
        ("visit_occurrence", "condition_occurrence", "visit_occurrence_id", "fk"),
        ("visit_occurrence", "drug_exposure", "visit_occurrence_id", "fk"),
        ("visit_occurrence", "measurement", "visit_occurrence_id", "fk"),
        ("visit_occurrence", "observation", "visit_occurrence_id", "fk"),
        ("visit_occurrence", "procedure_occurrence", "visit_occurrence_id", "fk"),
        ("condition_occurrence", "condition_era", "person_id 기반 집계", "derived"),
        ("drug_exposure", "drug_era", "person_id 기반 집계", "derived"),
        ("location", "person", "location_id → location_id", "fk"),
        ("care_site", "visit_occurrence", "care_site_id → care_site_id", "fk"),
        ("provider", "visit_occurrence", "provider_id → provider_id", "fk"),
        ("visit_occurrence", "cost", "visit_occurrence_id → cost_event_id", "fk"),
    ]
    for src, tgt, rel, dtype in deps:
        await conn.execute("""
            INSERT INTO etl_table_dependency (source_table, target_table, relationship, dep_type)
            VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING
        """, src, tgt, rel, dtype)
