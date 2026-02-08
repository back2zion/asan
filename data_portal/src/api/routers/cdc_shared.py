"""
CDC 공유 모듈 - DB config, connection, schema, seed data, Pydantic models
"""
import os
import json
from typing import Optional, Dict, Any

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

class ConnectorCreate(BaseModel):
    name: str = Field(..., max_length=100)
    db_type: str = Field(..., pattern=r"^(postgresql|mysql|oracle|sqlserver|mongodb)$")
    host: str = Field(..., max_length=200)
    port: int = Field(default=5432)
    database_name: str = Field(..., max_length=100)
    username: str = Field(..., max_length=100)
    password: str = Field(default="", max_length=200)
    schema_name: Optional[str] = Field(None, max_length=100)
    extra_config: Optional[Dict[str, Any]] = None
    description: Optional[str] = Field(None, max_length=500)


class ConnectorUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    host: Optional[str] = Field(None, max_length=200)
    port: Optional[int] = None
    database_name: Optional[str] = Field(None, max_length=100)
    username: Optional[str] = Field(None, max_length=100)
    password: Optional[str] = Field(None, max_length=200)
    schema_name: Optional[str] = Field(None, max_length=100)
    extra_config: Optional[Dict[str, Any]] = None
    description: Optional[str] = Field(None, max_length=500)


class TopicCreate(BaseModel):
    connector_id: int
    table_name: str = Field(..., max_length=200)
    topic_name: Optional[str] = Field(None, max_length=200)
    capture_mode: str = Field(default="log_based", pattern=r"^(log_based|query_based|trigger_based)$")
    target_table: Optional[str] = Field(None, max_length=200)
    enabled: bool = True


class IcebergSinkCreate(BaseModel):
    connector_id: int
    catalog_name: str = Field(default="asan_catalog", max_length=100)
    warehouse_path: str = Field(default="s3://asan-datalake/iceberg/", max_length=500)
    namespace: str = Field(default="omop_cdm", max_length=100)
    file_format: str = Field(default="parquet", pattern=r"^(parquet|orc|avro)$")
    partition_spec: Optional[Dict[str, Any]] = None
    write_mode: str = Field(default="append", pattern=r"^(append|upsert|overwrite)$")


class ServiceAction(BaseModel):
    action: str = Field(..., pattern=r"^(start|stop|restart|pause|resume)$")


# ── DB Schema ──

async def ensure_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cdc_connector (
            connector_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            db_type VARCHAR(20) NOT NULL CHECK (db_type IN ('postgresql','mysql','oracle','sqlserver','mongodb')),
            host VARCHAR(200) NOT NULL,
            port INT NOT NULL,
            database_name VARCHAR(100) NOT NULL,
            username VARCHAR(100) NOT NULL,
            password VARCHAR(200) DEFAULT '',
            schema_name VARCHAR(100),
            extra_config JSONB DEFAULT '{}',
            description VARCHAR(500),
            status VARCHAR(20) DEFAULT 'stopped' CHECK (status IN ('running','stopped','error','paused')),
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cdc_topic (
            topic_id SERIAL PRIMARY KEY,
            connector_id INT REFERENCES cdc_connector(connector_id) ON DELETE CASCADE,
            table_name VARCHAR(200) NOT NULL,
            topic_name VARCHAR(200),
            capture_mode VARCHAR(20) DEFAULT 'log_based' CHECK (capture_mode IN ('log_based','query_based','trigger_based')),
            target_table VARCHAR(200),
            enabled BOOLEAN DEFAULT TRUE,
            status VARCHAR(20) DEFAULT 'inactive' CHECK (status IN ('active','inactive','error','lag')),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cdc_offset_tracker (
            offset_id SERIAL PRIMARY KEY,
            connector_id INT REFERENCES cdc_connector(connector_id) ON DELETE CASCADE,
            topic_name VARCHAR(200) NOT NULL,
            lsn VARCHAR(100),
            binlog_file VARCHAR(200),
            binlog_pos BIGINT,
            scn VARCHAR(100),
            resume_token JSONB,
            last_event_time TIMESTAMP,
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(connector_id, topic_name)
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cdc_event_log (
            event_id BIGSERIAL PRIMARY KEY,
            connector_id INT,
            topic_name VARCHAR(200),
            event_type VARCHAR(20) CHECK (event_type IN ('INSERT','UPDATE','DELETE','DDL','SNAPSHOT','ERROR')),
            table_name VARCHAR(200),
            row_count INT DEFAULT 1,
            latency_ms INT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cdc_iceberg_sink (
            sink_id SERIAL PRIMARY KEY,
            connector_id INT REFERENCES cdc_connector(connector_id) ON DELETE CASCADE,
            catalog_name VARCHAR(100) DEFAULT 'asan_catalog',
            warehouse_path VARCHAR(500) DEFAULT 's3://asan-datalake/iceberg/',
            namespace VARCHAR(100) DEFAULT 'omop_cdm',
            file_format VARCHAR(20) DEFAULT 'parquet' CHECK (file_format IN ('parquet','orc','avro')),
            partition_spec JSONB DEFAULT '{}',
            write_mode VARCHAR(20) DEFAULT 'append' CHECK (write_mode IN ('append','upsert','overwrite')),
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)


async def ensure_seed_data(conn):
    count = await conn.fetchval("SELECT COUNT(*) FROM cdc_connector")
    if count > 0:
        return

    # Seed connectors
    c1 = await conn.fetchval("""
        INSERT INTO cdc_connector (name, db_type, host, port, database_name, username, password, schema_name, description, status)
        VALUES ('OMOP CDM (PostgreSQL)', 'postgresql', 'localhost', 5436, 'omop_cdm', 'omopuser', '***', 'public',
                'OMOP CDM PostgreSQL - Synthea 합성 데이터 (92M rows)', 'running')
        RETURNING connector_id
    """)
    c2 = await conn.fetchval("""
        INSERT INTO cdc_connector (name, db_type, host, port, database_name, username, password, schema_name, description, status)
        VALUES ('EMR Oracle DB', 'oracle', '10.10.1.50', 1521, 'EMRDB', 'emr_cdc', '***', 'EMR',
                '아산병원 EMR Oracle 운영 DB', 'stopped')
        RETURNING connector_id
    """)
    c3 = await conn.fetchval("""
        INSERT INTO cdc_connector (name, db_type, host, port, database_name, username, password, schema_name, description, status)
        VALUES ('Lab MySQL', 'mysql', '10.10.2.30', 3306, 'lab_results', 'lab_cdc', '***', NULL,
                '검사실 결과 MySQL DB', 'stopped')
        RETURNING connector_id
    """)
    c4 = await conn.fetchval("""
        INSERT INTO cdc_connector (name, db_type, host, port, database_name, username, password, schema_name, description, status)
        VALUES ('수술기록 SQL Server', 'sqlserver', '10.10.3.20', 1433, 'SurgeryDB', 'surg_cdc', '***', 'dbo',
                '수술기록 SQL Server DB', 'stopped')
        RETURNING connector_id
    """)
    c5 = await conn.fetchval("""
        INSERT INTO cdc_connector (name, db_type, host, port, database_name, username, password, schema_name, description, status)
        VALUES ('PACS MongoDB', 'mongodb', '10.10.4.10', 27017, 'pacs', 'pacs_cdc', '***', NULL,
                'PACS 영상메타데이터 MongoDB', 'stopped')
        RETURNING connector_id
    """)

    # Topics for OMOP CDM connector
    omop_tables = [
        ('person', 'omop.person.cdc', 'log_based', 'person'),
        ('visit_occurrence', 'omop.visit.cdc', 'log_based', 'visit_occurrence'),
        ('condition_occurrence', 'omop.condition.cdc', 'log_based', 'condition_occurrence'),
        ('drug_exposure', 'omop.drug.cdc', 'log_based', 'drug_exposure'),
        ('measurement', 'omop.measurement.cdc', 'log_based', 'measurement'),
        ('procedure_occurrence', 'omop.procedure.cdc', 'log_based', 'procedure_occurrence'),
        ('observation', 'omop.observation.cdc', 'log_based', 'observation'),
    ]
    for tbl, topic, mode, target in omop_tables:
        await conn.execute("""
            INSERT INTO cdc_topic (connector_id, table_name, topic_name, capture_mode, target_table, enabled, status)
            VALUES ($1, $2, $3, $4, $5, true, 'active')
        """, c1, tbl, topic, mode, target)

    # Topics for other connectors
    await conn.execute("""
        INSERT INTO cdc_topic (connector_id, table_name, topic_name, capture_mode, target_table, enabled, status)
        VALUES ($1, 'PATIENT', 'emr.patient.cdc', 'log_based', 'person', true, 'inactive')
    """, c2)
    await conn.execute("""
        INSERT INTO cdc_topic (connector_id, table_name, topic_name, capture_mode, target_table, enabled, status)
        VALUES ($1, 'lab_results', 'lab.results.cdc', 'query_based', 'measurement', true, 'inactive')
    """, c3)

    # Offsets for active connector
    for idx, (tbl, topic, mode, target) in enumerate(omop_tables):
        await conn.execute("""
            INSERT INTO cdc_offset_tracker (connector_id, topic_name, lsn, last_event_time)
            VALUES ($1, $2, $3, NOW() - INTERVAL '5 minutes')
        """, c1, topic, f"0/{1000000 + idx * 100000:X}")

    # Event logs (sample)
    import random
    for _ in range(30):
        tbl_idx = random.randint(0, len(omop_tables) - 1)
        evt_type = random.choice(['INSERT', 'UPDATE', 'INSERT', 'INSERT', 'DELETE'])
        await conn.execute("""
            INSERT INTO cdc_event_log (connector_id, topic_name, event_type, table_name, row_count, latency_ms, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW() - INTERVAL '1 minute' * $7)
        """, c1, omop_tables[tbl_idx][1], evt_type, omop_tables[tbl_idx][0],
             random.randint(1, 100), random.randint(5, 500), random.randint(0, 120))

    # Iceberg sink
    await conn.execute("""
        INSERT INTO cdc_iceberg_sink (connector_id, catalog_name, warehouse_path, namespace, file_format, partition_spec, write_mode)
        VALUES ($1, 'asan_catalog', 's3://asan-datalake/iceberg/', 'omop_cdm', 'parquet',
                '{"person": {"column": "year_of_birth", "transform": "identity"}, "measurement": {"column": "measurement_date", "transform": "month"}}'::jsonb,
                'upsert')
    """, c1)

    # Error event
    await conn.execute("""
        INSERT INTO cdc_event_log (connector_id, topic_name, event_type, table_name, error_message, created_at)
        VALUES ($1, 'omop.measurement.cdc', 'ERROR', 'measurement', 'WAL segment 0000000100000003 not found - archive_command 확인 필요', NOW() - INTERVAL '30 minutes')
    """, c1)
