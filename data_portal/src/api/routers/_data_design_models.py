"""
DIT-001: 데이터 설계 — Pydantic 모델 + DDL 스키마
Extracted from data_design.py to keep module size manageable.
"""
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, Field


# ── Pydantic Models ──

class ZoneCreate(BaseModel):
    zone_name: str = Field(..., max_length=50)
    zone_type: str = Field(..., pattern=r"^(source|bronze|silver|gold|mart)$")
    storage_type: str = Field(default="object_storage", pattern=r"^(object_storage|rdbms|hybrid)$")
    storage_path: Optional[str] = Field(None, max_length=500)
    file_format: str = Field(default="parquet", pattern=r"^(parquet|orc|avro|delta|iceberg|csv|json)$")
    description: Optional[str] = Field(None, max_length=500)
    retention_days: int = Field(default=365)
    partition_strategy: Optional[str] = Field(None, max_length=200)


class EntityCreate(BaseModel):
    entity_name: str = Field(..., max_length=100)
    logical_name: str = Field(..., max_length=200)
    zone_id: Optional[int] = None
    domain: Optional[str] = Field(None, max_length=50)
    entity_type: str = Field(default="table", pattern=r"^(table|view|materialized_view|external)$")
    description: Optional[str] = Field(None, max_length=500)
    columns_json: Optional[List[Dict[str, Any]]] = None


class RelationCreate(BaseModel):
    source_entity: str = Field(..., max_length=100)
    target_entity: str = Field(..., max_length=100)
    relation_type: str = Field(default="1:N", pattern=r"^(1:1|1:N|N:M|inheritance)$")
    fk_columns: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=300)


class NamingRuleCreate(BaseModel):
    rule_name: str = Field(..., max_length=100)
    target: str = Field(..., pattern=r"^(table|column|schema|index|constraint)$")
    pattern: str = Field(..., max_length=200)
    example: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=500)


class UnstructuredMappingCreate(BaseModel):
    source_type: str = Field(..., max_length=50)
    source_description: str = Field(..., max_length=200)
    target_table: str = Field(..., max_length=100)
    extraction_method: str = Field(..., max_length=100)
    nlp_model: Optional[str] = Field(None, max_length=200)
    output_columns: Optional[List[str]] = None
    description: Optional[str] = Field(None, max_length=500)


class NamingRuleUpdate(BaseModel):
    rule_name: Optional[str] = Field(None, max_length=100)
    target: Optional[str] = Field(None, pattern=r"^(table|column|schema|index|constraint)$")
    pattern: Optional[str] = Field(None, max_length=200)
    example: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=500)


class NamingCheckRequest(BaseModel):
    names: List[str]
    target: str = Field(default="table", pattern=r"^(table|column|schema)$")


# ── DB Schema (DDL) ──

async def _ensure_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS data_zone (
            zone_id SERIAL PRIMARY KEY,
            zone_name VARCHAR(50) NOT NULL UNIQUE,
            zone_type VARCHAR(20) NOT NULL CHECK (zone_type IN ('source','bronze','silver','gold','mart')),
            storage_type VARCHAR(20) DEFAULT 'object_storage' CHECK (storage_type IN ('object_storage','rdbms','hybrid')),
            storage_path VARCHAR(500),
            file_format VARCHAR(20) DEFAULT 'parquet',
            description VARCHAR(500),
            retention_days INT DEFAULT 365,
            partition_strategy VARCHAR(200),
            table_count INT DEFAULT 0,
            total_size_gb NUMERIC(10,2) DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS data_model_entity (
            entity_id SERIAL PRIMARY KEY,
            entity_name VARCHAR(100) NOT NULL,
            logical_name VARCHAR(200) NOT NULL,
            zone_id INT REFERENCES data_zone(zone_id) ON DELETE SET NULL,
            domain VARCHAR(50),
            entity_type VARCHAR(20) DEFAULT 'table' CHECK (entity_type IN ('table','view','materialized_view','external')),
            description VARCHAR(500),
            columns_json JSONB DEFAULT '[]',
            row_count BIGINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS data_model_relation (
            relation_id SERIAL PRIMARY KEY,
            source_entity VARCHAR(100) NOT NULL,
            target_entity VARCHAR(100) NOT NULL,
            relation_type VARCHAR(20) DEFAULT '1:N' CHECK (relation_type IN ('1:1','1:N','N:M','inheritance')),
            fk_columns VARCHAR(200),
            description VARCHAR(300),
            UNIQUE(source_entity, target_entity)
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS naming_standard (
            rule_id SERIAL PRIMARY KEY,
            rule_name VARCHAR(100) NOT NULL,
            target VARCHAR(20) NOT NULL CHECK (target IN ('table','column','schema','index','constraint')),
            pattern VARCHAR(200) NOT NULL,
            example VARCHAR(200),
            description VARCHAR(500),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS unstructured_mapping (
            mapping_id SERIAL PRIMARY KEY,
            source_type VARCHAR(50) NOT NULL,
            source_description VARCHAR(200) NOT NULL,
            target_table VARCHAR(100) NOT NULL,
            extraction_method VARCHAR(100) NOT NULL,
            nlp_model VARCHAR(200),
            output_columns TEXT[],
            description VARCHAR(500),
            status VARCHAR(20) DEFAULT 'planned' CHECK (status IN ('planned','active','testing','deprecated')),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
