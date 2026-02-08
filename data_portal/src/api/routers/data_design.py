"""
DIT-001: 통합적인 데이터 분석, 구성 체계 수립
- 데이터 영역(Zone) 관리 (원천/Bronze/Silver/Gold)
- 논리/물리 ERD 엔티티/관계 관리
- 용어 표준/명명 규칙 검증
- 비정형 데이터 구조화 매핑
- 파티셔닝/파일 포맷 설계
"""
import os
import json
import re
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import asyncpg
from ._data_design_seed import _ensure_seed_data

router = APIRouter(prefix="/data-design", tags=["DataDesign"])

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


# ── DB Schema ──

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


# ═══════════════════════════════════════════════════
#  Zones
# ═══════════════════════════════════════════════════

@router.get("/zones")
async def list_zones():
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        rows = await conn.fetch("""
            SELECT z.*,
                   COUNT(e.entity_id) AS entity_count,
                   COALESCE(SUM(e.row_count), 0) AS total_rows
            FROM data_zone z
            LEFT JOIN data_model_entity e ON e.zone_id = z.zone_id
            GROUP BY z.zone_id
            ORDER BY CASE z.zone_type WHEN 'source' THEN 1 WHEN 'bronze' THEN 2 WHEN 'silver' THEN 3 WHEN 'gold' THEN 4 WHEN 'mart' THEN 5 END
        """)
        return {
            "zones": [
                {
                    "zone_id": r["zone_id"],
                    "zone_name": r["zone_name"],
                    "zone_type": r["zone_type"],
                    "storage_type": r["storage_type"],
                    "storage_path": r["storage_path"],
                    "file_format": r["file_format"],
                    "description": r["description"],
                    "retention_days": r["retention_days"],
                    "partition_strategy": r["partition_strategy"],
                    "entity_count": r["entity_count"],
                    "total_rows": r["total_rows"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.post("/zones")
async def create_zone(body: ZoneCreate):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        zid = await conn.fetchval("""
            INSERT INTO data_zone (zone_name, zone_type, storage_type, storage_path, file_format, description, retention_days, partition_strategy)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING zone_id
        """, body.zone_name, body.zone_type, body.storage_type, body.storage_path,
             body.file_format, body.description, body.retention_days, body.partition_strategy)
        return {"success": True, "zone_id": zid}
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="동일한 영역명이 이미 존재합니다")
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Entities (ERD)
# ═══════════════════════════════════════════════════

@router.get("/entities")
async def list_entities(zone_id: Optional[int] = Query(None), domain: Optional[str] = Query(None)):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        query = """
            SELECT e.*, z.zone_name, z.zone_type
            FROM data_model_entity e
            LEFT JOIN data_zone z ON z.zone_id = e.zone_id
            WHERE 1=1
        """
        params = []
        idx = 1
        if zone_id is not None:
            query += f" AND e.zone_id = ${idx}"
            params.append(zone_id)
            idx += 1
        if domain:
            query += f" AND e.domain = ${idx}"
            params.append(domain)
            idx += 1
        query += " ORDER BY z.zone_type, e.domain, e.entity_name"
        rows = await conn.fetch(query, *params)
        return {
            "entities": [
                {
                    "entity_id": r["entity_id"],
                    "entity_name": r["entity_name"],
                    "logical_name": r["logical_name"],
                    "zone_id": r["zone_id"],
                    "zone_name": r["zone_name"],
                    "zone_type": r["zone_type"],
                    "domain": r["domain"],
                    "entity_type": r["entity_type"],
                    "description": r["description"],
                    "column_count": len(json.loads(r["columns_json"])) if isinstance(r["columns_json"], str) else len(r["columns_json"] or []),
                    "row_count": r["row_count"],
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.get("/entities/{entity_id}")
async def get_entity(entity_id: int):
    conn = await get_connection()
    try:
        r = await conn.fetchrow("""
            SELECT e.*, z.zone_name, z.zone_type
            FROM data_model_entity e
            LEFT JOIN data_zone z ON z.zone_id = e.zone_id
            WHERE e.entity_id = $1
        """, entity_id)
        if not r:
            raise HTTPException(status_code=404, detail="엔티티를 찾을 수 없습니다")
        return {
            "entity_id": r["entity_id"],
            "entity_name": r["entity_name"],
            "logical_name": r["logical_name"],
            "zone_name": r["zone_name"],
            "zone_type": r["zone_type"],
            "domain": r["domain"],
            "entity_type": r["entity_type"],
            "description": r["description"],
            "columns": json.loads(r["columns_json"]) if isinstance(r["columns_json"], str) else (r["columns_json"] or []),
            "row_count": r["row_count"],
        }
    finally:
        await conn.close()


@router.post("/entities")
async def create_entity(body: EntityCreate):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        eid = await conn.fetchval("""
            INSERT INTO data_model_entity (entity_name, logical_name, zone_id, domain, entity_type, description, columns_json)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb) RETURNING entity_id
        """, body.entity_name, body.logical_name, body.zone_id, body.domain,
             body.entity_type, body.description, json.dumps(body.columns_json or []))
        return {"success": True, "entity_id": eid}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Relations (ERD)
# ═══════════════════════════════════════════════════

@router.get("/relations")
async def list_relations():
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        rows = await conn.fetch("SELECT * FROM data_model_relation ORDER BY relation_id")
        return {
            "relations": [
                {
                    "relation_id": r["relation_id"],
                    "source_entity": r["source_entity"],
                    "target_entity": r["target_entity"],
                    "relation_type": r["relation_type"],
                    "fk_columns": r["fk_columns"],
                    "description": r["description"],
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.get("/erd-graph")
async def get_erd_graph(zone_type: Optional[str] = Query(None)):
    """ReactFlow 포맷 ERD 그래프"""
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)

        # Entities
        query = """
            SELECT e.*, z.zone_type FROM data_model_entity e
            LEFT JOIN data_zone z ON z.zone_id = e.zone_id
        """
        params = []
        if zone_type:
            query += " WHERE z.zone_type = $1"
            params.append(zone_type)
        query += " ORDER BY e.entity_name"
        entities = await conn.fetch(query, *params)

        # Relations
        relations = await conn.fetch("SELECT * FROM data_model_relation")

        entity_names = {e["entity_name"] for e in entities}

        zone_colors = {
            "source": "#E53E3E",
            "bronze": "#DD6B20",
            "silver": "#805AD5",
            "gold": "#D69E2E",
            "mart": "#38A169",
        }

        domain_y_offset = {}
        nodes = []
        for i, e in enumerate(entities):
            domain = e["domain"] or "기타"
            if domain not in domain_y_offset:
                domain_y_offset[domain] = len(domain_y_offset) * 400
            zone = e["zone_type"] or "silver"
            zone_x = {"source": 0, "bronze": 300, "silver": 600, "gold": 900, "mart": 1200}.get(zone, 600)
            col_count = len(json.loads(e["columns_json"])) if isinstance(e["columns_json"], str) else len(e["columns_json"] or [])
            nodes.append({
                "id": e["entity_name"],
                "type": "default",
                "position": {"x": zone_x, "y": domain_y_offset[domain] + (i % 8) * 80},
                "data": {
                    "label": f"{e['entity_name']} ({e['logical_name']})",
                    "zone": zone,
                    "domain": domain,
                    "color": zone_colors.get(zone, "#718096"),
                    "rowCount": e["row_count"],
                    "columnCount": col_count,
                    "entityType": e["entity_type"],
                },
            })

        edges = []
        for r in relations:
            if r["source_entity"] in entity_names and r["target_entity"] in entity_names:
                is_etl = "ETL" in (r["description"] or "")
                edges.append({
                    "id": f"rel-{r['relation_id']}",
                    "source": r["source_entity"],
                    "target": r["target_entity"],
                    "label": r["relation_type"],
                    "type": "smoothstep",
                    "animated": is_etl,
                    "style": {
                        "stroke": "#E53E3E" if is_etl else "#005BAC",
                        "strokeDasharray": "5 5" if is_etl else "none",
                    },
                })

        return {"nodes": nodes, "edges": edges}
    finally:
        await conn.close()


@router.post("/relations")
async def create_relation(body: RelationCreate):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        rid = await conn.fetchval("""
            INSERT INTO data_model_relation (source_entity, target_entity, relation_type, fk_columns, description)
            VALUES ($1, $2, $3, $4, $5) RETURNING relation_id
        """, body.source_entity, body.target_entity, body.relation_type, body.fk_columns, body.description)
        return {"success": True, "relation_id": rid}
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="동일한 관계가 이미 존재합니다")
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Naming Standards
# ═══════════════════════════════════════════════════

@router.get("/naming-rules")
async def list_naming_rules():
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        rows = await conn.fetch("SELECT * FROM naming_standard ORDER BY target, rule_id")
        return {
            "rules": [
                {
                    "rule_id": r["rule_id"],
                    "rule_name": r["rule_name"],
                    "target": r["target"],
                    "pattern": r["pattern"],
                    "example": r["example"],
                    "description": r["description"],
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.post("/naming-rules")
async def create_naming_rule(body: NamingRuleCreate):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        rid = await conn.fetchval("""
            INSERT INTO naming_standard (rule_name, target, pattern, example, description)
            VALUES ($1, $2, $3, $4, $5) RETURNING rule_id
        """, body.rule_name, body.target, body.pattern, body.example, body.description)
        return {"success": True, "rule_id": rid}
    finally:
        await conn.close()


@router.put("/naming-rules/{rule_id}")
async def update_naming_rule(rule_id: int, body: NamingRuleUpdate):
    conn = await get_connection()
    try:
        existing = await conn.fetchrow("SELECT * FROM naming_standard WHERE rule_id=$1", rule_id)
        if not existing:
            raise HTTPException(status_code=404, detail="규칙을 찾을 수 없습니다")
        updates = []
        params = []
        idx = 1
        for field in ["rule_name", "target", "pattern", "example", "description"]:
            val = getattr(body, field)
            if val is not None:
                updates.append(f"{field}=${idx}")
                params.append(val)
                idx += 1
        if not updates:
            raise HTTPException(status_code=400, detail="수정할 항목이 없습니다")
        params.append(rule_id)
        await conn.execute(
            f"UPDATE naming_standard SET {', '.join(updates)} WHERE rule_id=${idx}",
            *params,
        )
        return {"success": True, "rule_id": rule_id}
    finally:
        await conn.close()


@router.delete("/naming-rules/{rule_id}")
async def delete_naming_rule(rule_id: int):
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM naming_standard WHERE rule_id=$1", rule_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="규칙을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


@router.post("/naming-check")
async def check_naming(body: NamingCheckRequest):
    """명명 규칙 검증"""
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        rules = await conn.fetch("SELECT * FROM naming_standard WHERE target=$1", body.target)
        results = []
        for name in body.names:
            violations = []
            for rule in rules:
                try:
                    if not re.match(rule["pattern"], name):
                        violations.append({
                            "rule": rule["rule_name"],
                            "pattern": rule["pattern"],
                            "example": rule["example"],
                        })
                except re.error:
                    pass
            results.append({
                "name": name,
                "valid": len(violations) == 0,
                "violations": violations,
            })
        total_valid = sum(1 for r in results if r["valid"])
        return {
            "results": results,
            "summary": {
                "total": len(results),
                "valid": total_valid,
                "invalid": len(results) - total_valid,
                "compliance_rate": round(total_valid / max(len(results), 1) * 100, 1),
            },
        }
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Unstructured Data Mapping
# ═══════════════════════════════════════════════════

@router.get("/unstructured-mappings")
async def list_unstructured_mappings():
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        rows = await conn.fetch("SELECT * FROM unstructured_mapping ORDER BY mapping_id")
        return {
            "mappings": [
                {
                    "mapping_id": r["mapping_id"],
                    "source_type": r["source_type"],
                    "source_description": r["source_description"],
                    "target_table": r["target_table"],
                    "extraction_method": r["extraction_method"],
                    "nlp_model": r["nlp_model"],
                    "output_columns": list(r["output_columns"]) if r["output_columns"] else [],
                    "description": r["description"],
                    "status": r["status"],
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.post("/unstructured-mappings")
async def create_unstructured_mapping(body: UnstructuredMappingCreate):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        mid = await conn.fetchval("""
            INSERT INTO unstructured_mapping (source_type, source_description, target_table, extraction_method, nlp_model, output_columns, description)
            VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING mapping_id
        """, body.source_type, body.source_description, body.target_table,
             body.extraction_method, body.nlp_model, body.output_columns, body.description)
        return {"success": True, "mapping_id": mid}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Overview / Summary
# ═══════════════════════════════════════════════════

@router.get("/overview")
async def get_design_overview():
    """데이터 설계 현황 요약"""
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)

        zones = await conn.fetch("SELECT zone_type, COUNT(*) AS cnt FROM data_zone GROUP BY zone_type")
        entities = await conn.fetchrow("SELECT COUNT(*) AS total, COUNT(DISTINCT domain) AS domains FROM data_model_entity")
        relations = await conn.fetchval("SELECT COUNT(*) FROM data_model_relation")
        naming = await conn.fetchval("SELECT COUNT(*) FROM naming_standard")
        unstructured = await conn.fetchrow("""
            SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE status='active') AS active FROM unstructured_mapping
        """)

        # Naming compliance: check all actual tables
        tables = await conn.fetch("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'
        """)
        rules = await conn.fetch("SELECT pattern FROM naming_standard WHERE target='table'")
        compliant = 0
        for t in tables:
            ok = True
            for rule in rules:
                try:
                    if not re.match(rule["pattern"], t["table_name"]):
                        ok = False
                        break
                except re.error:
                    pass
            if ok:
                compliant += 1
        compliance = round(compliant / max(len(tables), 1) * 100, 1)

        return {
            "zones": {z["zone_type"]: z["cnt"] for z in zones},
            "entities": {"total": entities["total"], "domains": entities["domains"]},
            "relations": relations,
            "naming_rules": naming,
            "naming_compliance": compliance,
            "unstructured": {"total": unstructured["total"], "active": unstructured["active"]},
        }
    finally:
        await conn.close()
