"""
카탈로그 분석 헬퍼 — 상수, 모델, 시드 데이터, 유틸 함수
catalog_analytics.py 에서 import 해서 사용
"""
import os
import uuid
import json
import sqlite3
from typing import Optional

from pydantic import BaseModel, Field
import asyncpg


# ═══════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}

CONV_MEMORY_DB = os.getenv(
    "CONVERSATION_MEMORY_DB",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "conversation_memory.db"),
)


# ═══════════════════════════════════════════════════
#  Column Lineage
# ═══════════════════════════════════════════════════

COLUMN_LINEAGE = {
    "person": {
        "person_id": {"sources": ["Synthea patients.csv \u2192 person_id"], "targets": ["visit_occurrence.person_id", "condition_occurrence.person_id", "drug_exposure.person_id", "measurement.person_id"]},
        "gender_source_value": {"sources": ["Synthea patients.GENDER"], "targets": []},
        "year_of_birth": {"sources": ["Synthea patients.BIRTHDATE (year extracted)"], "targets": []},
    },
    "visit_occurrence": {
        "visit_occurrence_id": {"sources": ["Synthea encounters.Id \u2192 hash"], "targets": ["condition_occurrence.visit_occurrence_id", "measurement.visit_occurrence_id"]},
        "person_id": {"sources": ["person.person_id (FK)"], "targets": []},
        "visit_concept_id": {"sources": ["Synthea encounters.ENCOUNTERCLASS \u2192 concept mapping"], "targets": []},
        "visit_start_date": {"sources": ["Synthea encounters.START"], "targets": []},
    },
    "condition_occurrence": {
        "condition_concept_id": {"sources": ["Synthea conditions.CODE \u2192 SNOMED CT mapping"], "targets": ["condition_era.condition_concept_id"]},
        "person_id": {"sources": ["person.person_id (FK)"], "targets": []},
    },
    "measurement": {
        "measurement_concept_id": {"sources": ["Synthea observations.CODE \u2192 concept mapping"], "targets": []},
        "value_as_number": {"sources": ["Synthea observations.VALUE (numeric)"], "targets": []},
        "person_id": {"sources": ["person.person_id (FK)"], "targets": []},
    },
    "drug_exposure": {
        "drug_concept_id": {"sources": ["Synthea medications.CODE \u2192 RxNorm mapping"], "targets": ["drug_era.drug_concept_id"]},
        "person_id": {"sources": ["person.person_id (FK)"], "targets": []},
    },
}


# ═══════════════════════════════════════════════════
#  Pydantic Models
# ═══════════════════════════════════════════════════

class QueryLogCreate(BaseModel):
    user_id: str = Field(default="anonymous", max_length=64)
    query_text: Optional[str] = None
    query_type: str = Field(default="search", max_length=32)
    tables_accessed: list[str] = Field(default_factory=list)
    columns_accessed: list[str] = Field(default_factory=list)
    filters_used: Optional[dict] = None
    response_time_ms: int = 0
    result_count: int = 0


class MasterModelCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    creator: str = Field(default="admin", max_length=64)
    model_type: str = Field(default="cohort", pattern="^(cohort|trend|comparison|correlation)$")
    base_tables: list[str] = Field(default_factory=list)
    query_template: Optional[str] = None
    parameters: Optional[dict] = None
    shared: bool = True


class MasterModelUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    model_type: Optional[str] = None
    base_tables: Optional[list[str]] = None
    query_template: Optional[str] = None
    parameters: Optional[dict] = None
    shared: Optional[bool] = None


# ═══════════════════════════════════════════════════
#  DB Connection & Table Bootstrapping
# ═══════════════════════════════════════════════════

_tables_ensured = False


async def get_connection():
    return await asyncpg.connect(**OMOP_DB_CONFIG)


async def _ensure_tables():
    global _tables_ensured
    if _tables_ensured:
        return
    conn = await get_connection()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS catalog_query_log (
                log_id SERIAL PRIMARY KEY,
                user_id VARCHAR(64) NOT NULL DEFAULT 'anonymous',
                query_text TEXT,
                query_type VARCHAR(32) DEFAULT 'search',
                tables_accessed TEXT[] DEFAULT '{}',
                columns_accessed TEXT[] DEFAULT '{}',
                filters_used JSONB DEFAULT '{}',
                response_time_ms INTEGER DEFAULT 0,
                result_count INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_cql_created ON catalog_query_log(created_at);
            CREATE INDEX IF NOT EXISTS idx_cql_user ON catalog_query_log(user_id);

            CREATE TABLE IF NOT EXISTS catalog_master_model (
                model_id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                description TEXT,
                creator VARCHAR(64) NOT NULL DEFAULT 'admin',
                model_type VARCHAR(32) NOT NULL DEFAULT 'cohort',
                base_tables TEXT[] DEFAULT '{}',
                query_template TEXT,
                parameters JSONB DEFAULT '{}',
                usage_count INTEGER DEFAULT 0,
                shared BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        _tables_ensured = True
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Exported Utility — log_query_to_catalog
# ═══════════════════════════════════════════════════

async def log_query_to_catalog(
    user_id: str = "anonymous",
    query_text: str = "",
    query_type: str = "chat",
    tables_accessed: list = None,
    columns_accessed: list = None,
    filters_used: dict = None,
    response_time_ms: int = 0,
    result_count: int = 0,
):
    """AI \ucffc\ub9ac \uc2e4\ud589\uc744 catalog_query_log\uc5d0 \uc790\ub3d9 \uae30\ub85d (AAR-001: AI-Driven Lineage)

    chat_core.py, text2sql.py, conversation.py \ub4f1\uc5d0\uc11c \ud638\ucd9c.
    \uc2e4\ud328\ud574\ub3c4 \uc608\uc678\ub97c \ubc1c\uc0dd\uc2dc\ud0a4\uc9c0 \uc54a\uc74c (fire-and-forget).
    """
    try:
        await _ensure_tables()
        conn = await get_connection()
        try:
            await conn.execute(
                """INSERT INTO catalog_query_log
                   (user_id, query_text, query_type, tables_accessed, columns_accessed,
                    filters_used, response_time_ms, result_count)
                   VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8)""",
                user_id,
                (query_text or "")[:2000],
                query_type,
                tables_accessed or [],
                columns_accessed or [],
                json.dumps(filters_used or {}),
                response_time_ms,
                result_count,
            )
        finally:
            await conn.close()
    except Exception as e:
        print(f"[CatalogQueryLog] logging failed (non-blocking): {e}")


# ═══════════════════════════════════════════════════
#  Seed Data
# ═══════════════════════════════════════════════════

async def _ensure_seed_models():
    conn = await get_connection()
    try:
        cnt = await conn.fetchval("SELECT COUNT(*) FROM catalog_master_model")
        if cnt > 0:
            return
        seeds = [
            {
                "id": str(uuid.uuid4()),
                "name": "\ucf54\ud638\ud2b8 \ubd84\uc11d",
                "desc": "\ud2b9\uc815 \uc9c4\ub2e8(condition_concept_id) \uae30\ubc18 \ud658\uc790 \ucf54\ud638\ud2b8\ub97c \ucd94\ucd9c\ud569\ub2c8\ub2e4. \uc778\uad6c\ud1b5\uacc4 + \ubc29\ubb38\uc774\ub825 \uc870\uc778.",
                "type": "cohort",
                "tables": ["person", "condition_occurrence", "visit_occurrence"],
                "template": "SELECT p.person_id, p.gender_source_value, p.year_of_birth,\n       COUNT(DISTINCT v.visit_occurrence_id) AS visit_count\nFROM person p\nJOIN condition_occurrence co ON p.person_id = co.person_id\nJOIN visit_occurrence v ON p.person_id = v.person_id\nWHERE co.condition_concept_id = :concept_id\nGROUP BY p.person_id, p.gender_source_value, p.year_of_birth\nLIMIT 1000;",
                "params": {"concept_id": {"type": "integer", "default": 44054006, "label": "\uc9c4\ub2e8 \ucf54\ub4dc (SNOMED CT)"}},
            },
            {
                "id": str(uuid.uuid4()),
                "name": "\uc2dc\uac04 \ucd94\uc774 \ubd84\uc11d",
                "desc": "\ud2b9\uc815 \ubc29\ubb38\uc720\ud615 \ub610\ub294 \uc9c8\ud658\uc758 \uc5f0\ub3c4\ubcc4/\uc6d4\ubcc4 \ucd94\uc774\ub97c \ubd84\uc11d\ud569\ub2c8\ub2e4.",
                "type": "trend",
                "tables": ["visit_occurrence", "condition_occurrence"],
                "template": "SELECT EXTRACT(YEAR FROM v.visit_start_date) AS year,\n       EXTRACT(MONTH FROM v.visit_start_date) AS month,\n       COUNT(*) AS count\nFROM visit_occurrence v\nWHERE v.visit_concept_id = :visit_type\nGROUP BY year, month\nORDER BY year, month;",
                "params": {"visit_type": {"type": "integer", "default": 9202, "label": "\ubc29\ubb38 \uc720\ud615 (9201=\uc785\uc6d0, 9202=\uc678\ub798, 9203=\uc751\uae09)"}},
            },
            {
                "id": str(uuid.uuid4()),
                "name": "\uc57d\ubb3c \uc0c1\ud638\uc791\uc6a9 \ubd84\uc11d",
                "desc": "\ub3d9\uc77c \ud658\uc790\uc5d0\uac8c \ub3d9\uc2dc \ucc98\ubc29\ub41c \uc57d\ubb3c \uc870\ud569\uc744 \ubd84\uc11d\ud569\ub2c8\ub2e4.",
                "type": "comparison",
                "tables": ["drug_exposure", "person"],
                "template": "SELECT d1.drug_concept_id AS drug_a,\n       d2.drug_concept_id AS drug_b,\n       COUNT(DISTINCT d1.person_id) AS patient_count\nFROM drug_exposure d1\nJOIN drug_exposure d2 ON d1.person_id = d2.person_id\n  AND d1.drug_concept_id < d2.drug_concept_id\n  AND d1.drug_exposure_start_date = d2.drug_exposure_start_date\nGROUP BY d1.drug_concept_id, d2.drug_concept_id\nORDER BY patient_count DESC\nLIMIT 50;",
                "params": {},
            },
            {
                "id": str(uuid.uuid4()),
                "name": "\uac80\uc0ac \uc0c1\uad00\uad00\uacc4 \ubd84\uc11d",
                "desc": "\ub450 \uac80\uc0ac \ud56d\ubaa9(measurement_concept_id) \uac04 \uc218\uce58 \uc0c1\uad00\uad00\uacc4\ub97c \ubd84\uc11d\ud569\ub2c8\ub2e4.",
                "type": "correlation",
                "tables": ["measurement", "person"],
                "template": "SELECT m1.value_as_number AS val_a, m2.value_as_number AS val_b\nFROM measurement m1\nJOIN measurement m2 ON m1.person_id = m2.person_id\n  AND m1.measurement_date = m2.measurement_date\nWHERE m1.measurement_concept_id = :concept_a\n  AND m2.measurement_concept_id = :concept_b\n  AND m1.value_as_number IS NOT NULL\n  AND m2.value_as_number IS NOT NULL\nLIMIT 5000;",
                "params": {
                    "concept_a": {"type": "integer", "default": 3004249, "label": "\uac80\uc0ac\ud56d\ubaa9 A (concept_id)"},
                    "concept_b": {"type": "integer", "default": 3012888, "label": "\uac80\uc0ac\ud56d\ubaa9 B (concept_id)"},
                },
            },
            {
                "id": str(uuid.uuid4()),
                "name": "\ub3d9\ubc18\uc9c8\ud658 \ubd84\uc11d",
                "desc": "\ud2b9\uc815 \uc9c8\ud658 \ud658\uc790\uc758 \ub3d9\ubc18 \uc9c4\ub2e8 Top-N\uc744 \ucd94\ucd9c\ud569\ub2c8\ub2e4.",
                "type": "cohort",
                "tables": ["condition_occurrence", "person"],
                "template": "SELECT co2.condition_concept_id, COUNT(DISTINCT co2.person_id) AS patient_count\nFROM condition_occurrence co1\nJOIN condition_occurrence co2 ON co1.person_id = co2.person_id\n  AND co1.condition_concept_id != co2.condition_concept_id\nWHERE co1.condition_concept_id = :primary_condition\nGROUP BY co2.condition_concept_id\nORDER BY patient_count DESC\nLIMIT 20;",
                "params": {"primary_condition": {"type": "integer", "default": 44054006, "label": "\uc8fc \uc9c4\ub2e8 \ucf54\ub4dc (SNOMED CT)"}},
            },
        ]
        for s in seeds:
            await conn.execute(
                "INSERT INTO catalog_master_model (model_id, name, description, creator, model_type, base_tables, query_template, parameters, usage_count, shared) "
                "VALUES ($1, $2, $3, 'admin', $4, $5, $6, $7::jsonb, $8, TRUE)",
                s["id"], s["name"], s["desc"], s["type"], s["tables"],
                s["template"], json.dumps(s["params"]),
                (5 - i) * 10 + 5 if (i := seeds.index(s)) < 5 else 5,
            )
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Helper Functions
# ═══════════════════════════════════════════════════

def _query_conversation_memory() -> list[dict]:
    """conversation_memory.db\uc5d0\uc11c \ucd5c\uadfc \ud14c\uc774\ube14 \uc0ac\uc6a9 \uc774\ub825 \uc870\ud68c"""
    if not os.path.exists(CONV_MEMORY_DB):
        return []
    try:
        con = sqlite3.connect(CONV_MEMORY_DB, timeout=2)
        con.row_factory = sqlite3.Row
        rows = con.execute(
            "SELECT user_id, tables_used, created_at FROM conversations "
            "WHERE tables_used IS NOT NULL AND tables_used != '' "
            "ORDER BY created_at DESC LIMIT 500"
        ).fetchall()
        con.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _parse_params(val) -> dict:
    """JSONB \ucee8\ub7fc\uc774 str\ub85c \ubc18\ud658\ub420 \uc218 \uc788\uc73c\ubbc0\ub85c \uc548\uc804\ud558\uac8c \ud30c\uc2f1"""
    if not val:
        return {}
    if isinstance(val, dict):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return {}


def _row_to_model(r) -> dict:
    return {
        "model_id": r["model_id"],
        "name": r["name"],
        "description": r["description"],
        "creator": r["creator"],
        "model_type": r["model_type"],
        "base_tables": list(r["base_tables"]) if r["base_tables"] else [],
        "query_template": r["query_template"],
        "parameters": _parse_params(r["parameters"]),
        "usage_count": r["usage_count"],
        "shared": r["shared"],
        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
    }
