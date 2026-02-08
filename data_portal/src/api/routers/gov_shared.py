"""
거버넌스 공통 모듈 - DB 연결, Pydantic 모델, 분류 규칙, 유틸리티
"""
import os
import re
import asyncio
import json as _json
import sqlite3
import urllib.request
import base64
from datetime import datetime, date
from typing import List, Optional, Dict, Any
from fastapi import HTTPException, Query
from pydantic import BaseModel, Field
import asyncpg


# ── Pydantic 모델 ──

class SensitivityUpdate(BaseModel):
    table_name: str = Field(..., max_length=100)
    column_name: str = Field(..., max_length=100)
    level: str = Field(..., pattern=r"^(극비|민감|일반)$")
    reason: Optional[str] = Field(None, max_length=200)


class RoleCreate(BaseModel):
    role_name: str = Field(..., max_length=50)
    description: Optional[str] = Field(None, max_length=200)
    access_scope: str = Field(..., max_length=100)
    allowed_tables: List[str] = []
    security_level: str = Field(..., max_length=50)


class DeidentRuleCreate(BaseModel):
    target_column: str = Field(..., max_length=200)
    method: str = Field(..., pattern=r"^(마스킹|해시|라운딩|범주화|삭제)$")
    pattern: Optional[str] = Field(None, max_length=200)
    enabled: bool = True


class TableMetadataUpdate(BaseModel):
    table_name: str = Field(..., max_length=100)
    business_name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=500)
    domain: Optional[str] = Field(None, max_length=50)
    tags: Optional[List[str]] = None
    owner: Optional[str] = Field(None, max_length=100)


class ColumnMetadataUpdate(BaseModel):
    table_name: str = Field(..., max_length=100)
    column_name: str = Field(..., max_length=100)
    business_name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=500)


class SuggestTagsRequest(BaseModel):
    table_name: str = Field(..., max_length=100)


class StandardTermCreate(BaseModel):
    standard_name: str = Field(..., max_length=100)
    definition: str = Field(..., max_length=500)
    domain: str = Field(..., max_length=50)
    synonyms: List[str] = []
    abbreviation: Optional[str] = Field(None, max_length=50)
    english_name: Optional[str] = Field(None, max_length=100)


class ReidentRequestCreate(BaseModel):
    requester: str = Field(..., max_length=50)
    purpose: str = Field(..., max_length=500)
    target_tables: List[str]
    target_columns: List[str] = []
    expires_at: Optional[str] = None


class StandardIndicatorCreate(BaseModel):
    name: str = Field(..., max_length=100)
    definition: str = Field(..., max_length=500)
    formula: str = Field(..., max_length=300)
    unit: str = Field(..., max_length=50)
    frequency: str = Field(..., max_length=50)
    owner_dept: str = Field(..., max_length=100)
    data_source: str = Field(..., max_length=200)
    category: str = Field("임상", max_length=50)


# ── Auto-Tagging 도메인 기반 폴백 규칙 ──
DOMAIN_TAG_DEFAULTS: Dict[str, List[str]] = {
    "환자": ["마스터", "환자", "인구통계"],
    "진료": ["임상", "진단", "방문"],
    "검사": ["임상", "검사", "결과"],
    "처방": ["약물", "처방", "투약"],
    "시술": ["시술", "수술", "처치"],
    "관찰": ["관찰", "사회력", "임상"],
    "영상": ["영상", "X-ray", "PACS"],
    "의료진": ["의료진", "마스터", "조직"],
    "기관": ["기관", "마스터", "위치"],
}


# ── DB 연결 ──
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


# ── 민감도 분류 규칙 ──

CRITICAL_PATTERNS = {
    "person_id", "person_source_value", "year_of_birth", "month_of_birth",
    "day_of_birth", "birth_datetime", "gender_source_value",
    "race_source_value", "ethnicity_source_value", "location_id",
    "death_date", "death_datetime", "cause_source_value",
    "note_text", "note_title",
    "provider_source_value", "care_site_source_value",
    "npi",
}
CRITICAL_SUBSTRINGS = ["_source_value"]

SENSITIVE_PATTERNS = {
    "condition_concept_id", "drug_concept_id", "procedure_concept_id",
    "measurement_concept_id", "observation_concept_id", "device_concept_id",
    "value_as_number", "value_as_string", "value_as_concept_id",
    "quantity", "dose_unit_source_value", "route_source_value",
    "finding_labels", "impression",
}
SENSITIVE_SUBSTRINGS = ["_concept_id", "value_as_"]


def classify_column(table_name: str, column_name: str) -> str:
    """컬럼 민감도 분류: 극비/민감/일반"""
    col = column_name.lower()

    # 극비 판단
    if col in CRITICAL_PATTERNS:
        return "극비"
    if col == "person_id":
        return "극비"
    if table_name == "person" and col in {
        "person_source_value", "year_of_birth", "month_of_birth",
        "day_of_birth", "birth_datetime", "gender_source_value",
        "race_source_value", "ethnicity_source_value", "location_id",
    }:
        return "극비"
    if table_name in ("note", "note_nlp") and col in {"note_text", "note_title", "note_source_value", "snippet", "term"}:
        return "극비"
    if table_name == "death":
        return "극비"
    if col.endswith("_source_value"):
        return "극비"

    # 민감 판단
    if col in SENSITIVE_PATTERNS:
        return "민감"
    if col.endswith("_concept_id") and col != "person_id":
        return "민감"
    if col.startswith("value_as_"):
        return "민감"

    return "일반"


# ── Airflow 유틸리티 ──

def _fetch_airflow_dag_sync(dag_id: str) -> dict:
    """Airflow REST API에서 DAG 최근 실행 조회 (동기)"""
    airflow_url = os.getenv("AIRFLOW_API_URL", "http://localhost:18080")
    creds = base64.b64encode(
        f"{os.getenv('AIRFLOW_USER', 'admin')}:{os.getenv('AIRFLOW_PASSWORD', 'admin')}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {creds}"}
    req = urllib.request.Request(
        f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns?limit=1&order_by=-start_date",
        headers=headers,
    )
    with urllib.request.urlopen(req, timeout=3) as resp:
        data = _json.loads(resp.read())
        runs = data.get("dag_runs", [])
        if runs:
            return {"lastRun": runs[0].get("start_date", ""), "state": runs[0].get("state", "")}
    return {}


async def _fetch_airflow_dag(dag_id: str) -> dict:
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _fetch_airflow_dag_sync, dag_id), timeout=5
        )
    except Exception:
        return {}


# ── SQLite Usage-Lineage 유틸리티 ──

def _get_sqlite_db_path() -> str:
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "conversation_memory.db")


def _parse_usage_data() -> Dict[str, Any]:
    """SQLite query_history에서 tables_used 파싱 및 동시 출현 분석"""
    import json

    db_path = _get_sqlite_db_path()
    if not os.path.exists(db_path):
        return {"total_queries": 0, "table_frequency": {}, "co_occurrence_edges": [], "analysis_period": None}

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM query_history WHERE tables_used IS NOT NULL AND tables_used != ''")
    total_queries = cursor.fetchone()[0]

    cursor.execute("SELECT MIN(created_at), MAX(created_at) FROM query_history WHERE tables_used IS NOT NULL AND tables_used != ''")
    period_row = cursor.fetchone()
    analysis_period = {"start": period_row[0], "end": period_row[1]} if period_row[0] else None

    cursor.execute("SELECT tables_used FROM query_history WHERE tables_used IS NOT NULL AND tables_used != ''")
    rows = cursor.fetchall()
    conn.close()

    table_freq: Dict[str, int] = {}
    co_occurrence: Dict[str, int] = {}

    for (tables_used_str,) in rows:
        try:
            parsed = json.loads(tables_used_str)
            if isinstance(parsed, list):
                tables = [t.strip().lower() for t in parsed if isinstance(t, str) and t.strip()]
            else:
                tables = [t.strip().lower() for t in tables_used_str.split(",") if t.strip()]
        except (json.JSONDecodeError, Exception):
            tables = [t.strip().lower() for t in tables_used_str.split(",") if t.strip()]
        tables = list(dict.fromkeys(tables))
        if not tables:
            continue

        for t in tables:
            table_freq[t] = table_freq.get(t, 0) + 1

        for i in range(len(tables)):
            for j in range(i + 1, len(tables)):
                pair = tuple(sorted([tables[i], tables[j]]))
                key = f"{pair[0]}|{pair[1]}"
                co_occurrence[key] = co_occurrence.get(key, 0) + 1

    co_edges = []
    for key, count in sorted(co_occurrence.items(), key=lambda x: -x[1]):
        parts = key.split("|")
        co_edges.append({"source": parts[0], "target": parts[1], "weight": count})

    return {
        "total_queries": total_queries,
        "table_frequency": dict(sorted(table_freq.items(), key=lambda x: -x[1])),
        "co_occurrence_edges": co_edges,
        "analysis_period": analysis_period,
    }
