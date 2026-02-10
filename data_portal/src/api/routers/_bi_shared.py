"""
DPR-004: BI 공유 모듈
DB 설정, Pydantic 모델, 테이블 생성, SQL 검증, 시드 데이터
"""
import os
import re
import json
from datetime import datetime
from typing import List, Optional, Dict, Any

from fastapi import HTTPException
from pydantic import BaseModel, Field
import asyncpg

# ── DB Config (reuse from _mart_ops_shared) ──

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

class SqlExecuteRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=10000)
    limit: int = Field(default=1000, ge=1, le=10000)

class SqlValidateRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=10000)

class SaveQueryRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    sql_text: str = Field(..., min_length=1, max_length=10000)
    creator: str = Field(default="user")
    tags: List[str] = Field(default_factory=list)
    shared: bool = Field(default=False)

class ChartCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    chart_type: str = Field(..., pattern=r"^(bar|line|pie|doughnut|area|scatter|gauge|radar|heatmap|treemap|funnel|table)$")
    sql_query: str = Field(..., min_length=1, max_length=10000)
    config: Dict[str, Any] = Field(default_factory=dict)
    description: Optional[str] = Field(None, max_length=1000)
    creator: str = Field(default="user")

class ChartUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    chart_type: Optional[str] = Field(None, pattern=r"^(bar|line|pie|doughnut|area|scatter|gauge|radar|heatmap|treemap|funnel|table)$")
    sql_query: Optional[str] = Field(None, max_length=10000)
    config: Optional[Dict[str, Any]] = None
    description: Optional[str] = Field(None, max_length=1000)

class DrillDownRequest(BaseModel):
    dimension: str = Field(..., max_length=100)
    value: Any
    filters: Dict[str, Any] = Field(default_factory=dict)

class DashboardCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    layout: Dict[str, Any] = Field(default_factory=dict)
    chart_ids: List[int] = Field(default_factory=list)
    creator: str = Field(default="user")
    shared: bool = Field(default=False)

class DashboardUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=1000)
    layout: Optional[Dict[str, Any]] = None
    chart_ids: Optional[List[int]] = None
    shared: Optional[bool] = None

class ExportRequest(BaseModel):
    chart_ids: List[int] = Field(default_factory=list)
    dashboard_id: Optional[int] = None
    title: str = Field(default="BI 보고서")
    include_data: bool = Field(default=True)

# ── SQL Validation ──

FORBIDDEN_KEYWORDS = [
    "DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE",
    "GRANT", "REVOKE", "EXEC", "EXECUTE", "MERGE", "CALL",
    "PG_SLEEP", "PG_TERMINATE", "PG_CANCEL",
]

def validate_sql(sql: str) -> tuple[bool, str]:
    """SQL 검증: DML/DDL 차단, SELECT만 허용"""
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        return False, "빈 SQL"

    # Remove comments
    clean = re.sub(r'--[^\n]*', '', stripped)
    clean = re.sub(r'/\*.*?\*/', '', clean, flags=re.DOTALL)
    clean_upper = clean.upper().strip()

    # Must start with SELECT or WITH
    if not re.match(r'^(SELECT|WITH)\b', clean_upper):
        return False, "SELECT/WITH 문만 실행 가능합니다"

    # Check forbidden keywords
    for kw in FORBIDDEN_KEYWORDS:
        pattern = r'\b' + kw + r'\b'
        if re.search(pattern, clean_upper):
            return False, f"금지된 키워드: {kw}"

    # Block semicolons (multi-statement)
    if ";" in clean:
        return False, "다중 문(;)은 허용되지 않습니다"

    return True, "OK"


def ensure_limit(sql: str, limit: int = 1000) -> str:
    """LIMIT이 없으면 자동 추가"""
    clean_upper = sql.strip().rstrip(";").upper()
    if "LIMIT" not in clean_upper:
        return sql.strip().rstrip(";") + f" LIMIT {limit}"
    return sql.strip().rstrip(";")


# ── Table Setup ──

async def _ensure_bi_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS bi_query_history (
            id SERIAL PRIMARY KEY,
            sql_text TEXT NOT NULL,
            status VARCHAR(20) DEFAULT 'success',
            row_count INTEGER DEFAULT 0,
            execution_time_ms INTEGER DEFAULT 0,
            columns TEXT[],
            error_message TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS bi_saved_query (
            query_id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            sql_text TEXT NOT NULL,
            creator VARCHAR(50) DEFAULT 'user',
            tags JSONB DEFAULT '[]',
            shared BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS bi_chart (
            chart_id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            chart_type VARCHAR(20) NOT NULL,
            sql_query TEXT NOT NULL,
            config JSONB DEFAULT '{}',
            description TEXT,
            creator VARCHAR(50) DEFAULT 'user',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS bi_dashboard (
            dashboard_id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            layout JSONB DEFAULT '{}',
            chart_ids JSONB DEFAULT '[]',
            creator VARCHAR(50) DEFAULT 'user',
            shared BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)


# ── Seed Data ──

_BI_SEEDED = False

async def _ensure_bi_seed(conn):
    global _BI_SEEDED
    if _BI_SEEDED:
        return
    cnt = await conn.fetchval("SELECT COUNT(*) FROM bi_saved_query")
    if cnt > 0:
        _BI_SEEDED = True
        return

    # Saved queries
    queries = [
        ("환자 성별 분포", "성별 환자 수 분포",
         "SELECT gender_source_value AS 성별, COUNT(*) AS 환자수 FROM person GROUP BY gender_source_value",
         "admin", '["환자","인구통계"]'),
        ("연도별 방문 건수", "방문 유형별 연도별 추이",
         "SELECT EXTRACT(YEAR FROM visit_start_date)::int AS 연도, visit_concept_id AS 방문유형, COUNT(*) AS 건수 FROM visit_occurrence GROUP BY 1, 2 ORDER BY 1",
         "admin", '["방문","추이"]'),
        ("상위 10 진단", "가장 빈번한 상위 10개 진단 코드",
         "SELECT condition_source_value AS 진단코드, COUNT(*) AS 건수 FROM condition_occurrence GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
         "admin", '["진단","TOP10"]'),
        ("약물 처방 건수 TOP 10", "처방 빈도 상위 약물",
         "SELECT drug_source_value AS 약물코드, COUNT(*) AS 처방건수 FROM drug_exposure GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
         "admin", '["약물","TOP10"]'),
        ("연령대별 환자 분포", "10세 단위 연령대별 분포",
         "SELECT ((2025 - year_of_birth) / 10 * 10)::text || '대' AS 연령대, COUNT(*) AS 환자수 FROM person GROUP BY 1 ORDER BY 1",
         "admin", '["환자","연령"]'),
    ]
    for q in queries:
        await conn.execute("""
            INSERT INTO bi_saved_query (name, description, sql_text, creator, tags)
            VALUES ($1, $2, $3, $4, $5::jsonb)
        """, *q)

    # Charts
    charts = [
        ("성별 환자 분포", "pie",
         "SELECT gender_source_value AS 성별, COUNT(*) AS 환자수 FROM person GROUP BY gender_source_value",
         '{"xField":"성별","yField":"환자수","colorScheme":"default"}',
         "환자 성별 비율 파이 차트", "admin"),
        ("연도별 방문 추이", "line",
         "SELECT EXTRACT(YEAR FROM visit_start_date)::int AS 연도, COUNT(*) AS 방문건수 FROM visit_occurrence GROUP BY 1 ORDER BY 1",
         '{"xField":"연도","yField":"방문건수"}',
         "연도별 총 방문 건수 추이", "admin"),
        ("상위 진단 코드", "bar",
         "SELECT condition_source_value AS 진단코드, COUNT(*) AS 건수 FROM condition_occurrence GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
         '{"xField":"진단코드","yField":"건수","layout":"horizontal"}',
         "빈도 상위 10 진단코드 바 차트", "admin"),
    ]
    for c in charts:
        await conn.execute("""
            INSERT INTO bi_chart (name, chart_type, sql_query, config, description, creator)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6)
        """, *c)

    # Dashboard
    await conn.execute("""
        INSERT INTO bi_dashboard (name, description, layout, chart_ids, creator, shared)
        VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6)
    """,
        "OMOP CDM 기본 대시보드",
        "환자/방문/진단 핵심 지표 대시보드",
        '{"columns":2,"rows":2}',
        '[1, 2, 3]',
        "admin",
        True,
    )

    _BI_SEEDED = True


async def bi_init(conn):
    await _ensure_bi_tables(conn)
    await _ensure_bi_seed(conn)
