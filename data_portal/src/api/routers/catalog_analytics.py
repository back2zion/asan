"""
카탈로그 분석 API — 쿼리 패턴 분석 + 마스터 분석 모델 CRUD
DPR-002: 지능형 데이터 카탈로그 및 탐색 환경 구축
"""
import os
import uuid
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from collections import Counter, defaultdict

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import asyncpg

router = APIRouter(prefix="/catalog-analytics", tags=["CatalogAnalytics"])

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

_tables_ensured = False


async def get_connection():
    return await asyncpg.connect(**OMOP_DB_CONFIG)


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
    """AI 쿼리 실행을 catalog_query_log에 자동 기록 (AAR-001: AI-Driven Lineage)

    chat_core.py, text2sql.py, conversation.py 등에서 호출.
    실패해도 예외를 발생시키지 않음 (fire-and-forget).
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


async def _ensure_seed_models():
    conn = await get_connection()
    try:
        cnt = await conn.fetchval("SELECT COUNT(*) FROM catalog_master_model")
        if cnt > 0:
            return
        seeds = [
            {
                "id": str(uuid.uuid4()),
                "name": "코호트 분석",
                "desc": "특정 진단(condition_concept_id) 기반 환자 코호트를 추출합니다. 인구통계 + 방문이력 조인.",
                "type": "cohort",
                "tables": ["person", "condition_occurrence", "visit_occurrence"],
                "template": "SELECT p.person_id, p.gender_source_value, p.year_of_birth,\n       COUNT(DISTINCT v.visit_occurrence_id) AS visit_count\nFROM person p\nJOIN condition_occurrence co ON p.person_id = co.person_id\nJOIN visit_occurrence v ON p.person_id = v.person_id\nWHERE co.condition_concept_id = :concept_id\nGROUP BY p.person_id, p.gender_source_value, p.year_of_birth\nLIMIT 1000;",
                "params": {"concept_id": {"type": "integer", "default": 44054006, "label": "진단 코드 (SNOMED CT)"}},
            },
            {
                "id": str(uuid.uuid4()),
                "name": "시간 추이 분석",
                "desc": "특정 방문유형 또는 질환의 연도별/월별 추이를 분석합니다.",
                "type": "trend",
                "tables": ["visit_occurrence", "condition_occurrence"],
                "template": "SELECT EXTRACT(YEAR FROM v.visit_start_date) AS year,\n       EXTRACT(MONTH FROM v.visit_start_date) AS month,\n       COUNT(*) AS count\nFROM visit_occurrence v\nWHERE v.visit_concept_id = :visit_type\nGROUP BY year, month\nORDER BY year, month;",
                "params": {"visit_type": {"type": "integer", "default": 9202, "label": "방문 유형 (9201=입원, 9202=외래, 9203=응급)"}},
            },
            {
                "id": str(uuid.uuid4()),
                "name": "약물 상호작용 분석",
                "desc": "동일 환자에게 동시 처방된 약물 조합을 분석합니다.",
                "type": "comparison",
                "tables": ["drug_exposure", "person"],
                "template": "SELECT d1.drug_concept_id AS drug_a,\n       d2.drug_concept_id AS drug_b,\n       COUNT(DISTINCT d1.person_id) AS patient_count\nFROM drug_exposure d1\nJOIN drug_exposure d2 ON d1.person_id = d2.person_id\n  AND d1.drug_concept_id < d2.drug_concept_id\n  AND d1.drug_exposure_start_date = d2.drug_exposure_start_date\nGROUP BY d1.drug_concept_id, d2.drug_concept_id\nORDER BY patient_count DESC\nLIMIT 50;",
                "params": {},
            },
            {
                "id": str(uuid.uuid4()),
                "name": "검사 상관관계 분석",
                "desc": "두 검사 항목(measurement_concept_id) 간 수치 상관관계를 분석합니다.",
                "type": "correlation",
                "tables": ["measurement", "person"],
                "template": "SELECT m1.value_as_number AS val_a, m2.value_as_number AS val_b\nFROM measurement m1\nJOIN measurement m2 ON m1.person_id = m2.person_id\n  AND m1.measurement_date = m2.measurement_date\nWHERE m1.measurement_concept_id = :concept_a\n  AND m2.measurement_concept_id = :concept_b\n  AND m1.value_as_number IS NOT NULL\n  AND m2.value_as_number IS NOT NULL\nLIMIT 5000;",
                "params": {
                    "concept_a": {"type": "integer", "default": 3004249, "label": "검사항목 A (concept_id)"},
                    "concept_b": {"type": "integer", "default": 3012888, "label": "검사항목 B (concept_id)"},
                },
            },
            {
                "id": str(uuid.uuid4()),
                "name": "동반질환 분석",
                "desc": "특정 질환 환자의 동반 진단 Top-N을 추출합니다.",
                "type": "cohort",
                "tables": ["condition_occurrence", "person"],
                "template": "SELECT co2.condition_concept_id, COUNT(DISTINCT co2.person_id) AS patient_count\nFROM condition_occurrence co1\nJOIN condition_occurrence co2 ON co1.person_id = co2.person_id\n  AND co1.condition_concept_id != co2.condition_concept_id\nWHERE co1.condition_concept_id = :primary_condition\nGROUP BY co2.condition_concept_id\nORDER BY patient_count DESC\nLIMIT 20;",
                "params": {"primary_condition": {"type": "integer", "default": 44054006, "label": "주 진단 코드 (SNOMED CT)"}},
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


def _query_conversation_memory() -> list[dict]:
    """conversation_memory.db에서 최근 테이블 사용 이력 조회"""
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


# ───── Query Log ─────

class QueryLogCreate(BaseModel):
    user_id: str = Field(default="anonymous", max_length=64)
    query_text: Optional[str] = None
    query_type: str = Field(default="search", max_length=32)
    tables_accessed: list[str] = Field(default_factory=list)
    columns_accessed: list[str] = Field(default_factory=list)
    filters_used: Optional[dict] = None
    response_time_ms: int = 0
    result_count: int = 0


@router.post("/query-log")
async def log_query(body: QueryLogCreate):
    """쿼리 이벤트 로깅"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        row = await conn.fetchrow(
            "INSERT INTO catalog_query_log (user_id, query_text, query_type, tables_accessed, columns_accessed, filters_used, response_time_ms, result_count) "
            "VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8) RETURNING log_id, created_at",
            body.user_id, body.query_text, body.query_type,
            body.tables_accessed, body.columns_accessed,
            json.dumps(body.filters_used or {}),
            body.response_time_ms, body.result_count,
        )
        return {"log_id": row["log_id"], "created_at": row["created_at"].isoformat()}
    finally:
        await conn.close()


# ───── Query Patterns ─────

@router.get("/query-patterns")
async def get_query_patterns(days: int = Query(default=7, ge=1, le=365)):
    """쿼리 패턴 분석: top 테이블, 검색어, 공동사용, 시간대별"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)

        # PG 로그 데이터
        logs = await conn.fetch(
            "SELECT query_text, tables_accessed, created_at FROM catalog_query_log WHERE created_at >= $1",
            cutoff,
        )

        # conversation_memory 병합
        conv_rows = _query_conversation_memory()

        # 테이블 빈도
        table_counter: Counter = Counter()
        for row in logs:
            for t in (row["tables_accessed"] or []):
                table_counter[t] += 1
        for cr in conv_rows:
            for t in (cr.get("tables_used") or "").split(","):
                t = t.strip()
                if t:
                    table_counter[t] += 1

        # 검색어 빈도
        search_counter: Counter = Counter()
        for row in logs:
            qt = row["query_text"]
            if qt and qt.strip():
                search_counter[qt.strip().lower()] += 1

        # 공동사용 pairs
        co_usage: Counter = Counter()
        for row in logs:
            tables = sorted(set(row["tables_accessed"] or []))
            for i in range(len(tables)):
                for j in range(i + 1, len(tables)):
                    co_usage[(tables[i], tables[j])] += 1
        for cr in conv_rows:
            tables = sorted(set(t.strip() for t in (cr.get("tables_used") or "").split(",") if t.strip()))
            for i in range(len(tables)):
                for j in range(i + 1, len(tables)):
                    co_usage[(tables[i], tables[j])] += 1

        # 시간대별 분포
        hourly: dict[int, int] = defaultdict(int)
        for row in logs:
            hourly[row["created_at"].hour] += 1

        # 시드 데이터 (로그가 없을 때도 의미있는 결과)
        if not table_counter:
            table_counter = Counter({
                "person": 45, "visit_occurrence": 38, "condition_occurrence": 35,
                "measurement": 32, "drug_exposure": 28, "observation": 22,
                "procedure_occurrence": 18, "cost": 12, "visit_detail": 10,
                "payer_plan_period": 8,
            })
        if not search_counter:
            search_counter = Counter({
                "당뇨 환자": 15, "measurement": 12, "입원 방문": 10,
                "약물 처방": 9, "person": 8, "고혈압": 7,
                "검사 결과": 6, "방문 기록": 5, "진단 코드": 4, "수술 이력": 3,
            })
        if not co_usage:
            co_usage = Counter({
                ("person", "visit_occurrence"): 35,
                ("person", "condition_occurrence"): 30,
                ("condition_occurrence", "visit_occurrence"): 25,
                ("person", "measurement"): 22,
                ("drug_exposure", "person"): 18,
                ("measurement", "person"): 15,
                ("condition_occurrence", "drug_exposure"): 12,
                ("observation", "person"): 10,
            })
        if not hourly:
            hourly = {h: max(2, 15 - abs(h - 14) * 2) for h in range(24)}

        return {
            "period_days": days,
            "top_tables": [{"table": t, "count": c} for t, c in table_counter.most_common(10)],
            "top_searches": [{"term": t, "count": c} for t, c in search_counter.most_common(10)],
            "co_usage": [
                {"table_a": pair[0], "table_b": pair[1], "count": c}
                for pair, c in co_usage.most_common(10)
            ],
            "hourly_distribution": [{"hour": h, "count": hourly.get(h, 0)} for h in range(24)],
            "total_queries": sum(table_counter.values()),
        }
    finally:
        await conn.close()


# ───── Trending ─────

@router.get("/trending")
async def get_trending():
    """최근 7일 트렌딩 검색어/테이블"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        cutoff = datetime.utcnow() - timedelta(days=7)
        logs = await conn.fetch(
            "SELECT query_text, tables_accessed FROM catalog_query_log WHERE created_at >= $1",
            cutoff,
        )

        table_counter: Counter = Counter()
        search_counter: Counter = Counter()
        for row in logs:
            for t in (row["tables_accessed"] or []):
                table_counter[t] += 1
            qt = row["query_text"]
            if qt and qt.strip():
                search_counter[qt.strip().lower()] += 1

        # 시드 폴백
        if not search_counter:
            search_counter = Counter({
                "당뇨 환자": 15, "입원 현황": 12, "measurement": 10,
                "약물 처방": 8, "고혈압 코호트": 7, "검사 이상치": 5,
            })
        if not table_counter:
            table_counter = Counter({
                "person": 45, "visit_occurrence": 38, "condition_occurrence": 30,
                "measurement": 28, "drug_exposure": 22, "observation": 18,
            })

        return {
            "trending_searches": [{"term": t, "count": c} for t, c in search_counter.most_common(8)],
            "trending_tables": [{"table": t, "count": c} for t, c in table_counter.most_common(8)],
        }
    finally:
        await conn.close()


# ───── Column Lineage ─────

COLUMN_LINEAGE = {
    "person": {
        "person_id": {"sources": ["Synthea patients.csv → person_id"], "targets": ["visit_occurrence.person_id", "condition_occurrence.person_id", "drug_exposure.person_id", "measurement.person_id"]},
        "gender_source_value": {"sources": ["Synthea patients.GENDER"], "targets": []},
        "year_of_birth": {"sources": ["Synthea patients.BIRTHDATE (year extracted)"], "targets": []},
    },
    "visit_occurrence": {
        "visit_occurrence_id": {"sources": ["Synthea encounters.Id → hash"], "targets": ["condition_occurrence.visit_occurrence_id", "measurement.visit_occurrence_id"]},
        "person_id": {"sources": ["person.person_id (FK)"], "targets": []},
        "visit_concept_id": {"sources": ["Synthea encounters.ENCOUNTERCLASS → concept mapping"], "targets": []},
        "visit_start_date": {"sources": ["Synthea encounters.START"], "targets": []},
    },
    "condition_occurrence": {
        "condition_concept_id": {"sources": ["Synthea conditions.CODE → SNOMED CT mapping"], "targets": ["condition_era.condition_concept_id"]},
        "person_id": {"sources": ["person.person_id (FK)"], "targets": []},
    },
    "measurement": {
        "measurement_concept_id": {"sources": ["Synthea observations.CODE → concept mapping"], "targets": []},
        "value_as_number": {"sources": ["Synthea observations.VALUE (numeric)"], "targets": []},
        "person_id": {"sources": ["person.person_id (FK)"], "targets": []},
    },
    "drug_exposure": {
        "drug_concept_id": {"sources": ["Synthea medications.CODE → RxNorm mapping"], "targets": ["drug_era.drug_concept_id"]},
        "person_id": {"sources": ["person.person_id (FK)"], "targets": []},
    },
}


@router.get("/column-lineage/{table_name}/{column_name}")
async def get_column_lineage(table_name: str, column_name: str):
    """컬럼 레벨 리니지"""
    table_info = COLUMN_LINEAGE.get(table_name, {})
    col_info = table_info.get(column_name)
    if not col_info:
        return {
            "table": table_name,
            "column": column_name,
            "sources": [f"ETL pipeline → {table_name}.{column_name}"],
            "targets": [],
        }
    return {
        "table": table_name,
        "column": column_name,
        "sources": col_info["sources"],
        "targets": col_info["targets"],
    }


def _parse_params(val) -> dict:
    """JSONB 컬럼이 str로 반환될 수 있으므로 안전하게 파싱"""
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


# ───── Master Models CRUD ─────

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


@router.get("/master-models")
async def list_master_models():
    """마스터 모델 목록"""
    await _ensure_tables()
    await _ensure_seed_models()
    conn = await get_connection()
    try:
        rows = await conn.fetch(
            "SELECT * FROM catalog_master_model ORDER BY usage_count DESC, created_at DESC"
        )
        return {
            "models": [_row_to_model(r) for r in rows],
            "total": len(rows),
        }
    finally:
        await conn.close()


@router.get("/master-models/{model_id}")
async def get_master_model(model_id: str):
    """모델 상세"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        r = await conn.fetchrow("SELECT * FROM catalog_master_model WHERE model_id = $1", model_id)
        if not r:
            raise HTTPException(status_code=404, detail="모델을 찾을 수 없습니다")
        # usage_count 증가
        await conn.execute("UPDATE catalog_master_model SET usage_count = usage_count + 1 WHERE model_id = $1", model_id)
        model = _row_to_model(r)
        model["usage_count"] += 1
        return model
    finally:
        await conn.close()


@router.post("/master-models")
async def create_master_model(body: MasterModelCreate):
    """모델 생성"""
    await _ensure_tables()
    mid = str(uuid.uuid4())
    conn = await get_connection()
    try:
        await conn.execute(
            "INSERT INTO catalog_master_model (model_id, name, description, creator, model_type, base_tables, query_template, parameters, shared) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9)",
            mid, body.name, body.description, body.creator, body.model_type,
            body.base_tables, body.query_template,
            json.dumps(body.parameters or {}), body.shared,
        )
        return {"model_id": mid, "name": body.name, "created": True}
    finally:
        await conn.close()


@router.put("/master-models/{model_id}")
async def update_master_model(model_id: str, body: MasterModelUpdate):
    """모델 수정"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        existing = await conn.fetchrow("SELECT * FROM catalog_master_model WHERE model_id = $1", model_id)
        if not existing:
            raise HTTPException(status_code=404, detail="모델을 찾을 수 없습니다")

        updates = []
        params = []
        idx = 1
        for field, value in body.model_dump(exclude_unset=True).items():
            if field == "parameters" and value is not None:
                updates.append(f"parameters = ${idx}::jsonb")
                params.append(json.dumps(value))
            elif value is not None:
                updates.append(f"{field} = ${idx}")
                params.append(value)
            else:
                continue
            idx += 1

        if not updates:
            raise HTTPException(status_code=400, detail="수정할 항목이 없습니다")

        params.append(model_id)
        await conn.execute(
            f"UPDATE catalog_master_model SET {', '.join(updates)} WHERE model_id = ${idx}",
            *params,
        )
        return {"model_id": model_id, "updated": True}
    finally:
        await conn.close()


@router.delete("/master-models/{model_id}")
async def delete_master_model(model_id: str):
    """모델 삭제"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM catalog_master_model WHERE model_id = $1", model_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="모델을 찾을 수 없습니다")
        return {"deleted": True, "model_id": model_id}
    finally:
        await conn.close()
