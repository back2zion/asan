"""
카탈로그 분석 API — 쿼리 패턴 분석 + 마스터 분석 모델 CRUD
DPR-002: 지능형 데이터 카탈로그 및 탐색 환경 구축
"""
import json
import uuid
from datetime import datetime, timedelta
from collections import Counter, defaultdict

from fastapi import APIRouter, HTTPException, Query

from services.redis_cache import cache_get as redis_get, cache_set as redis_set
from ._catalog_analytics_helpers import (
    COLUMN_LINEAGE,
    QueryLogCreate,
    MasterModelCreate,
    MasterModelUpdate,
    get_connection,
    _ensure_tables,
    _ensure_seed_models,
    _query_conversation_memory,
    _parse_params,
    _row_to_model,
    log_query_to_catalog,
)

router = APIRouter(prefix="/catalog-analytics", tags=["CatalogAnalytics"])


# ───── Query Log ─────

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
    # Redis 캐시 (days 별로 키 구분, 5분 TTL)
    cache_key = f"catalog-query-patterns:{days}"
    redis_data = await redis_get(cache_key)
    if redis_data:
        return redis_data

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

        result = {
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
        await redis_set(cache_key, result, 300)  # 5분 TTL
        return result
    finally:
        await conn.close()


# ───── Trending ─────

@router.get("/trending")
async def get_trending():
    """최근 7일 트렌딩 검색어/테이블"""
    # Redis 캐시 (5분 TTL)
    redis_data = await redis_get("catalog-trending")
    if redis_data:
        return redis_data

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

        result = {
            "trending_searches": [{"term": t, "count": c} for t, c in search_counter.most_common(8)],
            "trending_tables": [{"table": t, "count": c} for t, c in table_counter.most_common(8)],
        }
        await redis_set("catalog-trending", result, 300)  # 5분 TTL
        return result
    finally:
        await conn.close()


# ───── Column Lineage ─────

@router.get("/column-lineage/{table_name}/{column_name}")
async def get_column_lineage(table_name: str, column_name: str):
    """컬럼 레벨 리니지"""
    table_info = COLUMN_LINEAGE.get(table_name, {})
    col_info = table_info.get(column_name)
    if not col_info:
        return {
            "table": table_name,
            "column": column_name,
            "sources": [f"ETL pipeline \u2192 {table_name}.{column_name}"],
            "targets": [],
        }
    return {
        "table": table_name,
        "column": column_name,
        "sources": col_info["sources"],
        "targets": col_info["targets"],
    }


# ───── Master Models CRUD ─────

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
