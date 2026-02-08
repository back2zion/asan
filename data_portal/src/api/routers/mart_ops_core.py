"""
Mart CRUD + Dimensions + Metrics + Schema Changes
Sub-module of data_mart_ops.
"""
import json
import hashlib
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Query

from ._mart_ops_shared import (
    get_connection, _init,
    MartCreate, DimensionCreate, StandardMetricCreate,
)

router = APIRouter()


# ── Mart Registry ──

@router.get("/marts")
async def list_marts(zone: Optional[str] = None, status: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if zone:
            params.append(zone)
            where.append(f"zone = ${len(params)}")
        if status:
            params.append(status)
            where.append(f"status = ${len(params)}")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"""
            SELECT *, (SELECT COUNT(*) FROM dm_schema_change sc WHERE sc.mart_id = m.mart_id AND sc.status='pending') AS pending_changes
            FROM dm_mart_registry m {w} ORDER BY mart_id
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/marts")
async def create_mart(body: MartCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        schema_json = json.dumps(body.target_schema) if body.target_schema else None
        schema_hash = hashlib.sha256(schema_json.encode()).hexdigest()[:16] if schema_json else None

        # 유사 마트 중복 감지
        duplicates = []
        if body.source_tables:
            existing = await conn.fetch("SELECT mart_id, mart_name, source_tables, schema_hash FROM dm_mart_registry WHERE status='active'")
            for e in existing:
                e_tables = set(json.loads(e["source_tables"]) if isinstance(e["source_tables"], str) else e["source_tables"])
                new_tables = set(body.source_tables)
                overlap = e_tables & new_tables
                if len(overlap) > 0 and len(overlap) / max(len(e_tables), len(new_tables)) > 0.5:
                    duplicates.append({
                        "mart_id": e["mart_id"], "mart_name": e["mart_name"],
                        "overlap_tables": list(overlap),
                        "similarity": round(len(overlap) / max(len(e_tables), len(new_tables)) * 100, 1),
                    })
                if schema_hash and e["schema_hash"] == schema_hash:
                    duplicates.append({
                        "mart_id": e["mart_id"], "mart_name": e["mart_name"],
                        "reason": "schema_hash_match",
                    })

        row = await conn.fetchrow("""
            INSERT INTO dm_mart_registry (mart_name, description, purpose, zone, source_tables,
                                          target_schema, schema_hash, owner, refresh_schedule, retention_days)
            VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9,$10)
            RETURNING mart_id
        """, body.mart_name, body.description, body.purpose, body.zone,
            json.dumps(body.source_tables), schema_json, schema_hash,
            body.owner, body.refresh_schedule, body.retention_days)
        return {"mart_id": row["mart_id"], "duplicates": duplicates}
    finally:
        await conn.close()


@router.put("/marts/{mart_id}")
async def update_mart(mart_id: int, body: MartCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        schema_json = json.dumps(body.target_schema) if body.target_schema else None
        schema_hash = hashlib.sha256(schema_json.encode()).hexdigest()[:16] if schema_json else None
        await conn.execute("""
            UPDATE dm_mart_registry SET mart_name=$2, description=$3, purpose=$4, zone=$5,
                source_tables=$6::jsonb, target_schema=$7::jsonb, schema_hash=$8,
                owner=$9, refresh_schedule=$10, retention_days=$11, updated_at=NOW()
            WHERE mart_id=$1
        """, mart_id, body.mart_name, body.description, body.purpose, body.zone,
            json.dumps(body.source_tables), schema_json, schema_hash,
            body.owner, body.refresh_schedule, body.retention_days)
        return {"ok": True}
    finally:
        await conn.close()


@router.delete("/marts/{mart_id}")
async def delete_mart(mart_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("UPDATE dm_mart_registry SET status='archived' WHERE mart_id=$1", mart_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/marts/{mart_id}")
async def get_mart_detail(mart_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("SELECT * FROM dm_mart_registry WHERE mart_id=$1", mart_id)
        if not row:
            raise HTTPException(404, "마트를 찾을 수 없습니다")
        data = dict(row)
        # 관련 스키마 변경
        changes = await conn.fetch(
            "SELECT * FROM dm_schema_change WHERE mart_id=$1 ORDER BY created_at DESC LIMIT 10", mart_id)
        data["schema_changes"] = [dict(c) for c in changes]
        # 관련 최적화
        opts = await conn.fetch(
            "SELECT * FROM dm_connection_optimization WHERE mart_id=$1 ORDER BY created_at DESC", mart_id)
        data["optimizations"] = [dict(o) for o in opts]
        # 관련 지표
        metrics = await conn.fetch(
            "SELECT * FROM dm_standard_metric WHERE mart_id=$1 ORDER BY metric_id", mart_id)
        data["metrics"] = [dict(m) for m in metrics]
        return data
    finally:
        await conn.close()


@router.post("/marts/check-duplicates")
async def check_duplicates(source_tables: List[str], schema: Optional[Dict[str, Any]] = None):
    """마트 생성 전 유사 마트 중복 검사"""
    conn = await get_connection()
    try:
        await _init(conn)
        schema_hash = hashlib.sha256(json.dumps(schema).encode()).hexdigest()[:16] if schema else None
        existing = await conn.fetch("SELECT mart_id, mart_name, source_tables, schema_hash, zone FROM dm_mart_registry WHERE status='active'")
        results = []
        for e in existing:
            e_tables = set(json.loads(e["source_tables"]) if isinstance(e["source_tables"], str) else e["source_tables"])
            new_tables = set(source_tables)
            overlap = e_tables & new_tables
            similarity = round(len(overlap) / max(len(e_tables), len(new_tables), 1) * 100, 1)
            if similarity > 30:
                results.append({
                    "mart_id": e["mart_id"], "mart_name": e["mart_name"], "zone": e["zone"],
                    "overlap_tables": list(overlap), "similarity": similarity,
                    "schema_match": schema_hash == e["schema_hash"] if schema_hash else False,
                })
        return {"duplicates": sorted(results, key=lambda x: x["similarity"], reverse=True)}
    finally:
        await conn.close()


# ── Schema Changes ──

@router.get("/schema-changes")
async def list_schema_changes(mart_id: Optional[int] = None, status: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if mart_id:
            params.append(mart_id)
            where.append(f"sc.mart_id = ${len(params)}")
        if status:
            params.append(status)
            where.append(f"sc.status = ${len(params)}")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"""
            SELECT sc.*, m.mart_name FROM dm_schema_change sc
            JOIN dm_mart_registry m ON m.mart_id = sc.mart_id
            {w} ORDER BY sc.created_at DESC
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/schema-changes/analyze")
async def analyze_schema_change(mart_id: int, new_schema: Dict[str, Any]):
    """스키마 변경 영향 분석"""
    conn = await get_connection()
    try:
        await _init(conn)
        mart = await conn.fetchrow("SELECT * FROM dm_mart_registry WHERE mart_id=$1", mart_id)
        if not mart:
            raise HTTPException(404, "마트를 찾을 수 없습니다")

        old_cols = set()
        new_cols = set()
        if mart["target_schema"]:
            old_schema = json.loads(mart["target_schema"]) if isinstance(mart["target_schema"], str) else mart["target_schema"]
            old_cols = set(old_schema.get("columns", []))
        new_cols = set(new_schema.get("columns", []))

        added = list(new_cols - old_cols)
        removed = list(old_cols - new_cols)

        # 영향 받는 마트 찾기
        dependent = []
        all_marts = await conn.fetch("SELECT mart_id, mart_name, source_tables FROM dm_mart_registry WHERE status='active' AND mart_id != $1", mart_id)
        for m in all_marts:
            m_tables = json.loads(m["source_tables"]) if isinstance(m["source_tables"], str) else m["source_tables"]
            if mart["mart_name"] in m_tables or any(t in m_tables for t in (json.loads(mart["source_tables"]) if isinstance(mart["source_tables"], str) else mart["source_tables"])):
                dependent.append({"mart_id": m["mart_id"], "mart_name": m["mart_name"]})

        impact = f"컬럼 추가: {len(added)}개, 삭제: {len(removed)}개, 영향 마트: {len(dependent)}개"
        return {
            "mart_id": mart_id,
            "columns_added": added,
            "columns_removed": removed,
            "dependent_marts": dependent,
            "impact_summary": impact,
            "risk_level": "high" if removed else ("medium" if dependent else "low"),
        }
    finally:
        await conn.close()


@router.put("/schema-changes/{change_id}/apply")
async def apply_schema_change(change_id: int, applied_by: str = "system"):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE dm_schema_change SET status='applied', applied_by=$2, applied_at=NOW()
            WHERE change_id=$1
        """, change_id, applied_by)
        return {"ok": True}
    finally:
        await conn.close()


@router.put("/schema-changes/{change_id}/rollback")
async def rollback_schema_change(change_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("UPDATE dm_schema_change SET status='rolled_back' WHERE change_id=$1", change_id)
        return {"ok": True}
    finally:
        await conn.close()


# ── Dimensions ──

@router.get("/dimensions")
async def list_dimensions():
    conn = await get_connection()
    try:
        await _init(conn)
        rows = await conn.fetch("SELECT * FROM dm_dimension ORDER BY dim_id")
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/dimensions")
async def create_dimension(body: DimensionCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO dm_dimension (dimension_name, logical_name, hierarchy_levels, attributes, description, mart_ids)
            VALUES ($1,$2,$3::jsonb,$4::jsonb,$5,$6::jsonb) RETURNING dim_id
        """, body.dimension_name, body.logical_name, json.dumps(body.hierarchy_levels),
            json.dumps(body.attributes), body.description, json.dumps(body.mart_ids))
        return {"dim_id": row["dim_id"]}
    finally:
        await conn.close()


@router.put("/dimensions/{dim_id}")
async def update_dimension(dim_id: int, body: DimensionCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE dm_dimension SET dimension_name=$2, logical_name=$3, hierarchy_levels=$4::jsonb,
                attributes=$5::jsonb, description=$6, mart_ids=$7::jsonb, updated_at=NOW()
            WHERE dim_id=$1
        """, dim_id, body.dimension_name, body.logical_name, json.dumps(body.hierarchy_levels),
            json.dumps(body.attributes), body.description, json.dumps(body.mart_ids))
        return {"ok": True}
    finally:
        await conn.close()


@router.delete("/dimensions/{dim_id}")
async def delete_dimension(dim_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("DELETE FROM dm_dimension WHERE dim_id=$1", dim_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/dimensions/{dim_id}/hierarchy")
async def get_dimension_hierarchy(dim_id: int):
    """Dimension 계층 트리 구조 반환"""
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("SELECT * FROM dm_dimension WHERE dim_id=$1", dim_id)
        if not row:
            raise HTTPException(404, "Dimension을 찾을 수 없습니다")
        data = dict(row)
        levels = json.loads(data["hierarchy_levels"]) if isinstance(data["hierarchy_levels"], str) else data["hierarchy_levels"]
        # Build tree
        tree = {"name": data["dimension_name"], "logical_name": data["logical_name"], "children": []}
        parent = tree
        for lv in levels:
            node = {"name": lv.get("level", ""), "key": lv.get("key", ""), "children": []}
            parent["children"].append(node)
            parent = node
        return {"dimension": data, "tree": tree}
    finally:
        await conn.close()


# ── Standard Metrics ──

@router.get("/metrics")
async def list_metrics(category: Optional[str] = None, catalog_only: bool = False):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if category:
            params.append(category)
            where.append(f"sm.category = ${len(params)}")
        if catalog_only:
            where.append("sm.catalog_visible = TRUE")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"""
            SELECT sm.*, m.mart_name FROM dm_standard_metric sm
            LEFT JOIN dm_mart_registry m ON m.mart_id = sm.mart_id
            {w} ORDER BY sm.category, sm.metric_id
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/metrics")
async def create_metric(body: StandardMetricCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO dm_standard_metric (metric_name, logical_name, formula, unit, dimension_ids,
                                            mart_id, category, description, catalog_visible)
            VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9) RETURNING metric_id
        """, body.metric_name, body.logical_name, body.formula, body.unit,
            json.dumps(body.dimension_ids), body.mart_id, body.category,
            body.description, body.catalog_visible)
        return {"metric_id": row["metric_id"]}
    finally:
        await conn.close()


@router.put("/metrics/{metric_id}")
async def update_metric(metric_id: int, body: StandardMetricCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE dm_standard_metric SET metric_name=$2, logical_name=$3, formula=$4, unit=$5,
                dimension_ids=$6::jsonb, mart_id=$7, category=$8, description=$9,
                catalog_visible=$10, updated_at=NOW()
            WHERE metric_id=$1
        """, metric_id, body.metric_name, body.logical_name, body.formula, body.unit,
            json.dumps(body.dimension_ids), body.mart_id, body.category,
            body.description, body.catalog_visible)
        return {"ok": True}
    finally:
        await conn.close()


@router.delete("/metrics/{metric_id}")
async def delete_metric(metric_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("DELETE FROM dm_standard_metric WHERE metric_id=$1", metric_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/metrics/catalog")
async def metric_catalog(search: Optional[str] = None):
    """카탈로그 서비스: 표준 지표 검색"""
    conn = await get_connection()
    try:
        await _init(conn)
        if search:
            rows = await conn.fetch("""
                SELECT sm.*, m.mart_name FROM dm_standard_metric sm
                LEFT JOIN dm_mart_registry m ON m.mart_id = sm.mart_id
                WHERE sm.catalog_visible = TRUE
                  AND (sm.metric_name ILIKE $1 OR sm.logical_name ILIKE $1 OR sm.description ILIKE $1)
                ORDER BY sm.category, sm.metric_id
            """, f"%{search}%")
        else:
            rows = await conn.fetch("""
                SELECT sm.*, m.mart_name FROM dm_standard_metric sm
                LEFT JOIN dm_mart_registry m ON m.mart_id = sm.mart_id
                WHERE sm.catalog_visible = TRUE
                ORDER BY sm.category, sm.metric_id
            """)
        # Group by category
        categories = {}
        for r in rows:
            cat = r["category"]
            if cat not in categories:
                categories[cat] = []
            categories[cat].append(dict(r))
        return {"total": len(rows), "categories": categories}
    finally:
        await conn.close()
