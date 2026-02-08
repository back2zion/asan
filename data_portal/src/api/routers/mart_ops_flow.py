"""
Flow Stages + Optimizations + Overview
Sub-module of data_mart_ops.
"""
import json
from typing import Optional

from fastapi import APIRouter

from ._mart_ops_shared import (
    get_connection, _init,
    FlowStageCreate, OptimizationCreate,
)

router = APIRouter()


# ── Data Flow Stages ──

@router.get("/flow-stages")
async def list_flow_stages():
    conn = await get_connection()
    try:
        await _init(conn)
        rows = await conn.fetch("SELECT * FROM dm_flow_stage ORDER BY stage_order")
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/flow-stages")
async def create_flow_stage(body: FlowStageCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO dm_flow_stage (stage_name, stage_order, stage_type, storage_type,
                                       file_format, processing_rules, meta_config, description)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8) RETURNING stage_id
        """, body.stage_name, body.stage_order, body.stage_type, body.storage_type,
            body.file_format,
            json.dumps(body.processing_rules) if body.processing_rules else None,
            json.dumps(body.meta_config) if body.meta_config else None,
            body.description)
        return {"stage_id": row["stage_id"]}
    finally:
        await conn.close()


@router.put("/flow-stages/{stage_id}")
async def update_flow_stage(stage_id: int, body: FlowStageCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE dm_flow_stage SET stage_name=$2, stage_order=$3, stage_type=$4, storage_type=$5,
                file_format=$6, processing_rules=$7::jsonb, meta_config=$8::jsonb,
                description=$9, updated_at=NOW()
            WHERE stage_id=$1
        """, stage_id, body.stage_name, body.stage_order, body.stage_type, body.storage_type,
            body.file_format,
            json.dumps(body.processing_rules) if body.processing_rules else None,
            json.dumps(body.meta_config) if body.meta_config else None,
            body.description)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/flow-stages/pipeline")
async def get_pipeline_view():
    """데이터 흐름 파이프라인 시각화 데이터"""
    conn = await get_connection()
    try:
        await _init(conn)
        stages = await conn.fetch("SELECT * FROM dm_flow_stage ORDER BY stage_order")
        nodes = []
        edges = []
        for i, s in enumerate(stages):
            d = dict(s)
            nodes.append({
                "id": f"stage-{d['stage_id']}",
                "type": "default",
                "position": {"x": i * 280, "y": 100},
                "data": {
                    "label": d["stage_name"],
                    "stage_type": d["stage_type"],
                    "storage_type": d["storage_type"],
                    "file_format": d["file_format"],
                    "description": d["description"],
                    "processing_rules": d["processing_rules"],
                    "meta_config": d["meta_config"],
                },
            })
            if i > 0:
                edges.append({
                    "id": f"e-{stages[i-1]['stage_id']}-{d['stage_id']}",
                    "source": f"stage-{stages[i-1]['stage_id']}",
                    "target": f"stage-{d['stage_id']}",
                    "animated": True,
                    "label": d["stage_type"],
                })
        return {"nodes": nodes, "edges": edges}
    finally:
        await conn.close()


# ── Connection Optimization ──

@router.get("/optimizations")
async def list_optimizations(mart_id: Optional[int] = None, status: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if mart_id:
            params.append(mart_id)
            where.append(f"o.mart_id = ${len(params)}")
        if status:
            params.append(status)
            where.append(f"o.status = ${len(params)}")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"""
            SELECT o.*, m.mart_name FROM dm_connection_optimization o
            JOIN dm_mart_registry m ON m.mart_id = o.mart_id
            {w} ORDER BY o.created_at DESC
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/optimizations")
async def create_optimization(body: OptimizationCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO dm_connection_optimization (mart_id, opt_type, config, description)
            VALUES ($1,$2,$3::jsonb,$4) RETURNING opt_id
        """, body.mart_id, body.opt_type, json.dumps(body.config), body.description)
        return {"opt_id": row["opt_id"]}
    finally:
        await conn.close()


@router.put("/optimizations/{opt_id}/apply")
async def apply_optimization(opt_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("""
            UPDATE dm_connection_optimization SET status='applied', applied_at=NOW()
            WHERE opt_id=$1
        """, opt_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.delete("/optimizations/{opt_id}")
async def delete_optimization(opt_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("DELETE FROM dm_connection_optimization WHERE opt_id=$1", opt_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/optimizations/suggestions")
async def suggest_optimizations():
    """마트별 최적화 제안 자동 생성"""
    conn = await get_connection()
    try:
        await _init(conn)
        marts = await conn.fetch("SELECT * FROM dm_mart_registry WHERE status='active'")
        suggestions = []
        for m in marts:
            d = dict(m)
            src = json.loads(d["source_tables"]) if isinstance(d["source_tables"], str) else d["source_tables"]
            # 대용량 소스 테이블 보유 시 MV 제안
            large_tables = {"measurement", "observation", "procedure_occurrence", "visit_occurrence"}
            if set(src) & large_tables:
                suggestions.append({
                    "mart_id": d["mart_id"], "mart_name": d["mart_name"],
                    "opt_type": "materialized_view",
                    "reason": f"대용량 소스 테이블 사용: {list(set(src) & large_tables)}",
                    "priority": "high",
                })
            # 3개 이상 소스 테이블 JOIN 시 비정규화 제안
            if len(src) >= 3:
                suggestions.append({
                    "mart_id": d["mart_id"], "mart_name": d["mart_name"],
                    "opt_type": "denormalize",
                    "reason": f"{len(src)}개 소스 테이블 JOIN — 비정규화로 쿼리 간소화 가능",
                    "priority": "medium",
                })
            # 날짜 범위 쿼리 빈번 시 파티셔닝 제안
            if any(t in src for t in ["condition_occurrence", "drug_exposure", "measurement"]):
                suggestions.append({
                    "mart_id": d["mart_id"], "mart_name": d["mart_name"],
                    "opt_type": "partition",
                    "reason": "날짜 기반 조회가 빈번한 테이블 — 연도별 파티셔닝 권장",
                    "priority": "medium",
                })
        return {"suggestions": suggestions}
    finally:
        await conn.close()


# ── Overview ──

@router.get("/overview")
async def get_overview():
    conn = await get_connection()
    try:
        await _init(conn)
        marts = await conn.fetchval("SELECT COUNT(*) FROM dm_mart_registry WHERE status='active'")
        total_rows = await conn.fetchval("SELECT COALESCE(SUM(row_count),0) FROM dm_mart_registry WHERE status='active'")
        pending_changes = await conn.fetchval("SELECT COUNT(*) FROM dm_schema_change WHERE status='pending'")
        dims = await conn.fetchval("SELECT COUNT(*) FROM dm_dimension")
        metrics_total = await conn.fetchval("SELECT COUNT(*) FROM dm_standard_metric")
        metrics_catalog = await conn.fetchval("SELECT COUNT(*) FROM dm_standard_metric WHERE catalog_visible=TRUE")
        stages = await conn.fetchval("SELECT COUNT(*) FROM dm_flow_stage")
        opts_applied = await conn.fetchval("SELECT COUNT(*) FROM dm_connection_optimization WHERE status='applied'")
        opts_proposed = await conn.fetchval("SELECT COUNT(*) FROM dm_connection_optimization WHERE status='proposed'")
        return {
            "marts": marts,
            "total_rows": total_rows,
            "pending_schema_changes": pending_changes,
            "dimensions": dims,
            "metrics_total": metrics_total,
            "metrics_in_catalog": metrics_catalog,
            "flow_stages": stages,
            "optimizations_applied": opts_applied,
            "optimizations_proposed": opts_proposed,
        }
    finally:
        await conn.close()
