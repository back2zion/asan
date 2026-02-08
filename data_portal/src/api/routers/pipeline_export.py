"""
Pipeline — 외부 시스템 Export Pipeline + 대시보드 + 실행 이력
"""
import json
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query

from .pipeline_shared import (
    get_conn, ensure_tables, seed_query_history, seed_executions,
    ExportPipelineCreate,
    builtin_lz_templates, builtin_export_pipelines,
    load_custom_templates, load_custom_exports, save_custom_exports,
)

router = APIRouter()


# ════════════════════════════════════════════════════════════
#  C. 외부 시스템 Export Pipeline
# ════════════════════════════════════════════════════════════

@router.get("/exports")
async def list_exports():
    """Export Pipeline 전체 조회 (Built-in + Custom)"""
    builtin = builtin_export_pipelines()
    custom = load_custom_exports()
    return {"exports": builtin + custom, "builtin_count": len(builtin), "custom_count": len(custom)}


@router.post("/exports")
async def create_export(body: ExportPipelineCreate):
    """커스텀 Export Pipeline 생성"""
    custom = load_custom_exports()
    new_id = f"exp-custom-{len(custom) + 1}"
    export = {"id": new_id, **body.model_dump(), "created_at": datetime.now().isoformat()}
    custom.append(export)
    save_custom_exports(custom)
    return {"status": "created", "export": export}


@router.put("/exports/{export_id}")
async def update_export(export_id: str, body: ExportPipelineCreate):
    """커스텀 Export Pipeline 수정"""
    custom = load_custom_exports()
    for i, e in enumerate(custom):
        if e["id"] == export_id:
            custom[i] = {"id": export_id, **body.model_dump(), "created_at": e.get("created_at")}
            save_custom_exports(custom)
            return {"status": "updated", "export": custom[i]}
    raise HTTPException(status_code=404, detail=f"Export '{export_id}' 없음 (Built-in은 수정 불가)")


@router.delete("/exports/{export_id}")
async def delete_export(export_id: str):
    """커스텀 Export Pipeline 삭제"""
    custom = load_custom_exports()
    orig_len = len(custom)
    custom = [e for e in custom if e["id"] != export_id]
    if len(custom) == orig_len:
        raise HTTPException(status_code=404, detail=f"Export '{export_id}' 없음 (Built-in은 삭제 불가)")
    save_custom_exports(custom)
    return {"status": "deleted", "id": export_id}


@router.post("/exports/{export_id}/execute")
async def execute_export(export_id: str):
    """Export Pipeline 실행 (시뮬레이션)"""
    all_exports = builtin_export_pipelines() + load_custom_exports()
    export = next((e for e in all_exports if e["id"] == export_id), None)
    if not export:
        raise HTTPException(status_code=404, detail=f"Export '{export_id}' 없음")

    conn = await get_conn()
    try:
        await ensure_tables(conn)
        # 시뮬레이션 실행: 실제 OMOP 테이블에서 행 수 조회
        total_rows = 0
        for tbl in export.get("source_tables", []):
            cnt = await conn.fetchval("SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = $1", tbl)
            total_rows += (cnt or 0)

        # 실행 이력 기록
        row = await conn.fetchrow("""
            INSERT INTO pipeline_execution (pipeline_id, pipeline_type, pipeline_name, status, rows_processed)
            VALUES ($1, 'export', $2, 'completed', $3)
            RETURNING exec_id, pipeline_id, pipeline_name, status, started_at, rows_processed
        """, export_id, export["name"], total_rows)

        return {
            "status": "completed",
            "execution": dict(row),
            "summary": {
                "export_name": export["name"],
                "target_system": export["target_system"],
                "format": export["format"],
                "tables_exported": export["source_tables"],
                "total_rows": total_rows,
                "deidentified": export.get("deidentify", False),
            }
        }
    finally:
        await conn.close()


@router.get("/exports/{export_id}/preview")
async def preview_export(export_id: str):
    """Export Pipeline 미리보기 (샘플 10행)"""
    all_exports = builtin_export_pipelines() + load_custom_exports()
    export = next((e for e in all_exports if e["id"] == export_id), None)
    if not export:
        raise HTTPException(status_code=404, detail=f"Export '{export_id}' 없음")

    conn = await get_conn()
    try:
        previews = {}
        for tbl in export.get("source_tables", [])[:3]:  # 최대 3 테이블
            try:
                rows = await conn.fetch(f"SELECT * FROM {tbl} LIMIT 10")  # noqa: S608
                data = []
                for r in rows:
                    d = dict(r)
                    for k, v in d.items():
                        if isinstance(v, datetime):
                            d[k] = v.isoformat()
                        elif hasattr(v, 'isoformat'):
                            d[k] = str(v)
                    data.append(d)
                previews[tbl] = data
            except Exception:
                previews[tbl] = []
        return {"export_name": export["name"], "format": export["format"], "previews": previews}
    finally:
        await conn.close()


# ════════════════════════════════════════════════════════════
#  D. 대시보드
# ════════════════════════════════════════════════════════════

@router.get("/dashboard")
async def pipeline_dashboard():
    """Pipeline 대시보드 통계"""
    conn = await get_conn()
    try:
        await ensure_tables(conn)
        await seed_query_history(conn)
        await seed_executions(conn)

        lz_count = len(builtin_lz_templates()) + len(load_custom_templates())
        export_count = len(builtin_export_pipelines()) + len(load_custom_exports())
        mart_count = await conn.fetchval("SELECT COUNT(*) FROM pipeline_mart_design")

        # 24시간 처리 건수
        recent_24h = await conn.fetchval("""
            SELECT COUNT(*) FROM pipeline_execution WHERE started_at >= NOW() - INTERVAL '24 hours'
        """)
        recent_rows = await conn.fetchval("""
            SELECT COALESCE(SUM(rows_processed), 0) FROM pipeline_execution WHERE started_at >= NOW() - INTERVAL '24 hours'
        """)

        # 상태별 카운트
        status_counts = await conn.fetch("""
            SELECT status, COUNT(*) AS cnt FROM pipeline_execution GROUP BY status
        """)
        statuses = {r["status"]: r["cnt"] for r in status_counts}

        return {
            "landing_zone_templates": lz_count,
            "export_pipelines": export_count,
            "mart_designs": mart_count,
            "executions_24h": recent_24h,
            "rows_processed_24h": recent_rows,
            "status_summary": statuses,
        }
    finally:
        await conn.close()


@router.get("/executions")
async def list_executions(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    pipeline_type: Optional[str] = None,
):
    """Pipeline 실행 이력"""
    conn = await get_conn()
    try:
        await ensure_tables(conn)
        await seed_executions(conn)

        where = ""
        params: list = []
        if pipeline_type:
            where = "WHERE pipeline_type = $1"
            params.append(pipeline_type)

        total = await conn.fetchval(f"SELECT COUNT(*) FROM pipeline_execution {where}", *params)

        offset_param = f"${len(params) + 1}"
        limit_param = f"${len(params) + 2}"
        rows = await conn.fetch(
            f"SELECT * FROM pipeline_execution {where} ORDER BY started_at DESC LIMIT {limit_param} OFFSET {offset_param}",
            *params, size, (page - 1) * size
        )
        items = []
        for r in rows:
            d = dict(r)
            for k, v in d.items():
                if isinstance(v, datetime):
                    d[k] = v.isoformat()
            items.append(d)
        return {"items": items, "total": total, "page": page, "size": size}
    finally:
        await conn.close()
