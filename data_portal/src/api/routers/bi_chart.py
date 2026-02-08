"""
DPR-004: BI Chart & Dashboard Sub-Router
차트 CRUD, 드릴다운, 대시보드 CRUD, 내보내기, 통계
"""
import json
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse

from ._bi_shared import (
    get_connection, bi_init, validate_sql, ensure_limit,
    ChartCreateRequest, ChartUpdateRequest, DrillDownRequest,
    DashboardCreateRequest, DashboardUpdateRequest, ExportRequest,
)

router = APIRouter(tags=["BI-Chart"])


# ── Helper: execute chart SQL ──

async def _execute_chart_sql(conn, sql: str, limit: int = 1000):
    """차트 SQL 실행 후 columns/rows 반환"""
    sql = ensure_limit(sql, limit)
    rows = await conn.fetch(sql)
    if not rows:
        return [], []
    columns = [str(k) for k in rows[0].keys()]
    result_rows = []
    for row in rows:
        r = []
        for c in rows[0].keys():
            val = row[c]
            if hasattr(val, 'isoformat'):
                r.append(val.isoformat())
            elif isinstance(val, (bytes, memoryview)):
                r.append(str(val))
            else:
                r.append(val)
        result_rows.append(r)
    return columns, result_rows


# ── Charts ──

@router.post("/charts")
async def create_chart(body: ChartCreateRequest):
    valid, msg = validate_sql(body.sql_query)
    if not valid:
        raise HTTPException(status_code=400, detail=msg)

    conn = await get_connection()
    try:
        await bi_init(conn)
        row = await conn.fetchrow(
            "INSERT INTO bi_chart (name, chart_type, sql_query, config, description, creator) "
            "VALUES ($1,$2,$3,$4::jsonb,$5,$6) RETURNING chart_id, created_at",
            body.name, body.chart_type, body.sql_query,
            json.dumps(body.config), body.description, body.creator,
        )
        return {"chart_id": row["chart_id"], "created_at": row["created_at"].isoformat()}
    finally:
        await conn.close()


@router.get("/charts")
async def list_charts():
    conn = await get_connection()
    try:
        await bi_init(conn)
        rows = await conn.fetch(
            "SELECT chart_id, name, chart_type, sql_query, config, description, creator, created_at, updated_at "
            "FROM bi_chart ORDER BY updated_at DESC"
        )
        return [
            {
                "chart_id": r["chart_id"],
                "name": r["name"],
                "chart_type": r["chart_type"],
                "sql_query": r["sql_query"],
                "config": r["config"],
                "description": r["description"],
                "creator": r["creator"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.get("/charts/{chart_id}")
async def get_chart(chart_id: int):
    """차트 상세 (데이터 포함)"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        r = await conn.fetchrow("SELECT * FROM bi_chart WHERE chart_id = $1", chart_id)
        if not r:
            raise HTTPException(status_code=404, detail="차트를 찾을 수 없습니다")

        # Execute chart SQL for live data
        columns, rows = [], []
        try:
            columns, rows = await _execute_chart_sql(conn, r["sql_query"])
        except Exception:
            pass

        return {
            "chart_id": r["chart_id"],
            "name": r["name"],
            "chart_type": r["chart_type"],
            "sql_query": r["sql_query"],
            "config": r["config"],
            "description": r["description"],
            "creator": r["creator"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            "data": {"columns": columns, "rows": rows},
        }
    finally:
        await conn.close()


@router.put("/charts/{chart_id}")
async def update_chart(chart_id: int, body: ChartUpdateRequest):
    conn = await get_connection()
    try:
        await bi_init(conn)
        existing = await conn.fetchrow("SELECT * FROM bi_chart WHERE chart_id = $1", chart_id)
        if not existing:
            raise HTTPException(status_code=404, detail="차트를 찾을 수 없습니다")

        if body.sql_query:
            valid, msg = validate_sql(body.sql_query)
            if not valid:
                raise HTTPException(status_code=400, detail=msg)

        await conn.execute(
            "UPDATE bi_chart SET name=COALESCE($2,name), chart_type=COALESCE($3,chart_type), "
            "sql_query=COALESCE($4,sql_query), config=COALESCE($5::jsonb,config), "
            "description=COALESCE($6,description), updated_at=NOW() WHERE chart_id=$1",
            chart_id,
            body.name,
            body.chart_type,
            body.sql_query,
            json.dumps(body.config) if body.config is not None else None,
            body.description,
        )
        return {"updated": True, "chart_id": chart_id}
    finally:
        await conn.close()


@router.delete("/charts/{chart_id}")
async def delete_chart(chart_id: int):
    conn = await get_connection()
    try:
        await bi_init(conn)
        result = await conn.execute("DELETE FROM bi_chart WHERE chart_id = $1", chart_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="차트를 찾을 수 없습니다")
        return {"deleted": True, "chart_id": chart_id}
    finally:
        await conn.close()


@router.get("/charts/{chart_id}/raw-data")
async def chart_raw_data(chart_id: int, limit: int = Query(default=500, ge=1, le=5000)):
    """차트 원본 데이터 역추적"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        r = await conn.fetchrow("SELECT sql_query FROM bi_chart WHERE chart_id = $1", chart_id)
        if not r:
            raise HTTPException(status_code=404, detail="차트를 찾을 수 없습니다")

        columns, rows = await _execute_chart_sql(conn, r["sql_query"], limit)
        return {"chart_id": chart_id, "sql_query": r["sql_query"], "columns": columns, "rows": rows, "row_count": len(rows)}
    finally:
        await conn.close()


@router.post("/charts/{chart_id}/drill-down")
async def chart_drill_down(chart_id: int, body: DrillDownRequest):
    """차트 드릴다운"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        r = await conn.fetchrow("SELECT sql_query FROM bi_chart WHERE chart_id = $1", chart_id)
        if not r:
            raise HTTPException(status_code=404, detail="차트를 찾을 수 없습니다")

        base_sql = r["sql_query"].strip().rstrip(";")
        # Wrap as sub-query and add WHERE for drill-down dimension
        # Sanitize dimension name
        if not body.dimension.isidentifier():
            raise HTTPException(status_code=400, detail="잘못된 차원명")

        drill_sql = f"SELECT * FROM ({base_sql}) AS sub WHERE sub.\"{body.dimension}\" = $1 LIMIT 200"

        start = time.time()
        rows = await conn.fetch(drill_sql, str(body.value))
        elapsed_ms = int((time.time() - start) * 1000)

        if not rows:
            return {"columns": [], "rows": [], "row_count": 0, "execution_time_ms": elapsed_ms}

        columns = [str(k) for k in rows[0].keys()]
        result_rows = []
        for row in rows:
            rr = []
            for c in rows[0].keys():
                val = row[c]
                if hasattr(val, 'isoformat'):
                    rr.append(val.isoformat())
                elif isinstance(val, (bytes, memoryview)):
                    rr.append(str(val))
                else:
                    rr.append(val)
            result_rows.append(rr)

        return {"columns": columns, "rows": result_rows, "row_count": len(result_rows), "execution_time_ms": elapsed_ms}
    finally:
        await conn.close()


# ── Dashboards ──

@router.post("/dashboards")
async def create_dashboard(body: DashboardCreateRequest):
    conn = await get_connection()
    try:
        await bi_init(conn)
        row = await conn.fetchrow(
            "INSERT INTO bi_dashboard (name, description, layout, chart_ids, creator, shared) "
            "VALUES ($1,$2,$3::jsonb,$4::jsonb,$5,$6) RETURNING dashboard_id, created_at",
            body.name, body.description, json.dumps(body.layout),
            json.dumps(body.chart_ids), body.creator, body.shared,
        )
        return {"dashboard_id": row["dashboard_id"], "created_at": row["created_at"].isoformat()}
    finally:
        await conn.close()


@router.get("/dashboards")
async def list_dashboards():
    conn = await get_connection()
    try:
        await bi_init(conn)
        rows = await conn.fetch(
            "SELECT dashboard_id, name, description, layout, chart_ids, creator, shared, created_at, updated_at "
            "FROM bi_dashboard ORDER BY updated_at DESC"
        )
        return [
            {
                "dashboard_id": r["dashboard_id"],
                "name": r["name"],
                "description": r["description"],
                "layout": r["layout"],
                "chart_ids": r["chart_ids"],
                "creator": r["creator"],
                "shared": r["shared"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.get("/dashboards/{dashboard_id}")
async def get_dashboard(dashboard_id: int):
    """대시보드 상세 (차트 데이터 포함)"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        r = await conn.fetchrow("SELECT * FROM bi_dashboard WHERE dashboard_id = $1", dashboard_id)
        if not r:
            raise HTTPException(status_code=404, detail="대시보드를 찾을 수 없습니다")

        chart_ids = r["chart_ids"] if isinstance(r["chart_ids"], list) else json.loads(r["chart_ids"]) if r["chart_ids"] else []
        charts = []
        for cid in chart_ids:
            chart = await conn.fetchrow("SELECT * FROM bi_chart WHERE chart_id = $1", cid)
            if chart:
                columns, rows = [], []
                try:
                    columns, rows = await _execute_chart_sql(conn, chart["sql_query"], 500)
                except Exception:
                    pass
                charts.append({
                    "chart_id": chart["chart_id"],
                    "name": chart["name"],
                    "chart_type": chart["chart_type"],
                    "sql_query": chart["sql_query"],
                    "config": chart["config"],
                    "data": {"columns": columns, "rows": rows},
                })

        return {
            "dashboard_id": r["dashboard_id"],
            "name": r["name"],
            "description": r["description"],
            "layout": r["layout"],
            "chart_ids": chart_ids,
            "charts": charts,
            "creator": r["creator"],
            "shared": r["shared"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
        }
    finally:
        await conn.close()


@router.put("/dashboards/{dashboard_id}")
async def update_dashboard(dashboard_id: int, body: DashboardUpdateRequest):
    conn = await get_connection()
    try:
        await bi_init(conn)
        existing = await conn.fetchrow("SELECT * FROM bi_dashboard WHERE dashboard_id = $1", dashboard_id)
        if not existing:
            raise HTTPException(status_code=404, detail="대시보드를 찾을 수 없습니다")

        await conn.execute(
            "UPDATE bi_dashboard SET name=COALESCE($2,name), description=COALESCE($3,description), "
            "layout=COALESCE($4::jsonb,layout), chart_ids=COALESCE($5::jsonb,chart_ids), "
            "shared=COALESCE($6,shared), updated_at=NOW() WHERE dashboard_id=$1",
            dashboard_id,
            body.name,
            body.description,
            json.dumps(body.layout) if body.layout is not None else None,
            json.dumps(body.chart_ids) if body.chart_ids is not None else None,
            body.shared,
        )
        return {"updated": True, "dashboard_id": dashboard_id}
    finally:
        await conn.close()


@router.delete("/dashboards/{dashboard_id}")
async def delete_dashboard(dashboard_id: int):
    conn = await get_connection()
    try:
        await bi_init(conn)
        result = await conn.execute("DELETE FROM bi_dashboard WHERE dashboard_id = $1", dashboard_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="대시보드를 찾을 수 없습니다")
        return {"deleted": True, "dashboard_id": dashboard_id}
    finally:
        await conn.close()


# ── Export ──

@router.post("/export/{fmt}")
async def export_report(fmt: str, body: ExportRequest):
    """보고서 내보내기 (csv, json)"""
    if fmt not in ("csv", "json"):
        raise HTTPException(status_code=400, detail="지원 형식: csv, json")

    conn = await get_connection()
    try:
        await bi_init(conn)
        all_data = []

        for cid in body.chart_ids:
            chart = await conn.fetchrow("SELECT name, sql_query FROM bi_chart WHERE chart_id = $1", cid)
            if chart:
                columns, rows = await _execute_chart_sql(conn, chart["sql_query"], 5000)
                all_data.append({"chart_name": chart["name"], "columns": columns, "rows": rows})

        if fmt == "json":
            import io
            content = json.dumps({"title": body.title, "charts": all_data}, ensure_ascii=False, indent=2, default=str)
            return StreamingResponse(
                io.BytesIO(content.encode("utf-8")),
                media_type="application/json",
                headers={"Content-Disposition": f'attachment; filename="{body.title}.json"'},
            )
        elif fmt == "csv":
            import io
            output = io.StringIO()
            for chart_data in all_data:
                output.write(f"# {chart_data['chart_name']}\n")
                output.write(",".join(chart_data["columns"]) + "\n")
                for row in chart_data["rows"]:
                    output.write(",".join(str(v) for v in row) + "\n")
                output.write("\n")

            csv_bytes = output.getvalue().encode("utf-8-sig")
            return StreamingResponse(
                io.BytesIO(csv_bytes),
                media_type="text/csv; charset=utf-8",
                headers={"Content-Disposition": f'attachment; filename="{body.title}.csv"'},
            )
    finally:
        await conn.close()


# ── Overview ──

@router.get("/overview")
async def bi_overview():
    """BI 통계 요약"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        chart_count = await conn.fetchval("SELECT COUNT(*) FROM bi_chart")
        dashboard_count = await conn.fetchval("SELECT COUNT(*) FROM bi_dashboard")
        saved_query_count = await conn.fetchval("SELECT COUNT(*) FROM bi_saved_query")
        history_count = await conn.fetchval("SELECT COUNT(*) FROM bi_query_history")

        recent_queries = await conn.fetch(
            "SELECT id, sql_text, status, execution_time_ms, created_at "
            "FROM bi_query_history ORDER BY created_at DESC LIMIT 5"
        )

        return {
            "chart_count": chart_count,
            "dashboard_count": dashboard_count,
            "saved_query_count": saved_query_count,
            "query_history_count": history_count,
            "recent_queries": [
                {
                    "id": r["id"],
                    "sql_text": r["sql_text"][:100] + "..." if len(r["sql_text"]) > 100 else r["sql_text"],
                    "status": r["status"],
                    "execution_time_ms": r["execution_time_ms"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in recent_queries
            ],
        }
    finally:
        await conn.close()
