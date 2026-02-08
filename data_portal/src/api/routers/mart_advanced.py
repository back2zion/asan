"""
DIT-002: 데이터 마트 고급 운영
MV 생성/새로고침, 종속성 관리, 스케줄링
"""
import json
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/advanced", tags=["MartAdvanced"])

async def _get_conn():
    from services.db_pool import get_pool
    return await (await get_pool()).acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    await (await get_pool()).release(conn)

_tbl_ok = False

async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS mart_materialization (
            mat_id SERIAL PRIMARY KEY,
            mart_name VARCHAR(200) NOT NULL,
            view_name VARCHAR(100),
            source_sql TEXT,
            status VARCHAR(20) DEFAULT 'pending',
            row_count BIGINT DEFAULT 0,
            duration_ms DOUBLE PRECISION,
            error_message TEXT,
            created_by VARCHAR(50) DEFAULT 'system',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS mart_dependency (
            dep_id SERIAL PRIMARY KEY,
            mart_name VARCHAR(200) NOT NULL,
            depends_on VARCHAR(200) NOT NULL,
            dependency_type VARCHAR(20) DEFAULT 'upstream',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(mart_name, depends_on)
        );
        CREATE TABLE IF NOT EXISTS mart_schedule (
            schedule_id SERIAL PRIMARY KEY,
            mart_name VARCHAR(200) NOT NULL,
            cron_expression VARCHAR(100) NOT NULL DEFAULT '0 2 * * *',
            enabled BOOLEAN DEFAULT TRUE,
            last_run_at TIMESTAMPTZ,
            next_run_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_mart_mat_name ON mart_materialization(mart_name);
        CREATE INDEX IF NOT EXISTS idx_mart_dep_name ON mart_dependency(mart_name);
    """)
    _tbl_ok = True


class MaterializeRequest(BaseModel):
    source_sql: str = Field(..., min_length=10, max_length=10000)
    mart_name: Optional[str] = Field(None, max_length=200)

class ScheduleRequest(BaseModel):
    cron_expression: str = Field(default="0 2 * * *", pattern=r"^[\d\*\/\-\,\s]+$", max_length=100)
    enabled: bool = True

class DependencyRequest(BaseModel):
    depends_on: str = Field(..., max_length=200)
    dependency_type: str = Field(default="upstream", pattern=r"^(upstream|downstream)$")


@router.post("/marts/{mart_id}/materialize")
async def materialize_mart(mart_id: str, body: MaterializeRequest):
    """Materialized View 생성"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        sql_upper = body.source_sql.upper().strip()
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            raise HTTPException(400, "SELECT/WITH 쿼리만 허용됩니다")
        for kw in ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE"]:
            if f" {kw} " in f" {sql_upper} ":
                raise HTTPException(400, f"금지 키워드: {kw}")

        import time
        view_name = f"mv_{mart_id}_{int(time.time())}"
        mart_name = body.mart_name or mart_id
        t0 = time.monotonic()

        try:
            await conn.execute(f"CREATE MATERIALIZED VIEW IF NOT EXISTS {view_name} AS {body.source_sql}")
            row_count = await conn.fetchval(f"SELECT COUNT(*) FROM {view_name}") or 0
            duration = (time.monotonic() - t0) * 1000

            mat_id = await conn.fetchval("""
                INSERT INTO mart_materialization (mart_name, view_name, source_sql, status, row_count, duration_ms)
                VALUES ($1,$2,$3,'completed',$4,$5) RETURNING mat_id
            """, mart_name, view_name, body.source_sql, row_count, duration)

            return {
                "mat_id": mat_id,
                "mart_name": mart_name,
                "view_name": view_name,
                "row_count": row_count,
                "duration_ms": round(duration, 2),
                "status": "completed",
            }
        except Exception as e:
            duration = (time.monotonic() - t0) * 1000
            mat_id = await conn.fetchval("""
                INSERT INTO mart_materialization (mart_name, view_name, source_sql, status, error_message, duration_ms)
                VALUES ($1,$2,$3,'error',$4,$5) RETURNING mat_id
            """, mart_name, view_name, body.source_sql, str(e)[:500], duration)
            raise HTTPException(400, f"MV 생성 실패: {e}")
    finally:
        await _rel(conn)


@router.post("/marts/{mart_id}/refresh")
async def refresh_mart(mart_id: str):
    """Materialized View 새로고침"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        # 최신 MV 조회
        row = await conn.fetchrow("""
            SELECT * FROM mart_materialization
            WHERE mart_name=$1 AND status='completed' AND view_name IS NOT NULL
            ORDER BY created_at DESC LIMIT 1
        """, mart_id)
        if not row:
            raise HTTPException(404, f"마트 '{mart_id}'의 MV를 찾을 수 없습니다")

        import time
        t0 = time.monotonic()
        try:
            await conn.execute(f"REFRESH MATERIALIZED VIEW {row['view_name']}")
            row_count = await conn.fetchval(f"SELECT COUNT(*) FROM {row['view_name']}") or 0
            duration = (time.monotonic() - t0) * 1000

            mat_id = await conn.fetchval("""
                INSERT INTO mart_materialization (mart_name, view_name, source_sql, status, row_count, duration_ms)
                VALUES ($1,$2,$3,'completed',$4,$5) RETURNING mat_id
            """, mart_id, row["view_name"], row["source_sql"], row_count, duration)

            return {"mat_id": mat_id, "view_name": row["view_name"], "row_count": row_count,
                    "duration_ms": round(duration, 2), "status": "refreshed"}
        except Exception as e:
            raise HTTPException(400, f"MV 새로고침 실패: {e}")
    finally:
        await _rel(conn)


@router.get("/marts/{mart_id}/refresh-history")
async def refresh_history(mart_id: str, limit: int = Query(20, le=100)):
    """MV 새로고침 이력"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("""
            SELECT * FROM mart_materialization WHERE mart_name=$1
            ORDER BY created_at DESC LIMIT $2
        """, mart_id, limit)
        return {"mart_name": mart_id, "history": [dict(r) for r in rows], "total": len(rows)}
    finally:
        await _rel(conn)


@router.get("/marts/{mart_id}/dependencies")
async def mart_dependencies(mart_id: str):
    """마트 종속성 (업/다운스트림)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        upstream = await conn.fetch(
            "SELECT * FROM mart_dependency WHERE mart_name=$1 AND dependency_type='upstream'", mart_id)
        downstream = await conn.fetch(
            "SELECT * FROM mart_dependency WHERE depends_on=$1", mart_id)
        return {
            "mart_name": mart_id,
            "upstream": [dict(r) for r in upstream],
            "downstream": [dict(r) for r in downstream],
        }
    finally:
        await _rel(conn)


@router.post("/marts/{mart_id}/dependencies")
async def add_dependency(mart_id: str, body: DependencyRequest):
    """마트 종속성 추가"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        dep_id = await conn.fetchval("""
            INSERT INTO mart_dependency (mart_name, depends_on, dependency_type)
            VALUES ($1,$2,$3)
            ON CONFLICT (mart_name, depends_on) DO UPDATE SET dependency_type=$3
            RETURNING dep_id
        """, mart_id, body.depends_on, body.dependency_type)
        return {"dep_id": dep_id, "mart_name": mart_id, "depends_on": body.depends_on}
    finally:
        await _rel(conn)


@router.post("/marts/{mart_id}/schedule")
async def schedule_mart(mart_id: str, body: ScheduleRequest):
    """자동 새로고침 스케줄"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        sid = await conn.fetchval("""
            INSERT INTO mart_schedule (mart_name, cron_expression, enabled)
            VALUES ($1,$2,$3) RETURNING schedule_id
        """, mart_id, body.cron_expression, body.enabled)
        return {"schedule_id": sid, "mart_name": mart_id, "cron": body.cron_expression, "enabled": body.enabled}
    finally:
        await _rel(conn)


@router.get("/dependency-graph")
async def dependency_graph():
    """전체 마트 종속성 그래프"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        deps = await conn.fetch("SELECT mart_name, depends_on, dependency_type FROM mart_dependency ORDER BY mart_name")
        nodes = set()
        edges = []
        for r in deps:
            nodes.add(r["mart_name"])
            nodes.add(r["depends_on"])
            edges.append({"from": r["depends_on"], "to": r["mart_name"], "type": r["dependency_type"]})

        # 마트별 최신 상태
        mart_status = {}
        for node in nodes:
            latest = await conn.fetchrow("""
                SELECT status, row_count, created_at FROM mart_materialization
                WHERE mart_name=$1 ORDER BY created_at DESC LIMIT 1
            """, node)
            mart_status[node] = dict(latest) if latest else {"status": "unknown"}

        return {
            "nodes": [{"id": n, **mart_status.get(n, {})} for n in sorted(nodes)],
            "edges": edges,
            "total_nodes": len(nodes),
            "total_edges": len(edges),
        }
    finally:
        await _rel(conn)
