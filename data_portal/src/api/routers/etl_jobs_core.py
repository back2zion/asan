"""
ETL Jobs Core: Job Group CRUD + Job CRUD endpoints.
"""
import json
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
import asyncpg

from routers.etl_jobs_shared import (
    get_connection, _ensure_tables, _ensure_seed_data,
    JobGroupCreate, JobCreate, JobUpdate, ReorderBody,
)

router = APIRouter()


# ═══════════════════════════════════════════════════
#  Job Group CRUD
# ═══════════════════════════════════════════════════

@router.get("/job-groups")
async def list_job_groups():
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        rows = await conn.fetch("""
            SELECT g.*, COUNT(j.job_id) AS job_count
            FROM etl_job_group g
            LEFT JOIN etl_job j ON j.group_id = g.group_id
            GROUP BY g.group_id
            ORDER BY g.priority, g.group_id
        """)
        return {
            "groups": [
                {
                    "group_id": r["group_id"],
                    "name": r["name"],
                    "description": r["description"],
                    "priority": r["priority"],
                    "enabled": r["enabled"],
                    "job_count": r["job_count"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.post("/job-groups")
async def create_job_group(body: JobGroupCreate):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        gid = await conn.fetchval("""
            INSERT INTO etl_job_group (name, description, priority, enabled)
            VALUES ($1, $2, $3, $4) RETURNING group_id
        """, body.name, body.description, body.priority, body.enabled)
        return {"success": True, "group_id": gid}
    finally:
        await conn.close()


@router.put("/job-groups/{group_id}")
async def update_job_group(group_id: int, body: JobGroupCreate):
    conn = await get_connection()
    try:
        result = await conn.execute("""
            UPDATE etl_job_group SET name=$1, description=$2, priority=$3, enabled=$4, updated_at=NOW()
            WHERE group_id=$5
        """, body.name, body.description, body.priority, body.enabled, group_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="그룹을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


@router.delete("/job-groups/{group_id}")
async def delete_job_group(group_id: int):
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM etl_job_group WHERE group_id=$1", group_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="그룹을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  Job CRUD
# ═══════════════════════════════════════════════════

@router.get("/jobs")
async def list_jobs(
    group_id: Optional[int] = Query(None),
    job_type: Optional[str] = Query(None),
):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await _ensure_seed_data(conn)
        query = """
            SELECT j.*, g.name AS group_name,
                   (SELECT status FROM etl_execution_log WHERE job_id = j.job_id ORDER BY log_id DESC LIMIT 1) AS last_status,
                   (SELECT started_at FROM etl_execution_log WHERE job_id = j.job_id ORDER BY log_id DESC LIMIT 1) AS last_run
            FROM etl_job j
            JOIN etl_job_group g ON g.group_id = j.group_id
            WHERE 1=1
        """
        params = []
        idx = 1
        if group_id is not None:
            query += f" AND j.group_id = ${idx}"
            params.append(group_id)
            idx += 1
        if job_type:
            query += f" AND j.job_type = ${idx}"
            params.append(job_type)
            idx += 1
        query += " ORDER BY j.group_id, j.sort_order, j.job_id"
        rows = await conn.fetch(query, *params)
        return {
            "jobs": [
                {
                    "job_id": r["job_id"],
                    "group_id": r["group_id"],
                    "group_name": r["group_name"],
                    "name": r["name"],
                    "job_type": r["job_type"],
                    "source_id": r["source_id"],
                    "target_table": r["target_table"],
                    "schedule": r["schedule"],
                    "dag_id": r["dag_id"],
                    "config": json.loads(r["config"]) if isinstance(r["config"], str) else (r["config"] or {}),
                    "enabled": r["enabled"],
                    "sort_order": r["sort_order"],
                    "last_status": r["last_status"],
                    "last_run": r["last_run"].isoformat() if r["last_run"] else None,
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await conn.close()


@router.get("/jobs/{job_id}")
async def get_job(job_id: int):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        r = await conn.fetchrow("SELECT * FROM etl_job WHERE job_id=$1", job_id)
        if not r:
            raise HTTPException(status_code=404, detail="Job을 찾을 수 없습니다")
        recent_logs = await conn.fetch("""
            SELECT log_id, run_id, status, started_at, ended_at, rows_processed, rows_failed
            FROM etl_execution_log WHERE job_id=$1 ORDER BY log_id DESC LIMIT 5
        """, job_id)
        return {
            "job_id": r["job_id"],
            "group_id": r["group_id"],
            "name": r["name"],
            "job_type": r["job_type"],
            "source_id": r["source_id"],
            "target_table": r["target_table"],
            "schedule": r["schedule"],
            "dag_id": r["dag_id"],
            "config": json.loads(r["config"]) if isinstance(r["config"], str) else (r["config"] or {}),
            "enabled": r["enabled"],
            "sort_order": r["sort_order"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            "recent_logs": [
                {
                    "log_id": l["log_id"],
                    "run_id": l["run_id"],
                    "status": l["status"],
                    "started_at": l["started_at"].isoformat() if l["started_at"] else None,
                    "ended_at": l["ended_at"].isoformat() if l["ended_at"] else None,
                    "rows_processed": l["rows_processed"],
                    "rows_failed": l["rows_failed"],
                }
                for l in recent_logs
            ],
        }
    finally:
        await conn.close()


@router.post("/jobs")
async def create_job(body: JobCreate):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        jid = await conn.fetchval("""
            INSERT INTO etl_job (group_id, name, job_type, source_id, target_table, schedule, dag_id, config, enabled, sort_order)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10) RETURNING job_id
        """, body.group_id, body.name, body.job_type, body.source_id, body.target_table,
             body.schedule, body.dag_id, json.dumps(body.config or {}), body.enabled, body.sort_order)
        return {"success": True, "job_id": jid}
    except asyncpg.ForeignKeyViolationError:
        raise HTTPException(status_code=400, detail="존재하지 않는 group_id입니다")
    finally:
        await conn.close()


@router.put("/jobs/{job_id}")
async def update_job(job_id: int, body: JobUpdate):
    conn = await get_connection()
    try:
        existing = await conn.fetchrow("SELECT * FROM etl_job WHERE job_id=$1", job_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Job을 찾을 수 없습니다")
        await conn.execute("""
            UPDATE etl_job SET
                group_id = COALESCE($1, group_id),
                name = COALESCE($2, name),
                job_type = COALESCE($3, job_type),
                source_id = COALESCE($4, source_id),
                target_table = COALESCE($5, target_table),
                schedule = COALESCE($6, schedule),
                dag_id = COALESCE($7, dag_id),
                config = COALESCE($8::jsonb, config),
                enabled = COALESCE($9, enabled),
                sort_order = COALESCE($10, sort_order),
                updated_at = NOW()
            WHERE job_id = $11
        """, body.group_id, body.name, body.job_type, body.source_id, body.target_table,
             body.schedule, body.dag_id,
             json.dumps(body.config) if body.config is not None else None,
             body.enabled, body.sort_order, job_id)
        return {"success": True}
    finally:
        await conn.close()


@router.delete("/jobs/{job_id}")
async def delete_job(job_id: int):
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM etl_job WHERE job_id=$1", job_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Job을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


@router.put("/jobs/{job_id}/reorder")
async def reorder_job(job_id: int, body: ReorderBody):
    conn = await get_connection()
    try:
        result = await conn.execute(
            "UPDATE etl_job SET sort_order=$1, updated_at=NOW() WHERE job_id=$2",
            body.sort_order, job_id
        )
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Job을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


@router.post("/jobs/{job_id}/trigger")
async def trigger_job(job_id: int):
    conn = await get_connection()
    try:
        job = await conn.fetchrow("SELECT * FROM etl_job WHERE job_id=$1", job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job을 찾을 수 없습니다")

        run_id = f"manual-{job_id}-{int(time.time())}"
        log_id = await conn.fetchval("""
            INSERT INTO etl_execution_log (job_id, run_id, status, started_at, rows_processed)
            VALUES ($1, $2, 'running', NOW(), 0) RETURNING log_id
        """, job_id, run_id)

        # Simulate completion after insert (demo mode)
        await conn.execute("""
            UPDATE etl_execution_log SET status='success', ended_at=NOW(), rows_processed=1000
            WHERE log_id=$1
        """, log_id)

        return {"success": True, "message": f"Job '{job['name']}' 실행 트리거됨", "run_id": run_id, "log_id": log_id}
    finally:
        await conn.close()
