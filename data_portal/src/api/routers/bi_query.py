"""
DPR-004: BI Query Sub-Router
SQL 실행, 비동기 쿼리, 유효성 검증, 히스토리, 저장 쿼리, 자동완성
"""
import asyncio
import time
import uuid
from typing import Dict, Any

from fastapi import APIRouter, HTTPException, Query

from ._bi_shared import (
    get_connection, bi_init, validate_sql, ensure_limit,
    SqlExecuteRequest, SqlValidateRequest, SaveQueryRequest,
)

router = APIRouter(prefix="/query", tags=["BI-Query"])

# In-memory async job store
_async_jobs: Dict[str, Dict[str, Any]] = {}


@router.post("/execute")
async def execute_sql(body: SqlExecuteRequest):
    """SQL 실행 (동기, LIMIT 자동 적용)"""
    valid, msg = validate_sql(body.sql)
    if not valid:
        raise HTTPException(status_code=400, detail=msg)

    sql = ensure_limit(body.sql, body.limit)
    conn = await get_connection()
    try:
        await bi_init(conn)
        start = time.time()
        rows = await conn.fetch(sql)
        elapsed_ms = int((time.time() - start) * 1000)

        if not rows:
            # Log history
            await conn.execute(
                "INSERT INTO bi_query_history (sql_text, status, row_count, execution_time_ms, columns) VALUES ($1,$2,$3,$4,$5)",
                body.sql, "success", 0, elapsed_ms, [],
            )
            return {"columns": [], "rows": [], "row_count": 0, "execution_time_ms": elapsed_ms}

        columns = [str(k) for k in rows[0].keys()]
        result_rows = [[row[c] for c in rows[0].keys()] for row in rows]

        # Serialize special types
        for i, row in enumerate(result_rows):
            for j, val in enumerate(row):
                if hasattr(val, 'isoformat'):
                    result_rows[i][j] = val.isoformat()
                elif isinstance(val, (bytes, memoryview)):
                    result_rows[i][j] = str(val)

        await conn.execute(
            "INSERT INTO bi_query_history (sql_text, status, row_count, execution_time_ms, columns) VALUES ($1,$2,$3,$4,$5)",
            body.sql, "success", len(result_rows), elapsed_ms, columns,
        )
        return {
            "columns": columns,
            "rows": result_rows,
            "row_count": len(result_rows),
            "execution_time_ms": elapsed_ms,
        }
    except Exception as e:
        err_msg = str(e)
        try:
            await conn.execute(
                "INSERT INTO bi_query_history (sql_text, status, error_message) VALUES ($1,$2,$3)",
                body.sql, "error", err_msg,
            )
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"SQL 실행 오류: {err_msg}")
    finally:
        await conn.close()


@router.post("/execute-async")
async def execute_sql_async(body: SqlExecuteRequest):
    """비동기 쿼리 실행 (job_id 반환)"""
    valid, msg = validate_sql(body.sql)
    if not valid:
        raise HTTPException(status_code=400, detail=msg)

    job_id = str(uuid.uuid4())[:8]
    _async_jobs[job_id] = {"status": "running", "sql": body.sql, "limit": body.limit}

    async def _run():
        try:
            sql = ensure_limit(body.sql, body.limit)
            conn = await get_connection()
            try:
                await bi_init(conn)
                start = time.time()
                rows = await conn.fetch(sql)
                elapsed_ms = int((time.time() - start) * 1000)

                if not rows:
                    _async_jobs[job_id] = {"status": "completed", "columns": [], "rows": [], "row_count": 0, "execution_time_ms": elapsed_ms}
                    return

                columns = [str(k) for k in rows[0].keys()]
                result_rows = [[row[c] for c in rows[0].keys()] for row in rows]
                for i, row in enumerate(result_rows):
                    for j, val in enumerate(row):
                        if hasattr(val, 'isoformat'):
                            result_rows[i][j] = val.isoformat()
                        elif isinstance(val, (bytes, memoryview)):
                            result_rows[i][j] = str(val)

                _async_jobs[job_id] = {
                    "status": "completed",
                    "columns": columns,
                    "rows": result_rows,
                    "row_count": len(result_rows),
                    "execution_time_ms": elapsed_ms,
                }
            finally:
                await conn.close()
        except Exception as e:
            _async_jobs[job_id] = {"status": "error", "error": str(e)}

    asyncio.create_task(_run())
    return {"job_id": job_id, "status": "running"}


@router.get("/status/{job_id}")
async def get_job_status(job_id: str):
    """비동기 쿼리 상태 확인"""
    job = _async_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": job["status"]}


@router.get("/result/{job_id}")
async def get_job_result(job_id: str):
    """비동기 쿼리 결과 조회"""
    job = _async_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job["status"] == "running":
        return {"job_id": job_id, "status": "running"}
    if job["status"] == "error":
        raise HTTPException(status_code=400, detail=job.get("error", "Unknown error"))
    return {
        "job_id": job_id,
        "status": "completed",
        "columns": job.get("columns", []),
        "rows": job.get("rows", []),
        "row_count": job.get("row_count", 0),
        "execution_time_ms": job.get("execution_time_ms", 0),
    }


@router.post("/validate")
async def validate_sql_query(body: SqlValidateRequest):
    """SQL 문법 검증"""
    valid, msg = validate_sql(body.sql)
    return {"valid": valid, "message": msg}


@router.get("/history")
async def get_query_history(limit: int = Query(default=50, ge=1, le=200)):
    """최근 쿼리 실행 이력"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        rows = await conn.fetch(
            "SELECT id, sql_text, status, row_count, execution_time_ms, columns, error_message, created_at "
            "FROM bi_query_history ORDER BY created_at DESC LIMIT $1", limit
        )
        return [
            {
                "id": r["id"],
                "sql_text": r["sql_text"],
                "status": r["status"],
                "row_count": r["row_count"],
                "execution_time_ms": r["execution_time_ms"],
                "columns": r["columns"],
                "error_message": r["error_message"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.post("/save")
async def save_query(body: SaveQueryRequest):
    """쿼리 저장"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        import json
        row = await conn.fetchrow(
            "INSERT INTO bi_saved_query (name, description, sql_text, creator, tags, shared) "
            "VALUES ($1,$2,$3,$4,$5::jsonb,$6) RETURNING query_id, created_at",
            body.name, body.description, body.sql_text, body.creator,
            json.dumps(body.tags), body.shared,
        )
        return {"query_id": row["query_id"], "created_at": row["created_at"].isoformat()}
    finally:
        await conn.close()


@router.get("/saved")
async def list_saved_queries():
    """저장된 쿼리 목록"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        rows = await conn.fetch(
            "SELECT query_id, name, description, sql_text, creator, tags, shared, created_at "
            "FROM bi_saved_query ORDER BY created_at DESC"
        )
        return [
            {
                "query_id": r["query_id"],
                "name": r["name"],
                "description": r["description"],
                "sql_text": r["sql_text"],
                "creator": r["creator"],
                "tags": r["tags"],
                "shared": r["shared"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.delete("/saved/{query_id}")
async def delete_saved_query(query_id: int):
    """저장 쿼리 삭제"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        result = await conn.execute("DELETE FROM bi_saved_query WHERE query_id = $1", query_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="쿼리를 찾을 수 없습니다")
        return {"deleted": True, "query_id": query_id}
    finally:
        await conn.close()


@router.get("/tables")
async def list_tables():
    """OMOP CDM 테이블 목록 (자동완성용)"""
    conn = await get_connection()
    try:
        await bi_init(conn)
        rows = await conn.fetch("""
            SELECT table_name, pg_stat_user_tables.n_live_tup AS row_count
            FROM information_schema.tables
            LEFT JOIN pg_stat_user_tables ON table_name = relname
            WHERE table_schema = 'public'
              AND table_type = 'BASE TABLE'
              AND table_name NOT LIKE 'bi_%'
              AND table_name NOT LIKE 'dm_%'
            ORDER BY table_name
        """)
        return [{"table_name": r["table_name"], "row_count": r["row_count"] or 0} for r in rows]
    finally:
        await conn.close()


@router.get("/columns/{table}")
async def list_columns(table: str):
    """테이블 컬럼 목록 (자동완성용)"""
    # Sanitize table name
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="잘못된 테이블명")

    conn = await get_connection()
    try:
        await bi_init(conn)
        rows = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position
        """, table)
        if not rows:
            raise HTTPException(status_code=404, detail=f"테이블 '{table}'을 찾을 수 없습니다")
        return [
            {"column_name": r["column_name"], "data_type": r["data_type"], "is_nullable": r["is_nullable"]}
            for r in rows
        ]
    finally:
        await conn.close()
