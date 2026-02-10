"""
AI 분석환경 API - 분석 요청, 프로젝트별 자원 관리, 데이터셋 이관
PostgreSQL-backed (asyncpg via services.db_pool)
"""
import csv
import re
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException, Header
import docker

from .aienv_shared import (
    logger,
    get_docker_client,
    NOTEBOOKS_DIR,
    JUPYTER_WORKSPACE,
)
from ._aienv_projects_helpers import (
    AnalysisRequestCreate, AnalysisRequestUpdate, ProjectCreate, ProjectUpdate, DatasetExportRequest,
    OMOP_TABLE_DESCRIPTIONS,
    _get_conn, _rel, _ensure_tables, _get_omop_pool,
)

router = APIRouter()

DATASETS_DIR = JUPYTER_WORKSPACE / "datasets"


# =============================================
# 업무 요청/결과 전달 (PostgreSQL)
# =============================================

_projects_cache: dict = {"data": None, "ts": 0}
_PROJECTS_CACHE_TTL = 60


@router.get("/shared/requests")
async def list_requests():
    """분석 업무 요청 목록"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM aienv_analysis_request ORDER BY created_at DESC"
        )
        requests = []
        for r in rows:
            requests.append({
                "id": r["id"],
                "title": r["title"],
                "description": r["description"],
                "requester": r["requester"],
                "priority": r["priority"],
                "status": r["status"],
                "assignee": r["assignee"],
                "response": r["response"],
                "result_notebook": r["result_notebook"],
                "created": r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r["created_at"] else "",
                "updated": r["updated_at"].strftime("%Y-%m-%d %H:%M:%S") if r["updated_at"] else "",
            })
        return {"requests": requests, "total": len(requests)}
    finally:
        await _rel(conn)


@router.post("/shared/requests")
async def create_request(
    req: AnalysisRequestCreate,
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
):
    """분석 업무 요청 등록"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        # 다음 ID 생성
        row = await conn.fetchrow(
            "SELECT COUNT(*) AS cnt FROM aienv_analysis_request"
        )
        next_num = (row["cnt"] or 0) + 1
        new_id = f"REQ-{next_num:04d}"

        requester = req.requester or x_user_name
        now = datetime.now()

        await conn.execute("""
            INSERT INTO aienv_analysis_request
                (id, title, description, requester, priority, status,
                 assignee, response, result_notebook, created_at, updated_at)
            VALUES ($1,$2,$3,$4,$5,'pending','',$6,'',$7,$7)
        """, new_id, req.title, req.description, requester,
            req.priority or "medium", "", now)

        new_req = {
            "id": new_id,
            "title": req.title,
            "description": req.description,
            "requester": requester,
            "priority": req.priority or "medium",
            "status": "pending",
            "assignee": "",
            "response": "",
            "result_notebook": "",
            "created": now.strftime("%Y-%m-%d %H:%M:%S"),
            "updated": now.strftime("%Y-%m-%d %H:%M:%S"),
        }
        return {"message": "분석 요청이 등록되었습니다", "request": new_req}
    finally:
        await _rel(conn)


@router.put("/shared/requests/{request_id}")
async def update_request(
    request_id: str,
    req: AnalysisRequestUpdate,
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
):
    """분석 업무 요청 상태/결과 업데이트"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM aienv_analysis_request WHERE id=$1", request_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="요청을 찾을 수 없습니다")

        sets = []
        params = []
        idx = 1

        if req.status is not None:
            if req.status not in ("pending", "in_progress", "completed", "rejected"):
                raise HTTPException(status_code=400, detail="유효하지 않은 상태입니다")
            sets.append(f"status=${idx}")
            params.append(req.status)
            idx += 1
        if req.response is not None:
            sets.append(f"response=${idx}")
            params.append(req.response)
            idx += 1
        if req.assignee is not None:
            sets.append(f"assignee=${idx}")
            params.append(req.assignee)
            idx += 1
        if req.result_notebook is not None:
            sets.append(f"result_notebook=${idx}")
            params.append(req.result_notebook)
            idx += 1

        if not sets:
            raise HTTPException(status_code=400, detail="업데이트할 필드가 없습니다")

        sets.append(f"updated_at=${idx}")
        params.append(datetime.now())
        idx += 1

        params.append(request_id)
        sql = f"UPDATE aienv_analysis_request SET {', '.join(sets)} WHERE id=${idx} RETURNING *"
        updated = await conn.fetchrow(sql, *params)

        result = {
            "id": updated["id"],
            "title": updated["title"],
            "description": updated["description"],
            "requester": updated["requester"],
            "priority": updated["priority"],
            "status": updated["status"],
            "assignee": updated["assignee"],
            "response": updated["response"],
            "result_notebook": updated["result_notebook"],
            "created": updated["created_at"].strftime("%Y-%m-%d %H:%M:%S") if updated["created_at"] else "",
            "updated": updated["updated_at"].strftime("%Y-%m-%d %H:%M:%S") if updated["updated_at"] else "",
        }
        return {"message": "요청이 업데이트되었습니다", "request": result}
    finally:
        await _rel(conn)


# =============================================
# 프로젝트별 자원 관리 (PostgreSQL)
# =============================================

@router.get("/projects")
async def list_projects():
    """프로젝트별 자원 할당 및 사용현황 목록"""
    now = time.time()
    if _projects_cache["data"] and now - _projects_cache["ts"] < _PROJECTS_CACHE_TTL:
        return _projects_cache["data"]

    await _ensure_tables()
    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM aienv_project ORDER BY created_at DESC"
        )
    finally:
        await _rel(conn)

    projects = []
    for r in rows:
        projects.append({
            "id": r["id"],
            "name": r["name"],
            "description": r["description"],
            "owner": r["owner"],
            "status": r["status"],
            "cpu_quota": r["cpu_quota"],
            "memory_quota_gb": r["memory_quota_gb"],
            "storage_quota_gb": r["storage_quota_gb"],
            "created": r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r["created_at"] else "",
            "updated": r["updated_at"].strftime("%Y-%m-%d %H:%M:%S") if r["updated_at"] else "",
        })

    # 각 프로젝트의 실제 컨테이너 사용량 집계
    try:
        client = get_docker_client()
        containers = client.containers.list(all=True)
    except docker.errors.DockerException:
        containers = []

    for proj in projects:
        proj_containers = [
            c for c in containers
            if c.labels.get("project") == proj["id"]
        ]
        proj["container_count"] = len(proj_containers)
        total_cpu = 0.0
        total_mem_gb = 0.0
        for c in proj_containers:
            hc = c.attrs.get("HostConfig", {})
            cpu_quota = hc.get("CpuQuota", 0)
            cpu_period = hc.get("CpuPeriod", 0)
            if cpu_period and cpu_quota:
                total_cpu += cpu_quota / cpu_period
            mem = hc.get("Memory", 0)
            if mem:
                total_mem_gb += mem / (1024 ** 3)
        proj["cpu_used"] = round(total_cpu, 1)
        proj["memory_used_gb"] = round(total_mem_gb, 1)
        proj_dir = JUPYTER_WORKSPACE / proj["id"]
        if proj_dir.exists():
            total_size = sum(f.stat().st_size for f in proj_dir.rglob("*") if f.is_file())
            proj["storage_used_gb"] = round(total_size / (1024 ** 3), 2)
        else:
            proj["storage_used_gb"] = 0.0

    result = {"projects": projects, "total": len(projects)}
    _projects_cache["data"] = result
    _projects_cache["ts"] = time.time()
    return result


@router.post("/projects")
async def create_project(
    req: ProjectCreate,
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
):
    """분석 프로젝트 생성 (자원 쿼터 할당)"""
    await _ensure_tables()
    proj_id = req.name.lower().replace(" ", "-").replace("_", "-")

    conn = await _get_conn()
    try:
        existing = await conn.fetchrow(
            "SELECT 1 FROM aienv_project WHERE id=$1", proj_id
        )
        if existing:
            raise HTTPException(status_code=409, detail=f"프로젝트 '{proj_id}'가 이미 존재합니다")

        now = datetime.now()
        await conn.execute("""
            INSERT INTO aienv_project
                (id, name, description, owner, status,
                 cpu_quota, memory_quota_gb, storage_quota_gb,
                 created_at, updated_at)
            VALUES ($1,$2,$3,$4,'active',$5,$6,$7,$8,$8)
        """, proj_id, req.name, req.description or "",
            req.owner or x_user_name,
            req.cpu_quota, req.memory_quota_gb, req.storage_quota_gb, now)
    finally:
        await _rel(conn)

    # 프로젝트 디렉토리 생성
    proj_dir = JUPYTER_WORKSPACE / proj_id
    proj_dir.mkdir(parents=True, exist_ok=True)

    _projects_cache["data"] = None  # invalidate cache

    proj = {
        "id": proj_id,
        "name": req.name,
        "description": req.description or "",
        "owner": req.owner or x_user_name,
        "status": "active",
        "cpu_quota": req.cpu_quota,
        "memory_quota_gb": req.memory_quota_gb,
        "storage_quota_gb": req.storage_quota_gb,
        "created": now.strftime("%Y-%m-%d %H:%M:%S"),
        "updated": now.strftime("%Y-%m-%d %H:%M:%S"),
    }
    return {"message": f"프로젝트 '{req.name}' 생성 완료", "project": proj}


@router.put("/projects/{project_id}")
async def update_project(project_id: str, req: ProjectUpdate):
    """프로젝트 자원 쿼터 변경"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        existing = await conn.fetchrow(
            "SELECT 1 FROM aienv_project WHERE id=$1", project_id
        )
        if not existing:
            raise HTTPException(status_code=404, detail="프로젝트를 찾을 수 없습니다")

        sets = []
        params = []
        idx = 1

        if req.description is not None:
            sets.append(f"description=${idx}")
            params.append(req.description)
            idx += 1
        if req.cpu_quota is not None:
            sets.append(f"cpu_quota=${idx}")
            params.append(req.cpu_quota)
            idx += 1
        if req.memory_quota_gb is not None:
            sets.append(f"memory_quota_gb=${idx}")
            params.append(req.memory_quota_gb)
            idx += 1
        if req.storage_quota_gb is not None:
            sets.append(f"storage_quota_gb=${idx}")
            params.append(req.storage_quota_gb)
            idx += 1
        if req.status is not None:
            sets.append(f"status=${idx}")
            params.append(req.status)
            idx += 1

        if not sets:
            raise HTTPException(status_code=400, detail="업데이트할 필드가 없습니다")

        sets.append(f"updated_at=${idx}")
        params.append(datetime.now())
        idx += 1

        params.append(project_id)
        sql = f"UPDATE aienv_project SET {', '.join(sets)} WHERE id=${idx} RETURNING *"
        updated = await conn.fetchrow(sql, *params)

        _projects_cache["data"] = None  # invalidate cache

        result = {
            "id": updated["id"],
            "name": updated["name"],
            "description": updated["description"],
            "owner": updated["owner"],
            "status": updated["status"],
            "cpu_quota": updated["cpu_quota"],
            "memory_quota_gb": updated["memory_quota_gb"],
            "storage_quota_gb": updated["storage_quota_gb"],
            "created": updated["created_at"].strftime("%Y-%m-%d %H:%M:%S") if updated["created_at"] else "",
            "updated": updated["updated_at"].strftime("%Y-%m-%d %H:%M:%S") if updated["updated_at"] else "",
        }
        return {"message": "프로젝트가 업데이트되었습니다", "project": result}
    finally:
        await _rel(conn)


@router.delete("/projects/{project_id}")
async def delete_project(project_id: str):
    """프로젝트 삭제"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        result = await conn.execute(
            "DELETE FROM aienv_project WHERE id=$1", project_id
        )
        _projects_cache["data"] = None  # invalidate cache
        return {"message": f"프로젝트 '{project_id}' 삭제됨"}
    finally:
        await _rel(conn)


# =============================================
# 데이터셋 이관 (SFR-005) — asyncpg
# =============================================

@router.get("/datasets")
async def list_omop_datasets():
    """OMOP CDM 테이블 목록 (이관 가능 데이터셋)"""
    try:
        pool = await _get_omop_pool()
        rows = await pool.fetch("""
            SELECT relname, n_live_tup
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
            ORDER BY n_live_tup DESC
        """)
        tables = []
        for r in rows:
            tables.append({
                "table_name": r["relname"],
                "row_count": int(r["n_live_tup"]),
                "description": OMOP_TABLE_DESCRIPTIONS.get(r["relname"], ""),
            })
        return {"tables": tables, "total": len(tables)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"OMOP CDM 연결 실패: {str(e)}")


@router.post("/datasets/export")
async def export_dataset(req: DatasetExportRequest):
    """OMOP CDM 테이블 데이터를 JupyterLab 워크스페이스로 이관 (CSV)"""
    if not re.match(r'^[a-z_][a-z0-9_]*$', req.table_name):
        raise HTTPException(status_code=400, detail="잘못된 테이블명입니다")

    limit = min(req.limit or 10000, 500000)

    try:
        pool = await _get_omop_pool()

        # 테이블 존재 확인
        exists = await pool.fetchrow(
            "SELECT 1 FROM pg_stat_user_tables WHERE relname = $1",
            req.table_name,
        )
        if not exists:
            raise HTTPException(
                status_code=404,
                detail=f"테이블 '{req.table_name}'을(를) 찾을 수 없습니다",
            )

        # 컬럼 선택
        if req.columns:
            for col in req.columns:
                if not re.match(r'^[a-z_][a-z0-9_]*$', col):
                    raise HTTPException(status_code=400, detail=f"잘못된 컬럼명: {col}")
            select_cols = ", ".join(req.columns)
        else:
            select_cols = "*"

        query = f"SELECT {select_cols} FROM {req.table_name} LIMIT $1"
        data_rows = await pool.fetch(query, limit)

        if not data_rows:
            col_names = req.columns or []
        else:
            col_names = list(data_rows[0].keys())

        # CSV 저장
        DATASETS_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{req.table_name}_{timestamp}.csv"
        filepath = DATASETS_DIR / filename

        with open(filepath, "w", encoding="utf-8-sig", newline="") as fp:
            writer = csv.writer(fp)
            writer.writerow(col_names)
            for row in data_rows:
                writer.writerow([row[c] for c in col_names])

        file_size = filepath.stat().st_size

        # 이력 기록 (PostgreSQL)
        await _ensure_tables()
        conn = await _get_conn()
        try:
            import json as _json
            await conn.execute("""
                INSERT INTO aienv_export_history
                    (filename, table_name, row_count, columns, size_kb, export_limit)
                VALUES ($1, $2, $3, $4::jsonb, $5, $6)
            """, filename, req.table_name, len(data_rows),
                _json.dumps(col_names), round(file_size / 1024, 1), limit)
        finally:
            await _rel(conn)

        return {
            "message": f"'{req.table_name}' 데이터 {len(data_rows)}행을 이관했습니다",
            "filename": filename,
            "path": f"datasets/{filename}",
            "jupyter_url": f"/jupyter/lab/tree/work/datasets/{filename}",
            "row_count": len(data_rows),
            "column_count": len(col_names),
            "columns": col_names,
            "size_kb": round(file_size / 1024, 1),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 이관 실패: {str(e)}")


@router.get("/datasets/exported")
async def list_exported_datasets():
    """이관된 데이터셋 파일 목록"""
    datasets = []
    if DATASETS_DIR.exists():
        for f in DATASETS_DIR.glob("*.csv"):
            try:
                stat = f.stat()
                stem = f.stem
                parts = stem.rsplit("_", 2)
                table_name = parts[0] if len(parts) >= 3 else stem
                datasets.append({
                    "filename": f.name,
                    "table_name": table_name,
                    "size_kb": round(stat.st_size / 1024, 1),
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime(
                        "%Y-%m-%d %H:%M"
                    ),
                    "jupyter_url": f"/jupyter/lab/tree/work/datasets/{f.name}",
                })
            except OSError:
                continue
    datasets.sort(key=lambda x: x["modified"], reverse=True)

    # 이력 from PostgreSQL
    await _ensure_tables()
    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM aienv_export_history ORDER BY exported_at DESC LIMIT 20"
        )
        history = []
        for r in rows:
            history.append({
                "filename": r["filename"],
                "table_name": r["table_name"],
                "row_count": r["row_count"],
                "columns": r["columns"] if isinstance(r["columns"], list) else [],
                "size_kb": r["size_kb"],
                "exported_at": r["exported_at"].strftime("%Y-%m-%d %H:%M:%S") if r["exported_at"] else "",
                "limit": r["export_limit"],
            })
    finally:
        await _rel(conn)

    return {
        "datasets": datasets,
        "total": len(datasets),
        "history": history,
    }


@router.delete("/datasets/{filename}")
async def delete_exported_dataset(filename: str):
    """이관된 데이터셋 삭제"""
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    filepath = DATASETS_DIR / filename
    if not filepath.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다")
    filepath.unlink()
    return {"message": f"'{filename}' 삭제 완료"}
