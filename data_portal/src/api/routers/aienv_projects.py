"""
AI 분석환경 API - 분석 요청, 프로젝트별 자원 관리, 데이터셋 이관
"""
import csv
import json
import re
from typing import Optional
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Header
from pydantic import BaseModel
import docker
import psycopg2

from .aienv_shared import (
    logger,
    get_docker_client,
    NOTEBOOKS_DIR,
    JUPYTER_WORKSPACE,
)

router = APIRouter()


# --- Pydantic Models ---

class AnalysisRequestCreate(BaseModel):
    title: str
    description: str
    requester: Optional[str] = "anonymous"
    priority: Optional[str] = "medium"


class AnalysisRequestUpdate(BaseModel):
    status: Optional[str] = None
    response: Optional[str] = None
    assignee: Optional[str] = None
    result_notebook: Optional[str] = None


class ProjectCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    owner: Optional[str] = "anonymous"
    cpu_quota: Optional[float] = 4.0
    memory_quota_gb: Optional[float] = 8.0
    storage_quota_gb: Optional[float] = 50.0


class ProjectUpdate(BaseModel):
    description: Optional[str] = None
    cpu_quota: Optional[float] = None
    memory_quota_gb: Optional[float] = None
    storage_quota_gb: Optional[float] = None
    status: Optional[str] = None


class DatasetExportRequest(BaseModel):
    table_name: str
    limit: Optional[int] = 10000
    columns: Optional[list[str]] = None


# --- 분석 요청 파일 관리 ---

REQUESTS_FILE = NOTEBOOKS_DIR / ".requests.json"


def _load_requests() -> list:
    """분석 요청 목록 로드"""
    if REQUESTS_FILE.exists():
        try:
            with open(REQUESTS_FILE, "r", encoding="utf-8") as fp:
                return json.load(fp)
        except (json.JSONDecodeError, OSError):
            pass
    return []


def _save_requests(requests: list):
    """분석 요청 목록 저장"""
    NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)
    with open(REQUESTS_FILE, "w", encoding="utf-8") as fp:
        json.dump(requests, fp, ensure_ascii=False, indent=2)


# --- 프로젝트 파일 관리 ---

PROJECTS_FILE = NOTEBOOKS_DIR / ".projects.json"


def _load_projects() -> list:
    if PROJECTS_FILE.exists():
        try:
            with open(PROJECTS_FILE, "r", encoding="utf-8") as fp:
                return json.load(fp)
        except (json.JSONDecodeError, OSError):
            pass
    return []


def _save_projects(projects: list):
    NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)
    with open(PROJECTS_FILE, "w", encoding="utf-8") as fp:
        json.dump(projects, fp, ensure_ascii=False, indent=2)


# --- 데이터셋 이관 ---

OMOP_DB_CONFIG = {
    "host": "localhost",
    "port": 5436,
    "dbname": "omop_cdm",
    "user": "omopuser",
    "password": "omop",
}

DATASETS_DIR = JUPYTER_WORKSPACE / "datasets"

OMOP_TABLE_DESCRIPTIONS = {
    "person": "환자 기본 인구통계 정보",
    "visit_occurrence": "방문(입원/외래/응급) 기록",
    "condition_occurrence": "진단/상병 기록",
    "drug_exposure": "약물 처방/투약 기록",
    "procedure_occurrence": "시술/수술 기록",
    "measurement": "검사 결과 (검체/생체 측정)",
    "observation": "관찰 기록 (증상/징후/가족력 등)",
    "observation_period": "환자별 관찰 기간",
    "condition_era": "진단 에피소드 (연속 진단 기간)",
    "drug_era": "약물 에피소드 (연속 투약 기간)",
    "cost": "의료비용 정보",
    "payer_plan_period": "보험 가입 기간",
    "death": "사망 기록",
    "specimen": "검체 정보",
    "device_exposure": "의료기기 사용 기록",
    "note": "임상 노트/문서",
    "care_site": "진료 장소 정보",
    "provider": "의료 제공자 정보",
    "location": "위치 정보",
}

EXPORT_HISTORY_FILE = NOTEBOOKS_DIR / ".export_history.json"


def _load_export_history() -> list:
    """데이터셋 이관 이력 로드"""
    if EXPORT_HISTORY_FILE.exists():
        try:
            with open(EXPORT_HISTORY_FILE, "r", encoding="utf-8") as fp:
                return json.load(fp)
        except (json.JSONDecodeError, OSError):
            pass
    return []


def _save_export_history(history: list):
    """데이터셋 이관 이력 저장 (최근 200건)"""
    history = history[-200:]
    NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)
    with open(EXPORT_HISTORY_FILE, "w", encoding="utf-8") as fp:
        json.dump(history, fp, ensure_ascii=False, indent=2)


# =============================================
# 업무 요청/결과 전달
# =============================================

@router.get("/shared/requests")
async def list_requests():
    """분석 업무 요청 목록"""
    requests = _load_requests()
    requests_copy = list(reversed(requests))
    return {"requests": requests_copy, "total": len(requests_copy)}


@router.post("/shared/requests")
async def create_request(
    req: AnalysisRequestCreate,
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
):
    """분석 업무 요청 등록"""
    requests = _load_requests()
    new_id = f"REQ-{len(requests) + 1:04d}"
    new_req = {
        "id": new_id,
        "title": req.title,
        "description": req.description,
        "requester": req.requester or x_user_name,
        "priority": req.priority or "medium",
        "status": "pending",
        "assignee": "",
        "response": "",
        "result_notebook": "",
        "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    requests.append(new_req)
    _save_requests(requests)
    return {"message": "분석 요청이 등록되었습니다", "request": new_req}


@router.put("/shared/requests/{request_id}")
async def update_request(
    request_id: str,
    req: AnalysisRequestUpdate,
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
):
    """분석 업무 요청 상태/결과 업데이트"""
    requests = _load_requests()
    target = None
    for r in requests:
        if r["id"] == request_id:
            target = r
            break
    if not target:
        raise HTTPException(status_code=404, detail="요청을 찾을 수 없습니다")

    if req.status is not None:
        if req.status not in ("pending", "in_progress", "completed", "rejected"):
            raise HTTPException(status_code=400, detail="유효하지 않은 상태입니다")
        target["status"] = req.status
    if req.response is not None:
        target["response"] = req.response
    if req.assignee is not None:
        target["assignee"] = req.assignee
    if req.result_notebook is not None:
        target["result_notebook"] = req.result_notebook
    target["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    _save_requests(requests)
    return {"message": "요청이 업데이트되었습니다", "request": target}


# =============================================
# 프로젝트별 자원 관리
# =============================================

@router.get("/projects")
async def list_projects():
    """프로젝트별 자원 할당 및 사용현황 목록"""
    projects = _load_projects()
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
        # 실행 중 컨테이너의 할당 자원 합산
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
        # 스토리지 사용량 (프로젝트 디렉토리)
        proj_dir = JUPYTER_WORKSPACE / proj["id"]
        if proj_dir.exists():
            total_size = sum(f.stat().st_size for f in proj_dir.rglob("*") if f.is_file())
            proj["storage_used_gb"] = round(total_size / (1024 ** 3), 2)
        else:
            proj["storage_used_gb"] = 0.0

    return {"projects": projects, "total": len(projects)}


@router.post("/projects")
async def create_project(
    req: ProjectCreate,
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
):
    """분석 프로젝트 생성 (자원 쿼터 할당)"""
    projects = _load_projects()
    proj_id = req.name.lower().replace(" ", "-").replace("_", "-")

    # 중복 확인
    if any(p["id"] == proj_id for p in projects):
        raise HTTPException(status_code=409, detail=f"프로젝트 '{proj_id}'가 이미 존재합니다")

    proj = {
        "id": proj_id,
        "name": req.name,
        "description": req.description or "",
        "owner": req.owner or x_user_name,
        "status": "active",
        "cpu_quota": req.cpu_quota,
        "memory_quota_gb": req.memory_quota_gb,
        "storage_quota_gb": req.storage_quota_gb,
        "created": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    projects.append(proj)
    _save_projects(projects)

    # 프로젝트 디렉토리 생성
    proj_dir = JUPYTER_WORKSPACE / proj_id
    proj_dir.mkdir(parents=True, exist_ok=True)

    return {"message": f"프로젝트 '{req.name}' 생성 완료", "project": proj}


@router.put("/projects/{project_id}")
async def update_project(project_id: str, req: ProjectUpdate):
    """프로젝트 자원 쿼터 변경"""
    projects = _load_projects()
    target = None
    for p in projects:
        if p["id"] == project_id:
            target = p
            break
    if not target:
        raise HTTPException(status_code=404, detail="프로젝트를 찾을 수 없습니다")

    if req.description is not None:
        target["description"] = req.description
    if req.cpu_quota is not None:
        target["cpu_quota"] = req.cpu_quota
    if req.memory_quota_gb is not None:
        target["memory_quota_gb"] = req.memory_quota_gb
    if req.storage_quota_gb is not None:
        target["storage_quota_gb"] = req.storage_quota_gb
    if req.status is not None:
        target["status"] = req.status
    target["updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    _save_projects(projects)
    return {"message": "프로젝트가 업데이트되었습니다", "project": target}


@router.delete("/projects/{project_id}")
async def delete_project(project_id: str):
    """프로젝트 삭제"""
    projects = _load_projects()
    projects = [p for p in projects if p["id"] != project_id]
    _save_projects(projects)
    return {"message": f"프로젝트 '{project_id}' 삭제됨"}


# =============================================
# 데이터셋 이관 (SFR-005)
# =============================================

@router.get("/datasets")
async def list_omop_datasets():
    """OMOP CDM 테이블 목록 (이관 가능 데이터셋)"""
    try:
        conn = psycopg2.connect(**OMOP_DB_CONFIG)
        cur = conn.cursor()
        cur.execute("""
            SELECT relname, n_live_tup
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
            ORDER BY n_live_tup DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        tables = []
        for table, row_count in rows:
            tables.append({
                "table_name": table,
                "row_count": int(row_count),
                "description": OMOP_TABLE_DESCRIPTIONS.get(table, ""),
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
        conn = psycopg2.connect(**OMOP_DB_CONFIG)
        cur = conn.cursor()

        # 테이블 존재 확인
        cur.execute(
            "SELECT 1 FROM pg_stat_user_tables WHERE relname = %s",
            (req.table_name,),
        )
        if not cur.fetchone():
            cur.close()
            conn.close()
            raise HTTPException(
                status_code=404,
                detail=f"테이블 '{req.table_name}'을(를) 찾을 수 없습니다",
            )

        # 컬럼 선택
        if req.columns:
            for col in req.columns:
                if not re.match(r'^[a-z_][a-z0-9_]*$', col):
                    cur.close()
                    conn.close()
                    raise HTTPException(status_code=400, detail=f"잘못된 컬럼명: {col}")
            select_cols = ", ".join(req.columns)
        else:
            select_cols = "*"

        query = f"SELECT {select_cols} FROM {req.table_name} LIMIT %s"
        cur.execute(query, (limit,))

        col_names = [desc[0] for desc in cur.description]
        data_rows = cur.fetchall()

        cur.close()
        conn.close()

        # CSV 저장
        DATASETS_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{req.table_name}_{timestamp}.csv"
        filepath = DATASETS_DIR / filename

        with open(filepath, "w", encoding="utf-8-sig", newline="") as fp:
            writer = csv.writer(fp)
            writer.writerow(col_names)
            for row in data_rows:
                writer.writerow(row)

        file_size = filepath.stat().st_size

        # 이력 기록
        history = _load_export_history()
        history.append({
            "filename": filename,
            "table_name": req.table_name,
            "row_count": len(data_rows),
            "columns": col_names,
            "size_kb": round(file_size / 1024, 1),
            "exported_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "limit": limit,
        })
        _save_export_history(history)

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

    history = _load_export_history()

    return {
        "datasets": datasets,
        "total": len(datasets),
        "history": list(reversed(history[-20:])),
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
