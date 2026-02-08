"""
ETL Airflow Endpoints — /etl/health, /etl/dags, /etl/dags/{dag_id}/runs, /etl/dags/{dag_id}/trigger
"""
import os
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException

router = APIRouter()

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:18080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")


def _airflow_auth() -> httpx.BasicAuth:
    return httpx.BasicAuth(AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


async def _airflow_get(path: str, params: Optional[dict] = None) -> dict:
    """Airflow REST API GET 호출"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{AIRFLOW_BASE_URL}/api/v1{path}",
                auth=_airflow_auth(),
                params=params,
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Airflow 서버에 연결할 수 없습니다")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))


async def _airflow_post(path: str, json_data: Optional[dict] = None) -> dict:
    """Airflow REST API POST 호출"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                f"{AIRFLOW_BASE_URL}/api/v1{path}",
                auth=_airflow_auth(),
                json=json_data or {},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.ConnectError:
        raise HTTPException(status_code=503, detail="Airflow 서버에 연결할 수 없습니다")
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))


@router.get("/health")
async def etl_health():
    """Airflow 서비스 상태 확인"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                f"{AIRFLOW_BASE_URL}/health",
                auth=_airflow_auth(),
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "status": "healthy",
                "airflow": data,
            }
    except Exception:
        return {
            "status": "unhealthy",
            "airflow": None,
        }


@router.get("/dags")
async def list_dags():
    """DAG 목록 조회 (파이프라인 목록)"""
    data = await _airflow_get("/dags", params={"limit": 100})

    dags = []
    for dag in data.get("dags", []):
        schedule = dag.get("schedule_interval")
        schedule_str = ""
        if schedule:
            if isinstance(schedule, dict):
                schedule_str = schedule.get("value", str(schedule))
            else:
                schedule_str = str(schedule)

        # 최근 실행 정보를 가져오기 위해 dagRuns 조회
        runs_data = await _airflow_get(
            f"/dags/{dag['dag_id']}/dagRuns",
            params={"limit": 5, "order_by": "-start_date"},
        )
        dag_runs = runs_data.get("dag_runs", [])

        # 최근 실행 상태 목록
        recent_states = [r.get("state", "unknown") for r in dag_runs]

        # 최근 실행의 현재 상태
        current_state = recent_states[0] if recent_states else "no_runs"

        # 마지막 실행 시간
        last_run = dag_runs[0].get("start_date") if dag_runs else None

        dags.append({
            "dag_id": dag["dag_id"],
            "description": dag.get("description", ""),
            "owners": dag.get("owners", []),
            "schedule_interval": schedule_str,
            "is_paused": dag.get("is_paused", False),
            "is_active": dag.get("is_active", True),
            "tags": [t["name"] for t in dag.get("tags", [])],
            "next_dagrun": dag.get("next_dagrun"),
            "status": current_state,
            "last_run": last_run,
            "recent_runs": recent_states,
        })

    return {
        "success": True,
        "dags": dags,
        "total": data.get("total_entries", 0),
    }


@router.get("/dags/{dag_id}/runs")
async def get_dag_runs(dag_id: str, limit: int = 10):
    """특정 DAG의 실행 기록 조회"""
    data = await _airflow_get(
        f"/dags/{dag_id}/dagRuns",
        params={"limit": limit, "order_by": "-start_date"},
    )

    runs = []
    for run in data.get("dag_runs", []):
        start = run.get("start_date")
        end = run.get("end_date")

        runs.append({
            "run_id": run.get("dag_run_id", ""),
            "state": run.get("state", "unknown"),
            "start_date": start,
            "end_date": end,
            "execution_date": run.get("logical_date") or run.get("execution_date"),
            "run_type": run.get("run_type", ""),
            "note": run.get("note"),
        })

    return {
        "success": True,
        "dag_id": dag_id,
        "runs": runs,
        "total": data.get("total_entries", 0),
    }


@router.post("/dags/{dag_id}/trigger")
async def trigger_dag(dag_id: str):
    """DAG 수동 실행 트리거"""
    data = await _airflow_post(
        f"/dags/{dag_id}/dagRuns",
        json_data={"conf": {}, "note": "Triggered from IDP Portal"},
    )

    return {
        "success": True,
        "dag_id": dag_id,
        "run_id": data.get("dag_run_id", ""),
        "state": data.get("state", ""),
        "message": f"{dag_id} 파이프라인 실행이 요청되었습니다.",
    }
