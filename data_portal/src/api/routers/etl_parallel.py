"""
ETL Parallel Load Endpoints — /etl/parallel-config, /etl/parallel-config/{table_name}
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .etl_shared import load_parallel_config, save_parallel_config

router = APIRouter()


class ParallelTableConfig(BaseModel):
    workers: int
    batch_size: int
    enabled: bool


@router.get("/parallel-config")
async def get_parallel_config():
    """병렬 적재 설정 조회"""
    config = load_parallel_config()
    return config


@router.put("/parallel-config/{table_name}")
async def update_parallel_config(table_name: str, req: ParallelTableConfig):
    """테이블별 병렬 적재 설정 변경"""
    config = load_parallel_config()
    if table_name not in config.get("tables", {}):
        raise HTTPException(status_code=404, detail="테이블 설정을 찾을 수 없습니다")
    tbl = config["tables"][table_name]
    tbl["workers"] = max(1, min(req.workers, 16))
    tbl["batch_size"] = max(1000, min(req.batch_size, 200000))
    tbl["enabled"] = req.enabled
    save_parallel_config(config)
    return {"message": f"{table_name} 병렬 설정 업데이트됨", "config": tbl}
