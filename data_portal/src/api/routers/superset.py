"""Superset proxy - fetch stats from Superset DB directly."""
from fastapi import APIRouter
import asyncpg

router = APIRouter(prefix="/api/v1/superset", tags=["superset"])

SUPERSET_DB = "postgresql://superset:superset@localhost:15432/superset"


@router.get("/stats")
async def get_stats():
    """Superset 차트/대시보드/데이터셋 건수 조회"""
    try:
        conn = await asyncpg.connect(SUPERSET_DB)
        try:
            charts = await conn.fetchval("SELECT count(*) FROM slices")
            dashboards = await conn.fetchval("SELECT count(*) FROM dashboards")
            datasets = await conn.fetchval("SELECT count(*) FROM tables")
            return {"charts": charts, "dashboards": dashboards, "datasets": datasets}
        finally:
            await conn.close()
    except Exception as e:
        return {"charts": 0, "dashboards": 0, "datasets": 0, "error": str(e)}
