"""Superset proxy - fetch stats from Superset DB directly."""
from fastapi import APIRouter, HTTPException
import psycopg2

router = APIRouter(prefix="/superset", tags=["superset"])

SUPERSET_DB = "postgresql://superset:superset@localhost:15432/superset"


@router.get("/stats")
async def get_stats():
    """Superset 차트/대시보드/데이터셋 건수 조회"""
    conn = None
    try:
        # psycopg2 동기 방식 사용 (asyncpg는 superset-db와 호환 이슈)
        conn = psycopg2.connect(
            host="localhost",
            port=15432,
            user="superset",
            password="superset",
            database="superset",
            connect_timeout=10
        )
        cursor = conn.cursor()
        cursor.execute("SELECT count(*) FROM slices")
        charts = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM dashboards")
        dashboards = cursor.fetchone()[0]
        cursor.execute("SELECT count(*) FROM tables")
        datasets = cursor.fetchone()[0]
        cursor.close()
        return {"charts": charts, "dashboards": dashboards, "datasets": datasets}
    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Superset DB 연결 실패: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Superset 통계 조회 실패: {str(e)}")
    finally:
        if conn:
            conn.close()
