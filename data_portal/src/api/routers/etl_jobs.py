"""
DIR-001: 데이터 취득 요구사항 — Job/Group 관리, 테이블 종속관계, 실행 로그, 알림

Combiner router: merges etl_jobs_core, etl_jobs_deps, etl_jobs_alerts
under the /etl prefix.
"""
from fastapi import APIRouter

from routers.etl_jobs_core import router as core_router
from routers.etl_jobs_deps import router as deps_router
from routers.etl_jobs_alerts import router as alerts_router

router = APIRouter(prefix="/etl", tags=["ETL Jobs"])

router.include_router(core_router)
router.include_router(deps_router)
router.include_router(alerts_router)
