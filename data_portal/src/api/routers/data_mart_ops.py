"""
DIT-002: 데이터 마트 생성 및 운영 체계 수립

Sub-modules:
  - mart_ops_core: Mart CRUD, schema changes, dimensions, metrics
  - mart_ops_flow: Flow stages, connection optimizations, overview
  - _mart_ops_shared: DB config, models, table setup, seed data
"""
from fastapi import APIRouter

from .mart_ops_core import router as core_router
from .mart_ops_flow import router as flow_router

router = APIRouter(prefix="/data-mart-ops", tags=["DataMartOps"])
router.include_router(core_router)
router.include_router(flow_router)
