"""
DPR-005: 포털 운영 관리 우산 라우터
"""
from fastapi import APIRouter

from .portal_ops_monitor import router as monitor_router
from .portal_ops_admin import router as admin_router
from .portal_ops_home import router as home_router

router = APIRouter(prefix="/portal-ops", tags=["PortalOps"])
router.include_router(monitor_router)
router.include_router(admin_router)
router.include_router(home_router)
