"""
DPR-004: BI 우산 라우터
"""
from fastapi import APIRouter

from .bi_query import router as query_router
from .bi_chart import router as chart_router

router = APIRouter(prefix="/bi", tags=["BI"])
router.include_router(query_router)
router.include_router(chart_router)
