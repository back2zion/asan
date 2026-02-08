"""
코호트 빌더 API — Aggregator Router

Sub-modules:
  - cohort_core:   /count, /execute-flow, /set-operation
  - cohort_review: /drill-down, /patient/{id}/timeline, /summary-stats
  - cohort_shared: Pydantic 모델 + SQL 생성 엔진
"""
from fastapi import APIRouter

from .cohort_core import router as core_router
from .cohort_review import router as review_router
from .cohort_persist import router as persist_router

router = APIRouter(prefix="/cohort", tags=["Cohort"])
router.include_router(core_router)
router.include_router(review_router)
router.include_router(persist_router)
