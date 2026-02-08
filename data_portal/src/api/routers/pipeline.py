"""
DIR-004: 데이터 Pipeline 구축
- CDW 조회 이력 분석 → 마트 설계 추천
- CDM/PMS/REDCap 등 외부 시스템 Export Pipeline
- CDC 후 Landing Zone 템플릿 기반 취득

Sub-modules:
  - pipeline_lz:      Landing Zone templates + CDW query history + mart design
  - pipeline_export:   Export pipelines + dashboard + execution history
  - pipeline_shared:   Common DB config, helpers, models, built-in templates
"""
from fastapi import APIRouter

from .pipeline_lz import router as lz_router
from .pipeline_export import router as export_router
from .pipeline_dq import router as dq_router

router = APIRouter(prefix="/pipeline", tags=["Pipeline"])
router.include_router(lz_router)
router.include_router(export_router)
router.include_router(dq_router)
