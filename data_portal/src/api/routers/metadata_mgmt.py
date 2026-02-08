"""
DGR-002: 메타데이터 관리 체계 구축

Sub-modules:
  - metamgmt_changes:  Change request workflow + overview
  - metamgmt_quality:  Quality rules engine, check history, dashboard, alerts
  - metamgmt_mappings: Source mappings, compliance rules, pipeline biz metadata
  - metamgmt_shared:   DB config, Pydantic models, table DDL, seed data
"""
from fastapi import APIRouter

from .metamgmt_changes import router as changes_router
from .metamgmt_quality import router as quality_router
from .metamgmt_mappings import router as mappings_router

router = APIRouter(prefix="/metadata-mgmt", tags=["MetadataMgmt"])
router.include_router(changes_router)
router.include_router(quality_router)
router.include_router(mappings_router)
