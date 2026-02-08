"""
데이터 거버넌스 API - OMOP CDM 기반 민감도/RBAC/비식별화/리니지/Auto-Tagging/Usage Lineage

Sub-modules:
  - gov_sensitivity: Sensitivity classification, metadata, lineage, auto-tagging, usage lineage
  - gov_rbac:        Role management CRUD
  - gov_deident:     De-identification rules/status/pipeline/monitoring, re-identification
  - gov_standards:   Standard terms dictionary, standard indicators
  - gov_smart:       Smart governance features
  - gov_shared:      Common DB config, Pydantic models, classification rules, utilities
"""
from fastapi import APIRouter

from .gov_sensitivity import router as sensitivity_router
from .gov_rbac import router as rbac_router
from .gov_deident import router as deident_router
from .gov_standards import router as standards_router
from .gov_smart import router as smart_router

router = APIRouter(prefix="/governance", tags=["Governance"])
router.include_router(sensitivity_router)
router.include_router(rbac_router)
router.include_router(deident_router)
router.include_router(standards_router)
router.include_router(smart_router)
