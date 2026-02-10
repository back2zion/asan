"""
Semantic Layer API - 데이터 카탈로그, 메타데이터 관리

Sub-modules:
  - semantic_search:   Search, faceted search, translation, SQL context, popular, related
  - semantic_metadata: Table metadata, domains, lineage, quality, tags, usage
"""
from fastapi import APIRouter

from .semantic_search import router as search_router
from .semantic_metadata import router as metadata_router

# Re-export shared constants for backward compatibility
# (e.g. gov_sensitivity.py does `from routers.semantic import SAMPLE_TABLES, TAGS`)
from ._semantic_data import SAMPLE_TABLES, DOMAINS, TAGS  # noqa: F401

router = APIRouter(prefix="/semantic", tags=["Semantic"])
router.include_router(search_router)
router.include_router(metadata_router)
