"""
AI 분석환경 API - Docker 기반 컨테이너 관리 및 리소스 모니터링

Sub-modules:
  - aienv_containers: Container CRUD + stats
  - aienv_resources:  System/GPU monitoring, templates, health, languages
  - aienv_notebooks:  Shared notebooks, workspace sync, permissions, export
  - aienv_projects:   Analysis requests, projects, dataset export
  - aienv_shared:     Common constants, Docker client, path helpers
"""
from fastapi import APIRouter

from .aienv_containers import router as containers_router
from .aienv_resources import router as resources_router
from .aienv_notebooks import router as notebooks_router
from .aienv_projects import router as projects_router

router = APIRouter(prefix="/ai-environment", tags=["AIEnvironment"])
router.include_router(containers_router)
router.include_router(resources_router)
router.include_router(notebooks_router)
router.include_router(projects_router)
