"""
DGR-007: 데이터 권한 관리 구축
- 데이터세트 단위 권한 관리 (테이블/컬럼/로우 grant)
- 복수 역할 할당 및 권한 구분 처리
- 역할 파라미터 관리
- AMIS 3.0 EAM 역할 연계
- EDW 통계/재식별 권한 이관
- 권한 감사 로그

Sub-modules:
  - perm_datasets_grants: Datasets, grants, role assignments, role params
  - perm_integration:     EAM mappings, EDW migration, audit, overview
  - permission_mgmt_shared: Pydantic models, DB config, table/seed helpers
"""
from fastapi import APIRouter

from .perm_datasets_grants import router as datasets_grants_router
from .perm_integration import router as integration_router

router = APIRouter(prefix="/permission-mgmt", tags=["PermissionMgmt"])
router.include_router(datasets_grants_router)
router.include_router(integration_router)
