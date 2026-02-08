"""
DGR-003: 데이터 카탈로그 구축
- 통합 카탈로그 엔트리 (테이블 + API + View + SQL + JSON/XML)
- 오너쉽/공유/권한 관리
- 연관 데이터 관계
- 작업 이력 규격화 (cohort, job, ETL)
- 스키마 변경 자동 동기화
- 출처/사용방법/정책/품질 종합 정보

Sub-modules:
  - data_catalog_entries:   Entry CRUD, overview, schema sync
  - data_catalog_auxiliary: Ownership, relations, history, resources
  - data_catalog_shared:    DB config, models, table setup/seed
"""
from fastapi import APIRouter

from .data_catalog_entries import router as entries_router
from .data_catalog_auxiliary import router as auxiliary_router

router = APIRouter(prefix="/data-catalog", tags=["DataCatalog"])
router.include_router(entries_router)
router.include_router(auxiliary_router)
