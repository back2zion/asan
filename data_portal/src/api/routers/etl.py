"""
ETL Pipeline API Router - Airflow REST API Proxy + 이기종 소스/병렬 적재/매핑 자동생성

Sub-modules:
  - etl_shared:     Shared state, DB helpers, registries, models
  - etl_airflow:    Airflow proxy endpoints (health, dags, trigger)
  - etl_sources:    Source connector CRUD + connection test
  - etl_parallel:   Parallel load configuration
  - etl_mapping:    Auto-mapping generation (source -> OMOP CDM)
  - etl_schema:     Schema discovery, versioning, change detection, impact analysis
  - etl_templates:  Ingestion templates + terminology validation

SFR-003 / DIR-002 요구사항:
- Airflow REST API 프록시
- 이기종 소스 커넥터 관리 (DBMS, File, Log) + 동적 CRUD
- 커넥터 타입 레지스트리 (코딩 없이 설정만으로 연결)
- 스키마 자동 탐색 + 버전 관리 + 변경 감지 + 영향도 분석
- 수집 템플릿 자동 생성 + 표준 용어 검증
- 병렬 적재 설정
- 1:1 매핑 자동 생성
"""
from fastapi import APIRouter

from .etl_airflow import router as airflow_router
from .etl_sources import router as sources_router
from .etl_parallel import router as parallel_router
from .etl_mapping import router as mapping_router
from .etl_schema import router as schema_router
from .etl_templates import router as templates_router

router = APIRouter(prefix="/etl", tags=["ETL"])
router.include_router(airflow_router)
router.include_router(sources_router)
router.include_router(parallel_router)
router.include_router(mapping_router)
router.include_router(schema_router)
router.include_router(templates_router)
