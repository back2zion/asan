"""
DIR-005: 변경 데이터 캡처(CDC) 관리

Sub-modules:
  - cdc_streams:    커넥터 CRUD, 토픽, 오프셋, Iceberg 싱크, 서비스 제어
  - cdc_monitoring: 서비스 현황, 이벤트 로그, 통계
  - cdc_shared:     DB config, connection, schema, seed data, Pydantic models
"""
from fastapi import APIRouter

from .cdc_streams import router as streams_router
from .cdc_monitoring import router as monitoring_router

router = APIRouter(prefix="/cdc", tags=["CDC"])
router.include_router(streams_router)
router.include_router(monitoring_router)
