"""
DGR-005: 데이터 보안 관리 구축
- 보안 정책 관리 (테이블+컬럼 단위, 유형/수준/방법)
- 표준 용어 기반 보안 규칙 전파
- Biz 메타데이터 보안 속성 관리
- 재식별 허용 신청 워크플로우
- 사용자 속성 기반 동적 보안 정책
- Row/Column/Cell 3단계 보안
- 접근 감사 로그

Sub-modules:
  - secmgmt_policies: Security Policies, Term Rules, Biz Security Meta
  - secmgmt_access:   Reid Requests, User Attributes, Access Logs, Overview
  - secmgmt_dynamic:  Dynamic Policies, Masking Rules
  - secmgmt_shared:   DB config/helpers, Pydantic models
"""
from fastapi import APIRouter

from .secmgmt_policies import router as policies_router
from .secmgmt_access import router as access_router
from .secmgmt_dynamic import router as dynamic_router

router = APIRouter(prefix="/security-mgmt", tags=["SecurityMgmt"])
router.include_router(policies_router)
router.include_router(access_router)
router.include_router(dynamic_router)
