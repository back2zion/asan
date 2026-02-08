"""
SER-002: 접근통제 — 인증/인가 합성 라우터
Sub-modules: auth_core (로그인/로그아웃), auth_admin (계정/IP 관리)
"""
from fastapi import APIRouter

from .auth_core import router as core_router
from .auth_admin import router as admin_router

router = APIRouter(prefix="/auth", tags=["Auth"])
router.include_router(core_router)
router.include_router(admin_router)
