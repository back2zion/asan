"""
AI Assistant Chat API — Natural Language Interface (PRD AAR-001)

Sub-modules:
  - chat_core:      Main chat endpoint (POST /) + shared state & helpers
  - chat_streaming:  SSE streaming endpoint (POST /stream)
  - chat_helpers:    Session management (list, detail, timeline, restore)
"""
from fastapi import APIRouter

from .chat_core import router as core_router
from .chat_streaming import router as streaming_router
from .chat_helpers import router as helpers_router

router = APIRouter(prefix="/chat", tags=["Chat"])
router.include_router(core_router)
router.include_router(streaming_router)
router.include_router(helpers_router)
