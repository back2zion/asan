"""
서울아산병원 IDP - FastAPI Backend Server
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from routers import chat, semantic, vector, mcp, health, text2sql, conversation
from core.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 IDP API Server starting...")
    yield
    # Shutdown
    print("👋 IDP API Server shutting down...")


app = FastAPI(
    title="서울아산병원 IDP API",
    description="통합 데이터 플랫폼 백엔드 API",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(health.router, prefix="/api/v1", tags=["Health"])
app.include_router(chat.router, prefix="/api/v1", tags=["Chat"])
app.include_router(semantic.router, prefix="/api/v1", tags=["Semantic"])
app.include_router(vector.router, prefix="/api/v1", tags=["Vector"])
app.include_router(mcp.router, prefix="/api/v1", tags=["MCP"])
app.include_router(text2sql.router, prefix="/api/v1", tags=["Text2SQL"])
app.include_router(conversation.router, prefix="/api/v1", tags=["Conversation"])


@app.get("/")
async def root():
    return {"message": "서울아산병원 IDP API", "version": "1.0.0"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
