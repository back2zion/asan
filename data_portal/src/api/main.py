"""
서울아산병원 IDP - FastAPI Backend Server
"""
import asyncio
import logging
import time
import threading

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager

from routers import chat, semantic, vector, mcp, health, text2sql, conversation, presentation, imaging, datamart, superset, ner, ai_environment, etl, etl_jobs, governance, ai_ops, migration, schema_monitor, cdc, data_design, pipeline, data_mart_ops, ontology, metadata_mgmt, data_catalog, security_mgmt, permission_mgmt, catalog_ext, catalog_analytics, catalog_recommend, catalog_compose, cohort, bi, portal_ops, ai_architecture, auth, lakehouse, cdc_executor, data_export, fhir, external_api, gov_lineage_ext, mart_recommend, ai_safety, unstructured
from routers.health import REQUEST_COUNT, REQUEST_LATENCY, ACTIVE_REQUESTS
from middleware.csrf import CSRFMiddleware
from middleware.audit import AuditMiddleware
from middleware.security_headers import SecurityHeadersMiddleware
from middleware.rate_limit import RateLimitMiddleware
from core.config import settings

logger = logging.getLogger(__name__)


class MetricsMiddleware(BaseHTTPMiddleware):
    """Prometheus 요청 메트릭 수집 미들웨어"""

    async def dispatch(self, request: Request, call_next):
        if request.url.path == "/api/v1/metrics":
            return await call_next(request)
        ACTIVE_REQUESTS.inc()
        start = time.monotonic()
        try:
            response = await call_next(request)
            endpoint = request.url.path
            REQUEST_COUNT.labels(
                method=request.method,
                endpoint=endpoint,
                status=response.status_code,
            ).inc()
            REQUEST_LATENCY.labels(
                method=request.method,
                endpoint=endpoint,
            ).observe(time.monotonic() - start)
            return response
        finally:
            ACTIVE_REQUESTS.dec()


def _init_rag_background():
    """RAG 파이프라인을 백그라운드 스레드에서 초기화합니다."""
    try:
        from ai_services.rag.retriever import get_retriever
        retriever = get_retriever()
        retriever.initialize()
        logger.info("RAG pipeline initialized successfully")
    except Exception as e:
        logger.warning(f"RAG initialization failed (non-blocking): {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 IDP API Server starting...")

    # DB 연결 풀 초기화
    from services.db_pool import init_pool
    await init_pool()

    # S3 (MinIO) 버킷 초기화
    try:
        from services.s3_service import ensure_buckets
        ensure_buckets()
        logger.info("S3 buckets initialized")
    except Exception as e:
        logger.warning(f"S3 bucket init failed (non-blocking): {e}")

    # RAG 초기화 (별도 스레드, 서버 기동 차단 방지)
    rag_thread = threading.Thread(target=_init_rag_background, daemon=True)
    rag_thread.start()
    logger.info("RAG initialization started in background thread")

    # BizMeta 캐시 워밍 (IT메타 추출 + LLM 비즈메타 생성)
    async def _warm_biz_meta():
        try:
            from services.biz_meta_generator import warm_cache
            await warm_cache()
        except Exception as e:
            logger.warning(f"BizMeta cache warming failed (non-blocking): {e}")

    asyncio.create_task(_warm_biz_meta())
    logger.info("BizMeta cache warming started in background")

    # Ontology cache warming (비동기 백그라운드 — 서버 기동 차단 방지)
    async def _warm_ontology():
        try:
            from routers.ontology import warm_ontology_cache
            await warm_ontology_cache()
        except Exception as e:
            logger.warning(f"Ontology cache warming failed (non-blocking): {e}")

    asyncio.create_task(_warm_ontology())
    logger.info("Ontology cache warming started in background")

    # 느린 엔드포인트 캐시 워밍 (서버 기동 차단 방지)
    async def _warm_slow_endpoints():
        import httpx
        base = "http://127.0.0.1:8000/api/v1"
        endpoints = [
            "/portal-ops/home-dashboard",
            "/etl/dags",
            "/datamart/dashboard-stats",
            "/ai-environment/resources/system",
            "/ai-environment/containers",
            "/ai-environment/resources/gpu",
            "/datamart/cdm-summary",
            "/etl/jobs?limit=10",
            "/etl/logs?limit=10",
        ]
        await asyncio.sleep(3)  # 서버 준비 대기
        async with httpx.AsyncClient(timeout=120) as client:
            for ep in endpoints:
                try:
                    await client.get(f"{base}{ep}")
                    logger.info(f"Cache warmed: {ep}")
                except Exception as e:
                    logger.warning(f"Cache warm failed {ep}: {e}")

    asyncio.create_task(_warm_slow_endpoints())
    logger.info("Slow endpoint cache warming started in background")

    yield
    # Shutdown
    from services.redis_cache import close_redis
    from services.db_pool import close_pool
    await close_redis()
    await close_pool()
    print("👋 IDP API Server shutting down...")


app = FastAPI(
    title="서울아산병원 IDP API",
    description="통합 데이터 플랫폼 백엔드 API",
    version="1.0.0",
    lifespan=lifespan,
    redirect_slashes=False,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# 미들웨어 순서 (역순 등록 — Starlette 스택 규칙)
# 실행 순서: CORS → SecurityHeaders → RateLimit → Audit → CSRF → GZip → Metrics

# CORS 설정 — SER-004: 명시적 origin 목록 사용
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-CSRF-Token"],
    expose_headers=["X-CSRF-Token"],
)

# SER-004: 보안 헤더 미들웨어
app.add_middleware(SecurityHeadersMiddleware)

# SER-004: Rate Limiting 미들웨어
app.add_middleware(RateLimitMiddleware, per_minute=300, per_hour=3000)

# SER-004: 감사 로그 미들웨어
app.add_middleware(AuditMiddleware)

# SER-004: CSRF 보호 미들웨어
app.add_middleware(CSRFMiddleware)

# GZip 압축 미들웨어 (1KB 이상 응답 압축)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Prometheus 메트릭 미들웨어
app.add_middleware(MetricsMiddleware)

# 라우터 등록
app.include_router(health.router, prefix="/api/v1", tags=["Health"])
app.include_router(chat.router, prefix="/api/v1", tags=["Chat"])
app.include_router(semantic.router, prefix="/api/v1", tags=["Semantic"])
app.include_router(vector.router, prefix="/api/v1", tags=["Vector"])
app.include_router(mcp.router, prefix="/api/v1", tags=["MCP"])
app.include_router(text2sql.router, prefix="/api/v1", tags=["Text2SQL"])
app.include_router(conversation.router, prefix="/api/v1", tags=["Conversation"])
app.include_router(presentation.router, prefix="/api/v1", tags=["Presentation"])
app.include_router(imaging.router, prefix="/api/v1", tags=["Imaging"])
app.include_router(datamart.router, prefix="/api/v1", tags=["DataMart"])
app.include_router(superset.router, prefix="/api/v1", tags=["Superset"])
app.include_router(ner.router, prefix="/api/v1", tags=["NER"])
app.include_router(ai_environment.router, prefix="/api/v1", tags=["AIEnvironment"])
app.include_router(etl.router, prefix="/api/v1", tags=["ETL"])
app.include_router(etl_jobs.router, prefix="/api/v1", tags=["ETL Jobs"])
app.include_router(governance.router, prefix="/api/v1", tags=["Governance"])
app.include_router(ai_ops.router, prefix="/api/v1", tags=["AIOps"])
app.include_router(migration.router, prefix="/api/v1", tags=["Migration"])
app.include_router(schema_monitor.router, prefix="/api/v1", tags=["SchemaMonitor"])
app.include_router(cdc.router, prefix="/api/v1", tags=["CDC"])
app.include_router(data_design.router, prefix="/api/v1", tags=["DataDesign"])
app.include_router(pipeline.router, prefix="/api/v1", tags=["Pipeline"])
app.include_router(data_mart_ops.router, prefix="/api/v1", tags=["DataMartOps"])
app.include_router(ontology.router, prefix="/api/v1", tags=["Ontology"])
app.include_router(metadata_mgmt.router, prefix="/api/v1", tags=["MetadataMgmt"])
app.include_router(data_catalog.router, prefix="/api/v1", tags=["DataCatalog"])
app.include_router(security_mgmt.router, prefix="/api/v1", tags=["SecurityMgmt"])
app.include_router(permission_mgmt.router, prefix="/api/v1", tags=["PermissionMgmt"])
app.include_router(catalog_ext.router, prefix="/api/v1", tags=["CatalogExt"])
app.include_router(catalog_analytics.router, prefix="/api/v1", tags=["CatalogAnalytics"])
app.include_router(catalog_recommend.router, prefix="/api/v1", tags=["CatalogRecommend"])
app.include_router(catalog_compose.router, prefix="/api/v1", tags=["CatalogCompose"])
app.include_router(cohort.router, prefix="/api/v1", tags=["Cohort"])
app.include_router(bi.router, prefix="/api/v1", tags=["BI"])
app.include_router(portal_ops.router, prefix="/api/v1", tags=["PortalOps"])
app.include_router(ai_architecture.router, prefix="/api/v1", tags=["AIArchitecture"])
app.include_router(auth.router, prefix="/api/v1", tags=["Auth"])
app.include_router(lakehouse.router, prefix="/api/v1", tags=["Lakehouse"])
app.include_router(cdc_executor.router, prefix="/api/v1", tags=["CDCExecutor"])
app.include_router(data_export.router, prefix="/api/v1", tags=["DataExport"])
app.include_router(fhir.router, prefix="/api/v1", tags=["FHIR"])
app.include_router(external_api.router, prefix="/api/v1", tags=["ExternalAPI"])
app.include_router(gov_lineage_ext.router, prefix="/api/v1", tags=["GovernanceExt"])
app.include_router(mart_recommend.router, prefix="/api/v1", tags=["MartRecommend"])
app.include_router(ai_safety.router, prefix="/api/v1", tags=["AISafety"])
app.include_router(unstructured.router, prefix="/api/v1", tags=["Unstructured"])


@app.get("/")
async def root():
    return {"message": "서울아산병원 IDP API", "version": "1.0.0"}


if __name__ == "__main__":
    import uvicorn
    import os

    if os.getenv("PRODUCTION", "").lower() in ("1", "true"):
        # 프로덕션: 멀티 워커, reload 비활성
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            workers=4,
            timeout_keep_alive=30,
            access_log=True,
        )
    else:
        # 개발: 단일 워커, reload 활성
        uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
