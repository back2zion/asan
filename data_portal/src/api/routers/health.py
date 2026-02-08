"""
Health Check + Prometheus Metrics API
"""
import time
from fastapi import APIRouter, Request, Response
from datetime import datetime

from prometheus_client import (
    Counter, Histogram, Gauge, Info,
    generate_latest, CONTENT_TYPE_LATEST, CollectorRegistry, REGISTRY,
)

router = APIRouter()

# --- Prometheus 메트릭 정의 ---
REQUEST_COUNT = Counter(
    "idp_http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "idp_http_request_duration_seconds",
    "HTTP request duration",
    ["method", "endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)
ACTIVE_REQUESTS = Gauge(
    "idp_http_active_requests",
    "Currently active HTTP requests",
)
APP_INFO = Info("idp_app", "IDP application info")
APP_INFO.info({"version": "1.0.0", "service": "asan-idp-api"})


@router.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "IDP API",
        "version": "1.0.0",
    }


@router.get("/metrics")
async def prometheus_metrics():
    """Prometheus 메트릭 엔드포인트"""
    return Response(
        content=generate_latest(REGISTRY),
        media_type=CONTENT_TYPE_LATEST,
    )
