"""
AI 운영 및 라이프사이클 관리 API (AAR-003)
- 모델 레지스트리 & 설정 관리 (JSON 영속화)
- 모델 헬스체크 & 테스트 쿼리
- 리소스 모니터링 (실제 psutil)

안전성(PII/인젝션/환각) → ai_ops_safety.py
감사 로그 → ai_ops_audit.py
AI 실험 → ai_experiment.py
"""
import json
import time
from typing import Dict

from fastapi import APIRouter, HTTPException
import httpx

# ─── 공유 모듈에서 임포트 ────────────────────────────────
from ._ai_ops_shared import (
    # 상수 & 파일 경로
    DATA_DIR,
    AUDIT_LOG_FILE,
    MODEL_CONFIG_FILE,
    PII_PATTERNS_FILE,
    MAX_AUDIT_ENTRIES,
    # JSON 유틸
    _load_json,
    _save_json,
    # 모델 관리
    _DEFAULT_MODELS,
    _load_models,
    _save_models,
    # PII 유틸 (하위 호환)
    detect_pii,
    mask_pii,
    _load_pii_patterns,
    _save_pii_patterns,
    # 환각 검증 (하위 호환)
    verify_hallucination,
    # 감사 로그 (하위 호환)
    _load_audit_log,
    _save_audit_log,
    append_audit_log,
    # Pydantic 모델
    ModelConfigUpdate,
    TestQueryRequest,
)

# ─── 라우터 설정 ─────────────────────────────────────────
router = APIRouter(prefix="/ai-ops", tags=["AIOps"])

# Sub-router: AI 실험 + A/B 테스트
from .ai_experiment import router as experiment_router
router.include_router(experiment_router)

# Sub-router: AI 안전성 (PII, 환각, 인젝션)
from .ai_ops_safety import router as safety_router
router.include_router(safety_router)

# Sub-router: 감사 로그
from .ai_ops_audit import router as audit_router
router.include_router(audit_router)


# ═══════════════════════════════════════════════════════
#  모델 관리 엔드포인트
# ═══════════════════════════════════════════════════════

@router.get("/models")
async def list_models():
    """등록된 AI 모델 목록 + 라이브 헬스체크"""
    models = _load_models()
    results = []
    for model in models:
        m = {**model}
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(model["health_url"])
                m["status"] = "healthy" if resp.status_code == 200 else "unhealthy"
                m["health_detail"] = resp.json() if resp.status_code == 200 else None
        except Exception as e:
            m["status"] = "offline"
            m["health_detail"] = str(e)
        results.append(m)
    return {"models": results, "total": len(results)}


@router.get("/models/{model_id}")
async def get_model(model_id: str):
    """모델 상세 정보"""
    models = _load_models()
    model = next((m for m in models if m["id"] == model_id), None)
    if not model:
        raise HTTPException(status_code=404, detail="모델을 찾을 수 없습니다")
    return model


@router.put("/models/{model_id}")
async def update_model(model_id: str, body: ModelConfigUpdate):
    """모델 설정 수정 (영속화)"""
    models = _load_models()
    model = next((m for m in models if m["id"] == model_id), None)
    if not model:
        raise HTTPException(status_code=404, detail="모델을 찾을 수 없습니다")

    for k, v in body.model_dump(exclude_none=True).items():
        if k == "config" and v:
            model.setdefault("config", {})
            model["config"].update(v)
        else:
            model[k] = v
    _save_models(models)
    return {"success": True, "model": model}


@router.post("/models/{model_id}/test-connection")
async def test_model_connection(model_id: str):
    """모델 엔드포인트 연결 테스트 (실제 HTTP 요청)"""
    models = _load_models()
    model = next((m for m in models if m["id"] == model_id), None)
    if not model:
        raise HTTPException(status_code=404, detail="모델을 찾을 수 없습니다")

    results = {}
    start = time.time()

    # Health check
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(model["health_url"])
            results["health"] = {
                "status": "ok" if resp.status_code == 200 else "error",
                "status_code": resp.status_code,
                "latency_ms": round((time.time() - start) * 1000, 1),
            }
    except Exception as e:
        results["health"] = {
            "status": "unreachable",
            "error": str(e),
            "latency_ms": round((time.time() - start) * 1000, 1),
        }

    # Test endpoint reachability
    test_start = time.time()
    test_url = model.get("test_url", "")
    if test_url:
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.options(test_url)
                results["inference"] = {
                    "status": "reachable",
                    "status_code": resp.status_code,
                    "latency_ms": round((time.time() - test_start) * 1000, 1),
                }
        except Exception as e:
            results["inference"] = {
                "status": "unreachable",
                "error": str(e),
                "latency_ms": round((time.time() - test_start) * 1000, 1),
            }

    total_ms = round((time.time() - start) * 1000, 1)
    all_ok = all(r.get("status") in ("ok", "reachable") for r in results.values())

    return {
        "model_id": model_id,
        "model_name": model["name"],
        "success": all_ok,
        "total_latency_ms": total_ms,
        "results": results,
    }


@router.post("/models/{model_id}/test-query")
async def test_query(model_id: str, req: TestQueryRequest):
    """실제 모델에 테스트 쿼리 전송"""
    models = _load_models()
    model = next((m for m in models if m["id"] == model_id), None)
    if not model:
        raise HTTPException(status_code=404, detail="모델을 찾을 수 없습니다")

    test_url = model.get("test_url", "")
    if not test_url:
        raise HTTPException(status_code=400, detail="테스트 URL이 설정되지 않았습니다")

    config = model.get("config", {})
    start = time.time()

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            if model["id"] == "bioclinical-bert":
                # NER 모델은 다른 API 형식
                resp = await client.post(test_url, json={
                    "text": req.prompt,
                    "language": "auto",
                })
                latency = round((time.time() - start) * 1000, 1)
                if resp.status_code == 200:
                    data = resp.json()
                    return {
                        "success": True,
                        "model_id": model_id,
                        "response": json.dumps(data, ensure_ascii=False, indent=2),
                        "latency_ms": latency,
                        "tokens_used": None,
                    }
                else:
                    return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}", "latency_ms": latency}
            else:
                # OpenAI-compatible chat completions
                payload = {
                    "model": model.get("version", "default"),
                    "messages": [],
                    "max_tokens": req.max_tokens,
                    "temperature": config.get("temperature", 0.7),
                }
                if config.get("system_prompt"):
                    payload["messages"].append({"role": "system", "content": config["system_prompt"]})
                payload["messages"].append({"role": "user", "content": req.prompt})

                resp = await client.post(test_url, json=payload)
                latency = round((time.time() - start) * 1000, 1)

                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
                    usage = data.get("usage", {})
                    return {
                        "success": True,
                        "model_id": model_id,
                        "response": content,
                        "latency_ms": latency,
                        "tokens_used": usage.get("total_tokens"),
                    }
                else:
                    return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:500]}", "latency_ms": latency}
    except Exception as e:
        latency = round((time.time() - start) * 1000, 1)
        return {"success": False, "error": str(e), "latency_ms": latency}


@router.get("/models/{model_id}/metrics")
async def model_metrics(model_id: str):
    """감사 로그 기반 모델 성능 지표"""
    models = _load_models()
    if not any(m["id"] == model_id for m in models):
        raise HTTPException(status_code=404, detail="모델을 찾을 수 없습니다")

    logs = _load_audit_log()
    model_logs = [l for l in logs if l.get("model") == model_id]

    if not model_logs:
        return {
            "model_id": model_id,
            "total_requests": 0,
            "avg_latency_ms": 0,
            "error_rate": 0,
            "p95_latency_ms": 0,
            "daily_trend": [],
            "note": "감사 로그에 데이터가 없습니다. AI 대화를 사용하면 자동으로 기록됩니다.",
        }

    latencies = [l.get("latency_ms", 0) for l in model_logs if l.get("latency_ms")]
    errors = sum(1 for l in model_logs if l.get("error"))
    total = len(model_logs)
    sorted_lat = sorted(latencies) if latencies else [0]
    p95_idx = int(len(sorted_lat) * 0.95)

    daily: Dict[str, int] = {}
    for l in model_logs:
        day = l.get("timestamp", "")[:10]
        if day:
            daily[day] = daily.get(day, 0) + 1

    return {
        "model_id": model_id,
        "total_requests": total,
        "avg_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else 0,
        "error_rate": round(errors / total * 100, 1) if total else 0,
        "p95_latency_ms": round(sorted_lat[p95_idx], 1) if sorted_lat else 0,
        "daily_trend": [{"date": k, "count": v} for k, v in sorted(daily.items())],
    }


# ═══════════════════════════════════════════════════════
#  리소스 모니터링
# ═══════════════════════════════════════════════════════

@router.get("/resources/overview")
async def resources_overview():
    """시스템 + 모델별 메모리 통합 조회 (실제 psutil)"""
    import psutil

    cpu = psutil.cpu_percent(interval=0.3)
    cpu_count = psutil.cpu_count()
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    system = {
        "cpu_percent": cpu,
        "cpu_cores": cpu_count,
        "memory_total_gb": round(mem.total / (1024**3), 1),
        "memory_used_gb": round(mem.used / (1024**3), 1),
        "memory_percent": mem.percent,
        "disk_total_gb": round(disk.total / (1024**3), 1),
        "disk_used_gb": round(disk.used / (1024**3), 1),
        "disk_percent": disk.percent,
    }

    models = _load_models()
    model_gpu = [{"model": m["name"], "gpu_memory_mb": m["gpu_memory_mb"]} for m in models]
    total_gpu_alloc = sum(m["gpu_memory_mb"] for m in models)

    # 실제 프로세스 정보
    top_procs = []
    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
        try:
            info = proc.info
            if info['cpu_percent'] and info['cpu_percent'] > 1.0:
                top_procs.append(info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    top_procs.sort(key=lambda p: p.get('cpu_percent', 0), reverse=True)

    return {
        "system": system,
        "gpu_models": model_gpu,
        "total_gpu_allocated_mb": total_gpu_alloc,
        "top_processes": top_procs[:10],
    }
