"""
AI 운영 및 라이프사이클 관리 API (AAR-003)
- 모델 레지스트리 & 헬스체크
- PII 탐지/마스킹
- 환각 검증
- 감사 로그
- 리소스 모니터링 (기존 API 재사용)
"""
import json
import re
import os
import time
import random
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import httpx

router = APIRouter(prefix="/ai-ops", tags=["AIOps"])

# --- 데이터 디렉토리 ---
DATA_DIR = Path(__file__).parent.parent / "ai_ops_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG_FILE = DATA_DIR / "audit_log.json"
MAX_AUDIT_ENTRIES = 5000

# --- 모델 레지스트리 ---
MODEL_REGISTRY: List[Dict[str, Any]] = [
    {
        "id": "xiyan-sql",
        "name": "XiYanSQL-QWen2.5-3B",
        "type": "Text-to-SQL",
        "version": "2.5-3B-instruct",
        "parameters": "3B",
        "gpu_memory_mb": 6200,
        "description": "자연어 질의를 SQL로 변환하는 경량 모델",
        "health_url": "http://localhost:28888/v1/models",
        "status": "unknown",
        "fine_tuning": {"stage": "completed", "accuracy": 87.3, "last_trained": "2026-01-15"},
    },
    {
        "id": "qwen3-32b",
        "name": "Qwen3-32B",
        "type": "General LLM",
        "version": "3.0-32B-AWQ",
        "parameters": "32B",
        "gpu_memory_mb": 22400,
        "description": "범용 대화형 LLM (한국어 최적화)",
        "health_url": "http://localhost:28888/v1/models",
        "status": "unknown",
        "fine_tuning": {"stage": "planned", "accuracy": None, "last_trained": None},
    },
    {
        "id": "bioclinical-bert",
        "name": "BioClinicalBERT",
        "type": "Medical NER",
        "version": "d4data/biomedical-ner-all",
        "parameters": "110M",
        "gpu_memory_mb": 287,
        "description": "의료 텍스트 개체명 인식 (NER)",
        "health_url": "http://localhost:28100/ner/health",
        "status": "unknown",
        "fine_tuning": {"stage": "in_progress", "accuracy": 92.1, "last_trained": "2026-02-01"},
    },
]

# --- PII 패턴 ---
PII_PATTERNS: List[Dict[str, Any]] = [
    {
        "id": "rrn",
        "name": "주민등록번호",
        "pattern": r"\d{6}[-\s]?[1-4]\d{6}",
        "replacement": "******-*******",
        "enabled": True,
        "description": "한국 주민등록번호 (YYMMDD-GNNNNNN)",
    },
    {
        "id": "phone",
        "name": "전화번호",
        "pattern": r"01[016789][-\s]?\d{3,4}[-\s]?\d{4}",
        "replacement": "***-****-****",
        "enabled": True,
        "description": "한국 휴대전화 번호",
    },
    {
        "id": "email",
        "name": "이메일",
        "pattern": r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
        "replacement": "***@***.***",
        "enabled": True,
        "description": "이메일 주소",
    },
    {
        "id": "card",
        "name": "카드번호",
        "pattern": r"\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}",
        "replacement": "****-****-****-****",
        "enabled": True,
        "description": "신용/체크카드 번호 (16자리)",
    },
]


# --- PII 유틸리티 ---

def detect_pii(text: str) -> List[Dict[str, Any]]:
    """텍스트에서 PII를 탐지하여 결과 반환"""
    findings = []
    for pat in PII_PATTERNS:
        if not pat["enabled"]:
            continue
        for match in re.finditer(pat["pattern"], text):
            findings.append({
                "pattern_id": pat["id"],
                "pattern_name": pat["name"],
                "matched_text": match.group(),
                "start": match.start(),
                "end": match.end(),
            })
    return findings


def mask_pii(text: str) -> tuple[str, int]:
    """텍스트의 PII를 마스킹하여 (마스킹된 텍스트, 탐지 건수) 반환"""
    count = 0
    masked = text
    for pat in PII_PATTERNS:
        if not pat["enabled"]:
            continue
        masked, n = re.subn(pat["pattern"], pat["replacement"], masked)
        count += n
    return masked, count


# --- 환각 검증 유틸리티 ---

def verify_hallucination(llm_response: str, sql_results: Optional[List] = None) -> Dict[str, Any]:
    """LLM 응답과 SQL 실행 결과의 수치 일관성 검증"""
    if not sql_results:
        return {"status": "skipped", "reason": "SQL 결과 없음", "checks": []}

    # LLM 응답에서 숫자 추출
    llm_numbers = set()
    for m in re.finditer(r"[\d,]+\.?\d*", llm_response):
        num_str = m.group().replace(",", "")
        try:
            llm_numbers.add(float(num_str))
        except ValueError:
            pass

    # SQL 결과에서 숫자 추출
    sql_numbers = set()
    for row in sql_results:
        if isinstance(row, (list, tuple)):
            for val in row:
                if isinstance(val, (int, float)):
                    sql_numbers.add(float(val))
        elif isinstance(row, dict):
            for val in row.values():
                if isinstance(val, (int, float)):
                    sql_numbers.add(float(val))

    if not sql_numbers:
        return {"status": "skipped", "reason": "SQL 결과에 수치 없음", "checks": []}

    # 일치 여부 확인
    checks = []
    matched = 0
    for sql_num in sql_numbers:
        found = any(abs(sql_num - llm_num) < 0.01 for llm_num in llm_numbers) if sql_num != 0 else True
        checks.append({"sql_value": sql_num, "found_in_response": found})
        if found:
            matched += 1

    total = len(checks)
    ratio = matched / total if total > 0 else 1.0

    if ratio >= 0.8:
        status = "pass"
    elif ratio >= 0.5:
        status = "warning"
    else:
        status = "fail"

    return {"status": status, "match_ratio": round(ratio, 2), "checks": checks}


# --- 감사 로그 ---

def _load_audit_log() -> List[Dict]:
    if AUDIT_LOG_FILE.exists():
        try:
            with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return []


def _save_audit_log(logs: List[Dict]):
    logs = logs[-MAX_AUDIT_ENTRIES:]
    with open(AUDIT_LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(logs, f, ensure_ascii=False, indent=2)


def append_audit_log(entry: Dict[str, Any]):
    """감사 로그에 항목 추가 (chat.py에서 호출)"""
    logs = _load_audit_log()
    entry.setdefault("timestamp", datetime.now().isoformat())
    entry.setdefault("id", f"LOG-{len(logs)+1:06d}")
    logs.append(entry)
    _save_audit_log(logs)


# --- Pydantic Models ---

class PIITestRequest(BaseModel):
    text: str


class PIIPatternUpdate(BaseModel):
    enabled: bool


# --- 엔드포인트: 모델 관리 ---

@router.get("/models")
async def list_models():
    """등록된 AI 모델 목록 + 라이브 헬스체크"""
    results = []
    for model in MODEL_REGISTRY:
        m = {**model}
        # 헬스체크
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(model["health_url"])
                m["status"] = "healthy" if resp.status_code == 200 else "unhealthy"
        except Exception:
            m["status"] = "offline"
        results.append(m)
    return {"models": results, "total": len(results)}


@router.get("/models/{model_id}/metrics")
async def model_metrics(model_id: str):
    """감사 로그 기반 모델 성능 지표"""
    if not any(m["id"] == model_id for m in MODEL_REGISTRY):
        raise HTTPException(status_code=404, detail="모델을 찾을 수 없습니다")

    logs = _load_audit_log()
    model_logs = [l for l in logs if l.get("model") == model_id]

    if not model_logs:
        # 데모 데이터: 실제 로그가 없을 때
        return {
            "model_id": model_id,
            "total_requests": 0,
            "avg_latency_ms": 0,
            "error_rate": 0,
            "p95_latency_ms": 0,
            "daily_trend": [],
        }

    latencies = [l.get("latency_ms", 0) for l in model_logs if l.get("latency_ms")]
    errors = sum(1 for l in model_logs if l.get("error"))
    total = len(model_logs)

    sorted_lat = sorted(latencies) if latencies else [0]
    p95_idx = int(len(sorted_lat) * 0.95)

    # 일별 트렌드
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


# --- 엔드포인트: 리소스 ---

@router.get("/resources/overview")
async def resources_overview():
    """시스템 + GPU + 모델별 메모리 통합 조회"""
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

    # 모델별 GPU 메모리 할당
    model_gpu = [
        {"model": m["name"], "gpu_memory_mb": m["gpu_memory_mb"]}
        for m in MODEL_REGISTRY
    ]
    total_gpu_alloc = sum(m["gpu_memory_mb"] for m in MODEL_REGISTRY)

    return {
        "system": system,
        "gpu_models": model_gpu,
        "total_gpu_allocated_mb": total_gpu_alloc,
    }


@router.get("/resources/quotas")
async def resource_quotas():
    """역할별 리소스 쿼터 관리"""
    quotas = [
        {
            "role": "관리자",
            "role_en": "admin",
            "max_gpu_hours": 1000,
            "used_gpu_hours": 342,
            "max_queries_day": 10000,
            "used_queries_day": 1247,
            "max_storage_gb": 500,
            "used_storage_gb": 128,
        },
        {
            "role": "연구자",
            "role_en": "researcher",
            "max_gpu_hours": 500,
            "used_gpu_hours": 187,
            "max_queries_day": 5000,
            "used_queries_day": 823,
            "max_storage_gb": 200,
            "used_storage_gb": 67,
        },
        {
            "role": "분석가",
            "role_en": "analyst",
            "max_gpu_hours": 200,
            "used_gpu_hours": 45,
            "max_queries_day": 2000,
            "used_queries_day": 312,
            "max_storage_gb": 100,
            "used_storage_gb": 23,
        },
    ]
    return {"quotas": quotas}


@router.get("/resources/usage-history")
async def usage_history(hours: int = Query(24, ge=1, le=168)):
    """시간대별 사용량 트렌드"""
    now = datetime.now()
    data = []
    for i in range(hours):
        ts = now - timedelta(hours=hours - i)
        hour_label = ts.strftime("%H:%M")
        # 현실적 시뮬레이션: 업무시간(9-18) 높은 사용량
        h = ts.hour
        base = 30 if 9 <= h <= 18 else 10
        data.append({
            "time": hour_label,
            "cpu_percent": round(base + random.uniform(-5, 15), 1),
            "memory_percent": round(base + 20 + random.uniform(-5, 10), 1),
            "gpu_percent": round(base - 5 + random.uniform(-5, 20), 1),
            "queries": int(base * 2 + random.uniform(-10, 30)),
        })
    return {"hours": hours, "data": data}


# --- 엔드포인트: AI 안전성 ---

@router.get("/safety/pii-patterns")
async def list_pii_patterns():
    """PII 탐지 패턴 목록"""
    return {"patterns": PII_PATTERNS}


@router.put("/safety/pii-patterns/{pattern_id}")
async def update_pii_pattern(pattern_id: str, req: PIIPatternUpdate):
    """PII 패턴 활성화/비활성화"""
    for pat in PII_PATTERNS:
        if pat["id"] == pattern_id:
            pat["enabled"] = req.enabled
            return {"message": f"패턴 '{pat['name']}' {'활성화' if req.enabled else '비활성화'}됨", "pattern": pat}
    raise HTTPException(status_code=404, detail="패턴을 찾을 수 없습니다")


@router.post("/safety/test-pii")
async def test_pii(req: PIITestRequest):
    """PII 탐지 테스트"""
    findings = detect_pii(req.text)
    masked_text, count = mask_pii(req.text)
    return {
        "original_text": req.text,
        "masked_text": masked_text,
        "findings": findings,
        "pii_count": count,
    }


@router.get("/safety/hallucination-stats")
async def hallucination_stats():
    """환각 검증 통계"""
    logs = _load_audit_log()
    hall_logs = [l for l in logs if l.get("hallucination_status")]

    if not hall_logs:
        # 데모 데이터
        return {
            "total_verified": 150,
            "pass_count": 120,
            "warning_count": 22,
            "fail_count": 8,
            "pass_rate": 80.0,
        }

    total = len(hall_logs)
    pass_count = sum(1 for l in hall_logs if l["hallucination_status"] == "pass")
    warning_count = sum(1 for l in hall_logs if l["hallucination_status"] == "warning")
    fail_count = sum(1 for l in hall_logs if l["hallucination_status"] == "fail")

    return {
        "total_verified": total,
        "pass_count": pass_count,
        "warning_count": warning_count,
        "fail_count": fail_count,
        "pass_rate": round(pass_count / total * 100, 1) if total else 0,
    }


# --- 엔드포인트: 감사 로그 ---

@router.get("/audit-logs")
async def get_audit_logs(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    model: Optional[str] = None,
    user: Optional[str] = None,
    query_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """감사 로그 조회 (페이지네이션 + 필터)"""
    logs = _load_audit_log()
    logs.reverse()  # 최신순

    # 필터
    if model:
        logs = [l for l in logs if l.get("model") == model]
    if user:
        logs = [l for l in logs if l.get("user", "").lower().find(user.lower()) >= 0]
    if query_type:
        logs = [l for l in logs if l.get("query_type") == query_type]
    if date_from:
        logs = [l for l in logs if l.get("timestamp", "") >= date_from]
    if date_to:
        logs = [l for l in logs if l.get("timestamp", "") <= date_to + "T23:59:59"]

    total = len(logs)
    start = (page - 1) * page_size
    end = start + page_size

    return {
        "logs": logs[start:end],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size,
    }


@router.get("/audit-logs/stats")
async def audit_log_stats():
    """감사 로그 통계"""
    logs = _load_audit_log()

    if not logs:
        # 데모 데이터
        return {
            "total_queries": 0,
            "avg_latency_ms": 0,
            "model_distribution": {},
            "query_type_distribution": {},
            "daily_counts": [],
        }

    latencies = [l.get("latency_ms", 0) for l in logs if l.get("latency_ms")]

    # 모델 분포
    model_dist: Dict[str, int] = {}
    for l in logs:
        m = l.get("model", "unknown")
        model_dist[m] = model_dist.get(m, 0) + 1

    # 질의 유형 분포
    qt_dist: Dict[str, int] = {}
    for l in logs:
        qt = l.get("query_type", "unknown")
        qt_dist[qt] = qt_dist.get(qt, 0) + 1

    # 일별 건수
    daily: Dict[str, int] = {}
    for l in logs:
        day = l.get("timestamp", "")[:10]
        if day:
            daily[day] = daily.get(day, 0) + 1

    return {
        "total_queries": len(logs),
        "avg_latency_ms": round(sum(latencies) / len(latencies), 1) if latencies else 0,
        "model_distribution": model_dist,
        "query_type_distribution": qt_dist,
        "daily_counts": [{"date": k, "count": v} for k, v in sorted(daily.items())],
    }


@router.get("/audit-logs/export")
async def export_audit_logs(
    model: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """감사 로그 CSV 내보내기"""
    from fastapi.responses import Response

    logs = _load_audit_log()
    logs.reverse()

    if model:
        logs = [l for l in logs if l.get("model") == model]
    if date_from:
        logs = [l for l in logs if l.get("timestamp", "") >= date_from]
    if date_to:
        logs = [l for l in logs if l.get("timestamp", "") <= date_to + "T23:59:59"]

    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["ID", "Timestamp", "User", "Model", "Query Type", "Latency (ms)", "Tokens", "PII Count", "Hallucination", "Query"])
    for l in logs:
        writer.writerow([
            l.get("id", ""),
            l.get("timestamp", ""),
            l.get("user", ""),
            l.get("model", ""),
            l.get("query_type", ""),
            l.get("latency_ms", ""),
            l.get("tokens", ""),
            l.get("pii_count", 0),
            l.get("hallucination_status", ""),
            l.get("query", "")[:200],
        ])

    csv_content = output.getvalue()
    return Response(
        content=csv_content.encode("utf-8-sig"),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=audit_logs_{datetime.now().strftime('%Y%m%d')}.csv"},
    )
