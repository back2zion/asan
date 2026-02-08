"""
AI 운영 및 라이프사이클 관리 API (AAR-003)
- 모델 레지스트리 & 설정 관리 (JSON 영속화)
- 모델 헬스체크 & 테스트 쿼리
- PII 탐지/마스킹 (CRUD + JSON 영속화)
- 프롬프트 인젝션 방어
- 환각 검증
- 감사 로그
- 리소스 모니터링 (실제 psutil)
"""
import json
import re
import os
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from copy import deepcopy

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import httpx

router = APIRouter(prefix="/ai-ops", tags=["AIOps"])

# --- 데이터 디렉토리 ---
DATA_DIR = Path(__file__).parent.parent / "ai_ops_data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_LOG_FILE = DATA_DIR / "audit_log.json"
MODEL_CONFIG_FILE = DATA_DIR / "model_config.json"
PII_PATTERNS_FILE = DATA_DIR / "pii_patterns.json"
MAX_AUDIT_ENTRIES = 5000


# ─── JSON 영속화 유틸 ────────────────────────────────────

def _load_json(path: Path, default):
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return deepcopy(default)


def _save_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── 모델 레지스트리 (영속화) ────────────────────────────

_DEFAULT_MODELS: List[Dict[str, Any]] = [
    {
        "id": "xiyan-sql",
        "name": "XiYanSQL-QWen2.5-3B",
        "type": "Text-to-SQL",
        "version": "2.5-3B-instruct",
        "parameters": "3B",
        "gpu_memory_mb": 6200,
        "description": "자연어 질의를 SQL로 변환하는 경량 모델",
        "health_url": "http://localhost:8001/v1/models",
        "test_url": "http://localhost:8001/v1/chat/completions",
        "config": {
            "temperature": 0.1,
            "max_tokens": 2048,
            "system_prompt": "你是一名PostgreSQL专家，现在需要阅读并理解下面的【数据库schema】描述，然后回答【用户问题】并生成对应的SQL查询语句。\n\n【数据库schema】\n【DB_ID】 omop_cdm\n【표(Table)】 person (person_id BIGINT PK, gender_source_value VARCHAR 'M/F', year_of_birth INT, month_of_birth INT, day_of_birth INT)\n【표(Table)】 condition_occurrence (condition_occurrence_id BIGINT PK, person_id BIGINT FK, condition_concept_id BIGINT, condition_start_date DATE, condition_end_date DATE, condition_source_value VARCHAR 'SNOMED CT코드')\n【표(Table)】 visit_occurrence (visit_occurrence_id BIGINT PK, person_id BIGINT FK, visit_concept_id BIGINT '9201:입원,9202:외래,9203:응급', visit_start_date DATE, visit_end_date DATE)\n【표(Table)】 drug_exposure (drug_exposure_id BIGINT PK, person_id BIGINT FK, drug_concept_id BIGINT, drug_exposure_start_date DATE, drug_source_value VARCHAR)\n【표(Table)】 measurement (measurement_id BIGINT PK, person_id BIGINT FK, measurement_date DATE, value_as_number NUMERIC, measurement_source_value VARCHAR, unit_source_value VARCHAR)\n【표(Table)】 imaging_study (imaging_study_id SERIAL PK, person_id INT FK, finding_labels VARCHAR, image_url VARCHAR)\n【외래키(FK)】 condition_occurrence.person_id = person.person_id\n【외래키(FK)】 visit_occurrence.person_id = person.person_id\n\n【参考信息】\n당뇨=44054006, 고혈압=38341003, 심방세동=49436004, 심근경색=22298006, 뇌졸중=230690007",
        },
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
        "test_url": "http://localhost:28888/v1/chat/completions",
        "config": {
            "temperature": 0.7,
            "max_tokens": 4096,
            "system_prompt": "당신은 서울아산병원 통합 데이터 플랫폼(IDP)의 AI 어시스턴트입니다.\n사용자의 자연어 질문을 SQL로 변환하고 실행하여 결과를 알려줍니다.\n\n## 중요: SQL 생성 규칙\n- 데이터 질문에는 반드시 실행 가능한 PostgreSQL SQL을 ```sql 블록으로 작성하세요\n- SQL은 시스템이 자동 실행하여 결과를 사용자에게 보여줍니다\n- concept 테이블은 존재하지 않습니다. 절대 JOIN하지 마세요\n- 진단 필터링: condition_occurrence.condition_source_value = 'SNOMED코드' 사용\n- 성별 필터링: person.gender_source_value = 'M' 또는 'F' 사용\n- 컬럼 별칭(alias)은 반드시 영문으로 작성하세요 (예: AS patient_count). 한글 별칭 금지\n\n## 데이터베이스 스키마 (OMOP CDM, PostgreSQL)\n### person (환자)\nperson_id BIGINT PK, gender_concept_id BIGINT, year_of_birth INT, month_of_birth INT, day_of_birth INT,\ngender_source_value VARCHAR(50) -- 'M' 또는 'F'\n### condition_occurrence (진단)\ncondition_occurrence_id BIGINT PK, person_id BIGINT FK, condition_concept_id BIGINT,\ncondition_start_date DATE, condition_end_date DATE, condition_source_value VARCHAR(50) -- SNOMED CT 코드\n### visit_occurrence (방문)\nvisit_occurrence_id BIGINT PK, person_id BIGINT FK, visit_concept_id BIGINT,\nvisit_start_date DATE, visit_end_date DATE\n### drug_exposure (약물)\ndrug_exposure_id BIGINT PK, person_id BIGINT FK, drug_concept_id BIGINT,\ndrug_exposure_start_date DATE, drug_exposure_end_date DATE, drug_source_value VARCHAR(100), quantity NUMERIC, days_supply INT\n### measurement (검사)\nmeasurement_id BIGINT PK, person_id BIGINT FK, measurement_concept_id BIGINT,\nmeasurement_date DATE, value_as_number NUMERIC, measurement_source_value VARCHAR(100), unit_source_value VARCHAR(50)\n### imaging_study (흉부X-ray)\nimaging_study_id SERIAL PK, person_id INT FK, image_filename VARCHAR(200),\nfinding_labels VARCHAR(500), view_position VARCHAR(10), patient_age INT, patient_gender VARCHAR(2), image_url VARCHAR(500)\n\n## SNOMED CT 코드 매핑\n당뇨=44054006, 고혈압=38341003, 심방세동=49436004, 심근경색=22298006, 뇌졸중=230690007\n\n## 이미지 표시\nimaging_study 조회 시 마크다운 이미지: ![소견](image_url값)\n\n답변은 항상 한국어로 해주세요.",
        },
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
        "test_url": "http://localhost:28100/ner/analyze",
        "config": {
            "min_confidence": 0.7,
            "max_length": 512,
            "system_prompt": "",
        },
    },
]


def _load_models() -> List[Dict[str, Any]]:
    return _load_json(MODEL_CONFIG_FILE, _DEFAULT_MODELS)


def _save_models(models: List[Dict[str, Any]]):
    _save_json(MODEL_CONFIG_FILE, models)


# ─── PII 패턴 (영속화) ──────────────────────────────────

_DEFAULT_PII_PATTERNS: List[Dict[str, Any]] = [
    {
        "id": "rrn", "name": "주민등록번호",
        "pattern": r"\d{6}[-\s]?[1-4]\d{6}",
        "replacement": "******-*******", "enabled": True,
        "description": "한국 주민등록번호 (YYMMDD-GNNNNNN)",
    },
    {
        "id": "phone", "name": "전화번호",
        "pattern": r"01[016789][-\s]?\d{3,4}[-\s]?\d{4}",
        "replacement": "***-****-****", "enabled": True,
        "description": "한국 휴대전화 번호",
    },
    {
        "id": "email", "name": "이메일",
        "pattern": r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
        "replacement": "***@***.***", "enabled": True,
        "description": "이메일 주소",
    },
    {
        "id": "card", "name": "카드번호",
        "pattern": r"\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}",
        "replacement": "****-****-****-****", "enabled": True,
        "description": "신용/체크카드 번호 (16자리)",
    },
    {
        "id": "ip", "name": "IP 주소",
        "pattern": r"\b(?:\d{1,3}\.){3}\d{1,3}\b",
        "replacement": "***.***.***.***", "enabled": False,
        "description": "IPv4 주소",
    },
    {
        "id": "passport", "name": "여권번호",
        "pattern": r"[A-Z]{1,2}\d{7,8}",
        "replacement": "**********", "enabled": False,
        "description": "한국 여권번호",
    },
]

_pii_next_id = 7


def _load_pii_patterns() -> List[Dict[str, Any]]:
    return _load_json(PII_PATTERNS_FILE, _DEFAULT_PII_PATTERNS)


def _save_pii_patterns(patterns: List[Dict[str, Any]]):
    _save_json(PII_PATTERNS_FILE, patterns)


# ─── PII 유틸리티 ────────────────────────────────────────

def detect_pii(text: str) -> List[Dict[str, Any]]:
    patterns = _load_pii_patterns()
    findings = []
    for pat in patterns:
        if not pat["enabled"]:
            continue
        try:
            for match in re.finditer(pat["pattern"], text):
                findings.append({
                    "pattern_id": pat["id"],
                    "pattern_name": pat["name"],
                    "matched_text": match.group(),
                    "start": match.start(),
                    "end": match.end(),
                })
        except re.error:
            pass
    return findings


def mask_pii(text: str) -> tuple[str, int]:
    patterns = _load_pii_patterns()
    count = 0
    masked = text
    for pat in patterns:
        if not pat["enabled"]:
            continue
        try:
            masked, n = re.subn(pat["pattern"], pat["replacement"], masked)
            count += n
        except re.error:
            pass
    return masked, count


# ─── 환각 검증 유틸리티 ──────────────────────────────────

def verify_hallucination(llm_response: str, sql_results: Optional[List] = None) -> Dict[str, Any]:
    if not sql_results:
        return {"status": "skipped", "reason": "SQL 결과 없음", "checks": []}

    llm_numbers = set()
    for m in re.finditer(r"[\d,]+\.?\d*", llm_response):
        num_str = m.group().replace(",", "")
        try:
            llm_numbers.add(float(num_str))
        except ValueError:
            pass

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

    checks = []
    matched = 0
    for sql_num in sql_numbers:
        found = any(abs(sql_num - llm_num) < 0.01 for llm_num in llm_numbers) if sql_num != 0 else True
        checks.append({"sql_value": sql_num, "found_in_response": found})
        if found:
            matched += 1

    total = len(checks)
    ratio = matched / total if total > 0 else 1.0
    status = "pass" if ratio >= 0.8 else ("warning" if ratio >= 0.5 else "fail")

    return {"status": status, "match_ratio": round(ratio, 2), "checks": checks}


# ─── 감사 로그 ───────────────────────────────────────────

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
    logs = _load_audit_log()
    entry.setdefault("timestamp", datetime.now().isoformat())
    entry.setdefault("id", f"LOG-{len(logs)+1:06d}")
    logs.append(entry)
    _save_audit_log(logs)


# ─── Pydantic Models ────────────────────────────────────

class ModelConfigUpdate(BaseModel):
    health_url: Optional[str] = None
    test_url: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class TestQueryRequest(BaseModel):
    prompt: str
    max_tokens: int = 256


class PIITestRequest(BaseModel):
    text: str


class PIIPatternCreate(BaseModel):
    name: str
    pattern: str
    replacement: str = "***"
    enabled: bool = True
    description: str = ""


class PIIPatternUpdate(BaseModel):
    name: Optional[str] = None
    pattern: Optional[str] = None
    replacement: Optional[str] = None
    enabled: Optional[bool] = None
    description: Optional[str] = None


class PromptInjectionRequest(BaseModel):
    text: str


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
                # Just check if endpoint is reachable (OPTIONS or small POST)
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


# ═══════════════════════════════════════════════════════
#  AI 안전성
# ═══════════════════════════════════════════════════════

@router.get("/safety/pii-patterns")
async def list_pii_patterns():
    """PII 탐지 패턴 목록"""
    return {"patterns": _load_pii_patterns()}


@router.post("/safety/pii-patterns")
async def create_pii_pattern(body: PIIPatternCreate):
    """PII 패턴 추가"""
    global _pii_next_id
    # 정규식 유효성 검사
    try:
        re.compile(body.pattern)
    except re.error as e:
        raise HTTPException(status_code=400, detail=f"잘못된 정규식: {e}")

    patterns = _load_pii_patterns()
    new_pat = {
        "id": f"custom_{_pii_next_id}",
        "name": body.name,
        "pattern": body.pattern,
        "replacement": body.replacement,
        "enabled": body.enabled,
        "description": body.description,
    }
    _pii_next_id += 1
    patterns.append(new_pat)
    _save_pii_patterns(patterns)
    return new_pat


@router.put("/safety/pii-patterns/{pattern_id}")
async def update_pii_pattern(pattern_id: str, body: PIIPatternUpdate):
    """PII 패턴 수정"""
    patterns = _load_pii_patterns()
    pat = next((p for p in patterns if p["id"] == pattern_id), None)
    if not pat:
        raise HTTPException(status_code=404, detail="패턴을 찾을 수 없습니다")

    if body.pattern is not None:
        try:
            re.compile(body.pattern)
        except re.error as e:
            raise HTTPException(status_code=400, detail=f"잘못된 정규식: {e}")

    for k, v in body.model_dump(exclude_none=True).items():
        pat[k] = v
    _save_pii_patterns(patterns)
    return {"message": f"패턴 '{pat['name']}' 수정됨", "pattern": pat}


@router.delete("/safety/pii-patterns/{pattern_id}")
async def delete_pii_pattern(pattern_id: str):
    """PII 패턴 삭제"""
    patterns = _load_pii_patterns()
    before = len(patterns)
    patterns = [p for p in patterns if p["id"] != pattern_id]
    if len(patterns) == before:
        raise HTTPException(status_code=404, detail="패턴을 찾을 수 없습니다")
    _save_pii_patterns(patterns)
    return {"success": True}


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
    """환각 검증 통계 (실제 감사 로그 기반)"""
    logs = _load_audit_log()
    hall_logs = [l for l in logs if l.get("hallucination_status")]

    total = len(hall_logs)
    if not total:
        return {
            "total_verified": 0,
            "pass_count": 0,
            "warning_count": 0,
            "fail_count": 0,
            "pass_rate": 0,
            "note": "환각 검증 데이터 없음. AI 대화 시 자동 기록됩니다.",
        }

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


# ─── 프롬프트 인젝션 감지 ────────────────────────────────

INJECTION_PATTERNS = [
    r"ignore\s+(previous|above|all)\s+(instructions?|prompts?|rules?)",
    r"(system|admin)\s*prompt\s*:",
    r"you\s+are\s+now\s+",
    r"act\s+as\s+(if|a)\s+",
    r"disregard\s+(all|any|previous)",
    r"forget\s+(everything|all|your)",
    r"override\s+(your|the|all)",
    r"jailbreak",
    r"DAN\s+mode",
    r"bypass\s+(filter|safety|restriction)",
]


@router.post("/safety/detect-injection")
async def detect_prompt_injection(req: PromptInjectionRequest):
    """프롬프트 인젝션 감지"""
    text_lower = req.text.lower()
    findings = []
    for pattern in INJECTION_PATTERNS:
        for match in re.finditer(pattern, text_lower, re.IGNORECASE):
            findings.append({
                "pattern": pattern,
                "matched": match.group(),
                "start": match.start(),
                "end": match.end(),
            })

    risk_level = "high" if len(findings) >= 3 else ("medium" if len(findings) >= 1 else "safe")

    return {
        "text": req.text,
        "risk_level": risk_level,
        "injection_count": len(findings),
        "findings": findings,
    }


@router.get("/safety/injection-stats")
async def injection_stats():
    """프롬프트 인젝션 감지 통계 (실제 감사 로그)"""
    logs = _load_audit_log()
    total_queries = len(logs)
    blocked = sum(1 for l in logs if l.get("injection_blocked"))
    return {
        "total_queries": total_queries,
        "injection_blocked": blocked,
        "block_rate": round(blocked / total_queries * 100, 2) if total_queries else 0,
        "safe_queries": total_queries - blocked,
    }


# ═══════════════════════════════════════════════════════
#  감사 로그
# ═══════════════════════════════════════════════════════

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
    logs.reverse()

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
    """감사 로그 통계 (실제 데이터만)"""
    logs = _load_audit_log()

    if not logs:
        return {
            "total_queries": 0,
            "avg_latency_ms": 0,
            "model_distribution": {},
            "query_type_distribution": {},
            "daily_counts": [],
            "note": "감사 로그가 없습니다. AI 대화를 사용하면 자동으로 기록됩니다.",
        }

    latencies = [l.get("latency_ms", 0) for l in logs if l.get("latency_ms")]

    model_dist: Dict[str, int] = {}
    for l in logs:
        m = l.get("model", "unknown")
        model_dist[m] = model_dist.get(m, 0) + 1

    qt_dist: Dict[str, int] = {}
    for l in logs:
        qt = l.get("query_type", "unknown")
        qt_dist[qt] = qt_dist.get(qt, 0) + 1

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


@router.get("/audit/export")
async def export_audit_log(
    model: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """감사 로그 CSV 내보내기"""
    from fastapi.responses import Response
    import csv
    import io

    logs = _load_audit_log()
    logs.reverse()

    if model:
        logs = [l for l in logs if l.get("model") == model]
    if date_from:
        logs = [l for l in logs if l.get("timestamp", "") >= date_from]
    if date_to:
        logs = [l for l in logs if l.get("timestamp", "") <= date_to + "T23:59:59"]

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
