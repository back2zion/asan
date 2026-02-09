"""
AI Ops 공유 유틸리티, 상수, 모델 정의
ai_ops.py, ai_ops_safety.py, ai_ops_audit.py 에서 공통으로 사용
"""
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
from copy import deepcopy

from pydantic import BaseModel


# ─── 데이터 디렉토리 & 파일 경로 ─────────────────────────

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


# ─── 모델 레지스트리 기본값 ──────────────────────────────

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
        "pattern": r"(?<![.\d])\d{6}[-\s]?[1-4]\d{6}(?!\d)",
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
