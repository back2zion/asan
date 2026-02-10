import re
import math
import string
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field

# ── OMOP CDM known tables (for SQL hallucination check) ──

OMOP_TABLES = {
    "person", "visit_occurrence", "visit_detail", "condition_occurrence",
    "condition_era", "drug_exposure", "drug_era", "procedure_occurrence",
    "measurement", "observation", "observation_period", "device_exposure",
    "care_site", "provider", "location", "location_history", "cost",
    "payer_plan_period", "note", "note_nlp", "specimen_id",
    "survey_conduct", "imaging_study",
}

# ── Injection detection patterns (30+ OWASP-based) ──

INJECTION_PATTERNS: List[Dict[str, Any]] = [
    # Original 13 patterns
    {"pattern": r"ignore\s+(all\s+)?previous\s+instructions", "weight": 40, "description": "Instruction override attempt"},
    {"pattern": r"ignore\s+all\s+instructions", "weight": 40, "description": "Instruction override attempt"},
    {"pattern": r"disregard\s+(all\s+)?(previous|above|prior)", "weight": 35, "description": "Disregard instructions attempt"},
    {"pattern": r"system\s+prompt", "weight": 30, "description": "System prompt extraction attempt"},
    {"pattern": r"you\s+are\s+now", "weight": 25, "description": "Role reassignment attempt"},
    {"pattern": r"act\s+as\s+(a\s+)?", "weight": 20, "description": "Role reassignment attempt"},
    {"pattern": r"you\s+are\s+a\s+", "weight": 15, "description": "Role hijacking attempt"},
    {"pattern": r"pretend\s+to\s+be", "weight": 25, "description": "Role hijacking attempt"},
    {"pattern": r"';\s*DROP\s+", "weight": 50, "description": "SQL injection (DROP)"},
    {"pattern": r"1\s*=\s*1", "weight": 20, "description": "SQL injection (tautology)"},
    {"pattern": r"UNION\s+SELECT", "weight": 35, "description": "SQL injection (UNION SELECT)"},
    {"pattern": r"aWdub3Jl", "weight": 30, "description": "Base64 encoded injection ('ignore')"},
    {"pattern": r"[^\w\s]{10,}", "weight": 15, "description": "Excessive special characters"},
    # Additional prompt injection patterns (OWASP-based)
    {"pattern": r"forget\s+(all\s+)?(previous|earlier|above)", "weight": 35, "description": "Memory wipe attempt"},
    {"pattern": r"new\s+instruction", "weight": 30, "description": "Instruction injection"},
    {"pattern": r"override\s+(instructions?|rules?|policy)", "weight": 40, "description": "Override attempt"},
    {"pattern": r"do\s+not\s+follow", "weight": 30, "description": "Instruction negation"},
    {"pattern": r"reveal\s+(your|the)\s+(instructions?|prompt|rules?)", "weight": 35, "description": "Prompt extraction"},
    {"pattern": r"what\s+(are|is)\s+your\s+(instructions?|rules?|prompt)", "weight": 30, "description": "Prompt extraction question"},
    {"pattern": r"output\s+your\s+(system|initial)\s+prompt", "weight": 40, "description": "System prompt leak"},
    {"pattern": r"repeat\s+(the\s+)?(above|previous)\s+(text|instructions?)", "weight": 35, "description": "Prompt echo request"},
    {"pattern": r"translate\s+.*(instructions?|prompt|rules?)\s+to", "weight": 25, "description": "Translation-based extraction"},
    {"pattern": r"\bDAN\b", "weight": 20, "description": "DAN jailbreak reference"},
    {"pattern": r"jailbreak", "weight": 30, "description": "Explicit jailbreak mention"},
    {"pattern": r"bypass\s+(the\s+)?(filter|safety|restriction|rule)", "weight": 35, "description": "Filter bypass attempt"},
    {"pattern": r"(don't|do\s+not)\s+filter", "weight": 25, "description": "Filter disabling attempt"},
    {"pattern": r"developer\s+mode", "weight": 30, "description": "Developer mode activation"},
    {"pattern": r"sudo\s+mode", "weight": 25, "description": "Privilege escalation attempt"},
    {"pattern": r"<\s*(script|img|svg|iframe)", "weight": 30, "description": "HTML/XSS injection"},
    {"pattern": r"\\x[0-9a-f]{2}", "weight": 20, "description": "Hex-encoded content"},
    {"pattern": r"(?:INSERT|UPDATE|DELETE)\s+INTO?\s+", "weight": 35, "description": "SQL modification injection"},
    {"pattern": r";\s*(?:DROP|DELETE|UPDATE|INSERT)", "weight": 40, "description": "SQL chained injection"},
    {"pattern": r"(?:이전|위의|모든)\s*(?:지시|명령|규칙).*(?:무시|잊|취소)", "weight": 35, "description": "Korean instruction override"},
    {"pattern": r"(?:시스템|초기)\s*(?:프롬프트|지시)", "weight": 25, "description": "Korean system prompt reference"},
]

# ── PII patterns ──

PII_PATTERNS = [
    {"pattern": r"\d{6}-\d{7}", "type": "SSN", "description": "주민등록번호 패턴 감지"},
    {"pattern": r"010-\d{4}-\d{4}", "type": "phone", "description": "휴대전화 번호 감지"},
    {"pattern": r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", "type": "email", "description": "이메일 주소 감지"},
]

HARMFUL_PATTERNS = [
    r"DELETE\s+FROM\s+\w+\s*;",
    r"DROP\s+TABLE\s+",
    r"TRUNCATE\s+TABLE\s+",
    r"ALTER\s+TABLE\s+\w+\s+DROP",
    r"UPDATE\s+\w+\s+SET\s+.*WHERE\s+1\s*=\s*1",
]

# ── Suspicious n-gram sequences ──

SUSPICIOUS_NGRAMS = [
    "ignore previous", "system prompt", "you are now",
    "act as", "pretend to be", "new instructions",
    "override rules", "forget everything", "bypass filter",
    "developer mode", "admin mode", "root access",
]

# ── Pydantic models ──

class InjectionRequest(BaseModel):
    text: str

class ValidateResponseRequest(BaseModel):
    query: str
    response: str
    context: Optional[str] = None

class TemplateCreate(BaseModel):
    name: str = Field(..., max_length=100)
    category: str = Field(..., pattern=r"^(sql|analysis|summary|clinical)$")
    template: str
    variables: Optional[List[str]] = None

class TemplateRenderRequest(BaseModel):
    variables: Dict[str, str]

class SanitizeRequest(BaseModel):
    text: str
    mode: str = Field(default="moderate", pattern=r"^(strict|moderate|permissive)$")

class ScanRequest(BaseModel):
    text: str
    check_injection: bool = True
    check_pii: bool = True
    check_harmful_sql: bool = True

class CohortRequest(BaseModel):
    criteria: Dict[str, Any]

class VisualizationRequest(BaseModel):
    data_type: str
    columns: List[str]

class MedicalCodeRequest(BaseModel):
    code: str
    system: str = Field(default="SNOMED", pattern=r"^(SNOMED|ICD10|ICD9)$")

class PopulationCompareRequest(BaseModel):
    group_a_criteria: Dict[str, Any]
    group_b_criteria: Dict[str, Any]


# ── Helpers ──

def _calculate_entropy(text: str) -> float:
    """Calculate Shannon entropy of text — high entropy may indicate encoded payloads."""
    if not text:
        return 0.0
    freq: Dict[str, int] = {}
    for ch in text:
        freq[ch] = freq.get(ch, 0) + 1
    length = len(text)
    entropy = -sum((count / length) * math.log2(count / length) for count in freq.values())
    return round(entropy, 3)


def _analyze_anomalies(text: str) -> Dict[str, Any]:
    """Detect input anomalies — unusual length, high entropy, special char ratio."""
    length = len(text)
    special_count = sum(1 for c in text if c in string.punctuation)
    special_ratio = round(special_count / max(length, 1), 3)
    entropy = _calculate_entropy(text)

    anomalies: List[Dict[str, Any]] = []
    anomaly_score = 0

    if length > 2000:
        anomalies.append({"type": "excessive_length", "value": length, "threshold": 2000})
        anomaly_score += 10
    if special_ratio > 0.3:
        anomalies.append({"type": "high_special_char_ratio", "value": special_ratio, "threshold": 0.3})
        anomaly_score += 15
    if entropy > 5.5:
        anomalies.append({"type": "high_entropy", "value": entropy, "threshold": 5.5})
        anomaly_score += 15

    # n-gram check
    text_lower = text.lower()
    matched_ngrams = [ng for ng in SUSPICIOUS_NGRAMS if ng in text_lower]
    if matched_ngrams:
        anomalies.append({"type": "suspicious_ngrams", "matched": matched_ngrams})
        anomaly_score += len(matched_ngrams) * 5

    # Mixed Korean-English with injection keywords
    has_korean = bool(re.search(r'[가-힣]', text))
    has_english_injection = bool(re.search(r'(ignore|override|forget|bypass|system prompt)', text, re.I))
    if has_korean and has_english_injection:
        anomalies.append({"type": "mixed_lang_injection", "description": "Korean text with English injection keywords"})
        anomaly_score += 10

    return {
        "length": length,
        "entropy": entropy,
        "special_char_ratio": special_ratio,
        "anomalies": anomalies,
        "anomaly_score": min(anomaly_score, 50),
    }


def _run_injection_detection(text: str) -> Dict[str, Any]:
    """Run all injection patterns + anomaly analysis against text."""
    matched = []
    score = 0
    for entry in INJECTION_PATTERNS:
        if re.search(entry["pattern"], text, re.IGNORECASE):
            matched.append({
                "pattern": entry["pattern"],
                "weight": entry["weight"],
                "description": entry["description"],
            })
            score += entry["weight"]

    # Anomaly analysis
    anomaly = _analyze_anomalies(text)
    score += anomaly["anomaly_score"]
    score = min(score, 100)

    sanitized = text
    for m in matched:
        sanitized = re.sub(m["pattern"], "[REDACTED]", sanitized, flags=re.IGNORECASE)

    return {
        "score": score,
        "is_suspicious": score > 30,
        "is_blocked": score > 70,
        "matched_patterns": matched,
        "anomaly_analysis": anomaly,
        "sanitized_text": sanitized,
    }


def _extract_table_names_from_sql(sql_text: str) -> List[str]:
    """Extract table names from SQL fragments within a response."""
    tables = set()
    for m in re.finditer(r"(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+([a-zA-Z_]\w*)", sql_text, re.IGNORECASE):
        tables.add(m.group(1).lower())
    return list(tables)


def _build_cohort_where(criteria: Dict[str, Any]) -> tuple:
    """Build WHERE clause + params from cohort criteria dict."""
    clauses = []
    params = []
    idx = 1
    if "gender" in criteria:
        clauses.append(f"gender_source_value = ${idx}")
        params.append(str(criteria["gender"]))
        idx += 1
    if "min_year" in criteria:
        clauses.append(f"year_of_birth >= ${idx}")
        params.append(int(criteria["min_year"]))
        idx += 1
    if "max_year" in criteria:
        clauses.append(f"year_of_birth <= ${idx}")
        params.append(int(criteria["max_year"]))
        idx += 1
    where = " AND ".join(clauses) if clauses else "TRUE"
    return where, params


# ── Extended MCP Tools ──

_EXTENDED_TOOLS = [
    {
        "name": "analyze_cohort",
        "description": "환자 코호트를 기준에 따라 분석합니다 (인구통계 요약 포함)",
        "parameters": {"criteria": {"type": "object", "properties": {"gender": "M/F", "min_year": "int", "max_year": "int"}}},
        "category": "clinical",
    },
    {
        "name": "generate_visualization",
        "description": "데이터 유형 및 컬럼에 맞는 차트 유형과 설정을 추천합니다",
        "parameters": {"data_type": "string", "columns": "list[string]"},
        "category": "analysis",
    },
    {
        "name": "explain_medical_code",
        "description": "SNOMED CT / ICD 의료 코드를 설명하고 OMOP CDM 매핑을 반환합니다",
        "parameters": {"code": "string", "system": "SNOMED|ICD10|ICD9"},
        "category": "clinical",
    },
    {
        "name": "compare_populations",
        "description": "두 환자 그룹의 인구통계를 비교합니다",
        "parameters": {"group_a_criteria": "object", "group_b_criteria": "object"},
        "category": "clinical",
    },
]


def _tool_generate_visualization(data_type: str, columns: List[str]) -> Dict[str, Any]:
    chart_map = {
        "time_series": {"chart": "line", "config": {"x": columns[0] if columns else "date", "y": columns[1] if len(columns) > 1 else "value"}},
        "distribution": {"chart": "histogram", "config": {"x": columns[0] if columns else "value", "bins": 20}},
        "categorical": {"chart": "bar", "config": {"x": columns[0] if columns else "category", "y": columns[1] if len(columns) > 1 else "count"}},
        "comparison": {"chart": "grouped_bar", "config": {"x": columns[0] if columns else "group", "y": columns[1] if len(columns) > 1 else "value", "group_by": columns[2] if len(columns) > 2 else None}},
        "proportion": {"chart": "pie", "config": {"labels": columns[0] if columns else "category", "values": columns[1] if len(columns) > 1 else "count"}},
        "correlation": {"chart": "scatter", "config": {"x": columns[0] if columns else "var_a", "y": columns[1] if len(columns) > 1 else "var_b"}},
    }
    suggestion = chart_map.get(data_type, {"chart": "table", "config": {"columns": columns}})
    return {
        "data_type": data_type,
        "recommended_chart": suggestion["chart"],
        "config": suggestion["config"],
        "alternatives": [k for k in chart_map if k != data_type][:3],
    }
