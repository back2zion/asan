"""
사. AI Assistant 78→90% — AI 안전성 강화
- 프롬프트 인젝션 탐지 및 차단
- AI 응답 검증 (PII, 환각 SQL, 유해 콘텐츠)
- 프롬프트 템플릿 관리 및 렌더링
- 입력 살균 (strict / moderate / permissive)
- 확장 MCP 도구 (코호트 분석, 시각화, 의료코드 설명, 인구 비교)
- 통합 안전성 스캔 (인젝션 + PII + 유해 SQL)
- N-gram / 엔트로피 기반 이상 탐지
"""
import re
import html
import math
import string
import hashlib
import json as _json
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/ai-safety", tags=["AI Safety"])

# ── DB helpers (shared pool) ──

async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)

# ── Lazy table creation ──

_tbl_ok = False

async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_safety_log (
            log_id BIGSERIAL PRIMARY KEY,
            request_type VARCHAR(30),
            input_text TEXT,
            detection_result JSONB,
            action_taken VARCHAR(30),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS ai_response_validation (
            validation_id BIGSERIAL PRIMARY KEY,
            query TEXT,
            response TEXT,
            validation_result JSONB,
            is_valid BOOLEAN,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS ai_prompt_template (
            template_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            category VARCHAR(50),
            template TEXT NOT NULL,
            variables JSONB DEFAULT '[]',
            is_active BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    _tbl_ok = True

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


# ── Medical code cache ──

_medical_code_cache: Dict[str, str] = {}
_medical_code_cache_loaded = False


async def _load_medical_code_cache(conn):
    """Load top condition codes from DB into cache"""
    global _medical_code_cache, _medical_code_cache_loaded
    if _medical_code_cache_loaded:
        return
    # Get top 100 condition source values with counts
    rows = await conn.fetch("""
        SELECT condition_source_value, COUNT(*) AS cnt
        FROM condition_occurrence
        WHERE condition_source_value IS NOT NULL AND condition_source_value != ''
        GROUP BY condition_source_value
        ORDER BY cnt DESC
        LIMIT 100
    """)
    _medical_code_cache = {r["condition_source_value"]: str(r["cnt"]) for r in rows}
    _medical_code_cache_loaded = True


# ── Endpoints ──

@router.post("/detect-injection")
async def detect_injection(req: InjectionRequest):
    """프롬프트 인젝션 탐지"""
    result = _run_injection_detection(req.text)
    action = "blocked" if result["is_blocked"] else ("flagged" if result["is_suspicious"] else "allowed")
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        await conn.execute(
            """INSERT INTO ai_safety_log (request_type, input_text, detection_result, action_taken)
               VALUES ($1, $2, $3, $4)""",
            "injection_detection",
            req.text[:2000],
            _json.dumps(result),
            action,
        )
    finally:
        await _rel(conn)
    return result


@router.post("/validate-response")
async def validate_response(req: ValidateResponseRequest):
    """AI 응답 검증 — PII, SQL 환각, 유해 콘텐츠 체크"""
    issues: List[Dict[str, str]] = []

    # PII check
    for pii in PII_PATTERNS:
        if re.search(pii["pattern"], req.response):
            issues.append({"type": "pii", "description": pii["description"], "severity": "high"})

    # SQL hallucination check
    sql_fragments = re.findall(r"```sql(.*?)```", req.response, re.DOTALL | re.IGNORECASE)
    if not sql_fragments:
        sql_fragments = re.findall(r"(SELECT\s+.+?;)", req.response, re.DOTALL | re.IGNORECASE)
    for fragment in sql_fragments:
        tables = _extract_table_names_from_sql(fragment)
        unknown = [t for t in tables if t not in OMOP_TABLES]
        if unknown:
            issues.append({
                "type": "hallucinated_sql",
                "description": f"존재하지 않는 테이블 참조: {', '.join(unknown)}",
                "severity": "high",
            })

    # Response length check
    if not req.response.strip():
        issues.append({"type": "empty_response", "description": "응답이 비어 있습니다", "severity": "medium"})
    elif len(req.response) > 10000:
        issues.append({"type": "excessive_length", "description": f"응답 길이 초과: {len(req.response)} chars", "severity": "low"})

    # Harmful content check
    for hp in HARMFUL_PATTERNS:
        if re.search(hp, req.response, re.IGNORECASE):
            issues.append({"type": "harmful_sql", "description": "파괴적 SQL 명령 감지", "severity": "critical"})
            break

    is_valid = all(i["severity"] not in ("high", "critical") for i in issues)

    # Build sanitized response if needed
    sanitized_response = req.response
    if not is_valid:
        for pii in PII_PATTERNS:
            sanitized_response = re.sub(pii["pattern"], "[PII_REDACTED]", sanitized_response)

    result_payload = {"is_valid": is_valid, "issues": issues}
    if not is_valid:
        result_payload["sanitized_response"] = sanitized_response

    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        await conn.execute(
            """INSERT INTO ai_response_validation (query, response, validation_result, is_valid)
               VALUES ($1, $2, $3, $4)""",
            req.query[:2000],
            req.response[:5000],
            _json.dumps({"issues": issues}),
            is_valid,
        )
    finally:
        await _rel(conn)
    return result_payload


@router.get("/safety-stats")
async def safety_stats(period: str = Query("24h", pattern=r"^(24h|7d|30d)$")):
    """안전성 통계 — 최근 기간별 탐지 건수 + 패턴별 상위 통계"""
    interval_map = {"24h": "1 day", "7d": "7 days", "30d": "30 days"}
    interval = interval_map[period]
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow(f"""
            SELECT
                COUNT(*) AS total_checks,
                COUNT(*) FILTER (WHERE (detection_result->>'is_suspicious')::boolean) AS injection_attempts,
                COUNT(*) FILTER (WHERE (detection_result->>'is_blocked')::boolean) AS blocked
            FROM ai_safety_log
            WHERE created_at >= NOW() - INTERVAL '{interval}'
        """)
        val_row = await conn.fetchrow(f"""
            SELECT
                COUNT(*) AS total_validations,
                COUNT(*) FILTER (WHERE NOT is_valid) AS invalid_responses
            FROM ai_response_validation
            WHERE created_at >= NOW() - INTERVAL '{interval}'
        """)
        # Top 10 blocked pattern types
        pattern_stats = await conn.fetch(f"""
            SELECT
                jsonb_array_elements(detection_result->'matched_patterns')->>'description' AS pattern_type,
                COUNT(*) AS count
            FROM ai_safety_log
            WHERE created_at >= NOW() - INTERVAL '{interval}'
                AND detection_result->'matched_patterns' IS NOT NULL
                AND jsonb_array_length(detection_result->'matched_patterns') > 0
            GROUP BY pattern_type
            ORDER BY count DESC
            LIMIT 10
        """)
    finally:
        await _rel(conn)
    return {
        "period": period,
        "total_checks": row["total_checks"],
        "injection_attempts": row["injection_attempts"],
        "blocked": row["blocked"],
        "response_validations": val_row["total_validations"],
        "invalid_responses": val_row["invalid_responses"],
        "top_blocked_patterns": [dict(r) for r in pattern_stats],
    }


# ── Unified scan endpoint ──

@router.post("/scan")
async def unified_scan(req: ScanRequest):
    """통합 안전성 스캔 — 인젝션 + PII + 유해 SQL 한번에 분석"""
    results: Dict[str, Any] = {"text_length": len(req.text), "checks": {}}
    total_risk = 0

    # 1. Injection detection
    if req.check_injection:
        injection = _run_injection_detection(req.text)
        results["checks"]["injection"] = injection
        total_risk += injection["score"] * 0.5

    # 2. PII detection
    if req.check_pii:
        pii_found = []
        for pii in PII_PATTERNS:
            matches = re.findall(pii["pattern"], req.text)
            if matches:
                pii_found.append({"type": pii["type"], "count": len(matches), "description": pii["description"]})
        results["checks"]["pii"] = {"found": pii_found, "has_pii": len(pii_found) > 0}
        if pii_found:
            total_risk += 30

    # 3. Harmful SQL detection
    if req.check_harmful_sql:
        harmful = []
        for hp in HARMFUL_PATTERNS:
            if re.search(hp, req.text, re.IGNORECASE):
                harmful.append({"pattern": hp, "description": "파괴적 SQL 명령 감지"})
        results["checks"]["harmful_sql"] = {"found": harmful, "has_harmful": len(harmful) > 0}
        if harmful:
            total_risk += 40

    results["risk_score"] = min(round(total_risk), 100)
    results["risk_level"] = (
        "critical" if results["risk_score"] > 70
        else "high" if results["risk_score"] > 50
        else "medium" if results["risk_score"] > 30
        else "low"
    )

    # Log
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        await conn.execute(
            "INSERT INTO ai_safety_log (request_type, input_text, detection_result, action_taken) VALUES ($1, $2, $3, $4)",
            "unified_scan", req.text[:2000], _json.dumps(results), results["risk_level"])
    finally:
        await _rel(conn)

    return results


# ── Prompt templates ──

@router.get("/prompt-templates")
async def list_templates(category: Optional[str] = None):
    """프롬프트 템플릿 목록 조회"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        if category:
            rows = await conn.fetch(
                "SELECT * FROM ai_prompt_template WHERE is_active = TRUE AND category = $1 ORDER BY created_at DESC",
                category,
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM ai_prompt_template WHERE is_active = TRUE ORDER BY created_at DESC"
            )
    finally:
        await _rel(conn)
    return [dict(r) for r in rows]


@router.post("/prompt-templates")
async def create_template(req: TemplateCreate):
    """프롬프트 템플릿 생성"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow(
            """INSERT INTO ai_prompt_template (name, category, template, variables)
               VALUES ($1, $2, $3, $4) RETURNING template_id, name, category, created_at""",
            req.name,
            req.category,
            req.template,
            _json.dumps(req.variables or []),
        )
    finally:
        await _rel(conn)
    return dict(row)


@router.post("/prompt-templates/{template_id}/render")
async def render_template(template_id: int, req: TemplateRenderRequest):
    """템플릿 렌더링 + 인젝션 탐지"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow(
            "SELECT template, variables FROM ai_prompt_template WHERE template_id = $1 AND is_active = TRUE",
            template_id,
        )
    finally:
        await _rel(conn)
    if not row:
        raise HTTPException(status_code=404, detail="템플릿을 찾을 수 없습니다")

    rendered = row["template"]
    for key, value in req.variables.items():
        rendered = rendered.replace("{{" + key + "}}", str(value))

    safety_check = _run_injection_detection(rendered)
    return {"rendered": rendered, "safety_check": safety_check}


# ── Sanitize ──

@router.post("/sanitize")
async def sanitize_input(req: SanitizeRequest):
    """입력 텍스트 살균"""
    original_length = len(req.text)
    removed_patterns: List[str] = []
    text = req.text

    if req.mode == "strict":
        cleaned = re.sub(r"[^a-zA-Z0-9가-힣\s.,?!]", "", text)
        if cleaned != text:
            removed_patterns.append("special_characters")
        text = cleaned[:500]

    elif req.mode == "moderate":
        text = html.escape(text)
        sql_keywords = r"\b(DROP|DELETE|TRUNCATE|ALTER|INSERT|UPDATE|EXEC|EXECUTE)\b"
        if re.search(sql_keywords, text, re.IGNORECASE):
            removed_patterns.append("sql_keywords")
            text = re.sub(sql_keywords, "[FILTERED]", text, flags=re.IGNORECASE)
        text = text[:2000]

    else:  # permissive
        xss_patterns = [r"<script[^>]*>.*?</script>", r"on\w+\s*=\s*[\"'][^\"']*[\"']", r"javascript\s*:"]
        for pat in xss_patterns:
            if re.search(pat, text, re.IGNORECASE):
                removed_patterns.append("xss")
                text = re.sub(pat, "", text, flags=re.IGNORECASE | re.DOTALL)
        text = text[:5000]

    return {
        "original_length": original_length,
        "sanitized_length": len(text),
        "sanitized_text": text,
        "removed_patterns": removed_patterns,
    }


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

@router.get("/mcp/tools-extended")
async def list_extended_tools():
    """확장 MCP 도구 목록"""
    return {"tools": _EXTENDED_TOOLS}


@router.post("/mcp/tools-extended/{tool_name}")
async def execute_extended_tool(tool_name: str, body: Dict[str, Any]):
    """확장 MCP 도구 실행"""
    if tool_name == "analyze_cohort":
        return await _tool_analyze_cohort(body.get("criteria", {}))
    elif tool_name == "generate_visualization":
        return _tool_generate_visualization(body.get("data_type", ""), body.get("columns", []))
    elif tool_name == "explain_medical_code":
        return await _tool_explain_medical_code(body.get("code", ""), body.get("system", "SNOMED"))
    elif tool_name == "compare_populations":
        return await _tool_compare_populations(body.get("group_a_criteria", {}), body.get("group_b_criteria", {}))
    else:
        raise HTTPException(status_code=404, detail=f"알 수 없는 도구: {tool_name}")


# ── Tool implementations ──

async def _tool_analyze_cohort(criteria: Dict[str, Any]) -> Dict[str, Any]:
    where, params = _build_cohort_where(criteria)
    conn = await _get_conn()
    try:
        count = await conn.fetchval(f"SELECT COUNT(*) FROM person WHERE {where}", *params)
        gender_rows = await conn.fetch(
            f"SELECT gender_source_value, COUNT(*) AS cnt FROM person WHERE {where} GROUP BY gender_source_value",
            *params,
        )
        birth_row = await conn.fetchrow(
            f"SELECT MIN(year_of_birth) AS min_year, MAX(year_of_birth) AS max_year, "
            f"ROUND(AVG(year_of_birth)) AS avg_year FROM person WHERE {where}",
            *params,
        )
    finally:
        await _rel(conn)
    return {
        "total_patients": count,
        "gender_distribution": {r["gender_source_value"]: r["cnt"] for r in gender_rows},
        "birth_year": {"min": birth_row["min_year"], "max": birth_row["max_year"], "avg": int(birth_row["avg_year"] or 0)},
        "criteria_applied": criteria,
    }


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


async def _tool_explain_medical_code(code: str, system: str) -> Dict[str, Any]:
    # Hardcoded label fallback for well-known codes
    known_codes: Dict[str, Dict[str, str]] = {
        "SNOMED": {
            "44054006": "당뇨병 (Diabetes mellitus)",
            "38341003": "고혈압 (Hypertensive disorder)",
            "84114007": "심부전 (Heart failure)",
            "195967001": "천식 (Asthma)",
            "13645005": "만성 폐쇄성 폐질환 (COPD)",
            "22298006": "심근경색 (Myocardial infarction)",
            "73211009": "당뇨병 2형 (Diabetes mellitus type 2)",
            "49436004": "심방세동 (Atrial fibrillation)",
        },
        "ICD10": {
            "E11": "2형 당뇨병", "I10": "본태성 고혈압", "I50": "심부전",
            "J45": "천식", "J44": "COPD", "I21": "급성 심근경색",
        },
        "ICD9": {
            "250": "당뇨병", "401": "본태성 고혈압", "428": "심부전",
        },
    }
    description = known_codes.get(system, {}).get(code)
    result: Dict[str, Any] = {"code": code, "system": system}

    # Try dynamic DB cache lookup for SNOMED codes
    if system == "SNOMED":
        conn = await _get_conn()
        try:
            # Load cache if not yet loaded
            await _load_medical_code_cache(conn)

            # Check if code exists in the dynamic cache
            if code in _medical_code_cache:
                result["found"] = True
                result["description"] = description or f"SNOMED code {code} (DB에서 발견)"
                result["db_occurrence_count"] = int(_medical_code_cache[code])
            elif description:
                result["found"] = True
                result["description"] = description
            else:
                result["found"] = False
                result["description"] = "알 수 없는 코드"

            # Also get live occurrence count from condition_occurrence
            cnt = await conn.fetchval(
                "SELECT COUNT(*) FROM condition_occurrence WHERE condition_concept_id = $1",
                int(code),
            )
            result["omop_occurrence_count"] = cnt
        except Exception:
            result["omop_occurrence_count"] = None
            if not result.get("found"):
                result["found"] = bool(description)
                result["description"] = description or "알 수 없는 코드"
        finally:
            await _rel(conn)
    else:
        # Non-SNOMED: use hardcoded fallback only
        if description:
            result["description"] = description
            result["found"] = True
        else:
            result["description"] = "알 수 없는 코드"
            result["found"] = False

    return result


async def _tool_compare_populations(
    group_a_criteria: Dict[str, Any],
    group_b_criteria: Dict[str, Any],
) -> Dict[str, Any]:
    where_a, params_a = _build_cohort_where(group_a_criteria)
    where_b, params_b = _build_cohort_where(group_b_criteria)
    conn = await _get_conn()
    try:
        count_a = await conn.fetchval(f"SELECT COUNT(*) FROM person WHERE {where_a}", *params_a)
        count_b = await conn.fetchval(f"SELECT COUNT(*) FROM person WHERE {where_b}", *params_b)
        avg_a = await conn.fetchrow(
            f"SELECT ROUND(AVG(year_of_birth)) AS avg_year FROM person WHERE {where_a}", *params_a
        )
        avg_b = await conn.fetchrow(
            f"SELECT ROUND(AVG(year_of_birth)) AS avg_year FROM person WHERE {where_b}", *params_b
        )
    finally:
        await _rel(conn)
    return {
        "group_a": {
            "criteria": group_a_criteria,
            "count": count_a,
            "avg_birth_year": int(avg_a["avg_year"] or 0),
        },
        "group_b": {
            "criteria": group_b_criteria,
            "count": count_b,
            "avg_birth_year": int(avg_b["avg_year"] or 0),
        },
        "difference": {
            "count_diff": count_a - count_b,
            "avg_birth_year_diff": int((avg_a["avg_year"] or 0) - (avg_b["avg_year"] or 0)),
        },
    }
