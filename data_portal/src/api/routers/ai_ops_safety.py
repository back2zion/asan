"""
AI 안전성 엔드포인트 (PII, 환각 검증, 프롬프트 인젝션)
ai_ops.py 의 서브 라우터로 포함됨 (prefix 없음)
"""
import re

from fastapi import APIRouter, HTTPException

from . import _ai_ops_shared as shared
from ._ai_ops_shared import (
    _load_pii_patterns,
    _save_pii_patterns,
    _load_audit_log,
    detect_pii,
    mask_pii,
    PIITestRequest,
    PIIPatternCreate,
    PIIPatternUpdate,
    PromptInjectionRequest,
)

router = APIRouter()


# ═══════════════════════════════════════════════════════
#  PII 패턴 관리
# ═══════════════════════════════════════════════════════

@router.get("/safety/pii-patterns")
async def list_pii_patterns():
    """PII 탐지 패턴 목록"""
    return {"patterns": _load_pii_patterns()}


@router.post("/safety/pii-patterns")
async def create_pii_pattern(body: PIIPatternCreate):
    """PII 패턴 추가"""
    # 정규식 유효성 검사
    try:
        re.compile(body.pattern)
    except re.error as e:
        raise HTTPException(status_code=400, detail=f"잘못된 정규식: {e}")

    patterns = _load_pii_patterns()
    new_pat = {
        "id": f"custom_{shared._pii_next_id}",
        "name": body.name,
        "pattern": body.pattern,
        "replacement": body.replacement,
        "enabled": body.enabled,
        "description": body.description,
    }
    shared._pii_next_id += 1
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


# ═══════════════════════════════════════════════════════
#  환각 검증 통계
# ═══════════════════════════════════════════════════════

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


# ═══════════════════════════════════════════════════════
#  프롬프트 인젝션 감지
# ═══════════════════════════════════════════════════════

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
