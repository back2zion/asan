"""
AI 안전성 엔드포인트 (PII, 환각 검증, 프롬프트 인젝션)
ai_ops.py 의 서브 라우터로 포함됨 (prefix 없음)
PostgreSQL-backed via _ai_ops_shared
"""
import re

from fastapi import APIRouter, HTTPException

from ._ai_ops_shared import (
    db_load_pii_patterns,
    db_create_pii_pattern,
    db_update_pii_pattern,
    db_delete_pii_pattern,
    db_hallucination_stats,
    db_injection_stats,
    detect_pii,
    mask_pii,
    PIITestRequest,
    PIIPatternCreate,
    PIIPatternUpdate,
    PromptInjectionRequest,
)

router = APIRouter()


# ═══════════════════════════════════════════════════════
#  PII 패턴 관리 (PostgreSQL)
# ═══════════════════════════════════════════════════════

@router.get("/safety/pii-patterns")
async def list_pii_patterns():
    """PII 탐지 패턴 목록"""
    patterns = await db_load_pii_patterns()
    return {"patterns": patterns}


@router.post("/safety/pii-patterns")
async def create_pii_pattern(body: PIIPatternCreate):
    """PII 패턴 추가"""
    try:
        re.compile(body.pattern)
    except re.error as e:
        raise HTTPException(status_code=400, detail=f"잘못된 정규식: {e}")

    result = await db_create_pii_pattern({
        "name": body.name,
        "pattern": body.pattern,
        "replacement": body.replacement,
        "enabled": body.enabled,
        "description": body.description,
    })
    return result


@router.put("/safety/pii-patterns/{pattern_id}")
async def update_pii_pattern(pattern_id: str, body: PIIPatternUpdate):
    """PII 패턴 수정"""
    if body.pattern is not None:
        try:
            re.compile(body.pattern)
        except re.error as e:
            raise HTTPException(status_code=400, detail=f"잘못된 정규식: {e}")

    result = await db_update_pii_pattern(pattern_id, body.model_dump(exclude_none=True))
    if result is None:
        raise HTTPException(status_code=404, detail="패턴을 찾을 수 없습니다")
    return {"message": f"패턴 '{result['name']}' 수정됨", "pattern": result}


@router.delete("/safety/pii-patterns/{pattern_id}")
async def delete_pii_pattern(pattern_id: str):
    """PII 패턴 삭제"""
    deleted = await db_delete_pii_pattern(pattern_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="패턴을 찾을 수 없습니다")
    return {"success": True}


@router.post("/safety/test-pii")
async def test_pii(req: PIITestRequest):
    """PII 탐지 테스트"""
    # Ensure cache is fresh
    await db_load_pii_patterns()
    findings = detect_pii(req.text)
    masked_text, count = mask_pii(req.text)
    return {
        "original_text": req.text,
        "masked_text": masked_text,
        "findings": findings,
        "pii_count": count,
    }


# ═══════════════════════════════════════════════════════
#  환각 검증 통계 (PostgreSQL)
# ═══════════════════════════════════════════════════════

@router.get("/safety/hallucination-stats")
async def hallucination_stats():
    """환각 검증 통계 (PostgreSQL 기반)"""
    return await db_hallucination_stats()


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
    """프롬프트 인젝션 감지 통계 (PostgreSQL)"""
    return await db_injection_stats()
