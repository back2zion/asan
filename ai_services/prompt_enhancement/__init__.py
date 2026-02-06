"""
Prompt Enhancement 모듈

불완전한 키워드 입력을 완전한 질의문으로 자동 확장합니다.

예시:
    - "당뇨 환자" → "제2형 당뇨병 진단을 받은 환자는 몇 명입니까?"
    - "고혈압 입원" → "고혈압 진단을 받고 현재 입원 중인 환자는 몇 명입니까?"

Usage:
    from ai_services.prompt_enhancement import prompt_enhancement_service

    # 비동기 사용
    result = await prompt_enhancement_service.enhance("당뇨 환자")

    # 동기 사용
    result = prompt_enhancement_service.enhance_sync("고혈압 입원")
"""

from ai_services.prompt_enhancement.service import (
    PromptEnhancementService,
    PromptEnhancementResult,
    prompt_enhancement_service,
)
from ai_services.prompt_enhancement.detector import (
    QueryCompletenessLevel,
    QueryCompletenessResult,
    detect_completeness,
)
from ai_services.prompt_enhancement.medical_normalizer import (
    MedicalNormalizer,
    normalize_medical_terms,
)
from ai_services.prompt_enhancement.expander import QueryExpander

__all__ = [
    # Service
    "PromptEnhancementService",
    "PromptEnhancementResult",
    "prompt_enhancement_service",
    # Detector
    "QueryCompletenessLevel",
    "QueryCompletenessResult",
    "detect_completeness",
    # Normalizer
    "MedicalNormalizer",
    "normalize_medical_terms",
    # Expander
    "QueryExpander",
]
