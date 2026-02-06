"""
Prompt Enhancement Service

불완전한 키워드 입력을 완전한 질의문으로 자동 확장하는 통합 서비스입니다.
"""

import asyncio
from dataclasses import dataclass
from typing import Optional

from ai_services.prompt_enhancement.config import ENABLE_PROMPT_ENHANCEMENT
from ai_services.prompt_enhancement.detector import (
    QueryCompletenessLevel,
    QueryCompletenessResult,
    detect_completeness,
)
from ai_services.prompt_enhancement.medical_normalizer import (
    MedicalNormalizer,
    NormalizationResult,
)
from ai_services.prompt_enhancement.expander import QueryExpander, ExpansionResult


@dataclass
class PromptEnhancementResult:
    """Prompt Enhancement 최종 결과"""
    # 입력/출력
    original_query: str
    enhanced_query: str

    # 처리 상태
    enhancement_applied: bool
    confidence: float

    # 상세 정보
    completeness: QueryCompletenessResult
    normalization: NormalizationResult
    expansion: Optional[ExpansionResult] = None

    # 디버그 정보
    reasoning: Optional[str] = None
    error: Optional[str] = None


class PromptEnhancementService:
    """Prompt Enhancement 통합 서비스

    사용자 입력의 완성도를 분석하고, 필요시 LLM을 사용하여
    완전한 질의문으로 확장합니다.

    파이프라인:
    1. 완성도 탐지 (detector)
    2. 의료 용어 정규화 (medical_normalizer)
    3. LLM 기반 확장 (expander) - 불완전한 입력에만
    """

    def __init__(
        self,
        normalizer: Optional[MedicalNormalizer] = None,
        expander: Optional[QueryExpander] = None,
        enabled: bool = ENABLE_PROMPT_ENHANCEMENT,
    ):
        """
        Args:
            normalizer: 의료 용어 정규화기 (기본: MedicalNormalizer)
            expander: 쿼리 확장기 (기본: QueryExpander)
            enabled: 기능 활성화 여부
        """
        self.normalizer = normalizer or MedicalNormalizer()
        self.expander = expander or QueryExpander()
        self.enabled = enabled

    async def enhance(
        self,
        query: str,
        context: Optional[str] = None,
    ) -> PromptEnhancementResult:
        """쿼리를 분석하고 필요시 확장합니다.

        Args:
            query: 사용자 입력
            context: 추가 컨텍스트 (이전 대화 등)

        Returns:
            PromptEnhancementResult: 확장 결과
        """
        query = query.strip()

        # 1. 완성도 탐지
        completeness = detect_completeness(query)

        # 2. 의료 용어 정규화
        normalization = self.normalizer.normalize(query)
        normalized_query = normalization.normalized

        # 기능 비활성화 또는 완전한 질의인 경우
        if not self.enabled or completeness.level == QueryCompletenessLevel.COMPLETE:
            return PromptEnhancementResult(
                original_query=query,
                enhanced_query=normalized_query,
                enhancement_applied=normalization.has_changes,
                confidence=1.0,
                completeness=completeness,
                normalization=normalization,
                reasoning="완전한 질의문" if completeness.level == QueryCompletenessLevel.COMPLETE else "기능 비활성화",
            )

        # 3. LLM 기반 확장 (불완전한 입력에만)
        expansion = await self.expander.expand(
            original_query=query,
            normalized_query=normalized_query,
            completeness=completeness,
            context=context,
        )

        return PromptEnhancementResult(
            original_query=query,
            enhanced_query=expansion.enhanced_query,
            enhancement_applied=expansion.expansion_applied,
            confidence=expansion.confidence,
            completeness=completeness,
            normalization=normalization,
            expansion=expansion,
            reasoning=expansion.reasoning,
            error=expansion.error,
        )

    def enhance_sync(
        self,
        query: str,
        context: Optional[str] = None,
    ) -> PromptEnhancementResult:
        """enhance의 동기 래퍼.

        Args:
            query: 사용자 입력
            context: 추가 컨텍스트

        Returns:
            PromptEnhancementResult: 확장 결과
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(
                    asyncio.run,
                    self.enhance(query, context),
                ).result()
        else:
            return asyncio.run(self.enhance(query, context))


# 싱글톤 인스턴스
prompt_enhancement_service = PromptEnhancementService()
