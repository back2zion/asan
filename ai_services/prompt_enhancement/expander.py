"""
Query Expander

LLM을 사용하여 불완전한 키워드를 완전한 질의문으로 확장합니다.
"""

import json
import re
from dataclasses import dataclass
from typing import Optional

import httpx

from ai_services.prompt_enhancement.config import (
    PROMPT_ENHANCEMENT_API_URL,
    PROMPT_ENHANCEMENT_MODEL,
    ENHANCEMENT_TEMPERATURE,
    ENHANCEMENT_MAX_TOKENS,
    ENHANCEMENT_TIMEOUT,
    ENHANCEMENT_MIN_CONFIDENCE,
)
from ai_services.prompt_enhancement.detector import (
    QueryCompletenessLevel,
    QueryCompletenessResult,
)
from ai_services.prompt_enhancement.prompts.expansion_prompts import (
    QUERY_EXPANSION_SYSTEM_PROMPT,
    build_expansion_prompt,
)


@dataclass
class ExpansionResult:
    """확장 결과"""
    original_query: str
    enhanced_query: str
    confidence: float
    reasoning: Optional[str] = None
    expansion_applied: bool = True
    error: Optional[str] = None


class QueryExpander:
    """LLM 기반 쿼리 확장기"""

    def __init__(
        self,
        api_url: str = PROMPT_ENHANCEMENT_API_URL,
        model_name: str = PROMPT_ENHANCEMENT_MODEL,
        min_confidence: float = ENHANCEMENT_MIN_CONFIDENCE,
    ):
        """
        Args:
            api_url: LLM API base URL
            model_name: 모델명
            min_confidence: 확장 적용 최소 신뢰도
        """
        self.api_url = api_url.rstrip("/")
        self.model = model_name
        self.min_confidence = min_confidence

    async def expand(
        self,
        original_query: str,
        normalized_query: str,
        completeness: QueryCompletenessResult,
        context: Optional[str] = None,
    ) -> ExpansionResult:
        """쿼리를 확장합니다.

        Args:
            original_query: 사용자 원본 입력
            normalized_query: 정규화된 입력
            completeness: 완성도 분석 결과
            context: 추가 컨텍스트

        Returns:
            ExpansionResult: 확장 결과
        """
        # 완전한 질의는 확장 불필요
        if completeness.level == QueryCompletenessLevel.COMPLETE:
            return ExpansionResult(
                original_query=original_query,
                enhanced_query=normalized_query,
                confidence=1.0,
                reasoning="완전한 질의문으로 확장 불필요",
                expansion_applied=False,
            )

        # 프롬프트 구성
        user_prompt = build_expansion_prompt(
            original_query=original_query,
            normalized_query=normalized_query,
            completeness_level=completeness.level.value,
            missing_elements=completeness.missing_elements,
            inferred_intent=completeness.inferred_intent,
            medical_terms=completeness.detected_medical_terms,
            context=context,
        )

        try:
            response = await self._call_llm(user_prompt)
            result = self._parse_response(response, original_query, normalized_query)

            # 신뢰도 미달 시 확장 미적용
            if result.confidence < self.min_confidence:
                return ExpansionResult(
                    original_query=original_query,
                    enhanced_query=normalized_query,
                    confidence=result.confidence,
                    reasoning=f"신뢰도 {result.confidence:.2f} < 임계값 {self.min_confidence}",
                    expansion_applied=False,
                )

            return result

        except Exception as e:
            # 오류 시 원본 반환
            return ExpansionResult(
                original_query=original_query,
                enhanced_query=normalized_query,
                confidence=0.0,
                reasoning=None,
                expansion_applied=False,
                error=str(e),
            )

    async def _call_llm(self, user_prompt: str) -> str:
        """LLM API를 호출합니다.

        Args:
            user_prompt: 사용자 프롬프트

        Returns:
            LLM 응답 텍스트
        """
        async with httpx.AsyncClient(timeout=float(ENHANCEMENT_TIMEOUT)) as client:
            response = await client.post(
                f"{self.api_url}/chat/completions",
                headers={"Content-Type": "application/json"},
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": QUERY_EXPANSION_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    "max_tokens": ENHANCEMENT_MAX_TOKENS,
                    "temperature": ENHANCEMENT_TEMPERATURE,
                },
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

    def _parse_response(
        self,
        response: str,
        original_query: str,
        normalized_query: str,
    ) -> ExpansionResult:
        """LLM 응답을 파싱합니다.

        Args:
            response: LLM 응답 텍스트
            original_query: 원본 입력
            normalized_query: 정규화된 입력

        Returns:
            ExpansionResult: 파싱된 결과
        """
        # JSON 블록 추출
        json_match = re.search(r"```json\s*(.*?)\s*```", response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # JSON 블록이 없으면 전체를 JSON으로 시도
            json_str = response.strip()

        try:
            data = json.loads(json_str)
            return ExpansionResult(
                original_query=original_query,
                enhanced_query=data.get("enhanced_query", normalized_query),
                confidence=float(data.get("confidence", 0.5)),
                reasoning=data.get("reasoning"),
                expansion_applied=True,
            )
        except json.JSONDecodeError:
            # JSON 파싱 실패 시 응답에서 문장 추출 시도
            lines = response.strip().split("\n")
            for line in lines:
                line = line.strip()
                if line and not line.startswith("{") and "?" in line:
                    return ExpansionResult(
                        original_query=original_query,
                        enhanced_query=line,
                        confidence=0.5,
                        reasoning="JSON 파싱 실패, 문장 추출",
                        expansion_applied=True,
                    )

            # 추출 실패 시 원본 반환
            return ExpansionResult(
                original_query=original_query,
                enhanced_query=normalized_query,
                confidence=0.0,
                reasoning="응답 파싱 실패",
                expansion_applied=False,
                error=f"파싱 실패: {response[:100]}",
            )
