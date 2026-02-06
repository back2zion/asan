"""
Query Expander 테스트 (Mock LLM)
"""

import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from ai_services.prompt_enhancement.expander import QueryExpander, ExpansionResult
from ai_services.prompt_enhancement.detector import (
    QueryCompletenessLevel,
    QueryCompletenessResult,
)


@pytest.fixture
def expander():
    """테스트용 QueryExpander"""
    return QueryExpander(
        api_url="http://localhost:8888/v1",
        model_name="test-model",
        min_confidence=0.7,
    )


@pytest.fixture
def keyword_completeness():
    """KEYWORD_ONLY 완성도 결과"""
    return QueryCompletenessResult(
        level=QueryCompletenessLevel.KEYWORD_ONLY,
        missing_elements=["질문 표현", "문장 종결"],
        inferred_intent="환자 조회/집계",
        has_question_word=False,
        has_sentence_ending=False,
        word_count=2,
        detected_medical_terms=["당뇨", "환자"],
    )


@pytest.fixture
def complete_completeness():
    """COMPLETE 완성도 결과"""
    return QueryCompletenessResult(
        level=QueryCompletenessLevel.COMPLETE,
        missing_elements=[],
        inferred_intent="환자 조회/집계",
        has_question_word=True,
        has_sentence_ending=True,
        word_count=5,
        detected_medical_terms=["당뇨병", "환자"],
    )


class TestQueryExpanderComplete:
    """완전한 질의에 대한 테스트"""

    @pytest.mark.asyncio
    async def test_complete_query_no_expansion(self, expander, complete_completeness):
        """완전한 질의는 확장하지 않음"""
        result = await expander.expand(
            original_query="당뇨병 환자는 몇 명입니까?",
            normalized_query="당뇨병 환자는 몇 명입니까?",
            completeness=complete_completeness,
        )

        assert not result.expansion_applied
        assert result.confidence == 1.0
        assert result.enhanced_query == "당뇨병 환자는 몇 명입니까?"


class TestQueryExpanderWithMock:
    """Mock LLM을 사용한 확장 테스트"""

    @pytest.mark.asyncio
    async def test_successful_expansion(self, expander, keyword_completeness):
        """성공적인 확장"""
        mock_response = {
            "choices": [{
                "message": {
                    "content": json.dumps({
                        "enhanced_query": "제2형 당뇨병 진단을 받은 환자는 몇 명입니까?",
                        "confidence": 0.9,
                        "reasoning": "당뇨 키워드를 제2형 당뇨병으로 확장하고 환자 수 조회 의도 반영"
                    })
                }
            }]
        }

        with patch.object(expander, "_call_llm", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = json.dumps({
                "enhanced_query": "제2형 당뇨병 진단을 받은 환자는 몇 명입니까?",
                "confidence": 0.9,
                "reasoning": "당뇨 키워드를 제2형 당뇨병으로 확장"
            })

            result = await expander.expand(
                original_query="당뇨 환자",
                normalized_query="당뇨병 환자",
                completeness=keyword_completeness,
            )

            assert result.expansion_applied
            assert result.confidence == 0.9
            assert "당뇨병" in result.enhanced_query
            assert "몇 명" in result.enhanced_query

    @pytest.mark.asyncio
    async def test_low_confidence_no_expansion(self, expander, keyword_completeness):
        """신뢰도 미달 시 확장 미적용"""
        with patch.object(expander, "_call_llm", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = json.dumps({
                "enhanced_query": "당뇨병 관련 정보를 조회합니다",
                "confidence": 0.5,
                "reasoning": "불명확한 의도"
            })

            result = await expander.expand(
                original_query="당뇨",
                normalized_query="당뇨병",
                completeness=keyword_completeness,
            )

            assert not result.expansion_applied
            assert result.confidence == 0.5
            assert result.enhanced_query == "당뇨병"  # 정규화된 쿼리 유지

    @pytest.mark.asyncio
    async def test_json_in_code_block(self, expander, keyword_completeness):
        """코드 블록 내 JSON 응답 파싱"""
        with patch.object(expander, "_call_llm", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = """```json
{
    "enhanced_query": "고혈압 진단을 받고 현재 입원 중인 환자는 몇 명입니까?",
    "confidence": 0.85,
    "reasoning": "고혈압과 입원 키워드 조합"
}
```"""

            result = await expander.expand(
                original_query="고혈압 입원",
                normalized_query="고혈압 입원 환자",
                completeness=keyword_completeness,
            )

            assert result.expansion_applied
            assert result.confidence == 0.85

    @pytest.mark.asyncio
    async def test_llm_error_fallback(self, expander, keyword_completeness):
        """LLM 오류 시 원본 반환"""
        with patch.object(expander, "_call_llm", new_callable=AsyncMock) as mock_call:
            mock_call.side_effect = Exception("Connection error")

            result = await expander.expand(
                original_query="당뇨 환자",
                normalized_query="당뇨병 환자",
                completeness=keyword_completeness,
            )

            assert not result.expansion_applied
            assert result.enhanced_query == "당뇨병 환자"
            assert result.error is not None
            assert "Connection error" in result.error

    @pytest.mark.asyncio
    async def test_invalid_json_response(self, expander, keyword_completeness):
        """유효하지 않은 JSON 응답 처리"""
        with patch.object(expander, "_call_llm", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = "이것은 JSON이 아닙니다."

            result = await expander.expand(
                original_query="당뇨 환자",
                normalized_query="당뇨병 환자",
                completeness=keyword_completeness,
            )

            # 파싱 실패 시 신뢰도 0으로 확장 미적용
            assert not result.expansion_applied
            assert result.confidence == 0.0
            assert result.enhanced_query == "당뇨병 환자"  # 정규화된 쿼리 유지


class TestQueryExpanderParsing:
    """응답 파싱 테스트"""

    def test_parse_valid_json(self, expander):
        """유효한 JSON 파싱"""
        response = json.dumps({
            "enhanced_query": "테스트 질의입니까?",
            "confidence": 0.8,
            "reasoning": "테스트"
        })

        result = expander._parse_response(response, "원본", "정규화")

        assert result.enhanced_query == "테스트 질의입니까?"
        assert result.confidence == 0.8
        assert result.expansion_applied

    def test_parse_json_in_markdown(self, expander):
        """마크다운 코드 블록 내 JSON 파싱"""
        response = """```json
{
    "enhanced_query": "테스트 질의입니까?",
    "confidence": 0.75
}
```"""

        result = expander._parse_response(response, "원본", "정규화")

        assert result.enhanced_query == "테스트 질의입니까?"
        assert result.confidence == 0.75

    def test_parse_fallback_sentence_extraction(self, expander):
        """JSON 실패 시 문장 추출"""
        response = "확장된 질의: 당뇨병 환자는 몇 명입니까?"

        result = expander._parse_response(response, "원본", "정규화")

        assert "?" in result.enhanced_query
        assert result.confidence == 0.5  # 기본값
