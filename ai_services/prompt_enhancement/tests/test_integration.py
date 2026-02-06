"""
Prompt Enhancement 통합 테스트
"""

import json
import pytest
from unittest.mock import AsyncMock, patch

from ai_services.prompt_enhancement import (
    PromptEnhancementService,
    PromptEnhancementResult,
    QueryCompletenessLevel,
    normalize_medical_terms,
)
from ai_services.prompt_enhancement.expander import QueryExpander
from ai_services.prompt_enhancement.medical_normalizer import MedicalNormalizer


@pytest.fixture
def service():
    """테스트용 PromptEnhancementService"""
    return PromptEnhancementService(enabled=True)


@pytest.fixture
def disabled_service():
    """비활성화된 서비스"""
    return PromptEnhancementService(enabled=False)


class TestPromptEnhancementService:
    """PromptEnhancementService 통합 테스트"""

    @pytest.mark.asyncio
    async def test_complete_query_no_llm_call(self, service):
        """완전한 질의는 LLM 호출 없이 처리"""
        result = await service.enhance("당뇨병 환자는 몇 명입니까?")

        assert result.original_query == "당뇨병 환자는 몇 명입니까?"
        assert result.completeness.level == QueryCompletenessLevel.COMPLETE
        # 완전한 질의는 LLM 확장 없음
        assert result.expansion is None

    @pytest.mark.asyncio
    async def test_disabled_service(self, disabled_service):
        """비활성화된 서비스는 정규화만 수행"""
        result = await disabled_service.enhance("DM환자")

        assert result.enhanced_query == "당뇨병 환자"  # 정규화만 적용
        assert result.normalization.has_changes
        assert result.reasoning == "기능 비활성화"

    @pytest.mark.asyncio
    async def test_normalization_applied(self, service):
        """의료 용어 정규화 적용 확인"""
        # 완전한 질의로 테스트 (LLM 호출 없이)
        result = await service.enhance("DM환자는 몇 명입니까?")

        # 정규화 확인
        assert result.normalization.has_changes
        assert ("DM환자", "당뇨병 환자") in result.normalization.replacements

    @pytest.mark.asyncio
    async def test_keyword_query_with_mock_llm(self, service):
        """키워드만 있는 경우 LLM 확장"""
        with patch.object(
            service.expander, "_call_llm", new_callable=AsyncMock
        ) as mock_call:
            mock_call.return_value = json.dumps({
                "enhanced_query": "제2형 당뇨병 진단을 받은 환자는 몇 명입니까?",
                "confidence": 0.9,
                "reasoning": "환자 수 조회 의도로 확장"
            })

            result = await service.enhance("당뇨 환자")

            assert result.enhancement_applied
            assert result.confidence == 0.9
            assert "몇 명" in result.enhanced_query
            assert result.expansion is not None

    @pytest.mark.asyncio
    async def test_incomplete_query_with_mock_llm(self, service):
        """불완전한 문장 확장"""
        with patch.object(
            service.expander, "_call_llm", new_callable=AsyncMock
        ) as mock_call:
            mock_call.return_value = json.dumps({
                "enhanced_query": "고혈압 진단을 받고 현재 입원 중인 환자는 몇 명입니까?",
                "confidence": 0.85,
                "reasoning": "입원 환자 조회 의도로 확장"
            })

            result = await service.enhance("고혈압 입원")

            assert result.enhancement_applied
            assert result.completeness.level in [
                QueryCompletenessLevel.KEYWORD_ONLY,
                QueryCompletenessLevel.INCOMPLETE
            ]

    @pytest.mark.asyncio
    async def test_with_context(self, service):
        """컨텍스트와 함께 확장"""
        with patch.object(
            service.expander, "_call_llm", new_callable=AsyncMock
        ) as mock_call:
            mock_call.return_value = json.dumps({
                "enhanced_query": "이전 조회 결과 중 남성 환자는 몇 명입니까?",
                "confidence": 0.88,
                "reasoning": "이전 컨텍스트 참조"
            })

            result = await service.enhance(
                "그 중 남성",
                context="이전 질의: 당뇨병 환자 조회"
            )

            assert result.enhancement_applied


class TestMedicalNormalization:
    """의료 용어 정규화 단독 테스트"""

    def test_normalize_abbreviations(self):
        """약어 정규화"""
        assert normalize_medical_terms("DM환자") == "당뇨병 환자"
        assert normalize_medical_terms("HTN") == "고혈압"
        assert normalize_medical_terms("COPD") == "COPD"  # COPD는 SYNONYM_MAP에 없음

    def test_normalize_colloquial(self):
        """구어체 정규화"""
        assert normalize_medical_terms("입원환자") == "입원 환자"
        assert normalize_medical_terms("외래환자") == "외래 환자"
        assert normalize_medical_terms("피검사") == "임상검사"

    def test_normalize_combined(self):
        """복합 정규화"""
        result = normalize_medical_terms("DM환자 HTN 입원환자")
        assert "당뇨병 환자" in result
        assert "고혈압" in result
        assert "입원 환자" in result

    def test_no_change_for_standard(self):
        """표준 용어는 변경 없음"""
        assert normalize_medical_terms("당뇨병 환자") == "당뇨병 환자"
        assert normalize_medical_terms("고혈압") == "고혈압"


class TestServiceSync:
    """동기 인터페이스 테스트"""

    def test_enhance_sync_complete(self):
        """동기 호출 - 완전한 질의"""
        service = PromptEnhancementService(enabled=True)
        result = service.enhance_sync("당뇨병 환자는 몇 명입니까?")

        assert result.original_query == "당뇨병 환자는 몇 명입니까?"
        assert result.completeness.level == QueryCompletenessLevel.COMPLETE

    def test_enhance_sync_disabled(self):
        """동기 호출 - 비활성화"""
        service = PromptEnhancementService(enabled=False)
        result = service.enhance_sync("DM환자")

        assert result.enhanced_query == "당뇨병 환자"


class TestEndToEndScenarios:
    """E2E 시나리오 테스트"""

    @pytest.mark.asyncio
    async def test_scenario_diabetes_patient(self):
        """시나리오: 당뇨 환자 → 완전한 질의"""
        service = PromptEnhancementService(enabled=True)

        with patch.object(
            service.expander, "_call_llm", new_callable=AsyncMock
        ) as mock_call:
            mock_call.return_value = json.dumps({
                "enhanced_query": "제2형 당뇨병 진단을 받은 환자는 몇 명입니까?",
                "confidence": 0.9,
                "reasoning": "당뇨 키워드를 제2형 당뇨병으로 확장, 환자 수 조회 의도"
            })

            result = await service.enhance("당뇨 환자")

            # 검증
            assert result.original_query == "당뇨 환자"
            assert "제2형 당뇨병" in result.enhanced_query
            assert result.enhancement_applied
            assert result.completeness.inferred_intent == "환자 조회/집계"

    @pytest.mark.asyncio
    async def test_scenario_hypertension_admission(self):
        """시나리오: 고혈압 입원 → 완전한 질의"""
        service = PromptEnhancementService(enabled=True)

        with patch.object(
            service.expander, "_call_llm", new_callable=AsyncMock
        ) as mock_call:
            mock_call.return_value = json.dumps({
                "enhanced_query": "고혈압 진단을 받고 현재 입원 중인 환자는 몇 명입니까?",
                "confidence": 0.88,
                "reasoning": "고혈압 + 입원 키워드 조합"
            })

            result = await service.enhance("고혈압 입원")

            assert result.original_query == "고혈압 입원"
            assert "입원" in result.enhanced_query
            assert "환자" in result.enhanced_query
            assert result.enhancement_applied

    @pytest.mark.asyncio
    async def test_scenario_abbreviation_to_full_query(self):
        """시나리오: DM환자 → 정규화 + 확장"""
        service = PromptEnhancementService(enabled=True)

        with patch.object(
            service.expander, "_call_llm", new_callable=AsyncMock
        ) as mock_call:
            mock_call.return_value = json.dumps({
                "enhanced_query": "제2형 당뇨병 진단을 받은 환자는 몇 명입니까?",
                "confidence": 0.92,
                "reasoning": "DM을 당뇨병으로 정규화 후 질문 형태로 확장"
            })

            result = await service.enhance("DM환자")

            # 정규화 확인
            assert result.normalization.has_changes
            # 확장 확인
            assert result.enhancement_applied
            assert "당뇨병" in result.enhanced_query
