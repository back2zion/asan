"""
Phase 3: LLM Service 단위 테스트
"""
import pytest
from unittest.mock import AsyncMock, patch


class TestRuleBasedIntent:
    """규칙 기반 의도 추출 (LLM 없이 동작하는 순수 로직)"""

    @pytest.fixture()
    def service(self):
        with patch.dict("os.environ", {"LLM_PROVIDER": "local"}):
            from services.llm_service import LLMService
            return LLMService()

    def test_count_intent(self, service):
        result = service._rule_based_intent("당뇨 환자가 몇 명인가요?")
        assert result.action == "COUNT"
        assert "환자" in result.entities

    def test_count_intent_with_건수(self, service):
        result = service._rule_based_intent("입원 건수를 알려주세요")
        assert result.action == "COUNT"
        assert "입원" in result.entities

    def test_sum_intent(self, service):
        result = service._rule_based_intent("총 비용 합계를 보여주세요")
        assert result.action == "SUM"

    def test_avg_intent(self, service):
        result = service._rule_based_intent("평균 입원 기간은?")
        assert result.action == "AVG"
        assert "입원" in result.entities

    def test_list_intent(self, service):
        result = service._rule_based_intent("환자 목록을 조회해주세요")
        assert result.action == "LIST"
        assert "환자" in result.entities

    def test_entity_detection_진단(self, service):
        result = service._rule_based_intent("진단 데이터를 보여주세요")
        assert "진단" in result.entities

    def test_entity_detection_검사(self, service):
        result = service._rule_based_intent("검사 결과를 조회해주세요")
        assert "검사" in result.entities

    def test_entity_detection_외래(self, service):
        result = service._rule_based_intent("외래 환자 수를 알려주세요")
        assert "외래" in result.entities
        assert "환자" in result.entities

    def test_default_action_is_list(self, service):
        result = service._rule_based_intent("데이터를 보여주세요")
        assert result.action == "LIST"

    def test_confidence_is_valid(self, service):
        result = service._rule_based_intent("환자 몇 명?")
        assert 0.0 <= result.confidence <= 1.0


class TestExtractIntentWithLLMFallback:
    """LLM 호출 실패 시 규칙 기반 폴백 테스트"""

    async def test_fallback_on_llm_failure(self):
        with patch.dict("os.environ", {"LLM_PROVIDER": "local"}):
            from services.llm_service import LLMService
            service = LLMService()
            service._call_llm = AsyncMock(side_effect=Exception("LLM unavailable"))
            result = await service.extract_intent("환자 몇 명?")
            assert result.action == "COUNT"

    async def test_fallback_on_invalid_json(self):
        with patch.dict("os.environ", {"LLM_PROVIDER": "local"}):
            from services.llm_service import LLMService
            service = LLMService()
            service._call_llm = AsyncMock(return_value="This is not JSON at all")
            result = await service.extract_intent("환자 목록 조회")
            # fallback으로 규칙 기반 사용
            assert result.action == "LIST"
