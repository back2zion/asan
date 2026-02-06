"""
Query Completeness Detector 테스트
"""

import pytest

from ai_services.prompt_enhancement.detector import (
    QueryCompletenessLevel,
    detect_completeness,
)


class TestDetectCompleteness:
    """detect_completeness 함수 테스트"""

    def test_keyword_only_simple(self):
        """단순 키워드만 있는 경우"""
        result = detect_completeness("당뇨 환자")

        assert result.level == QueryCompletenessLevel.KEYWORD_ONLY
        assert result.word_count == 2
        assert "당뇨" in result.detected_medical_terms
        assert "환자" in result.detected_medical_terms
        assert not result.has_question_word
        assert not result.has_sentence_ending

    def test_keyword_only_medical_abbreviation(self):
        """의료 약어 키워드"""
        result = detect_completeness("DM 입원")

        assert result.level == QueryCompletenessLevel.KEYWORD_ONLY
        assert "입원" in result.detected_medical_terms

    def test_incomplete_trailing_particle(self):
        """조사로 끝나는 불완전 문장"""
        result = detect_completeness("당뇨 환자 중")

        assert result.level == QueryCompletenessLevel.INCOMPLETE
        assert "문장 종결" in result.missing_elements

    def test_incomplete_with_question_word(self):
        """질문어는 있지만 종결어미 없음"""
        result = detect_completeness("당뇨 환자 몇 명")

        assert result.level == QueryCompletenessLevel.INCOMPLETE
        assert result.has_question_word
        assert not result.has_sentence_ending

    def test_complete_question(self):
        """완전한 질문문"""
        result = detect_completeness("당뇨병 환자는 몇 명입니까?")

        assert result.level == QueryCompletenessLevel.COMPLETE
        assert result.has_question_word
        assert result.has_sentence_ending

    def test_complete_with_polite_ending(self):
        """존댓말 종결어미"""
        result = detect_completeness("고혈압 환자 목록을 보여주세요")

        # "보여" + "주세요" 패턴
        assert result.level == QueryCompletenessLevel.COMPLETE

    def test_complete_request_form(self):
        """요청 형태의 완전한 문장"""
        result = detect_completeness("입원 환자 목록 조회해 주세요")

        assert result.level == QueryCompletenessLevel.COMPLETE

    def test_inferred_intent_patient_query(self):
        """환자 조회 의도 추론"""
        result = detect_completeness("당뇨 환자")

        assert result.inferred_intent == "환자 조회/집계"

    def test_inferred_intent_admission(self):
        """입원 환자 조회 의도 추론"""
        result = detect_completeness("고혈압 입원")

        assert result.inferred_intent == "입원 환자 조회"

    def test_inferred_intent_outpatient(self):
        """외래 환자 조회 의도 추론"""
        result = detect_completeness("당뇨 외래")

        assert result.inferred_intent == "외래 환자 조회"

    def test_inferred_intent_cancer(self):
        """암 환자 조회 의도 추론"""
        result = detect_completeness("위암")

        assert result.inferred_intent == "암 환자 조회"

    def test_medical_terms_detection(self):
        """의료 용어 탐지"""
        result = detect_completeness("당뇨병 고혈압 환자 입원")

        assert "당뇨" in result.detected_medical_terms or "당뇨병" in result.detected_medical_terms
        assert "고혈압" in result.detected_medical_terms
        assert "환자" in result.detected_medical_terms
        assert "입원" in result.detected_medical_terms

    def test_empty_query(self):
        """빈 쿼리"""
        result = detect_completeness("")

        assert result.word_count == 0
        assert result.level == QueryCompletenessLevel.KEYWORD_ONLY

    def test_question_mark_ending(self):
        """물음표로 끝나는 경우"""
        result = detect_completeness("환자는 몇 명?")

        assert result.has_sentence_ending
        assert result.has_question_word

    def test_period_ending(self):
        """마침표로 끝나는 경우"""
        result = detect_completeness("당뇨 환자를 조회합니다.")

        assert result.has_sentence_ending


class TestCompletenessEdgeCases:
    """엣지 케이스 테스트"""

    def test_only_question_word(self):
        """질문어만 있는 경우"""
        result = detect_completeness("몇 명")

        assert result.has_question_word
        assert not result.detected_medical_terms

    def test_long_incomplete_sentence(self):
        """긴 불완전 문장"""
        result = detect_completeness("제2형 당뇨병 진단을 받은 환자 중에서 현재 입원 중인")

        assert result.level == QueryCompletenessLevel.INCOMPLETE

    def test_mixed_language(self):
        """영어 약어 혼용"""
        result = detect_completeness("COPD 환자")

        assert "COPD" in result.detected_medical_terms

    def test_multiple_diseases(self):
        """복수 질환 언급"""
        result = detect_completeness("당뇨 고혈압 암")

        assert len(result.detected_medical_terms) >= 3
