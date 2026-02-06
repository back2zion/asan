"""
한국어 참조 패턴 테스트
"""

import pytest

from ai_services.conversation_memory.state.conversation_state import ReferenceType
from ai_services.conversation_memory.utils.korean_patterns import (
    KOREAN_REFERENCE_PATTERNS,
    detect_korean_references,
    has_any_reference,
    get_reference_types,
)


class TestKoreanReferencePatterns:
    """한국어 참조 패턴 테스트 클래스"""

    def test_temporal_references(self):
        """시간적 참조 표현 테스트"""
        test_cases = [
            ("방금 조회한 환자 보여줘", ReferenceType.TEMPORAL),
            ("이전 결과에서 남성만", ReferenceType.TEMPORAL),
            ("아까 검색한 데이터", ReferenceType.TEMPORAL),
            ("직전 쿼리 다시 실행", ReferenceType.TEMPORAL),
            ("앞서 조회한 목록에서", ReferenceType.TEMPORAL),
        ]

        for query, expected_type in test_cases:
            refs = detect_korean_references(query)
            assert len(refs) > 0, f"Failed to detect: {query}"
            assert refs[0].ref_type == expected_type, f"Wrong type for: {query}"

    def test_entity_references(self):
        """엔티티 참조 표현 테스트"""
        test_cases = [
            ("그 환자의 검사 결과", ReferenceType.ENTITY),
            ("해당 환자들의 진단명", ReferenceType.ENTITY),
            ("위 환자 중에서", ReferenceType.ENTITY),
            ("그 사람들 나이", ReferenceType.ENTITY),
            ("해당 케이스 분석", ReferenceType.ENTITY),
        ]

        for query, expected_type in test_cases:
            refs = detect_korean_references(query)
            assert len(refs) > 0, f"Failed to detect: {query}"
            assert refs[0].ref_type == expected_type, f"Wrong type for: {query}"

    def test_result_references(self):
        """결과 참조 표현 테스트"""
        test_cases = [
            ("그 중에서 65세 이상만", ReferenceType.RESULT),
            ("거기서 남성만 필터링", ReferenceType.RESULT),
            ("위 결과에서 당뇨 환자", ReferenceType.RESULT),
            ("조회된 결과에서 추가 필터", ReferenceType.RESULT),
        ]

        for query, expected_type in test_cases:
            refs = detect_korean_references(query)
            assert len(refs) > 0, f"Failed to detect: {query}"
            assert refs[0].ref_type == expected_type, f"Wrong type for: {query}"

    def test_condition_references(self):
        """조건 추가 표현 테스트"""
        test_cases = [
            ("추가로 고혈압도 있는", ReferenceType.CONDITION),
            ("더 좁혀서 내분비내과만", ReferenceType.CONDITION),
            ("그리고 HbA1c 7 이상", ReferenceType.CONDITION),
            ("조건 추가해줘", ReferenceType.CONDITION),
        ]

        for query, expected_type in test_cases:
            refs = detect_korean_references(query)
            assert len(refs) > 0, f"Failed to detect: {query}"
            assert refs[0].ref_type == expected_type, f"Wrong type for: {query}"

    def test_no_references(self):
        """참조 없는 질의 테스트"""
        queries = [
            "당뇨 환자 몇 명이야?",
            "2024년 입원 환자 조회",
            "내분비내과 외래 환자",
            "HbA1c 7 이상인 환자",
        ]

        for query in queries:
            refs = detect_korean_references(query)
            # 참조가 없거나 매우 적어야 함
            assert len(refs) == 0, f"Unexpected reference in: {query}"

    def test_multiple_references(self):
        """여러 참조가 포함된 질의 테스트"""
        query = "방금 조회한 그 환자들 중에서 추가로 고혈압 있는 사람"
        refs = detect_korean_references(query)

        # 여러 유형의 참조가 탐지되어야 함
        ref_types = {ref.ref_type for ref in refs}
        assert len(ref_types) >= 2, f"Expected multiple reference types, got: {ref_types}"

    def test_has_any_reference(self):
        """has_any_reference 함수 테스트"""
        assert has_any_reference("방금 조회한 결과") is True
        assert has_any_reference("그 환자 목록") is True
        assert has_any_reference("당뇨 환자 조회") is False

    def test_get_reference_types(self):
        """get_reference_types 함수 테스트"""
        query = "방금 조회한 그 환자들 중에서"
        types = get_reference_types(query)

        assert ReferenceType.TEMPORAL in types or ReferenceType.ENTITY in types or ReferenceType.RESULT in types

    def test_reference_position(self):
        """참조 표현 위치 정확성 테스트"""
        query = "방금 조회한 환자"
        refs = detect_korean_references(query)

        assert len(refs) > 0
        ref = refs[0]
        assert ref.position == 0, "Reference should be at the beginning"
        assert ref.original_text in query


class TestIntegrationScenarios:
    """통합 시나리오 테스트"""

    def test_multi_turn_scenario(self):
        """다중 턴 대화 시나리오 테스트"""
        turns = [
            ("당뇨 환자 몇 명이야?", False),  # 첫 턴 - 참조 없음
            ("그 중 남성만", True),           # 두 번째 턴 - 참조 있음
            ("65세 이상만 보여줘", False),    # 세 번째 턴 - 참조 없음 (독립 질의)
            ("방금 조회한 환자 중 고혈압도 있는", True),  # 네 번째 턴 - 참조 있음
        ]

        for query, expected_has_ref in turns:
            has_ref = has_any_reference(query)
            assert has_ref == expected_has_ref, f"Query: {query}, Expected: {expected_has_ref}, Got: {has_ref}"

    def test_realistic_medical_queries(self):
        """실제 의료 질의 시나리오 테스트"""
        # 첫 번째 질의: 당뇨 환자 조회
        q1 = "2024년 당뇨병으로 진단받은 환자 목록"
        assert has_any_reference(q1) is False

        # 두 번째 질의: 결과 필터링
        q2 = "그 중에서 HbA1c 7 이상인 환자만"
        refs = detect_korean_references(q2)
        assert len(refs) > 0
        assert ReferenceType.RESULT in {r.ref_type for r in refs}

        # 세 번째 질의: 추가 조건
        q3 = "해당 환자들의 진료과별 분포"
        refs = detect_korean_references(q3)
        assert len(refs) > 0
        assert ReferenceType.ENTITY in {r.ref_type for r in refs}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
