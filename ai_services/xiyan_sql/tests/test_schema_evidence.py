"""
XiYanSQL 스키마 및 Evidence 테스트

단위 테스트: OMOP CDM 스키마 구조, M-Schema 형식, evidence 추출
"""

import pytest

from ai_services.xiyan_sql.schema import get_omop_cdm_schema, get_evidence_for_query


# =============================================================================
# 단위 테스트: 스키마 및 Evidence
# =============================================================================

class TestSchema:
    """스키마 및 evidence 테스트"""

    def test_schema_contains_all_tables(self):
        """OMOP CDM 핵심 테이블 포함 확인"""
        schema = get_omop_cdm_schema()

        for table in ["person", "condition_occurrence", "visit_occurrence",
                       "drug_exposure", "measurement", "observation",
                       "procedure_occurrence", "imaging_study"]:
            assert table in schema

    def test_schema_contains_fk(self):
        """FK 관계 포함 확인"""
        schema = get_omop_cdm_schema()

        assert "condition_occurrence.person_id = person.person_id" in schema
        assert "visit_occurrence.person_id = person.person_id" in schema
        assert "drug_exposure.person_id = person.person_id" in schema
        assert "measurement.person_id = person.person_id" in schema

    def test_schema_m_schema_format(self):
        """M-Schema 형식 준수 확인"""
        schema = get_omop_cdm_schema()

        assert "【DB_ID】" in schema
        assert "【표(Table)】" in schema
        assert "【외래키(FK)】" in schema

    def test_evidence_for_diabetes_query(self):
        """당뇨 질의에 대한 evidence 추출"""
        evidence = get_evidence_for_query("당뇨 환자 몇 명?")
        assert "44054006" in evidence
        assert "condition_source_value" in evidence

    def test_evidence_for_gender_query(self):
        """성별 질의에 대한 evidence 추출"""
        evidence = get_evidence_for_query("남성 환자 목록")
        assert "gender_source_value = 'M'" in evidence

    def test_evidence_for_combined_query(self):
        """복합 질의에 대한 evidence 추출 (다중 매칭)"""
        evidence = get_evidence_for_query("당뇨 환자 중 남성 입원 환자")
        assert "44054006" in evidence
        assert "gender_source_value = 'M'" in evidence
        assert "visit_concept_id = 9201" in evidence

    def test_evidence_no_match(self):
        """매칭 키워드 없을 때 빈 문자열"""
        evidence = get_evidence_for_query("전체 데이터 조회")
        assert evidence == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
