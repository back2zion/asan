"""
XiYanSQL 프롬프트 구성 및 SQL 추출 테스트

단위 테스트: _build_prompt, _extract_sql
OMOP CDM 기반 테이블/컬럼명 사용
"""

import pytest

from ai_services.xiyan_sql.service import XiYanSQLService
from ai_services.xiyan_sql.schema import get_omop_cdm_schema
from ai_services.xiyan_sql.config import XIYAN_SQL_DIALECT


# =============================================================================
# 단위 테스트: 프롬프트 구성
# =============================================================================

class TestBuildPrompt:
    """_build_prompt 테스트"""

    def setup_method(self):
        self.service = XiYanSQLService(
            api_url="http://test:8001/v1",
            model_name="test-model",
        )

    def test_basic_prompt_structure(self):
        """기본 프롬프트 구조 검증"""
        prompt = self.service._build_prompt(
            question="당뇨 환자 몇 명?",
            db_schema="【DB_ID】 test_db",
            evidence="",
        )

        assert f"你是一名{XIYAN_SQL_DIALECT}专家" in prompt
        assert "【用户问题】" in prompt
        assert "당뇨 환자 몇 명?" in prompt
        assert "【数据库schema】" in prompt
        assert "【DB_ID】 test_db" in prompt

    def test_prompt_with_evidence(self):
        """참조 정보(evidence) 포함 프롬프트"""
        prompt = self.service._build_prompt(
            question="당뇨 환자 몇 명?",
            db_schema="【DB_ID】 test_db",
            evidence="당뇨병 SNOMED CT: 44054006",
        )

        assert "【参考信息】" in prompt
        assert "당뇨병 SNOMED CT: 44054006" in prompt

    def test_prompt_without_evidence(self):
        """evidence 없을 때 참조 정보 섹션 미포함"""
        prompt = self.service._build_prompt(
            question="환자 목록",
            db_schema="【DB_ID】 test_db",
            evidence="",
        )

        assert "【参考信息】" not in prompt

    def test_prompt_with_context(self):
        """이전 대화 컨텍스트가 포함된 질의"""
        context_query = """## 이전 대화 컨텍스트
- 이전 질의: 당뇨 환자 몇 명?
- 실행된 SQL:
```sql
SELECT COUNT(DISTINCT co.person_id) FROM condition_occurrence co WHERE co.condition_source_value = '44054006'
```
- 결과 건수: 69건

## 현재 질의
그 중 남성만"""

        prompt = self.service._build_prompt(
            question=context_query,
            db_schema=get_omop_cdm_schema(),
            evidence="성별 조건: person.gender_source_value = 'M' 또는 person.gender_concept_id = 8507",
        )

        assert "이전 대화 컨텍스트" in prompt
        assert "당뇨 환자 몇 명?" in prompt
        assert "그 중 남성만" in prompt
        assert "gender_source_value = 'M'" in prompt


# =============================================================================
# 단위 테스트: SQL 추출
# =============================================================================

class TestExtractSQL:
    """_extract_sql 테스트"""

    def test_extract_from_sql_code_block(self):
        """```sql 블록에서 SQL 추출"""
        response = """다음은 당뇨 환자를 조회하는 SQL입니다:

```sql
SELECT COUNT(DISTINCT co.person_id) AS patient_count
FROM condition_occurrence co
INNER JOIN person p ON co.person_id = p.person_id
WHERE co.condition_source_value = '44054006'
```

이 쿼리는 당뇨병(SNOMED: 44054006) 환자 수를 조회합니다."""

        sql = XiYanSQLService._extract_sql(response)

        assert "SELECT COUNT(DISTINCT co.person_id)" in sql
        assert "condition_occurrence" in sql
        assert "condition_source_value = '44054006'" in sql

    def test_extract_from_generic_code_block(self):
        """언어 미지정 ``` 블록에서 SQL 추출"""
        response = """```
SELECT person_id, gender_source_value FROM person WHERE gender_source_value = 'M'
```"""

        sql = XiYanSQLService._extract_sql(response)
        assert "SELECT person_id" in sql
        assert "gender_source_value = 'M'" in sql

    def test_extract_bare_select(self):
        """코드 블록 없이 직접 SQL 문"""
        response = "SELECT COUNT(*) FROM person;"

        sql = XiYanSQLService._extract_sql(response)
        assert "SELECT COUNT(*) FROM person" in sql

    def test_extract_with_cte(self):
        """WITH CTE 포함 SQL 추출"""
        response = """```sql
WITH diabetes_patients AS (
    SELECT DISTINCT co.person_id
    FROM condition_occurrence co
    WHERE co.condition_source_value = '44054006'
)
SELECT COUNT(*) AS cnt
FROM diabetes_patients dp
INNER JOIN person p ON dp.person_id = p.person_id
WHERE p.gender_source_value = 'M'
```"""

        sql = XiYanSQLService._extract_sql(response)
        assert "WITH diabetes_patients AS" in sql
        assert "gender_source_value = 'M'" in sql

    def test_extract_fallback(self):
        """SQL 패턴이 없을 때 원본 반환"""
        response = "이 질문에 대한 SQL을 생성할 수 없습니다."
        sql = XiYanSQLService._extract_sql(response)
        assert sql == response.strip()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
