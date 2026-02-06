"""
SQL 유틸리티 테스트
"""

import pytest

from ai_services.conversation_memory.utils.sql_utils import (
    add_where_condition,
    create_subquery_filter,
    extract_table_names,
    extract_where_conditions,
    merge_sql_conditions,
    create_cte_with_previous_result,
)


class TestAddWhereCondition:
    """add_where_condition 함수 테스트"""

    def test_add_condition_no_existing_where(self):
        """WHERE 절 없는 SQL에 조건 추가"""
        sql = "SELECT * FROM person"
        result = add_where_condition(sql, "sex_cd = 'M'")

        assert "WHERE sex_cd = 'M'" in result

    def test_add_condition_existing_where(self):
        """기존 WHERE 절에 AND 조건 추가"""
        sql = "SELECT * FROM person WHERE age > 20"
        result = add_where_condition(sql, "sex_cd = 'M'")

        assert "WHERE age > 20" in result
        assert "AND sex_cd = 'M'" in result

    def test_add_condition_with_or(self):
        """OR 연산자로 조건 추가"""
        sql = "SELECT * FROM person WHERE age > 20"
        result = add_where_condition(sql, "age < 10", operator="OR")

        assert "OR age < 10" in result

    def test_add_condition_with_group_by(self):
        """GROUP BY 앞에 조건 추가"""
        sql = "SELECT dept, COUNT(*) FROM person GROUP BY dept"
        result = add_where_condition(sql, "age > 20")

        assert "WHERE age > 20" in result
        assert result.index("WHERE") < result.index("GROUP BY")

    def test_add_condition_with_order_by(self):
        """ORDER BY 앞에 조건 추가"""
        sql = "SELECT * FROM person WHERE age > 20 ORDER BY name"
        result = add_where_condition(sql, "sex_cd = 'M'")

        assert result.index("sex_cd = 'M'") < result.index("ORDER BY")

    def test_add_condition_removes_semicolon(self):
        """세미콜론 제거 테스트"""
        sql = "SELECT * FROM person;"
        result = add_where_condition(sql, "age > 20")

        assert result.endswith("age > 20") or ";" not in result


class TestCreateSubqueryFilter:
    """create_subquery_filter 함수 테스트"""

    def test_basic_subquery_filter(self):
        """기본 서브쿼리 필터 생성"""
        base_sql = "SELECT * FROM condition_occurrence"
        subquery = "SELECT person_id FROM person WHERE age > 65"

        result = create_subquery_filter(
            base_sql,
            "person_id",
            subquery,
            "person_id",
        )

        assert "person_id IN" in result
        assert "SELECT person_id FROM person WHERE age > 65" in result

    def test_subquery_filter_with_existing_where(self):
        """기존 WHERE 절이 있는 SQL에 서브쿼리 필터 추가"""
        base_sql = "SELECT * FROM condition_occurrence WHERE condition_concept_id = 201826"
        subquery = "SELECT person_id FROM person WHERE age > 65"

        result = create_subquery_filter(
            base_sql,
            "person_id",
            subquery,
            "person_id",
        )

        assert "condition_concept_id = 201826" in result
        assert "person_id IN" in result


class TestExtractTableNames:
    """extract_table_names 함수 테스트"""

    def test_extract_single_table(self):
        """단일 테이블 추출"""
        sql = "SELECT * FROM person"
        tables = extract_table_names(sql)

        assert "person" in tables

    def test_extract_joined_tables(self):
        """JOIN된 테이블 추출"""
        sql = """
        SELECT p.*, c.condition_concept_id
        FROM person p
        JOIN condition_occurrence c ON p.person_id = c.person_id
        """
        tables = extract_table_names(sql)

        assert "person" in tables
        assert "condition_occurrence" in tables

    def test_extract_multiple_joins(self):
        """다중 JOIN 테이블 추출"""
        sql = """
        SELECT *
        FROM person p
        JOIN condition_occurrence c ON p.person_id = c.person_id
        LEFT JOIN drug_exposure d ON p.person_id = d.person_id
        """
        tables = extract_table_names(sql)

        assert len(tables) == 3
        assert "person" in tables
        assert "condition_occurrence" in tables
        assert "drug_exposure" in tables


class TestExtractWhereConditions:
    """extract_where_conditions 함수 테스트"""

    def test_extract_single_condition(self):
        """단일 조건 추출"""
        sql = "SELECT * FROM person WHERE age > 20"
        conditions = extract_where_conditions(sql)

        assert len(conditions) == 1
        assert "age > 20" in conditions

    def test_extract_multiple_conditions(self):
        """다중 조건 추출"""
        sql = "SELECT * FROM person WHERE age > 20 AND sex_cd = 'M'"
        conditions = extract_where_conditions(sql)

        assert len(conditions) == 2

    def test_extract_no_conditions(self):
        """조건 없는 SQL"""
        sql = "SELECT * FROM person"
        conditions = extract_where_conditions(sql)

        assert len(conditions) == 0

    def test_extract_conditions_with_group_by(self):
        """GROUP BY 앞까지만 조건 추출"""
        sql = "SELECT * FROM person WHERE age > 20 GROUP BY dept"
        conditions = extract_where_conditions(sql)

        assert len(conditions) == 1
        assert "age > 20" in conditions


class TestMergeSqlConditions:
    """merge_sql_conditions 함수 테스트"""

    def test_merge_single_condition(self):
        """단일 조건 병합"""
        sql = "SELECT * FROM person"
        conditions = ["age > 20"]

        result = merge_sql_conditions(sql, conditions)

        assert "WHERE age > 20" in result

    def test_merge_multiple_conditions(self):
        """다중 조건 병합"""
        sql = "SELECT * FROM person"
        conditions = ["age > 20", "sex_cd = 'M'"]

        result = merge_sql_conditions(sql, conditions)

        assert "age > 20" in result
        assert "sex_cd = 'M'" in result


class TestCreateCteWithPreviousResult:
    """create_cte_with_previous_result 함수 테스트"""

    def test_basic_cte(self):
        """기본 CTE 생성"""
        prev_sql = "SELECT person_id FROM person WHERE age > 65"
        curr_sql = "SELECT * FROM condition_occurrence"

        result = create_cte_with_previous_result(curr_sql, prev_sql)

        assert "WITH previous_result AS" in result
        assert "SELECT person_id FROM person WHERE age > 65" in result
        assert "person_id IN (SELECT person_id FROM previous_result)" in result

    def test_cte_custom_name(self):
        """커스텀 CTE 이름"""
        prev_sql = "SELECT person_id FROM person WHERE age > 65"
        curr_sql = "SELECT * FROM condition_occurrence"

        result = create_cte_with_previous_result(
            curr_sql,
            prev_sql,
            cte_name="elderly_patients",
        )

        assert "WITH elderly_patients AS" in result
        assert "FROM elderly_patients" in result


class TestIntegrationScenarios:
    """통합 시나리오 테스트"""

    def test_multi_turn_filter_chain(self):
        """다중 턴 필터 체인 시나리오"""
        # Turn 1: 당뇨 환자
        sql1 = "SELECT person_id FROM person JOIN condition_occurrence c ON person_id = c.person_id WHERE c.condition_concept_id = 201826"

        # Turn 2: 그 중 남성만
        sql2 = add_where_condition(sql1, "gender_source_value = 'M'")
        assert "gender_source_value = 'M'" in sql2

        # Turn 3: 65세 이상만
        sql3 = add_where_condition(sql2, "year_of_birth <= 1959")
        assert "year_of_birth <= 1959" in sql3

        # 모든 조건이 포함되어야 함
        assert "condition_concept_id = 201826" in sql3
        assert "gender_source_value = 'M'" in sql3
        assert "year_of_birth <= 1959" in sql3

    def test_realistic_medical_query_modification(self):
        """실제 의료 쿼리 수정 시나리오"""
        # 기본 쿼리: 2024년 당뇨병 환자
        base_sql = """
        SELECT DISTINCT p.person_id, p.gender_source_value, p.year_of_birth
        FROM person p
        JOIN condition_occurrence c ON p.person_id = c.person_id
        WHERE c.condition_concept_id = 201826
        AND c.condition_start_date >= '2024-01-01'
        """

        # 남성만 필터
        filtered_sql = add_where_condition(base_sql, "p.gender_source_value = 'M'")

        # 검증
        assert "p.gender_source_value = 'M'" in filtered_sql
        assert "condition_concept_id = 201826" in filtered_sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
