"""
SQL 생성 노드

사용자 질의와 의도를 기반으로 SQL을 생성합니다.
기존 NL2SQL 파이프라인의 SQL Generation 단계를 래핑합니다.
"""

from datetime import datetime
from typing import Any, Optional

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
    QueryContext,
)
from ai_services.conversation_memory.utils.sql_utils import extract_table_names


def sql_generator_node(state: ConversationState) -> dict:
    """SQL을 생성하는 노드.

    사용자 질의와 추출된 의도를 기반으로 SQL을 생성합니다.
    query_modifier가 이후에 참조 기반 수정을 적용하며,
    state_updater가 최종 SQL로 query_history를 업데이트합니다.

    Args:
        state: 현재 대화 상태

    Returns:
        업데이트할 상태 필드 딕셔너리:
        - base_sql: 생성된 기본 SQL
        - turn_count: 증가된 턴 수
    """
    current_query = state.get("current_query", "")
    turn_count = state.get("turn_count", 0) + 1

    # 항상 의도 기반으로 새 SQL 생성
    intent = state.get("session_metadata", {}).get("last_intent", {})
    base_sql = generate_sql_from_intent(current_query, intent)

    return {
        "base_sql": base_sql,
        "turn_count": turn_count,
    }


def generate_sql_from_intent(query: str, intent: dict[str, Any]) -> str:
    """의도 정보를 기반으로 SQL을 생성합니다.

    실제 구현에서는 LLM (Qwen3-235B) 또는 기존 NL2SQL 서비스를 호출합니다.
    여기서는 규칙 기반 기본 구현을 제공합니다.

    Args:
        query: 사용자 질의
        intent: 추출된 의도 정보

    Returns:
        생성된 SQL
    """
    query_type = intent.get("query_type", "list")
    entities = intent.get("entities", [])
    conditions = intent.get("conditions", [])
    time_range = intent.get("time_range")

    # 기본 테이블 결정
    main_table = _determine_main_table(entities)

    # SELECT 절 구성
    if query_type == "count":
        select_clause = "SELECT COUNT(DISTINCT p.person_id) as patient_count"
    elif query_type == "aggregate":
        agg = intent.get("aggregation", "AVG")
        select_clause = f"SELECT {agg}(m.value_as_number) as result"
    else:
        select_clause = "SELECT DISTINCT p.person_id, p.gender_source_value as sex, p.year_of_birth"

    # FROM 절 구성
    from_clause = f"FROM person p"

    # JOIN 절 구성
    join_clauses = []
    if any(e.get("type") == "disease" for e in entities):
        join_clauses.append(
            "JOIN condition_occurrence c ON p.person_id = c.person_id"
        )

    if any(e.get("type") == "drug" for e in entities):
        join_clauses.append(
            "JOIN drug_exposure d ON p.person_id = d.person_id"
        )

    # WHERE 절 구성
    where_conditions = []

    # 엔티티 조건 추가
    for entity in entities:
        if entity.get("type") == "disease":
            concept_id = entity.get("concept_id")
            if concept_id:
                where_conditions.append(f"c.condition_concept_id = {concept_id}")
            else:
                icd10 = entity.get("icd10", "")
                if icd10:
                    where_conditions.append(f"c.condition_source_value LIKE '{icd10}%'")

        elif entity.get("type") == "drug":
            rxnorm = entity.get("rxnorm")
            if rxnorm:
                where_conditions.append(f"d.drug_concept_id = {rxnorm}")

    # 일반 조건 추가
    for cond in conditions:
        field = cond["field"]
        operator = cond["operator"]
        value = cond["value"]

        if field == "sex":
            where_conditions.append(f"p.gender_source_value = '{value}'")
        elif field == "age":
            # 나이 계산: 현재년도 - 출생년도
            current_year = datetime.now().year
            if operator == ">=":
                birth_year = current_year - value
                where_conditions.append(f"p.year_of_birth <= {birth_year}")
            elif operator == "<=":
                birth_year = current_year - value
                where_conditions.append(f"p.year_of_birth >= {birth_year}")
            elif operator == "<":
                birth_year = current_year - value
                where_conditions.append(f"p.year_of_birth > {birth_year}")
            elif operator == ">":
                birth_year = current_year - value
                where_conditions.append(f"p.year_of_birth < {birth_year}")
        elif field == "department":
            where_conditions.append(f"c.visit_detail_id IN (SELECT visit_detail_id FROM visit_detail WHERE care_site_id IN (SELECT care_site_id FROM care_site WHERE care_site_name = '{value}'))")

    # 시간 범위 조건
    if time_range:
        start_date = time_range["start"]
        end_date = time_range["end"]
        if "condition_occurrence" in " ".join(join_clauses):
            where_conditions.append(f"c.condition_start_date >= '{start_date}'")
            where_conditions.append(f"c.condition_start_date <= '{end_date}'")

    # SQL 조합
    sql_parts = [select_clause, from_clause]
    sql_parts.extend(join_clauses)

    if where_conditions:
        sql_parts.append("WHERE " + " AND ".join(where_conditions))

    return "\n".join(sql_parts)


def _determine_main_table(entities: list[dict]) -> str:
    """엔티티를 기반으로 메인 테이블을 결정합니다."""
    entity_types = {e.get("type") for e in entities}

    if "disease" in entity_types:
        return "condition_occurrence"
    elif "drug" in entity_types:
        return "drug_exposure"
    elif "measurement" in entity_types:
        return "measurement"
    else:
        return "person"


def enhance_sql_with_context(
    base_sql: str,
    context: QueryContext,
    new_condition: Optional[str] = None,
) -> str:
    """컨텍스트를 반영하여 SQL을 개선합니다.

    Args:
        base_sql: 기본 SQL
        context: 이전 쿼리 컨텍스트
        new_condition: 추가할 새 조건

    Returns:
        개선된 SQL
    """
    from ai_services.conversation_memory.utils.sql_utils import add_where_condition

    enhanced_sql = base_sql

    # 이전 조건 적용
    for condition in context.conditions:
        enhanced_sql = add_where_condition(enhanced_sql, condition)

    # 새 조건 적용
    if new_condition:
        enhanced_sql = add_where_condition(enhanced_sql, new_condition)

    return enhanced_sql


def validate_generated_sql(sql: str) -> dict[str, Any]:
    """생성된 SQL의 유효성을 검사합니다.

    Args:
        sql: 검사할 SQL

    Returns:
        검증 결과
    """
    result = {
        "is_valid": True,
        "errors": [],
        "warnings": [],
    }

    sql_upper = sql.upper()

    # 기본 구조 검사
    if "SELECT" not in sql_upper:
        result["is_valid"] = False
        result["errors"].append("SELECT 절이 없습니다.")

    if "FROM" not in sql_upper:
        result["is_valid"] = False
        result["errors"].append("FROM 절이 없습니다.")

    # 위험한 패턴 검사
    dangerous_patterns = ["DROP", "DELETE", "TRUNCATE", "UPDATE", "INSERT", "ALTER"]
    for pattern in dangerous_patterns:
        if pattern in sql_upper:
            result["is_valid"] = False
            result["errors"].append(f"위험한 키워드가 포함되어 있습니다: {pattern}")

    # 경고 패턴 검사
    if "SELECT *" in sql_upper:
        result["warnings"].append("SELECT *는 필요한 컬럼만 명시하는 것이 좋습니다.")

    if "LIMIT" not in sql_upper:
        result["warnings"].append("LIMIT 절이 없습니다. 대량 데이터 조회에 주의하세요.")

    return result
