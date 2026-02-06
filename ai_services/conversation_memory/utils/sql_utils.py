"""
SQL 조작 유틸리티

이전 쿼리 결과를 기반으로 SQL을 수정하는 유틸리티 함수들.
"""

import re
from typing import Optional


def add_where_condition(
    sql: str,
    condition: str,
    operator: str = "AND",
) -> str:
    """SQL에 WHERE 조건을 추가합니다.

    기존 WHERE 절이 있으면 조건을 AND/OR로 연결하고,
    없으면 새로운 WHERE 절을 추가합니다.

    Args:
        sql: 원본 SQL
        condition: 추가할 조건 (예: "SEX_CD = 'M'")
        operator: 연결 연산자 ("AND" 또는 "OR")

    Returns:
        수정된 SQL

    Example:
        >>> sql = "SELECT * FROM person WHERE age > 20"
        >>> add_where_condition(sql, "SEX_CD = 'M'")
        "SELECT * FROM person WHERE age > 20 AND SEX_CD = 'M'"
    """
    sql = sql.strip()
    if sql.endswith(';'):
        sql = sql[:-1]

    # WHERE 절 위치 찾기 (대소문자 무시)
    where_match = re.search(r'\bWHERE\b', sql, re.IGNORECASE)

    if where_match:
        # GROUP BY, ORDER BY, LIMIT 등의 위치 찾기
        end_keywords = re.search(
            r'\b(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|UNION|INTERSECT|EXCEPT)\b',
            sql[where_match.end():],
            re.IGNORECASE,
        )

        if end_keywords:
            insert_pos = where_match.end() + end_keywords.start()
            modified = (
                sql[:insert_pos].rstrip() +
                f" {operator} {condition} " +
                sql[insert_pos:]
            )
        else:
            modified = f"{sql} {operator} {condition}"
    else:
        # FROM 절 이후에 WHERE 추가
        from_match = re.search(
            r'\b(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING)\b',
            sql,
            re.IGNORECASE,
        )

        if from_match:
            modified = (
                sql[:from_match.start()].rstrip() +
                f" WHERE {condition} " +
                sql[from_match.start():]
            )
        else:
            modified = f"{sql} WHERE {condition}"

    return modified


def create_subquery_filter(
    base_sql: str,
    filter_column: str,
    subquery_sql: str,
    subquery_column: str,
) -> str:
    """서브쿼리를 이용한 필터 조건을 생성합니다.

    이전 쿼리 결과의 특정 컬럼 값들로 현재 쿼리를 필터링합니다.

    Args:
        base_sql: 기본 SQL
        filter_column: 필터링할 컬럼 (현재 쿼리)
        subquery_sql: 서브쿼리 SQL (이전 쿼리)
        subquery_column: 서브쿼리에서 가져올 컬럼

    Returns:
        서브쿼리 필터가 적용된 SQL

    Example:
        >>> base_sql = "SELECT * FROM condition_occurrence"
        >>> subquery = "SELECT person_id FROM person WHERE age > 65"
        >>> create_subquery_filter(base_sql, "person_id", subquery, "person_id")
        "SELECT * FROM condition_occurrence WHERE person_id IN (SELECT person_id FROM person WHERE age > 65)"
    """
    # 서브쿼리에서 필요한 컬럼만 선택하도록 수정
    subquery_sql = subquery_sql.strip()
    if subquery_sql.endswith(';'):
        subquery_sql = subquery_sql[:-1]

    # SELECT 절 수정하여 필요한 컬럼만 선택
    modified_subquery = re.sub(
        r'SELECT\s+.+?\s+FROM',
        f'SELECT {subquery_column} FROM',
        subquery_sql,
        count=1,
        flags=re.IGNORECASE | re.DOTALL,
    )

    condition = f"{filter_column} IN ({modified_subquery})"
    return add_where_condition(base_sql, condition)


def extract_table_names(sql: str) -> list[str]:
    """SQL에서 테이블명을 추출합니다.

    Args:
        sql: SQL 문

    Returns:
        테이블명 목록
    """
    # FROM 절과 JOIN 절에서 테이블명 추출
    tables = []

    # FROM 절
    from_matches = re.findall(
        r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        sql,
        re.IGNORECASE,
    )
    tables.extend(from_matches)

    # JOIN 절
    join_matches = re.findall(
        r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        sql,
        re.IGNORECASE,
    )
    tables.extend(join_matches)

    return list(set(tables))


def extract_where_conditions(sql: str) -> list[str]:
    """SQL에서 WHERE 조건들을 추출합니다.

    Args:
        sql: SQL 문

    Returns:
        WHERE 조건 목록
    """
    where_match = re.search(
        r'\bWHERE\s+(.+?)(?:\bGROUP\s+BY|\bORDER\s+BY|\bLIMIT|\bHAVING|$)',
        sql,
        re.IGNORECASE | re.DOTALL,
    )

    if not where_match:
        return []

    where_clause = where_match.group(1).strip()

    # AND/OR로 분리
    conditions = re.split(r'\s+AND\s+|\s+OR\s+', where_clause, flags=re.IGNORECASE)
    return [c.strip() for c in conditions if c.strip()]


def merge_sql_conditions(
    sql: str,
    previous_conditions: list[str],
    operator: str = "AND",
) -> str:
    """이전 쿼리의 조건들을 현재 SQL에 병합합니다.

    Args:
        sql: 현재 SQL
        previous_conditions: 이전 쿼리의 조건 목록
        operator: 연결 연산자

    Returns:
        조건이 병합된 SQL
    """
    result = sql
    for condition in previous_conditions:
        result = add_where_condition(result, condition, operator)
    return result


def create_cte_with_previous_result(
    current_sql: str,
    previous_sql: str,
    cte_name: str = "previous_result",
    join_column: str = "person_id",
) -> str:
    """이전 쿼리 결과를 CTE로 사용하여 현재 쿼리와 조인합니다.

    Args:
        current_sql: 현재 SQL
        previous_sql: 이전 쿼리 SQL
        cte_name: CTE 이름
        join_column: 조인 컬럼

    Returns:
        CTE가 적용된 SQL

    Example:
        >>> prev = "SELECT person_id FROM person WHERE age > 65"
        >>> curr = "SELECT * FROM condition_occurrence"
        >>> create_cte_with_previous_result(curr, prev)
        '''WITH previous_result AS (
            SELECT person_id FROM person WHERE age > 65
        )
        SELECT * FROM condition_occurrence
        WHERE person_id IN (SELECT person_id FROM previous_result)'''
    """
    previous_sql = previous_sql.strip()
    if previous_sql.endswith(';'):
        previous_sql = previous_sql[:-1]

    cte = f"WITH {cte_name} AS (\n    {previous_sql}\n)\n"
    condition = f"{join_column} IN (SELECT {join_column} FROM {cte_name})"

    return cte + add_where_condition(current_sql, condition)
