"""
SQL Executor Service - SQL validation and PostgreSQL execution (read-only)
"""
import re
import time
from typing import List, Tuple, Optional
import asyncpg

from core.config import settings
from models.text2sql import ExecutionResult


# 금지된 SQL 키워드 (읽기 전용 보장)
FORBIDDEN_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
    "GRANT", "REVOKE", "EXECUTE", "EXEC", "CALL", "MERGE", "REPLACE",
    "COPY", "LOAD", "VACUUM", "ANALYZE", "CLUSTER", "COMMENT",
]

# 허용된 SQL 키워드
ALLOWED_KEYWORDS = ["SELECT", "WITH", "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "JOIN", "UNION", "INTERSECT", "EXCEPT"]


class SQLExecutor:
    """SQL 실행 엔진"""

    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None

    async def get_pool(self) -> asyncpg.Pool:
        """커넥션 풀 획득"""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                host=settings.POSTGRES_HOST,
                port=settings.POSTGRES_PORT,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                database=settings.POSTGRES_DB,
                min_size=1,
                max_size=5,
            )
        return self.pool

    def validate_sql(self, sql: str) -> Tuple[bool, str]:
        """SQL 유효성 검증

        Returns:
            Tuple[is_valid, error_message]
        """
        sql_upper = sql.upper().strip()

        # 1. 빈 쿼리 체크
        if not sql_upper:
            return False, "Empty SQL query"

        # 2. SELECT로 시작하는지 체크 (WITH 절 허용)
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            return False, "Only SELECT queries are allowed"

        # 3. 금지된 키워드 체크
        for keyword in FORBIDDEN_KEYWORDS:
            # 단어 경계 체크 (예: SELECT_COUNT는 허용하지만 DELETE는 금지)
            pattern = rf'\b{keyword}\b'
            if re.search(pattern, sql_upper):
                return False, f"Forbidden keyword detected: {keyword}"

        # 4. 세미콜론 다중 쿼리 체크
        # 문자열 리터럴 내부의 세미콜론은 무시
        sql_no_strings = re.sub(r"'[^']*'", "", sql)
        if sql_no_strings.count(";") > 1:
            return False, "Multiple queries not allowed"

        # 5. 주석을 통한 SQL 인젝션 체크
        if "--" in sql or "/*" in sql:
            return False, "SQL comments not allowed"

        return True, ""

    def sanitize_sql(self, sql: str) -> str:
        """SQL 정리"""
        # 앞뒤 공백 제거
        sql = sql.strip()

        # 마지막 세미콜론 제거
        if sql.endswith(";"):
            sql = sql[:-1]

        # LIMIT 없으면 추가 (최대 1000행)
        sql_upper = sql.upper()
        if "LIMIT" not in sql_upper:
            sql = f"{sql}\nLIMIT 1000"

        return sql

    async def execute(self, sql: str) -> ExecutionResult:
        """SQL 실행

        Args:
            sql: 실행할 SQL 쿼리

        Returns:
            ExecutionResult
        """
        # 1. 유효성 검증
        is_valid, error_msg = self.validate_sql(sql)
        if not is_valid:
            return ExecutionResult(
                results=[],
                row_count=0,
                columns=[],
                execution_time_ms=0,
                natural_language_explanation=f"SQL 검증 실패: {error_msg}"
            )

        # 2. SQL 정리
        sanitized_sql = self.sanitize_sql(sql)

        # 3. 실행
        start_time = time.time()
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                # 읽기 전용 트랜잭션으로 실행
                async with conn.transaction(readonly=True):
                    # 타임아웃 설정 (10초)
                    await conn.execute("SET statement_timeout = '10s'")

                    rows = await conn.fetch(sanitized_sql)

                    # 결과 변환
                    if rows:
                        columns = list(rows[0].keys())
                        results = [list(row.values()) for row in rows]
                    else:
                        columns = []
                        results = []

            execution_time = (time.time() - start_time) * 1000

            return ExecutionResult(
                results=results,
                row_count=len(results),
                columns=columns,
                execution_time_ms=round(execution_time, 2),
                natural_language_explanation=None
            )

        except asyncpg.PostgresError as e:
            execution_time = (time.time() - start_time) * 1000
            return ExecutionResult(
                results=[],
                row_count=0,
                columns=[],
                execution_time_ms=round(execution_time, 2),
                natural_language_explanation=f"SQL 실행 오류: {str(e)}"
            )
        except Exception as e:
            return ExecutionResult(
                results=[],
                row_count=0,
                columns=[],
                execution_time_ms=0,
                natural_language_explanation=f"시스템 오류: {str(e)}"
            )

    async def execute_with_mock(self, sql: str) -> ExecutionResult:
        """모의 실행 (DB 없이 테스트용)

        실제 DB 연결이 없을 때 사용
        """
        is_valid, error_msg = self.validate_sql(sql)
        if not is_valid:
            return ExecutionResult(
                results=[],
                row_count=0,
                columns=[],
                execution_time_ms=0,
                natural_language_explanation=f"SQL 검증 실패: {error_msg}"
            )

        sql_upper = sql.upper()

        # 모의 결과 생성
        if "COUNT" in sql_upper:
            return ExecutionResult(
                results=[[42]],
                row_count=1,
                columns=["count"],
                execution_time_ms=10.5,
                natural_language_explanation="모의 실행 결과입니다."
            )
        else:
            return ExecutionResult(
                results=[
                    ["P001", "홍길동", "1980-01-15", "M"],
                    ["P002", "김영희", "1992-05-20", "F"],
                    ["P003", "이철수", "1975-11-03", "M"],
                ],
                row_count=3,
                columns=["PT_NO", "PT_NM", "BRTH_DT", "SEX_CD"],
                execution_time_ms=15.2,
                natural_language_explanation="모의 실행 결과입니다."
            )

    async def close(self):
        """커넥션 풀 종료"""
        if self.pool:
            await self.pool.close()
            self.pool = None


# Singleton instance
sql_executor = SQLExecutor()
