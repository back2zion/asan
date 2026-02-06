"""
SQL Executor Service - SQL validation and PostgreSQL execution (read-only)
Uses docker exec for OMOP DB connection to bypass network authentication issues.
"""
import re
import time
import json
import asyncio
import subprocess
from typing import List, Tuple, Optional

import os
from core.config import settings
from models.text2sql import ExecutionResult

# OMOP DB 설정
OMOP_CONTAINER = os.getenv("OMOP_CONTAINER", "infra-omop-db-1")
OMOP_USER = os.getenv("OMOP_USER", "omopuser")
OMOP_DB = os.getenv("OMOP_DB", "omop_cdm")


# 금지된 SQL 키워드 (읽기 전용 보장)
FORBIDDEN_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
    "GRANT", "REVOKE", "EXECUTE", "EXEC", "CALL", "MERGE", "REPLACE",
    "COPY", "LOAD", "VACUUM", "ANALYZE", "CLUSTER", "COMMENT",
]

# 허용된 SQL 키워드
ALLOWED_KEYWORDS = ["SELECT", "WITH", "FROM", "WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "JOIN", "UNION", "INTERSECT", "EXCEPT"]


class SQLExecutor:
    """SQL 실행 엔진 - docker exec 사용"""

    def __init__(self):
        self.container = OMOP_CONTAINER
        self.user = OMOP_USER
        self.db = OMOP_DB

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
        """SQL 실행 (docker exec 사용)

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

        # 3. docker exec로 실행
        start_time = time.time()
        try:
            # psql로 JSON 형식 출력 (-t: tuples only, -A: unaligned)
            cmd = [
                "docker", "exec", self.container,
                "psql", "-U", self.user, "-d", self.db,
                "-t", "-A", "-F", "\t",
                "-c", sanitized_sql
            ]

            # 비동기로 subprocess 실행
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=10.0
            )

            execution_time = (time.time() - start_time) * 1000

            if process.returncode != 0:
                error = stderr.decode('utf-8').strip()
                return ExecutionResult(
                    results=[],
                    row_count=0,
                    columns=[],
                    execution_time_ms=round(execution_time, 2),
                    natural_language_explanation=f"SQL 실행 오류: {error}"
                )

            output = stdout.decode('utf-8').strip()

            if not output:
                return ExecutionResult(
                    results=[],
                    row_count=0,
                    columns=[],
                    execution_time_ms=round(execution_time, 2),
                    natural_language_explanation=None
                )

            # 결과 파싱
            lines = output.split('\n')
            results = []
            for line in lines:
                if line.strip():
                    row = line.split('\t')
                    # 숫자 변환 시도
                    parsed_row = []
                    for val in row:
                        try:
                            if '.' in val:
                                parsed_row.append(float(val))
                            else:
                                parsed_row.append(int(val))
                        except ValueError:
                            parsed_row.append(val if val else None)
                    results.append(parsed_row)

            # 컬럼명 추출 (별도 쿼리)
            columns = await self._get_columns(sanitized_sql)

            return ExecutionResult(
                results=results,
                row_count=len(results),
                columns=columns,
                execution_time_ms=round(execution_time, 2),
                natural_language_explanation=None
            )

        except asyncio.TimeoutError:
            execution_time = (time.time() - start_time) * 1000
            return ExecutionResult(
                results=[],
                row_count=0,
                columns=[],
                execution_time_ms=round(execution_time, 2),
                natural_language_explanation="SQL 실행 시간 초과 (10초)"
            )
        except Exception as e:
            return ExecutionResult(
                results=[],
                row_count=0,
                columns=[],
                execution_time_ms=0,
                natural_language_explanation=f"시스템 오류: {str(e)}"
            )

    async def _get_columns(self, sql: str) -> List[str]:
        """SQL에서 컬럼명 추출"""
        try:
            # LIMIT 0으로 컬럼명만 가져오기
            col_sql = re.sub(r'LIMIT\s+\d+', 'LIMIT 0', sql, flags=re.IGNORECASE)
            if 'LIMIT' not in col_sql.upper():
                col_sql = f"{col_sql} LIMIT 0"

            cmd = [
                "docker", "exec", self.container,
                "psql", "-U", self.user, "-d", self.db,
                "-c", col_sql
            ]

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await asyncio.wait_for(process.communicate(), timeout=5.0)
            output = stdout.decode('utf-8').strip()

            # 첫 번째 줄이 컬럼 헤더
            if output:
                first_line = output.split('\n')[0]
                # |로 구분된 컬럼명 추출
                columns = [col.strip() for col in first_line.split('|')]
                return columns
        except Exception:
            pass
        return []

    async def execute_with_mock(self, sql: str) -> ExecutionResult:
        """실제 DB 실행으로 리다이렉트 (mock 모드 제거됨)"""
        return await self.execute(sql)

    async def close(self):
        """리소스 정리 (현재 필요없음)"""
        pass


# Singleton instance
sql_executor = SQLExecutor()
