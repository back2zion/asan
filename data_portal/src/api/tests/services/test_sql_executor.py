"""
Phase 3: SQL Executor 서비스 단위 테스트 (최고 우선순위 — 순수 함수)
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import asyncio


class TestValidateSQL:
    """sql_executor.validate_sql() — 순수 함수 테스트"""

    @pytest.fixture()
    def executor(self):
        from services.sql_executor import SQLExecutor
        return SQLExecutor()

    def test_select_allowed(self, executor):
        ok, msg = executor.validate_sql("SELECT * FROM person")
        assert ok
        assert msg == ""

    def test_with_cte_allowed(self, executor):
        ok, msg = executor.validate_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")
        assert ok

    def test_empty_query_rejected(self, executor):
        ok, msg = executor.validate_sql("")
        assert not ok
        assert "Empty" in msg

    def test_whitespace_only_rejected(self, executor):
        ok, msg = executor.validate_sql("   ")
        assert not ok

    def test_insert_rejected(self, executor):
        ok, msg = executor.validate_sql("INSERT INTO person VALUES (1, 'test')")
        assert not ok

    def test_delete_rejected(self, executor):
        ok, msg = executor.validate_sql("DELETE FROM person WHERE 1=1")
        assert not ok

    def test_drop_rejected(self, executor):
        ok, msg = executor.validate_sql("DROP TABLE person")
        assert not ok

    def test_update_rejected(self, executor):
        ok, msg = executor.validate_sql("UPDATE person SET name='hack'")
        assert not ok

    def test_insert_in_select_context_rejected(self, executor):
        """SELECT 시작이지만 INSERT 포함 → 금지"""
        ok, msg = executor.validate_sql("SELECT 1; INSERT INTO person VALUES (1, 'test')")
        assert not ok

    def test_delete_in_select_context_rejected(self, executor):
        """SELECT 시작이지만 DELETE 포함"""
        ok, msg = executor.validate_sql("SELECT 1; DELETE FROM person")
        assert not ok

    def test_truncate_rejected(self, executor):
        ok, msg = executor.validate_sql("TRUNCATE TABLE person")
        assert not ok

    def test_create_rejected(self, executor):
        ok, msg = executor.validate_sql("CREATE TABLE evil (id int)")
        assert not ok

    def test_alter_rejected(self, executor):
        ok, msg = executor.validate_sql("ALTER TABLE person ADD COLUMN hack TEXT")
        assert not ok

    def test_grant_rejected(self, executor):
        ok, msg = executor.validate_sql("GRANT ALL ON person TO public")
        assert not ok

    def test_multi_statement_rejected(self, executor):
        ok, msg = executor.validate_sql("SELECT 1; SELECT 2;")
        assert not ok
        assert "Multiple" in msg

    def test_comment_dash_rejected(self, executor):
        ok, msg = executor.validate_sql("SELECT 1 -- injection")
        assert not ok
        assert "comment" in msg.lower()

    def test_comment_block_rejected(self, executor):
        ok, msg = executor.validate_sql("SELECT 1 /* injection */")
        assert not ok

    def test_select_with_joins_allowed(self, executor):
        sql = """
        SELECT p.person_id, co.condition_concept_id
        FROM person p
        JOIN condition_occurrence co ON p.person_id = co.person_id
        WHERE co.condition_concept_id = 44054006
        """
        ok, msg = executor.validate_sql(sql)
        assert ok

    def test_select_with_aggregation_allowed(self, executor):
        sql = "SELECT gender_source_value, COUNT(*) FROM person GROUP BY gender_source_value"
        ok, msg = executor.validate_sql(sql)
        assert ok

    def test_semicolon_in_string_literal_allowed(self, executor):
        sql = "SELECT * FROM person WHERE name = 'test;value'"
        ok, msg = executor.validate_sql(sql)
        assert ok

    def test_begin_transaction_rejected(self, executor):
        ok, msg = executor.validate_sql("BEGIN; SELECT 1; COMMIT;")
        assert not ok

    def test_copy_rejected(self, executor):
        ok, msg = executor.validate_sql("COPY person TO '/tmp/data.csv'")
        assert not ok

    def test_set_rejected(self, executor):
        ok, msg = executor.validate_sql("SET statement_timeout = 0")
        assert not ok


class TestSanitizeSQL:
    """sql_executor.sanitize_sql() — 순수 함수 테스트"""

    @pytest.fixture()
    def executor(self):
        from services.sql_executor import SQLExecutor
        return SQLExecutor()

    def test_adds_limit_when_missing(self, executor):
        result = executor.sanitize_sql("SELECT * FROM person")
        assert "LIMIT 1000" in result

    def test_preserves_existing_limit(self, executor):
        result = executor.sanitize_sql("SELECT * FROM person LIMIT 50")
        assert "LIMIT 1000" not in result
        assert "LIMIT 50" in result

    def test_removes_trailing_semicolon(self, executor):
        result = executor.sanitize_sql("SELECT * FROM person;")
        assert not result.endswith(";")

    def test_strips_whitespace(self, executor):
        result = executor.sanitize_sql("  SELECT * FROM person  ")
        assert result.startswith("SELECT")

    def test_preserves_existing_limit_case_insensitive(self, executor):
        result = executor.sanitize_sql("SELECT * FROM person limit 100")
        assert "LIMIT 1000" not in result


class TestExecuteSQL:
    """sql_executor.execute() — subprocess mocked"""

    @pytest.fixture()
    def executor(self):
        from services.sql_executor import SQLExecutor
        return SQLExecutor()

    async def test_execute_success(self, executor):
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(
            return_value=(b"1\tJohn\n2\tJane\n", b"")
        )
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
             patch.object(executor, "_get_columns", return_value=["id", "name"]):
            result = await executor.execute("SELECT id, name FROM person")
        assert result.row_count == 2
        assert result.results[0] == [1, "John"]

    async def test_execute_invalid_sql(self, executor):
        result = await executor.execute("DROP TABLE person")
        assert result.row_count == 0
        assert "검증 실패" in result.natural_language_explanation

    async def test_execute_timeout(self, executor):
        async def slow_communicate():
            await asyncio.sleep(100)
            return (b"", b"")

        mock_proc = AsyncMock()
        mock_proc.communicate = slow_communicate
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc), \
             patch("asyncio.wait_for", side_effect=asyncio.TimeoutError()):
            result = await executor.execute("SELECT * FROM measurement")
        assert "시간 초과" in result.natural_language_explanation

    async def test_execute_subprocess_error(self, executor):
        mock_proc = AsyncMock()
        mock_proc.returncode = 1
        mock_proc.communicate = AsyncMock(
            return_value=(b"", b"ERROR: relation does not exist")
        )
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            result = await executor.execute("SELECT * FROM nonexistent")
        assert result.row_count == 0
        assert "오류" in result.natural_language_explanation

    async def test_execute_empty_result(self, executor):
        mock_proc = AsyncMock()
        mock_proc.returncode = 0
        mock_proc.communicate = AsyncMock(return_value=(b"", b""))
        with patch("asyncio.create_subprocess_exec", return_value=mock_proc):
            result = await executor.execute("SELECT * FROM person WHERE 1=0")
        assert result.row_count == 0
        assert result.results == []
