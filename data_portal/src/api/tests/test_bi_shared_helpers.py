"""
BI 공유 모듈 순수 함수 테스트 — validate_sql(), ensure_limit()
외부 의존성 없음 (DB/네트워크 불필요)
"""
import pytest

from routers._bi_shared import validate_sql, ensure_limit


# ══════════════════════════════════════════════════════════
#  validate_sql()
# ══════════════════════════════════════════════════════════

class TestValidateSql:
    """SQL 검증 함수 테스트"""

    def test_select_allowed(self):
        ok, msg = validate_sql("SELECT * FROM person")
        assert ok is True
        assert msg == "OK"

    def test_select_with_where(self):
        ok, _ = validate_sql("SELECT person_id FROM person WHERE year_of_birth > 1990")
        assert ok is True

    def test_with_cte_allowed(self):
        ok, _ = validate_sql(
            "WITH cte AS (SELECT person_id FROM person) SELECT * FROM cte"
        )
        assert ok is True

    def test_empty_sql_rejected(self):
        ok, msg = validate_sql("")
        assert ok is False
        assert "빈 SQL" in msg

    def test_whitespace_only_rejected(self):
        ok, msg = validate_sql("   ")
        assert ok is False

    def test_drop_blocked(self):
        ok, msg = validate_sql("DROP TABLE person")
        assert ok is False
        assert "SELECT/WITH" in msg or "금지" in msg

    def test_delete_blocked(self):
        ok, msg = validate_sql("DELETE FROM person")
        assert ok is False

    def test_insert_blocked(self):
        ok, msg = validate_sql("INSERT INTO person VALUES (1)")
        assert ok is False

    def test_update_blocked(self):
        ok, msg = validate_sql("UPDATE person SET year_of_birth = 2000")
        assert ok is False

    def test_truncate_blocked(self):
        ok, msg = validate_sql("TRUNCATE TABLE person")
        assert ok is False

    def test_select_with_drop_in_body(self):
        ok, msg = validate_sql("SELECT * FROM person; DROP TABLE person")
        assert ok is False
        assert "다중 문" in msg or "금지" in msg

    def test_multi_statement_semicolon_blocked(self):
        ok, msg = validate_sql("SELECT 1; SELECT 2")
        assert ok is False
        assert "다중 문" in msg

    def test_comment_removal_single_line(self):
        ok, _ = validate_sql("SELECT * FROM person -- comment")
        assert ok is True

    def test_comment_removal_block(self):
        ok, _ = validate_sql("SELECT /* block comment */ * FROM person")
        assert ok is True

    def test_trailing_semicolon_stripped(self):
        ok, _ = validate_sql("SELECT * FROM person;")
        assert ok is True

    def test_forbidden_grant(self):
        ok, _ = validate_sql("GRANT ALL ON person TO public")
        assert ok is False

    def test_forbidden_revoke(self):
        ok, _ = validate_sql("REVOKE ALL ON person FROM public")
        assert ok is False

    def test_forbidden_exec(self):
        ok, _ = validate_sql("EXEC sp_something")
        assert ok is False

    def test_alter_blocked(self):
        ok, _ = validate_sql("ALTER TABLE person ADD COLUMN foo INT")
        assert ok is False

    def test_create_blocked(self):
        ok, _ = validate_sql("CREATE TABLE evil (id INT)")
        assert ok is False


# ══════════════════════════════════════════════════════════
#  ensure_limit()
# ══════════════════════════════════════════════════════════

class TestEnsureLimit:
    """LIMIT 자동 추가 함수 테스트"""

    def test_adds_limit_when_missing(self):
        result = ensure_limit("SELECT * FROM person")
        assert "LIMIT 1000" in result

    def test_custom_limit_value(self):
        result = ensure_limit("SELECT * FROM person", limit=500)
        assert "LIMIT 500" in result

    def test_keeps_existing_limit(self):
        sql = "SELECT * FROM person LIMIT 10"
        result = ensure_limit(sql)
        assert result.count("LIMIT") == 1
        assert "LIMIT 10" in result

    def test_keeps_existing_limit_lowercase(self):
        sql = "SELECT * FROM person limit 5"
        result = ensure_limit(sql)
        assert "LIMIT 1000" not in result

    def test_strips_trailing_semicolon(self):
        result = ensure_limit("SELECT * FROM person;")
        assert not result.endswith(";")

    def test_strips_whitespace(self):
        result = ensure_limit("  SELECT * FROM person  ")
        assert result.startswith("SELECT")
