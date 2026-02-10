"""
Phase 5: 통합 테스트 — 실제 OMOP CDM DB 연동
@pytest.mark.integration (DB 없으면 skip)
"""
import os
import pytest
import asyncpg

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}


@pytest.fixture(scope="module")
async def db_conn():
    """실제 DB 연결 (없으면 skip)"""
    try:
        conn = await asyncpg.connect(**OMOP_DB_CONFIG)
        yield conn
        await conn.close()
    except Exception as e:
        pytest.skip(f"OMOP DB not available: {e}")


@pytest.mark.integration
class TestPersonTable:
    """person 테이블 데이터 검증"""

    async def test_person_table_has_data(self, db_conn):
        count = await db_conn.fetchval("SELECT COUNT(*) FROM person")
        assert count > 70000, f"Expected ~76K patients, got {count}"

    async def test_gender_distribution(self, db_conn):
        rows = await db_conn.fetch(
            "SELECT gender_source_value, COUNT(*) as cnt FROM person GROUP BY gender_source_value"
        )
        gender_map = {r["gender_source_value"]: r["cnt"] for r in rows}
        assert "M" in gender_map
        assert "F" in gender_map
        assert gender_map["M"] > 30000
        assert gender_map["F"] > 30000


@pytest.mark.integration
class TestVisitOccurrence:
    """visit_occurrence 테이블 검증"""

    async def test_visit_types_exist(self, db_conn):
        rows = await db_conn.fetch(
            "SELECT DISTINCT visit_concept_id FROM visit_occurrence"
        )
        concept_ids = {r["visit_concept_id"] for r in rows}
        # 입원(9201), 외래(9202), 응급(9203)
        assert 9201 in concept_ids, "입원 visit type not found"
        assert 9202 in concept_ids, "외래 visit type not found"
        assert 9203 in concept_ids, "응급 visit type not found"

    async def test_visit_occurrence_has_data(self, db_conn):
        count = await db_conn.fetchval("SELECT COUNT(*) FROM visit_occurrence")
        assert count > 4000000, f"Expected ~4.5M visits, got {count}"


@pytest.mark.integration
class TestTableRowCounts:
    """주요 테이블 row count 합리성 검증"""

    @pytest.mark.parametrize("table,min_rows", [
        ("person", 70000),
        ("condition_occurrence", 2000000),
        ("drug_exposure", 3000000),
        ("measurement", 30000000),
        ("observation", 20000000),
        ("procedure_occurrence", 10000000),
    ])
    async def test_table_has_minimum_rows(self, db_conn, table, min_rows):
        count = await db_conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        assert count >= min_rows, f"{table}: expected >= {min_rows}, got {count}"


@pytest.mark.integration
class TestConditionConcepts:
    """SNOMED CT 코드 검증"""

    async def test_diabetes_snomed_exists(self, db_conn):
        """당뇨 (SNOMED 44054006)"""
        count = await db_conn.fetchval(
            "SELECT COUNT(*) FROM condition_occurrence WHERE condition_concept_id = 44054006"
        )
        assert count > 0, "Diabetes (44054006) not found"

    async def test_hypertension_snomed_exists(self, db_conn):
        """고혈압 (SNOMED 38341003)"""
        count = await db_conn.fetchval(
            "SELECT COUNT(*) FROM condition_occurrence WHERE condition_concept_id = 38341003"
        )
        assert count > 0, "Hypertension (38341003) not found"
