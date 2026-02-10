"""
Migration helpers — DB config, constants, connection, and utility functions.
Extracted from migration.py to reduce file size.
"""
import os
import re
import time
import hashlib
import json
import asyncio
from datetime import datetime, date
from typing import Optional
from fastapi import HTTPException
import asyncpg

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}

# Synthea ETL 기대 행수 (대략적 기준, 76K 환자 기반)
EXPECTED_ROWS = {
    "person": 76_000,
    "observation_period": 76_000,
    "visit_occurrence": 4_500_000,
    "visit_detail": 0,
    "condition_occurrence": 2_800_000,
    "drug_exposure": 3_900_000,
    "procedure_occurrence": 12_000_000,
    "device_exposure": 0,
    "measurement": 36_000_000,
    "observation": 21_000_000,
    "death": 10_000,
    "note": 0,
    "note_nlp": 0,
    "condition_era": 1_500_000,
    "drug_era": 500_000,
    "cost": 4_400_000,
    "payer_plan_period": 2_800_000,
    "care_site": 1_000,
}

# 테이블별 필수 컬럼 (NOT NULL이어야 함)
REQUIRED_COLUMNS = {
    "person": ["person_id", "gender_concept_id", "year_of_birth"],
    "visit_occurrence": ["visit_occurrence_id", "person_id", "visit_concept_id", "visit_start_date"],
    "condition_occurrence": ["condition_occurrence_id", "person_id", "condition_concept_id", "condition_start_date"],
    "drug_exposure": ["drug_exposure_id", "person_id", "drug_concept_id", "drug_exposure_start_date"],
    "procedure_occurrence": ["procedure_occurrence_id", "person_id", "procedure_concept_id", "procedure_date"],
    "measurement": ["measurement_id", "person_id", "measurement_concept_id", "measurement_date"],
    "observation": ["observation_id", "person_id", "observation_concept_id", "observation_date"],
    "death": ["person_id"],
    "condition_era": ["condition_era_id", "person_id", "condition_concept_id"],
    "drug_era": ["drug_era_id", "person_id", "drug_concept_id"],
    "observation_period": ["observation_period_id", "person_id"],
    "cost": ["cost_id"],
    "payer_plan_period": ["payer_plan_period_id", "person_id"],
    "care_site": ["care_site_id"],
}

# 날짜 컬럼 (미래 날짜 검사용)
DATE_COLUMNS = {
    "person": ["birth_datetime"],
    "visit_occurrence": ["visit_start_date", "visit_end_date"],
    "condition_occurrence": ["condition_start_date", "condition_end_date"],
    "drug_exposure": ["drug_exposure_start_date", "drug_exposure_end_date"],
    "procedure_occurrence": ["procedure_date"],
    "measurement": ["measurement_date"],
    "observation": ["observation_date"],
    "death": ["death_date"],
    "condition_era": ["condition_era_start_date", "condition_era_end_date"],
    "drug_era": ["drug_era_start_date", "drug_era_end_date"],
    "observation_period": ["observation_period_start_date", "observation_period_end_date"],
}

# 벤치마크 쿼리 (8개)
BENCHMARK_QUERIES = [
    {
        "id": "Q1",
        "name": "단순 카운트",
        "description": "person 전체 건수 조회",
        "sql": "SELECT COUNT(*) FROM person",
        "threshold_ms": 500,
        "category": "기본",
    },
    {
        "id": "Q2",
        "name": "조건 필터",
        "description": "2000년 이후 출생 환자 수",
        "sql": "SELECT COUNT(*) FROM person WHERE year_of_birth >= 2000",
        "threshold_ms": 500,
        "category": "기본",
    },
    {
        "id": "Q3",
        "name": "2테이블 조인",
        "description": "환자별 방문 수 (TOP 10)",
        "sql": """
            SELECT p.person_id, COUNT(v.visit_occurrence_id) AS visit_cnt
            FROM person p
            JOIN visit_occurrence v ON p.person_id = v.person_id
            GROUP BY p.person_id
            ORDER BY visit_cnt DESC
            LIMIT 10
        """,
        "threshold_ms": 5000,
        "category": "조인",
    },
    {
        "id": "Q4",
        "name": "다중 집계",
        "description": "성별 × 연도별 환자 수 분포",
        "sql": """
            SELECT gender_concept_id, year_of_birth, COUNT(*) AS cnt
            FROM person
            GROUP BY gender_concept_id, year_of_birth
            ORDER BY year_of_birth
        """,
        "threshold_ms": 2000,
        "category": "집계",
    },
    {
        "id": "Q5",
        "name": "상관 서브쿼리",
        "description": "진단 3건 이상 환자 수",
        "sql": """
            SELECT COUNT(*) FROM person p
            WHERE (SELECT COUNT(*) FROM condition_occurrence c WHERE c.person_id = p.person_id) >= 3
        """,
        "threshold_ms": 30000,
        "category": "서브쿼리",
    },
    {
        "id": "Q6",
        "name": "날짜 범위 필터",
        "description": "최근 1년 방문 건수",
        "sql": """
            SELECT COUNT(*) FROM visit_occurrence
            WHERE visit_start_date >= CURRENT_DATE - INTERVAL '1 year'
        """,
        "threshold_ms": 3000,
        "category": "날짜",
    },
    {
        "id": "Q7",
        "name": "3테이블 조인 + 집계",
        "description": "환자별 진단+약물 건수 (TOP 5)",
        "sql": """
            SELECT p.person_id,
                   COUNT(DISTINCT c.condition_occurrence_id) AS dx_cnt,
                   COUNT(DISTINCT d.drug_exposure_id) AS rx_cnt
            FROM person p
            LEFT JOIN condition_occurrence c ON p.person_id = c.person_id
            LEFT JOIN drug_exposure d ON p.person_id = d.person_id
            GROUP BY p.person_id
            ORDER BY dx_cnt DESC
            LIMIT 5
        """,
        "threshold_ms": 60000,
        "category": "조인",
    },
    {
        "id": "Q8",
        "name": "ETL 시뮬레이션",
        "description": "INSERT INTO ... SELECT 시뮬레이션 (읽기 전용 EXPLAIN)",
        "sql": """
            EXPLAIN (ANALYZE false, FORMAT JSON)
            SELECT person_id, condition_concept_id, condition_start_date
            FROM condition_occurrence
            WHERE condition_concept_id = 44054006
        """,
        "threshold_ms": 1000,
        "category": "ETL",
    },
]



async def _get_conn():
    try:
        return await asyncpg.connect(**OMOP_DB_CONFIG)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"OMOP DB 연결 실패: {e}")


async def _ensure_history_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS migration_verification_history (
            id SERIAL PRIMARY KEY,
            run_type VARCHAR(30) NOT NULL,
            run_date TIMESTAMP NOT NULL DEFAULT NOW(),
            duration_ms INTEGER,
            results JSONB
        )
    """)



def _generate_recommendation(q: dict, elapsed_ms: int) -> str:
    """벤치마크 미달 시 개선 권고 생성"""
    ratio = elapsed_ms / max(q["threshold_ms"], 1)
    recs = []
    sql_lower = q["sql"].lower()

    if "join" in sql_lower:
        recs.append("JOIN 대상 컬럼에 인덱스 추가 검토 (CREATE INDEX)")
    if "group by" in sql_lower:
        recs.append("집계 대상 컬럼에 복합 인덱스 추가 또는 물화 뷰(Materialized View) 활용")
    if "count(*)" in sql_lower and "where" not in sql_lower:
        recs.append("pg_stat_user_tables.n_live_tup 근사치 활용 또는 캐시 레이어 도입")
    if "subquery" in q.get("category", "").lower() or "exists" in sql_lower or "select count" in sql_lower:
        recs.append("상관 서브쿼리를 JOIN + GROUP BY로 리팩토링 검토")
    if ratio > 3:
        recs.append("파티셔닝(Range/Hash) 도입 검토")
    if not recs:
        recs.append(f"응답시간 {elapsed_ms}ms (임계값 {q['threshold_ms']}ms) - 인덱스 추가 또는 쿼리 최적화 필요")
    return "; ".join(recs)


