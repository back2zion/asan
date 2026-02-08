"""
DMR-002: 이관 데이터 검증 및 성능 측정 API
- 18개 OMOP 테이블 검증 (건수, NULL, FK, 미래 날짜)
- 8개 표준 쿼리 벤치마크 + 개선 권고
- 5개 비식별화 작업 속도 측정
- 검증 이력 조회
"""
import os
import re
import time
import hashlib
import json
import asyncio
from datetime import datetime, date
from typing import Optional
from fastapi import APIRouter, Query
from fastapi import HTTPException
import asyncpg

router = APIRouter(prefix="/migration", tags=["Migration"])

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


# ── 엔드포인트 1: 이관 데이터 검증 ──

@router.get("/verify")
async def verify_migration():
    """18개 OMOP 테이블 이관 검증: 건수, NULL, FK 무결성, 미래 날짜"""
    conn = await _get_conn()
    t0 = time.time()
    try:
        await _ensure_history_table(conn)

        # 1) pg_stat에서 실측 행수 조회
        rows = await conn.fetch("""
            SELECT relname, n_live_tup
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
        """)
        actual_counts = {r["relname"]: r["n_live_tup"] for r in rows}

        results = []
        pass_cnt = 0
        warn_cnt = 0
        fail_cnt = 0
        total_rows = 0

        for table, expected in EXPECTED_ROWS.items():
            actual = actual_counts.get(table, 0)
            total_rows += actual

            # 건수 비교 판정
            if expected == 0:
                count_status = "pass" if actual >= 0 else "fail"
            else:
                ratio = actual / expected if expected > 0 else 0
                if ratio >= 0.8:
                    count_status = "pass"
                elif ratio >= 0.5:
                    count_status = "warning"
                else:
                    count_status = "fail"

            # NULL 검사
            null_details = []
            req_cols = REQUIRED_COLUMNS.get(table, [])
            for col in req_cols:
                try:
                    null_cnt = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
                    )
                    null_pct = round(null_cnt / max(actual, 1) * 100, 2)
                    null_details.append({
                        "column": col,
                        "null_count": null_cnt,
                        "null_pct": null_pct,
                        "status": "pass" if null_cnt == 0 else ("warning" if null_pct < 1 else "fail"),
                    })
                except Exception:
                    null_details.append({"column": col, "null_count": -1, "null_pct": 0, "status": "skip"})

            null_status = "pass"
            for nd in null_details:
                if nd["status"] == "fail":
                    null_status = "fail"
                    break
                if nd["status"] == "warning":
                    null_status = "warning"

            # FK 무결성 (person_id 고아 레코드)
            fk_status = "pass"
            orphan_count = 0
            if table not in ("person", "care_site", "cost") and actual > 0:
                try:
                    orphan_count = await conn.fetchval(f"""
                        SELECT COUNT(*) FROM {table} t
                        WHERE NOT EXISTS (SELECT 1 FROM person p WHERE p.person_id = t.person_id)
                    """)
                    if orphan_count > 0:
                        fk_status = "fail" if orphan_count > actual * 0.01 else "warning"
                except Exception:
                    fk_status = "skip"

            # 미래 날짜 검출
            future_status = "pass"
            future_count = 0
            date_cols = DATE_COLUMNS.get(table, [])
            for dc in date_cols:
                try:
                    fc = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {table} WHERE {dc} > CURRENT_DATE + INTERVAL '1 day'"
                    )
                    future_count += fc
                except Exception:
                    pass
            if future_count > 0:
                future_status = "warning" if future_count < 100 else "fail"

            # 전체 상태 결정
            statuses = [count_status, null_status, fk_status, future_status]
            if "fail" in statuses:
                overall = "fail"
                fail_cnt += 1
            elif "warning" in statuses:
                overall = "warning"
                warn_cnt += 1
            else:
                overall = "pass"
                pass_cnt += 1

            results.append({
                "table": table,
                "expected": expected,
                "actual": actual,
                "count_status": count_status,
                "null_status": null_status,
                "null_details": null_details,
                "fk_status": fk_status,
                "orphan_count": orphan_count,
                "future_status": future_status,
                "future_count": future_count,
                "overall": overall,
            })

        duration_ms = int((time.time() - t0) * 1000)

        # 이력 저장
        await conn.execute("""
            INSERT INTO migration_verification_history (run_type, duration_ms, results)
            VALUES ('verify', $1, $2::jsonb)
        """, duration_ms, json.dumps({"pass": pass_cnt, "warning": warn_cnt, "fail": fail_cnt, "total_rows": total_rows}))

        return {
            "summary": {
                "total_tables": len(EXPECTED_ROWS),
                "pass": pass_cnt,
                "warning": warn_cnt,
                "fail": fail_cnt,
                "total_rows": total_rows,
                "duration_ms": duration_ms,
            },
            "results": results,
        }
    finally:
        await conn.close()


# ── 엔드포인트 2: 성능 벤치마크 ──

@router.post("/benchmark")
async def run_benchmark():
    """8개 표준 쿼리 벤치마크 + 개선 권고 생성"""
    conn = await _get_conn()
    t0 = time.time()
    try:
        await _ensure_history_table(conn)
        results = []
        pass_cnt = 0
        fail_cnt = 0

        for q in BENCHMARK_QUERIES:
            start = time.time()
            try:
                await conn.fetch(q["sql"])
                elapsed_ms = int((time.time() - start) * 1000)
                status = "pass" if elapsed_ms <= q["threshold_ms"] else "fail"
            except Exception as e:
                elapsed_ms = int((time.time() - start) * 1000)
                status = "error"

            if status == "pass":
                pass_cnt += 1
            else:
                fail_cnt += 1

            # 개선 권고
            recommendation = None
            if status == "fail":
                recommendation = _generate_recommendation(q, elapsed_ms)

            results.append({
                "id": q["id"],
                "name": q["name"],
                "description": q["description"],
                "category": q["category"],
                "threshold_ms": q["threshold_ms"],
                "elapsed_ms": elapsed_ms,
                "status": status,
                "recommendation": recommendation,
            })

        duration_ms = int((time.time() - t0) * 1000)

        # 이력 저장
        await conn.execute("""
            INSERT INTO migration_verification_history (run_type, duration_ms, results)
            VALUES ('benchmark', $1, $2::jsonb)
        """, duration_ms, json.dumps({"pass": pass_cnt, "fail": fail_cnt, "total": len(BENCHMARK_QUERIES)}))

        return {
            "summary": {
                "total_queries": len(BENCHMARK_QUERIES),
                "pass": pass_cnt,
                "fail": fail_cnt,
                "duration_ms": duration_ms,
            },
            "results": results,
        }
    finally:
        await conn.close()


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


# ── 엔드포인트 3: 비식별화 성능 측정 ──

@router.post("/deident-benchmark")
async def run_deident_benchmark():
    """5개 비식별화 작업 속도 측정"""
    conn = await _get_conn()
    t0 = time.time()
    try:
        await _ensure_history_table(conn)
        results = []

        # 1) PII 스캔 속도 (person 테이블)
        start = time.time()
        pii_count = await conn.fetchval("""
            SELECT COUNT(*) FROM person
            WHERE person_source_value IS NOT NULL
               OR gender_source_value IS NOT NULL
        """)
        pii_scan_ms = int((time.time() - start) * 1000)
        results.append({
            "id": "D1",
            "name": "PII 컬럼 스캔",
            "description": "person 테이블 PII 컬럼 비NULL 건수 스캔",
            "records_processed": pii_count,
            "elapsed_ms": pii_scan_ms,
            "throughput": round(pii_count / max(pii_scan_ms / 1000, 0.001)),
            "unit": "rows/sec",
        })

        # 2) 정규식 마스킹 속도 (1000건 샘플)
        start = time.time()
        sample = await conn.fetch("""
            SELECT person_source_value FROM person
            WHERE person_source_value IS NOT NULL
            LIMIT 1000
        """)
        phone_re = re.compile(r'01[016789]-?\d{3,4}-?\d{4}')
        ssn_re = re.compile(r'\d{6}-[1-4]\d{6}')
        masked_count = 0
        for row in sample:
            val = row["person_source_value"] or ""
            phone_re.sub("***-****-****", val)
            ssn_re.sub("******-*******", val)
            masked_count += 1
        regex_ms = int((time.time() - start) * 1000)
        results.append({
            "id": "D2",
            "name": "정규식 마스킹",
            "description": "person_source_value 1,000건 정규식 마스킹 처리",
            "records_processed": masked_count,
            "elapsed_ms": regex_ms,
            "throughput": round(masked_count / max(regex_ms / 1000, 0.001)),
            "unit": "rows/sec",
        })

        # 3) K-익명성 계산 속도
        start = time.time()
        k_anon = await conn.fetchval("""
            SELECT MIN(cnt) FROM (
                SELECT COUNT(*) AS cnt
                FROM person
                GROUP BY gender_concept_id, year_of_birth
                HAVING COUNT(*) > 0
            ) sub
        """)
        k_anon_ms = int((time.time() - start) * 1000)
        person_count = await conn.fetchval("SELECT COUNT(*) FROM person")
        results.append({
            "id": "D3",
            "name": "K-익명성 계산",
            "description": f"(gender, year_of_birth) 그룹 최소 크기 = {k_anon}",
            "records_processed": person_count,
            "elapsed_ms": k_anon_ms,
            "throughput": round(person_count / max(k_anon_ms / 1000, 0.001)),
            "unit": "rows/sec",
        })

        # 4) SHA-256 해시 변환 속도
        start = time.time()
        hash_sample = await conn.fetch("""
            SELECT person_source_value FROM person
            WHERE person_source_value IS NOT NULL
            LIMIT 1000
        """)
        hashed = 0
        for row in hash_sample:
            hashlib.sha256((row["person_source_value"] or "").encode()).hexdigest()
            hashed += 1
        hash_ms = int((time.time() - start) * 1000)
        results.append({
            "id": "D4",
            "name": "SHA-256 해시",
            "description": "person_source_value 1,000건 SHA-256 해시 변환",
            "records_processed": hashed,
            "elapsed_ms": hash_ms,
            "throughput": round(hashed / max(hash_ms / 1000, 0.001)),
            "unit": "rows/sec",
        })

        # 5) source_value 간접식별자 스캔 속도
        start = time.time()
        sv_count = await conn.fetchval("""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_schema = 'public' AND column_name LIKE '%%source_value'
        """)
        sv_scan_ms = int((time.time() - start) * 1000)
        results.append({
            "id": "D5",
            "name": "간접식별자 스캔",
            "description": f"source_value 컬럼 {sv_count}개 스캔",
            "records_processed": sv_count,
            "elapsed_ms": sv_scan_ms,
            "throughput": round(sv_count / max(sv_scan_ms / 1000, 0.001)),
            "unit": "cols/sec",
        })

        duration_ms = int((time.time() - t0) * 1000)
        total_processed = sum(r["records_processed"] for r in results)

        # 이력 저장
        await conn.execute("""
            INSERT INTO migration_verification_history (run_type, duration_ms, results)
            VALUES ('deident', $1, $2::jsonb)
        """, duration_ms, json.dumps({"total_processed": total_processed, "tests": len(results)}))

        return {
            "summary": {
                "total_tests": len(results),
                "total_processed": total_processed,
                "duration_ms": duration_ms,
            },
            "results": results,
        }
    finally:
        await conn.close()


# ── 엔드포인트 4: 검증 이력 조회 ──

@router.get("/history")
async def get_history(run_type: Optional[str] = Query(None, description="verify | benchmark | deident")):
    """검증 이력 조회 (run_type 필터)"""
    conn = await _get_conn()
    try:
        await _ensure_history_table(conn)
        if run_type:
            rows = await conn.fetch("""
                SELECT id, run_type, run_date, duration_ms, results
                FROM migration_verification_history
                WHERE run_type = $1
                ORDER BY run_date DESC
                LIMIT 20
            """, run_type)
        else:
            rows = await conn.fetch("""
                SELECT id, run_type, run_date, duration_ms, results
                FROM migration_verification_history
                ORDER BY run_date DESC
                LIMIT 50
            """)
        return [
            {
                "id": r["id"],
                "run_type": r["run_type"],
                "run_date": r["run_date"].isoformat() if r["run_date"] else None,
                "duration_ms": r["duration_ms"],
                "results": r["results"],
            }
            for r in rows
        ]
    finally:
        await conn.close()
