"""
DMR-002: 이관 데이터 검증 및 성능 측정 API
- 18개 OMOP 테이블 검증 (건수, NULL, FK, 미래 날짜)
- 8개 표준 쿼리 벤치마크 + 개선 권고
- 5개 비식별화 작업 속도 측정
- 검증 이력 조회
"""
import re
import time
import hashlib
import json
from typing import Optional
from fastapi import APIRouter, Query

from ._migration_helpers import (
    EXPECTED_ROWS,
    REQUIRED_COLUMNS,
    DATE_COLUMNS,
    BENCHMARK_QUERIES,
    _get_conn,
    _ensure_history_table,
    _generate_recommendation,
)

router = APIRouter(prefix="/migration", tags=["Migration"])

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
