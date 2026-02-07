"""
OMOP CDM 통계 요약 파이프라인
CDM 전체 통계 요약을 cdm_summary 테이블에 저장.
테이블별 row count, 환자 demographics, condition/drug 상위 빈도, 최근 적재 시점.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import psycopg2
import json

OMOP_DB_CONN = {
    "host": "omop-db",
    "port": 5432,
    "dbname": "omop_cdm",
    "user": "omopuser",
    "password": "omop",
}

# 통계 수집 대상 테이블
STATS_TABLES = [
    "person",
    "condition_occurrence",
    "drug_exposure",
    "visit_occurrence",
    "measurement",
    "imaging_study",
    "condition_era",
    "drug_era",
    "observation",
    "procedure_occurrence",
]

default_args = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def ensure_summary_table(**context):
    """cdm_summary 테이블이 없으면 생성"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cdm_summary (
                summary_id SERIAL PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                key VARCHAR(100) NOT NULL,
                value TEXT,
                numeric_value NUMERIC,
                updated_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(category, key)
            );
        """)
        conn.commit()
        print("cdm_summary table ready")
    finally:
        conn.close()


def _upsert(cur, category, key, value, numeric_value=None):
    """cdm_summary에 upsert"""
    cur.execute(
        """
        INSERT INTO cdm_summary (category, key, value, numeric_value, updated_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (category, key)
        DO UPDATE SET value = EXCLUDED.value,
                      numeric_value = EXCLUDED.numeric_value,
                      updated_at = NOW();
        """,
        (category, key, str(value), numeric_value),
    )


def collect_table_counts(**context):
    """테이블별 row count 수집"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()
        total = 0

        for table in STATS_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {table};")  # noqa: S608
            count = cur.fetchone()[0]
            _upsert(cur, "table_count", table, f"{count:,}", count)
            print(f"  {table}: {count:,}")
            total += count

        _upsert(cur, "table_count", "_total", f"{total:,}", total)
        print(f"  Total: {total:,}")

        conn.commit()
        return {"total": total}
    finally:
        conn.close()


def collect_demographics(**context):
    """환자 demographics 수집 (성별, 연령대 분포)"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()

        # 성별 분포
        cur.execute("""
            SELECT gender_source_value, COUNT(*) as cnt
            FROM person
            GROUP BY gender_source_value
            ORDER BY cnt DESC;
        """)
        gender_dist = {}
        for row in cur.fetchall():
            gender = row[0] or "Unknown"
            count = row[1]
            gender_dist[gender] = count
            _upsert(cur, "demographics_gender", gender, count, count)

        print(f"  Gender distribution: {json.dumps(gender_dist)}")

        # 연령대 분포 (10세 단위)
        cur.execute("""
            SELECT
                CASE
                    WHEN year_of_birth IS NULL THEN 'Unknown'
                    ELSE (((EXTRACT(YEAR FROM CURRENT_DATE) - year_of_birth) / 10)::int * 10)::text
                         || 's'
                END AS age_group,
                COUNT(*) as cnt
            FROM person
            GROUP BY age_group
            ORDER BY age_group;
        """)
        age_dist = {}
        for row in cur.fetchall():
            age_group = row[0]
            count = row[1]
            age_dist[age_group] = count
            _upsert(cur, "demographics_age", age_group, count, count)

        print(f"  Age distribution: {json.dumps(age_dist)}")

        # 총 환자 수
        cur.execute("SELECT COUNT(*) FROM person;")
        total_patients = cur.fetchone()[0]
        _upsert(cur, "demographics", "total_patients", total_patients, total_patients)

        conn.commit()
        return {"gender": gender_dist, "age": age_dist, "total": total_patients}
    finally:
        conn.close()


def collect_top_conditions(**context):
    """상위 빈도 condition 수집"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()

        # 상위 20개 condition (concept_id 기준)
        cur.execute("""
            SELECT
                condition_concept_id,
                condition_source_value,
                COUNT(*) as occurrence_count,
                COUNT(DISTINCT person_id) as patient_count
            FROM condition_occurrence
            WHERE condition_concept_id IS NOT NULL
              AND condition_concept_id != 0
            GROUP BY condition_concept_id, condition_source_value
            ORDER BY occurrence_count DESC
            LIMIT 20;
        """)
        rows = cur.fetchall()
        print(f"  Top {len(rows)} conditions:")
        for i, row in enumerate(rows, 1):
            concept_id, source_val, occ_count, pat_count = row
            label = source_val or str(concept_id)
            entry = json.dumps({
                "concept_id": concept_id,
                "source_value": source_val,
                "occurrences": occ_count,
                "patients": pat_count,
            })
            _upsert(
                cur, "top_conditions",
                f"rank_{i:02d}", entry, occ_count
            )
            print(f"    {i}. {label} (concept:{concept_id}): "
                  f"{occ_count:,} occurrences, {pat_count:,} patients")

        conn.commit()
        return {"count": len(rows)}
    finally:
        conn.close()


def collect_top_drugs(**context):
    """상위 빈도 drug 수집"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()

        # 상위 20개 drug (concept_id 기준)
        cur.execute("""
            SELECT
                drug_concept_id,
                drug_source_value,
                COUNT(*) as exposure_count,
                COUNT(DISTINCT person_id) as patient_count
            FROM drug_exposure
            WHERE drug_concept_id IS NOT NULL
              AND drug_concept_id != 0
            GROUP BY drug_concept_id, drug_source_value
            ORDER BY exposure_count DESC
            LIMIT 20;
        """)
        rows = cur.fetchall()
        print(f"  Top {len(rows)} drugs:")
        for i, row in enumerate(rows, 1):
            concept_id, source_val, exp_count, pat_count = row
            label = source_val or str(concept_id)
            entry = json.dumps({
                "concept_id": concept_id,
                "source_value": source_val,
                "exposures": exp_count,
                "patients": pat_count,
            })
            _upsert(
                cur, "top_drugs",
                f"rank_{i:02d}", entry, exp_count
            )
            print(f"    {i}. {label} (concept:{concept_id}): "
                  f"{exp_count:,} exposures, {pat_count:,} patients")

        conn.commit()
        return {"count": len(rows)}
    finally:
        conn.close()


def collect_data_freshness(**context):
    """최근 데이터 적재 시점 확인"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()

        date_checks = {
            "condition_occurrence": "condition_start_date",
            "drug_exposure": "drug_exposure_start_date",
            "visit_occurrence": "visit_start_date",
            "measurement": "measurement_date",
        }

        for table, date_col in date_checks.items():
            cur.execute(
                f"SELECT MAX({date_col})::text, MIN({date_col})::text "  # noqa: S608
                f"FROM {table};"
            )
            row = cur.fetchone()
            max_date, min_date = row[0], row[1]
            _upsert(
                cur, "data_freshness",
                f"{table}_max_date", max_date or "N/A"
            )
            _upsert(
                cur, "data_freshness",
                f"{table}_min_date", min_date or "N/A"
            )
            print(f"  {table}: {min_date} ~ {max_date}")

        # 요약 실행 시각 기록
        _upsert(
            cur, "data_freshness",
            "last_summary_run", datetime.now().isoformat()
        )

        conn.commit()
        print(f"  Summary run at: {datetime.now().isoformat()}")
    finally:
        conn.close()


with DAG(
    "omop_stats_summary",
    default_args=default_args,
    description="OMOP CDM 전체 통계 요약을 생성하여 cdm_summary 테이블에 저장",
    schedule_interval="0 4 * * *",  # 매일 04:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["omop", "stats", "summary"],
) as dag:

    start = EmptyOperator(task_id="start")

    init_table = PythonOperator(
        task_id="ensure_summary_table",
        python_callable=ensure_summary_table,
    )

    table_counts = PythonOperator(
        task_id="collect_table_counts",
        python_callable=collect_table_counts,
    )

    demographics = PythonOperator(
        task_id="collect_demographics",
        python_callable=collect_demographics,
    )

    top_conditions = PythonOperator(
        task_id="collect_top_conditions",
        python_callable=collect_top_conditions,
    )

    top_drugs = PythonOperator(
        task_id="collect_top_drugs",
        python_callable=collect_top_drugs,
    )

    freshness = PythonOperator(
        task_id="collect_data_freshness",
        python_callable=collect_data_freshness,
    )

    end = EmptyOperator(task_id="end")

    start >> init_table
    init_table >> [table_counts, demographics, top_conditions, top_drugs, freshness]
    [table_counts, demographics, top_conditions, top_drugs, freshness] >> end
