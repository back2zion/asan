"""
OMOP CDM Condition Era Builder
condition_occurrence 테이블에서 condition_era를 생성하는 표준 OMOP ETL 파이프라인.
같은 환자의 같은 condition이 30일 이내 연속이면 하나의 era로 병합.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import psycopg2

OMOP_DB_CONN = {
    "host": "omop-db",
    "port": 5432,
    "dbname": "omop_cdm",
    "user": "omopuser",
    "password": "omop",
}

PERSISTENCE_WINDOW = 30  # days

default_args = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def build_condition_era(**context):
    """
    OMOP 표준 로직으로 condition_era를 생성한다.

    알고리즘:
    1. condition_occurrence에서 (person_id, condition_concept_id, start_date, end_date) 추출
    2. end_date가 NULL이면 start_date로 대체
    3. 같은 (person_id, condition_concept_id) 그룹 내에서 날짜순 정렬
    4. 이전 occurrence의 end_date + 30일 이내에 다음 occurrence가 시작되면 같은 era로 병합
    5. 결과를 condition_era 테이블에 TRUNCATE & INSERT
    """
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()

        # 기존 condition_era 데이터 삭제
        cur.execute("TRUNCATE TABLE condition_era;")
        print("Truncated condition_era table")

        # SQL 기반으로 condition_era 생성 (OMOP 표준 era 알고리즘)
        # Window function으로 그룹 경계를 탐지하고, 연속 occurrence를 era로 병합
        era_sql = f"""
        INSERT INTO condition_era
            (condition_era_id, person_id, condition_concept_id,
             condition_era_start_date, condition_era_end_date,
             condition_occurrence_count)
        WITH cte_source AS (
            SELECT
                person_id,
                condition_concept_id,
                condition_start_date,
                COALESCE(condition_end_date, condition_start_date) AS condition_end_date
            FROM condition_occurrence
            WHERE condition_concept_id IS NOT NULL
              AND condition_concept_id != 0
        ),
        cte_end_dates AS (
            -- 각 occurrence의 end_date + persistence window를 후보 era 경계로 사용
            SELECT DISTINCT
                person_id,
                condition_concept_id,
                (condition_end_date + INTERVAL '{PERSISTENCE_WINDOW} days')::date AS end_date
            FROM cte_source
        ),
        cte_ends AS (
            -- era 경계 후보 중, 그 시점에 활성 occurrence가 없는 날짜만 선택
            SELECT
                e.person_id,
                e.condition_concept_id,
                e.end_date
            FROM cte_end_dates e
            WHERE NOT EXISTS (
                SELECT 1
                FROM cte_source s
                WHERE s.person_id = e.person_id
                  AND s.condition_concept_id = e.condition_concept_id
                  AND s.condition_start_date <= e.end_date
                  AND (s.condition_end_date + INTERVAL '{PERSISTENCE_WINDOW} days')::date > e.end_date
            )
        ),
        cte_era_ends AS (
            -- 각 occurrence에 대해 가장 가까운 era 종료일 매핑
            SELECT
                s.person_id,
                s.condition_concept_id,
                s.condition_start_date,
                s.condition_end_date,
                MIN(e.end_date) AS era_end_date
            FROM cte_source s
            JOIN cte_ends e
              ON s.person_id = e.person_id
             AND s.condition_concept_id = e.condition_concept_id
             AND e.end_date >= s.condition_start_date
            GROUP BY s.person_id, s.condition_concept_id,
                     s.condition_start_date, s.condition_end_date
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY person_id, condition_concept_id, era_start_date),
            person_id,
            condition_concept_id,
            era_start_date,
            (era_end_date - INTERVAL '{PERSISTENCE_WINDOW} days')::date AS era_end_date,
            occurrence_count
        FROM (
            SELECT
                person_id,
                condition_concept_id,
                MIN(condition_start_date) AS era_start_date,
                era_end_date,
                COUNT(*) AS occurrence_count
            FROM cte_era_ends
            GROUP BY person_id, condition_concept_id, era_end_date
        ) sub;
        """

        cur.execute(era_sql)
        row_count = cur.rowcount
        conn.commit()

        print(f"Built {row_count} condition eras from condition_occurrence")

        # 검증: 몇 가지 통계 출력
        cur.execute("SELECT COUNT(DISTINCT person_id) FROM condition_era;")
        patient_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT condition_concept_id) FROM condition_era;")
        concept_count = cur.fetchone()[0]
        cur.execute(
            "SELECT AVG(condition_occurrence_count)::numeric(10,2) FROM condition_era;"
        )
        avg_count = cur.fetchone()[0]

        print(f"  - Distinct patients: {patient_count}")
        print(f"  - Distinct conditions: {concept_count}")
        print(f"  - Avg occurrences per era: {avg_count}")

        return {
            "era_count": row_count,
            "patient_count": patient_count,
            "concept_count": concept_count,
        }
    finally:
        conn.close()


def verify_condition_era(**context):
    """생성된 condition_era 데이터 무결성 검증"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()
        issues = []

        # 1. era_start_date <= era_end_date 확인
        cur.execute("""
            SELECT COUNT(*)
            FROM condition_era
            WHERE condition_era_start_date > condition_era_end_date;
        """)
        bad_dates = cur.fetchone()[0]
        if bad_dates > 0:
            issues.append(f"Found {bad_dates} eras with start_date > end_date")

        # 2. person_id FK 참조 확인
        cur.execute("""
            SELECT COUNT(*)
            FROM condition_era ce
            WHERE NOT EXISTS (
                SELECT 1 FROM person p WHERE p.person_id = ce.person_id
            );
        """)
        orphans = cur.fetchone()[0]
        if orphans > 0:
            issues.append(f"Found {orphans} eras with invalid person_id")

        # 3. occurrence_count > 0 확인
        cur.execute("""
            SELECT COUNT(*)
            FROM condition_era
            WHERE condition_occurrence_count IS NULL
               OR condition_occurrence_count <= 0;
        """)
        bad_counts = cur.fetchone()[0]
        if bad_counts > 0:
            issues.append(f"Found {bad_counts} eras with invalid occurrence_count")

        if issues:
            msg = "Condition Era verification issues:\n" + "\n".join(issues)
            print(msg)
            raise ValueError(msg)

        cur.execute("SELECT COUNT(*) FROM condition_era;")
        total = cur.fetchone()[0]
        print(f"Verification passed: {total} condition eras are valid")
        return {"verified": True, "total": total}
    finally:
        conn.close()


with DAG(
    "omop_condition_era_builder",
    default_args=default_args,
    description="condition_occurrence에서 condition_era를 생성하는 OMOP 표준 ETL 파이프라인",
    schedule_interval="0 2 * * *",  # 매일 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["omop", "era", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    build = PythonOperator(
        task_id="build_condition_era",
        python_callable=build_condition_era,
    )

    verify = PythonOperator(
        task_id="verify_condition_era",
        python_callable=verify_condition_era,
    )

    end = EmptyOperator(task_id="end")

    start >> build >> verify >> end
