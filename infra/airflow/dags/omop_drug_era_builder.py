"""
OMOP CDM Drug Era Builder
drug_exposure 테이블에서 drug_era를 생성하는 표준 OMOP ETL 파이프라인.
같은 환자의 같은 drug이 30일 이내 연속이면 하나의 era로 병합.
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


def build_drug_era(**context):
    """
    OMOP 표준 로직으로 drug_era를 생성한다.

    알고리즘:
    1. drug_exposure에서 (person_id, drug_concept_id, start_date, end_date) 추출
    2. end_date가 NULL이면 start_date + days_supply (또는 start_date + 1) 로 대체
    3. 같은 (person_id, drug_concept_id) 그룹 내에서 날짜순 정렬
    4. 이전 exposure의 end_date + 30일 이내에 다음 exposure가 시작되면 같은 era로 병합
    5. gap_days = era 내 exposure 사이 빈 날 수 합계
    6. 결과를 drug_era 테이블에 TRUNCATE & INSERT
    """
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()

        # 기존 drug_era 데이터 삭제
        cur.execute("TRUNCATE TABLE drug_era;")
        print("Truncated drug_era table")

        # SQL 기반으로 drug_era 생성 (OMOP 표준 era 알고리즘)
        era_sql = f"""
        INSERT INTO drug_era
            (drug_era_id, person_id, drug_concept_id,
             drug_era_start_date, drug_era_end_date,
             drug_exposure_count, gap_days)
        WITH cte_source AS (
            SELECT
                person_id,
                drug_concept_id,
                drug_exposure_start_date,
                COALESCE(
                    drug_exposure_end_date,
                    drug_exposure_start_date + COALESCE(days_supply, 1) * INTERVAL '1 day'
                )::date AS drug_exposure_end_date
            FROM drug_exposure
            WHERE drug_concept_id IS NOT NULL
              AND drug_concept_id != 0
        ),
        cte_end_dates AS (
            SELECT DISTINCT
                person_id,
                drug_concept_id,
                (drug_exposure_end_date + INTERVAL '{PERSISTENCE_WINDOW} days')::date AS end_date
            FROM cte_source
        ),
        cte_ends AS (
            SELECT
                e.person_id,
                e.drug_concept_id,
                e.end_date
            FROM cte_end_dates e
            WHERE NOT EXISTS (
                SELECT 1
                FROM cte_source s
                WHERE s.person_id = e.person_id
                  AND s.drug_concept_id = e.drug_concept_id
                  AND s.drug_exposure_start_date <= e.end_date
                  AND (s.drug_exposure_end_date + INTERVAL '{PERSISTENCE_WINDOW} days')::date > e.end_date
            )
        ),
        cte_era_ends AS (
            SELECT
                s.person_id,
                s.drug_concept_id,
                s.drug_exposure_start_date,
                s.drug_exposure_end_date,
                MIN(e.end_date) AS era_end_date
            FROM cte_source s
            JOIN cte_ends e
              ON s.person_id = e.person_id
             AND s.drug_concept_id = e.drug_concept_id
             AND e.end_date >= s.drug_exposure_start_date
            GROUP BY s.person_id, s.drug_concept_id,
                     s.drug_exposure_start_date, s.drug_exposure_end_date
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY person_id, drug_concept_id, era_start_date),
            person_id,
            drug_concept_id,
            era_start_date,
            (era_end_date - INTERVAL '{PERSISTENCE_WINDOW} days')::date AS era_end_date,
            exposure_count,
            -- gap_days: era 전체 기간 - 실제 exposure 기간 합
            GREATEST(
                (era_end_date - INTERVAL '{PERSISTENCE_WINDOW} days')::date - era_start_date
                - total_exposure_days, 0
            )::integer AS gap_days
        FROM (
            SELECT
                person_id,
                drug_concept_id,
                MIN(drug_exposure_start_date) AS era_start_date,
                era_end_date,
                COUNT(*) AS exposure_count,
                SUM(drug_exposure_end_date - drug_exposure_start_date) AS total_exposure_days
            FROM cte_era_ends
            GROUP BY person_id, drug_concept_id, era_end_date
        ) sub;
        """

        cur.execute(era_sql)
        row_count = cur.rowcount
        conn.commit()

        print(f"Built {row_count} drug eras from drug_exposure")

        # 검증: 통계 출력
        cur.execute("SELECT COUNT(DISTINCT person_id) FROM drug_era;")
        patient_count = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT drug_concept_id) FROM drug_era;")
        concept_count = cur.fetchone()[0]
        cur.execute(
            "SELECT AVG(drug_exposure_count)::numeric(10,2) FROM drug_era;"
        )
        avg_count = cur.fetchone()[0]
        cur.execute(
            "SELECT AVG(gap_days)::numeric(10,2) FROM drug_era;"
        )
        avg_gap = cur.fetchone()[0]

        print(f"  - Distinct patients: {patient_count}")
        print(f"  - Distinct drugs: {concept_count}")
        print(f"  - Avg exposures per era: {avg_count}")
        print(f"  - Avg gap days per era: {avg_gap}")

        return {
            "era_count": row_count,
            "patient_count": patient_count,
            "concept_count": concept_count,
        }
    finally:
        conn.close()


def verify_drug_era(**context):
    """생성된 drug_era 데이터 무결성 검증"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()
        issues = []

        # 1. era_start_date <= era_end_date
        cur.execute("""
            SELECT COUNT(*)
            FROM drug_era
            WHERE drug_era_start_date > drug_era_end_date;
        """)
        bad_dates = cur.fetchone()[0]
        if bad_dates > 0:
            issues.append(f"Found {bad_dates} eras with start_date > end_date")

        # 2. person_id FK
        cur.execute("""
            SELECT COUNT(*)
            FROM drug_era de
            WHERE NOT EXISTS (
                SELECT 1 FROM person p WHERE p.person_id = de.person_id
            );
        """)
        orphans = cur.fetchone()[0]
        if orphans > 0:
            issues.append(f"Found {orphans} eras with invalid person_id")

        # 3. gap_days >= 0
        cur.execute("""
            SELECT COUNT(*)
            FROM drug_era
            WHERE gap_days < 0;
        """)
        neg_gaps = cur.fetchone()[0]
        if neg_gaps > 0:
            issues.append(f"Found {neg_gaps} eras with negative gap_days")

        if issues:
            msg = "Drug Era verification issues:\n" + "\n".join(issues)
            print(msg)
            raise ValueError(msg)

        cur.execute("SELECT COUNT(*) FROM drug_era;")
        total = cur.fetchone()[0]
        print(f"Verification passed: {total} drug eras are valid")
        return {"verified": True, "total": total}
    finally:
        conn.close()


with DAG(
    "omop_drug_era_builder",
    default_args=default_args,
    description="drug_exposure에서 drug_era를 생성하는 OMOP 표준 ETL 파이프라인",
    schedule_interval="30 2 * * *",  # 매일 02:30
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["omop", "era", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")

    build = PythonOperator(
        task_id="build_drug_era",
        python_callable=build_drug_era,
    )

    verify = PythonOperator(
        task_id="verify_drug_era",
        python_callable=verify_drug_era,
    )

    end = EmptyOperator(task_id="end")

    start >> build >> verify >> end
