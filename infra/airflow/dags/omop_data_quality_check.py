"""
OMOP CDM 데이터 품질 검증 파이프라인
주요 테이블의 row count, NULL 비율, 날짜 유효성, FK 참조 무결성을 검증.
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

# 검증 대상 테이블과 예상 최소 row count
EXPECTED_MIN_COUNTS = {
    "person": 1000,
    "condition_occurrence": 5000,
    "drug_exposure": 10000,
    "visit_occurrence": 20000,
    "measurement": 100000,
    "condition_era": 100,
    "drug_era": 100,
}

# 필수 컬럼 (NULL이면 안 되는 컬럼) 정의
REQUIRED_COLUMNS = {
    "person": ["person_id", "gender_concept_id", "year_of_birth"],
    "condition_occurrence": [
        "condition_occurrence_id",
        "person_id",
        "condition_concept_id",
        "condition_start_date",
    ],
    "drug_exposure": [
        "drug_exposure_id",
        "person_id",
        "drug_concept_id",
        "drug_exposure_start_date",
    ],
    "visit_occurrence": [
        "visit_occurrence_id",
        "person_id",
        "visit_concept_id",
        "visit_start_date",
    ],
    "measurement": [
        "measurement_id",
        "person_id",
        "measurement_concept_id",
        "measurement_date",
    ],
}

# 날짜 컬럼 (미래 날짜가 없어야 하는 컬럼)
DATE_COLUMNS = {
    "condition_occurrence": ["condition_start_date", "condition_end_date"],
    "drug_exposure": ["drug_exposure_start_date", "drug_exposure_end_date"],
    "visit_occurrence": ["visit_start_date", "visit_end_date"],
    "measurement": ["measurement_date"],
}

# FK 참조 무결성 검증 (child_table.column -> person.person_id)
FK_PERSON_TABLES = [
    "condition_occurrence",
    "drug_exposure",
    "visit_occurrence",
    "measurement",
    "condition_era",
    "drug_era",
]

default_args = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def check_row_counts(**context):
    """주요 테이블 row count 확인"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()
        results = {}
        warnings = []

        for table, min_count in EXPECTED_MIN_COUNTS.items():
            cur.execute(f"SELECT COUNT(*) FROM {table};")  # noqa: S608
            count = cur.fetchone()[0]
            results[table] = count
            status = "OK" if count >= min_count else "WARNING"
            print(f"  {table}: {count:,} rows [{status}]")
            if count < min_count:
                warnings.append(
                    f"{table}: {count} rows (expected >= {min_count})"
                )

        if warnings:
            print(f"\nRow count warnings ({len(warnings)}):")
            for w in warnings:
                print(f"  - {w}")

        context["ti"].xcom_push(key="row_counts", value=results)
        context["ti"].xcom_push(key="row_count_warnings", value=warnings)
        return results
    finally:
        conn.close()


def check_null_ratios(**context):
    """필수 컬럼의 NULL 비율 체크"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()
        issues = []

        for table, columns in REQUIRED_COLUMNS.items():
            cur.execute(f"SELECT COUNT(*) FROM {table};")  # noqa: S608
            total = cur.fetchone()[0]
            if total == 0:
                print(f"  {table}: EMPTY (skipping NULL check)")
                continue

            for col in columns:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL;"  # noqa: S608
                )
                null_count = cur.fetchone()[0]
                null_pct = (null_count / total) * 100
                status = "OK" if null_count == 0 else "ISSUE"
                if null_count > 0:
                    print(
                        f"  {table}.{col}: {null_count:,} NULLs "
                        f"({null_pct:.2f}%) [{status}]"
                    )
                    issues.append(
                        f"{table}.{col}: {null_count} NULLs ({null_pct:.2f}%)"
                    )

        if not issues:
            print("  All required columns have zero NULLs")
        else:
            print(f"\nNULL issues ({len(issues)}):")
            for i in issues:
                print(f"  - {i}")

        context["ti"].xcom_push(key="null_issues", value=issues)
        return {"issue_count": len(issues), "issues": issues}
    finally:
        conn.close()


def check_date_validity(**context):
    """날짜 범위 유효성 검증 (미래 날짜 없어야 함)"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()
        issues = []

        for table, columns in DATE_COLUMNS.items():
            for col in columns:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table} "  # noqa: S608
                    f"WHERE {col} > CURRENT_DATE;"
                )
                future_count = cur.fetchone()[0]
                if future_count > 0:
                    print(
                        f"  {table}.{col}: {future_count:,} future dates [ISSUE]"
                    )
                    issues.append(
                        f"{table}.{col}: {future_count} future dates"
                    )

        if not issues:
            print("  No future dates found in any date column")
        else:
            print(f"\nDate validity issues ({len(issues)}):")
            for i in issues:
                print(f"  - {i}")

        context["ti"].xcom_push(key="date_issues", value=issues)
        return {"issue_count": len(issues), "issues": issues}
    finally:
        conn.close()


def check_fk_integrity(**context):
    """FK 참조 무결성 검증 (person_id가 person 테이블에 존재하는지)"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()
        issues = []

        for table in FK_PERSON_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {table};")  # noqa: S608
            total = cur.fetchone()[0]
            if total == 0:
                print(f"  {table}: EMPTY (skipping FK check)")
                continue

            cur.execute(
                f"SELECT COUNT(*) FROM {table} t "  # noqa: S608
                f"WHERE t.person_id IS NOT NULL "
                f"AND NOT EXISTS ("
                f"  SELECT 1 FROM person p WHERE p.person_id = t.person_id"
                f");"
            )
            orphan_count = cur.fetchone()[0]
            if orphan_count > 0:
                pct = (orphan_count / total) * 100
                print(
                    f"  {table}: {orphan_count:,} orphan records "
                    f"({pct:.2f}%) [ISSUE]"
                )
                issues.append(
                    f"{table}: {orphan_count} orphan person_id refs"
                )
            else:
                print(f"  {table}: FK integrity OK ({total:,} rows)")

        if issues:
            print(f"\nFK integrity issues ({len(issues)}):")
            for i in issues:
                print(f"  - {i}")

        context["ti"].xcom_push(key="fk_issues", value=issues)
        return {"issue_count": len(issues), "issues": issues}
    finally:
        conn.close()


def generate_quality_report(**context):
    """전체 품질 검증 결과 리포트 생성"""
    ti = context["ti"]
    row_counts = ti.xcom_pull(
        task_ids="check_row_counts", key="row_counts"
    ) or {}
    row_warnings = ti.xcom_pull(
        task_ids="check_row_counts", key="row_count_warnings"
    ) or []
    null_issues = ti.xcom_pull(
        task_ids="check_null_ratios", key="null_issues"
    ) or []
    date_issues = ti.xcom_pull(
        task_ids="check_date_validity", key="date_issues"
    ) or []
    fk_issues = ti.xcom_pull(
        task_ids="check_fk_integrity", key="fk_issues"
    ) or []

    total_issues = len(row_warnings) + len(null_issues) + len(date_issues) + len(fk_issues)
    total_rows = sum(row_counts.values()) if row_counts else 0

    report = {
        "timestamp": datetime.now().isoformat(),
        "total_rows": total_rows,
        "total_issues": total_issues,
        "row_counts": row_counts,
        "row_count_warnings": row_warnings,
        "null_issues": null_issues,
        "date_issues": date_issues,
        "fk_issues": fk_issues,
    }

    print("=" * 60)
    print("OMOP CDM Data Quality Report")
    print("=" * 60)
    print(f"Total rows across checked tables: {total_rows:,}")
    print(f"Total issues found: {total_issues}")
    print(f"  - Row count warnings: {len(row_warnings)}")
    print(f"  - NULL violations: {len(null_issues)}")
    print(f"  - Date validity issues: {len(date_issues)}")
    print(f"  - FK integrity issues: {len(fk_issues)}")
    print("=" * 60)

    if total_issues > 0:
        print("\nWARNING: Data quality issues detected. Review details above.")
    else:
        print("\nAll quality checks passed successfully.")

    return report


with DAG(
    "omop_data_quality_check",
    default_args=default_args,
    description="OMOP CDM 데이터 무결성을 검증하는 품질 체크 파이프라인",
    schedule_interval="0 3 * * *",  # 매일 03:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["omop", "quality", "validation"],
) as dag:

    start = EmptyOperator(task_id="start")

    row_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )

    null_ratios = PythonOperator(
        task_id="check_null_ratios",
        python_callable=check_null_ratios,
    )

    date_validity = PythonOperator(
        task_id="check_date_validity",
        python_callable=check_date_validity,
    )

    fk_integrity = PythonOperator(
        task_id="check_fk_integrity",
        python_callable=check_fk_integrity,
    )

    report = PythonOperator(
        task_id="generate_quality_report",
        python_callable=generate_quality_report,
    )

    end = EmptyOperator(task_id="end")

    start >> [row_counts, null_ratios, date_validity, fk_integrity] >> report >> end
