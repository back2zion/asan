"""
OMOP CDM 준 실시간 동기화 파이프라인
주요 임상 테이블의 변경분을 30분 간격으로 감지하고 증분 적재(CDC).
SFR-003: 준 실시간 데이터 처리 기능 (30분 간격)

대상 테이블:
  - measurement (검사 결과) — 가장 빈번한 변경
  - condition_occurrence (진단) — 외래/입원 진단
  - visit_occurrence (방문) — 접수/퇴원
  - drug_exposure (약물) — 처방
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

# CDC 워터마크 저장 테이블 (없으면 자동 생성)
WATERMARK_TABLE = "_etl_watermarks"

# 동기화 대상 테이블 및 타임스탬프 컬럼
SYNC_TARGETS = {
    "measurement": {
        "ts_column": "measurement_date",
        "pk_column": "measurement_id",
        "label": "검사 결과",
    },
    "condition_occurrence": {
        "ts_column": "condition_start_date",
        "pk_column": "condition_occurrence_id",
        "label": "진단",
    },
    "visit_occurrence": {
        "ts_column": "visit_start_date",
        "pk_column": "visit_occurrence_id",
        "label": "방문",
    },
    "drug_exposure": {
        "ts_column": "drug_exposure_start_date",
        "pk_column": "drug_exposure_id",
        "label": "약물 처방",
    },
}

default_args = {
    "owner": "Data Engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


def ensure_watermark_table(**context):
    """워터마크 테이블 생성 (없으면)"""
    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
                table_name VARCHAR(100) PRIMARY KEY,
                last_sync_ts TIMESTAMP NOT NULL DEFAULT '2000-01-01',
                last_sync_id BIGINT NOT NULL DEFAULT 0,
                rows_synced BIGINT NOT NULL DEFAULT 0,
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
        """)
        # 대상 테이블 워터마크 초기화
        for table_name in SYNC_TARGETS:
            cur.execute(f"""
                INSERT INTO {WATERMARK_TABLE} (table_name)
                VALUES (%s)
                ON CONFLICT (table_name) DO NOTHING;
            """, (table_name,))
        conn.commit()
        print(f"Watermark table '{WATERMARK_TABLE}' ready")
    finally:
        conn.close()


def sync_table(table_name: str, **context):
    """
    단일 테이블의 증분 동기화 수행.
    워터마크(마지막 동기화 ID) 이후의 새 레코드를 감지하고 통계를 기록.
    실제 운영에서는 여기서 소스→타겟 복사가 수행됨.
    """
    target = SYNC_TARGETS[table_name]
    pk_col = target["pk_column"]
    ts_col = target["ts_column"]
    label = target["label"]

    conn = psycopg2.connect(**OMOP_DB_CONN)
    try:
        cur = conn.cursor()

        # 현재 워터마크 조회
        cur.execute(
            f"SELECT last_sync_id, last_sync_ts FROM {WATERMARK_TABLE} WHERE table_name = %s",
            (table_name,)
        )
        row = cur.fetchone()
        last_id = row[0] if row else 0
        last_ts = row[1] if row else datetime(2000, 1, 1)

        # 새 레코드 감지 (워터마크 ID 이후)
        cur.execute(
            f"SELECT COUNT(*), MAX({pk_col}), MAX({ts_col}) "
            f"FROM {table_name} WHERE {pk_col} > %s",
            (last_id,)
        )
        result = cur.fetchone()
        new_count = result[0] or 0
        new_max_id = result[1] or last_id
        new_max_ts = result[2] or last_ts

        print(f"[{label}] {table_name}: {new_count:,} new records since ID {last_id}")

        if new_count > 0:
            # 워터마크 업데이트
            cur.execute(
                f"""UPDATE {WATERMARK_TABLE}
                    SET last_sync_id = %s, last_sync_ts = %s,
                        rows_synced = rows_synced + %s, updated_at = NOW()
                    WHERE table_name = %s""",
                (new_max_id, new_max_ts, new_count, table_name)
            )
            conn.commit()
            print(f"  Watermark updated: ID {last_id} → {new_max_id}")
        else:
            print(f"  No new records. Watermark unchanged at ID {last_id}")

        # XCom으로 결과 전달
        context["ti"].xcom_push(
            key=f"sync_{table_name}",
            value={
                "table": table_name,
                "new_records": new_count,
                "last_id": int(new_max_id),
                "timestamp": str(new_max_ts),
            }
        )
        return new_count
    finally:
        conn.close()


def generate_sync_report(**context):
    """동기화 결과 통합 리포트"""
    ti = context["ti"]
    total_new = 0
    report_lines = []

    for table_name in SYNC_TARGETS:
        result = ti.xcom_pull(
            task_ids=f"sync_{table_name}",
            key=f"sync_{table_name}",
        )
        if result:
            count = result.get("new_records", 0)
            total_new += count
            status = "NEW" if count > 0 else "NO_CHANGE"
            report_lines.append(
                f"  {table_name}: {count:,} records [{status}]"
            )

    print("=" * 60)
    print("OMOP CDM Realtime Sync Report (30min)")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)
    for line in report_lines:
        print(line)
    print(f"\nTotal new records: {total_new:,}")
    print("=" * 60)

    return {"total_new": total_new, "tables": len(SYNC_TARGETS)}


# --- DAG 정의 ---
with DAG(
    "omop_realtime_sync",
    default_args=default_args,
    description="주요 임상 테이블 30분 간격 증분 동기화 (CDC)",
    schedule_interval="*/30 * * * *",  # 30분 간격
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # 동시 실행 방지
    tags=["omop", "realtime", "cdc", "sync"],
) as dag:

    start = EmptyOperator(task_id="start")

    init_watermark = PythonOperator(
        task_id="ensure_watermark_table",
        python_callable=ensure_watermark_table,
    )

    # 테이블별 병렬 동기화 태스크
    sync_tasks = []
    for tbl in SYNC_TARGETS:
        task = PythonOperator(
            task_id=f"sync_{tbl}",
            python_callable=sync_table,
            op_args=[tbl],
        )
        sync_tasks.append(task)

    report = PythonOperator(
        task_id="generate_sync_report",
        python_callable=generate_sync_report,
    )

    end = EmptyOperator(task_id="end")

    # 워터마크 초기화 → 테이블별 병렬 동기화 → 리포트
    start >> init_watermark >> sync_tasks >> report >> end
