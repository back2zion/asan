"""
EDW 처방 데이터 일별 배치 파이프라인
OCS 처방 데이터를 취합하여 일별 EDW 배치 작업 수행
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import time
import random

default_args = {
    'owner': 'Data Engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

def extract_ocs_prescriptions(**context):
    """OCS에서 처방 데이터 추출"""
    print("Connecting to OCS database...")
    time.sleep(random.uniform(1, 3))
    print("Extracting prescription data from OCS...")
    time.sleep(random.uniform(5, 10))
    print("Extracted 125,000 prescription records")
    return {'record_count': 125000}

def validate_prescriptions(**context):
    """처방 데이터 유효성 검증"""
    print("Validating prescription data...")
    time.sleep(random.uniform(2, 4))
    print("- Invalid records found: 42")
    print("- Quarantined invalid records")
    return {'valid_count': 124958}

def transform_prescriptions(**context):
    """처방 데이터 변환 및 표준화"""
    print("Transforming prescription data...")
    time.sleep(random.uniform(5, 8))
    print("- Standardized medication codes")
    print("- Converted units to standard format")
    return {'transformed_count': 124958}

def load_to_edw(**context):
    """EDW에 적재"""
    print("Loading data to EDW...")
    time.sleep(random.uniform(3, 6))
    print("Successfully loaded to EDW.Prescription_Daily")
    return {'loaded_count': 124958}

def update_statistics(**context):
    """통계 테이블 업데이트"""
    print("Updating statistics tables...")
    time.sleep(random.uniform(1, 2))
    print("Statistics updated successfully")
    return True

with DAG(
    'edw_prescription_daily_batch',
    default_args=default_args,
    description='OCS 처방 데이터를 취합하여 일별 EDW 배치 작업을 수행합니다.',
    schedule_interval='0 3 * * *',  # 매일 03:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['edw', 'prescription', 'etl', 'daily'],
) as dag:

    start = EmptyOperator(task_id='start')

    extract = PythonOperator(
        task_id='extract_ocs_prescriptions',
        python_callable=extract_ocs_prescriptions,
    )

    validate = PythonOperator(
        task_id='validate_prescriptions',
        python_callable=validate_prescriptions,
    )

    transform = PythonOperator(
        task_id='transform_prescriptions',
        python_callable=transform_prescriptions,
    )

    load = PythonOperator(
        task_id='load_to_edw',
        python_callable=load_to_edw,
    )

    update_stats = PythonOperator(
        task_id='update_statistics',
        python_callable=update_statistics,
    )

    end = EmptyOperator(task_id='end')

    start >> extract >> validate >> transform >> load >> update_stats >> end
