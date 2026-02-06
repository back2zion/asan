"""
CDW 환자 마스터 동기화 파이프라인
Bronze -> Silver 단계로 환자 정보 정제
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
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_patient_data(**context):
    """Bronze 레이어에서 환자 데이터 추출"""
    print("Extracting patient data from Bronze layer...")
    time.sleep(random.uniform(2, 5))
    print("Extracted 50,000 patient records")
    return {'record_count': 50000}

def transform_patient_data(**context):
    """데이터 정제 및 변환"""
    print("Transforming patient data...")
    time.sleep(random.uniform(3, 7))
    print("Data cleansing completed")
    print("- Removed duplicates: 123")
    print("- Fixed null values: 456")
    return {'cleaned_count': 49421}

def load_to_silver(**context):
    """Silver 레이어에 적재"""
    print("Loading data to Silver layer...")
    time.sleep(random.uniform(2, 4))
    print("Successfully loaded to Silver.Patient_Master")
    return {'loaded_count': 49421}

def validate_data(**context):
    """데이터 품질 검증"""
    print("Validating data quality...")
    time.sleep(random.uniform(1, 3))
    print("Quality check passed: 99.8% accuracy")
    return {'quality_score': 0.998}

with DAG(
    'cdw_patient_master_sync',
    default_args=default_args,
    description='CDW 환자 마스터 정보를 Bronze에서 Silver 단계로 정제하고 동기화합니다.',
    schedule_interval='0 1 * * *',  # 매일 01:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['cdw', 'patient', 'etl', 'daily'],
) as dag:

    start = EmptyOperator(task_id='start')

    extract = PythonOperator(
        task_id='extract_patient_data',
        python_callable=extract_patient_data,
    )

    transform = PythonOperator(
        task_id='transform_patient_data',
        python_callable=transform_patient_data,
    )

    load = PythonOperator(
        task_id='load_to_silver',
        python_callable=load_to_silver,
    )

    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    end = EmptyOperator(task_id='end')

    start >> extract >> transform >> load >> validate >> end
