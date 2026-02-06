"""
실시간 바이탈 스트리밍 파이프라인
EMR에서 발생하는 환자 바이탈 데이터를 실시간으로 수집하여 Landing Zone에 적재
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.time_delta import TimeDeltaSensor
import time
import random

default_args = {
    'owner': 'Platform Team',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
}

def check_kafka_connection(**context):
    """Kafka 연결 확인"""
    print("Checking Kafka connection...")
    time.sleep(random.uniform(1, 2))
    print("Kafka cluster is healthy")
    return True

def consume_vitals_batch(**context):
    """바이탈 데이터 배치 소비"""
    batch_size = random.randint(1000, 5000)
    print(f"Consuming vitals batch from Kafka...")
    time.sleep(random.uniform(2, 5))
    print(f"Consumed {batch_size} vital sign records")
    return {'batch_size': batch_size}

def validate_vitals(**context):
    """바이탈 데이터 검증"""
    print("Validating vital signs data...")
    time.sleep(random.uniform(1, 2))
    print("- Heart rate range check: PASS")
    print("- Blood pressure range check: PASS")
    print("- SpO2 range check: PASS")
    return True

def write_to_landing_zone(**context):
    """Landing Zone에 적재"""
    print("Writing to Landing Zone (Parquet format)...")
    time.sleep(random.uniform(1, 3))
    print("Data written to s3://datalake/landing/vitals/")
    return True

def update_metadata(**context):
    """메타데이터 업데이트"""
    print("Updating metadata catalog...")
    time.sleep(random.uniform(0.5, 1))
    print("Metadata updated")
    return True

with DAG(
    'realtime_vitals_stream_to_lake',
    default_args=default_args,
    description='EMR에서 발생하는 환자 바이탈 데이터를 실시간으로 수집하여 Landing Zone에 적재합니다.',
    schedule_interval='*/5 * * * *',  # 5분마다 실행 (스트리밍 시뮬레이션)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['streaming', 'vitals', 'realtime', 'emr'],
) as dag:

    start = EmptyOperator(task_id='start')

    check_kafka = PythonOperator(
        task_id='check_kafka_connection',
        python_callable=check_kafka_connection,
    )

    consume = PythonOperator(
        task_id='consume_vitals_batch',
        python_callable=consume_vitals_batch,
    )

    validate = PythonOperator(
        task_id='validate_vitals',
        python_callable=validate_vitals,
    )

    write = PythonOperator(
        task_id='write_to_landing_zone',
        python_callable=write_to_landing_zone,
    )

    update_meta = PythonOperator(
        task_id='update_metadata',
        python_callable=update_metadata,
    )

    end = EmptyOperator(task_id='end')

    start >> check_kafka >> consume >> validate >> write >> update_meta >> end
