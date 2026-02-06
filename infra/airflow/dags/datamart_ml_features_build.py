"""
ML 피처 빌드 파이프라인
주요 마트 데이터를 기반으로 ML 모델 학습용 피처 생성
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import time
import random

default_args = {
    'owner': 'ML Engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

def extract_patient_features(**context):
    """환자 피처 추출"""
    print("Extracting patient demographic features...")
    time.sleep(random.uniform(5, 10))
    print("Extracted features for 500,000 patients")
    return {'patient_count': 500000}

def extract_diagnosis_features(**context):
    """진단 피처 추출"""
    print("Extracting diagnosis features...")
    time.sleep(random.uniform(10, 15))
    print("Generated 2,500,000 diagnosis feature vectors")
    return {'diagnosis_count': 2500000}

def extract_medication_features(**context):
    """투약 피처 추출"""
    print("Extracting medication features...")
    time.sleep(random.uniform(8, 12))
    print("Generated 1,800,000 medication feature vectors")
    return {'medication_count': 1800000}

def merge_features(**context):
    """피처 병합"""
    print("Merging all feature sets...")
    time.sleep(random.uniform(15, 20))
    print("Merged features for 500,000 patients")
    return {'merged_count': 500000}

def normalize_features(**context):
    """피처 정규화"""
    print("Normalizing feature vectors...")
    time.sleep(random.uniform(5, 8))
    print("Normalization complete")
    return True

def save_feature_store(**context):
    """피처 스토어에 저장"""
    print("Saving to feature store...")
    time.sleep(random.uniform(3, 5))
    print("Features saved to ML_Features.Patient_Weekly")
    return True

with DAG(
    'datamart_ml_features_build',
    default_args=default_args,
    description='주요 마트 데이터를 기반으로 ML 모델 학습용 피처를 생성합니다.',
    schedule_interval='0 6 * * 1',  # 매주 월요일 06:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['ml', 'features', 'weekly'],
) as dag:

    start = EmptyOperator(task_id='start')

    extract_patient = PythonOperator(
        task_id='extract_patient_features',
        python_callable=extract_patient_features,
    )

    extract_diagnosis = PythonOperator(
        task_id='extract_diagnosis_features',
        python_callable=extract_diagnosis_features,
    )

    extract_medication = PythonOperator(
        task_id='extract_medication_features',
        python_callable=extract_medication_features,
    )

    merge = PythonOperator(
        task_id='merge_features',
        python_callable=merge_features,
    )

    normalize = PythonOperator(
        task_id='normalize_features',
        python_callable=normalize_features,
    )

    save = PythonOperator(
        task_id='save_feature_store',
        python_callable=save_feature_store,
    )

    end = EmptyOperator(task_id='end')

    start >> [extract_patient, extract_diagnosis, extract_medication] >> merge >> normalize >> save >> end
