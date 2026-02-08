"""
MinIO S3 오브젝트 스토리지 클라이언트

플랫폼 공용 S3 스토리지 — ETL 아티팩트, 모델 가중치, 문서, 백업, 데이터 내보내기 등.
"""
import logging
from typing import Optional

from minio import Minio
from minio.error import S3Error

from core.config import settings

logger = logging.getLogger(__name__)

# IDP 버킷 정의
IDP_BUCKETS = [
    "idp-etl",         # ETL 아티팩트
    "idp-models",      # 모델 가중치
    "idp-documents",   # 문서 저장
    "idp-backups",     # 백업
    "idp-exports",     # 데이터 내보내기
]

_client: Optional[Minio] = None


def get_s3_client() -> Minio:
    """MinIO 클라이언트 싱글톤을 반환합니다."""
    global _client
    if _client is None:
        _client = Minio(
            endpoint=settings.MINIO_ENDPOINT,
            access_key=settings.MINIO_ACCESS_KEY,
            secret_key=settings.MINIO_SECRET_KEY,
            secure=settings.MINIO_SECURE,
        )
    return _client


def ensure_buckets() -> None:
    """IDP 버킷이 없으면 자동 생성합니다."""
    client = get_s3_client()
    for bucket in IDP_BUCKETS:
        try:
            if not client.bucket_exists(bucket):
                client.make_bucket(bucket)
                logger.info(f"Created S3 bucket: {bucket}")
            else:
                logger.debug(f"S3 bucket already exists: {bucket}")
        except S3Error as e:
            logger.warning(f"Failed to ensure bucket '{bucket}': {e}")
        except Exception as e:
            logger.warning(f"S3 connection error for bucket '{bucket}': {e}")
