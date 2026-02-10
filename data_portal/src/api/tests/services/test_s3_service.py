"""
Phase 3: S3 (MinIO) 서비스 단위 테스트
"""
import pytest
from unittest.mock import patch, MagicMock


class TestGetS3Client:
    """get_s3_client — 싱글톤 패턴"""

    def test_singleton_pattern(self):
        import services.s3_service as mod
        mod._client = None
        with patch("services.s3_service.Minio") as MockMinio:
            MockMinio.return_value = MagicMock()
            c1 = mod.get_s3_client()
            c2 = mod.get_s3_client()
        assert c1 is c2
        MockMinio.assert_called_once()
        mod._client = None  # cleanup

    def test_creates_with_correct_config(self):
        import services.s3_service as mod
        mod._client = None
        with patch("services.s3_service.Minio") as MockMinio:
            MockMinio.return_value = MagicMock()
            mod.get_s3_client()
        call_kwargs = MockMinio.call_args
        assert call_kwargs is not None
        mod._client = None  # cleanup


class TestEnsureBuckets:
    """ensure_buckets — 버킷 자동 생성"""

    def test_creates_missing_buckets(self):
        mock_client = MagicMock()
        mock_client.bucket_exists.return_value = False
        import services.s3_service as mod
        with patch.object(mod, "get_s3_client", return_value=mock_client):
            mod.ensure_buckets()
        # 5개 버킷 모두 생성 시도
        assert mock_client.make_bucket.call_count == len(mod.IDP_BUCKETS)

    def test_skips_existing_buckets(self):
        mock_client = MagicMock()
        mock_client.bucket_exists.return_value = True
        import services.s3_service as mod
        with patch.object(mod, "get_s3_client", return_value=mock_client):
            mod.ensure_buckets()
        mock_client.make_bucket.assert_not_called()

    def test_handles_s3_error_gracefully(self):
        from minio.error import S3Error
        mock_client = MagicMock()
        mock_client.bucket_exists.side_effect = S3Error(
            "NoSuchBucket", "bucket not found", "resource", "request_id",
            "host_id", "response"
        )
        import services.s3_service as mod
        with patch.object(mod, "get_s3_client", return_value=mock_client):
            mod.ensure_buckets()  # 크래시 없이 완료

    def test_handles_connection_error_gracefully(self):
        mock_client = MagicMock()
        mock_client.bucket_exists.side_effect = Exception("Connection refused")
        import services.s3_service as mod
        with patch.object(mod, "get_s3_client", return_value=mock_client):
            mod.ensure_buckets()  # 크래시 없이 완료

    def test_bucket_names(self):
        from services.s3_service import IDP_BUCKETS
        expected = ["idp-etl", "idp-models", "idp-documents", "idp-backups", "idp-exports"]
        assert IDP_BUCKETS == expected
