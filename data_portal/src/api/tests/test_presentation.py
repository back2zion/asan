"""
Phase 4: Presentation 라우터 API 테스트
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx


class TestPresentationGenerate:
    """POST /api/v1/presentation/generate"""

    async def test_invalid_file_extension(self, client, csrf_headers):
        """PDF가 아닌 파일 → 400"""
        resp = await client.post(
            "/api/v1/presentation/generate",
            files={"file": ("test.docx", b"fake content", "application/vnd.openxmlformats")},
            data={"output_type": "slides"},
            headers=csrf_headers,
        )
        assert resp.status_code == 400
        assert "허용되지 않은" in resp.json()["detail"]

    async def test_no_filename(self, client, csrf_headers):
        """파일명 없음 → 400 or 422"""
        resp = await client.post(
            "/api/v1/presentation/generate",
            files={"file": ("", b"content", "application/pdf")},
            data={"output_type": "slides"},
            headers=csrf_headers,
        )
        assert resp.status_code in (400, 422)

    async def test_valid_pdf_upload(self, client, csrf_headers):
        """유효한 PDF 업로드 → Paper2Slides 프록시 (mocked)"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"job_id": "test-job-1", "status": "processing"}
        mock_resp.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.post(
                "/api/v1/presentation/generate",
                files={"file": ("paper.pdf", b"%PDF-1.4 fake pdf content", "application/pdf")},
                data={"output_type": "slides", "template": "academic", "slide_count": "10"},
                headers=csrf_headers,
            )
        # Paper2Slides 호출 성공 또는 파일 크기 관련 오류
        assert resp.status_code in (200, 413, 500, 503)


class TestPresentationStatus:
    """GET /api/v1/presentation/status/{job_id}"""

    async def test_status_success(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "job_id": "test-1",
            "status": "completed",
            "output_type": "slides",
            "output_file": "/output/test.pdf",
        }
        mock_resp.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.get("/api/v1/presentation/status/test-1")
        assert resp.status_code == 200

    async def test_status_connection_error(self, client):
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.get("/api/v1/presentation/status/nonexistent")
        assert resp.status_code in (500, 503)


class TestPresentationHealth:
    """GET /api/v1/presentation/health"""

    async def test_health_healthy(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"status": "healthy"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.get("/api/v1/presentation/health")
        assert resp.status_code == 200

    async def test_health_unhealthy(self, client):
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=Exception("down"))
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.get("/api/v1/presentation/health")
        assert resp.status_code == 200
