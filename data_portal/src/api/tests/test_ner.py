"""
Phase 4: NER 라우터 API 테스트
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx


NER_MOCK_RESPONSE = {
    "entities": [
        {
            "text": "diabetes",
            "type": "Disease",
            "start": 0,
            "end": 8,
            "omopConcept": "Diabetes mellitus",
            "standardCode": "44054006",
            "codeSystem": "SNOMED",
            "confidence": 0.95,
            "source": "BioClinicalBERT",
        }
    ],
    "language": "en",
    "model": "d4data/biomedical-ner-all",
    "processingTimeMs": 150,
}


class TestNERAnalyze:
    """POST /api/v1/ner/analyze"""

    async def test_analyze_success(self, client, csrf_headers):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = NER_MOCK_RESPONSE
        mock_resp.raise_for_status = MagicMock()

        mock_http = AsyncMock()
        mock_http.post = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.post(
                "/api/v1/ner/analyze",
                json={"text": "diabetes mellitus type 2"},
                headers=csrf_headers,
            )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["entities"]) == 1
        assert data["entities"][0]["type"] == "Disease"

    async def test_analyze_timeout(self, client, csrf_headers):
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.post(
                "/api/v1/ner/analyze",
                json={"text": "test"},
                headers=csrf_headers,
            )
        assert resp.status_code == 504

    async def test_analyze_connection_error(self, client, csrf_headers):
        mock_http = AsyncMock()
        mock_http.post = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.post(
                "/api/v1/ner/analyze",
                json={"text": "test"},
                headers=csrf_headers,
            )
        assert resp.status_code == 503


class TestNERHealth:
    """GET /api/v1/ner/health"""

    async def test_health_healthy(self, client):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"status": "ok", "device": "cuda:0"}

        mock_http = AsyncMock()
        mock_http.get = AsyncMock(return_value=mock_resp)
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.get("/api/v1/ner/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"

    async def test_health_unhealthy(self, client):
        mock_http = AsyncMock()
        mock_http.get = AsyncMock(side_effect=Exception("Connection failed"))
        mock_http.__aenter__ = AsyncMock(return_value=mock_http)
        mock_http.__aexit__ = AsyncMock(return_value=False)

        with patch("httpx.AsyncClient", return_value=mock_http):
            resp = await client.get("/api/v1/ner/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "unhealthy"
