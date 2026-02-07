"""
Medical NER API 라우터
- GPU 서버의 BioClinicalBERT NER 서비스 프록시
- SSH 터널: localhost:28100 -> GPU:8100
"""
import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional

router = APIRouter(prefix="/ner", tags=["NER"])

# NER Service (SSH 터널: localhost:28100 -> GPU서버:8100)
NER_SERVICE_URL = "http://localhost:28100"


class AnalyzeRequest(BaseModel):
    text: str
    language: Optional[str] = None


class NEREntity(BaseModel):
    text: str
    type: str
    start: int
    end: int
    omopConcept: str
    standardCode: str
    codeSystem: str
    confidence: float
    source: str


class AnalyzeResponse(BaseModel):
    entities: List[NEREntity]
    language: str
    model: str
    processingTimeMs: int


@router.post("/analyze", response_model=AnalyzeResponse)
async def analyze(req: AnalyzeRequest):
    """GPU NER 서비스에 텍스트 분석 요청을 프록시"""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{NER_SERVICE_URL}/ner/analyze",
                json=req.model_dump(),
            )
            response.raise_for_status()
            return AnalyzeResponse(**response.json())
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NER 서비스 응답 시간 초과")
    except httpx.ConnectError:
        raise HTTPException(
            status_code=503,
            detail="NER 서비스에 연결할 수 없습니다. SSH 터널(localhost:28100)을 확인해주세요.",
        )
    except httpx.HTTPStatusError as e:
        detail = str(e)
        try:
            detail = e.response.json().get("detail", detail)
        except Exception:
            pass
        raise HTTPException(status_code=e.response.status_code, detail=detail)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NER 서비스 호출 실패: {str(e)}")


@router.get("/health")
async def health_check():
    """NER 서비스 연결 상태 확인"""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(f"{NER_SERVICE_URL}/ner/health")
            if response.status_code == 200:
                data = response.json()
                return {
                    "status": "healthy",
                    "backend": "BioClinicalBERT (d4data/biomedical-ner-all)",
                    "device": data.get("device", "unknown"),
                    "endpoint": NER_SERVICE_URL,
                }
    except Exception:
        pass
    return {
        "status": "unhealthy",
        "backend": "BioClinicalBERT (d4data/biomedical-ner-all)",
        "endpoint": NER_SERVICE_URL,
    }
