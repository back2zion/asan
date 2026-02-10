"""
의료 지식 관리 API — medical_knowledge Milvus 컬렉션 관리 + 검색

PRD AAR-001: 전문 의학지식 데이터 RAG 통합
- /medical-knowledge/stats — 컬렉션 통계
- /medical-knowledge/health — 헬스체크
- /medical-knowledge/search — 벡터 유사도 검색
- /medical-knowledge/etl/start — ETL 시작
- /medical-knowledge/etl/status — ETL 진행률
- /medical-knowledge/reset — 컬렉션 초기화
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/medical-knowledge")


# ─── Request / Response 모델 ───


class SearchRequest(BaseModel):
    query: str = Field(..., min_length=1, max_length=500, description="검색 질의")
    top_k: int = Field(5, ge=1, le=50, description="반환할 문서 수")
    doc_type: Optional[str] = Field(None, description="필터링할 문서 유형")


class SearchResult(BaseModel):
    score: float
    content: str
    doc_type: str
    source: str = ""
    department: str = ""
    metadata: Dict[str, Any] = {}


class SearchResponse(BaseModel):
    results: List[SearchResult]
    count: int
    query: str


class StatsResponse(BaseModel):
    exists: bool
    collection_name: str = ""
    count: int = 0


class ETLStartRequest(BaseModel):
    reset: bool = Field(False, description="기존 컬렉션 삭제 후 재적재")
    batch_size: int = Field(64, ge=1, le=256, description="임베딩 배치 크기")


class ETLStatusResponse(BaseModel):
    running: bool
    total_zips: int = 0
    processed_zips: int = 0
    total_docs: int = 0
    total_chunks: int = 0
    current_zip: str = ""
    errors: List[str] = []
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


# ─── 엔드포인트 ───


@router.get("/stats", response_model=StatsResponse)
async def get_stats():
    """컬렉션 통계 (문서 수)."""
    try:
        from ai_services.rag.medical_knowledge_etl import get_collection_stats
        stats = await asyncio.to_thread(get_collection_stats)
        return StatsResponse(**stats)
    except Exception as e:
        logger.error(f"Stats failed: {e}")
        return StatsResponse(exists=False, count=0)


@router.get("/health")
async def health_check():
    """컬렉션 상태 확인."""
    try:
        from ai_services.rag.medical_knowledge_etl import get_collection_stats
        stats = await asyncio.to_thread(get_collection_stats)
        if stats.get("exists") and stats.get("count", 0) > 0:
            return {"status": "healthy", "count": stats["count"]}
        elif stats.get("exists"):
            return {"status": "empty", "count": 0}
        else:
            return {"status": "not_initialized", "count": 0}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/search", response_model=SearchResponse)
async def search_medical_knowledge(req: SearchRequest):
    """의학 지식 벡터 유사도 검색."""
    try:
        from ai_services.rag.retriever import get_retriever
        retriever = get_retriever()
        if not retriever._initialized:
            await asyncio.to_thread(retriever.initialize)
        hits = await asyncio.to_thread(retriever.retrieve_medical, req.query, req.top_k)

        results = []
        for hit in hits:
            payload = hit.get("payload", {})
            doc_type = payload.get("type", "unknown")

            # doc_type 필터링
            if req.doc_type and doc_type != req.doc_type:
                continue

            results.append(SearchResult(
                score=hit.get("score", 0),
                content=payload.get("content", ""),
                doc_type=doc_type,
                source=payload.get("source", ""),
                department=payload.get("department", ""),
                metadata={k: v for k, v in payload.items()
                          if k not in ("content", "type", "source", "department")},
            ))

        return SearchResponse(results=results, count=len(results), query=req.query)
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=f"검색 실패: {e}")


def _run_etl_background(reset: bool, batch_size: int):
    """백그라운드 ETL 실행 (동기 함수)."""
    from ai_services.rag.medical_knowledge_etl import run_etl
    run_etl(reset=reset, batch_size=batch_size)


@router.post("/etl/start")
async def start_etl(req: ETLStartRequest, background_tasks: BackgroundTasks):
    """ETL 시작 (백그라운드 태스크)."""
    from ai_services.rag.medical_knowledge_etl import get_etl_status

    status = get_etl_status()
    if status.get("running"):
        raise HTTPException(status_code=409, detail="ETL이 이미 실행 중입니다")

    background_tasks.add_task(_run_etl_background, req.reset, req.batch_size)
    return {"message": "ETL 시작됨", "reset": req.reset, "batch_size": req.batch_size}


@router.get("/etl/status", response_model=ETLStatusResponse)
async def get_etl_progress():
    """ETL 진행률 조회."""
    from ai_services.rag.medical_knowledge_etl import get_etl_status
    return ETLStatusResponse(**get_etl_status())


@router.delete("/reset")
async def reset_collection():
    """컬렉션 초기화 (재적재용)."""
    try:
        from ai_services.rag.medical_knowledge_etl import reset_collection as do_reset
        await asyncio.to_thread(do_reset)
        return {"message": "컬렉션 초기화 완료"}
    except Exception as e:
        logger.error(f"Reset failed: {e}")
        raise HTTPException(status_code=500, detail=f"초기화 실패: {e}")
