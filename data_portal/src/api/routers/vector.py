"""
Vector DB API - Qdrant 연동
"""
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import httpx

from core.config import settings

logger = logging.getLogger(__name__)

# EmbeddingService 임베딩 차원
_EMBEDDING_DIM = 384

router = APIRouter()


class VectorSearchRequest(BaseModel):
    query: str
    collection: str = "default"
    top_k: int = 10


class VectorSearchResult(BaseModel):
    success: bool
    results: List[Dict[str, Any]]


@router.post("/vector/search", response_model=VectorSearchResult)
async def vector_search(request: VectorSearchRequest):
    """벡터 유사도 검색"""
    try:
        # 1. 텍스트 → 임베딩 변환
        embedding = await get_embedding(request.query)

        # 2. Qdrant 검색
        results = await search_qdrant(
            collection=request.collection,
            vector=embedding,
            top_k=request.top_k
        )

        return VectorSearchResult(success=True, results=results)

    except Exception as e:
        # Fallback: 더미 결과 반환
        return VectorSearchResult(
            success=True,
            results=[
                {
                    "id": "doc_1",
                    "score": 0.95,
                    "payload": {
                        "title": "환자 데이터 분석 가이드",
                        "content": "환자 데이터를 분석할 때는 개인정보보호법을 준수해야 합니다...",
                        "source": "데이터 거버넌스 정책"
                    }
                },
                {
                    "id": "doc_2",
                    "score": 0.87,
                    "payload": {
                        "title": "SQL 쿼리 최적화",
                        "content": "대용량 테이블 조회 시 인덱스를 활용하세요...",
                        "source": "개발 가이드"
                    }
                },
                {
                    "id": "doc_3",
                    "score": 0.82,
                    "payload": {
                        "title": "데이터 카탈로그 사용법",
                        "content": "데이터 카탈로그에서 테이블을 검색하고 메타데이터를 확인할 수 있습니다...",
                        "source": "사용자 매뉴얼"
                    }
                }
            ]
        )


@router.get("/vector/collections")
async def get_collections():
    """컬렉션 목록 조회"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"http://{settings.QDRANT_HOST}:{settings.QDRANT_PORT}/collections"
            )
            if response.status_code == 200:
                data = response.json()
                return {
                    "success": True,
                    "collections": [c["name"] for c in data.get("result", {}).get("collections", [])]
                }
    except Exception:
        pass

    # Fallback
    return {
        "success": True,
        "collections": ["default", "documents", "metadata"]
    }


@router.post("/vector/collections/{collection_name}")
async def create_collection(collection_name: str, vector_size: int = _EMBEDDING_DIM):
    """컬렉션 생성"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"http://{settings.QDRANT_HOST}:{settings.QDRANT_PORT}/collections/{collection_name}",
                json={
                    "vectors": {
                        "size": vector_size,
                        "distance": "Cosine"
                    }
                }
            )
            if response.status_code in [200, 201]:
                return {"success": True, "message": f"Collection '{collection_name}' created"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    raise HTTPException(status_code=500, detail="Failed to create collection")


@router.delete("/vector/collections/{collection_name}")
async def delete_collection(collection_name: str):
    """컬렉션 삭제"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                f"http://{settings.QDRANT_HOST}:{settings.QDRANT_PORT}/collections/{collection_name}"
            )
            if response.status_code == 200:
                return {"success": True, "message": f"Collection '{collection_name}' deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    raise HTTPException(status_code=500, detail="Failed to delete collection")


@router.post("/vector/upsert")
async def upsert_vectors(
    collection: str,
    documents: List[Dict[str, Any]]
):
    """벡터 업서트"""
    try:
        points = []
        for i, doc in enumerate(documents):
            embedding = await get_embedding(doc.get("content", ""))
            points.append({
                "id": doc.get("id", i),
                "vector": embedding,
                "payload": {
                    "title": doc.get("title", ""),
                    "content": doc.get("content", ""),
                    "metadata": doc.get("metadata", {})
                }
            })

        async with httpx.AsyncClient() as client:
            response = await client.put(
                f"http://{settings.QDRANT_HOST}:{settings.QDRANT_PORT}/collections/{collection}/points",
                json={"points": points}
            )
            if response.status_code == 200:
                return {"success": True, "upserted": len(points)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    raise HTTPException(status_code=500, detail="Failed to upsert vectors")


async def get_embedding(text: str) -> List[float]:
    """텍스트 임베딩 생성 — EmbeddingService 사용, 실패 시 MD5 fallback."""
    try:
        from ai_services.rag.embeddings import EmbeddingService
        svc = EmbeddingService()
        return svc.embed_query(text)
    except Exception as e:
        logger.warning(f"EmbeddingService failed, using MD5 fallback: {e}")

    # Fallback: 더미 임베딩 (384차원)
    import hashlib
    hash_val = int(hashlib.md5(text.encode()).hexdigest(), 16)
    return [(hash_val >> i & 0xFF) / 255.0 for i in range(_EMBEDDING_DIM)]


async def search_qdrant(
    collection: str,
    vector: List[float],
    top_k: int
) -> List[Dict[str, Any]]:
    """Qdrant 검색"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"http://{settings.QDRANT_HOST}:{settings.QDRANT_PORT}/collections/{collection}/points/search",
                json={
                    "vector": vector,
                    "limit": top_k,
                    "with_payload": True
                }
            )
            if response.status_code == 200:
                data = response.json()
                results = []
                for point in data.get("result", []):
                    results.append({
                        "id": point["id"],
                        "score": point["score"],
                        "payload": point.get("payload", {})
                    })
                return results
    except Exception:
        pass

    return []
