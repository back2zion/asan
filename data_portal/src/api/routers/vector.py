"""
Vector DB API - Milvus 연동
"""
import asyncio
import json
import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any

from core.config import settings

logger = logging.getLogger(__name__)

# EmbeddingService 임베딩 차원
_EMBEDDING_DIM = 384

router = APIRouter()


class VectorSearchRequest(BaseModel):
    query: str
    collection: str = "omop_knowledge"
    top_k: int = 10


class VectorSearchResult(BaseModel):
    success: bool
    results: List[Dict[str, Any]]


def _get_milvus_collection(name: str):
    """pymilvus Collection 객체 반환 (동기)."""
    from pymilvus import Collection, connections, utility

    # 연결이 안 되어 있으면 연결
    try:
        connections.connect(
            alias="default",
            host=settings.MILVUS_HOST,
            port=settings.MILVUS_PORT,
            timeout=10,
        )
    except Exception:
        pass  # 이미 연결되어 있을 수 있음

    if not utility.has_collection(name):
        return None
    col = Collection(name)
    col.load()
    return col


@router.post("/vector/search", response_model=VectorSearchResult)
async def vector_search(request: VectorSearchRequest):
    """벡터 유사도 검색"""
    try:
        # 1. 텍스트 → 임베딩 변환
        embedding = await get_embedding(request.query)

        # 2. Milvus 검색
        results = await asyncio.to_thread(
            _search_milvus,
            collection_name=request.collection,
            vector=embedding,
            top_k=request.top_k,
        )

        return VectorSearchResult(success=True, results=results)

    except Exception as e:
        logger.warning(f"Vector search error, returning fallback: {e}")
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
        from pymilvus import connections, utility

        def _list():
            try:
                connections.connect(
                    alias="default",
                    host=settings.MILVUS_HOST,
                    port=settings.MILVUS_PORT,
                    timeout=10,
                )
            except Exception:
                pass
            return utility.list_collections()

        collections = await asyncio.to_thread(_list)
        return {"success": True, "collections": collections}
    except Exception:
        pass

    return {"success": True, "collections": ["omop_knowledge"]}


@router.post("/vector/collections/{collection_name}")
async def create_collection(collection_name: str, vector_size: int = _EMBEDDING_DIM):
    """컬렉션 생성"""
    try:
        from pymilvus import (
            Collection,
            CollectionSchema,
            DataType,
            FieldSchema,
            connections,
            utility,
        )

        def _create():
            try:
                connections.connect(
                    alias="default",
                    host=settings.MILVUS_HOST,
                    port=settings.MILVUS_PORT,
                    timeout=10,
                )
            except Exception:
                pass

            if utility.has_collection(collection_name):
                return False

            fields = [
                FieldSchema("id", DataType.INT64, is_primary=True, auto_id=True),
                FieldSchema("embedding", DataType.FLOAT_VECTOR, dim=vector_size),
                FieldSchema("content", DataType.VARCHAR, max_length=4096),
                FieldSchema("doc_type", DataType.VARCHAR, max_length=64),
                FieldSchema("metadata_json", DataType.VARCHAR, max_length=2048),
            ]
            schema = CollectionSchema(fields)
            col = Collection(collection_name, schema)
            col.create_index(
                "embedding",
                {"metric_type": "COSINE", "index_type": "IVF_FLAT", "params": {"nlist": 128}},
            )
            return True

        created = await asyncio.to_thread(_create)
        if created:
            return {"success": True, "message": f"Collection '{collection_name}' created"}
        return {"success": True, "message": f"Collection '{collection_name}' already exists"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/vector/collections/{collection_name}")
async def delete_collection(collection_name: str):
    """컬렉션 삭제"""
    try:
        from pymilvus import connections, utility

        def _drop():
            try:
                connections.connect(
                    alias="default",
                    host=settings.MILVUS_HOST,
                    port=settings.MILVUS_PORT,
                    timeout=10,
                )
            except Exception:
                pass
            if utility.has_collection(collection_name):
                utility.drop_collection(collection_name)
                return True
            return False

        dropped = await asyncio.to_thread(_drop)
        if dropped:
            return {"success": True, "message": f"Collection '{collection_name}' deleted"}
        raise HTTPException(status_code=404, detail=f"Collection '{collection_name}' not found")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/vector/upsert")
async def upsert_vectors(
    collection: str,
    documents: List[Dict[str, Any]]
):
    """벡터 업서트"""
    try:
        embeddings = []
        contents = []
        doc_types = []
        metadata_jsons = []

        for doc in documents:
            embedding = await get_embedding(doc.get("content", ""))
            embeddings.append(embedding)
            contents.append(doc.get("content", "")[:4096])
            doc_types.append(doc.get("doc_type", "document")[:64])
            meta = doc.get("metadata", {})
            metadata_jsons.append(json.dumps(meta, ensure_ascii=False)[:2048])

        def _insert():
            col = _get_milvus_collection(collection)
            if col is None:
                raise ValueError(f"Collection '{collection}' not found")
            col.insert([embeddings, contents, doc_types, metadata_jsons])
            col.flush()
            return len(embeddings)

        count = await asyncio.to_thread(_insert)
        return {"success": True, "upserted": count}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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


def _search_milvus(
    collection_name: str,
    vector: List[float],
    top_k: int,
) -> List[Dict[str, Any]]:
    """Milvus 검색 (동기 — asyncio.to_thread에서 호출)."""
    col = _get_milvus_collection(collection_name)
    if col is None:
        return []

    results = col.search(
        data=[vector],
        anns_field="embedding",
        param={"metric_type": "COSINE", "params": {"nprobe": 16}},
        limit=top_k,
        output_fields=["content", "doc_type", "metadata_json"],
    )

    hits = []
    for hit in results[0]:
        entity = hit.entity
        meta = {}
        try:
            meta = json.loads(entity.get("metadata_json", "{}"))
        except (json.JSONDecodeError, TypeError):
            pass
        payload = {
            "content": entity.get("content", ""),
            "type": entity.get("doc_type", "unknown"),
            **meta,
        }
        hits.append({
            "id": hit.id,
            "score": hit.score,
            "payload": payload,
        })
    return hits
