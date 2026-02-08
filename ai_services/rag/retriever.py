"""
RAG Retriever — 지식 적재 + Qdrant 검색 + 결과 포맷

OMOP CDM 도메인 지식(스키마, 의료 코드, SQL 패턴)을 Qdrant에 적재하고,
사용자 질의에 관련된 컨텍스트를 검색합니다.
"""

import logging
import os
import uuid
from typing import Any, Dict, List, Optional

from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    Distance,
    PointStruct,
    VectorParams,
)

from ai_services.rag.embeddings import EMBEDDING_DIM, EmbeddingService
from ai_services.xiyan_sql.schema import (
    ICD_CODE_MAP,
    KEYWORD_TABLE_MAP,
    MEDICAL_EVIDENCE,
    SAMPLE_TABLES,
    TABLE_RELATIONSHIPS,
)

logger = logging.getLogger(__name__)

COLLECTION_NAME = "omop_knowledge"

# Qdrant 접속 정보
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))


def _build_knowledge_documents() -> List[Dict[str, Any]]:
    """스키마·의료 지식으로부터 Qdrant에 적재할 문서 리스트를 생성합니다."""
    docs: List[Dict[str, Any]] = []

    # 1) 테이블별 스키마 문서
    for table in SAMPLE_TABLES:
        cols_desc = ", ".join(
            f"{c['physical_name']}({c['business_name']})" for c in table["columns"]
        )
        text = (
            f"테이블 {table['physical_name']} ({table['business_name']}): "
            f"{table['description']} "
            f"컬럼: {cols_desc}"
        )
        docs.append({
            "text": text,
            "type": "schema",
            "table": table["physical_name"],
        })

    # 2) 의료 참조 지식
    for keyword, evidence in MEDICAL_EVIDENCE.items():
        docs.append({
            "text": f"{keyword}: {evidence}",
            "type": "medical_evidence",
            "keyword": keyword,
        })

    # 3) ICD(SNOMED) 코드 매핑
    for keyword, (code, name) in ICD_CODE_MAP.items():
        text = (
            f"질환 코드 매핑 — '{keyword}' → SNOMED CT {code} ({name}). "
            f"condition_occurrence.condition_source_value = '{code}'"
        )
        docs.append({
            "text": text,
            "type": "code_mapping",
            "keyword": keyword,
            "code": code,
        })

    # 4) 키워드→테이블 매핑 (대표 키워드만)
    seen_tables = set()
    for keyword, tables in KEYWORD_TABLE_MAP.items():
        key = tuple(tables)
        if key in seen_tables:
            continue
        seen_tables.add(key)
        text = (
            f"키워드 '{keyword}' 질의 시 사용 테이블: {', '.join(tables)}"
        )
        docs.append({
            "text": text,
            "type": "keyword_mapping",
            "keyword": keyword,
            "tables": tables,
        })

    # 5) FK 관계 문서
    for rel in TABLE_RELATIONSHIPS:
        text = (
            f"외래키: {rel['from_table']}.{rel['from_column']} → "
            f"{rel['to_table']}.{rel['to_column']} ({rel['relationship']})"
        )
        docs.append({
            "text": text,
            "type": "relationship",
        })

    # 6) 자주 쓰는 SQL 패턴
    sql_patterns = [
        {
            "text": "당뇨 환자 수 조회: SELECT COUNT(DISTINCT co.person_id) FROM condition_occurrence co WHERE co.condition_source_value = '44054006'",
            "type": "sql_pattern",
            "pattern": "disease_count",
        },
        {
            "text": "성별 환자 수: SELECT p.gender_source_value, COUNT(*) FROM person p GROUP BY p.gender_source_value",
            "type": "sql_pattern",
            "pattern": "gender_count",
        },
        {
            "text": "입원 환자 목록: SELECT p.person_id, p.gender_source_value, p.year_of_birth FROM person p JOIN visit_occurrence vo ON p.person_id = vo.person_id WHERE vo.visit_concept_id = 9201",
            "type": "sql_pattern",
            "pattern": "inpatient_list",
        },
        {
            "text": "특정 질환의 성별 분포: SELECT p.gender_source_value, COUNT(DISTINCT co.person_id) FROM condition_occurrence co JOIN person p ON co.person_id = p.person_id WHERE co.condition_source_value = '44054006' GROUP BY p.gender_source_value",
            "type": "sql_pattern",
            "pattern": "disease_gender_dist",
        },
        {
            "text": "연도별 방문 추이: SELECT EXTRACT(YEAR FROM vo.visit_start_date) AS year, COUNT(*) FROM visit_occurrence vo GROUP BY year ORDER BY year",
            "type": "sql_pattern",
            "pattern": "visit_trend",
        },
        {
            "text": "약물 처방 환자 수: SELECT drug_source_value, COUNT(DISTINCT person_id) FROM drug_exposure GROUP BY drug_source_value ORDER BY COUNT(DISTINCT person_id) DESC LIMIT 10",
            "type": "sql_pattern",
            "pattern": "drug_top10",
        },
    ]
    docs.extend(sql_patterns)

    return docs


class RAGRetriever:
    """Qdrant 기반 RAG 검색기."""

    def __init__(self):
        self._client: Optional[QdrantClient] = None
        self._embedder = EmbeddingService()
        self._initialized = False

    def _get_client(self) -> QdrantClient:
        if self._client is None:
            self._client = QdrantClient(
                host=QDRANT_HOST, port=QDRANT_PORT,
                timeout=60, check_compatibility=False,
            )
        return self._client

    # ------------------------------------------------------------------
    # 초기화: 컬렉션 생성 + 지식 적재
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """컬렉션 생성 + 지식 문서 적재. 이미 데이터가 있으면 스킵합니다."""
        client = self._get_client()

        # 컬렉션 존재 확인
        collections = [c.name for c in client.get_collections().collections]
        if COLLECTION_NAME in collections:
            info = client.get_collection(COLLECTION_NAME)
            if info.points_count and info.points_count > 0:
                logger.info(
                    f"Collection '{COLLECTION_NAME}' already has "
                    f"{info.points_count} points — skipping load"
                )
                self._initialized = True
                return

        # 컬렉션 생성 (없으면)
        if COLLECTION_NAME not in collections:
            client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=EMBEDDING_DIM,
                    distance=Distance.COSINE,
                ),
            )
            logger.info(f"Created collection '{COLLECTION_NAME}' (dim={EMBEDDING_DIM})")

        # 문서 생성 + 임베딩
        docs = _build_knowledge_documents()
        texts = [d["text"] for d in docs]
        logger.info(f"Embedding {len(texts)} knowledge documents...")
        vectors = self._embedder.embed_batch(texts)

        # Qdrant에 적재
        points = []
        for i, (doc, vec) in enumerate(zip(docs, vectors)):
            payload = {k: v for k, v in doc.items() if k != "text"}
            payload["content"] = doc["text"]
            points.append(
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=vec,
                    payload=payload,
                )
            )

        # 배치 업서트 (100개씩)
        batch_size = 100
        for start in range(0, len(points), batch_size):
            batch = points[start : start + batch_size]
            client.upsert(collection_name=COLLECTION_NAME, points=batch)

        logger.info(f"Loaded {len(points)} documents into '{COLLECTION_NAME}'")
        self._initialized = True

    # ------------------------------------------------------------------
    # 검색
    # ------------------------------------------------------------------

    def retrieve(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """질의와 유사한 지식 문서를 검색합니다.

        Args:
            query: 사용자 질의
            top_k: 반환할 최대 문서 수

        Returns:
            [{"score": float, "payload": dict}, ...]
        """
        if not self._initialized:
            logger.warning("RAG not initialized — returning empty")
            return []

        try:
            vector = self._embedder.embed_query(query)
            results = self._get_client().query_points(
                collection_name=COLLECTION_NAME,
                query=vector,
                limit=top_k,
            )
            return [
                {"score": hit.score, "payload": hit.payload}
                for hit in results.points
            ]
        except Exception as e:
            logger.error(f"RAG retrieval failed: {e}")
            return []

    # ------------------------------------------------------------------
    # 포맷
    # ------------------------------------------------------------------

    @staticmethod
    def format_as_context(results: List[Dict[str, Any]]) -> str:
        """검색 결과를 LLM 프롬프트용 문자열로 포맷합니다."""
        if not results:
            return ""

        lines = []
        for r in results:
            payload = r.get("payload", {})
            score = r.get("score", 0)
            content = payload.get("content", "")
            doc_type = payload.get("type", "unknown")
            lines.append(f"- [{doc_type}] (score={score:.2f}) {content}")

        return "\n".join(lines)


# 모듈 레벨 싱글톤
_retriever: Optional[RAGRetriever] = None


def get_retriever() -> RAGRetriever:
    """RAGRetriever 싱글톤을 반환합니다."""
    global _retriever
    if _retriever is None:
        _retriever = RAGRetriever()
    return _retriever
