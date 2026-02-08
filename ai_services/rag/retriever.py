"""
RAG Retriever — 지식 적재 + Milvus 검색 + 결과 포맷

OMOP CDM 도메인 지식(스키마, 의료 코드, SQL 패턴)을 Milvus에 적재하고,
사용자 질의에 관련된 컨텍스트를 검색합니다.
"""

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    MilvusException,
    connections,
    utility,
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

# Milvus 접속 정보
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))

# Milvus 기동 대기 설정
_MAX_RETRIES = 10
_RETRY_DELAY = 5  # seconds


def _build_knowledge_documents() -> List[Dict[str, Any]]:
    """스키마·의료 지식으로부터 Milvus에 적재할 문서 리스트를 생성합니다."""
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


def _connect_milvus() -> None:
    """Milvus 연결 (재시도 포함)."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            connections.connect(
                alias="default",
                host=MILVUS_HOST,
                port=MILVUS_PORT,
                timeout=30,
            )
            logger.info(f"Connected to Milvus at {MILVUS_HOST}:{MILVUS_PORT}")
            return
        except Exception as e:
            logger.warning(
                f"Milvus connection attempt {attempt}/{_MAX_RETRIES} failed: {e}"
            )
            if attempt < _MAX_RETRIES:
                time.sleep(_RETRY_DELAY)

    raise ConnectionError(
        f"Cannot connect to Milvus at {MILVUS_HOST}:{MILVUS_PORT} "
        f"after {_MAX_RETRIES} retries"
    )


def _get_or_create_collection() -> Collection:
    """컬렉션 존재 확인 후 반환, 없으면 생성."""
    if utility.has_collection(COLLECTION_NAME):
        return Collection(COLLECTION_NAME)

    fields = [
        FieldSchema("id", DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema("embedding", DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
        FieldSchema("content", DataType.VARCHAR, max_length=4096),
        FieldSchema("doc_type", DataType.VARCHAR, max_length=64),
        FieldSchema("metadata_json", DataType.VARCHAR, max_length=2048),
    ]
    schema = CollectionSchema(fields, description="OMOP CDM Knowledge for RAG")
    collection = Collection(COLLECTION_NAME, schema)

    # IVF_FLAT 인덱스 생성
    index_params = {
        "metric_type": "COSINE",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128},
    }
    collection.create_index("embedding", index_params)
    logger.info(f"Created collection '{COLLECTION_NAME}' (dim={EMBEDDING_DIM})")
    return collection


class RAGRetriever:
    """Milvus 기반 RAG 검색기."""

    def __init__(self):
        self._collection: Optional[Collection] = None
        self._embedder = EmbeddingService()
        self._initialized = False

    def _ensure_connection(self) -> Collection:
        if self._collection is None:
            _connect_milvus()
            self._collection = _get_or_create_collection()
        return self._collection

    # ------------------------------------------------------------------
    # 초기화: 컬렉션 생성 + 지식 적재
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """컬렉션 생성 + 지식 문서 적재. 이미 데이터가 있으면 스킵합니다."""
        collection = self._ensure_connection()

        # 이미 데이터가 있으면 스킵
        collection.flush()
        if collection.num_entities > 0:
            logger.info(
                f"Collection '{COLLECTION_NAME}' already has "
                f"{collection.num_entities} entities — skipping load"
            )
            collection.load()
            self._initialized = True
            return

        # 문서 생성 + 임베딩
        docs = _build_knowledge_documents()
        texts = [d["text"] for d in docs]
        logger.info(f"Embedding {len(texts)} knowledge documents...")
        vectors = self._embedder.embed_batch(texts)

        # Milvus에 적재 (배치)
        contents = []
        doc_types = []
        metadata_jsons = []
        for doc in docs:
            contents.append(doc["text"][:4096])
            doc_types.append(doc.get("type", "unknown")[:64])
            meta = {k: v for k, v in doc.items() if k not in ("text", "type")}
            metadata_jsons.append(json.dumps(meta, ensure_ascii=False)[:2048])

        batch_size = 100
        for start in range(0, len(vectors), batch_size):
            end = min(start + batch_size, len(vectors))
            collection.insert([
                vectors[start:end],
                contents[start:end],
                doc_types[start:end],
                metadata_jsons[start:end],
            ])

        collection.flush()
        collection.load()
        logger.info(f"Loaded {len(docs)} documents into '{COLLECTION_NAME}'")
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
            collection = self._ensure_connection()
            results = collection.search(
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
                hits.append({"score": hit.score, "payload": payload})
            return hits
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
