"""
RAG Retriever — 지식 적재 + Milvus 검색 + 결과 포맷

OMOP CDM 도메인 지식(스키마, 의료 코드, SQL 패턴)을 Milvus에 적재하고,
사용자 질의에 관련된 컨텍스트를 검색합니다.
medical_knowledge 컬렉션과 함께 멀티 컬렉션 검색을 지원합니다.
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
MEDICAL_COLLECTION = "medical_knowledge"

# Milvus 접속 정보
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))

# Milvus 기동 대기 설정
_MAX_RETRIES = 10
_RETRY_DELAY = 5  # seconds

# QA 문서에 대한 가중치 부스트
_QA_SCORE_BOOST = 0.05

# 검색 결과 최소 콘텐츠 길이 (짧은 꼬리 청크 필터링)
_MIN_CONTENT_LENGTH = 80

# 최소 유사도 임계값 — 이 이하는 무관한 결과로 판단
_MIN_SCORE_THRESHOLD = 0.55

# ── LLM 기반 쿼리 리라이팅 ──────────────────────────────────
_LLM_API_URL = os.getenv("LLM_API_URL", "http://localhost:28888/v1")
_LLM_MODEL = os.getenv("LLM_MODEL", "default-model")
_LLM_REWRITE_TIMEOUT = 3  # 초 — GPU busy 시 빠르게 fallback
_rewrite_cache: Dict[str, List[str]] = {}  # query -> [rewritten_queries]

_REWRITE_SYSTEM_PROMPT = """당신은 의학 검색 쿼리 전문가입니다. 사용자의 검색어를 의학 정식 용어로 확장합니다.
규칙: 구어를 의학 정식 용어(한국어+영어)로 변환, 원본 포함, JSON 배열만 응답, 최대 3개
예시: 입력 "오십견" -> ["오십견", "동결견 frozen shoulder", "adhesive capsulitis 유착성 관절낭염"]
예시: 입력 "허리디스크" -> ["허리디스크", "요추 추간판 탈출증 lumbar disc herniation", "추간판탈출"]
생각 과정 없이 바로 JSON 배열만 출력하세요. /no_think"""


def _rewrite_query_via_llm(query: str) -> List[str]:
    """LLM으로 검색 쿼리를 의학 용어로 확장합니다.

    Returns:
        확장된 쿼리 리스트. 실패 시 [원본 쿼리].
    """
    # 캐시 확인
    if query in _rewrite_cache:
        return _rewrite_cache[query]

    import re
    try:
        import httpx
        resp = httpx.post(
            f"{_LLM_API_URL}/chat/completions",
            json={
                "model": _LLM_MODEL,
                "messages": [
                    {"role": "system", "content": _REWRITE_SYSTEM_PROMPT},
                    {"role": "user", "content": query},
                ],
                "max_tokens": 500,
                "temperature": 0.1,
            },
            timeout=httpx.Timeout(_LLM_REWRITE_TIMEOUT, connect=2.0),
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        # Qwen3 <think> 블록 제거
        content = re.sub(r'<think>[\s\S]*?</think>\s*', '', content).strip()
        if '<think>' in content:
            content = content.split('</think>')[-1].strip()
        # JSON 배열 파싱
        arr_match = re.search(r'\[.*\]', content, re.DOTALL)
        if arr_match:
            queries = json.loads(arr_match.group())
            if isinstance(queries, list) and len(queries) > 0:
                result = [str(q) for q in queries[:3]]
                _rewrite_cache[query] = result
                logger.info(f"LLM rewrite: '{query}' -> {result}")
                return result
    except Exception as e:
        logger.warning(f"LLM query rewrite failed: {e}")

    # LLM 실패 시 원본만 반환
    return [query]


def _mmr_rerank(
    hits: List[Dict[str, Any]],
    top_k: int,
    lambda_param: float = 0.7,
    max_per_source: int = 2,
) -> List[Dict[str, Any]]:
    """MMR(Maximal Marginal Relevance) 스타일 리랭킹.

    단어 겹침을 의미 유사도 프록시로 사용하여 검색 결과 다양성을 확보합니다.
    같은 출처(source)에서 max_per_source 개까지만 선택합니다.
    """
    if len(hits) <= 1:
        return hits

    import re as _re

    _stop = frozenset({
        "a", "an", "the", "is", "are", "was", "were", "in", "of", "to",
        "and", "or", "as", "by", "for", "on", "at", "from", "with",
        "that", "this", "it", "등", "및", "의", "에", "을", "를", "이", "가",
    })

    def _words(text: str) -> set:
        return {w for w in _re.findall(r'[a-z가-힣]+', text[:500].lower())
                if w not in _stop and len(w) > 1}

    def _sim(t1: str, t2: str) -> float:
        w1, w2 = _words(t1), _words(t2)
        if not w1 or not w2:
            return 0.0
        return len(w1 & w2) / min(len(w1), len(w2))

    src_counts: Dict[str, int] = {}
    selected: List[Dict[str, Any]] = []
    pool = list(hits)

    while pool and len(selected) < top_k:
        best_i, best_mmr = -1, -float("inf")
        for i, c in enumerate(pool):
            src = c["payload"].get("source", "") or c["payload"].get("type", "")
            if src and src_counts.get(src, 0) >= max_per_source:
                continue
            rel = c["score"]
            max_s = max(
                (_sim(c["payload"].get("content", ""), s["payload"].get("content", ""))
                 for s in selected),
                default=0.0,
            )
            mmr = lambda_param * rel - (1 - lambda_param) * max_s
            if mmr > best_mmr:
                best_mmr, best_i = mmr, i

        if best_i < 0:
            break

        chosen = pool.pop(best_i)
        src = chosen["payload"].get("source", "") or chosen["payload"].get("type", "")
        if src:
            src_counts[src] = src_counts.get(src, 0) + 1
        selected.append(chosen)

    return selected


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


def _search_collection(
    collection_name: str,
    vector: List[float],
    top_k: int,
    output_fields: List[str],
) -> List[Dict[str, Any]]:
    """단일 컬렉션에서 벡터 검색을 수행합니다.

    짧은 콘텐츠(< _MIN_CONTENT_LENGTH)를 필터링하기 위해
    3배 over-fetch 후 후처리합니다.
    """
    if not utility.has_collection(collection_name):
        return []

    try:
        col = Collection(collection_name)
        col.load()
        # Over-fetch to compensate for short content + duplicate filtering
        fetch_limit = min(top_k * 5, 300)
        results = col.search(
            data=[vector],
            anns_field="embedding",
            param={"metric_type": "COSINE", "params": {"nprobe": 16}},
            limit=fetch_limit,
            output_fields=output_fields,
        )

        hits = []
        seen_fingerprints: list = []  # 중복 콘텐츠 필터링용 (단어 집합)
        for hit in results[0]:
            entity = hit.entity
            content = entity.get("content", "")

            # 짧은 꼬리 청크 필터링 (영문 문서 조각 등)
            if len(content) < _MIN_CONTENT_LENGTH:
                continue

            # 중복 콘텐츠 필터링 — 앞 200자의 핵심 단어가 70% 이상 겹치면 중복
            import re as _re
            _stop = {"a", "an", "the", "is", "are", "was", "were", "in", "of", "to", "and", "or", "as", "by", "for", "on", "at", "from", "with", "that", "this", "it"}
            raw_words = _re.findall(r'[a-z가-힣]+', content[:200].lower())
            words = set(w for w in raw_words if w not in _stop and len(w) > 1)
            is_dup = False
            for seen_words in seen_fingerprints:
                if not words or not seen_words:
                    continue
                overlap = len(words & seen_words) / min(len(words), len(seen_words))
                if overlap > 0.5:
                    is_dup = True
                    break
            if is_dup:
                continue
            seen_fingerprints.append(words)

            meta = {}
            try:
                meta = json.loads(entity.get("metadata_json", "{}"))
            except (json.JSONDecodeError, TypeError):
                pass
            payload = {
                "content": content,
                "type": entity.get("doc_type", "unknown"),
                "source": entity.get("source", ""),
                "department": entity.get("department", ""),
                **meta,
            }
            score = hit.score
            # 한국어 콘텐츠 우선 — 한글이 포함되면 스코어 부스트
            import re as _re2
            if _re2.search(r'[가-힣]', content[:100]):
                score = min(1.0, score + 0.05)
            # QA 문서는 직접적인 답변이므로 가중치 부스트
            if payload["type"] in ("qa_case", "qa_short", "qa_essay"):
                score = min(1.0, score + _QA_SCORE_BOOST)
            hits.append({
                "score": score,
                "payload": payload,
                "collection": collection_name,
            })

            if len(hits) >= top_k:
                break
        return hits
    except Exception as e:
        logger.debug(f"Search on '{collection_name}' failed: {e}")
        return []


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

            # medical_knowledge 컬렉션 존재 여부 확인
            if utility.has_collection(MEDICAL_COLLECTION):
                med_col = Collection(MEDICAL_COLLECTION)
                med_col.flush()
                med_count = med_col.num_entities
                if med_count > 0:
                    med_col.load()
                    logger.info(f"Medical knowledge collection loaded: {med_count} entities")
                else:
                    logger.warning(
                        f"Collection '{MEDICAL_COLLECTION}' exists but empty. "
                        "Run: python -m ai_services.rag.medical_knowledge_etl"
                    )
            else:
                logger.warning(
                    f"Collection '{MEDICAL_COLLECTION}' not found. "
                    "Run: python -m ai_services.rag.medical_knowledge_etl"
                )
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

    def retrieve(
        self,
        query: str,
        top_k: int = 5,
        collections: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """질의와 유사한 지식 문서를 검색합니다.

        Args:
            query: 사용자 질의
            top_k: 반환할 최대 문서 수
            collections: 검색할 컬렉션 목록. None이면 양쪽 모두 검색.

        Returns:
            [{"score": float, "payload": dict, "collection": str}, ...]
        """
        if not self._initialized:
            logger.warning("RAG not initialized — returning empty")
            return []

        if collections is None:
            collections = [COLLECTION_NAME, MEDICAL_COLLECTION]

        try:
            # ── LLM 기반 쿼리 리라이팅: 구어 → 의학 정식 용어 확장 ──
            expanded_queries = _rewrite_query_via_llm(query)

            vectors = [self._embedder.embed_query(q) for q in expanded_queries]
            all_hits: List[Dict[str, Any]] = []

            for col_name in collections:
                if col_name == COLLECTION_NAME:
                    output_fields = ["content", "doc_type", "metadata_json"]
                else:
                    output_fields = ["content", "doc_type", "source", "department", "metadata_json"]

                for vec in vectors:
                    hits = _search_collection(col_name, vec, top_k, output_fields)
                    all_hits.extend(hits)

            # 최소 유사도 필터링
            all_hits = [h for h in all_hits if h["score"] >= _MIN_SCORE_THRESHOLD]

            # 중복 제거 (content 앞 100자 기준)
            seen = set()
            unique_hits = []
            for h in all_hits:
                key = h["payload"].get("content", "")[:100]
                if key not in seen:
                    seen.add(key)
                    unique_hits.append(h)

            # MMR 다양성 리랭킹 — 유사 문서 중복 방지 + 소스 다양성
            unique_hits.sort(key=lambda x: x["score"], reverse=True)
            return _mmr_rerank(unique_hits, top_k)

        except Exception as e:
            logger.error(f"RAG retrieval failed: {e}")
            return []

    def retrieve_medical(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """medical_knowledge 컬렉션만 검색합니다.

        Args:
            query: 사용자 질의
            top_k: 반환할 최대 문서 수

        Returns:
            [{"score": float, "payload": dict, "collection": str}, ...]
        """
        return self.retrieve(query, top_k=top_k, collections=[MEDICAL_COLLECTION])

    def retrieve_omop(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """omop_knowledge 컬렉션만 검색합니다.

        Args:
            query: 사용자 질의
            top_k: 반환할 최대 문서 수

        Returns:
            [{"score": float, "payload": dict, "collection": str}, ...]
        """
        return self.retrieve(query, top_k=top_k, collections=[COLLECTION_NAME])

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
            source = payload.get("source", "")
            source_tag = f" ({source})" if source else ""
            lines.append(f"- [{doc_type}]{source_tag} (score={score:.2f}) {content}")

        return "\n".join(lines)


# 모듈 레벨 싱글톤
_retriever: Optional[RAGRetriever] = None


def get_retriever() -> RAGRetriever:
    """RAGRetriever 싱글톤을 반환합니다."""
    global _retriever
    if _retriever is None:
        _retriever = RAGRetriever()
    return _retriever
