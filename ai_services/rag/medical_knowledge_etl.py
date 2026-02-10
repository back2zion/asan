"""
전문 의학지식 데이터 ETL — ZIP→JSON→청킹→임베딩→Milvus 적재

AI Hub 전문 의학지식 126,013개 JSON 파일을 Milvus medical_knowledge 컬렉션에 적재합니다.
- 원천데이터(TS_*): content 필드를 1,000자 청크 단위로 분할 (200자 overlap)
- 라벨링데이터(TL_*, VL_*): question+answer를 하나의 문서로 저장

Usage:
    python -m ai_services.rag.medical_knowledge_etl [--reset] [--batch-size 64]
"""

import json
import logging
import os
import sys
import time
import zipfile
from pathlib import Path
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

logger = logging.getLogger(__name__)

COLLECTION_NAME = "medical_knowledge"

# Milvus 접속 정보
MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))

# 데이터 경로 (복수 지원)
_DATA_ROOT = "/home/babelai/datastreams-work/datastreams/asan/data"
DATA_BASE_PATHS = [
    Path(os.getenv("MEDICAL_DATA_PATH", f"{_DATA_ROOT}/08.전문 의학지식 데이터/3.개방데이터/1.데이터")),
    Path(f"{_DATA_ROOT}/09.필수의료 의학지식 데이터/3.개방데이터/1.데이터"),
]

# 청킹 설정
CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
MIN_CHUNK_LENGTH = 80  # 이보다 짧은 꼬리 청크 무시 (영문 문서 조각 방지)

# 원천데이터 source 코드 → doc_type 매핑
SOURCE_TYPE_MAP = {
    "의학 교과서": "textbook",
    "학회 가이드라인": "guideline",
    "국제기관 가이드라인": "guideline",
    "학술 논문 및 저널": "journal",
    "온라인 의료 정보 제공 사이트": "online",
    "기타": "online",
}

# 라벨링데이터 q_type → doc_type 매핑
Q_TYPE_MAP = {
    1: "qa_case",    # 증례형
    2: "qa_short",   # 단답형
    3: "qa_essay",   # 서술형
}

# 진행 상황 추적 파일
PROGRESS_FILE = Path(__file__).parent / ".medical_etl_progress.json"

# 전역 ETL 상태 (API에서 조회용)
etl_status: Dict[str, Any] = {
    "running": False,
    "total_zips": 0,
    "processed_zips": 0,
    "total_docs": 0,
    "total_chunks": 0,
    "current_zip": "",
    "errors": [],
    "started_at": None,
    "finished_at": None,
}


def _chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """텍스트를 청크 단위로 분할합니다."""
    if len(text) <= chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        if chunk.strip() and len(chunk.strip()) >= MIN_CHUNK_LENGTH:
            chunks.append(chunk)
        start += chunk_size - overlap

    return chunks


def _extract_source_type(zip_name: str) -> str:
    """ZIP 파일명에서 source type을 추출합니다."""
    for key, doc_type in SOURCE_TYPE_MAP.items():
        if key in zip_name:
            return doc_type
    return "online"


def _extract_department(zip_name: str) -> str:
    """ZIP 파일명에서 진료과명을 추출합니다."""
    # TL_/VL_ 파일에서 진료과 추출
    name = Path(zip_name).stem
    for prefix in ("TL_", "VL_"):
        if name.startswith(prefix):
            return name[len(prefix):]
    return ""


def _parse_source_data(data: dict, zip_name: str) -> List[Dict[str, Any]]:
    """원천데이터(TS_*) JSON을 청크 단위 문서 리스트로 변환합니다."""
    content = data.get("content", "")
    if not content or not content.strip():
        return []

    c_id = data.get("c_id", "")
    source_spec = data.get("source_spec", "")
    doc_type = _extract_source_type(zip_name)

    chunks = _chunk_text(content)
    docs = []
    for i, chunk in enumerate(chunks):
        meta = {
            "c_id": c_id,
            "chunk_index": i,
            "total_chunks": len(chunks),
            "domain": data.get("domain", ""),
            "creation_year": data.get("creation_year", ""),
        }
        docs.append({
            "content": chunk[:4096],
            "doc_type": doc_type,
            "source": source_spec[:128] if source_spec else "",
            "department": "",
            "metadata_json": json.dumps(meta, ensure_ascii=False)[:2048],
        })
    return docs


def _parse_label_data(data: dict, zip_name: str) -> List[Dict[str, Any]]:
    """라벨링데이터(TL_*, VL_*) JSON을 문서로 변환합니다."""
    question = data.get("question", "")
    answer = data.get("answer", "")
    if not question:
        return []

    qa_text = f"Q: {question}\nA: {answer}" if answer else f"Q: {question}"
    qa_id = data.get("qa_id", "")
    q_type = data.get("q_type", 0)
    doc_type = Q_TYPE_MAP.get(q_type, "qa_case")
    department = _extract_department(zip_name)

    meta = {
        "qa_id": qa_id,
        "q_type": q_type,
        "domain": data.get("domain", ""),
    }

    return [{
        "content": qa_text[:4096],
        "doc_type": doc_type,
        "source": "",
        "department": department[:64] if department else "",
        "metadata_json": json.dumps(meta, ensure_ascii=False)[:2048],
    }]


def _get_or_create_collection() -> Collection:
    """medical_knowledge 컬렉션을 가져오거나 생성합니다."""
    if utility.has_collection(COLLECTION_NAME):
        return Collection(COLLECTION_NAME)

    fields = [
        FieldSchema("id", DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema("embedding", DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
        FieldSchema("content", DataType.VARCHAR, max_length=4096),
        FieldSchema("doc_type", DataType.VARCHAR, max_length=64),
        FieldSchema("source", DataType.VARCHAR, max_length=128),
        FieldSchema("department", DataType.VARCHAR, max_length=64),
        FieldSchema("metadata_json", DataType.VARCHAR, max_length=2048),
    ]
    schema = CollectionSchema(fields, description="Professional Medical Knowledge for RAG")
    collection = Collection(COLLECTION_NAME, schema)

    # IVF_FLAT 인덱스 생성 (데이터량 많으므로 nlist=256)
    index_params = {
        "metric_type": "COSINE",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 256},
    }
    collection.create_index("embedding", index_params)
    logger.info(f"Created collection '{COLLECTION_NAME}' (dim={EMBEDDING_DIM}, nlist=256)")
    return collection


def _load_progress() -> set:
    """처리 완료된 ZIP 파일 목록을 로드합니다."""
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text())
            return set(data.get("completed_zips", []))
        except Exception:
            pass
    return set()


def _save_progress(completed_zips: set):
    """처리 완료된 ZIP 파일 목록을 저장합니다."""
    PROGRESS_FILE.write_text(json.dumps(
        {"completed_zips": sorted(completed_zips)},
        ensure_ascii=False,
        indent=2,
    ))


def _collect_zip_files() -> List[Path]:
    """데이터 디렉토리에서 모든 ZIP 파일을 수집합니다."""
    zips = []
    for base_path in DATA_BASE_PATHS:
        if not base_path.exists():
            continue
        for pattern in ["Training/01.원천데이터/*.zip", "Training/02.라벨링데이터/*.zip", "Validation/02.라벨링데이터/*.zip"]:
            zips.extend(sorted(base_path.glob(pattern)))
    return zips


def _process_zip(
    zip_path: Path,
    embedder: EmbeddingService,
    collection: Collection,
    batch_size: int = 64,
) -> int:
    """단일 ZIP 파일을 처리하여 Milvus에 적재합니다. 적재된 문서 수를 반환합니다."""
    zip_name = zip_path.name
    is_source = zip_name.startswith("TS_")
    total_inserted = 0

    # 배치 버퍼
    batch_docs: List[Dict[str, Any]] = []

    def flush_batch():
        nonlocal total_inserted
        if not batch_docs:
            return

        texts = [d["content"] for d in batch_docs]
        vectors = embedder.embed_batch(texts, batch_size=batch_size)

        collection.insert([
            vectors,
            [d["content"] for d in batch_docs],
            [d["doc_type"] for d in batch_docs],
            [d["source"] for d in batch_docs],
            [d["department"] for d in batch_docs],
            [d["metadata_json"] for d in batch_docs],
        ])
        total_inserted += len(batch_docs)
        batch_docs.clear()

    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            json_files = [f for f in zf.namelist() if f.endswith(".json")]
            for json_file in json_files:
                try:
                    raw = zf.read(json_file)
                    data = json.loads(raw)
                except (json.JSONDecodeError, KeyError, UnicodeDecodeError):
                    continue

                if is_source:
                    docs = _parse_source_data(data, zip_name)
                else:
                    docs = _parse_label_data(data, zip_name)

                batch_docs.extend(docs)

                if len(batch_docs) >= batch_size:
                    flush_batch()

        # 남은 배치 처리
        flush_batch()

    except zipfile.BadZipFile:
        logger.error(f"Bad ZIP file: {zip_path}")
    except Exception as e:
        logger.error(f"Error processing {zip_name}: {e}")

    return total_inserted


def run_etl(reset: bool = False, batch_size: int = 64) -> Dict[str, Any]:
    """ETL 메인 실행 함수.

    Args:
        reset: True이면 컬렉션을 삭제 후 재생성
        batch_size: 임베딩 배치 크기

    Returns:
        ETL 결과 요약 딕셔너리
    """
    global etl_status
    etl_status = {
        "running": True,
        "total_zips": 0,
        "processed_zips": 0,
        "total_docs": 0,
        "total_chunks": 0,
        "current_zip": "",
        "errors": [],
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "finished_at": None,
    }

    start_time = time.time()

    # Milvus 연결
    try:
        connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=30)
        logger.info(f"Connected to Milvus at {MILVUS_HOST}:{MILVUS_PORT}")
    except Exception as e:
        msg = f"Milvus connection failed: {e}"
        logger.error(msg)
        etl_status["running"] = False
        etl_status["errors"].append(msg)
        return etl_status

    # 컬렉션 초기화
    if reset:
        if utility.has_collection(COLLECTION_NAME):
            utility.drop_collection(COLLECTION_NAME)
            logger.info(f"Dropped existing collection '{COLLECTION_NAME}'")
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()

    collection = _get_or_create_collection()

    # ZIP 파일 수집
    zip_files = _collect_zip_files()
    if not zip_files:
        msg = f"No ZIP files found in {DATA_BASE_PATHS}"
        logger.warning(msg)
        etl_status["running"] = False
        etl_status["errors"].append(msg)
        return etl_status

    # 중단 후 재개 지원
    completed = _load_progress() if not reset else set()
    remaining = [z for z in zip_files if z.name not in completed]

    etl_status["total_zips"] = len(zip_files)
    etl_status["processed_zips"] = len(completed)

    logger.info(f"Found {len(zip_files)} ZIP files, {len(remaining)} remaining")

    # 임베더 초기화
    embedder = EmbeddingService()

    total_docs = 0
    for i, zip_path in enumerate(remaining):
        zip_name = zip_path.name
        etl_status["current_zip"] = zip_name
        logger.info(f"[{i+1}/{len(remaining)}] Processing {zip_name}...")

        try:
            count = _process_zip(zip_path, embedder, collection, batch_size)
            total_docs += count
            completed.add(zip_name)
            _save_progress(completed)
            etl_status["processed_zips"] = len(completed)
            etl_status["total_chunks"] += count
            logger.info(f"  → {count} chunks inserted")
        except Exception as e:
            msg = f"Failed: {zip_name}: {e}"
            logger.error(msg)
            etl_status["errors"].append(msg)

    # 최종 flush + load
    collection.flush()
    collection.load()

    elapsed = time.time() - start_time
    etl_status["running"] = False
    etl_status["total_docs"] = total_docs
    etl_status["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    etl_status["current_zip"] = ""

    final_count = collection.num_entities
    logger.info(
        f"ETL complete: {total_docs} chunks inserted in {elapsed:.1f}s. "
        f"Collection total: {final_count} entities"
    )

    return etl_status


def get_etl_status() -> Dict[str, Any]:
    """현재 ETL 상태를 반환합니다."""
    return etl_status.copy()


def get_collection_stats() -> Dict[str, Any]:
    """medical_knowledge 컬렉션 통계를 반환합니다."""
    try:
        connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=10)
    except Exception:
        pass  # 이미 연결된 경우

    if not utility.has_collection(COLLECTION_NAME):
        return {"exists": False, "count": 0}

    collection = Collection(COLLECTION_NAME)
    collection.flush()
    count = collection.num_entities

    return {
        "exists": True,
        "collection_name": COLLECTION_NAME,
        "count": count,
    }


def reset_collection():
    """컬렉션을 삭제합니다."""
    try:
        connections.connect(alias="default", host=MILVUS_HOST, port=MILVUS_PORT, timeout=10)
    except Exception:
        pass

    if utility.has_collection(COLLECTION_NAME):
        utility.drop_collection(COLLECTION_NAME)
        logger.info(f"Dropped collection '{COLLECTION_NAME}'")
    if PROGRESS_FILE.exists():
        PROGRESS_FILE.unlink()


# CLI 엔트리포인트
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Medical Knowledge ETL → Milvus")
    parser.add_argument("--reset", action="store_true", help="Drop collection and re-import all")
    parser.add_argument("--batch-size", type=int, default=64, help="Embedding batch size")
    args = parser.parse_args()

    result = run_etl(reset=args.reset, batch_size=args.batch_size)

    print(f"\n{'='*60}")
    print(f"ETL Complete")
    print(f"  Total chunks: {result['total_chunks']}")
    print(f"  Processed ZIPs: {result['processed_zips']}/{result['total_zips']}")
    print(f"  Errors: {len(result['errors'])}")
    if result["errors"]:
        for err in result["errors"]:
            print(f"    - {err}")
    print(f"{'='*60}")
