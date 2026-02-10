"""
의학 용어 자동 추출기 — medical_knowledge 컬렉션에서 주요 의학 용어 추출

적재된 의학 지식 텍스트에서 주요 의학 용어를 자동 추출하여
기존 ICD_CODE_MAP(97개)에 없는 새 용어를 식별합니다.
"""

import json
import logging
import os
import re
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

logger = logging.getLogger(__name__)

# 추출된 용어 저장 경로
EXTENDED_TERMS_PATH = Path(__file__).parent / "extended_terms.json"

# 의학 용어 패턴 (한국어)
_MEDICAL_PATTERNS = [
    # ~증, ~병, ~염, ~암, ~증후군
    re.compile(r"[\uAC00-\uD7AF]{2,6}(?:증후군|증|병|염|암|질환|장애|경색|부전|결핍|과다|비대|협착|출혈|혈증|부종)"),
    # 영문 약어
    re.compile(r"\b[A-Z]{2,6}(?:-\d+)?\b"),
]


def extract_terms_from_text(text: str) -> Set[str]:
    """텍스트에서 의학 용어를 추출합니다."""
    terms = set()
    for pattern in _MEDICAL_PATTERNS:
        for match in pattern.finditer(text):
            term = match.group().strip()
            if len(term) >= 2:
                terms.add(term)
    return terms


def extract_terms_from_collection(max_docs: int = 5000) -> Dict[str, int]:
    """medical_knowledge 컬렉션에서 의학 용어를 추출합니다.

    Args:
        max_docs: 분석할 최대 문서 수

    Returns:
        {용어: 출현 횟수} 딕셔너리
    """
    try:
        from pymilvus import Collection, connections, utility

        milvus_host = os.getenv("MILVUS_HOST", "localhost")
        milvus_port = int(os.getenv("MILVUS_PORT", "19530"))
        connections.connect(alias="default", host=milvus_host, port=milvus_port, timeout=10)

        collection_name = "medical_knowledge"
        if not utility.has_collection(collection_name):
            logger.warning(f"Collection '{collection_name}' not found")
            return {}

        col = Collection(collection_name)
        col.load()

        # 랜덤 샘플링으로 용어 추출
        term_counter: Counter = Counter()
        batch_size = 100
        for offset in range(0, min(max_docs, col.num_entities), batch_size):
            try:
                results = col.query(
                    expr=f"id > 0",
                    output_fields=["content"],
                    limit=batch_size,
                    offset=offset,
                )
                for row in results:
                    content = row.get("content", "")
                    terms = extract_terms_from_text(content)
                    term_counter.update(terms)
            except Exception:
                break

        # 최소 3회 이상 출현한 용어만 반환
        return {term: count for term, count in term_counter.items() if count >= 3}

    except Exception as e:
        logger.error(f"Term extraction failed: {e}")
        return {}


def get_new_terms(existing_terms: Set[str]) -> List[Tuple[str, int]]:
    """기존 ICD_CODE_MAP에 없는 새로운 용어를 식별합니다.

    Args:
        existing_terms: 기존에 등록된 용어 집합

    Returns:
        [(새 용어, 출현 횟수), ...] 리스트 (출현 횟수 내림차순)
    """
    all_terms = extract_terms_from_collection()
    new_terms = [
        (term, count) for term, count in all_terms.items()
        if term not in existing_terms and len(term) >= 2
    ]
    new_terms.sort(key=lambda x: x[1], reverse=True)
    return new_terms


def save_extended_terms(terms: Dict[str, int]):
    """추출된 확장 용어를 파일에 저장합니다."""
    EXTENDED_TERMS_PATH.write_text(
        json.dumps(terms, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.info(f"Saved {len(terms)} extended terms to {EXTENDED_TERMS_PATH}")


def load_extended_terms() -> Dict[str, int]:
    """저장된 확장 용어를 로드합니다."""
    if EXTENDED_TERMS_PATH.exists():
        try:
            return json.loads(EXTENDED_TERMS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "data_portal" / "src" / "api"))

    logging.basicConfig(level=logging.INFO)

    from data_portal.src.api.services.biz_meta import ICD_CODE_MAP

    existing = set(ICD_CODE_MAP.keys())
    print(f"Existing ICD terms: {len(existing)}")

    new_terms = get_new_terms(existing)
    print(f"\nNew medical terms found: {len(new_terms)}")
    for term, count in new_terms[:30]:
        print(f"  {term}: {count}회")

    # 저장
    term_dict = {term: count for term, count in new_terms}
    save_extended_terms(term_dict)
