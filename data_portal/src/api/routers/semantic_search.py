"""
Semantic Layer API - 검색 엔드포인트 (통합검색, Faceted, 번역, SQL 컨텍스트 등)
"""
from difflib import SequenceMatcher
from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

from ._semantic_data import SAMPLE_TABLES, DOMAINS, TAGS, MEDICAL_TERM_HIERARCHY
from services.redis_cache import cached

router = APIRouter()


def _expand_search_terms(query: str) -> Dict[str, Any]:
    """검색어 의미 확장 - MEDICAL_TERM_HIERARCHY + BizMeta 동의어 + ICD 코드 매핑"""
    if not query:
        return {"original": query, "expanded_terms": [], "related_tables": [], "expansion_source": []}

    q_lower = query.lower().strip()
    expanded_terms: List[str] = []
    related_tables: List[str] = []
    expansion_source: List[str] = []

    # 1) MEDICAL_TERM_HIERARCHY 매칭 (카테고리명 + synonyms + related_terms)
    q_nospace = q_lower.replace(" ", "")
    for category, info in MEDICAL_TERM_HIERARCHY.items():
        cat_lower = category.lower()
        cat_nospace = cat_lower.replace(" ", "")
        synonyms_lower = [s.lower() for s in info["synonyms"]]
        synonyms_nospace = [s.replace(" ", "") for s in synonyms_lower]
        related_lower = [s.lower() for s in info["related_terms"]]
        related_nospace = [s.replace(" ", "") for s in related_lower]

        # 카테고리명 또는 동의어 매칭
        cat_match = (q_lower == cat_lower or q_nospace == cat_nospace
                or q_lower in synonyms_lower or q_nospace in synonyms_nospace
                or cat_lower in q_lower or cat_nospace in q_nospace
                or q_lower in cat_lower or q_nospace in cat_nospace
                or any(s in q_lower for s in synonyms_lower)
                or any(s in q_nospace for s in synonyms_nospace)
                or any(q_lower in s for s in synonyms_lower)
                or any(q_nospace in s for s in synonyms_nospace))

        # related_terms 매칭 (구체적 질환명 검색 지원: 무좀, 골절, 우울증 등)
        related_match = (q_lower in related_lower or q_nospace in related_nospace
                or any(s in q_lower for s in related_lower if len(s) >= 2)
                or any(q_lower in s for s in related_lower if len(q_lower) >= 2))

        if cat_match or related_match:
            expanded_terms.extend(info["related_terms"])
            expanded_terms.extend(info["synonyms"])
            related_tables.extend(info["related_tables"])
            match_type = "카테고리" if cat_match else "질환명"
            expansion_source.append(f"의학 용어 계층({match_type}): {category}")
            break  # 1 category match is enough

    # 2) BizMeta SYNONYM_MAP 매칭
    try:
        from services.biz_meta import SYNONYM_MAP
        for key, val in SYNONYM_MAP.items():
            if q_lower == key.lower() or q_lower in key.lower():
                expanded_terms.append(val)
                expansion_source.append(f"동의어: {key} → {val}")
    except ImportError:
        pass

    # 3) BizMeta ICD_CODE_MAP 매칭
    try:
        from services.biz_meta import ICD_CODE_MAP
        for key, (code, desc) in ICD_CODE_MAP.items():
            if q_lower == key.lower() or q_lower in key.lower() or key.lower() in q_lower:
                expanded_terms.append(desc)
                expanded_terms.append(code)
                expansion_source.append(f"ICD 코드: {key} → {code} ({desc})")
    except ImportError:
        pass

    # 중복 제거
    expanded_terms = list(dict.fromkeys(expanded_terms))
    related_tables = list(dict.fromkeys(related_tables))

    return {
        "original": query,
        "expanded_terms": expanded_terms,
        "related_tables": related_tables,
        "expansion_source": expansion_source,
    }


def _fuzzy_similarity(query: str, text: str) -> float:
    """토큰 기반 퍼지 유사도 계산 (0.0~1.0). difflib SequenceMatcher 사용."""
    if not query or not text:
        return 0.0
    q = query.lower().strip()
    t = text.lower().strip()
    if q in t:
        return 1.0
    # 전체 문자열 유사도
    ratio = SequenceMatcher(None, q, t).ratio()
    # 토큰별 최대 유사도 (짧은 쿼리가 긴 텍스트의 일부와 매칭될 수 있도록)
    tokens = t.split()
    if tokens:
        token_max = max(SequenceMatcher(None, q, tok).ratio() for tok in tokens)
        ratio = max(ratio, token_max)
    return ratio


def _compute_relevance_score(
    query_lower: str,
    expanded_lower: List[str],
    related_tables: List[str],
    searchable: str,
    physical_name: str,
) -> float:
    """테이블/컬럼에 대한 관련도 점수 계산 (0.0~1.0)."""
    score = 0.0
    # 1) Exact substring match → 1.0
    if query_lower in searchable:
        score = max(score, 1.0)
    # 2) Expanded term match → 0.8
    if any(term in searchable for term in expanded_lower):
        score = max(score, 0.8)
    # 3) Related table match → 0.7
    if physical_name in related_tables:
        score = max(score, 0.7)
    # 4) Fuzzy similarity → 0.0~0.6 (scaled)
    fuzzy = _fuzzy_similarity(query_lower, searchable) * 0.6
    score = max(score, fuzzy)
    return score


# ══════════════════════════════════════════════════════════════════════════════
# Pydantic models
# ══════════════════════════════════════════════════════════════════════════════

class SearchResult(BaseModel):
    success: bool
    data: Dict[str, Any]


# ══════════════════════════════════════════════════════════════════════════════
# Search endpoints
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/search", response_model=SearchResult)
@cached("sem-search", ttl=300)
async def search(
    q: str = Query(..., description="검색어"),
    domain: Optional[str] = Query(None, description="도메인 필터"),
    limit: int = Query(20, description="최대 결과 수"),
    min_score: float = Query(0.25, description="최소 유사도 점수 (0.0~1.0)"),
):
    """통합 검색 (의미 확장 + 벡터 유사도 포함)

    AAR-001: Semantic Search — 키워드 매칭 + 의미 확장 + 퍼지 유사도 스코어링.
    결과는 relevance_score 기준 내림차순 정렬됩니다.
    """
    q_lower = q.lower()
    scored_tables: List[tuple] = []  # (score, table)
    scored_columns: List[tuple] = []  # (score, col_dict)

    # 의미 확장
    expansion = _expand_search_terms(q)
    expanded_lower = [t.lower() for t in expansion.get("expanded_terms", [])]
    related_tables = expansion.get("related_tables", [])

    for table in SAMPLE_TABLES:
        # 도메인 필터
        if domain and table["domain"] != domain:
            continue

        searchable = (
            table["physical_name"] + " " +
            table["business_name"] + " " +
            table["description"] + " " +
            " ".join(table.get("tags", []))
        ).lower()

        # 테이블 유사도 점수 계산
        table_score = _compute_relevance_score(
            q_lower, expanded_lower, related_tables, searchable, table["physical_name"]
        )
        if table_score >= min_score:
            scored_tables.append((table_score, table))

        # 컬럼 매칭 (유사도 포함)
        for col in table["columns"]:
            col_searchable = (
                col["physical_name"] + " " +
                col["business_name"] + " " +
                col.get("description", "")
            ).lower()
            col_score = _compute_relevance_score(
                q_lower, expanded_lower, [], col_searchable, ""
            )
            if col_score >= min_score:
                scored_columns.append((col_score, {
                    **col,
                    "table_physical_name": table["physical_name"],
                    "table_business_name": table["business_name"],
                    "relevance_score": round(col_score, 3),
                }))

    # 점수 내림차순 정렬
    scored_tables.sort(key=lambda x: x[0], reverse=True)
    scored_columns.sort(key=lambda x: x[0], reverse=True)

    result_tables = []
    for score, table in scored_tables[:limit]:
        result_tables.append({**table, "relevance_score": round(score, 3)})

    result_columns = [col for _, col in scored_columns[:limit]]

    return SearchResult(
        success=True,
        data={
            "tables": result_tables,
            "columns": result_columns,
            "total": len(result_tables) + len(result_columns),
            "expansion": expansion,
            "search_method": "semantic_fuzzy",
        }
    )


@router.get("/faceted-search")
async def faceted_search(
    q: Optional[str] = None,
    domains: Optional[str] = None,
    tags: Optional[str] = None,
    sensitivity: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Faceted Search (의미 확장 지원)"""
    domain_list = domains.split(",") if domains else []
    tag_list = tags.split(",") if tags else []
    sensitivity_list = sensitivity.split(",") if sensitivity else []

    # 의미 확장 (AAR-001)
    expansion = _expand_search_terms(q) if q else None

    results = []
    for table in SAMPLE_TABLES:
        # 도메인 필터
        if domain_list and table["domain"] not in domain_list:
            continue

        # 태그 필터
        if tag_list and not any(t in table["tags"] for t in tag_list):
            continue

        # 민감도 필터
        if sensitivity_list:
            has_sensitivity = any(
                col["sensitivity"] in sensitivity_list
                for col in table["columns"]
            )
            if not has_sensitivity:
                continue

        # 검색어 필터 (불용어 제거 후 AND 매칭 + 의미 확장)
        if q:
            _stop = {"테이블", "데이터", "정보", "목록", "조회", "보여줘", "알려줘", "찾아줘", "뭐야", "table", "data", "list", "show"}
            tokens = [t for t in q.lower().split() if t not in _stop]
            if not tokens:
                pass  # 불용어만 입력한 경우 필터 건너뜀 (전체 반환)
            else:
                searchable = (table["physical_name"] + " " + table["business_name"] + " " + table["description"] + " " + " ".join(table.get("tags", []))).lower()
                col_text = " ".join(c["physical_name"] + " " + c["business_name"] for c in table["columns"]).lower()
                searchable += " " + col_text

                # 원본 매칭
                original_match = all(token in searchable for token in tokens)

                # 확장 용어 매칭
                expanded_match = False
                if expansion and expansion["expanded_terms"]:
                    for term in expansion["expanded_terms"]:
                        if term.lower() in searchable:
                            expanded_match = True
                            break

                # related_tables 매칭
                related_match = False
                if expansion and expansion["related_tables"]:
                    if table["physical_name"] in expansion["related_tables"]:
                        related_match = True

                if not (original_match or expanded_match or related_match):
                    continue

        results.append(table)

    total = len(results)
    resp: Dict[str, Any] = {
        "success": True,
        "data": {
            "tables": results[offset:offset + limit],
            "total": total,
            "facets": {
                "domains": DOMAINS,
                "tags": TAGS,
                "sensitivity": ["Normal", "PHI", "Restricted"],
            }
        }
    }
    # 의미 확장 정보 추가
    if expansion and expansion["expanded_terms"]:
        resp["data"]["expansion"] = expansion
    return resp


@router.post("/translate/physical-to-business")
async def translate_physical_to_business(body: Dict[str, List[str]]):
    """물리명 → 비즈니스명 변환"""
    physical_names = body.get("physical_names", [])
    result = {}

    for pname in physical_names:
        pname_upper = pname.upper()
        # 테이블 검색
        for table in SAMPLE_TABLES:
            if table["physical_name"].upper() == pname_upper:
                result[pname] = table["business_name"]
                break
            # 컬럼 검색
            for col in table["columns"]:
                if col["physical_name"].upper() == pname_upper:
                    result[pname] = col["business_name"]
                    break

    return {"success": True, "translations": result}


@router.get("/translate/business-to-physical")
async def translate_business_to_physical(q: str):
    """비즈니스명 → 물리명 변환"""
    q_lower = q.lower()
    results = []

    for table in SAMPLE_TABLES:
        if q_lower in table["business_name"].lower():
            results.append({
                "business_name": table["business_name"],
                "physical_name": table["physical_name"],
                "type": "table"
            })

        for col in table["columns"]:
            if q_lower in col["business_name"].lower():
                results.append({
                    "business_name": col["business_name"],
                    "physical_name": col["physical_name"],
                    "type": "column",
                    "table": table["physical_name"]
                })

    return {"success": True, "results": results}


@router.get("/sql-context")
async def get_sql_context(q: str):
    """SQL 생성용 컨텍스트 조회"""
    search_result = await search(q=q, domain=None, limit=5)
    tables = search_result.data.get("tables", [])

    context_parts = []
    for table in tables:
        cols = ", ".join([f"{c['physical_name']} ({c['business_name']})" for c in table["columns"][:5]])
        context_parts.append(f"테이블: {table['physical_name']} ({table['business_name']})\n컬럼: {cols}")

    return {
        "success": True,
        "context": "\n\n".join(context_parts),
        "tables": tables
    }


@router.get("/popular")
async def get_popular_data(top_n: int = 10):
    """인기 데이터"""
    sorted_tables = sorted(SAMPLE_TABLES, key=lambda x: x["usage_count"], reverse=True)
    return {"success": True, "data": {"items": [{"name": t["physical_name"], "business_name": t["business_name"], "count": t["usage_count"]} for t in sorted_tables[:top_n]]}}


@router.get("/related/{table_name}")
async def get_related_data(table_name: str, limit: int = 5):
    """연관 데이터"""
    # 같은 도메인의 테이블 반환
    target_table = None
    for t in SAMPLE_TABLES:
        if t["physical_name"].upper() == table_name.upper():
            target_table = t
            break

    if not target_table:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Table not found")

    related = [
        t for t in SAMPLE_TABLES
        if t["domain"] == target_table["domain"] and t["physical_name"] != target_table["physical_name"]
    ]

    return {"success": True, "related": related[:limit]}
