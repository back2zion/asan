"""
카탈로그 추천 API — 활용 이력 기반 지능형 추천
DPR-002: 지능형 데이터 카탈로그 및 탐색 환경 구축
"""
import os
import sqlite3
from collections import Counter, defaultdict

from fastapi import APIRouter, Query

router = APIRouter(prefix="/catalog-recommend", tags=["CatalogRecommend"])

CONV_MEMORY_DB = os.getenv(
    "CONVERSATION_MEMORY_DB",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "conversation_memory.db"),
)

# 전체 OMOP 테이블 메타 (도메인 매핑)
TABLE_META = {
    "person": {"domain": "Demographics", "label": "환자 인구통계", "usage_base": 95},
    "visit_occurrence": {"domain": "Clinical", "label": "내원/방문 이력", "usage_base": 90},
    "condition_occurrence": {"domain": "Clinical", "label": "진단/상병 기록", "usage_base": 85},
    "measurement": {"domain": "Lab/Vital", "label": "검사결과/활력징후", "usage_base": 88},
    "drug_exposure": {"domain": "Clinical", "label": "약물 처방/투약", "usage_base": 80},
    "observation": {"domain": "Clinical", "label": "관찰/기타 임상", "usage_base": 72},
    "procedure_occurrence": {"domain": "Clinical", "label": "시술/수술 이력", "usage_base": 75},
    "visit_detail": {"domain": "Clinical", "label": "방문 상세(병동이동)", "usage_base": 60},
    "condition_era": {"domain": "Derived", "label": "진단 기간 요약", "usage_base": 55},
    "drug_era": {"domain": "Derived", "label": "약물 기간 요약", "usage_base": 50},
    "observation_period": {"domain": "Derived", "label": "관찰 기간", "usage_base": 45},
    "cost": {"domain": "Financial", "label": "비용 정보", "usage_base": 40},
    "payer_plan_period": {"domain": "Financial", "label": "보험/급여 기간", "usage_base": 35},
    "care_site": {"domain": "Health System", "label": "진료 부서/기관", "usage_base": 30},
    "provider": {"domain": "Health System", "label": "의료진 정보", "usage_base": 28},
    "location": {"domain": "Health System", "label": "지역 정보", "usage_base": 20},
}

# 테이블 공동사용 관계 (하드코딩 — FK 기반)
CO_USAGE_GRAPH: dict[str, list[str]] = {
    "person": ["visit_occurrence", "condition_occurrence", "measurement", "drug_exposure", "observation", "procedure_occurrence"],
    "visit_occurrence": ["person", "condition_occurrence", "measurement", "procedure_occurrence", "visit_detail"],
    "condition_occurrence": ["person", "visit_occurrence", "drug_exposure", "condition_era"],
    "measurement": ["person", "visit_occurrence", "observation"],
    "drug_exposure": ["person", "condition_occurrence", "drug_era"],
    "observation": ["person", "measurement", "visit_occurrence"],
    "procedure_occurrence": ["person", "visit_occurrence", "condition_occurrence"],
    "visit_detail": ["visit_occurrence", "person", "care_site"],
    "condition_era": ["condition_occurrence", "person"],
    "drug_era": ["drug_exposure", "person"],
    "observation_period": ["person"],
    "cost": ["person", "visit_occurrence", "payer_plan_period"],
    "payer_plan_period": ["person", "cost"],
    "care_site": ["visit_detail", "provider", "location"],
    "provider": ["care_site"],
    "location": ["care_site"],
}

# 도메인별 테이블 그룹
DOMAIN_TABLES: dict[str, list[str]] = defaultdict(list)
for _t, _m in TABLE_META.items():
    DOMAIN_TABLES[_m["domain"]].append(_t)


def _query_user_tables(user_id: str) -> list[str]:
    """conversation_memory.db에서 사용자 최근 테이블 사용 이력 조회"""
    if not os.path.exists(CONV_MEMORY_DB):
        return []
    try:
        con = sqlite3.connect(CONV_MEMORY_DB, timeout=2)
        rows = con.execute(
            "SELECT tables_used FROM conversations "
            "WHERE user_id = ? AND tables_used IS NOT NULL AND tables_used != '' "
            "ORDER BY created_at DESC LIMIT 50",
            (user_id,),
        ).fetchall()
        con.close()
        tables: list[str] = []
        for (tu,) in rows:
            tables.extend(t.strip() for t in tu.split(",") if t.strip())
        return tables
    except Exception:
        return []


def _compute_recommendations(
    user_tables: list[str],
    exclude: set[str] | None = None,
    limit: int = 6,
) -> list[dict]:
    """3중 신호 기반 추천"""
    exclude = exclude or set()
    scores: dict[str, dict] = {}

    user_freq = Counter(user_tables)
    user_set = set(user_tables)

    # Signal 1: 쿼리 기반 공동출현
    for ut in user_set:
        for related in CO_USAGE_GRAPH.get(ut, []):
            if related not in user_set and related not in exclude:
                if related not in scores:
                    scores[related] = {"score": 0, "reasons": []}
                scores[related]["score"] += user_freq[ut] * 3
                if len(scores[related]["reasons"]) < 2:
                    scores[related]["reasons"].append(f"'{ut}'와 자주 함께 사용됨")

    # Signal 2: 도메인 기반 (사용자가 탐색한 도메인의 미조회 테이블)
    user_domains = set()
    for ut in user_set:
        meta = TABLE_META.get(ut)
        if meta:
            user_domains.add(meta["domain"])
    for domain in user_domains:
        for t in DOMAIN_TABLES.get(domain, []):
            if t not in user_set and t not in exclude:
                if t not in scores:
                    scores[t] = {"score": 0, "reasons": []}
                scores[t]["score"] += 5
                if not any("도메인" in r for r in scores[t]["reasons"]):
                    scores[t]["reasons"].append(f"'{domain}' 도메인의 관련 데이터")

    # Signal 3: 인기도 폴백
    for t, meta in TABLE_META.items():
        if t not in user_set and t not in exclude:
            if t not in scores:
                scores[t] = {"score": 0, "reasons": []}
            scores[t]["score"] += meta["usage_base"] / 10
            if not scores[t]["reasons"]:
                scores[t]["reasons"].append(f"높은 활용도 (인기 데이터)")

    # 정렬 및 반환
    ranked = sorted(scores.items(), key=lambda x: -x[1]["score"])[:limit]
    results = []
    for table_name, info in ranked:
        meta = TABLE_META.get(table_name, {})
        results.append({
            "table_name": table_name,
            "label": meta.get("label", table_name),
            "domain": meta.get("domain", "Other"),
            "relevance_score": round(min(info["score"], 100), 1),
            "reasons": info["reasons"][:2],
        })
    return results


@router.get("/for-user/{user_id}")
async def recommend_for_user(user_id: str, limit: int = Query(default=6, ge=1, le=20)):
    """개인 맞춤 추천 (사유 포함)"""
    user_tables = _query_user_tables(user_id)
    # 사용 이력이 없으면 기본 추천
    if not user_tables:
        user_tables = ["person", "visit_occurrence"]

    recommendations = _compute_recommendations(user_tables, limit=limit)
    return {
        "user_id": user_id,
        "recommendations": recommendations,
        "based_on_tables": list(set(user_tables))[:10],
    }


@router.get("/for-table/{table_name}")
async def recommend_for_table(table_name: str, limit: int = Query(default=5, ge=1, le=20)):
    """이 테이블을 사용한 사용자가 함께 사용한 테이블"""
    related = CO_USAGE_GRAPH.get(table_name, [])
    results = []
    for rel in related[:limit]:
        meta = TABLE_META.get(rel, {})
        results.append({
            "table_name": rel,
            "label": meta.get("label", rel),
            "domain": meta.get("domain", "Other"),
            "relevance_score": round(meta.get("usage_base", 50) * 0.9, 1),
            "reason": f"'{table_name}'과(와) 함께 자주 사용됨",
        })
    # 부족하면 인기도로 채움
    existing = {table_name} | set(r["table_name"] for r in results)
    if len(results) < limit:
        for t, meta in sorted(TABLE_META.items(), key=lambda x: -x[1]["usage_base"]):
            if t not in existing:
                results.append({
                    "table_name": t,
                    "label": meta["label"],
                    "domain": meta["domain"],
                    "relevance_score": round(meta["usage_base"] * 0.5, 1),
                    "reason": "인기 데이터셋",
                })
                if len(results) >= limit:
                    break

    return {
        "table_name": table_name,
        "related_tables": results,
    }


@router.get("/trending")
async def get_trending_recommend():
    """트렌딩 테이블/검색어 (추천 패널용)"""
    # conversation_memory에서 최근 가장 많이 사용된 테이블
    if os.path.exists(CONV_MEMORY_DB):
        try:
            con = sqlite3.connect(CONV_MEMORY_DB, timeout=2)
            rows = con.execute(
                "SELECT tables_used FROM conversations "
                "WHERE tables_used IS NOT NULL AND tables_used != '' "
                "ORDER BY created_at DESC LIMIT 200"
            ).fetchall()
            con.close()
            counter: Counter = Counter()
            for (tu,) in rows:
                for t in tu.split(","):
                    t = t.strip()
                    if t and t in TABLE_META:
                        counter[t] += 1
            if counter:
                trending = [
                    {"table_name": t, "label": TABLE_META[t]["label"], "count": c}
                    for t, c in counter.most_common(6)
                ]
                return {"trending_tables": trending}
        except Exception:
            pass

    # 폴백
    return {
        "trending_tables": [
            {"table_name": t, "label": TABLE_META[t]["label"], "count": TABLE_META[t]["usage_base"]}
            for t in ["person", "visit_occurrence", "condition_occurrence", "measurement", "drug_exposure", "observation"]
        ]
    }
