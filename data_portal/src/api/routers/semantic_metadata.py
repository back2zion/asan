"""
Semantic Layer API - 메타데이터, 계보(lineage), 품질, 태그, 활용 통계 엔드포인트
"""
from fastapi import APIRouter, HTTPException
from typing import Optional, List, Dict, Any
from datetime import datetime

from ._semantic_data import SAMPLE_TABLES, DOMAINS, TAGS
from services.redis_cache import cached

router = APIRouter()

# 활용 기록 (in-memory)
usage_records: List[Dict] = []


# ══════════════════════════════════════════════════════════════════════════════
# Metadata endpoints
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/table/{physical_name}")
async def get_table_metadata(physical_name: str):
    """테이블 메타데이터 조회"""
    for table in SAMPLE_TABLES:
        if table["physical_name"].lower() == physical_name.lower():
            return {"success": True, "data": table}

    raise HTTPException(status_code=404, detail="Table not found")


@router.get("/domains")
@cached("sem-domains", ttl=600)
async def get_domains():
    """도메인 목록"""
    domain_stats = {}
    for table in SAMPLE_TABLES:
        domain = table["domain"]
        if domain not in domain_stats:
            domain_stats[domain] = 0
        domain_stats[domain] += 1

    return {
        "success": True,
        "data": {
            "domains": list(domain_stats.keys()),
            "domain_stats": [
                {"name": d, "table_count": domain_stats.get(d, 0)}
                for d in DOMAINS if domain_stats.get(d, 0) > 0
            ],
        }
    }


@router.get("/domains/{domain}/tables")
async def get_tables_by_domain(domain: str):
    """도메인별 테이블 목록"""
    tables = [t for t in SAMPLE_TABLES if t["domain"] == domain]
    return {"success": True, "tables": tables}


# ══════════════════════════════════════════════════════════════════════════════
# Lineage endpoint
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/lineage/{table_name}")
async def get_lineage(table_name: str, include_usage: bool = False):
    """데이터 계보 - OMOP CDM 기반 Lakehouse 아키텍처 (DPR-002)"""
    table_name_upper = table_name.upper()

    # 테이블 정보 찾기 (대소문자 무관)
    target_table = None
    for t in SAMPLE_TABLES:
        if t["physical_name"].lower() == table_name.lower():
            target_table = t
            break

    # OMOP CDM 테이블 매핑 (Synthea 데이터)
    OMOP_SOURCE_MAP = {
        "PERSON": {"source": "HIS (AMIS 3.0)", "source_tables": ["환자등록", "인적정보"]},
        "VISIT_OCCURRENCE": {"source": "HIS (AMIS 3.0)", "source_tables": ["외래접수", "입원등록"]},
        "CONDITION_OCCURRENCE": {"source": "HIS (AMIS 3.0)", "source_tables": ["진단정보", "상병코드"]},
        "DRUG_EXPOSURE": {"source": "HIS (AMIS 3.0)", "source_tables": ["처방정보", "투약기록"]},
        "MEASUREMENT": {"source": "LIS", "source_tables": ["검사결과", "임상검사"]},
        "OBSERVATION": {"source": "HIS (AMIS 3.0)", "source_tables": ["관찰기록", "간호기록"]},
        "PROCEDURE_OCCURRENCE": {"source": "HIS (AMIS 3.0)", "source_tables": ["수술기록", "처치기록"]},
        "IMAGING_STUDY": {"source": "PACS (NIH CXR)", "source_tables": ["NIH Chest X-ray"]},
        "PROVIDER": {"source": "HIS (AMIS 3.0)", "source_tables": ["의사마스터", "의료진등록"]},
        "CARE_SITE": {"source": "HIS (AMIS 3.0)", "source_tables": ["부서코드", "진료실정보"]},
        "LOCATION": {"source": "HIS (AMIS 3.0)", "source_tables": ["주소마스터"]},
        "OBSERVATION_PERIOD": {"source": "ETL (Synthea)", "source_tables": ["환자등록기간"]},
        "VISIT_DETAIL": {"source": "HIS (AMIS 3.0)", "source_tables": ["입원이력", "전과기록"]},
        "CONDITION_ERA": {"source": "ETL (Derived)", "source_tables": ["condition_occurrence 집계"]},
        "DRUG_ERA": {"source": "ETL (Derived)", "source_tables": ["drug_exposure 집계"]},
    }

    source_info = OMOP_SOURCE_MAP.get(table_name_upper, {
        "source": "HIS (AMIS 3.0)",
        "source_tables": ["원천시스템"]
    })

    # 연관 테이블 (FK 관계)
    related_tables = []
    if target_table:
        # 같은 도메인 테이블
        for t in SAMPLE_TABLES:
            if t["physical_name"] != target_table["physical_name"] and t["domain"] == target_table["domain"]:
                related_tables.append({
                    "name": t["physical_name"],
                    "business_name": t["business_name"],
                    "relationship": "same_domain"
                })
        # person_id FK로 연결된 테이블 (OMOP CDM)
        for t in SAMPLE_TABLES:
            has_patient_fk = any(c["physical_name"] == "person_id" for c in t["columns"])
            if has_patient_fk and t["physical_name"] != target_table["physical_name"]:
                if not any(r["name"] == t["physical_name"] for r in related_tables):
                    related_tables.append({
                        "name": t["physical_name"],
                        "business_name": t["business_name"],
                        "relationship": "fk_patient"
                    })

    # 리니지 노드 생성
    nodes = [
        {
            "id": "source",
            "label": source_info["source"],
            "type": "source",
            "layer": "Source",
            "description": "원천 시스템",
            "metadata": {"tables": source_info["source_tables"]}
        },
        {
            "id": "cdc",
            "label": "DeltaStream CDC",
            "type": "process",
            "layer": "Process",
            "description": "실시간 변경 캡처"
        },
        {
            "id": "bronze",
            "label": f"RAW_{table_name_upper}",
            "type": "bronze",
            "layer": "Bronze (ODS)",
            "description": "원본 데이터 적재"
        },
        {
            "id": "etl",
            "label": "TeraStream ETL",
            "type": "process",
            "layer": "Process",
            "description": "정제 및 표준화"
        },
        {
            "id": "silver",
            "label": f"STD_{table_name_upper}",
            "type": "silver",
            "layer": "Silver (DW)",
            "description": "표준화 적용"
        },
        {
            "id": "gold",
            "label": table_name_upper,
            "type": "gold",
            "layer": "Gold (Mart)",
            "description": target_table["business_name"] if target_table else "데이터 마트"
        }
    ]

    edges = [
        {"source": "source", "target": "cdc", "label": "실시간"},
        {"source": "cdc", "target": "bronze", "label": "CDC"},
        {"source": "bronze", "target": "etl", "label": "배치"},
        {"source": "etl", "target": "silver", "label": "정제"},
        {"source": "silver", "target": "gold", "label": "집계"}
    ]

    # 활용 기반 리니지 병합 (AAR-001)
    usage_nodes = []
    usage_edges = []
    if include_usage:
        try:
            import sqlite3
            import os
            db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "conversation_memory.db")
            if os.path.exists(db_path):
                conn_sqlite = sqlite3.connect(db_path)
                cursor = conn_sqlite.cursor()
                cursor.execute("SELECT tables_used FROM query_history WHERE tables_used IS NOT NULL AND tables_used != '' AND tables_used != '[]'")
                rows = cursor.fetchall()
                conn_sqlite.close()

                import json as _json_mod
                co_tables: Dict[str, int] = {}
                for (tables_used_str,) in rows:
                    try:
                        parsed = _json_mod.loads(tables_used_str)
                        used = [t.strip().lower() for t in parsed if isinstance(t, str) and t.strip()] if isinstance(parsed, list) else [t.strip().lower() for t in tables_used_str.split(",") if t.strip()]
                    except Exception:
                        used = [t.strip().lower() for t in tables_used_str.split(",") if t.strip()]
                    if table_name.lower() in used:
                        for t in used:
                            if t != table_name.lower():
                                co_tables[t] = co_tables.get(t, 0) + 1

                for co_table, count in sorted(co_tables.items(), key=lambda x: -x[1])[:5]:
                    # Find business name
                    biz_name = co_table
                    for st in SAMPLE_TABLES:
                        if st["physical_name"].lower() == co_table:
                            biz_name = st["business_name"]
                            break
                    node_id = f"usage_{co_table}"
                    usage_nodes.append({
                        "id": node_id,
                        "label": co_table.upper(),
                        "type": "usage",
                        "layer": "Usage",
                        "description": f"쿼리 로그 동시 조회 {count}회 ({biz_name})",
                    })
                    usage_edges.append({
                        "source": "gold",
                        "target": node_id,
                        "label": f"동시 조회 {count}회",
                        "style": "dashed",
                    })
        except Exception:
            pass

    return {
        "success": True,
        "lineage": {
            "table": table_name_upper,
            "business_name": target_table["business_name"] if target_table else table_name,
            "nodes": nodes + usage_nodes,
            "edges": edges + usage_edges,
            "upstream": source_info["source_tables"],
            "downstream": [r["name"] for r in related_tables[:3]],
            "related_tables": related_tables[:5]
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# Quality endpoint
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/quality/{table_name}")
async def get_quality_metrics(table_name: str):
    """데이터 품질 지표 - DGR-004"""
    import random

    table_name_upper = table_name.upper()

    # 테이블 정보 찾기 (대소문자 무관)
    target_table = None
    for t in SAMPLE_TABLES:
        if t["physical_name"].lower() == table_name.lower():
            target_table = t
            break

    if not target_table:
        raise HTTPException(status_code=404, detail="Table not found")

    # 품질 지표 생성 (실제로는 QualityStream에서 조회)
    # 시드 설정으로 테이블별 일관된 값 생성
    random.seed(hash(table_name_upper))

    completeness = round(95 + random.uniform(0, 4.5), 1)
    accuracy = round(96 + random.uniform(0, 3.5), 1)
    consistency = round(94 + random.uniform(0, 5), 1)
    timeliness = "실시간 (CDC)" if random.random() > 0.3 else "배치 (1시간)"

    # 컬럼별 품질 지표
    column_quality = []
    for col in target_table["columns"]:
        random.seed(hash(col["physical_name"]))
        null_rate = round(random.uniform(0, 5), 2) if not col.get("is_pk") else 0
        column_quality.append({
            "column_name": col["physical_name"],
            "business_name": col["business_name"],
            "null_rate": null_rate,
            "distinct_count": random.randint(100, 10000),
            "min_length": random.randint(1, 5),
            "max_length": random.randint(10, 100),
        })

    return {
        "success": True,
        "quality": {
            "table": table_name_upper,
            "business_name": target_table["business_name"],
            "metrics": {
                "completeness": completeness,
                "accuracy": accuracy,
                "consistency": consistency,
                "timeliness": timeliness,
            },
            "column_quality": column_quality,
            "rules_applied": [
                {"name": "NOT NULL 검증", "status": "passed", "violations": 0},
                {"name": "참조 무결성 검증", "status": "passed", "violations": 0},
                {"name": "값 범위 검증", "status": "passed", "violations": random.randint(0, 5)},
                {"name": "중복 검증", "status": "passed", "violations": 0},
                {"name": "형식 검증", "status": "passed", "violations": random.randint(0, 3)},
            ],
            "last_checked": datetime.utcnow().isoformat(),
            "check_frequency": "매 1시간"
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# Tags endpoint
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/tags")
async def get_tags():
    """태그 목록"""
    tag_stats = {}
    for table in SAMPLE_TABLES:
        for tag in table["tags"]:
            if tag not in tag_stats:
                tag_stats[tag] = 0
            tag_stats[tag] += 1

    return {
        "success": True,
        "data": {
            "tags": list(tag_stats.keys()),
            "tag_stats": [{"name": t, "count": c} for t, c in tag_stats.items()],
        }
    }


# ══════════════════════════════════════════════════════════════════════════════
# Usage recording & statistics
# ══════════════════════════════════════════════════════════════════════════════

@router.post("/usage/record")
async def record_usage(body: Dict[str, Any]):
    """활용 기록"""
    record = {
        "id": len(usage_records) + 1,
        "user_id": body.get("user_id"),
        "action_type": body.get("action_type"),
        "target_type": body.get("target_type"),
        "target_name": body.get("target_name"),
        "metadata": body.get("metadata"),
        "timestamp": datetime.utcnow().isoformat(),
    }
    usage_records.append(record)
    return {"success": True, "record_id": record["id"]}


@router.get("/usage/statistics")
async def get_usage_statistics(
    target_type: Optional[str] = None,
    target_name: Optional[str] = None,
    top_n: int = 10
):
    """활용 통계"""
    filtered = usage_records
    if target_type:
        filtered = [r for r in filtered if r.get("target_type") == target_type]
    if target_name:
        filtered = [r for r in filtered if r.get("target_name") == target_name]

    return {
        "success": True,
        "total_records": len(filtered),
        "recent_records": filtered[-top_n:]
    }
