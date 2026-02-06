"""
Semantic Layer API - 데이터 카탈로그, 메타데이터 관리
"""
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from datetime import datetime

router = APIRouter()

# 샘플 메타데이터 (실제로는 DB에서 조회)
SAMPLE_TABLES = [
    {
        "physical_name": "PT_BSNF",
        "business_name": "환자기본정보",
        "description": "환자의 기본 인적사항 및 등록 정보를 관리하는 마스터 테이블",
        "domain": "환자",
        "columns": [
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": True, "is_nullable": False, "sensitivity": "PHI", "description": "환자 고유 식별번호"},
            {"physical_name": "PT_NM", "business_name": "환자성명", "data_type": "VARCHAR(50)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 이름"},
            {"physical_name": "BRTH_DT", "business_name": "생년월일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "환자 생년월일"},
            {"physical_name": "SEX_CD", "business_name": "성별코드", "data_type": "CHAR(1)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "M: 남성, F: 여성"},
            {"physical_name": "RGST_DT", "business_name": "등록일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "최초 등록 일자"},
        ],
        "usage_count": 1250,
        "tags": ["마스터", "환자", "기본정보"],
    },
    {
        "physical_name": "OPD_RCPT",
        "business_name": "외래접수",
        "description": "외래 진료 접수 정보를 관리하는 테이블",
        "domain": "진료",
        "columns": [
            {"physical_name": "RCPT_NO", "business_name": "접수번호", "data_type": "VARCHAR(15)", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "접수 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자번호 (FK)"},
            {"physical_name": "DEPT_CD", "business_name": "진료과코드", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "진료과 코드"},
            {"physical_name": "DR_ID", "business_name": "담당의ID", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "담당 의사 ID"},
            {"physical_name": "RCPT_DT", "business_name": "접수일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "접수 일자"},
            {"physical_name": "RCPT_TM", "business_name": "접수시간", "data_type": "TIME", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "접수 시간"},
        ],
        "usage_count": 890,
        "tags": ["외래", "진료", "접수"],
    },
    {
        "physical_name": "IPD_ADM",
        "business_name": "입원",
        "description": "입원 환자 정보를 관리하는 테이블",
        "domain": "진료",
        "columns": [
            {"physical_name": "ADM_NO", "business_name": "입원번호", "data_type": "VARCHAR(15)", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "입원 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자번호 (FK)"},
            {"physical_name": "ADM_DT", "business_name": "입원일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "입원 일자"},
            {"physical_name": "DSCH_DT", "business_name": "퇴원일자", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "퇴원 일자"},
            {"physical_name": "WARD_CD", "business_name": "병동코드", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "병동 코드"},
            {"physical_name": "ROOM_NO", "business_name": "병실번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "병실 번호"},
        ],
        "usage_count": 650,
        "tags": ["입원", "진료", "병동"],
    },
    {
        "physical_name": "LAB_RSLT",
        "business_name": "검사결과",
        "description": "임상검사 결과 정보를 관리하는 테이블",
        "domain": "검사",
        "columns": [
            {"physical_name": "RSLT_NO", "business_name": "결과번호", "data_type": "VARCHAR(20)", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "검사결과 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자번호 (FK)"},
            {"physical_name": "TEST_CD", "business_name": "검사코드", "data_type": "VARCHAR(20)", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "검사 항목 코드"},
            {"physical_name": "TEST_NM", "business_name": "검사명", "data_type": "VARCHAR(100)", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "검사 항목명"},
            {"physical_name": "RSLT_VAL", "business_name": "결과값", "data_type": "VARCHAR(100)", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "검사 결과 값"},
            {"physical_name": "RSLT_UNIT", "business_name": "결과단위", "data_type": "VARCHAR(20)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "결과 단위"},
            {"physical_name": "TEST_DT", "business_name": "검사일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "검사 일자"},
        ],
        "usage_count": 2100,
        "tags": ["검사", "임상", "결과"],
    },
    {
        "physical_name": "DIAG_INFO",
        "business_name": "진단정보",
        "description": "환자 진단 정보를 관리하는 테이블",
        "domain": "진료",
        "columns": [
            {"physical_name": "DIAG_NO", "business_name": "진단번호", "data_type": "VARCHAR(20)", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "진단 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자번호 (FK)"},
            {"physical_name": "ICD_CD", "business_name": "진단코드", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "ICD-10 진단 코드"},
            {"physical_name": "DIAG_NM", "business_name": "진단명", "data_type": "VARCHAR(200)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "진단명"},
            {"physical_name": "DIAG_DT", "business_name": "진단일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "진단 일자"},
            {"physical_name": "DIAG_TP", "business_name": "진단유형", "data_type": "CHAR(1)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "M: 주진단, S: 부진단"},
        ],
        "usage_count": 1800,
        "tags": ["진단", "진료", "ICD"],
    },
]

DOMAINS = ["환자", "진료", "검사", "처방", "수술", "영상", "간호"]
TAGS = ["마스터", "환자", "기본정보", "외래", "입원", "진료", "검사", "임상", "결과", "진단", "ICD", "병동"]

# 활용 기록 (in-memory)
usage_records: List[Dict] = []


class SearchResult(BaseModel):
    success: bool
    data: Dict[str, Any]


@router.get("/semantic/search", response_model=SearchResult)
async def search(
    q: str = Query(..., description="검색어"),
    domain: Optional[str] = Query(None, description="도메인 필터"),
    limit: int = Query(20, description="최대 결과 수")
):
    """통합 검색"""
    q_lower = q.lower()
    results_tables = []
    results_columns = []

    for table in SAMPLE_TABLES:
        # 도메인 필터
        if domain and table["domain"] != domain:
            continue

        # 테이블 매칭
        if (q_lower in table["physical_name"].lower() or
            q_lower in table["business_name"].lower() or
            q_lower in table["description"].lower() or
            any(q_lower in tag.lower() for tag in table["tags"])):
            results_tables.append(table)

        # 컬럼 매칭
        for col in table["columns"]:
            if (q_lower in col["physical_name"].lower() or
                q_lower in col["business_name"].lower() or
                q_lower in col.get("description", "").lower()):
                results_columns.append({
                    **col,
                    "table_physical_name": table["physical_name"],
                    "table_business_name": table["business_name"],
                })

    return SearchResult(
        success=True,
        data={
            "tables": results_tables[:limit],
            "columns": results_columns[:limit],
            "total": len(results_tables) + len(results_columns),
        }
    )


@router.get("/semantic/faceted-search")
async def faceted_search(
    q: Optional[str] = None,
    domains: Optional[str] = None,
    tags: Optional[str] = None,
    sensitivity: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Faceted Search"""
    domain_list = domains.split(",") if domains else []
    tag_list = tags.split(",") if tags else []
    sensitivity_list = sensitivity.split(",") if sensitivity else []

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

        # 검색어 필터
        if q:
            q_lower = q.lower()
            if not (q_lower in table["physical_name"].lower() or
                    q_lower in table["business_name"].lower() or
                    q_lower in table["description"].lower()):
                continue

        results.append(table)

    total = len(results)
    return {
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


@router.post("/semantic/translate/physical-to-business")
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


@router.get("/semantic/translate/business-to-physical")
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


@router.get("/semantic/table/{physical_name}")
async def get_table_metadata(physical_name: str):
    """테이블 메타데이터 조회"""
    for table in SAMPLE_TABLES:
        if table["physical_name"].upper() == physical_name.upper():
            return {"success": True, "data": table}

    raise HTTPException(status_code=404, detail="Table not found")


@router.get("/semantic/domains")
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
        "domains": [
            {"name": d, "table_count": domain_stats.get(d, 0)}
            for d in DOMAINS
        ]
    }


@router.get("/semantic/domains/{domain}/tables")
async def get_tables_by_domain(domain: str):
    """도메인별 테이블 목록"""
    tables = [t for t in SAMPLE_TABLES if t["domain"] == domain]
    return {"success": True, "tables": tables}


@router.get("/semantic/lineage/{table_name}")
async def get_lineage(table_name: str):
    """데이터 계보 - OMOP CDM 기반 Lakehouse 아키텍처 (DPR-002)"""
    table_name_upper = table_name.upper()

    # 테이블 정보 찾기
    target_table = None
    for t in SAMPLE_TABLES:
        if t["physical_name"].upper() == table_name_upper:
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
        # 병원 테이블 매핑
        "PT_BSNF": {"source": "HIS (AMIS 3.0)", "source_tables": ["환자마스터"]},
        "OPD_RCPT": {"source": "HIS (AMIS 3.0)", "source_tables": ["외래접수"]},
        "IPD_ADM": {"source": "HIS (AMIS 3.0)", "source_tables": ["입원관리"]},
        "LAB_RSLT": {"source": "LIS (nGLIS)", "source_tables": ["검사결과"]},
        "DIAG_INFO": {"source": "HIS (AMIS 3.0)", "source_tables": ["진단기록"]},
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
        # PT_NO로 연결된 테이블
        for t in SAMPLE_TABLES:
            if any(c["physical_name"] == "PT_NO" for c in t["columns"]) and t["physical_name"] != table_name_upper:
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

    return {
        "success": True,
        "lineage": {
            "table": table_name_upper,
            "business_name": target_table["business_name"] if target_table else table_name,
            "nodes": nodes,
            "edges": edges,
            "upstream": source_info["source_tables"],
            "downstream": [r["name"] for r in related_tables[:3]],
            "related_tables": related_tables[:5]
        }
    }


@router.get("/semantic/quality/{table_name}")
async def get_quality_metrics(table_name: str):
    """데이터 품질 지표 - DGR-004"""
    import random

    table_name_upper = table_name.upper()

    # 테이블 정보 찾기
    target_table = None
    for t in SAMPLE_TABLES:
        if t["physical_name"].upper() == table_name_upper:
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


@router.get("/semantic/sql-context")
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


@router.get("/semantic/popular")
async def get_popular_data(top_n: int = 10):
    """인기 데이터"""
    sorted_tables = sorted(SAMPLE_TABLES, key=lambda x: x["usage_count"], reverse=True)
    return {"success": True, "tables": sorted_tables[:top_n]}


@router.get("/semantic/related/{table_name}")
async def get_related_data(table_name: str, limit: int = 5):
    """연관 데이터"""
    # 같은 도메인의 테이블 반환
    target_table = None
    for t in SAMPLE_TABLES:
        if t["physical_name"].upper() == table_name.upper():
            target_table = t
            break

    if not target_table:
        raise HTTPException(status_code=404, detail="Table not found")

    related = [
        t for t in SAMPLE_TABLES
        if t["domain"] == target_table["domain"] and t["physical_name"] != target_table["physical_name"]
    ]

    return {"success": True, "related": related[:limit]}


@router.get("/semantic/tags")
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
        "tags": [{"name": t, "count": c} for t, c in tag_stats.items()]
    }


@router.post("/semantic/usage/record")
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


@router.get("/semantic/usage/statistics")
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
