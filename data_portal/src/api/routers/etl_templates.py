"""
ETL Template + Terminology Validation Endpoints — /etl/templates/*, /etl/terminology/validate
"""
import uuid
from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .etl_shared import (
    OMOP_TARGET_SCHEMA,
    COLUMN_SYNONYMS,
    STANDARD_TERMINOLOGY,
    load_sources,
    load_parallel_config,
    load_templates,
    save_templates,
    discover_pg_schema,
    simulate_schema,
    match_column,
)

router = APIRouter()


# =====================================================================
#  Default templates
# =====================================================================

DEFAULT_TEMPLATES: List[Dict[str, Any]] = [
    {
        "id": "tpl-ehr-person",
        "name": "EHR 환자정보 → person",
        "source_type": "rdbms",
        "source_subtype": "oracle",
        "target_table": "person",
        "description": "EHR 운영 DB의 환자 마스터 테이블을 OMOP CDM person 테이블로 적재",
        "mappings": [
            {"source": "PAT_ID", "target": "person_id"},
            {"source": "SEX_CD", "target": "gender_source_value"},
            {"source": "BIRTH_YEAR", "target": "year_of_birth"},
        ],
        "parallel_config": {"workers": 2, "batch_size": 10000},
        "tags": ["EHR", "환자", "person"],
        "is_builtin": True,
    },
    {
        "id": "tpl-ehr-visit",
        "name": "EHR 방문기록 → visit_occurrence",
        "source_type": "rdbms",
        "source_subtype": "oracle",
        "target_table": "visit_occurrence",
        "description": "EHR 내원/입퇴원 기록을 OMOP CDM visit_occurrence로 적재",
        "mappings": [
            {"source": "ENC_ID", "target": "visit_occurrence_id"},
            {"source": "PAT_ID", "target": "person_id"},
            {"source": "ADM_DATE", "target": "visit_start_date"},
            {"source": "DISCH_DATE", "target": "visit_end_date"},
        ],
        "parallel_config": {"workers": 2, "batch_size": 20000},
        "tags": ["EHR", "방문", "visit"],
        "is_builtin": True,
    },
    {
        "id": "tpl-dx-condition",
        "name": "진단/상병 → condition_occurrence",
        "source_type": "rdbms",
        "source_subtype": "oracle",
        "target_table": "condition_occurrence",
        "description": "진단 상병 데이터를 OMOP CDM condition_occurrence로 적재",
        "mappings": [
            {"source": "DX_ID", "target": "condition_occurrence_id"},
            {"source": "PAT_ID", "target": "person_id"},
            {"source": "ICD_CD", "target": "condition_source_value"},
            {"source": "DX_DATE", "target": "condition_start_date"},
        ],
        "parallel_config": {"workers": 2, "batch_size": 20000},
        "tags": ["진단", "상병", "ICD"],
        "is_builtin": True,
    },
    {
        "id": "tpl-rx-drug",
        "name": "처방/투약 → drug_exposure",
        "source_type": "rdbms",
        "source_subtype": "oracle",
        "target_table": "drug_exposure",
        "description": "처방/투약 데이터를 OMOP CDM drug_exposure로 적재",
        "mappings": [
            {"source": "RX_ID", "target": "drug_exposure_id"},
            {"source": "PAT_ID", "target": "person_id"},
            {"source": "DRUG_CD", "target": "drug_source_value"},
            {"source": "RX_DATE", "target": "drug_exposure_start_date"},
            {"source": "QTY", "target": "quantity"},
        ],
        "parallel_config": {"workers": 2, "batch_size": 20000},
        "tags": ["처방", "투약", "drug"],
        "is_builtin": True,
    },
    {
        "id": "tpl-lab-measurement",
        "name": "검사 결과 → measurement",
        "source_type": "rdbms",
        "source_subtype": "oracle",
        "target_table": "measurement",
        "description": "임상검사 결과를 OMOP CDM measurement로 적재",
        "mappings": [
            {"source": "LAB_ID", "target": "measurement_id"},
            {"source": "PAT_ID", "target": "person_id"},
            {"source": "TEST_CD", "target": "measurement_source_value"},
            {"source": "TEST_DATE", "target": "measurement_date"},
            {"source": "RESULT_VAL", "target": "value_as_number"},
            {"source": "UNIT", "target": "unit_source_value"},
        ],
        "parallel_config": {"workers": 8, "batch_size": 50000},
        "tags": ["검사", "LAB", "measurement"],
        "is_builtin": True,
    },
    {
        "id": "tpl-fhir-person",
        "name": "HL7 FHIR Patient → person",
        "source_type": "api",
        "source_subtype": "hl7-fhir",
        "target_table": "person",
        "description": "HL7 FHIR R4 Patient 리소스를 OMOP CDM person으로 적재",
        "mappings": [
            {"source": "Patient.id", "target": "person_source_value"},
            {"source": "Patient.gender", "target": "gender_source_value"},
            {"source": "Patient.birthDate(year)", "target": "year_of_birth"},
            {"source": "Patient.birthDate(month)", "target": "month_of_birth"},
            {"source": "Patient.birthDate(day)", "target": "day_of_birth"},
        ],
        "parallel_config": {"workers": 2, "batch_size": 5000},
        "tags": ["FHIR", "Patient", "person"],
        "is_builtin": True,
    },
    {
        "id": "tpl-csv-bulk",
        "name": "CSV 일괄 적재",
        "source_type": "file",
        "source_subtype": "csv",
        "target_table": "(선택 가능)",
        "description": "CSV 파일을 OMOP CDM 대상 테이블로 일괄 적재 (타겟 테이블 선택 가능)",
        "mappings": [],
        "parallel_config": {"workers": 4, "batch_size": 10000},
        "tags": ["CSV", "일괄적재", "bulk"],
        "is_builtin": True,
    },
]


# =====================================================================
#  Pydantic models
# =====================================================================

class TemplateCreateRequest(BaseModel):
    name: str
    source_type: str
    source_subtype: str
    target_table: str
    description: Optional[str] = ""
    mappings: Optional[List[Dict[str, str]]] = []
    parallel_config: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = []


class TemplateGenerateRequest(BaseModel):
    source_id: str
    target_table: str


class TerminologyValidateRequest(BaseModel):
    source_id: str
    columns: Optional[List[str]] = None


# =====================================================================
#  Endpoints
# =====================================================================

@router.get("/templates")
async def list_templates():
    """수집 템플릿 목록"""
    templates = load_templates()
    return {"templates": templates, "total": len(templates)}


@router.post("/templates")
async def create_template(req: TemplateCreateRequest):
    """수집 템플릿 생성"""
    templates = load_templates()
    tpl = {
        "id": f"tpl-{str(uuid.uuid4())[:8]}",
        "name": req.name,
        "source_type": req.source_type,
        "source_subtype": req.source_subtype,
        "target_table": req.target_table,
        "description": req.description or "",
        "mappings": req.mappings or [],
        "parallel_config": req.parallel_config or {"workers": 2, "batch_size": 10000},
        "tags": req.tags or [],
        "is_builtin": False,
        "created_at": datetime.now().isoformat(),
    }
    templates.append(tpl)
    save_templates(templates)
    return {"success": True, "template": tpl, "message": f"템플릿 '{req.name}' 생성됨"}


@router.post("/templates/generate")
async def auto_generate_template(req: TemplateGenerateRequest):
    """소스+타겟 조합으로 수집 템플릿 자동 생성"""
    sources = load_sources()
    source = next((s for s in sources if s["id"] == req.source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다")
    if req.target_table not in OMOP_TARGET_SCHEMA:
        raise HTTPException(status_code=404, detail=f"타겟 테이블 '{req.target_table}'을 찾을 수 없습니다")

    # 스키마 탐색하여 소스 컬럼 추출
    if source.get("subtype") == "postgresql":
        try:
            schema_tables = await discover_pg_schema(
                host=source.get("host", "localhost"),
                port=source.get("port", 5432),
                user=source.get("username", "omopuser"),
                password=source.get("password", "omop"),
                database=source.get("database", "omop_cdm"),
            )
        except Exception:
            schema_tables = simulate_schema(source)
    else:
        schema_tables = simulate_schema(source)

    # 타겟 테이블과 이름이 유사한 소스 테이블 찾기
    target_cols = OMOP_TARGET_SCHEMA[req.target_table]
    best_table = None
    best_score = 0
    for st in schema_tables:
        src_col_names = [c["name"] for c in st["columns"]]
        matched = 0
        for tc in target_cols:
            match = match_column(tc["name"], [{"name": sc} for sc in src_col_names])
            if match:
                matched += 1
        score = matched / len(target_cols) if target_cols else 0
        if score > best_score:
            best_score = score
            best_table = st

    # 매핑 자동 생성
    auto_mappings = []
    if best_table:
        for tc in target_cols:
            match = match_column(tc["name"], [{"name": c["name"]} for c in best_table["columns"]])
            if match:
                auto_mappings.append({"source": match["target_column"], "target": tc["name"]})

    # 병렬 설정 참조
    parallel = load_parallel_config()
    tbl_parallel = parallel.get("tables", {}).get(req.target_table, {"workers": 2, "batch_size": 10000})

    tpl = {
        "id": f"tpl-{str(uuid.uuid4())[:8]}",
        "name": f"{source['name']} → {req.target_table}",
        "source_type": source["type"],
        "source_subtype": source["subtype"],
        "target_table": req.target_table,
        "description": f"자동 생성: {source['name']}에서 {req.target_table}로 적재",
        "mappings": auto_mappings,
        "parallel_config": {"workers": tbl_parallel.get("workers", 2), "batch_size": tbl_parallel.get("batch_size", 10000)},
        "tags": ["자동생성", source["type"], req.target_table],
        "is_builtin": False,
        "created_at": datetime.now().isoformat(),
        "auto_generated": True,
        "source_table": best_table["table_name"] if best_table else None,
        "mapping_coverage": round(len(auto_mappings) / len(target_cols) * 100, 1) if target_cols else 0,
    }

    templates = load_templates()
    templates.append(tpl)
    save_templates(templates)

    return {"success": True, "template": tpl, "message": f"템플릿 자동 생성됨 ({len(auto_mappings)} 매핑)"}


@router.delete("/templates/{template_id}")
async def delete_template(template_id: str):
    """수집 템플릿 삭제"""
    templates = load_templates()
    original_len = len(templates)
    templates = [t for t in templates if t["id"] != template_id]
    if len(templates) == original_len:
        raise HTTPException(status_code=404, detail="템플릿을 찾을 수 없습니다")
    save_templates(templates)
    return {"success": True, "message": f"템플릿 '{template_id}' 삭제됨"}


@router.post("/terminology/validate")
async def validate_terminology(req: TerminologyValidateRequest):
    """표준 용어 메타 시스템 연계 검증"""
    sources = load_sources()
    source = next((s for s in sources if s["id"] == req.source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다")

    # 소스의 컬럼 목록 가져오기
    if req.columns:
        source_columns = req.columns
    else:
        if source.get("subtype") == "postgresql":
            try:
                tables = await discover_pg_schema(
                    host=source.get("host", "localhost"),
                    port=source.get("port", 5432),
                    user=source.get("username", "omopuser"),
                    password=source.get("password", "omop"),
                    database=source.get("database", "omop_cdm"),
                )
                source_columns = []
                for t in tables:
                    for c in t["columns"]:
                        source_columns.append(c["name"])
            except Exception:
                source_columns = []
        else:
            sim_tables = simulate_schema(source)
            source_columns = []
            for t in sim_tables:
                for c in t["columns"]:
                    source_columns.append(c["name"])

    matched = []
    unmatched = []
    for col in source_columns:
        col_lower = col.lower()
        if col_lower in STANDARD_TERMINOLOGY:
            term_info = STANDARD_TERMINOLOGY[col_lower]
            matched.append({
                "column": col,
                "standard": term_info["standard"],
                "code": term_info["code"],
                "domain": term_info["domain"],
                "description": term_info["description"],
            })
        else:
            # 동의어 사전에서도 검색
            found = False
            for target_name, synonyms in COLUMN_SYNONYMS.items():
                if col_lower in [s.lower() for s in synonyms]:
                    if target_name in STANDARD_TERMINOLOGY:
                        term_info = STANDARD_TERMINOLOGY[target_name]
                        matched.append({
                            "column": col,
                            "mapped_to": target_name,
                            "standard": term_info["standard"],
                            "code": term_info["code"],
                            "domain": term_info["domain"],
                            "description": term_info["description"],
                        })
                        found = True
                        break
            if not found:
                unmatched.append(col)

    total = len(source_columns)
    coverage = round(len(matched) / total * 100, 1) if total else 0

    return {
        "source_id": req.source_id,
        "source_name": source["name"],
        "total_columns": total,
        "matched_count": len(matched),
        "unmatched_count": len(unmatched),
        "coverage_percent": coverage,
        "matched": matched,
        "unmatched": unmatched,
        "standards_used": list(set(m["standard"] for m in matched)),
        "validated_at": datetime.now().isoformat(),
    }
