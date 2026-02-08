"""
MCP (Model Context Protocol) API — 실제 서비스 연결 버전

Tools:
  - search_catalog: RAG 벡터 검색 (Qdrant)
  - generate_sql:   Text2SQL LLM 호출
  - execute_sql:    OMOP CDM SQL 실행 (docker exec)
  - get_table_info: 실시간 테이블 메타데이터
  - vector_search:  RAG 벡터 유사도 검색
  - get_data_lineage: OMOP CDM 데이터 계보
  - check_data_quality: 실시간 품질 검사 (NULL 비율, 행 수)
"""
import logging
from fastapi import APIRouter, HTTPException
from typing import Optional, Dict, Any
from datetime import datetime

from services.sql_executor import sql_executor

logger = logging.getLogger(__name__)

router = APIRouter()

# ── MCP 도구 스키마 정의 ──────────────────────────────────

MCP_TOOLS = {
    "search_catalog": {
        "name": "search_catalog",
        "description": "데이터 카탈로그에서 테이블과 컬럼을 벡터 검색합니다 (RAG)",
        "category": "data",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "검색어"},
                "top_k": {"type": "integer", "description": "결과 개수", "default": 5},
            },
            "required": ["query"]
        }
    },
    "generate_sql": {
        "name": "generate_sql",
        "description": "자연어 질문을 SQL 쿼리로 변환합니다 (LLM 기반)",
        "category": "sql",
        "parameters": {
            "type": "object",
            "properties": {
                "question": {"type": "string", "description": "자연어 질문"},
                "tables": {"type": "array", "items": {"type": "string"}, "description": "사용할 테이블 목록"},
            },
            "required": ["question"]
        }
    },
    "execute_sql": {
        "name": "execute_sql",
        "description": "SQL 쿼리를 OMOP CDM 데이터베이스에서 실행합니다",
        "category": "sql",
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "실행할 SELECT SQL 쿼리"},
                "limit": {"type": "integer", "description": "최대 결과 행 수", "default": 100},
            },
            "required": ["sql"]
        }
    },
    "get_table_info": {
        "name": "get_table_info",
        "description": "테이블의 실시간 메타데이터를 조회합니다",
        "category": "data",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "테이블 물리명"},
            },
            "required": ["table_name"]
        }
    },
    "vector_search": {
        "name": "vector_search",
        "description": "Qdrant 벡터 유사도 검색을 수행합니다 (RAG Pipeline)",
        "category": "search",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "검색 질의"},
                "top_k": {"type": "integer", "description": "결과 개수", "default": 5},
            },
            "required": ["query"]
        }
    },
    "get_data_lineage": {
        "name": "get_data_lineage",
        "description": "OMOP CDM 데이터 계보 정보를 조회합니다",
        "category": "governance",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "테이블명"},
            },
            "required": ["table_name"]
        }
    },
    "check_data_quality": {
        "name": "check_data_quality",
        "description": "테이블의 데이터 품질을 실시간 검사합니다",
        "category": "governance",
        "parameters": {
            "type": "object",
            "properties": {
                "table_name": {"type": "string", "description": "테이블명"},
                "rules": {"type": "array", "items": {"type": "string"}, "description": "검사 규칙"},
            },
            "required": ["table_name"]
        }
    },
}

# OMOP CDM 테이블-컬럼 메타데이터
OMOP_TABLE_META = {
    "person": {
        "business_name": "환자",
        "columns": [
            {"name": "person_id", "type": "BIGINT", "pk": True, "description": "환자 고유 ID"},
            {"name": "gender_concept_id", "type": "INTEGER", "description": "성별 개념 ID"},
            {"name": "gender_source_value", "type": "VARCHAR(50)", "description": "성별 (M/F)"},
            {"name": "year_of_birth", "type": "INTEGER", "description": "출생연도"},
            {"name": "race_concept_id", "type": "INTEGER", "description": "인종 개념 ID"},
        ],
    },
    "visit_occurrence": {
        "business_name": "방문기록",
        "columns": [
            {"name": "visit_occurrence_id", "type": "BIGINT", "pk": True, "description": "방문 고유 ID"},
            {"name": "person_id", "type": "BIGINT", "description": "환자 ID (FK → person)"},
            {"name": "visit_concept_id", "type": "INTEGER", "description": "방문유형 (9201=입원, 9202=외래, 9203=응급)"},
            {"name": "visit_start_date", "type": "DATE", "description": "방문 시작일"},
            {"name": "visit_end_date", "type": "DATE", "description": "방문 종료일"},
        ],
    },
    "condition_occurrence": {
        "business_name": "진단기록",
        "columns": [
            {"name": "condition_occurrence_id", "type": "BIGINT", "pk": True, "description": "진단 고유 ID"},
            {"name": "person_id", "type": "BIGINT", "description": "환자 ID (FK → person)"},
            {"name": "condition_concept_id", "type": "INTEGER", "description": "진단 개념 ID (SNOMED)"},
            {"name": "condition_source_value", "type": "VARCHAR", "description": "진단 소스 코드"},
            {"name": "condition_start_date", "type": "DATE", "description": "진단 시작일"},
        ],
    },
    "drug_exposure": {
        "business_name": "약물처방",
        "columns": [
            {"name": "drug_exposure_id", "type": "BIGINT", "pk": True, "description": "약물 고유 ID"},
            {"name": "person_id", "type": "BIGINT", "description": "환자 ID (FK → person)"},
            {"name": "drug_concept_id", "type": "INTEGER", "description": "약물 개념 ID"},
            {"name": "drug_source_value", "type": "VARCHAR", "description": "약물 소스 코드"},
            {"name": "drug_exposure_start_date", "type": "DATE", "description": "처방 시작일"},
        ],
    },
    "measurement": {
        "business_name": "검사결과",
        "columns": [
            {"name": "measurement_id", "type": "BIGINT", "pk": True, "description": "검사 고유 ID"},
            {"name": "person_id", "type": "BIGINT", "description": "환자 ID (FK → person)"},
            {"name": "measurement_concept_id", "type": "INTEGER", "description": "검사 개념 ID"},
            {"name": "measurement_source_value", "type": "VARCHAR", "description": "검사 소스 코드"},
            {"name": "value_as_number", "type": "NUMERIC", "description": "검사 결과값"},
            {"name": "measurement_date", "type": "DATE", "description": "검사일"},
        ],
    },
    "procedure_occurrence": {
        "business_name": "시술기록",
        "columns": [
            {"name": "procedure_occurrence_id", "type": "BIGINT", "pk": True, "description": "시술 고유 ID"},
            {"name": "person_id", "type": "BIGINT", "description": "환자 ID (FK → person)"},
            {"name": "procedure_concept_id", "type": "INTEGER", "description": "시술 개념 ID"},
            {"name": "procedure_source_value", "type": "VARCHAR", "description": "시술 소스 코드"},
            {"name": "procedure_date", "type": "DATE", "description": "시술일"},
        ],
    },
    "observation": {
        "business_name": "관찰기록",
        "columns": [
            {"name": "observation_id", "type": "BIGINT", "pk": True, "description": "관찰 고유 ID"},
            {"name": "person_id", "type": "BIGINT", "description": "환자 ID (FK → person)"},
            {"name": "observation_concept_id", "type": "INTEGER", "description": "관찰 개념 ID"},
            {"name": "observation_source_value", "type": "VARCHAR", "description": "관찰 소스 코드"},
            {"name": "observation_date", "type": "DATE", "description": "관찰일"},
        ],
    },
}

# OMOP CDM 데이터 계보 정보
OMOP_LINEAGE = {
    "person": {"upstream": ["Synthea Patient Generator"], "downstream": ["visit_occurrence", "condition_occurrence", "drug_exposure", "measurement"]},
    "visit_occurrence": {"upstream": ["Synthea Encounter Generator", "person"], "downstream": ["condition_occurrence", "drug_exposure", "measurement", "cost"]},
    "condition_occurrence": {"upstream": ["Synthea Condition Module", "person", "visit_occurrence"], "downstream": ["condition_era"]},
    "drug_exposure": {"upstream": ["Synthea Medication Module", "person", "visit_occurrence"], "downstream": ["drug_era"]},
    "measurement": {"upstream": ["Synthea Observation Module", "person", "visit_occurrence"], "downstream": []},
    "procedure_occurrence": {"upstream": ["Synthea Procedure Module", "person", "visit_occurrence"], "downstream": []},
    "observation": {"upstream": ["Synthea Observation Module", "person", "visit_occurrence"], "downstream": []},
}


@router.get("/mcp/manifest")
async def get_manifest():
    """MCP 서버 매니페스트"""
    return {
        "name": "IDP MCP Server",
        "version": "2.0.0",
        "description": "서울아산병원 통합 데이터 플랫폼 MCP 서버 — OMOP CDM 실연동",
        "protocol_version": "2024-11-05",
        "capabilities": {
            "tools": True,
            "resources": True,
            "prompts": True,
        },
        "tools_count": len(MCP_TOOLS),
        "backend": {
            "database": "OMOP CDM (PostgreSQL 13)",
            "vector_db": "Qdrant v1.12.0",
            "embedding_model": "paraphrase-multilingual-MiniLM-L12-v2",
            "llm": "XiYanSQL-QwenCoder-7B / Qwen3-32B",
        },
    }


@router.get("/mcp/tools")
async def get_tools(category: Optional[str] = None):
    """도구 목록 조회"""
    tools = list(MCP_TOOLS.values())
    if category:
        tools = [t for t in tools if t["category"] == category]
    return {
        "success": True,
        "tools": tools,
        "categories": sorted(set(t["category"] for t in MCP_TOOLS.values()))
    }


@router.post("/mcp/tools/{tool_name}")
async def call_tool(tool_name: str, args: Dict[str, Any] = {}):
    """MCP 도구 호출"""
    if tool_name not in MCP_TOOLS:
        raise HTTPException(status_code=404, detail=f"Tool '{tool_name}' not found")

    try:
        handler = _TOOL_HANDLERS.get(tool_name)
        if not handler:
            return {"success": False, "error": "Tool not implemented"}
        return await handler(args)
    except Exception as e:
        logger.error(f"MCP tool '{tool_name}' error: {e}")
        return {"success": False, "error": str(e)}


# ═══ 실제 도구 구현 ═══════════════════════════════════════

async def _search_catalog(args: Dict) -> Dict:
    """RAG 벡터 검색으로 카탈로그 검색"""
    query = args.get("query", "")
    top_k = args.get("top_k", 5)

    results = []
    try:
        from ai_services.rag.retriever import get_retriever
        retriever = get_retriever()
        if retriever._initialized:
            hits = retriever.retrieve(query, top_k=top_k)
            for h in hits:
                payload = h.get("payload", {})
                results.append({
                    "score": round(h.get("score", 0), 4),
                    "type": payload.get("type", "unknown"),
                    "content": payload.get("content", ""),
                    "table": payload.get("table", ""),
                })
    except Exception as e:
        logger.warning(f"RAG search fallback: {e}")

    # Fallback: 키워드 기반 메타데이터 매칭
    if not results:
        q = query.lower()
        for tname, meta in OMOP_TABLE_META.items():
            if q in tname or q in meta["business_name"]:
                results.append({
                    "score": 0.80,
                    "type": "table_schema",
                    "content": f"{tname} ({meta['business_name']})",
                    "table": tname,
                })

    return {"success": True, "result": {"query": query, "results": results, "source": "rag" if results else "fallback"}}


async def _generate_sql(args: Dict) -> Dict:
    """LLM 기반 Text2SQL"""
    question = args.get("question", "")
    tables = args.get("tables", [])

    sql = ""
    source = "template"

    # 1차: LLM 서비스 시도
    try:
        from services.llm_service import llm_service
        llm_result = await llm_service.generate_sql(question)
        if llm_result and llm_result.get("sql"):
            sql = llm_result["sql"]
            source = llm_result.get("provider", "llm")
    except Exception as e:
        logger.warning(f"LLM SQL generation fallback: {e}")

    # 2차: 템플릿 폴백
    if not sql:
        if "환자" in question and ("수" in question or "몇" in question):
            sql = "SELECT COUNT(DISTINCT person_id) AS patient_count FROM person"
        elif "외래" in question:
            sql = "SELECT COUNT(*) AS visit_count FROM visit_occurrence WHERE visit_concept_id = 9202"
        elif "입원" in question:
            sql = "SELECT COUNT(*) AS admission_count FROM visit_occurrence WHERE visit_concept_id = 9201"
        elif "진단" in question or "질환" in question:
            sql = "SELECT condition_source_value, COUNT(DISTINCT person_id) AS cnt FROM condition_occurrence GROUP BY condition_source_value ORDER BY cnt DESC LIMIT 20"
        elif "약물" in question or "처방" in question:
            sql = "SELECT drug_source_value, COUNT(DISTINCT person_id) AS cnt FROM drug_exposure GROUP BY drug_source_value ORDER BY cnt DESC LIMIT 20"
        else:
            sql = f"SELECT * FROM {tables[0] if tables else 'person'} LIMIT 100"

    return {
        "success": True,
        "result": {
            "sql": sql,
            "source": source,
            "explanation": f"'{question}'에 대한 SQL ({source})",
            "tables_used": tables or ["person"],
        }
    }


async def _execute_sql(args: Dict) -> Dict:
    """OMOP CDM SQL 실행 (실제 docker exec)"""
    sql = args.get("sql", "")
    if not sql.strip():
        return {"success": False, "error": "SQL이 비어있습니다"}

    result = await sql_executor.execute(sql)

    if result.natural_language_explanation and "오류" in result.natural_language_explanation:
        return {"success": False, "error": result.natural_language_explanation}

    return {
        "success": True,
        "result": {
            "columns": result.columns,
            "rows": result.results,
            "row_count": result.row_count,
            "execution_time_ms": result.execution_time_ms,
        }
    }


async def _get_table_info(args: Dict) -> Dict:
    """실시간 테이블 메타데이터 (행 수 포함)"""
    table_name = args.get("table_name", "").lower()

    meta = OMOP_TABLE_META.get(table_name)
    if not meta:
        return {"success": False, "error": f"Unknown table: {table_name}"}

    # 실시간 행 수 조회 (pg_stat_user_tables 사용 — 빠름)
    row_count = 0
    try:
        count_sql = f"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = '{table_name}'"
        result = await sql_executor.execute(count_sql)
        if result.results:
            row_count = result.results[0][0]
    except Exception:
        pass

    return {
        "success": True,
        "result": {
            "physical_name": table_name,
            "business_name": meta["business_name"],
            "columns": meta["columns"],
            "row_count": row_count,
            "last_updated": datetime.utcnow().isoformat(),
        }
    }


async def _vector_search(args: Dict) -> Dict:
    """Qdrant RAG 벡터 유사도 검색"""
    query = args.get("query", "")
    top_k = args.get("top_k", 5)

    results = []
    try:
        from ai_services.rag.retriever import get_retriever
        retriever = get_retriever()
        if retriever._initialized:
            hits = retriever.retrieve(query, top_k=top_k)
            for h in hits:
                payload = h.get("payload", {})
                results.append({
                    "score": round(h.get("score", 0), 4),
                    "type": payload.get("type", "unknown"),
                    "content": payload.get("content", ""),
                })
            return {"success": True, "result": {"query": query, "results": results, "source": "qdrant"}}
    except Exception as e:
        logger.warning(f"Qdrant search error: {e}")

    return {"success": True, "result": {"query": query, "results": [], "source": "unavailable", "note": "Qdrant 연결 불가 — RAG 초기화 필요"}}


async def _get_data_lineage(args: Dict) -> Dict:
    """OMOP CDM 데이터 계보"""
    table_name = args.get("table_name", "").lower()

    lineage = OMOP_LINEAGE.get(table_name, {
        "upstream": ["Unknown Source"],
        "downstream": [],
    })

    return {
        "success": True,
        "result": {
            "table": table_name,
            "upstream": lineage["upstream"],
            "downstream": lineage["downstream"],
            "transformations": ["Synthea → OMOP CDM ETL (13 steps)", "SNOMED CT Vocabulary Mapping"],
            "etl_pipeline": "synthea/etl_synthea_to_omop.py",
        }
    }


async def _check_data_quality(args: Dict) -> Dict:
    """실시간 데이터 품질 검사"""
    table_name = args.get("table_name", "").lower()

    meta = OMOP_TABLE_META.get(table_name)
    if not meta:
        return {"success": False, "error": f"Unknown table: {table_name}"}

    checks = []

    # 행 수 확인
    try:
        count_sql = f"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = '{table_name}'"
        result = await sql_executor.execute(count_sql)
        row_count = result.results[0][0] if result.results else 0
        checks.append({
            "rule": "row_count",
            "status": "pass" if row_count > 0 else "fail",
            "value": row_count,
            "description": f"테이블 행 수: {row_count:,}",
        })
    except Exception:
        checks.append({"rule": "row_count", "status": "error", "value": 0, "description": "행 수 조회 실패"})

    # PK 유일성 검사 (person_id 기반)
    pk_col = next((c["name"] for c in meta["columns"] if c.get("pk")), None)
    if pk_col:
        try:
            dup_sql = f"SELECT COUNT(*) - COUNT(DISTINCT {pk_col}) FROM {table_name}"
            result = await sql_executor.execute(dup_sql)
            dup_count = result.results[0][0] if result.results else 0
            checks.append({
                "rule": "uniqueness",
                "status": "pass" if dup_count == 0 else "warning",
                "value": dup_count,
                "description": f"PK ({pk_col}) 중복 수: {dup_count}",
            })
        except Exception:
            pass

    # person_id NULL 검사 (person 테이블 제외)
    has_person_id = any(c["name"] == "person_id" and not c.get("pk") for c in meta["columns"])
    if has_person_id:
        try:
            null_sql = f"SELECT COUNT(*) FROM {table_name} WHERE person_id IS NULL"
            result = await sql_executor.execute(null_sql)
            null_count = result.results[0][0] if result.results else 0
            checks.append({
                "rule": "completeness",
                "status": "pass" if null_count == 0 else "warning",
                "value": null_count,
                "description": f"person_id NULL 수: {null_count}",
            })
        except Exception:
            pass

    overall = "pass" if all(c["status"] == "pass" for c in checks) else "warning"

    return {
        "success": True,
        "result": {
            "table": table_name,
            "checks": checks,
            "overall_status": overall,
            "checked_at": datetime.utcnow().isoformat(),
        }
    }


# 도구 핸들러 매핑
_TOOL_HANDLERS = {
    "search_catalog": _search_catalog,
    "generate_sql": _generate_sql,
    "execute_sql": _execute_sql,
    "get_table_info": _get_table_info,
    "vector_search": _vector_search,
    "get_data_lineage": _get_data_lineage,
    "check_data_quality": _check_data_quality,
}
