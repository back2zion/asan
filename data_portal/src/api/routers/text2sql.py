"""
Text2SQL API Router - 5-step pipeline implementation
"""
import os
import re
import asyncio
from fastapi import APIRouter, HTTPException

from models.text2sql import (
    Text2SQLRequest,
    Text2SQLResponse,
    EnhancedText2SQLRequest,
    EnhancedText2SQLResponse,
    ExecutionResult,
)
from services.biz_meta import biz_meta_service
from services.it_meta import it_meta_service
from services.llm_service import llm_service
from services.sql_executor import sql_executor

# AAR-001: Auto query logging
try:
    from routers.catalog_analytics import log_query_to_catalog
    QUERY_LOG_ENABLED = True
except ImportError:
    QUERY_LOG_ENABLED = False


router = APIRouter()

# Mock mode 설정 (DB 없이 테스트용) - OMOP CDM DB 사용 가능하므로 기본값 false
MOCK_MODE = os.getenv("TEXT2SQL_MOCK_MODE", "false").lower() == "true"


def _extract_tables_from_sql(sql: str) -> list:
    """SQL에서 테이블명 추출"""
    if not sql:
        return []
    matches = re.findall(r'\bFROM\s+(\w+)|\bJOIN\s+(\w+)', sql, re.IGNORECASE)
    return list(set(t for pair in matches for t in pair if t))


@router.post("/text2sql/generate", response_model=Text2SQLResponse)
async def generate_sql(request: Text2SQLRequest):
    """기본 Text2SQL 생성

    5단계 파이프라인:
    1. 의도 파악 (LLM)
    2. 용어 해석 (BizMeta) - 스킵
    3. 스키마 조회 (ITMeta)
    4. SQL 생성 (LLM)
    5. 검증 - 실행 없음
    """
    try:
        # Step 1: 의도 파악
        intent = await llm_service.extract_intent(request.question)

        # Step 2: 용어 해석 (기본 모드에서는 스킵)
        enhanced_question = request.question

        # Step 3: 스키마 조회
        keywords = intent.entities + intent.filters
        if not keywords:
            keywords = [request.question]
        schema_context = it_meta_service.get_schema_context(keywords)

        # Step 4: SQL 생성
        sql, explanation, confidence = await llm_service.generate_sql(
            question=request.question,
            enhanced_question=enhanced_question,
            schema_context=schema_context,
            intent=intent
        )

        # Step 5: SQL 검증
        is_valid, error_msg = sql_executor.validate_sql(sql)
        if not is_valid:
            raise HTTPException(
                status_code=400,
                detail=f"Generated SQL validation failed: {error_msg}"
            )

        # AAR-001: Auto query logging
        if QUERY_LOG_ENABLED:
            asyncio.ensure_future(log_query_to_catalog(
                query_text=request.question,
                query_type="text2sql",
                tables_accessed=_extract_tables_from_sql(sql),
            ))

        return Text2SQLResponse(
            sql=sql,
            explanation=explanation if request.include_explanation else None,
            confidence=confidence
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/text2sql/enhanced-generate", response_model=EnhancedText2SQLResponse)
async def enhanced_generate_sql(request: EnhancedText2SQLRequest):
    """Enhanced Text2SQL 생성 (의료 용어 지원)

    5단계 파이프라인:
    1. 의도 파악 (LLM)
    2. 용어 해석 (BizMeta) - 의료 용어 매핑
    3. 스키마 조회 (ITMeta)
    4. SQL 생성 (LLM)
    5. 검증 및 실행 (optional)
    """
    try:
        # Step 1: 의도 파악
        intent = await llm_service.extract_intent(request.question)

        # Step 2: 용어 해석 (BizMeta)
        enhanced_question, term_resolutions = biz_meta_service.resolve_terms_in_question(
            request.question
        )

        enhancements_applied = []
        enhancement_confidence = 1.0

        for resolution in term_resolutions:
            enhancements_applied.append(
                f"{resolution.original_term} → {resolution.resolved_term}"
            )
            enhancement_confidence = min(enhancement_confidence, resolution.confidence)

        if not enhancements_applied:
            enhancement_confidence = 0.5  # 변환된 용어 없음

        # Step 3: 스키마 조회
        keywords = intent.entities + intent.filters
        # 해석된 용어에서도 키워드 추출
        for resolution in term_resolutions:
            keywords.append(resolution.original_term)

        if not keywords:
            keywords = [request.question]

        schema_context = it_meta_service.get_schema_context(keywords)

        # Step 4: SQL 생성
        sql, sql_explanation, sql_confidence = await llm_service.generate_sql(
            question=request.question,
            enhanced_question=enhanced_question,
            schema_context=schema_context,
            intent=intent
        )

        # Step 5: 검증
        is_valid, error_msg = sql_executor.validate_sql(sql)
        if not is_valid:
            raise HTTPException(
                status_code=400,
                detail=f"Generated SQL validation failed: {error_msg}"
            )

        # 선택적 실행
        execution_result = None
        if request.auto_execute:
            if MOCK_MODE:
                execution_result = await sql_executor.execute_with_mock(sql)
            else:
                execution_result = await sql_executor.execute(sql)

            # 결과 설명 — LLM 미사용, 실제 데이터에서 직접 생성 (할루시네이션 원천 차단)
            if execution_result.results:
                rows = execution_result.results
                cols = execution_result.columns
                def _fmt(v):
                    if v is None: return "없음"
                    if isinstance(v, float): return f"{v:,.1f}" if abs(v) >= 1000 else f"{v:.1f}"
                    if isinstance(v, int): return f"{v:,}"
                    return str(v)
                if len(rows) == 1 and len(cols) == 1:
                    execution_result.natural_language_explanation = f"「{request.question}」 조회 결과: {_fmt(rows[0][0])}"
                elif len(rows) == 1 and len(cols) <= 5:
                    parts = [f"{cols[i]}={_fmt(rows[0][i])}" for i in range(len(cols))]
                    execution_result.natural_language_explanation = f"「{request.question}」 조회 결과: {', '.join(parts)}"
                else:
                    execution_result.natural_language_explanation = f"「{request.question}」 {len(rows)}건 조회 완료"

        # AAR-001: Auto query logging
        if QUERY_LOG_ENABLED:
            asyncio.ensure_future(log_query_to_catalog(
                query_text=request.question,
                query_type="text2sql_enhanced",
                tables_accessed=_extract_tables_from_sql(sql),
                result_count=execution_result.row_count if execution_result else 0,
            ))

        return EnhancedText2SQLResponse(
            original_question=request.question,
            enhanced_question=enhanced_question,
            enhancements_applied=enhancements_applied,
            enhancement_confidence=enhancement_confidence,
            sql=sql,
            sql_explanation=sql_explanation if request.include_explanation else None,
            sql_confidence=sql_confidence,
            execution_result=execution_result
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/text2sql/metadata/tables")
async def get_available_tables():
    """사용 가능한 테이블 목록 조회"""
    tables = it_meta_service.get_all_tables()
    return {
        "success": True,
        "tables": [
            {
                "physical_name": t["physical_name"],
                "business_name": t["business_name"],
                "description": t["description"],
                "domain": t["domain"],
                "column_count": len(t["columns"]),
            }
            for t in tables
        ]
    }


@router.get("/text2sql/metadata/icd-codes")
async def search_icd_codes(q: str):
    """ICD 코드 검색"""
    results = biz_meta_service.get_icd_codes_like(q)
    return {
        "success": True,
        "results": [
            {"term": term, "code": code, "name": name}
            for term, code, name in results
        ]
    }


@router.get("/text2sql/metadata/terms")
async def get_standard_terms():
    """표준 의료 용어 매핑 전체 조회"""
    snomed_results = [
        {"term": term, "code": code, "name": name, "codeSystem": "SNOMED CT"}
        for term, (code, name) in biz_meta_service.icd_map.items()
    ]
    standard_results = [
        {"term": term, "description": desc}
        for term, desc in biz_meta_service.standard_terms.items()
    ]
    return {
        "success": True,
        "snomed_codes": snomed_results,
        "standard_terms": standard_results,
    }


@router.post("/text2sql/validate")
async def validate_sql_query(body: dict):
    """SQL 유효성 검증"""
    sql = body.get("sql", "")
    is_valid, error_msg = sql_executor.validate_sql(sql)
    return {
        "success": True,
        "is_valid": is_valid,
        "error": error_msg if not is_valid else None
    }


@router.post("/text2sql/execute")
async def execute_sql_query(body: dict):
    """SQL 실행 (읽기 전용)"""
    sql = body.get("sql", "")

    # 유효성 검증
    is_valid, error_msg = sql_executor.validate_sql(sql)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error_msg)

    # 실행
    if MOCK_MODE:
        result = await sql_executor.execute_with_mock(sql)
    else:
        result = await sql_executor.execute(sql)

    return {
        "success": True,
        "result": result.model_dump()
    }
