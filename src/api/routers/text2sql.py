"""
Text2SQL API Router - 5-step pipeline implementation
"""
import os
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


router = APIRouter()

# Mock mode 설정 (DB 없이 테스트용)
MOCK_MODE = os.getenv("TEXT2SQL_MOCK_MODE", "true").lower() == "true"


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

            # 결과 설명 생성
            if execution_result.results:
                nl_explanation = await llm_service.explain_results(
                    question=request.question,
                    sql=sql,
                    results=execution_result.results,
                    columns=execution_result.columns
                )
                execution_result.natural_language_explanation = nl_explanation

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
