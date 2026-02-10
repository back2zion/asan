"""
MCP (Model Context Protocol) API — 실제 서비스 연결 버전

Tools:
  - search_catalog: RAG 벡터 검색 (Milvus)
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

from ._mcp_data import MCP_TOOLS, TOOL_HANDLERS

logger = logging.getLogger(__name__)

router = APIRouter()


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
            "vector_db": "Milvus v2.4.0",
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
        handler = TOOL_HANDLERS.get(tool_name)
        if not handler:
            return {"success": False, "error": "Tool not implemented"}
        return await handler(args)
    except Exception as e:
        logger.error(f"MCP tool '{tool_name}' error: {e}")
        return {"success": False, "error": str(e)}
