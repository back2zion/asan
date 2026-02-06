"""
XiYanSQL-QwenCoder-7B NL→SQL 서비스

vLLM 서빙을 통해 XiYanSQL 모델로 자연어→SQL 변환을 수행합니다.
ConversationMemory의 enriched_context와 연동하여 다중 턴 SQL 생성을 지원합니다.
"""

from ai_services.xiyan_sql.service import XiYanSQLService, xiyan_sql_service
from ai_services.xiyan_sql.schema import get_omop_cdm_schema, get_evidence_for_query
from ai_services.xiyan_sql.schema_linker import SchemaLinker, SchemaLinkResult
from ai_services.xiyan_sql.config import (
    XIYAN_SQL_API_URL,
    XIYAN_SQL_MODEL,
    XIYAN_SQL_TIMEOUT,
    XIYAN_SQL_DIALECT,
)

__all__ = [
    "XiYanSQLService",
    "xiyan_sql_service",
    "SchemaLinker",
    "SchemaLinkResult",
    "get_omop_cdm_schema",
    "get_evidence_for_query",
    "XIYAN_SQL_API_URL",
    "XIYAN_SQL_MODEL",
    "XIYAN_SQL_TIMEOUT",
    "XIYAN_SQL_DIALECT",
]

__version__ = "0.1.0"
