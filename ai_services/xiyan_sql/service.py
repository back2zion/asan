"""
XiYanSQL-QwenCoder-7B 서비스

vLLM OpenAI-compatible API를 통해 XiYanSQL 모델로 NL→SQL 변환을 수행합니다.
ConversationMemory의 enriched_context를 입력으로 받아 효율적인 SQL을 생성합니다.
"""

import re
import asyncio
from typing import Optional

import httpx

from ai_services.xiyan_sql.config import (
    XIYAN_SQL_API_URL,
    XIYAN_SQL_MODEL,
    XIYAN_SQL_TIMEOUT,
    XIYAN_SQL_DIALECT,
    XIYAN_SQL_TEMPERATURE,
    XIYAN_SQL_MAX_TOKENS,
)
from ai_services.xiyan_sql.schema import (
    get_omop_cdm_schema,
    get_evidence_for_query,
)
from ai_services.xiyan_sql.schema_linker import SchemaLinker


class XiYanSQLService:
    """XiYanSQL-QwenCoder-7B vLLM 서비스.

    vLLM의 OpenAI-compatible API를 호출하여 자연어 질의를
    SQL로 변환합니다.

    Attributes:
        api_url: vLLM API base URL
        model: 모델명
    """

    def __init__(
        self,
        api_url: str = XIYAN_SQL_API_URL,
        model_name: str = XIYAN_SQL_MODEL,
    ):
        self.api_url = api_url.rstrip("/")
        self.model = model_name
        self.schema_linker = SchemaLinker()

    async def generate_sql(
        self,
        question: str,
        db_schema: Optional[str] = None,
        evidence: str = "",
    ) -> str:
        """자연어 질의에서 SQL을 생성합니다.

        Args:
            question: 사용자 자연어 질의
            db_schema: M-Schema 형식 DB 스키마 (미지정 시 OMOP CDM 기본 스키마)
            evidence: 참조 정보 (ICD 코드 매핑, 의료 용어 등)

        Returns:
            생성된 SQL 문자열
        """
        if db_schema is None:
            db_schema = get_omop_cdm_schema()

        if not evidence:
            evidence = get_evidence_for_query(question)

        prompt = self._build_prompt(question, db_schema, evidence)
        response = await self._call_vllm(prompt)
        return self._extract_sql(response)

    async def generate_sql_with_context(
        self,
        enriched_context: dict,
        db_schema: Optional[str] = None,
    ) -> str:
        """ConversationMemory enriched_context 기반 SQL 생성.

        enriched_context["context_prompt"]에는 이전 대화 컨텍스트가
        포함되어 있어, 모델이 "그 중 남성만" 같은 후속 질의에서
        이전 SQL 조건을 유지한 효율적인 쿼리를 생성할 수 있습니다.

        Args:
            enriched_context: ConversationMemory가 생성한 enriched_context
                - original_query: 현재 질의
                - context_prompt: 이전 컨텍스트 포함 프롬프트
                - is_follow_up: 후속 질의 여부
                - previous_context: 이전 턴 정보
            db_schema: M-Schema 형식 DB 스키마 (미지정 시 OMOP CDM 기본 스키마)

        Returns:
            생성된 SQL 문자열
        """
        context_prompt = enriched_context.get("context_prompt", "")
        original_query = enriched_context.get("original_query", "")

        # context_prompt가 있으면 이전 컨텍스트 포함 질의 사용
        question = context_prompt if context_prompt else original_query

        if db_schema is None:
            # 동적 스키마 선별
            previous_tables = []
            if enriched_context.get("previous_context"):
                previous_tables = enriched_context["previous_context"].get(
                    "tables_used", []
                )

            link_result = self.schema_linker.link(
                query=original_query,
                previous_tables=previous_tables,
            )
            db_schema = link_result.m_schema
            evidence = link_result.evidence
        else:
            # 외부에서 스키마가 제공된 경우 기존 evidence 추출 사용
            evidence = get_evidence_for_query(original_query)

        # 후속 질의면 이전 SQL 정보도 evidence에 추가
        if enriched_context.get("is_follow_up") and enriched_context.get("previous_context"):
            prev = enriched_context["previous_context"]
            if prev.get("sql"):
                evidence += f"\n이전 쿼리 SQL: {prev['sql']}"
            if prev.get("conditions"):
                evidence += f"\n이전 적용 조건: {', '.join(prev['conditions'])}"

        prompt = self._build_prompt(question, db_schema, evidence)
        response = await self._call_vllm(prompt)
        return self._extract_sql(response)

    def generate_sql_sync(
        self,
        question: str,
        db_schema: Optional[str] = None,
        evidence: str = "",
    ) -> str:
        """generate_sql의 동기 래퍼.

        Args:
            question: 사용자 자연어 질의
            db_schema: M-Schema 형식 DB 스키마
            evidence: 참조 정보

        Returns:
            생성된 SQL 문자열
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(
                    asyncio.run,
                    self.generate_sql(question, db_schema, evidence),
                ).result()
        else:
            return asyncio.run(self.generate_sql(question, db_schema, evidence))

    def generate_sql_with_context_sync(
        self,
        enriched_context: dict,
        db_schema: Optional[str] = None,
    ) -> str:
        """generate_sql_with_context의 동기 래퍼.

        Args:
            enriched_context: ConversationMemory enriched_context
            db_schema: M-Schema 형식 DB 스키마

        Returns:
            생성된 SQL 문자열
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                return pool.submit(
                    asyncio.run,
                    self.generate_sql_with_context(enriched_context, db_schema),
                ).result()
        else:
            return asyncio.run(
                self.generate_sql_with_context(enriched_context, db_schema)
            )

    def _build_prompt(self, question: str, db_schema: str, evidence: str) -> str:
        """XiYanSQL-QwenCoder 모델용 프롬프트를 구성합니다.

        XiYanSQL 모델은 중국어 시스템 프롬프트 + M-Schema 형식을
        사용하며, PostgreSQL dialect를 지정합니다.

        Args:
            question: 사용자 질의 (컨텍스트 포함 가능)
            db_schema: M-Schema 형식 스키마
            evidence: 참조 정보

        Returns:
            모델에 전달할 프롬프트 문자열
        """
        dialect = XIYAN_SQL_DIALECT

        evidence_section = ""
        if evidence:
            evidence_section = f"\n\n【参考信息】\n{evidence}"

        return f"""你是一名{dialect}专家，现在需要阅读并理解下面的【数据库schema】描述，然后回答【用户问题】并生成对应的SQL查询语句。

【用户问题】
{question}

【数据库schema】
{db_schema}{evidence_section}"""

    async def _call_vllm(self, prompt: str) -> str:
        """vLLM OpenAI-compatible API를 호출합니다.

        Args:
            prompt: 모델에 전달할 프롬프트

        Returns:
            모델 응답 텍스트

        Raises:
            httpx.HTTPStatusError: HTTP 오류 응답
            httpx.ConnectError: vLLM 서버 연결 실패
        """
        async with httpx.AsyncClient(timeout=float(XIYAN_SQL_TIMEOUT)) as client:
            response = await client.post(
                f"{self.api_url}/chat/completions",
                headers={"Content-Type": "application/json"},
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": XIYAN_SQL_MAX_TOKENS,
                    "temperature": XIYAN_SQL_TEMPERATURE,
                },
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]

    @staticmethod
    def _extract_sql(response: str) -> str:
        """모델 응답에서 SQL 문을 추출합니다.

        ```sql 블록, SELECT/WITH로 시작하는 문장 등 다양한 형식을 처리합니다.

        Args:
            response: 모델 전체 응답 텍스트

        Returns:
            추출된 SQL 문자열 (추출 실패 시 원본 응답의 공백 정리 버전)
        """
        # 1. ```sql ... ``` 블록 추출
        sql_block = re.search(r"```sql\s*(.*?)\s*```", response, re.DOTALL | re.IGNORECASE)
        if sql_block:
            return sql_block.group(1).strip()

        # 2. ``` ... ``` 블록 추출 (언어 미지정)
        code_block = re.search(r"```\s*(.*?)\s*```", response, re.DOTALL)
        if code_block:
            content = code_block.group(1).strip()
            if re.match(r"(?i)(SELECT|WITH|INSERT|UPDATE|DELETE|CREATE)", content):
                return content

        # 3. SELECT/WITH 로 시작하는 SQL 문 추출
        sql_match = re.search(
            r"((?:WITH\s+\w+\s+AS\s*\(.*?\)\s*)?SELECT\s+.*?)(?:;|\Z)",
            response,
            re.DOTALL | re.IGNORECASE,
        )
        if sql_match:
            return sql_match.group(1).strip()

        # 4. 폴백: 응답 전체를 SQL로 간주
        return response.strip()


# 싱글톤 인스턴스
xiyan_sql_service = XiYanSQLService()
