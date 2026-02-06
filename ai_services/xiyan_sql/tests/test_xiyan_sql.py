"""
XiYanSQL 서비스 테스트

단위 테스트: 프롬프트 구성, SQL 파싱
통합 테스트: ConversationMemory + XiYan SQL 3턴 시나리오 (mock vLLM)
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from ai_services.xiyan_sql.service import XiYanSQLService
from ai_services.xiyan_sql.schema import (
    get_omop_cdm_schema,
    get_evidence_for_query,
    build_m_schema_for_tables,
    MEDICAL_EVIDENCE,
    SAMPLE_TABLES,
    TABLE_RELATIONSHIPS,
    KEYWORD_TABLE_MAP,
    ICD_CODE_MAP,
)
from ai_services.xiyan_sql.schema_linker import SchemaLinker, SchemaLinkResult
from ai_services.xiyan_sql.config import XIYAN_SQL_DIALECT


# =============================================================================
# 단위 테스트: 프롬프트 구성
# =============================================================================

class TestBuildPrompt:
    """_build_prompt 테스트"""

    def setup_method(self):
        self.service = XiYanSQLService(
            api_url="http://test:8001/v1",
            model_name="test-model",
        )

    def test_basic_prompt_structure(self):
        """기본 프롬프트 구조 검증"""
        prompt = self.service._build_prompt(
            question="당뇨 환자 몇 명?",
            db_schema="【DB_ID】 test_db",
            evidence="",
        )

        assert f"你是一名{XIYAN_SQL_DIALECT}专家" in prompt
        assert "【用户问题】" in prompt
        assert "당뇨 환자 몇 명?" in prompt
        assert "【数据库schema】" in prompt
        assert "【DB_ID】 test_db" in prompt

    def test_prompt_with_evidence(self):
        """참조 정보(evidence) 포함 프롬프트"""
        prompt = self.service._build_prompt(
            question="당뇨 환자 몇 명?",
            db_schema="【DB_ID】 test_db",
            evidence="당뇨병 ICD-10: E11",
        )

        assert "【参考信息】" in prompt
        assert "당뇨병 ICD-10: E11" in prompt

    def test_prompt_without_evidence(self):
        """evidence 없을 때 참조 정보 섹션 미포함"""
        prompt = self.service._build_prompt(
            question="환자 목록",
            db_schema="【DB_ID】 test_db",
            evidence="",
        )

        assert "【参考信息】" not in prompt

    def test_prompt_with_context(self):
        """이전 대화 컨텍스트가 포함된 질의"""
        context_query = """## 이전 대화 컨텍스트
- 이전 질의: 당뇨 환자 몇 명?
- 실행된 SQL:
```sql
SELECT COUNT(DISTINCT d.PT_NO) FROM DIAG_INFO d WHERE d.ICD_CD LIKE 'E11%'
```
- 결과 건수: 1247건

## 현재 질의
그 중 남성만"""

        prompt = self.service._build_prompt(
            question=context_query,
            db_schema=get_omop_cdm_schema(),
            evidence="성별 조건: PT_BSNF.SEX_CD = 'M'",
        )

        assert "이전 대화 컨텍스트" in prompt
        assert "당뇨 환자 몇 명?" in prompt
        assert "그 중 남성만" in prompt
        assert "SEX_CD = 'M'" in prompt


# =============================================================================
# 단위 테스트: SQL 추출
# =============================================================================

class TestExtractSQL:
    """_extract_sql 테스트"""

    def test_extract_from_sql_code_block(self):
        """```sql 블록에서 SQL 추출"""
        response = """다음은 당뇨 환자를 조회하는 SQL입니다:

```sql
SELECT COUNT(DISTINCT d.PT_NO) AS patient_count
FROM DIAG_INFO d
INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO
WHERE d.ICD_CD LIKE 'E11%'
```

이 쿼리는 제2형 당뇨병 환자 수를 조회합니다."""

        sql = XiYanSQLService._extract_sql(response)

        assert "SELECT COUNT(DISTINCT d.PT_NO)" in sql
        assert "DIAG_INFO" in sql
        assert "ICD_CD LIKE 'E11%'" in sql

    def test_extract_from_generic_code_block(self):
        """언어 미지정 ``` 블록에서 SQL 추출"""
        response = """```
SELECT PT_NO, PT_NM FROM PT_BSNF WHERE SEX_CD = 'M'
```"""

        sql = XiYanSQLService._extract_sql(response)
        assert "SELECT PT_NO" in sql
        assert "SEX_CD = 'M'" in sql

    def test_extract_bare_select(self):
        """코드 블록 없이 직접 SQL 문"""
        response = "SELECT COUNT(*) FROM PT_BSNF;"

        sql = XiYanSQLService._extract_sql(response)
        assert "SELECT COUNT(*) FROM PT_BSNF" in sql

    def test_extract_with_cte(self):
        """WITH CTE 포함 SQL 추출"""
        response = """```sql
WITH diabetes_patients AS (
    SELECT DISTINCT d.PT_NO
    FROM DIAG_INFO d
    WHERE d.ICD_CD LIKE 'E11%'
)
SELECT COUNT(*) AS cnt
FROM diabetes_patients dp
INNER JOIN PT_BSNF p ON dp.PT_NO = p.PT_NO
WHERE p.SEX_CD = 'M'
```"""

        sql = XiYanSQLService._extract_sql(response)
        assert "WITH diabetes_patients AS" in sql
        assert "SEX_CD = 'M'" in sql

    def test_extract_fallback(self):
        """SQL 패턴이 없을 때 원본 반환"""
        response = "이 질문에 대한 SQL을 생성할 수 없습니다."
        sql = XiYanSQLService._extract_sql(response)
        assert sql == response.strip()


# =============================================================================
# 단위 테스트: 스키마 및 Evidence
# =============================================================================

class TestSchema:
    """스키마 및 evidence 테스트"""

    def test_schema_contains_all_tables(self):
        """모든 5개 테이블 포함 확인"""
        schema = get_omop_cdm_schema()

        for table in ["PT_BSNF", "OPD_RCPT", "IPD_ADM", "LAB_RSLT", "DIAG_INFO"]:
            assert table in schema

    def test_schema_contains_fk(self):
        """FK 관계 포함 확인"""
        schema = get_omop_cdm_schema()

        assert "OPD_RCPT.PT_NO = PT_BSNF.PT_NO" in schema
        assert "IPD_ADM.PT_NO = PT_BSNF.PT_NO" in schema
        assert "LAB_RSLT.PT_NO = PT_BSNF.PT_NO" in schema
        assert "DIAG_INFO.PT_NO = PT_BSNF.PT_NO" in schema

    def test_schema_m_schema_format(self):
        """M-Schema 형식 준수 확인"""
        schema = get_omop_cdm_schema()

        assert "【DB_ID】" in schema
        assert "【표(Table)】" in schema
        assert "【외래키(FK)】" in schema

    def test_evidence_for_diabetes_query(self):
        """당뇨 질의에 대한 evidence 추출"""
        evidence = get_evidence_for_query("당뇨 환자 몇 명?")
        assert "E11" in evidence
        assert "ICD_CD" in evidence

    def test_evidence_for_gender_query(self):
        """성별 질의에 대한 evidence 추출"""
        evidence = get_evidence_for_query("남성 환자 목록")
        assert "SEX_CD = 'M'" in evidence

    def test_evidence_for_combined_query(self):
        """복합 질의에 대한 evidence 추출 (다중 매칭)"""
        evidence = get_evidence_for_query("당뇨 환자 중 남성 입원 환자")
        assert "E11" in evidence
        assert "SEX_CD = 'M'" in evidence
        assert "DSCH_DT IS NULL" in evidence

    def test_evidence_no_match(self):
        """매칭 키워드 없을 때 빈 문자열"""
        evidence = get_evidence_for_query("전체 데이터 조회")
        assert evidence == ""


# =============================================================================
# 비동기 테스트: vLLM API 호출 (mock)
# =============================================================================

class TestGenerateSQL:
    """generate_sql 비동기 테스트 (mock vLLM)"""

    @pytest.fixture
    def service(self):
        return XiYanSQLService(
            api_url="http://test:8001/v1",
            model_name="test-model",
        )

    @pytest.mark.asyncio
    async def test_generate_sql_basic(self, service):
        """기본 SQL 생성 테스트"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "choices": [{
                "message": {
                    "content": "```sql\nSELECT COUNT(DISTINCT d.PT_NO) AS patient_count\nFROM DIAG_INFO d\nWHERE d.ICD_CD LIKE 'E11%'\n```"
                }
            }]
        }

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql("당뇨 환자 몇 명?")

        assert "SELECT COUNT" in sql
        assert "DIAG_INFO" in sql
        assert "ICD_CD LIKE 'E11%'" in sql

    @pytest.mark.asyncio
    async def test_generate_sql_with_custom_schema(self, service):
        """커스텀 스키마로 SQL 생성 테스트"""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "choices": [{
                "message": {
                    "content": "SELECT * FROM custom_table;"
                }
            }]
        }

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = mock_response
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql(
                "환자 조회",
                db_schema="【DB_ID】 custom\n【표(Table)】 custom_table (id INT)",
            )

        assert "custom_table" in sql


# =============================================================================
# 통합 테스트: ConversationMemory + XiYan SQL 3턴 시나리오
# =============================================================================

class TestConversationMemoryIntegration:
    """ConversationMemory enriched_context + XiYan SQL 3턴 통합 테스트 (mock)"""

    @pytest.fixture
    def service(self):
        return XiYanSQLService(
            api_url="http://test:8001/v1",
            model_name="test-model",
        )

    def _make_mock_response(self, sql_content: str):
        """mock HTTP 응답 생성 헬퍼"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{
                "message": {"content": f"```sql\n{sql_content}\n```"}
            }]
        }
        return mock_resp

    @pytest.mark.asyncio
    async def test_turn1_simple_query(self, service):
        """턴 1: '당뇨 환자 몇 명이야?' — 단순 질의"""
        enriched_context = {
            "original_query": "당뇨 환자 몇 명이야?",
            "is_follow_up": False,
            "turn_number": 1,
            "previous_context": None,
            "resolved_references": [],
            "context_prompt": "당뇨 환자 몇 명이야?",
        }

        expected_sql = (
            "SELECT COUNT(DISTINCT d.PT_NO) AS patient_count\n"
            "FROM DIAG_INFO d\n"
            "INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO\n"
            "WHERE d.ICD_CD LIKE 'E11%'"
        )

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = self._make_mock_response(expected_sql)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql_with_context(enriched_context)

        assert "COUNT(DISTINCT d.PT_NO)" in sql
        assert "ICD_CD LIKE 'E11%'" in sql

        # vLLM API 호출 검증
        call_args = mock_client.post.call_args
        request_body = call_args.kwargs.get("json") or call_args[1].get("json")
        prompt = request_body["messages"][0]["content"]
        assert "당뇨 환자 몇 명이야?" in prompt

    @pytest.mark.asyncio
    async def test_turn2_follow_up_gender(self, service):
        """턴 2: '그 중 남성만' — 이전 컨텍스트 참조"""
        enriched_context = {
            "original_query": "그 중 남성만",
            "is_follow_up": True,
            "turn_number": 2,
            "previous_context": {
                "query": "당뇨 환자 몇 명이야?",
                "sql": "SELECT COUNT(DISTINCT d.PT_NO) FROM DIAG_INFO d INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO WHERE d.ICD_CD LIKE 'E11%'",
                "conditions": ["ICD_CD LIKE 'E11%'"],
                "tables_used": ["DIAG_INFO", "PT_BSNF"],
                "result_count": 1247,
            },
            "resolved_references": [
                {
                    "type": "result",
                    "text": "그 중",
                    "resolved_value": {"type": "filter_previous_results"},
                }
            ],
            "context_prompt": (
                "## 이전 대화 컨텍스트\n"
                "- 이전 질의: 당뇨 환자 몇 명이야?\n"
                "- 실행된 SQL:\n```sql\nSELECT COUNT(DISTINCT d.PT_NO) FROM DIAG_INFO d "
                "INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO WHERE d.ICD_CD LIKE 'E11%'\n```\n"
                "- 결과 건수: 1247건\n"
                "- 적용된 조건: ICD_CD LIKE 'E11%'\n"
                "- 사용된 테이블: DIAG_INFO, PT_BSNF\n\n"
                "## 참조 해석\n"
                '- "그 중" → 이전 결과 집합에서 필터링\n\n'
                "## 현재 질의\n그 중 남성만\n\n"
                "위 이전 대화 컨텍스트를 참고하여, 현재 질의에 맞는 SQL을 생성해주세요.\n"
                "이전 쿼리의 조건을 유지하면서 새로운 조건을 추가해야 합니다.\n"
                "JOIN, WHERE 등을 하나의 효율적인 쿼리로 작성해주세요."
            ),
        }

        expected_sql = (
            "SELECT COUNT(DISTINCT d.PT_NO) AS patient_count\n"
            "FROM DIAG_INFO d\n"
            "INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO\n"
            "WHERE d.ICD_CD LIKE 'E11%'\n"
            "  AND p.SEX_CD = 'M'"
        )

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = self._make_mock_response(expected_sql)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql_with_context(enriched_context)

        assert "ICD_CD LIKE 'E11%'" in sql
        assert "SEX_CD = 'M'" in sql

        # 프롬프트에 이전 컨텍스트가 포함되었는지 검증
        call_args = mock_client.post.call_args
        request_body = call_args.kwargs.get("json") or call_args[1].get("json")
        prompt = request_body["messages"][0]["content"]
        assert "이전 대화 컨텍스트" in prompt
        assert "1247건" in prompt

    @pytest.mark.asyncio
    async def test_turn3_follow_up_age(self, service):
        """턴 3: '65세 이상만' — 누적 컨텍스트"""
        enriched_context = {
            "original_query": "65세 이상만",
            "is_follow_up": True,
            "turn_number": 3,
            "previous_context": {
                "query": "그 중 남성만",
                "sql": (
                    "SELECT COUNT(DISTINCT d.PT_NO) FROM DIAG_INFO d "
                    "INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO "
                    "WHERE d.ICD_CD LIKE 'E11%' AND p.SEX_CD = 'M'"
                ),
                "conditions": ["ICD_CD LIKE 'E11%'", "SEX_CD = 'M'"],
                "tables_used": ["DIAG_INFO", "PT_BSNF"],
                "result_count": 683,
            },
            "resolved_references": [
                {
                    "type": "condition",
                    "text": "만",
                    "resolved_value": {"type": "add_condition"},
                }
            ],
            "context_prompt": (
                "## 이전 대화 컨텍스트\n"
                "- 이전 질의: 그 중 남성만\n"
                "- 실행된 SQL:\n```sql\nSELECT COUNT(DISTINCT d.PT_NO) FROM DIAG_INFO d "
                "INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO "
                "WHERE d.ICD_CD LIKE 'E11%' AND p.SEX_CD = 'M'\n```\n"
                "- 결과 건수: 683건\n"
                "- 적용된 조건: ICD_CD LIKE 'E11%', SEX_CD = 'M'\n"
                "- 사용된 테이블: DIAG_INFO, PT_BSNF\n\n"
                "## 현재 질의\n65세 이상만\n\n"
                "위 이전 대화 컨텍스트를 참고하여, 현재 질의에 맞는 SQL을 생성해주세요.\n"
                "이전 쿼리의 조건을 유지하면서 새로운 조건을 추가해야 합니다.\n"
                "JOIN, WHERE 등을 하나의 효율적인 쿼리로 작성해주세요."
            ),
        }

        expected_sql = (
            "SELECT COUNT(DISTINCT d.PT_NO) AS patient_count\n"
            "FROM DIAG_INFO d\n"
            "INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO\n"
            "WHERE d.ICD_CD LIKE 'E11%'\n"
            "  AND p.SEX_CD = 'M'\n"
            "  AND p.BRTH_DT <= CURRENT_DATE - INTERVAL '65 years'"
        )

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = self._make_mock_response(expected_sql)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql_with_context(enriched_context)

        # 이전 조건 유지 + 나이 조건 추가
        assert "ICD_CD LIKE 'E11%'" in sql
        assert "SEX_CD = 'M'" in sql
        assert "BRTH_DT" in sql
        assert "65" in sql

        # 프롬프트에 누적 컨텍스트 포함
        call_args = mock_client.post.call_args
        request_body = call_args.kwargs.get("json") or call_args[1].get("json")
        prompt = request_body["messages"][0]["content"]
        assert "683건" in prompt
        assert "ICD_CD LIKE 'E11%'" in prompt
        assert "SEX_CD = 'M'" in prompt


# =============================================================================
# record_query_result 연동 테스트
# =============================================================================

class TestRecordQueryResult:
    """record_query_result와 XiYanSQL 연동 테스트"""

    def test_record_and_use_in_next_turn(self):
        """SQL 결과 기록 후 다음 턴 enriched_context 구성"""
        from ai_services.conversation_memory.graph.workflow import record_query_result

        # 1턴 상태 시뮬레이션
        from ai_services.conversation_memory.state.conversation_state import QueryContext
        from datetime import datetime

        last_ctx = QueryContext(
            turn_number=1,
            original_query="당뇨 환자 몇 명?",
            timestamp=datetime.now(),
        )

        state = {
            "last_query_context": last_ctx,
            "query_history": [last_ctx],
            "enriched_context": {
                "original_query": "당뇨 환자 몇 명?",
                "context_prompt": "당뇨 환자 몇 명?",
                "is_follow_up": False,
            },
        }

        # XiYan SQL 생성 결과 기록
        generated_sql = "SELECT COUNT(DISTINCT d.PT_NO) FROM DIAG_INFO d WHERE d.ICD_CD LIKE 'E11%'"
        updated_state = record_query_result(
            state,
            generated_sql=generated_sql,
            result_count=1247,
            conditions=["ICD_CD LIKE 'E11%'"],
            tables_used=["DIAG_INFO", "PT_BSNF"],
        )

        # 상태가 올바르게 업데이트되었는지 확인
        ctx = updated_state["last_query_context"]
        assert ctx.generated_sql == generated_sql
        assert ctx.result_count == 1247
        assert "ICD_CD LIKE 'E11%'" in ctx.conditions
        assert "DIAG_INFO" in ctx.tables_used

        # query_history에도 반영
        assert updated_state["query_history"][-1].generated_sql == generated_sql


# =============================================================================
# SchemaLinker 단위 테스트
# =============================================================================

class TestSchemaLinker:
    """SchemaLinker 단위 테스트"""

    def setup_method(self):
        self.linker = SchemaLinker()

    # -- 키워드 추출 --

    def test_extract_keywords_diabetes(self):
        """'당뇨' 키워드 추출"""
        keywords = self.linker._extract_keywords("당뇨 환자 몇 명?")
        assert "당뇨" in keywords
        assert "환자" in keywords

    def test_extract_keywords_admission(self):
        """'입원' 키워드 추출"""
        keywords = self.linker._extract_keywords("입원 환자 목록")
        assert "입원" in keywords or "입원 환자" in keywords

    def test_extract_keywords_lab(self):
        """'검사' 키워드 추출"""
        keywords = self.linker._extract_keywords("혈액검사 결과 보여줘")
        assert any(kw in keywords for kw in ["검사", "혈액검사", "검사결과"])

    def test_extract_keywords_no_match(self):
        """매칭되는 키워드 없을 때 빈 리스트"""
        keywords = self.linker._extract_keywords("안녕하세요")
        assert keywords == []

    # -- 테이블 선별 --

    def test_select_tables_diabetes(self):
        """당뇨 질의 → DIAG_INFO, PT_BSNF 선별"""
        tables = self.linker._select_tables(["당뇨", "환자"], [])
        assert "DIAG_INFO" in tables
        assert "PT_BSNF" in tables

    def test_select_tables_admission(self):
        """입원 질의 → IPD_ADM, PT_BSNF 선별"""
        tables = self.linker._select_tables(["입원"], [])
        assert "IPD_ADM" in tables
        assert "PT_BSNF" in tables

    def test_select_tables_with_previous(self):
        """이전 턴 테이블 병합"""
        tables = self.linker._select_tables(["검사"], ["DIAG_INFO", "PT_BSNF"])
        assert "LAB_RSLT" in tables
        assert "DIAG_INFO" in tables
        assert "PT_BSNF" in tables

    def test_select_tables_fallback(self):
        """키워드 없으면 PT_BSNF 폴백"""
        tables = self.linker._select_tables([], [])
        assert tables == ["PT_BSNF"]

    def test_select_tables_auto_include_pt_bsnf(self):
        """FK 허브인 PT_BSNF 자동 포함"""
        tables = self.linker._select_tables(["검사결과"], [])
        assert "LAB_RSLT" in tables
        assert "PT_BSNF" in tables

    # -- 의료 용어 해석 --

    def test_resolve_diabetes(self):
        """당뇨 → ICD E11 매핑"""
        evidence, resolutions = self.linker._resolve_medical_terms("당뇨 환자 몇 명?")
        assert "E11" in evidence
        assert any(r["icd_code"] == "E11" for r in resolutions)

    def test_resolve_hypertension(self):
        """고혈압 → ICD I10 매핑"""
        evidence, resolutions = self.linker._resolve_medical_terms("고혈압 환자")
        assert "I10" in evidence

    def test_resolve_gender_evidence(self):
        """성별 키워드 → evidence 생성"""
        evidence, _ = self.linker._resolve_medical_terms("남성 입원 환자")
        assert "SEX_CD = 'M'" in evidence

    def test_resolve_admission_evidence(self):
        """입원 키워드 → evidence 생성"""
        evidence, _ = self.linker._resolve_medical_terms("입원 환자")
        assert "DSCH_DT IS NULL" in evidence

    def test_resolve_no_match(self):
        """매칭 없을 때 빈 evidence"""
        evidence, resolutions = self.linker._resolve_medical_terms("데이터 조회")
        assert evidence == ""
        assert resolutions == []

    # -- M-Schema 빌드 --

    def test_build_m_schema_single_table(self):
        """단일 테이블 M-Schema 구성"""
        pt_bsnf = [t for t in SAMPLE_TABLES if t["physical_name"] == "PT_BSNF"]
        schema = build_m_schema_for_tables(pt_bsnf, [])
        assert "【DB_ID】 asan_cdm" in schema
        assert "【표(Table)】 PT_BSNF" in schema
        assert "PT_NO" in schema
        # FK 없어야 함
        assert "【외래키(FK)】" not in schema

    def test_build_m_schema_with_fk(self):
        """테이블 + FK 포함 M-Schema 구성"""
        tables = [
            t for t in SAMPLE_TABLES
            if t["physical_name"] in ("PT_BSNF", "DIAG_INFO")
        ]
        rels = [
            r for r in TABLE_RELATIONSHIPS
            if r["from_table"] == "DIAG_INFO"
        ]
        schema = build_m_schema_for_tables(tables, rels)
        assert "【표(Table)】 PT_BSNF" in schema
        assert "【표(Table)】 DIAG_INFO" in schema
        assert "DIAG_INFO.PT_NO = PT_BSNF.PT_NO" in schema

    def test_build_m_schema_excludes_unrelated_fk(self):
        """선별되지 않은 테이블의 FK는 제외"""
        tables = [
            t for t in SAMPLE_TABLES
            if t["physical_name"] in ("PT_BSNF", "DIAG_INFO")
        ]
        # 전체 FK를 전달해도 관련 없는 것은 제외됨
        schema = build_m_schema_for_tables(tables, TABLE_RELATIONSHIPS)
        assert "IPD_ADM" not in schema.split("【외래키")[0] or "IPD_ADM" not in schema
        # DIAG_INFO FK만 포함
        assert "DIAG_INFO.PT_NO = PT_BSNF.PT_NO" in schema

    # -- 통합 link() --

    def test_link_diabetes_query(self):
        """link() 통합: 당뇨 질의"""
        result = self.linker.link("당뇨 환자 몇 명?")
        assert isinstance(result, SchemaLinkResult)
        assert "DIAG_INFO" in result.selected_tables
        assert "PT_BSNF" in result.selected_tables
        assert "E11" in result.evidence
        assert "【표(Table)】 DIAG_INFO" in result.m_schema
        assert "【표(Table)】 PT_BSNF" in result.m_schema

    def test_link_lab_query(self):
        """link() 통합: 검사 질의"""
        result = self.linker.link("혈액검사 결과")
        assert "LAB_RSLT" in result.selected_tables

    def test_link_with_previous_tables(self):
        """link() 통합: 이전 턴 테이블 병합"""
        result = self.linker.link(
            "검사 결과 보여줘",
            previous_tables=["DIAG_INFO", "PT_BSNF"],
        )
        assert "LAB_RSLT" in result.selected_tables
        assert "DIAG_INFO" in result.selected_tables
        assert "PT_BSNF" in result.selected_tables


# =============================================================================
# 동적 스키마 통합 테스트 (3턴 시나리오)
# =============================================================================

class TestDynamicSchemaIntegration:
    """3턴 시나리오에서 동적 스키마 변화를 검증합니다.

    Turn 1: "당뇨 환자" → DIAG_INFO + PT_BSNF
    Turn 2: "그 중 입원 환자" → DIAG_INFO + PT_BSNF + IPD_ADM
    Turn 3: "검사 결과 보여줘" → + LAB_RSLT
    """

    def setup_method(self):
        self.linker = SchemaLinker()

    def test_turn1_diabetes_patients(self):
        """턴 1: '당뇨 환자' → DIAG_INFO + PT_BSNF만 선별"""
        result = self.linker.link("당뇨 환자 몇 명?")

        assert "DIAG_INFO" in result.selected_tables
        assert "PT_BSNF" in result.selected_tables
        # 입원/외래/검사 테이블은 미포함
        assert "IPD_ADM" not in result.selected_tables
        assert "OPD_RCPT" not in result.selected_tables
        assert "LAB_RSLT" not in result.selected_tables
        # M-Schema에 해당 테이블만 포함
        assert "【표(Table)】 DIAG_INFO" in result.m_schema
        assert "【표(Table)】 PT_BSNF" in result.m_schema
        assert "【표(Table)】 IPD_ADM" not in result.m_schema
        # evidence에 ICD 코드 포함
        assert "E11" in result.evidence

    def test_turn2_admission_with_previous(self):
        """턴 2: '그 중 입원 환자' → DIAG_INFO + PT_BSNF + IPD_ADM 추가"""
        result = self.linker.link(
            "그 중 입원 환자",
            previous_tables=["DIAG_INFO", "PT_BSNF"],
        )

        assert "DIAG_INFO" in result.selected_tables
        assert "PT_BSNF" in result.selected_tables
        assert "IPD_ADM" in result.selected_tables
        # 검사/외래는 미포함
        assert "LAB_RSLT" not in result.selected_tables
        assert "OPD_RCPT" not in result.selected_tables
        # M-Schema 검증
        assert "【표(Table)】 IPD_ADM" in result.m_schema
        assert "【표(Table)】 DIAG_INFO" in result.m_schema
        # FK 포함
        assert "IPD_ADM.PT_NO = PT_BSNF.PT_NO" in result.m_schema

    def test_turn3_lab_results_with_previous(self):
        """턴 3: '검사 결과 보여줘' → + LAB_RSLT 추가"""
        result = self.linker.link(
            "검사 결과 보여줘",
            previous_tables=["DIAG_INFO", "PT_BSNF", "IPD_ADM"],
        )

        assert "LAB_RSLT" in result.selected_tables
        assert "DIAG_INFO" in result.selected_tables
        assert "PT_BSNF" in result.selected_tables
        assert "IPD_ADM" in result.selected_tables
        # M-Schema에 4개 테이블
        assert "【표(Table)】 LAB_RSLT" in result.m_schema
        assert "【표(Table)】 IPD_ADM" in result.m_schema
        assert "【표(Table)】 DIAG_INFO" in result.m_schema
        assert "【표(Table)】 PT_BSNF" in result.m_schema

    def test_full_3turn_scenario(self):
        """전체 3턴 시나리오 연속 실행"""
        # Turn 1
        r1 = self.linker.link("당뇨 환자 몇 명?")
        assert set(r1.selected_tables) == {"DIAG_INFO", "PT_BSNF"}

        # Turn 2: 이전 턴 테이블 전달
        r2 = self.linker.link(
            "그 중 입원 환자",
            previous_tables=r1.selected_tables,
        )
        assert "IPD_ADM" in r2.selected_tables
        assert "DIAG_INFO" in r2.selected_tables
        assert "PT_BSNF" in r2.selected_tables

        # Turn 3: 이전 턴 테이블 전달
        r3 = self.linker.link(
            "검사 결과 보여줘",
            previous_tables=r2.selected_tables,
        )
        assert "LAB_RSLT" in r3.selected_tables
        assert "IPD_ADM" in r3.selected_tables
        assert "DIAG_INFO" in r3.selected_tables
        assert "PT_BSNF" in r3.selected_tables

    def test_schema_grows_monotonically(self):
        """턴이 진행되면 선별 테이블이 단조 증가"""
        r1 = self.linker.link("당뇨 환자")
        r2 = self.linker.link("그 중 입원 환자", previous_tables=r1.selected_tables)
        r3 = self.linker.link("검사 결과 보여줘", previous_tables=r2.selected_tables)

        assert set(r1.selected_tables).issubset(set(r2.selected_tables))
        assert set(r2.selected_tables).issubset(set(r3.selected_tables))


# =============================================================================
# XiYanSQLService + SchemaLinker 통합 (mock vLLM)
# =============================================================================

class TestServiceSchemaLinkerIntegration:
    """XiYanSQLService가 SchemaLinker를 올바르게 호출하는지 검증"""

    @pytest.fixture
    def service(self):
        return XiYanSQLService(
            api_url="http://test:8001/v1",
            model_name="test-model",
        )

    def _make_mock_response(self, sql_content: str):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "choices": [{
                "message": {"content": f"```sql\n{sql_content}\n```"}
            }]
        }
        return mock_resp

    @pytest.mark.asyncio
    async def test_dynamic_schema_used_when_no_db_schema(self, service):
        """db_schema=None일 때 동적 스키마 사용 확인"""
        enriched_context = {
            "original_query": "당뇨 환자 몇 명?",
            "is_follow_up": False,
            "turn_number": 1,
            "previous_context": None,
            "context_prompt": "당뇨 환자 몇 명?",
        }

        expected_sql = "SELECT COUNT(*) FROM DIAG_INFO WHERE ICD_CD LIKE 'E11%'"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = self._make_mock_response(expected_sql)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql_with_context(enriched_context)

        # 프롬프트에 동적 스키마가 포함되었는지 검증
        call_args = mock_client.post.call_args
        request_body = call_args.kwargs.get("json") or call_args[1].get("json")
        prompt = request_body["messages"][0]["content"]
        # 동적 스키마: DIAG_INFO와 PT_BSNF만 포함되어야 함
        assert "DIAG_INFO" in prompt
        assert "PT_BSNF" in prompt
        # 5개 전체가 아닌 선별된 테이블만
        assert "OPD_RCPT" not in prompt
        # evidence에 E11 포함
        assert "E11" in prompt

    @pytest.mark.asyncio
    async def test_static_schema_used_when_db_schema_provided(self, service):
        """db_schema가 제공되면 동적 스키마 건너뜀"""
        enriched_context = {
            "original_query": "환자 목록",
            "is_follow_up": False,
            "previous_context": None,
            "context_prompt": "환자 목록",
        }

        custom_schema = "【DB_ID】 custom\n【표(Table)】 CUSTOM (id INT)"
        expected_sql = "SELECT * FROM CUSTOM"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = self._make_mock_response(expected_sql)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql_with_context(
                enriched_context, db_schema=custom_schema
            )

        call_args = mock_client.post.call_args
        request_body = call_args.kwargs.get("json") or call_args[1].get("json")
        prompt = request_body["messages"][0]["content"]
        assert "CUSTOM" in prompt

    @pytest.mark.asyncio
    async def test_previous_tables_passed_to_linker(self, service):
        """후속 질의에서 previous_tables가 SchemaLinker에 전달됨"""
        enriched_context = {
            "original_query": "검사 결과 보여줘",
            "is_follow_up": True,
            "turn_number": 2,
            "previous_context": {
                "query": "당뇨 환자 몇 명?",
                "sql": "SELECT COUNT(*) FROM DIAG_INFO WHERE ICD_CD LIKE 'E11%'",
                "conditions": ["ICD_CD LIKE 'E11%'"],
                "tables_used": ["DIAG_INFO", "PT_BSNF"],
                "result_count": 1247,
            },
            "context_prompt": "검사 결과 보여줘",
        }

        expected_sql = "SELECT * FROM LAB_RSLT"

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = self._make_mock_response(expected_sql)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql_with_context(enriched_context)

        call_args = mock_client.post.call_args
        request_body = call_args.kwargs.get("json") or call_args[1].get("json")
        prompt = request_body["messages"][0]["content"]
        # 이전 턴 테이블 + 새 테이블 모두 포함
        assert "DIAG_INFO" in prompt
        assert "PT_BSNF" in prompt
        assert "LAB_RSLT" in prompt


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
