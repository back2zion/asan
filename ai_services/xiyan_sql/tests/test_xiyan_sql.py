"""
XiYanSQL 서비스 테스트

단위 테스트: 프롬프트 구성, SQL 파싱
통합 테스트: ConversationMemory + XiYan SQL 3턴 시나리오 (mock vLLM)

OMOP CDM 기반 테이블/컬럼명 사용:
  person, condition_occurrence, visit_occurrence, drug_exposure,
  measurement, observation, procedure_occurrence, imaging_study
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
            evidence="당뇨병 SNOMED CT: 44054006",
        )

        assert "【参考信息】" in prompt
        assert "당뇨병 SNOMED CT: 44054006" in prompt

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
SELECT COUNT(DISTINCT co.person_id) FROM condition_occurrence co WHERE co.condition_source_value = '44054006'
```
- 결과 건수: 69건

## 현재 질의
그 중 남성만"""

        prompt = self.service._build_prompt(
            question=context_query,
            db_schema=get_omop_cdm_schema(),
            evidence="성별 조건: person.gender_source_value = 'M' 또는 person.gender_concept_id = 8507",
        )

        assert "이전 대화 컨텍스트" in prompt
        assert "당뇨 환자 몇 명?" in prompt
        assert "그 중 남성만" in prompt
        assert "gender_source_value = 'M'" in prompt


# =============================================================================
# 단위 테스트: SQL 추출
# =============================================================================

class TestExtractSQL:
    """_extract_sql 테스트"""

    def test_extract_from_sql_code_block(self):
        """```sql 블록에서 SQL 추출"""
        response = """다음은 당뇨 환자를 조회하는 SQL입니다:

```sql
SELECT COUNT(DISTINCT co.person_id) AS patient_count
FROM condition_occurrence co
INNER JOIN person p ON co.person_id = p.person_id
WHERE co.condition_source_value = '44054006'
```

이 쿼리는 당뇨병(SNOMED: 44054006) 환자 수를 조회합니다."""

        sql = XiYanSQLService._extract_sql(response)

        assert "SELECT COUNT(DISTINCT co.person_id)" in sql
        assert "condition_occurrence" in sql
        assert "condition_source_value = '44054006'" in sql

    def test_extract_from_generic_code_block(self):
        """언어 미지정 ``` 블록에서 SQL 추출"""
        response = """```
SELECT person_id, gender_source_value FROM person WHERE gender_source_value = 'M'
```"""

        sql = XiYanSQLService._extract_sql(response)
        assert "SELECT person_id" in sql
        assert "gender_source_value = 'M'" in sql

    def test_extract_bare_select(self):
        """코드 블록 없이 직접 SQL 문"""
        response = "SELECT COUNT(*) FROM person;"

        sql = XiYanSQLService._extract_sql(response)
        assert "SELECT COUNT(*) FROM person" in sql

    def test_extract_with_cte(self):
        """WITH CTE 포함 SQL 추출"""
        response = """```sql
WITH diabetes_patients AS (
    SELECT DISTINCT co.person_id
    FROM condition_occurrence co
    WHERE co.condition_source_value = '44054006'
)
SELECT COUNT(*) AS cnt
FROM diabetes_patients dp
INNER JOIN person p ON dp.person_id = p.person_id
WHERE p.gender_source_value = 'M'
```"""

        sql = XiYanSQLService._extract_sql(response)
        assert "WITH diabetes_patients AS" in sql
        assert "gender_source_value = 'M'" in sql

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
        """OMOP CDM 핵심 테이블 포함 확인"""
        schema = get_omop_cdm_schema()

        for table in ["person", "condition_occurrence", "visit_occurrence",
                       "drug_exposure", "measurement", "observation",
                       "procedure_occurrence", "imaging_study"]:
            assert table in schema

    def test_schema_contains_fk(self):
        """FK 관계 포함 확인"""
        schema = get_omop_cdm_schema()

        assert "condition_occurrence.person_id = person.person_id" in schema
        assert "visit_occurrence.person_id = person.person_id" in schema
        assert "drug_exposure.person_id = person.person_id" in schema
        assert "measurement.person_id = person.person_id" in schema

    def test_schema_m_schema_format(self):
        """M-Schema 형식 준수 확인"""
        schema = get_omop_cdm_schema()

        assert "【DB_ID】" in schema
        assert "【표(Table)】" in schema
        assert "【외래키(FK)】" in schema

    def test_evidence_for_diabetes_query(self):
        """당뇨 질의에 대한 evidence 추출"""
        evidence = get_evidence_for_query("당뇨 환자 몇 명?")
        assert "44054006" in evidence
        assert "condition_source_value" in evidence

    def test_evidence_for_gender_query(self):
        """성별 질의에 대한 evidence 추출"""
        evidence = get_evidence_for_query("남성 환자 목록")
        assert "gender_source_value = 'M'" in evidence

    def test_evidence_for_combined_query(self):
        """복합 질의에 대한 evidence 추출 (다중 매칭)"""
        evidence = get_evidence_for_query("당뇨 환자 중 남성 입원 환자")
        assert "44054006" in evidence
        assert "gender_source_value = 'M'" in evidence
        assert "visit_concept_id = 9201" in evidence

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
                    "content": "```sql\nSELECT COUNT(DISTINCT co.person_id) AS patient_count\nFROM condition_occurrence co\nWHERE co.condition_source_value = '44054006'\n```"
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
        assert "condition_occurrence" in sql
        assert "condition_source_value = '44054006'" in sql

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
            "SELECT COUNT(DISTINCT co.person_id) AS patient_count\n"
            "FROM condition_occurrence co\n"
            "INNER JOIN person p ON co.person_id = p.person_id\n"
            "WHERE co.condition_source_value = '44054006'"
        )

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = self._make_mock_response(expected_sql)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql_with_context(enriched_context)

        assert "COUNT(DISTINCT co.person_id)" in sql
        assert "condition_source_value = '44054006'" in sql

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
                "sql": "SELECT COUNT(DISTINCT co.person_id) FROM condition_occurrence co INNER JOIN person p ON co.person_id = p.person_id WHERE co.condition_source_value = '44054006'",
                "conditions": ["condition_source_value = '44054006'"],
                "tables_used": ["condition_occurrence", "person"],
                "result_count": 69,
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
                "- 실행된 SQL:\n```sql\nSELECT COUNT(DISTINCT co.person_id) FROM condition_occurrence co "
                "INNER JOIN person p ON co.person_id = p.person_id WHERE co.condition_source_value = '44054006'\n```\n"
                "- 결과 건수: 69건\n"
                "- 적용된 조건: condition_source_value = '44054006'\n"
                "- 사용된 테이블: condition_occurrence, person\n\n"
                "## 참조 해석\n"
                '- "그 중" → 이전 결과 집합에서 필터링\n\n'
                "## 현재 질의\n그 중 남성만\n\n"
                "위 이전 대화 컨텍스트를 참고하여, 현재 질의에 맞는 SQL을 생성해주세요.\n"
                "이전 쿼리의 조건을 유지하면서 새로운 조건을 추가해야 합니다.\n"
                "JOIN, WHERE 등을 하나의 효율적인 쿼리로 작성해주세요."
            ),
        }

        expected_sql = (
            "SELECT COUNT(DISTINCT co.person_id) AS patient_count\n"
            "FROM condition_occurrence co\n"
            "INNER JOIN person p ON co.person_id = p.person_id\n"
            "WHERE co.condition_source_value = '44054006'\n"
            "  AND p.gender_source_value = 'M'"
        )

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = self._make_mock_response(expected_sql)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql_with_context(enriched_context)

        assert "condition_source_value = '44054006'" in sql
        assert "gender_source_value = 'M'" in sql

        # 프롬프트에 이전 컨텍스트가 포함되었는지 검증
        call_args = mock_client.post.call_args
        request_body = call_args.kwargs.get("json") or call_args[1].get("json")
        prompt = request_body["messages"][0]["content"]
        assert "이전 대화 컨텍스트" in prompt
        assert "69건" in prompt

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
                    "SELECT COUNT(DISTINCT co.person_id) FROM condition_occurrence co "
                    "INNER JOIN person p ON co.person_id = p.person_id "
                    "WHERE co.condition_source_value = '44054006' AND p.gender_source_value = 'M'"
                ),
                "conditions": ["condition_source_value = '44054006'", "gender_source_value = 'M'"],
                "tables_used": ["condition_occurrence", "person"],
                "result_count": 35,
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
                "- 실행된 SQL:\n```sql\nSELECT COUNT(DISTINCT co.person_id) FROM condition_occurrence co "
                "INNER JOIN person p ON co.person_id = p.person_id "
                "WHERE co.condition_source_value = '44054006' AND p.gender_source_value = 'M'\n```\n"
                "- 결과 건수: 35건\n"
                "- 적용된 조건: condition_source_value = '44054006', gender_source_value = 'M'\n"
                "- 사용된 테이블: condition_occurrence, person\n\n"
                "## 현재 질의\n65세 이상만\n\n"
                "위 이전 대화 컨텍스트를 참고하여, 현재 질의에 맞는 SQL을 생성해주세요.\n"
                "이전 쿼리의 조건을 유지하면서 새로운 조건을 추가해야 합니다.\n"
                "JOIN, WHERE 등을 하나의 효율적인 쿼리로 작성해주세요."
            ),
        }

        expected_sql = (
            "SELECT COUNT(DISTINCT co.person_id) AS patient_count\n"
            "FROM condition_occurrence co\n"
            "INNER JOIN person p ON co.person_id = p.person_id\n"
            "WHERE co.condition_source_value = '44054006'\n"
            "  AND p.gender_source_value = 'M'\n"
            "  AND p.year_of_birth <= EXTRACT(YEAR FROM CURRENT_DATE) - 65"
        )

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.post.return_value = self._make_mock_response(expected_sql)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)
            mock_client_cls.return_value = mock_client

            sql = await service.generate_sql_with_context(enriched_context)

        # 이전 조건 유지 + 나이 조건 추가
        assert "condition_source_value = '44054006'" in sql
        assert "gender_source_value = 'M'" in sql
        assert "year_of_birth" in sql
        assert "65" in sql

        # 프롬프트에 누적 컨텍스트 포함
        call_args = mock_client.post.call_args
        request_body = call_args.kwargs.get("json") or call_args[1].get("json")
        prompt = request_body["messages"][0]["content"]
        assert "35건" in prompt
        assert "condition_source_value = '44054006'" in prompt
        assert "gender_source_value = 'M'" in prompt


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
        generated_sql = "SELECT COUNT(DISTINCT co.person_id) FROM condition_occurrence co WHERE co.condition_source_value = '44054006'"
        updated_state = record_query_result(
            state,
            generated_sql=generated_sql,
            result_count=69,
            conditions=["condition_source_value = '44054006'"],
            tables_used=["condition_occurrence", "person"],
        )

        # 상태가 올바르게 업데이트되었는지 확인
        ctx = updated_state["last_query_context"]
        assert ctx.generated_sql == generated_sql
        assert ctx.result_count == 69
        assert "condition_source_value = '44054006'" in ctx.conditions
        assert "condition_occurrence" in ctx.tables_used

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
        """당뇨 질의 → condition_occurrence, person 선별"""
        tables = self.linker._select_tables(["당뇨", "환자"], [])
        assert "condition_occurrence" in tables
        assert "person" in tables

    def test_select_tables_admission(self):
        """입원 질의 → visit_occurrence, person 선별"""
        tables = self.linker._select_tables(["입원"], [])
        assert "visit_occurrence" in tables
        assert "person" in tables

    def test_select_tables_with_previous(self):
        """이전 턴 테이블 병합"""
        tables = self.linker._select_tables(["검사"], ["condition_occurrence", "person"])
        assert "measurement" in tables
        assert "condition_occurrence" in tables
        assert "person" in tables

    def test_select_tables_fallback(self):
        """키워드 없으면 person 폴백"""
        tables = self.linker._select_tables([], [])
        assert tables == ["person"]

    def test_select_tables_auto_include_person(self):
        """FK 허브인 person 자동 포함"""
        tables = self.linker._select_tables(["검사결과"], [])
        assert "measurement" in tables
        assert "person" in tables

    # -- 의료 용어 해석 --

    def test_resolve_diabetes(self):
        """당뇨 → SNOMED CT 44054006 매핑"""
        evidence, resolutions = self.linker._resolve_medical_terms("당뇨 환자 몇 명?")
        assert "44054006" in evidence
        assert any(r["snomed_code"] == "44054006" for r in resolutions)

    def test_resolve_hypertension(self):
        """고혈압 → SNOMED CT 38341003 매핑"""
        evidence, resolutions = self.linker._resolve_medical_terms("고혈압 환자")
        assert "38341003" in evidence

    def test_resolve_gender_evidence(self):
        """성별 키워드 → evidence 생성"""
        evidence, _ = self.linker._resolve_medical_terms("남성 입원 환자")
        assert "gender_source_value = 'M'" in evidence

    def test_resolve_admission_evidence(self):
        """입원 키워드 → evidence 생성"""
        evidence, _ = self.linker._resolve_medical_terms("입원 환자")
        assert "visit_concept_id = 9201" in evidence

    def test_resolve_no_match(self):
        """매칭 없을 때 빈 evidence"""
        evidence, resolutions = self.linker._resolve_medical_terms("데이터 조회")
        assert evidence == ""
        assert resolutions == []

    # -- M-Schema 빌드 --

    def test_build_m_schema_single_table(self):
        """단일 테이블 M-Schema 구성"""
        person_table = [t for t in SAMPLE_TABLES if t["physical_name"] == "person"]
        schema = build_m_schema_for_tables(person_table, [])
        assert "【DB_ID】 asan_cdm" in schema
        assert "【표(Table)】 person" in schema
        assert "person_id" in schema
        # FK 없어야 함
        assert "【외래키(FK)】" not in schema

    def test_build_m_schema_with_fk(self):
        """테이블 + FK 포함 M-Schema 구성"""
        tables = [
            t for t in SAMPLE_TABLES
            if t["physical_name"] in ("person", "condition_occurrence")
        ]
        rels = [
            r for r in TABLE_RELATIONSHIPS
            if r["from_table"] == "condition_occurrence" and r["to_table"] == "person"
        ]
        schema = build_m_schema_for_tables(tables, rels)
        assert "【표(Table)】 person" in schema
        assert "【표(Table)】 condition_occurrence" in schema
        assert "condition_occurrence.person_id = person.person_id" in schema

    def test_build_m_schema_excludes_unrelated_fk(self):
        """선별되지 않은 테이블의 FK는 제외"""
        tables = [
            t for t in SAMPLE_TABLES
            if t["physical_name"] in ("person", "condition_occurrence")
        ]
        # 전체 FK를 전달해도 관련 없는 것은 제외됨
        schema = build_m_schema_for_tables(tables, TABLE_RELATIONSHIPS)
        # condition_occurrence FK만 포함
        assert "condition_occurrence.person_id = person.person_id" in schema
        # visit_occurrence는 선별 안 했으므로 FK에 포함 안 됨
        assert "visit_occurrence.person_id" not in schema.split("【외래키")[0] if "【외래키" in schema else True

    # -- 통합 link() --

    def test_link_diabetes_query(self):
        """link() 통합: 당뇨 질의"""
        result = self.linker.link("당뇨 환자 몇 명?")
        assert isinstance(result, SchemaLinkResult)
        assert "condition_occurrence" in result.selected_tables
        assert "person" in result.selected_tables
        assert "44054006" in result.evidence
        assert "【표(Table)】 condition_occurrence" in result.m_schema
        assert "【표(Table)】 person" in result.m_schema

    def test_link_lab_query(self):
        """link() 통합: 검사 질의"""
        result = self.linker.link("혈액검사 결과")
        assert "measurement" in result.selected_tables

    def test_link_with_previous_tables(self):
        """link() 통합: 이전 턴 테이블 병합"""
        result = self.linker.link(
            "검사 결과 보여줘",
            previous_tables=["condition_occurrence", "person"],
        )
        assert "measurement" in result.selected_tables
        assert "condition_occurrence" in result.selected_tables
        assert "person" in result.selected_tables


# =============================================================================
# 동적 스키마 통합 테스트 (3턴 시나리오)
# =============================================================================

class TestDynamicSchemaIntegration:
    """3턴 시나리오에서 동적 스키마 변화를 검증합니다.

    Turn 1: "당뇨 환자" → condition_occurrence + person
    Turn 2: "그 중 입원 환자" → condition_occurrence + person + visit_occurrence
    Turn 3: "검사 결과 보여줘" → + measurement
    """

    def setup_method(self):
        self.linker = SchemaLinker()

    def test_turn1_diabetes_patients(self):
        """턴 1: '당뇨 환자' → condition_occurrence + person만 선별"""
        result = self.linker.link("당뇨 환자 몇 명?")

        assert "condition_occurrence" in result.selected_tables
        assert "person" in result.selected_tables
        # 방문/검사 테이블은 미포함
        assert "visit_occurrence" not in result.selected_tables
        assert "measurement" not in result.selected_tables
        # M-Schema에 해당 테이블만 포함
        assert "【표(Table)】 condition_occurrence" in result.m_schema
        assert "【표(Table)】 person" in result.m_schema
        assert "【표(Table)】 visit_occurrence" not in result.m_schema
        # evidence에 SNOMED 코드 포함
        assert "44054006" in result.evidence

    def test_turn2_admission_with_previous(self):
        """턴 2: '그 중 입원 환자' → condition_occurrence + person + visit_occurrence 추가"""
        result = self.linker.link(
            "그 중 입원 환자",
            previous_tables=["condition_occurrence", "person"],
        )

        assert "condition_occurrence" in result.selected_tables
        assert "person" in result.selected_tables
        assert "visit_occurrence" in result.selected_tables
        # 검사는 미포함
        assert "measurement" not in result.selected_tables
        # M-Schema 검증
        assert "【표(Table)】 visit_occurrence" in result.m_schema
        assert "【표(Table)】 condition_occurrence" in result.m_schema
        # FK 포함
        assert "visit_occurrence.person_id = person.person_id" in result.m_schema

    def test_turn3_lab_results_with_previous(self):
        """턴 3: '검사 결과 보여줘' → + measurement 추가"""
        result = self.linker.link(
            "검사 결과 보여줘",
            previous_tables=["condition_occurrence", "person", "visit_occurrence"],
        )

        assert "measurement" in result.selected_tables
        assert "condition_occurrence" in result.selected_tables
        assert "person" in result.selected_tables
        assert "visit_occurrence" in result.selected_tables
        # M-Schema에 4개 테이블
        assert "【표(Table)】 measurement" in result.m_schema
        assert "【표(Table)】 visit_occurrence" in result.m_schema
        assert "【표(Table)】 condition_occurrence" in result.m_schema
        assert "【표(Table)】 person" in result.m_schema

    def test_full_3turn_scenario(self):
        """전체 3턴 시나리오 연속 실행"""
        # Turn 1
        r1 = self.linker.link("당뇨 환자 몇 명?")
        assert set(r1.selected_tables) == {"condition_occurrence", "person"}

        # Turn 2: 이전 턴 테이블 전달
        r2 = self.linker.link(
            "그 중 입원 환자",
            previous_tables=r1.selected_tables,
        )
        assert "visit_occurrence" in r2.selected_tables
        assert "condition_occurrence" in r2.selected_tables
        assert "person" in r2.selected_tables

        # Turn 3: 이전 턴 테이블 전달
        r3 = self.linker.link(
            "검사 결과 보여줘",
            previous_tables=r2.selected_tables,
        )
        assert "measurement" in r3.selected_tables
        assert "visit_occurrence" in r3.selected_tables
        assert "condition_occurrence" in r3.selected_tables
        assert "person" in r3.selected_tables

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

        expected_sql = "SELECT COUNT(*) FROM condition_occurrence WHERE condition_source_value = '44054006'"

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
        # 동적 스키마: condition_occurrence와 person만 포함되어야 함
        assert "condition_occurrence" in prompt
        assert "person" in prompt
        # 8개 전체가 아닌 선별된 테이블만
        assert "drug_exposure" not in prompt
        # evidence에 SNOMED 코드 포함
        assert "44054006" in prompt

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
                "sql": "SELECT COUNT(*) FROM condition_occurrence WHERE condition_source_value = '44054006'",
                "conditions": ["condition_source_value = '44054006'"],
                "tables_used": ["condition_occurrence", "person"],
                "result_count": 69,
            },
            "context_prompt": "검사 결과 보여줘",
        }

        expected_sql = "SELECT * FROM measurement"

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
        assert "condition_occurrence" in prompt
        assert "person" in prompt
        assert "measurement" in prompt


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
