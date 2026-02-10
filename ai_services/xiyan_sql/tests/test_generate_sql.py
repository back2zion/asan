"""
XiYanSQL SQL 생성 테스트

비동기 테스트: vLLM API 호출 (mock)
통합 테스트: ConversationMemory + XiYan SQL 3턴 시나리오
record_query_result 연동 테스트
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from ai_services.xiyan_sql.service import XiYanSQLService


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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
