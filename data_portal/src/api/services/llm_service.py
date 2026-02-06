"""
LLM Service - Claude/Gemini API integration for intent extraction and SQL generation
XiYanSQL integration for NL2SQL
"""
import os
import re
import json
from typing import Optional, Tuple
import httpx

from core.config import settings
from models.text2sql import IntentResult, SchemaContext

# XiYanSQL 서비스 import
try:
    from ai_services.xiyan_sql import xiyan_sql_service
    XIYAN_AVAILABLE = True
except ImportError:
    XIYAN_AVAILABLE = False
    print("Warning: XiYanSQL service not available")

# LLM Provider 설정
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "xiyan")  # "xiyan", "claude", "gemini", "local"
USE_XIYAN_SQL = os.getenv("USE_XIYAN_SQL", "true").lower() == "true"
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY", "")


class LLMService:
    """LLM 서비스 - 의도 파악 및 SQL 생성"""

    def __init__(self):
        self.provider = LLM_PROVIDER
        self.local_api_url = settings.LLM_API_URL
        self.local_model = settings.LLM_MODEL

    async def extract_intent(self, question: str) -> IntentResult:
        """Step 1: 질문에서 의도 추출"""
        prompt = f"""다음 자연어 질문을 분석하여 SQL 생성에 필요한 정보를 추출하세요.

질문: {question}

다음 JSON 형식으로 응답하세요:
{{
    "action": "COUNT|SUM|AVG|LIST|FILTER",  // 주요 액션
    "entities": ["entity1", "entity2"],      // 대상 엔티티(테이블/객체)
    "time_range": "YYYY-MM-DD ~ YYYY-MM-DD 또는 null",  // 시간 범위
    "filters": ["filter1", "filter2"],       // 필터 조건
    "confidence": 0.0~1.0                    // 확신도
}}

JSON만 응답하세요."""

        try:
            response = await self._call_llm(prompt)
            # JSON 파싱
            json_match = re.search(r'\{[^{}]*\}', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                return IntentResult(
                    action=data.get("action", "LIST"),
                    entities=data.get("entities", []),
                    time_range=data.get("time_range"),
                    filters=data.get("filters", []),
                    confidence=float(data.get("confidence", 0.7))
                )
        except Exception as e:
            print(f"Intent extraction error: {e}")

        # 폴백: 규칙 기반 의도 추출
        return self._rule_based_intent(question)

    def _rule_based_intent(self, question: str) -> IntentResult:
        """규칙 기반 의도 추출 (LLM 실패 시 폴백)"""
        action = "LIST"
        entities = []
        filters = []

        # 액션 감지
        if any(kw in question for kw in ["몇 명", "몇명", "수", "건수", "개수", "카운트"]):
            action = "COUNT"
        elif any(kw in question for kw in ["합계", "총", "합산"]):
            action = "SUM"
        elif any(kw in question for kw in ["평균"]):
            action = "AVG"
        elif any(kw in question for kw in ["목록", "리스트", "조회", "보여"]):
            action = "LIST"

        # 엔티티 감지
        entity_keywords = {
            "환자": "환자",
            "입원": "입원",
            "외래": "외래",
            "진단": "진단",
            "검사": "검사",
        }
        for kw, entity in entity_keywords.items():
            if kw in question:
                entities.append(entity)

        # 필터 감지
        filter_keywords = ["당뇨", "고혈압", "위암", "폐암", "남성", "여성", "2024년", "2023년", "올해", "작년"]
        for kw in filter_keywords:
            if kw in question:
                filters.append(kw)

        if not entities:
            entities = ["환자"]

        return IntentResult(
            action=action,
            entities=entities,
            time_range=None,
            filters=filters,
            confidence=0.6
        )

    async def generate_sql(
        self,
        question: str,
        enhanced_question: str,
        schema_context: SchemaContext,
        intent: IntentResult
    ) -> Tuple[str, str, float]:
        """Step 4: SQL 생성 (XiYanSQL 우선 사용)

        Returns:
            Tuple[sql, explanation, confidence]
        """
        # XiYanSQL 사용 (NL2SQL 전문 모델)
        if USE_XIYAN_SQL and XIYAN_AVAILABLE:
            try:
                sql = await xiyan_sql_service.generate_sql(
                    question=enhanced_question,
                    evidence=self._get_medical_evidence(intent.filters)
                )
                explanation = f"XiYanSQL을 통해 생성된 쿼리입니다. 원본 질문: {question}"
                return sql, explanation, 0.85
            except Exception as e:
                print(f"XiYanSQL error, falling back to LLM: {e}")

        # 폴백: 일반 LLM 사용
        prompt = f"""당신은 PostgreSQL SQL 전문가입니다. 다음 정보를 바탕으로 SQL 쿼리를 생성하세요.

## 원본 질문
{question}

## 분석된 질문 (의료 용어 해석 포함)
{enhanced_question}

## 분석된 의도
- 액션: {intent.action}
- 대상: {', '.join(intent.entities)}
- 필터: {', '.join(intent.filters) if intent.filters else '없음'}
- 시간 범위: {intent.time_range or '없음'}

## 사용 가능한 스키마
{schema_context.ddl_context}

## 테이블 관계
{self._format_relationships(schema_context.relationships)}

## 지침
1. PostgreSQL 문법을 사용하세요
2. 테이블과 컬럼명은 반드시 위 스키마에 있는 것만 사용하세요
3. 진단코드(ICD_CD)를 필터링할 때는 LIKE 'XX%' 패턴을 사용하세요 (예: E11% = 당뇨병)
4. 날짜 비교는 DATE 타입 컬럼에 적용하세요
5. 환자 정보 조인 시 PT_NO 컬럼을 사용하세요

## 응답 형식
다음 JSON 형식으로 응답하세요:
{{
    "sql": "SELECT ...",
    "explanation": "이 SQL은 ...",
    "confidence": 0.0~1.0
}}

JSON만 응답하세요."""

        try:
            response = await self._call_llm(prompt)
            # JSON 파싱
            json_match = re.search(r'\{[^{}]*"sql"[^{}]*\}', response, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
                return (
                    data.get("sql", ""),
                    data.get("explanation", ""),
                    float(data.get("confidence", 0.7))
                )
        except Exception as e:
            print(f"SQL generation error: {e}")

        # 폴백: 템플릿 기반 SQL 생성
        return self._template_based_sql(question, enhanced_question, schema_context, intent)

    def _template_based_sql(
        self,
        question: str,
        enhanced_question: str,
        schema_context: SchemaContext,
        intent: IntentResult
    ) -> Tuple[str, str, float]:
        """템플릿 기반 SQL 생성 (LLM 실패 시 폴백)"""
        tables = schema_context.tables

        # ICD 코드 추출
        icd_match = re.search(r'ICD:([A-Z0-9]+)', enhanced_question)
        icd_code = icd_match.group(1) if icd_match else None

        if intent.action == "COUNT":
            if "DIAG_INFO" in tables and icd_code:
                sql = f"""SELECT COUNT(DISTINCT d.PT_NO) as patient_count
FROM DIAG_INFO d
INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO
WHERE d.ICD_CD LIKE '{icd_code}%'"""
                explanation = f"진단코드 {icd_code}로 시작하는 환자 수를 조회합니다."
            elif "IPD_ADM" in tables:
                sql = """SELECT COUNT(DISTINCT PT_NO) as patient_count
FROM IPD_ADM
WHERE DSCH_DT IS NULL"""
                explanation = "현재 입원 중인 환자 수를 조회합니다."
            elif "OPD_RCPT" in tables:
                sql = """SELECT COUNT(DISTINCT PT_NO) as patient_count
FROM OPD_RCPT
WHERE RCPT_DT = CURRENT_DATE"""
                explanation = "오늘 외래 접수한 환자 수를 조회합니다."
            else:
                sql = "SELECT COUNT(*) as patient_count FROM PT_BSNF"
                explanation = "전체 환자 수를 조회합니다."

        elif intent.action == "LIST":
            if "IPD_ADM" in tables:
                sql = """SELECT p.PT_NO, p.PT_NM, i.ADM_DT, i.WARD_CD, i.ROOM_NO
FROM IPD_ADM i
INNER JOIN PT_BSNF p ON i.PT_NO = p.PT_NO
WHERE i.DSCH_DT IS NULL
ORDER BY i.ADM_DT DESC
LIMIT 100"""
                explanation = "현재 입원 중인 환자 목록을 조회합니다."
            elif "DIAG_INFO" in tables and icd_code:
                sql = f"""SELECT p.PT_NO, p.PT_NM, d.DIAG_NM, d.DIAG_DT
FROM DIAG_INFO d
INNER JOIN PT_BSNF p ON d.PT_NO = p.PT_NO
WHERE d.ICD_CD LIKE '{icd_code}%'
ORDER BY d.DIAG_DT DESC
LIMIT 100"""
                explanation = f"진단코드 {icd_code}로 시작하는 환자 목록을 조회합니다."
            else:
                sql = """SELECT PT_NO, PT_NM, BRTH_DT, SEX_CD
FROM PT_BSNF
ORDER BY RGST_DT DESC
LIMIT 100"""
                explanation = "환자 목록을 조회합니다."

        else:
            sql = "SELECT * FROM PT_BSNF LIMIT 10"
            explanation = "환자 정보를 조회합니다."

        return sql, explanation, 0.6

    def _format_relationships(self, relationships: list) -> str:
        """관계 정보 포맷팅"""
        if not relationships:
            return "없음"

        lines = []
        for rel in relationships:
            lines.append(f"- {rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']}")
        return "\n".join(lines)

    def _get_medical_evidence(self, filters: list) -> str:
        """의료 필터에서 참조 정보 생성 (SNOMED CT codes for Synthea OMOP data)"""
        evidence_map = {
            "당뇨": "당뇨병(Diabetes Mellitus)의 SNOMED CT 코드는 44054006입니다. condition_source_value = '44054006' 조건을 사용합니다.",
            "고혈압": "고혈압(Hypertension)의 SNOMED CT 코드는 38341003입니다. condition_source_value = '38341003' 조건을 사용합니다.",
            "심방세동": "심방세동(Atrial Fibrillation)의 SNOMED CT 코드는 49436004입니다. condition_source_value = '49436004' 조건을 사용합니다.",
            "심근경색": "심근경색(Myocardial Infarction)의 SNOMED CT 코드는 22298006입니다. condition_source_value = '22298006' 조건을 사용합니다.",
            "뇌졸중": "뇌졸중(Stroke)의 SNOMED CT 코드는 230690007입니다. condition_source_value = '230690007' 조건을 사용합니다.",
            "관상동맥": "관상동맥 질환(Coronary arteriosclerosis)의 SNOMED CT 코드는 53741008입니다. condition_source_value = '53741008' 조건을 사용합니다.",
            "남성": "성별 조건: gender_source_value = 'M' 또는 gender_concept_id = 8507",
            "여성": "성별 조건: gender_source_value = 'F' 또는 gender_concept_id = 8532",
        }

        evidences = []
        for f in filters:
            for key, value in evidence_map.items():
                if key in f:
                    evidences.append(value)

        return "\n".join(evidences) if evidences else ""

    async def explain_results(self, question: str, sql: str, results: list, columns: list) -> str:
        """결과를 자연어로 설명"""
        if not results:
            return "조회 결과가 없습니다."

        prompt = f"""다음 SQL 쿼리 결과를 자연어로 간단히 설명해주세요.

질문: {question}
SQL: {sql}
컬럼: {columns}
결과 (최대 5행): {results[:5]}
총 행 수: {len(results)}

한두 문장으로 결과를 요약해주세요."""

        try:
            response = await self._call_llm(prompt)
            return response.strip()
        except Exception as e:
            print(f"Result explanation error: {e}")

        # 폴백: 기본 설명
        if len(results) == 1 and len(columns) == 1:
            return f"결과: {results[0][0]}"
        return f"총 {len(results)}건의 결과가 조회되었습니다."

    async def _call_llm(self, prompt: str) -> str:
        """LLM API 호출"""
        if self.provider == "claude":
            return await self._call_claude(prompt)
        elif self.provider == "gemini":
            return await self._call_gemini(prompt)
        else:
            return await self._call_local(prompt)

    async def _call_claude(self, prompt: str) -> str:
        """Claude API 호출"""
        if not ANTHROPIC_API_KEY:
            raise ValueError("ANTHROPIC_API_KEY not set")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-3-haiku-20240307",
                    "max_tokens": 1024,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            response.raise_for_status()
            data = response.json()
            return data["content"][0]["text"]

    async def _call_gemini(self, prompt: str) -> str:
        """Gemini API 호출"""
        if not GOOGLE_API_KEY:
            raise ValueError("GOOGLE_API_KEY not set")

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent?key={GOOGLE_API_KEY}",
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                },
            )
            response.raise_for_status()
            data = response.json()
            return data["candidates"][0]["content"]["parts"][0]["text"]

    async def _call_local(self, prompt: str) -> str:
        """로컬 LLM API 호출 (OpenAI 호환)"""
        async with httpx.AsyncClient(timeout=60.0) as client:
            try:
                response = await client.post(
                    f"{self.local_api_url}/chat/completions",
                    headers={"Content-Type": "application/json"},
                    json={
                        "model": self.local_model,
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 1024,
                        "temperature": 0.1,
                    },
                )
                response.raise_for_status()
                data = response.json()
                return data["choices"][0]["message"]["content"]
            except Exception as e:
                print(f"Local LLM call failed: {e}")
                raise


# Singleton instance
llm_service = LLMService()
