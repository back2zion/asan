"""
Query Expansion 프롬프트 템플릿

LLM을 사용하여 불완전한 키워드를 완전한 질의문으로 확장합니다.
"""

from typing import Optional


QUERY_EXPANSION_SYSTEM_PROMPT = """당신은 서울아산병원 통합데이터플랫폼의 질의 확장 전문가입니다.
사용자가 입력한 불완전한 키워드나 짧은 문구를 분석하여,
의료 데이터 조회에 적합한 완전한 질의문으로 확장합니다.

## 규칙

1. **의료 용어 표준화**: 약어나 구어체를 표준 의료 용어로 변환
   - DM → 제2형 당뇨병
   - HTN → 고혈압
   - Ca → 암

2. **질의 의도 파악**: 키워드에서 사용자의 의도를 추론
   - "당뇨 환자" → 환자 수 조회
   - "고혈압 입원" → 입원 중인 환자 조회

3. **완전한 질문 형태**: 확장된 질의는 반드시 완전한 문장으로
   - "~는 몇 명입니까?"
   - "~를 조회해 주세요."
   - "~의 목록을 알려주세요."

4. **과도한 확장 금지**: 사용자가 명시하지 않은 조건을 임의로 추가하지 않음
   - "당뇨 환자" → "제2형 당뇨병 진단을 받은 환자는 몇 명입니까?" (O)
   - "당뇨 환자" → "2024년에 제2형 당뇨병 진단을 받은 65세 이상 환자" (X) - 과도한 조건 추가

5. **신뢰도 제공**: 확장의 확실성을 0.0~1.0 사이의 값으로 제공

## 응답 형식 (JSON)

```json
{
  "enhanced_query": "확장된 완전한 질의문",
  "confidence": 0.85,
  "reasoning": "확장 근거 설명"
}
```"""


QUERY_EXPANSION_USER_TEMPLATE = """## 입력 분석

**원본 입력**: {original_query}
**정규화된 입력**: {normalized_query}
**완성도**: {completeness_level}
**누락 요소**: {missing_elements}
**추론된 의도**: {inferred_intent}
**탐지된 의료 용어**: {medical_terms}

{context_section}

위 분석을 바탕으로 입력을 완전한 질의문으로 확장해 주세요.
JSON 형식으로만 응답하세요."""


def build_expansion_prompt(
    original_query: str,
    normalized_query: str,
    completeness_level: str,
    missing_elements: list[str],
    inferred_intent: Optional[str],
    medical_terms: list[str],
    context: Optional[str] = None,
) -> str:
    """확장 프롬프트를 구성합니다.

    Args:
        original_query: 사용자 원본 입력
        normalized_query: 정규화된 입력
        completeness_level: 완성도 수준
        missing_elements: 누락된 요소 목록
        inferred_intent: 추론된 의도
        medical_terms: 탐지된 의료 용어
        context: 추가 컨텍스트 (이전 대화 등)

    Returns:
        구성된 프롬프트 문자열
    """
    context_section = ""
    if context:
        context_section = f"\n**추가 컨텍스트**:\n{context}\n"

    return QUERY_EXPANSION_USER_TEMPLATE.format(
        original_query=original_query,
        normalized_query=normalized_query,
        completeness_level=completeness_level,
        missing_elements=", ".join(missing_elements) if missing_elements else "없음",
        inferred_intent=inferred_intent or "불명확",
        medical_terms=", ".join(medical_terms) if medical_terms else "없음",
        context_section=context_section,
    )
