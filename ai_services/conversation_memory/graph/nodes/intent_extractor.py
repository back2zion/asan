"""
Intent 추출 노드

사용자 질의에서 의도를 추출합니다.
기존 NL2SQL 파이프라인의 Intent Extraction 단계를 래핑합니다.
"""

from typing import Any, Optional

from ai_services.conversation_memory.state.conversation_state import (
    ConversationState,
)


def intent_extractor_node(state: ConversationState) -> dict:
    """사용자 의도를 추출하는 노드.

    참조 표현이 없는 새로운 질의에서 의도를 추출합니다.
    기존 NL2SQL 파이프라인의 Intent Extraction 단계를 호출합니다.

    Args:
        state: 현재 대화 상태

    Returns:
        업데이트할 상태 필드 딕셔너리:
        - session_metadata: 추출된 의도 정보 추가
    """
    current_query = state.get("current_query", "")

    if not current_query:
        return {}

    # 의도 추출 (실제 구현에서는 LLM 또는 기존 서비스 호출)
    intent = extract_query_intent(current_query)

    # 세션 메타데이터에 의도 정보 저장
    session_metadata = dict(state.get("session_metadata", {}))
    session_metadata["last_intent"] = intent

    return {
        "session_metadata": session_metadata,
    }


def extract_query_intent(query: str) -> dict[str, Any]:
    """질의에서 의도를 추출합니다.

    실제 구현에서는 LLM 또는 분류 모델을 사용합니다.
    여기서는 규칙 기반 기본 구현을 제공합니다.

    Args:
        query: 사용자 질의

    Returns:
        추출된 의도 정보
    """
    intent = {
        "query_type": "unknown",
        "action": "query",
        "entities": [],
        "conditions": [],
        "aggregation": None,
        "time_range": None,
    }

    query_lower = query.lower()

    # 질의 유형 판단
    if any(kw in query_lower for kw in ["몇 명", "몇명", "환자 수", "건수", "카운트"]):
        intent["query_type"] = "count"
        intent["aggregation"] = "COUNT"
    elif any(kw in query_lower for kw in ["평균", "average", "avg"]):
        intent["query_type"] = "aggregate"
        intent["aggregation"] = "AVG"
    elif any(kw in query_lower for kw in ["목록", "리스트", "조회", "보여", "알려"]):
        intent["query_type"] = "list"
    elif any(kw in query_lower for kw in ["비교", "차이", "vs"]):
        intent["query_type"] = "compare"

    # 엔티티 추출 (질환, 환자군 등)
    medical_entities = _extract_medical_entities(query)
    intent["entities"] = medical_entities

    # 조건 추출
    conditions = _extract_conditions(query)
    intent["conditions"] = conditions

    # 시간 범위 추출
    time_range = _extract_time_range(query)
    if time_range:
        intent["time_range"] = time_range

    return intent


def _extract_medical_entities(query: str) -> list[dict]:
    """의료 엔티티를 추출합니다.

    실제 구현에서는 BioClinicalBERT 또는 의료 NER 모델을 사용합니다.
    """
    entities = []
    query_lower = query.lower()

    # 질환 키워드 매핑
    disease_keywords = {
        "당뇨": {"name": "당뇨병", "icd10": "E10-E14", "concept_id": "201826"},
        "고혈압": {"name": "고혈압", "icd10": "I10-I15", "concept_id": "316866"},
        "암": {"name": "악성 신생물", "icd10": "C00-C97"},
        "심부전": {"name": "심부전", "icd10": "I50", "concept_id": "316139"},
        "폐렴": {"name": "폐렴", "icd10": "J12-J18", "concept_id": "255848"},
        "천식": {"name": "천식", "icd10": "J45", "concept_id": "317009"},
    }

    for keyword, info in disease_keywords.items():
        if keyword in query_lower:
            entities.append({
                "type": "disease",
                "text": keyword,
                **info,
            })

    # 약물 키워드
    drug_keywords = {
        "메트포르민": {"name": "Metformin", "rxnorm": "6809"},
        "인슐린": {"name": "Insulin", "rxnorm": "5856"},
        "아스피린": {"name": "Aspirin", "rxnorm": "1191"},
    }

    for keyword, info in drug_keywords.items():
        if keyword in query_lower:
            entities.append({
                "type": "drug",
                "text": keyword,
                **info,
            })

    return entities


def _extract_conditions(query: str) -> list[dict]:
    """필터 조건을 추출합니다."""
    conditions = []
    query_lower = query.lower()

    # 성별 조건
    if "남성" in query_lower or "남자" in query_lower:
        conditions.append({"field": "sex", "operator": "=", "value": "M"})
    if "여성" in query_lower or "여자" in query_lower:
        conditions.append({"field": "sex", "operator": "=", "value": "F"})

    # 연령 조건
    import re
    age_patterns = [
        (r"(\d+)세\s*이상", ">="),
        (r"(\d+)세\s*이하", "<="),
        (r"(\d+)세\s*초과", ">"),
        (r"(\d+)세\s*미만", "<"),
    ]

    for pattern, operator in age_patterns:
        match = re.search(pattern, query)
        if match:
            conditions.append({
                "field": "age",
                "operator": operator,
                "value": int(match.group(1)),
            })

    # 진료과 조건
    departments = {
        "내분비내과": "Endocrinology",
        "순환기내과": "Cardiology",
        "호흡기내과": "Pulmonology",
        "소화기내과": "Gastroenterology",
        "신장내과": "Nephrology",
    }

    for dept_kr, dept_en in departments.items():
        if dept_kr in query:
            conditions.append({
                "field": "department",
                "operator": "=",
                "value": dept_en,
            })

    return conditions


def _extract_time_range(query: str) -> Optional[dict]:
    """시간 범위를 추출합니다."""
    import re
    from datetime import datetime, timedelta

    query_lower = query.lower()

    # 상대적 시간 표현
    if "최근 1년" in query_lower or "지난 1년" in query_lower:
        end = datetime.now()
        start = end - timedelta(days=365)
        return {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")}

    if "최근 6개월" in query_lower or "지난 6개월" in query_lower:
        end = datetime.now()
        start = end - timedelta(days=180)
        return {"start": start.strftime("%Y-%m-%d"), "end": end.strftime("%Y-%m-%d")}

    if "올해" in query_lower:
        year = datetime.now().year
        return {"start": f"{year}-01-01", "end": f"{year}-12-31"}

    # 절대적 시간 표현
    year_match = re.search(r"(\d{4})년", query)
    if year_match:
        year = year_match.group(1)
        return {"start": f"{year}-01-01", "end": f"{year}-12-31"}

    return None


def build_intent_summary(intent: dict) -> str:
    """의도 요약 문자열을 생성합니다.

    Args:
        intent: 추출된 의도 정보

    Returns:
        요약 문자열
    """
    parts = []

    if intent.get("query_type"):
        parts.append(f"질의유형: {intent['query_type']}")

    if intent.get("entities"):
        entity_names = [e.get("name", e.get("text", "")) for e in intent["entities"]]
        parts.append(f"엔티티: {', '.join(entity_names)}")

    if intent.get("conditions"):
        cond_strs = [
            f"{c['field']} {c['operator']} {c['value']}"
            for c in intent["conditions"]
        ]
        parts.append(f"조건: {', '.join(cond_strs)}")

    if intent.get("time_range"):
        tr = intent["time_range"]
        parts.append(f"기간: {tr['start']} ~ {tr['end']}")

    return " | ".join(parts) if parts else "의도 불명확"
