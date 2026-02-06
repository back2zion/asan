"""
Query Completeness Detector

규칙 기반으로 사용자 입력의 완성도를 탐지합니다.
"""

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class QueryCompletenessLevel(str, Enum):
    """쿼리 완성도 수준"""
    KEYWORD_ONLY = "keyword_only"    # 키워드만 (예: "당뇨 환자")
    INCOMPLETE = "incomplete"         # 불완전한 문장 (예: "당뇨 환자 중")
    COMPLETE = "complete"             # 완전한 질의문 (예: "당뇨병 환자는 몇 명입니까?")


@dataclass
class QueryCompletenessResult:
    """완성도 탐지 결과"""
    level: QueryCompletenessLevel
    missing_elements: list[str] = field(default_factory=list)
    inferred_intent: Optional[str] = None
    has_question_word: bool = False
    has_sentence_ending: bool = False
    word_count: int = 0
    detected_medical_terms: list[str] = field(default_factory=list)


# 한국어 문장 종결 패턴
SENTENCE_ENDINGS = [
    r"[니까?]$",           # ~입니까?, ~습니까?
    r"[세요?]$",           # ~하세요?
    r"[나요?]$",           # ~인가요?
    r"[까요?]$",           # ~할까요?
    r"[죠?]$",             # ~하죠?
    r"[요?]$",             # ~해요?
    r"\?$",                # 물음표
    r"[다.]$",             # ~합니다., ~입니다.
    r"[요.]$",             # ~해요.
    r"알려\s*(?:줘|주세요|달라)",
    r"보여\s*(?:줘|주세요)",
    r"찾아\s*(?:줘|주세요)",
    r"조회\s*(?:해\s*줘|해\s*주세요)",
]

# 질문 키워드
QUESTION_WORDS = [
    "몇", "얼마", "어떤", "무엇", "누구", "어디", "언제", "왜", "어떻게",
    "몇명", "몇 명", "몇개", "몇 개", "몇건", "몇 건",
    "있나", "있는가", "인가", "인지",
    "알려", "보여", "찾아", "조회",
]

# 의료 도메인 키워드 (schema.py에서 가져온 것 기반)
MEDICAL_KEYWORDS = [
    # 질환
    "당뇨", "당뇨병", "고혈압", "암", "위암", "폐암", "간암", "대장암",
    "유방암", "전립선암", "췌장암", "갑상선암", "심근경색", "협심증",
    "심부전", "부정맥", "뇌졸중", "뇌경색", "뇌출혈", "폐렴", "천식",
    "COPD", "위염", "위궤양", "간경변", "췌장염", "신부전", "패혈증",
    "코로나", "COVID",
    # 환자/진료
    "환자", "입원", "퇴원", "외래", "진료", "진단", "검사",
    # 인구통계
    "남성", "여성", "남자", "여자", "나이", "연령",
]

# 불완전 종결 패턴 (문장이 끝나지 않음)
INCOMPLETE_PATTERNS = [
    r"중$",                # "~중"으로 끝남
    r"의$",                # "~의"로 끝남
    r"에서$",              # "~에서"로 끝남
    r"으로$",              # "~으로"로 끝남
    r"와$",                # "~와"로 끝남
    r"과$",                # "~과"로 끝남
    r"에$",                # "~에"로 끝남
    r"을$",                # "~을"로 끝남
    r"를$",                # "~를"로 끝남
]


def detect_completeness(query: str) -> QueryCompletenessResult:
    """쿼리의 완성도를 탐지합니다.

    Args:
        query: 사용자 입력 문자열

    Returns:
        QueryCompletenessResult: 완성도 분석 결과
    """
    query = query.strip()

    # 기본 통계
    words = query.split()
    word_count = len(words)

    # 의료 용어 탐지
    detected_medical = []
    query_lower = query.lower()
    for term in MEDICAL_KEYWORDS:
        if term.lower() in query_lower:
            detected_medical.append(term)

    # 질문 단어 존재 여부
    has_question_word = any(qw in query for qw in QUESTION_WORDS)

    # 문장 종결 패턴 확인
    has_sentence_ending = any(
        re.search(pattern, query) for pattern in SENTENCE_ENDINGS
    )

    # 완성도 판정
    missing_elements = []
    inferred_intent = None

    # 불완전 종결 패턴 확인
    has_incomplete_ending = any(
        re.search(pattern, query) for pattern in INCOMPLETE_PATTERNS
    )

    if has_incomplete_ending:
        level = QueryCompletenessLevel.INCOMPLETE
        missing_elements.append("문장 종결")
    elif has_sentence_ending and has_question_word:
        level = QueryCompletenessLevel.COMPLETE
    elif has_sentence_ending or has_question_word:
        # 둘 중 하나만 있으면 불완전
        level = QueryCompletenessLevel.INCOMPLETE
        if not has_sentence_ending:
            missing_elements.append("문장 종결")
        if not has_question_word:
            missing_elements.append("질문 표현")
    elif word_count <= 3 and detected_medical:
        # 짧은 키워드 나열
        level = QueryCompletenessLevel.KEYWORD_ONLY
        missing_elements.extend(["질문 표현", "문장 종결", "구체적 요청"])
    else:
        # 의료 용어 없이 짧은 입력
        level = QueryCompletenessLevel.KEYWORD_ONLY if word_count <= 3 else QueryCompletenessLevel.INCOMPLETE
        missing_elements.extend(["질문 표현", "문장 종결"])

    # 의도 추론
    if detected_medical:
        if "환자" in query:
            inferred_intent = "환자 조회/집계"
        elif "입원" in query:
            inferred_intent = "입원 환자 조회"
        elif "외래" in query:
            inferred_intent = "외래 환자 조회"
        elif "검사" in query:
            inferred_intent = "검사 결과 조회"
        elif any(cancer in query for cancer in ["암", "위암", "폐암", "간암"]):
            inferred_intent = "암 환자 조회"
        else:
            inferred_intent = "환자 정보 조회"

    return QueryCompletenessResult(
        level=level,
        missing_elements=missing_elements,
        inferred_intent=inferred_intent,
        has_question_word=has_question_word,
        has_sentence_ending=has_sentence_ending,
        word_count=word_count,
        detected_medical_terms=detected_medical,
    )
