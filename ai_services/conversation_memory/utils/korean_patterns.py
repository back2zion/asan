"""
한국어 참조 패턴 정의

다중 턴 대화에서 "방금", "이전", "그 환자" 등의 한국어 참조 표현을 탐지하기 위한 패턴 정의.
"""

import re
from dataclasses import dataclass
from typing import Optional

from ai_services.conversation_memory.state.conversation_state import (
    ReferenceInfo,
    ReferenceType,
)


@dataclass
class ReferencePattern:
    """참조 패턴 정의"""
    ref_type: ReferenceType
    patterns: list[str]  # 정규식 패턴 목록
    description: str     # 패턴 설명
    action: str          # 해석 방법 (예: "use_last_result", "add_filter")


# 한국어 참조 패턴 정의
KOREAN_REFERENCE_PATTERNS: list[ReferencePattern] = [
    # 시간적 참조 - 이전 쿼리 결과 참조
    ReferencePattern(
        ref_type=ReferenceType.TEMPORAL,
        patterns=[
            r"방금(?:\s+(?:그|전|조회한?|검색한?|찾은?))?",
            r"이전(?:\s+(?:에|의|결과|쿼리|조회))?",
            r"아까(?:\s+(?:그|전|조회한?|검색한?|찾은?))?",
            r"직전(?:\s+(?:에|의|결과|쿼리|조회))?",
            r"앞(?:서|에서)(?:\s+(?:조회한?|검색한?|찾은?))?",
            r"조금\s*전(?:\s+(?:에|의))?",
        ],
        description="시간적 참조 - 직전 쿼리 결과 참조",
        action="use_last_result",
    ),

    # 엔티티 참조 - 이전에 언급된 환자/대상 참조
    ReferencePattern(
        ref_type=ReferenceType.ENTITY,
        patterns=[
            r"그\s*환자(?:들)?",
            r"해당\s*환자(?:들)?",
            r"위\s*환자(?:들)?",
            r"이\s*환자(?:들)?",
            r"저\s*환자(?:들)?",
            r"그(?:\s+)?사람(?:들)?",
            r"해당(?:\s+)?(?:대상|케이스|건)(?:들)?",
            r"위(?:\s+)?(?:대상|케이스|건)(?:들)?",
        ],
        description="엔티티 참조 - 이전 쿼리의 환자/대상 참조",
        action="use_previous_patients",
    ),

    # 결과 참조 - 이전 쿼리 결과 집합에서 필터링
    ReferencePattern(
        ref_type=ReferenceType.RESULT,
        patterns=[
            r"그\s*중(?:에서?)?",
            r"거기(?:에)?서",
            r"위\s*결과(?:에서?)?",
            r"이\s*중(?:에서?)?",
            r"저\s*중(?:에서?)?",
            r"그\s*(?:데이터|목록|리스트)(?:에서?)?",
            r"해당\s*결과(?:에서?)?",
            r"조회(?:된|한)\s*(?:결과|데이터)(?:에서?)?",
        ],
        description="결과 참조 - 이전 결과 집합에서 추가 필터링",
        action="filter_from_result",
    ),

    # 조건 추가 - 기존 조건에 추가 조건 연결
    ReferencePattern(
        ref_type=ReferenceType.CONDITION,
        patterns=[
            r"추가(?:로|적으로)?",
            r"더\s*(?:좁혀|필터링|걸러)",
            r"그리고(?:\s+)?(?:추가로)?",
            r"또(?:한)?",
            r"게다가",
            r"여기(?:에)?서\s*(?:더|추가로)?",
            r"(?:조건을?\s*)?(?:더\s*)?(?:추가|넣어|걸어)",
        ],
        description="조건 추가 - 기존 조건에 AND 연결",
        action="add_filter",
    ),
]


def detect_korean_references(
    query: str,
    patterns: Optional[list[ReferencePattern]] = None,
) -> list[ReferenceInfo]:
    """한국어 질의에서 참조 표현을 탐지합니다.

    Args:
        query: 사용자의 자연어 질의
        patterns: 사용할 패턴 목록 (기본값: KOREAN_REFERENCE_PATTERNS)

    Returns:
        탐지된 참조 표현 정보 목록

    Example:
        >>> query = "방금 조회한 환자 중에서 남성만 보여줘"
        >>> refs = detect_korean_references(query)
        >>> print(refs[0].ref_type)
        ReferenceType.TEMPORAL
    """
    if patterns is None:
        patterns = KOREAN_REFERENCE_PATTERNS

    detected: list[ReferenceInfo] = []

    for pattern_def in patterns:
        for pattern in pattern_def.patterns:
            compiled = re.compile(pattern, re.IGNORECASE)
            for match in compiled.finditer(query):
                ref_info = ReferenceInfo(
                    ref_type=pattern_def.ref_type,
                    pattern=pattern,
                    position=match.start(),
                    original_text=match.group(),
                    resolved_value=None,  # 나중에 context_resolver에서 채움
                )
                detected.append(ref_info)

    # 위치 순으로 정렬하고 중복 제거
    detected = _deduplicate_references(detected)
    detected.sort(key=lambda x: x.position)

    return detected


def _deduplicate_references(refs: list[ReferenceInfo]) -> list[ReferenceInfo]:
    """중복된 참조 표현을 제거합니다.

    같은 위치에서 여러 패턴이 매칭된 경우, 더 긴 매칭을 우선합니다.
    """
    if not refs:
        return []

    # 위치별로 그룹화
    by_position: dict[int, list[ReferenceInfo]] = {}
    for ref in refs:
        if ref.position not in by_position:
            by_position[ref.position] = []
        by_position[ref.position].append(ref)

    # 각 위치에서 가장 긴 매칭 선택
    deduplicated = []
    for position, refs_at_pos in by_position.items():
        longest = max(refs_at_pos, key=lambda x: len(x.original_text))
        deduplicated.append(longest)

    return deduplicated


def has_any_reference(query: str) -> bool:
    """질의에 참조 표현이 있는지 빠르게 확인합니다.

    Args:
        query: 사용자의 자연어 질의

    Returns:
        참조 표현 존재 여부
    """
    refs = detect_korean_references(query)
    return len(refs) > 0


def get_reference_types(query: str) -> set[ReferenceType]:
    """질의에서 발견된 참조 유형들을 반환합니다.

    Args:
        query: 사용자의 자연어 질의

    Returns:
        발견된 참조 유형 집합
    """
    refs = detect_korean_references(query)
    return {ref.ref_type for ref in refs}
