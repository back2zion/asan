"""
Medical Term Normalizer

구어체/약어를 표준 의료 용어로 정규화합니다.
schema.py의 SYNONYM_MAP, ICD_CODE_MAP을 재사용합니다.
"""

import re
from dataclasses import dataclass
from typing import Optional

# schema.py에서 import (순환 참조 방지를 위해 직접 정의)
SYNONYM_MAP = {
    "당뇨환자": "당뇨병 환자",
    "DM": "당뇨병",
    "DM환자": "당뇨병 환자",
    "dm": "당뇨병",
    "dm환자": "당뇨병 환자",
    "고혈압환자": "고혈압 환자",
    "HTN": "고혈압",
    "htn": "고혈압",
    "혈압환자": "고혈압 환자",
    "Ca": "암",
    "ca": "암",
    "암환자": "암 환자",
    "위ca": "위암",
    "폐ca": "폐암",
    "간ca": "간암",
    "입원환자": "입원 환자",
    "입원한 환자": "입원 환자",
    "입원 중인 환자": "입원 환자",
    "퇴원환자": "퇴원 환자",
    "퇴원한 환자": "퇴원 환자",
    "외래환자": "외래 환자",
    "외래 진료 환자": "외래 환자",
    "혈액검사": "임상검사",
    "피검사": "임상검사",
    "Lab": "임상검사",
    "lab": "임상검사",
    # 추가 약어
    "MI": "심근경색",
    "CVA": "뇌졸중",
    "CKD": "만성신부전",
    "AKI": "급성신부전",
    "CHF": "심부전",
    "Afib": "심방세동",
    "AF": "심방세동",
    "HF": "심부전",
    "CAD": "관상동맥질환",
    "PCI": "경피적관상동맥중재술",
    "CABG": "관상동맥우회술",
}

# 의료 용어 정규화 (비표준 → 표준)
MEDICAL_TERM_NORMALIZATION = {
    "당뇨": "제2형 당뇨병",
    "혈압": "고혈압",
    "폐ca": "폐암",
    "위ca": "위암",
    "간ca": "간암",
    "코로나": "COVID-19",
    "코로나19": "COVID-19",
}


@dataclass
class NormalizationResult:
    """정규화 결과"""
    original: str
    normalized: str
    replacements: list[tuple[str, str]]  # (원본, 대체) 쌍
    has_changes: bool


class MedicalNormalizer:
    """의료 용어 정규화기"""

    def __init__(
        self,
        synonym_map: Optional[dict] = None,
        term_normalization: Optional[dict] = None,
    ):
        """
        Args:
            synonym_map: 동의어 매핑 딕셔너리
            term_normalization: 용어 정규화 딕셔너리
        """
        self.synonym_map = synonym_map or SYNONYM_MAP
        self.term_normalization = term_normalization or MEDICAL_TERM_NORMALIZATION

        # 긴 패턴부터 매칭하도록 정렬
        self._sorted_synonyms = sorted(
            self.synonym_map.keys(),
            key=len,
            reverse=True
        )

    def normalize(self, text: str) -> NormalizationResult:
        """텍스트 내 의료 용어를 정규화합니다.

        Args:
            text: 입력 텍스트

        Returns:
            NormalizationResult: 정규화 결과
        """
        original = text
        replacements = []

        # 1. 동의어 치환 (약어, 구어체 → 표준 용어)
        # 단어 경계를 확인하여 부분 문자열 오매칭 방지 (예: CDM의 DM → 당뇨병 방지)
        for pattern in self._sorted_synonyms:
            # 영문/숫자 패턴은 단어 경계(\b) 사용, 한글은 그대로 매칭
            if re.search(r'[a-zA-Z]', pattern):
                regex = re.compile(r'\b' + re.escape(pattern) + r'\b')
            else:
                regex = re.compile(re.escape(pattern))
            if regex.search(text):
                replacement = self.synonym_map[pattern]
                text = regex.sub(replacement, text)
                replacements.append((pattern, replacement))

        return NormalizationResult(
            original=original,
            normalized=text,
            replacements=replacements,
            has_changes=original != text,
        )

    def normalize_with_expansion(self, text: str) -> NormalizationResult:
        """텍스트 내 의료 용어를 정규화하고 더 구체적인 용어로 확장합니다.

        예: "당뇨" → "제2형 당뇨병"

        Args:
            text: 입력 텍스트

        Returns:
            NormalizationResult: 정규화 결과
        """
        # 먼저 기본 정규화 수행
        result = self.normalize(text)
        text = result.normalized
        replacements = list(result.replacements)

        # 2. 용어 확장 (간략 용어 → 정식 용어)
        for pattern, expansion in self.term_normalization.items():
            # 이미 정규화된 용어가 있으면 스킵
            if expansion in text:
                continue
            if re.search(r'[a-zA-Z]', pattern):
                regex = re.compile(r'\b' + re.escape(pattern) + r'\b')
            else:
                regex = re.compile(re.escape(pattern))
            if regex.search(text):
                text = regex.sub(expansion, text)
                replacements.append((pattern, expansion))

        return NormalizationResult(
            original=result.original,
            normalized=text,
            replacements=replacements,
            has_changes=result.original != text,
        )


# 모듈 수준 함수
_normalizer = MedicalNormalizer()


def normalize_medical_terms(text: str, expand: bool = False) -> str:
    """텍스트 내 의료 용어를 정규화합니다.

    Args:
        text: 입력 텍스트
        expand: True면 용어 확장도 수행

    Returns:
        정규화된 텍스트
    """
    if expand:
        return _normalizer.normalize_with_expansion(text).normalized
    return _normalizer.normalize(text).normalized
