"""
BizMeta Service - Medical terminology dictionary and ICD code mapping
"""
from typing import List, Dict, Optional, Tuple
from models.text2sql import TermResolution


# ICD-10 코드 매핑 (의료 용어 → ICD 코드)
ICD_CODE_MAP: Dict[str, Tuple[str, str]] = {
    # 내분비/대사 질환
    "당뇨": ("E11", "제2형 당뇨병"),
    "당뇨병": ("E11", "제2형 당뇨병"),
    "제2형 당뇨병": ("E11", "제2형 당뇨병"),
    "제1형 당뇨병": ("E10", "제1형 당뇨병"),
    "고혈압": ("I10", "본태성 고혈압"),
    "혈압": ("I10", "본태성 고혈압"),
    "고지혈증": ("E78", "지질대사장애"),
    "이상지질혈증": ("E78", "지질대사장애"),
    "갑상선기능저하증": ("E03", "갑상선기능저하증"),
    "갑상선기능항진증": ("E05", "갑상선기능항진증"),

    # 암/종양
    "위암": ("C16", "위의 악성 신생물"),
    "폐암": ("C34", "기관지 및 폐의 악성 신생물"),
    "간암": ("C22", "간 및 간내담관의 악성 신생물"),
    "대장암": ("C18", "결장의 악성 신생물"),
    "유방암": ("C50", "유방의 악성 신생물"),
    "전립선암": ("C61", "전립선의 악성 신생물"),
    "췌장암": ("C25", "췌장의 악성 신생물"),
    "갑상선암": ("C73", "갑상선의 악성 신생물"),

    # 심혈관 질환
    "심근경색": ("I21", "급성 심근경색증"),
    "협심증": ("I20", "협심증"),
    "심부전": ("I50", "심부전"),
    "부정맥": ("I49", "기타 심장 부정맥"),
    "뇌졸중": ("I64", "출혈 또는 경색으로 명시되지 않은 뇌졸중"),
    "뇌경색": ("I63", "뇌경색증"),
    "뇌출혈": ("I61", "뇌내출혈"),

    # 호흡기 질환
    "폐렴": ("J18", "상세불명의 폐렴"),
    "천식": ("J45", "천식"),
    "만성폐쇄성폐질환": ("J44", "기타 만성 폐쇄성 폐질환"),
    "COPD": ("J44", "기타 만성 폐쇄성 폐질환"),

    # 소화기 질환
    "위염": ("K29", "위염 및 십이지장염"),
    "위궤양": ("K25", "위궤양"),
    "간경변": ("K74", "간의 섬유증 및 경변증"),
    "급성췌장염": ("K85", "급성 췌장염"),

    # 신장 질환
    "만성신부전": ("N18", "만성 신장병"),
    "급성신부전": ("N17", "급성 신부전"),
    "신증후군": ("N04", "신증후군"),

    # 감염성 질환
    "패혈증": ("A41", "기타 패혈증"),
    "코로나": ("U07.1", "COVID-19"),
    "코로나19": ("U07.1", "COVID-19"),
    "COVID-19": ("U07.1", "COVID-19"),
}

# 동의어 매핑 (구어체/약어 → 표준 용어)
SYNONYM_MAP: Dict[str, str] = {
    # 당뇨 관련
    "당뇨환자": "당뇨병 환자",
    "DM": "당뇨병",
    "DM환자": "당뇨병 환자",

    # 고혈압 관련
    "고혈압환자": "고혈압 환자",
    "HTN": "고혈압",
    "혈압환자": "고혈압 환자",

    # 암 관련
    "Ca": "암",
    "암환자": "암 환자",
    "위ca": "위암",
    "폐ca": "폐암",
    "간ca": "간암",

    # 입퇴원 관련
    "입원환자": "입원 환자",
    "입원한 환자": "입원 환자",
    "입원 중인 환자": "입원 환자",
    "퇴원환자": "퇴원 환자",
    "퇴원한 환자": "퇴원 환자",

    # 외래 관련
    "외래환자": "외래 환자",
    "외래 진료 환자": "외래 환자",

    # 검사 관련
    "혈액검사": "임상검사",
    "피검사": "임상검사",
    "Lab": "임상검사",

    # 시간 관련
    "오늘": "금일",
    "어제": "전일",
    "이번달": "당월",
    "지난달": "전월",
    "올해": "금년",
    "작년": "전년",
}

# 표준 의료 용어 (비즈니스 용어 → 설명)
STANDARD_TERMS: Dict[str, str] = {
    "환자번호": "환자 고유 식별 번호 (PT_NO)",
    "진료과": "진료를 담당하는 과 (DEPT_CD)",
    "입원일자": "환자가 입원한 날짜 (ADM_DT)",
    "퇴원일자": "환자가 퇴원한 날짜 (DSCH_DT)",
    "진단코드": "ICD-10 기준 진단 코드 (ICD_CD)",
    "진단명": "진단 병명 (DIAG_NM)",
    "검사코드": "검사 항목 코드 (TEST_CD)",
    "검사결과": "검사 결과값 (RSLT_VAL)",
    "접수일자": "외래 접수 일자 (RCPT_DT)",
}


class BizMetaService:
    """의료 용어 사전 및 ICD 코드 매핑 서비스"""

    def __init__(self):
        self.icd_map = ICD_CODE_MAP
        self.synonym_map = SYNONYM_MAP
        self.standard_terms = STANDARD_TERMS

    def resolve_term(self, term: str) -> Optional[TermResolution]:
        """단일 용어 해석"""
        term_lower = term.lower().strip()

        # 1. ICD 코드 매핑 검색
        for medical_term, (icd_code, standard_name) in self.icd_map.items():
            if medical_term.lower() in term_lower or term_lower in medical_term.lower():
                return TermResolution(
                    original_term=term,
                    resolved_term=f"{standard_name} (ICD: {icd_code})",
                    term_type="icd_code",
                    confidence=0.95
                )

        # 2. 동의어 매핑 검색
        for synonym, standard in self.synonym_map.items():
            if synonym.lower() in term_lower:
                return TermResolution(
                    original_term=term,
                    resolved_term=standard,
                    term_type="synonym",
                    confidence=0.9
                )

        # 3. 표준 용어 검색
        for standard_term, description in self.standard_terms.items():
            if standard_term in term:
                return TermResolution(
                    original_term=term,
                    resolved_term=f"{standard_term}: {description}",
                    term_type="standard_term",
                    confidence=0.85
                )

        return None

    def resolve_terms_in_question(self, question: str) -> Tuple[str, List[TermResolution]]:
        """질문에서 모든 의료 용어 해석"""
        resolutions: List[TermResolution] = []
        enhanced_question = question

        # ICD 코드 매핑 (질병명 → 코드 추가)
        for medical_term, (icd_code, standard_name) in self.icd_map.items():
            if medical_term in question:
                resolution = TermResolution(
                    original_term=medical_term,
                    resolved_term=f"{standard_name} (ICD: {icd_code})",
                    term_type="icd_code",
                    confidence=0.95
                )
                resolutions.append(resolution)
                # 질문에 ICD 코드 정보 추가
                enhanced_question = enhanced_question.replace(
                    medical_term,
                    f"{medical_term}(ICD:{icd_code})"
                )

        # 동의어 변환
        for synonym, standard in self.synonym_map.items():
            if synonym in question and synonym not in [r.original_term for r in resolutions]:
                resolution = TermResolution(
                    original_term=synonym,
                    resolved_term=standard,
                    term_type="synonym",
                    confidence=0.9
                )
                resolutions.append(resolution)
                enhanced_question = enhanced_question.replace(synonym, standard)

        return enhanced_question, resolutions

    def get_icd_code(self, disease_name: str) -> Optional[str]:
        """질병명으로 ICD 코드 조회"""
        disease_lower = disease_name.lower().strip()
        for term, (code, _) in self.icd_map.items():
            if term.lower() == disease_lower or disease_lower in term.lower():
                return code
        return None

    def get_icd_codes_like(self, pattern: str) -> List[Tuple[str, str, str]]:
        """패턴으로 ICD 코드 목록 조회"""
        pattern_lower = pattern.lower()
        results = []
        for term, (code, name) in self.icd_map.items():
            if pattern_lower in term.lower() or pattern_lower in name.lower():
                results.append((term, code, name))
        return results

    def get_synonyms(self, term: str) -> List[str]:
        """용어의 동의어 목록 조회"""
        synonyms = []
        term_lower = term.lower()

        # 정방향 검색
        for syn, standard in self.synonym_map.items():
            if term_lower in syn.lower() or term_lower in standard.lower():
                synonyms.append(syn)

        return synonyms


# Singleton instance
biz_meta_service = BizMetaService()
