"""
BizMeta Service - Medical terminology dictionary and ICD code mapping
"""
from typing import List, Dict, Optional, Tuple
from models.text2sql import TermResolution


# SNOMED CT 코드 매핑 (Synthea OMOP CDM 데이터용)
# Synthea 데이터는 SNOMED CT 코드 사용 (condition_source_value)
ICD_CODE_MAP: Dict[str, Tuple[str, str]] = {
    # 내분비/대사 질환 (SNOMED CT)
    "당뇨": ("44054006", "제2형 당뇨병"),
    "당뇨병": ("44054006", "제2형 당뇨병"),
    "제2형 당뇨병": ("44054006", "제2형 당뇨병"),
    "고혈압": ("38341003", "고혈압성 장애"),
    "혈압": ("38341003", "고혈압성 장애"),

    # 심혈관 질환 (SNOMED CT)
    "심방세동": ("49436004", "심방세동"),
    "관상동맥질환": ("53741008", "관상동맥 죽상경화증"),
    "심근경색": ("22298006", "심근경색"),
    "뇌졸중": ("230690007", "뇌졸중"),

    # 호흡기 질환 (SNOMED CT)
    "기관지염": ("10509002", "급성 기관지염"),
    "인두염": ("195662009", "급성 바이러스성 인두염"),
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

    # 영상 소견 (한글→영문)
    "폐렴 소견": "Pneumonia 소견",
    "심비대 소견": "Cardiomegaly 소견",
    "흉수 소견": "Effusion 소견",
    "폐기종 소견": "Emphysema 소견",
    "침윤 소견": "Infiltration 소견",
    "무기폐 소견": "Atelectasis 소견",
    "기흉 소견": "Pneumothorax 소견",
    "결절 소견": "Nodule 소견",
    "종괴 소견": "Mass 소견",
    "경화 소견": "Consolidation 소견",
    "부종 소견": "Edema 소견",
    "섬유화 소견": "Fibrosis 소견",

    # 방문 유형
    "외래": "외래(visit_concept_id=9202)",
    "입원": "입원(visit_concept_id=9201)",
    "응급": "응급(visit_concept_id=9203)",
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
