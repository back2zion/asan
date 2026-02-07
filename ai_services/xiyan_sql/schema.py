"""
OMOP CDM 스키마 — M-Schema 형식

XiYanSQL-QwenCoder 모델에 전달하기 위한 M-Schema 형식의 스키마 정의.
서울아산병원 IDP의 7개 핵심 테이블을 기반으로 합니다.

M-Schema 형식:
  【DB_ID】 database_name
  【표(Table)】 table_name ( column1 type comment, ... )
  【외래키(FK)】 table.column = table.column

이 모듈은 SchemaLinker가 사용하는 공유 데이터(SAMPLE_TABLES, TABLE_RELATIONSHIPS,
KEYWORD_TABLE_MAP, ICD_CODE_MAP)도 정의합니다.
data_portal/src/api/services/it_meta.py, biz_meta.py와 동일한 데이터를 유지합니다.
"""

from typing import Dict, List, Tuple


def get_omop_cdm_schema() -> str:
    """OMOP CDM 스키마를 M-Schema 형식으로 반환합니다.

    Returns:
        M-Schema 형식의 전체 스키마 문자열
    """
    return """【DB_ID】 omop_cdm
【표(Table)】 person (
  person_id BIGINT PRIMARY KEY COMMENT '환자 고유 ID',
  gender_concept_id BIGINT COMMENT '성별 개념 ID (8507: 남성, 8532: 여성)',
  year_of_birth INTEGER COMMENT '출생 연도',
  month_of_birth INTEGER COMMENT '출생 월',
  day_of_birth INTEGER COMMENT '출생 일',
  birth_datetime TIMESTAMP COMMENT '출생 일시',
  race_concept_id BIGINT COMMENT '인종 개념 ID',
  ethnicity_concept_id BIGINT COMMENT '민족 개념 ID',
  person_source_value VARCHAR COMMENT '원본 환자번호',
  gender_source_value VARCHAR COMMENT '성별 원본값 (M/F)'
)
【표(Table)】 condition_occurrence (
  condition_occurrence_id BIGINT PRIMARY KEY COMMENT '진단 기록 ID',
  person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person)',
  condition_concept_id BIGINT COMMENT '진단 개념 ID (SNOMED)',
  condition_start_date DATE NOT NULL COMMENT '진단 시작일',
  condition_end_date DATE COMMENT '진단 종료일',
  condition_source_value VARCHAR COMMENT '원본 진단코드 (ICD-9: 25000=당뇨, 4019=고혈압)',
  visit_occurrence_id BIGINT COMMENT '방문 ID (FK → visit_occurrence)'
)
【표(Table)】 visit_occurrence (
  visit_occurrence_id BIGINT PRIMARY KEY COMMENT '방문 기록 ID',
  person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person)',
  visit_concept_id BIGINT COMMENT '방문 유형 (9201: 입원, 9202: 외래, 9203: 응급)',
  visit_start_date DATE NOT NULL COMMENT '방문 시작일',
  visit_end_date DATE COMMENT '방문 종료일',
  visit_source_value VARCHAR COMMENT '원본 방문 유형'
)
【표(Table)】 drug_exposure (
  drug_exposure_id BIGINT PRIMARY KEY COMMENT '약물 처방 ID',
  person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person)',
  drug_concept_id BIGINT COMMENT '약물 개념 ID',
  drug_exposure_start_date DATE NOT NULL COMMENT '처방 시작일',
  drug_exposure_end_date DATE COMMENT '처방 종료일',
  drug_source_value VARCHAR COMMENT '원본 약물 코드'
)
【표(Table)】 measurement (
  measurement_id BIGINT PRIMARY KEY COMMENT '검사 결과 ID',
  person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person)',
  measurement_concept_id BIGINT COMMENT '검사 개념 ID',
  measurement_date DATE NOT NULL COMMENT '검사일',
  value_as_number NUMERIC COMMENT '검사 결과값 (숫자)',
  value_as_concept_id BIGINT COMMENT '검사 결과값 (개념)',
  unit_source_value VARCHAR COMMENT '결과 단위',
  measurement_source_value VARCHAR COMMENT '원본 검사 코드'
)
【표(Table)】 observation (
  observation_id BIGINT PRIMARY KEY COMMENT '관찰 기록 ID',
  person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person)',
  observation_concept_id BIGINT COMMENT '관찰 개념 ID',
  observation_date DATE COMMENT '관찰일',
  observation_type_concept_id BIGINT COMMENT '관찰 유형 개념 ID',
  value_as_number NUMERIC COMMENT '관찰 결과값 (숫자)',
  value_as_string VARCHAR COMMENT '관찰 결과값 (문자)',
  observation_source_value VARCHAR COMMENT '원본 관찰 코드',
  visit_occurrence_id BIGINT COMMENT '방문 ID (FK → visit_occurrence)'
)
【표(Table)】 imaging_study (
  imaging_study_id SERIAL PRIMARY KEY COMMENT '영상 검사 ID',
  person_id INTEGER COMMENT '환자 ID (FK → person)',
  image_filename VARCHAR COMMENT '이미지 파일명 (예: 00000001_000.png)',
  finding_labels VARCHAR COMMENT '소견 (예: Cardiomegaly, Effusion, No Finding)',
  view_position VARCHAR COMMENT '촬영 자세 (PA, AP)',
  patient_age INTEGER COMMENT '환자 나이',
  patient_gender VARCHAR COMMENT '환자 성별 (M/F)',
  image_url VARCHAR COMMENT '이미지 URL (/api/v1/imaging/images/파일명.png)',
  created_at TIMESTAMP COMMENT '생성 일시'
)
【외래키(FK)】 condition_occurrence.person_id = person.person_id
【외래키(FK)】 condition_occurrence.visit_occurrence_id = visit_occurrence.visit_occurrence_id
【외래키(FK)】 visit_occurrence.person_id = person.person_id
【외래키(FK)】 drug_exposure.person_id = person.person_id
【외래키(FK)】 measurement.person_id = person.person_id
【외래키(FK)】 observation.person_id = person.person_id
【외래키(FK)】 imaging_study.person_id = person.person_id"""


# 의료 도메인 참조 정보 (evidence) - OMOP CDM + SNOMED CT codes (Synthea)
MEDICAL_EVIDENCE = {
    "당뇨": "당뇨병(Diabetes Mellitus)의 SNOMED CT 코드는 44054006입니다. condition_occurrence.condition_source_value = '44054006' 조건을 사용합니다.",
    "고혈압": "고혈압(Hypertension)의 SNOMED CT 코드는 38341003입니다. condition_source_value = '38341003' 조건을 사용합니다.",
    "심방세동": "심방세동(Atrial Fibrillation)의 SNOMED CT 코드는 49436004입니다. condition_source_value = '49436004' 조건을 사용합니다.",
    "심근경색": "심근경색(Myocardial Infarction)의 SNOMED CT 코드는 22298006입니다. condition_source_value = '22298006' 조건을 사용합니다.",
    "뇌졸중": "뇌졸중(Stroke)의 SNOMED CT 코드는 230690007입니다. condition_source_value = '230690007' 조건을 사용합니다.",
    "관상동맥": "관상동맥 질환(Coronary arteriosclerosis)의 SNOMED CT 코드는 53741008입니다. condition_source_value = '53741008' 조건을 사용합니다.",
    "기관지염": "급성 기관지염(Acute bronchitis)의 SNOMED CT 코드는 10509002입니다. condition_source_value = '10509002' 조건을 사용합니다.",
    "인두염": "급성 바이러스성 인두염(Acute viral pharyngitis)의 SNOMED CT 코드는 195662009입니다. condition_source_value = '195662009' 조건을 사용합니다.",
    "남성": "성별 조건: person.gender_source_value = 'M' 또는 person.gender_concept_id = 8507",
    "여성": "성별 조건: person.gender_source_value = 'F' 또는 person.gender_concept_id = 8532",
    "입원": "입원 환자: visit_occurrence.visit_concept_id = 9201",
    "외래": "외래 환자: visit_occurrence.visit_concept_id = 9202",
    "응급": "응급 환자: visit_occurrence.visit_concept_id = 9203",
    "X-ray": "흉부 X-ray 영상: imaging_study 테이블 사용. image_url 컬럼에 이미지 경로 있음. finding_labels로 소견 필터 가능.",
    "영상": "영상 검사: imaging_study 테이블 사용. finding_labels 컬럼에 소견 (Cardiomegaly, Effusion, Pneumonia 등) 저장.",
    "촬영": "영상 촬영: imaging_study 테이블 사용. view_position(PA/AP), patient_age, patient_gender 컬럼 제공.",
    "흉부": "흉부 X-ray: imaging_study 테이블에서 조회. image_url로 이미지 확인 가능.",
    # 영상 소견 한글→영문 매핑 (finding_labels는 반드시 영문으로 검색해야 함)
    "폐렴": "폐렴은 영문 Pneumonia입니다. imaging_study.finding_labels ILIKE '%Pneumonia%' 조건을 사용합니다.",
    "심비대": "심비대는 영문 Cardiomegaly입니다. imaging_study.finding_labels ILIKE '%Cardiomegaly%' 조건을 사용합니다.",
    "흉수": "흉수는 영문 Effusion입니다. imaging_study.finding_labels ILIKE '%Effusion%' 조건을 사용합니다.",
    "폐기종": "폐기종은 영문 Emphysema입니다. imaging_study.finding_labels ILIKE '%Emphysema%' 조건을 사용합니다.",
    "침윤": "침윤은 영문 Infiltration입니다. imaging_study.finding_labels ILIKE '%Infiltration%' 조건을 사용합니다.",
    "무기폐": "무기폐는 영문 Atelectasis입니다. imaging_study.finding_labels ILIKE '%Atelectasis%' 조건을 사용합니다.",
    "기흉": "기흉은 영문 Pneumothorax입니다. imaging_study.finding_labels ILIKE '%Pneumothorax%' 조건을 사용합니다.",
    "종괴": "종괴는 영문 Mass입니다. imaging_study.finding_labels ILIKE '%Mass%' 조건을 사용합니다.",
    "결절": "결절은 영문 Nodule입니다. imaging_study.finding_labels ILIKE '%Nodule%' 조건을 사용합니다.",
    "경화": "경화는 영문 Consolidation입니다. imaging_study.finding_labels ILIKE '%Consolidation%' 조건을 사용합니다.",
    "부종": "부종은 영문 Edema입니다. imaging_study.finding_labels ILIKE '%Edema%' 조건을 사용합니다.",
    "섬유화": "섬유화는 영문 Fibrosis입니다. imaging_study.finding_labels ILIKE '%Fibrosis%' 조건을 사용합니다.",
    "탈장": "탈장은 영문 Hernia입니다. imaging_study.finding_labels ILIKE '%Hernia%' 조건을 사용합니다.",
    "흉막비후": "흉막비후는 영문 Pleural_Thickening입니다. imaging_study.finding_labels ILIKE '%Pleural_Thickening%' 조건을 사용합니다.",
    "소견": "imaging_study.finding_labels 컬럼은 영문(Cardiomegaly, Effusion, Pneumonia 등)으로 저장됩니다. 소견 유형별 분포: SELECT finding_labels, COUNT(*) AS cnt FROM imaging_study GROUP BY finding_labels ORDER BY cnt DESC. 전체 건수 포함 시 SUM(COUNT(*)) OVER() AS total 사용. 한글 소견명은 반드시 영문으로 변환하여 검색하세요.",
}


def get_evidence_for_query(query: str) -> str:
    """질의에서 관련 의료 참조 정보를 추출합니다.

    Args:
        query: 사용자 질의 문자열

    Returns:
        관련 참조 정보를 합친 문자열 (없으면 빈 문자열)
    """
    evidence_parts = []
    for keyword, evidence in MEDICAL_EVIDENCE.items():
        if keyword in query:
            evidence_parts.append(evidence)

    return "\n".join(evidence_parts)


# =============================================================================
# 공유 데이터: OMOP CDM 기반 (Synthea 합성 데이터)
# ITMetaService / BizMetaService / SchemaLinker 공통 참조
# =============================================================================

SAMPLE_TABLES: List[Dict] = [
    {
        "physical_name": "person",
        "business_name": "환자",
        "description": "환자 기본 정보 (1,130명). gender_source_value로 성별(M/F) 조회.",
        "domain": "환자",
        "columns": [
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "description": "환자 고유 ID"},
            {"physical_name": "gender_concept_id", "business_name": "성별개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "8507: 남성, 8532: 여성"},
            {"physical_name": "year_of_birth", "business_name": "출생연도", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "description": "출생 연도"},
            {"physical_name": "month_of_birth", "business_name": "출생월", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "description": "출생 월"},
            {"physical_name": "day_of_birth", "business_name": "출생일", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "description": "출생 일"},
            {"physical_name": "birth_datetime", "business_name": "출생일시", "data_type": "TIMESTAMP", "is_pk": False, "is_nullable": True, "description": "출생 일시"},
            {"physical_name": "race_concept_id", "business_name": "인종개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "인종 개념 ID"},
            {"physical_name": "ethnicity_concept_id", "business_name": "민족개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "민족 개념 ID"},
            {"physical_name": "person_source_value", "business_name": "원본환자번호", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "원본 환자번호"},
            {"physical_name": "gender_source_value", "business_name": "성별", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "성별 원본값 (M/F)"},
        ],
    },
    {
        "physical_name": "condition_occurrence",
        "business_name": "진단기록",
        "description": "환자 진단/질환 기록 (7,900건). condition_source_value에 SNOMED CT 코드 저장.",
        "domain": "진료",
        "columns": [
            {"physical_name": "condition_occurrence_id", "business_name": "진단기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "description": "진단 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "description": "환자 ID (FK → person)"},
            {"physical_name": "condition_concept_id", "business_name": "진단개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "진단 SNOMED 개념 ID"},
            {"physical_name": "condition_start_date", "business_name": "진단시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "진단 시작일"},
            {"physical_name": "condition_end_date", "business_name": "진단종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "description": "진단 종료일"},
            {"physical_name": "condition_source_value", "business_name": "진단원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "SNOMED CT 코드 (예: 44054006=당뇨, 38341003=고혈압)"},
            {"physical_name": "visit_occurrence_id", "business_name": "방문ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "방문 ID (FK → visit_occurrence)"},
        ],
    },
    {
        "physical_name": "visit_occurrence",
        "business_name": "방문기록",
        "description": "환자 방문(입원/외래/응급) 기록 (32,153건). visit_concept_id로 방문 유형 구분.",
        "domain": "진료",
        "columns": [
            {"physical_name": "visit_occurrence_id", "business_name": "방문기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "description": "방문 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "description": "환자 ID (FK → person)"},
            {"physical_name": "visit_concept_id", "business_name": "방문유형ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "9201: 입원, 9202: 외래, 9203: 응급"},
            {"physical_name": "visit_start_date", "business_name": "방문시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "방문 시작일"},
            {"physical_name": "visit_end_date", "business_name": "방문종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "description": "방문 종료일"},
            {"physical_name": "visit_source_value", "business_name": "방문원본유형", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "원본 방문 유형"},
        ],
    },
    {
        "physical_name": "drug_exposure",
        "business_name": "약물처방",
        "description": "약물 처방 기록 (13,799건). drug_source_value에 원본 약물 코드 저장.",
        "domain": "처방",
        "columns": [
            {"physical_name": "drug_exposure_id", "business_name": "약물처방ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "description": "약물 처방 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "description": "환자 ID (FK → person)"},
            {"physical_name": "drug_concept_id", "business_name": "약물개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "약물 개념 ID"},
            {"physical_name": "drug_exposure_start_date", "business_name": "처방시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "처방 시작일"},
            {"physical_name": "drug_exposure_end_date", "business_name": "처방종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "description": "처방 종료일"},
            {"physical_name": "drug_source_value", "business_name": "약물원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "원본 약물 코드"},
        ],
    },
    {
        "physical_name": "measurement",
        "business_name": "검사결과",
        "description": "임상 검사 결과 (170,043건). value_as_number에 수치 결과, unit_source_value에 단위.",
        "domain": "검사",
        "columns": [
            {"physical_name": "measurement_id", "business_name": "검사결과ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "description": "검사 결과 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "description": "환자 ID (FK → person)"},
            {"physical_name": "measurement_concept_id", "business_name": "검사개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "검사 개념 ID"},
            {"physical_name": "measurement_date", "business_name": "검사일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "검사 일자"},
            {"physical_name": "value_as_number", "business_name": "결과값", "data_type": "NUMERIC", "is_pk": False, "is_nullable": True, "description": "검사 결과값 (숫자)"},
            {"physical_name": "value_as_concept_id", "business_name": "결과개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "검사 결과값 (개념)"},
            {"physical_name": "unit_source_value", "business_name": "결과단위", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "결과 단위"},
            {"physical_name": "measurement_source_value", "business_name": "검사원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "원본 검사 코드"},
        ],
    },
    {
        "physical_name": "observation",
        "business_name": "관찰기록",
        "description": "환자 관찰/관측 기록 (7,899건). 사회력, 알레르기, 기타 임상 관찰.",
        "domain": "관찰",
        "columns": [
            {"physical_name": "observation_id", "business_name": "관찰기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "description": "관찰 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "description": "환자 ID (FK → person)"},
            {"physical_name": "observation_concept_id", "business_name": "관찰개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "관찰 개념 ID"},
            {"physical_name": "observation_date", "business_name": "관찰일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "description": "관찰 일자"},
            {"physical_name": "observation_type_concept_id", "business_name": "관찰유형ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "관찰 유형 개념 ID"},
            {"physical_name": "value_as_number", "business_name": "결과값숫자", "data_type": "NUMERIC", "is_pk": False, "is_nullable": True, "description": "관찰 결과값 (숫자)"},
            {"physical_name": "value_as_string", "business_name": "결과값문자", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "관찰 결과값 (문자)"},
            {"physical_name": "observation_source_value", "business_name": "관찰원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "원본 관찰 코드"},
            {"physical_name": "visit_occurrence_id", "business_name": "방문ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "방문 ID (FK → visit_occurrence)"},
        ],
    },
    {
        "physical_name": "procedure_occurrence",
        "business_name": "시술기록",
        "description": "시술/수술 기록 (17,333건). procedure_source_value에 원본 시술 코드.",
        "domain": "시술",
        "columns": [
            {"physical_name": "procedure_occurrence_id", "business_name": "시술기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "description": "시술 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "description": "환자 ID (FK → person)"},
            {"physical_name": "procedure_concept_id", "business_name": "시술개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "시술 개념 ID"},
            {"physical_name": "procedure_date", "business_name": "시술일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "시술 일자"},
            {"physical_name": "procedure_source_value", "business_name": "시술원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "description": "원본 시술 코드"},
            {"physical_name": "visit_occurrence_id", "business_name": "방문ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "description": "방문 ID (FK → visit_occurrence)"},
        ],
    },
    {
        "physical_name": "imaging_study",
        "business_name": "흉부X-ray영상",
        "description": "NIH Chest X-ray 흉부 영상 데이터 (112,120건). finding_labels는 영문으로 저장됨.",
        "domain": "영상",
        "columns": [
            {"physical_name": "imaging_study_id", "business_name": "영상ID", "data_type": "SERIAL", "is_pk": True, "is_nullable": False, "description": "영상 고유 식별번호"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "description": "환자 ID (FK → person)"},
            {"physical_name": "image_filename", "business_name": "이미지파일명", "data_type": "VARCHAR(200)", "is_pk": False, "is_nullable": True, "description": "PNG 이미지 파일명"},
            {"physical_name": "finding_labels", "business_name": "소견", "data_type": "VARCHAR(500)", "is_pk": False, "is_nullable": True, "description": "영문 소견 라벨 (Pneumonia, Cardiomegaly 등). 복수 소견은 | 구분."},
            {"physical_name": "view_position", "business_name": "촬영방향", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": True, "description": "PA(후전면) 또는 AP(전후면)"},
            {"physical_name": "patient_age", "business_name": "환자나이", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "description": "촬영 시 환자 나이"},
            {"physical_name": "patient_gender", "business_name": "환자성별", "data_type": "VARCHAR(2)", "is_pk": False, "is_nullable": True, "description": "M: 남성, F: 여성"},
            {"physical_name": "image_url", "business_name": "이미지URL", "data_type": "VARCHAR(500)", "is_pk": False, "is_nullable": True, "description": "이미지 API 경로 (/api/v1/imaging/images/파일명.png)"},
        ],
    },
]

# OMOP CDM FK 관계
TABLE_RELATIONSHIPS: List[Dict] = [
    {"from_table": "condition_occurrence", "from_column": "person_id", "to_table": "person", "to_column": "person_id", "relationship": "many-to-one"},
    {"from_table": "condition_occurrence", "from_column": "visit_occurrence_id", "to_table": "visit_occurrence", "to_column": "visit_occurrence_id", "relationship": "many-to-one"},
    {"from_table": "visit_occurrence", "from_column": "person_id", "to_table": "person", "to_column": "person_id", "relationship": "many-to-one"},
    {"from_table": "drug_exposure", "from_column": "person_id", "to_table": "person", "to_column": "person_id", "relationship": "many-to-one"},
    {"from_table": "measurement", "from_column": "person_id", "to_table": "person", "to_column": "person_id", "relationship": "many-to-one"},
    {"from_table": "observation", "from_column": "person_id", "to_table": "person", "to_column": "person_id", "relationship": "many-to-one"},
    {"from_table": "observation", "from_column": "visit_occurrence_id", "to_table": "visit_occurrence", "to_column": "visit_occurrence_id", "relationship": "many-to-one"},
    {"from_table": "procedure_occurrence", "from_column": "person_id", "to_table": "person", "to_column": "person_id", "relationship": "many-to-one"},
    {"from_table": "procedure_occurrence", "from_column": "visit_occurrence_id", "to_table": "visit_occurrence", "to_column": "visit_occurrence_id", "relationship": "many-to-one"},
    {"from_table": "imaging_study", "from_column": "person_id", "to_table": "person", "to_column": "person_id", "relationship": "many-to-one"},
]

# 키워드 → OMOP CDM 테이블 매핑
KEYWORD_TABLE_MAP: Dict[str, List[str]] = {
    # 환자 관련
    "환자": ["person", "condition_occurrence"],
    "환자수": ["person"],
    "환자 수": ["person"],
    "환자목록": ["person"],
    "환자 목록": ["person"],
    "환자정보": ["person"],
    "환자 정보": ["person"],
    "성별": ["person"],
    "나이": ["person"],
    "연령": ["person"],

    # 방문/입원/외래 관련
    "방문": ["visit_occurrence", "person"],
    "입원": ["visit_occurrence", "person"],
    "입원환자": ["visit_occurrence", "person"],
    "입원 환자": ["visit_occurrence", "person"],
    "퇴원": ["visit_occurrence"],
    "퇴원환자": ["visit_occurrence", "person"],
    "외래": ["visit_occurrence", "person"],
    "외래환자": ["visit_occurrence", "person"],
    "외래 환자": ["visit_occurrence", "person"],
    "응급": ["visit_occurrence", "person"],

    # 진단 관련
    "진단": ["condition_occurrence", "person"],
    "진단코드": ["condition_occurrence"],
    "질병": ["condition_occurrence", "person"],
    "당뇨": ["condition_occurrence", "person"],
    "당뇨병": ["condition_occurrence", "person"],
    "고혈압": ["condition_occurrence", "person"],
    "심방세동": ["condition_occurrence", "person"],
    "심근경색": ["condition_occurrence", "person"],
    "뇌졸중": ["condition_occurrence", "person"],
    "기관지염": ["condition_occurrence", "person"],
    "인두염": ["condition_occurrence", "person"],

    # 약물 관련
    "약물": ["drug_exposure", "person"],
    "처방": ["drug_exposure", "person"],
    "투약": ["drug_exposure", "person"],
    "약": ["drug_exposure", "person"],

    # 검사 관련
    "검사": ["measurement", "person"],
    "검사결과": ["measurement"],
    "임상검사": ["measurement"],
    "혈액검사": ["measurement"],
    "Lab": ["measurement"],

    # 관찰 관련
    "관찰": ["observation", "person"],

    # 시술 관련
    "시술": ["procedure_occurrence", "person"],
    "수술": ["procedure_occurrence", "person"],

    # 영상 관련
    "영상": ["imaging_study"],
    "이미지": ["imaging_study"],
    "X-ray": ["imaging_study"],
    "xray": ["imaging_study"],
    "흉부": ["imaging_study"],
    "chest": ["imaging_study"],
    "촬영": ["imaging_study"],
    "방사선": ["imaging_study"],
    "폐렴": ["imaging_study", "condition_occurrence"],
    "심비대": ["imaging_study"],
    "흉수": ["imaging_study"],
    "기흉": ["imaging_study"],
    "폐기종": ["imaging_study"],
    "무기폐": ["imaging_study"],
    "결절": ["imaging_study"],
    "종괴": ["imaging_study"],
    "침윤": ["imaging_study"],
}

# SNOMED CT 코드 매핑 (Synthea OMOP CDM 데이터)
# condition_occurrence.condition_source_value 에서 사용되는 코드
ICD_CODE_MAP: Dict[str, Tuple[str, str]] = {
    # 내분비/대사 질환
    "당뇨": ("44054006", "제2형 당뇨병"),
    "당뇨병": ("44054006", "제2형 당뇨병"),
    "제2형 당뇨병": ("44054006", "제2형 당뇨병"),
    "고혈압": ("38341003", "고혈압성 장애"),
    "혈압": ("38341003", "고혈압성 장애"),

    # 심혈관 질환
    "심방세동": ("49436004", "심방세동"),
    "관상동맥질환": ("53741008", "관상동맥 죽상경화증"),
    "심근경색": ("22298006", "심근경색"),
    "뇌졸중": ("230690007", "뇌졸중"),

    # 호흡기 질환
    "기관지염": ("10509002", "급성 기관지염"),
    "인두염": ("195662009", "급성 바이러스성 인두염"),
}

# 동의어 매핑 (구어체/약어 → 표준 용어)
SYNONYM_MAP: Dict[str, str] = {
    "당뇨환자": "당뇨병 환자",
    "DM": "당뇨병",
    "DM환자": "당뇨병 환자",
    "고혈압환자": "고혈압 환자",
    "HTN": "고혈압",
    "혈압환자": "고혈압 환자",
    "Ca": "암",
    "암환자": "암 환자",
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
}


def build_m_schema_for_tables(
    tables: List[Dict],
    relationships: List[Dict],
    db_id: str = "asan_cdm",
) -> str:
    """선별된 테이블/FK만으로 M-Schema 문자열을 구성합니다.

    Args:
        tables: SAMPLE_TABLES 형식의 테이블 딕셔너리 리스트
        relationships: TABLE_RELATIONSHIPS 형식의 FK 관계 리스트
        db_id: 데이터베이스 ID

    Returns:
        M-Schema 형식 문자열
    """
    table_names = {t["physical_name"] for t in tables}
    parts = [f"【DB_ID】 {db_id}"]

    for table in tables:
        col_defs = []
        for col in table["columns"]:
            pk_str = " PRIMARY KEY" if col["is_pk"] else ""
            null_str = " NOT NULL" if not col["is_nullable"] else ""
            comment = col.get("description", col["business_name"])
            col_defs.append(
                f"  {col['physical_name']} {col['data_type']}{pk_str}{null_str} COMMENT '{col['business_name']} - {comment}'"
            )
        cols_str = ",\n".join(col_defs)
        parts.append(f"【표(Table)】 {table['physical_name']} (\n{cols_str}\n)")

    for rel in relationships:
        if rel["from_table"] in table_names and rel["to_table"] in table_names:
            parts.append(
                f"【외래키(FK)】 {rel['from_table']}.{rel['from_column']} = {rel['to_table']}.{rel['to_column']}"
            )

    return "\n".join(parts)
