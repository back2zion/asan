"""
OMOP CDM 스키마 — M-Schema 형식

XiYanSQL-QwenCoder 모델에 전달하기 위한 M-Schema 형식의 스키마 정의.
서울아산병원 IDP의 OMOP CDM 18개 전체 테이블을 기반으로 합니다.

M-Schema 형식:
  【DB_ID】 database_name
  【표(Table)】 table_name ( column1 type comment, ... )
  【외래키(FK)】 table.column = table.column

이 모듈은 SchemaLinker가 사용하는 공유 데이터(SAMPLE_TABLES, TABLE_RELATIONSHIPS,
KEYWORD_TABLE_MAP, ICD_CODE_MAP)도 정의합니다.
data_portal/src/api/services/it_meta.py, biz_meta.py와 동일한 데이터를 유지합니다.
"""

from typing import Dict, List, Tuple

# 데이터 상수는 schema_data.py에서 가져옴 (하위 호환 유지)
from ai_services.xiyan_sql.schema_data import (  # noqa: F401
    MEDICAL_EVIDENCE,
    SAMPLE_TABLES,
    TABLE_RELATIONSHIPS,
    KEYWORD_TABLE_MAP,
    ICD_CODE_MAP,
    SYNONYM_MAP,
)


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
【표(Table)】 procedure_occurrence (
  procedure_occurrence_id BIGINT PRIMARY KEY COMMENT '시술 기록 ID',
  person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person)',
  procedure_concept_id BIGINT COMMENT '시술 개념 ID',
  procedure_date DATE NOT NULL COMMENT '시술일',
  procedure_source_value VARCHAR COMMENT '원본 시술 코드',
  visit_occurrence_id BIGINT COMMENT '방문 ID (FK → visit_occurrence)'
)
【표(Table)】 cost (
  cost_id BIGINT PRIMARY KEY COMMENT '비용 기록 ID',
  cost_event_id BIGINT NOT NULL COMMENT '비용 발생 이벤트 ID',
  cost DOUBLE PRECISION COMMENT '비용 금액',
  incurred_date DATE COMMENT '비용 발생일',
  paid_date DATE COMMENT '지급일',
  revenue_code_source_value VARCHAR COMMENT '수익 코드 원본값',
  drg_source_value VARCHAR COMMENT 'DRG 코드 원본값',
  person_person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person.person_id)',
  payer_plan_period_payer_plan_period_id BIGINT NOT NULL COMMENT '보험기간 ID (FK → payer_plan_period)'
)
【표(Table)】 payer_plan_period (
  payer_plan_period_id BIGINT PRIMARY KEY COMMENT '보험기간 기록 ID',
  contract_person_id BIGINT COMMENT '계약자(환자) ID (FK → person.person_id)',
  payer_plan_period_start_date DATE NOT NULL COMMENT '보험 시작일',
  payer_plan_period_end_date DATE NOT NULL COMMENT '보험 종료일',
  payer_source_value VARCHAR COMMENT '보험자 원본값 (보험사명)',
  plan_source_value VARCHAR COMMENT '보험 플랜 원본값',
  person_person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person.person_id)'
)
【표(Table)】 condition_era (
  condition_era_id BIGINT PRIMARY KEY COMMENT '진단 기간 ID',
  person_id BIGINT COMMENT '환자 ID (FK → person)',
  condition_concept_id BIGINT COMMENT '진단 개념 ID (SNOMED)',
  condition_era_start_date DATE COMMENT '진단 기간 시작일',
  condition_era_end_date DATE COMMENT '진단 기간 종료일',
  condition_occurrence_count INTEGER COMMENT '해당 기간 내 진단 발생 횟수'
)
【표(Table)】 drug_era (
  drug_era_id BIGINT PRIMARY KEY COMMENT '약물 투여 기간 ID',
  person_id BIGINT COMMENT '환자 ID (FK → person)',
  drug_concept_id BIGINT COMMENT '약물 개념 ID',
  drug_era_start_date DATE COMMENT '약물 투여 시작일',
  drug_era_end_date DATE COMMENT '약물 투여 종료일',
  drug_exposure_count INTEGER COMMENT '해당 기간 내 처방 횟수',
  gap_days INTEGER COMMENT '투여 중단 일수'
)
【표(Table)】 device_exposure (
  device_exposure_id BIGINT PRIMARY KEY COMMENT '의료기기 사용 ID',
  device_concept_id BIGINT NOT NULL COMMENT '기기 개념 ID',
  device_exposure_start_date DATE NOT NULL COMMENT '기기 사용 시작일',
  device_exposure_end_date DATE COMMENT '기기 사용 종료일',
  quantity INTEGER COMMENT '사용 수량',
  device_source_value VARCHAR COMMENT '기기 원본 코드',
  person_person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person.person_id)',
  visit_occurrence_visit_occurrence_id BIGINT NOT NULL COMMENT '방문 ID (FK → visit_occurrence)'
)
【표(Table)】 observation_period (
  observation_period_id BIGINT PRIMARY KEY COMMENT '관찰 기간 ID',
  person_id BIGINT COMMENT '환자 ID (FK → person)',
  observation_period_start_date DATE COMMENT '관찰 시작일',
  observation_period_end_date DATE COMMENT '관찰 종료일',
  period_type_concept_id BIGINT COMMENT '기간 유형 개념 ID'
)
【표(Table)】 visit_detail (
  visit_detail_id BIGINT PRIMARY KEY COMMENT '방문 상세 ID',
  visit_detail_concept_id BIGINT NOT NULL COMMENT '방문 상세 유형 ID',
  visit_detail_start_date DATE NOT NULL COMMENT '상세 방문 시작일',
  visit_detail_end_date DATE NOT NULL COMMENT '상세 방문 종료일',
  visit_detail_source_value VARCHAR COMMENT '방문 상세 원본값',
  person_id BIGINT NOT NULL COMMENT '환자 ID (FK → person)',
  provider_id BIGINT NOT NULL COMMENT '의료진 ID (FK → provider)',
  care_site_id BIGINT NOT NULL COMMENT '진료소 ID (FK → care_site)',
  visit_occurrence_id BIGINT NOT NULL COMMENT '방문 ID (FK → visit_occurrence)'
)
【표(Table)】 provider (
  provider_id BIGINT PRIMARY KEY COMMENT '의료진 ID',
  provider_name VARCHAR COMMENT '의료진 이름',
  npi VARCHAR COMMENT '국가 제공자 식별번호 (NPI)',
  specialty_concept_id BIGINT COMMENT '전문 분야 개념 ID',
  specialty_source_value VARCHAR COMMENT '전문 분야 원본값',
  gender_source_value VARCHAR COMMENT '성별 원본값',
  care_site_care_site_id BIGINT NOT NULL COMMENT '소속 진료소 ID (FK → care_site)'
)
【표(Table)】 care_site (
  care_site_id BIGINT PRIMARY KEY COMMENT '진료소 ID',
  care_site_name VARCHAR COMMENT '진료소 이름',
  place_of_service_concept_id INTEGER NOT NULL COMMENT '서비스 장소 유형 ID',
  care_site_source_value VARCHAR COMMENT '진료소 원본 코드',
  location_location_id BIGINT NOT NULL COMMENT '위치 ID (FK → location)'
)
【표(Table)】 location (
  location_id BIGINT PRIMARY KEY COMMENT '위치 ID',
  city VARCHAR COMMENT '도시',
  state VARCHAR COMMENT '주/도',
  zip VARCHAR COMMENT '우편번호',
  county VARCHAR COMMENT '군/구',
  latitude REAL COMMENT '위도',
  longitude REAL COMMENT '경도'
)
【외래키(FK)】 condition_occurrence.person_id = person.person_id
【외래키(FK)】 condition_occurrence.visit_occurrence_id = visit_occurrence.visit_occurrence_id
【외래키(FK)】 visit_occurrence.person_id = person.person_id
【외래키(FK)】 drug_exposure.person_id = person.person_id
【외래키(FK)】 measurement.person_id = person.person_id
【외래키(FK)】 observation.person_id = person.person_id
【외래키(FK)】 observation.visit_occurrence_id = visit_occurrence.visit_occurrence_id
【외래키(FK)】 procedure_occurrence.person_id = person.person_id
【외래키(FK)】 procedure_occurrence.visit_occurrence_id = visit_occurrence.visit_occurrence_id
【외래키(FK)】 imaging_study.person_id = person.person_id
【외래키(FK)】 cost.person_person_id = person.person_id
【외래키(FK)】 cost.payer_plan_period_payer_plan_period_id = payer_plan_period.payer_plan_period_id
【외래키(FK)】 payer_plan_period.contract_person_id = person.person_id
【외래키(FK)】 payer_plan_period.person_person_id = person.person_id
【외래키(FK)】 condition_era.person_id = person.person_id
【외래키(FK)】 drug_era.person_id = person.person_id
【외래키(FK)】 device_exposure.person_person_id = person.person_id
【외래키(FK)】 device_exposure.visit_occurrence_visit_occurrence_id = visit_occurrence.visit_occurrence_id
【외래키(FK)】 observation_period.person_id = person.person_id
【외래키(FK)】 visit_detail.person_id = person.person_id
【외래키(FK)】 visit_detail.provider_id = provider.provider_id
【외래키(FK)】 visit_detail.care_site_id = care_site.care_site_id
【외래키(FK)】 visit_detail.visit_occurrence_id = visit_occurrence.visit_occurrence_id
【외래키(FK)】 provider.care_site_care_site_id = care_site.care_site_id
【외래키(FK)】 care_site.location_location_id = location.location_id"""


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
