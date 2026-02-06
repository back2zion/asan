"""
OMOP CDM 스키마 — M-Schema 형식

XiYanSQL-QwenCoder 모델에 전달하기 위한 M-Schema 형식의 스키마 정의.
서울아산병원 IDP의 5개 핵심 테이블을 기반으로 합니다.

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
【외래키(FK)】 condition_occurrence.person_id = person.person_id
【외래키(FK)】 condition_occurrence.visit_occurrence_id = visit_occurrence.visit_occurrence_id
【외래키(FK)】 visit_occurrence.person_id = person.person_id
【외래키(FK)】 drug_exposure.person_id = person.person_id
【외래키(FK)】 measurement.person_id = person.person_id"""


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
# 공유 데이터: ITMetaService / BizMetaService 동일 데이터
# (data_portal 모듈과 직접 import 불가하므로 여기서 정의)
# =============================================================================

SAMPLE_TABLES: List[Dict] = [
    {
        "physical_name": "PT_BSNF",
        "business_name": "환자기본정보",
        "description": "환자의 기본 인적사항 및 등록 정보를 관리하는 마스터 테이블",
        "domain": "환자",
        "columns": [
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": True, "is_nullable": False, "description": "환자 고유 식별번호"},
            {"physical_name": "PT_NM", "business_name": "환자성명", "data_type": "VARCHAR(50)", "is_pk": False, "is_nullable": False, "description": "환자 이름"},
            {"physical_name": "BRTH_DT", "business_name": "생년월일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "description": "환자 생년월일"},
            {"physical_name": "SEX_CD", "business_name": "성별코드", "data_type": "CHAR(1)", "is_pk": False, "is_nullable": True, "description": "M: 남성, F: 여성"},
            {"physical_name": "RGST_DT", "business_name": "등록일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "최초 등록 일자"},
        ],
    },
    {
        "physical_name": "OPD_RCPT",
        "business_name": "외래접수",
        "description": "외래 진료 접수 정보를 관리하는 테이블",
        "domain": "진료",
        "columns": [
            {"physical_name": "RCPT_NO", "business_name": "접수번호", "data_type": "VARCHAR(15)", "is_pk": True, "is_nullable": False, "description": "접수 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "description": "환자번호 (FK)"},
            {"physical_name": "DEPT_CD", "business_name": "진료과코드", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "description": "진료과 코드"},
            {"physical_name": "DR_ID", "business_name": "담당의ID", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": True, "description": "담당 의사 ID"},
            {"physical_name": "RCPT_DT", "business_name": "접수일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "접수 일자"},
            {"physical_name": "RCPT_TM", "business_name": "접수시간", "data_type": "TIME", "is_pk": False, "is_nullable": False, "description": "접수 시간"},
        ],
    },
    {
        "physical_name": "IPD_ADM",
        "business_name": "입원",
        "description": "입원 환자 정보를 관리하는 테이블",
        "domain": "진료",
        "columns": [
            {"physical_name": "ADM_NO", "business_name": "입원번호", "data_type": "VARCHAR(15)", "is_pk": True, "is_nullable": False, "description": "입원 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "description": "환자번호 (FK)"},
            {"physical_name": "ADM_DT", "business_name": "입원일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "입원 일자"},
            {"physical_name": "DSCH_DT", "business_name": "퇴원일자", "data_type": "DATE", "is_pk": False, "is_nullable": True, "description": "퇴원 일자"},
            {"physical_name": "WARD_CD", "business_name": "병동코드", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "description": "병동 코드"},
            {"physical_name": "ROOM_NO", "business_name": "병실번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": True, "description": "병실 번호"},
        ],
    },
    {
        "physical_name": "LAB_RSLT",
        "business_name": "검사결과",
        "description": "임상검사 결과 정보를 관리하는 테이블",
        "domain": "검사",
        "columns": [
            {"physical_name": "RSLT_NO", "business_name": "결과번호", "data_type": "VARCHAR(20)", "is_pk": True, "is_nullable": False, "description": "검사결과 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "description": "환자번호 (FK)"},
            {"physical_name": "TEST_CD", "business_name": "검사코드", "data_type": "VARCHAR(20)", "is_pk": False, "is_nullable": False, "description": "검사 항목 코드"},
            {"physical_name": "TEST_NM", "business_name": "검사명", "data_type": "VARCHAR(100)", "is_pk": False, "is_nullable": False, "description": "검사 항목명"},
            {"physical_name": "RSLT_VAL", "business_name": "결과값", "data_type": "VARCHAR(100)", "is_pk": False, "is_nullable": True, "description": "검사 결과 값"},
            {"physical_name": "RSLT_UNIT", "business_name": "결과단위", "data_type": "VARCHAR(20)", "is_pk": False, "is_nullable": True, "description": "결과 단위"},
            {"physical_name": "TEST_DT", "business_name": "검사일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "검사 일자"},
        ],
    },
    {
        "physical_name": "DIAG_INFO",
        "business_name": "진단정보",
        "description": "환자 진단 정보를 관리하는 테이블",
        "domain": "진료",
        "columns": [
            {"physical_name": "DIAG_NO", "business_name": "진단번호", "data_type": "VARCHAR(20)", "is_pk": True, "is_nullable": False, "description": "진단 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "description": "환자번호 (FK)"},
            {"physical_name": "ICD_CD", "business_name": "진단코드", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "description": "ICD-10 진단 코드"},
            {"physical_name": "DIAG_NM", "business_name": "진단명", "data_type": "VARCHAR(200)", "is_pk": False, "is_nullable": False, "description": "진단명"},
            {"physical_name": "DIAG_DT", "business_name": "진단일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "description": "진단 일자"},
            {"physical_name": "DIAG_TP", "business_name": "진단유형", "data_type": "CHAR(1)", "is_pk": False, "is_nullable": True, "description": "M: 주진단, S: 부진단"},
        ],
    },
]

TABLE_RELATIONSHIPS: List[Dict] = [
    {"from_table": "OPD_RCPT", "from_column": "PT_NO", "to_table": "PT_BSNF", "to_column": "PT_NO", "relationship": "many-to-one"},
    {"from_table": "IPD_ADM", "from_column": "PT_NO", "to_table": "PT_BSNF", "to_column": "PT_NO", "relationship": "many-to-one"},
    {"from_table": "LAB_RSLT", "from_column": "PT_NO", "to_table": "PT_BSNF", "to_column": "PT_NO", "relationship": "many-to-one"},
    {"from_table": "DIAG_INFO", "from_column": "PT_NO", "to_table": "PT_BSNF", "to_column": "PT_NO", "relationship": "many-to-one"},
]

KEYWORD_TABLE_MAP: Dict[str, List[str]] = {
    # 환자 관련
    "환자": ["PT_BSNF", "DIAG_INFO"],
    "환자수": ["PT_BSNF"],
    "환자 수": ["PT_BSNF"],
    "환자목록": ["PT_BSNF"],
    "환자 목록": ["PT_BSNF"],
    "환자정보": ["PT_BSNF"],
    "환자 정보": ["PT_BSNF"],
    # 입원 관련
    "입원": ["IPD_ADM", "PT_BSNF"],
    "입원환자": ["IPD_ADM", "PT_BSNF"],
    "입원 환자": ["IPD_ADM", "PT_BSNF"],
    "퇴원": ["IPD_ADM"],
    "퇴원환자": ["IPD_ADM", "PT_BSNF"],
    "병동": ["IPD_ADM"],
    "병실": ["IPD_ADM"],
    # 외래 관련
    "외래": ["OPD_RCPT", "PT_BSNF"],
    "외래환자": ["OPD_RCPT", "PT_BSNF"],
    "외래 환자": ["OPD_RCPT", "PT_BSNF"],
    "접수": ["OPD_RCPT"],
    "진료과": ["OPD_RCPT"],
    # 진단 관련
    "진단": ["DIAG_INFO", "PT_BSNF"],
    "진단코드": ["DIAG_INFO"],
    "ICD": ["DIAG_INFO"],
    "질병": ["DIAG_INFO", "PT_BSNF"],
    "당뇨": ["DIAG_INFO", "PT_BSNF"],
    "당뇨병": ["DIAG_INFO", "PT_BSNF"],
    "고혈압": ["DIAG_INFO", "PT_BSNF"],
    "암": ["DIAG_INFO", "PT_BSNF"],
    "위암": ["DIAG_INFO", "PT_BSNF"],
    "폐암": ["DIAG_INFO", "PT_BSNF"],
    # 검사 관련
    "검사": ["LAB_RSLT", "PT_BSNF"],
    "검사결과": ["LAB_RSLT"],
    "임상검사": ["LAB_RSLT"],
    "혈액검사": ["LAB_RSLT"],
    "Lab": ["LAB_RSLT"],
}

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
    "당뇨환자": "당뇨병 환자",
    "DM": "당뇨병",
    "DM환자": "당뇨병 환자",
    "고혈압환자": "고혈압 환자",
    "HTN": "고혈압",
    "혈압환자": "고혈압 환자",
    "Ca": "암",
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
