"""
ITMeta Service - OMOP CDM Schema lookup
실제 OMOP CDM 데이터베이스의 테이블/컬럼 정보를 제공합니다.
"""
from typing import List, Dict, Optional
from models.text2sql import SchemaContext


# OMOP CDM 테이블 정의 (Synthea 합성 데이터 기준)
SAMPLE_TABLES = [
    {
        "physical_name": "person",
        "business_name": "환자",
        "description": "환자 기본 정보 (1,130명). gender_source_value로 성별(M/F) 조회.",
        "domain": "환자",
        "columns": [
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "PHI", "description": "환자 고유 ID"},
            {"physical_name": "gender_concept_id", "business_name": "성별개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "8507: 남성, 8532: 여성"},
            {"physical_name": "year_of_birth", "business_name": "출생연도", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "출생 연도"},
            {"physical_name": "month_of_birth", "business_name": "출생월", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "출생 월"},
            {"physical_name": "day_of_birth", "business_name": "출생일", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "출생 일"},
            {"physical_name": "birth_datetime", "business_name": "출생일시", "data_type": "TIMESTAMP", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "출생 일시"},
            {"physical_name": "race_concept_id", "business_name": "인종개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "인종 개념 ID"},
            {"physical_name": "ethnicity_concept_id", "business_name": "민족개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "민족 개념 ID"},
            {"physical_name": "person_source_value", "business_name": "원본환자번호", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "원본 환자번호"},
            {"physical_name": "gender_source_value", "business_name": "성별", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "성별 원본값 (M/F)"},
        ],
    },
    {
        "physical_name": "condition_occurrence",
        "business_name": "진단기록",
        "description": "환자 진단/질환 기록 (7,900건). condition_source_value에 SNOMED CT 코드 저장.",
        "domain": "진료",
        "columns": [
            {"physical_name": "condition_occurrence_id", "business_name": "진단기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "진단 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "condition_concept_id", "business_name": "진단개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "진단 SNOMED 개념 ID"},
            {"physical_name": "condition_start_date", "business_name": "진단시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "진단 시작일"},
            {"physical_name": "condition_end_date", "business_name": "진단종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "진단 종료일"},
            {"physical_name": "condition_source_value", "business_name": "진단원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "SNOMED CT 코드 (예: 44054006=당뇨, 38341003=고혈압)"},
            {"physical_name": "visit_occurrence_id", "business_name": "방문ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "방문 ID (FK → visit_occurrence)"},
        ],
    },
    {
        "physical_name": "visit_occurrence",
        "business_name": "방문기록",
        "description": "환자 방문(입원/외래/응급) 기록 (32,153건). visit_concept_id로 방문 유형 구분.",
        "domain": "진료",
        "columns": [
            {"physical_name": "visit_occurrence_id", "business_name": "방문기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "방문 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "visit_concept_id", "business_name": "방문유형ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "9201: 입원, 9202: 외래, 9203: 응급"},
            {"physical_name": "visit_start_date", "business_name": "방문시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "방문 시작일"},
            {"physical_name": "visit_end_date", "business_name": "방문종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "방문 종료일"},
            {"physical_name": "visit_source_value", "business_name": "방문원본유형", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "원본 방문 유형"},
        ],
    },
    {
        "physical_name": "drug_exposure",
        "business_name": "약물처방",
        "description": "약물 처방 기록 (13,799건). drug_source_value에 원본 약물 코드 저장.",
        "domain": "처방",
        "columns": [
            {"physical_name": "drug_exposure_id", "business_name": "약물처방ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "약물 처방 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "drug_concept_id", "business_name": "약물개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "약물 개념 ID"},
            {"physical_name": "drug_exposure_start_date", "business_name": "처방시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "처방 시작일"},
            {"physical_name": "drug_exposure_end_date", "business_name": "처방종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "처방 종료일"},
            {"physical_name": "drug_source_value", "business_name": "약물원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "원본 약물 코드"},
        ],
    },
    {
        "physical_name": "measurement",
        "business_name": "검사결과",
        "description": "임상 검사 결과 (170,043건). value_as_number에 수치 결과, unit_source_value에 단위.",
        "domain": "검사",
        "columns": [
            {"physical_name": "measurement_id", "business_name": "검사결과ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "검사 결과 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "measurement_concept_id", "business_name": "검사개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "검사 개념 ID"},
            {"physical_name": "measurement_date", "business_name": "검사일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "검사 일자"},
            {"physical_name": "value_as_number", "business_name": "결과값", "data_type": "NUMERIC", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "검사 결과값 (숫자)"},
            {"physical_name": "value_as_concept_id", "business_name": "결과개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "검사 결과값 (개념)"},
            {"physical_name": "unit_source_value", "business_name": "결과단위", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "결과 단위"},
            {"physical_name": "measurement_source_value", "business_name": "검사원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "원본 검사 코드"},
        ],
    },
    {
        "physical_name": "observation",
        "business_name": "관찰기록",
        "description": "환자 관찰/관측 기록 (7,899건). 사회력, 알레르기, 기타 임상 관찰.",
        "domain": "관찰",
        "columns": [
            {"physical_name": "observation_id", "business_name": "관찰기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "관찰 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "observation_concept_id", "business_name": "관찰개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "관찰 개념 ID"},
            {"physical_name": "observation_date", "business_name": "관찰일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "관찰 일자"},
            {"physical_name": "observation_type_concept_id", "business_name": "관찰유형ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "관찰 유형 개념 ID"},
            {"physical_name": "value_as_number", "business_name": "결과값숫자", "data_type": "NUMERIC", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "관찰 결과값 (숫자)"},
            {"physical_name": "value_as_string", "business_name": "결과값문자", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "관찰 결과값 (문자)"},
            {"physical_name": "observation_source_value", "business_name": "관찰원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "원본 관찰 코드"},
            {"physical_name": "visit_occurrence_id", "business_name": "방문ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "방문 ID (FK → visit_occurrence)"},
        ],
    },
    {
        "physical_name": "procedure_occurrence",
        "business_name": "시술기록",
        "description": "시술/수술 기록 (17,333건). procedure_source_value에 원본 시술 코드.",
        "domain": "시술",
        "columns": [
            {"physical_name": "procedure_occurrence_id", "business_name": "시술기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "시술 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "procedure_concept_id", "business_name": "시술개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "시술 개념 ID"},
            {"physical_name": "procedure_date", "business_name": "시술일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "시술 일자"},
            {"physical_name": "procedure_source_value", "business_name": "시술원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "원본 시술 코드"},
            {"physical_name": "visit_occurrence_id", "business_name": "방문ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "방문 ID (FK → visit_occurrence)"},
        ],
    },
    {
        "physical_name": "imaging_study",
        "business_name": "흉부X-ray영상",
        "description": "NIH Chest X-ray 흉부 영상 데이터 (112,120건). finding_labels는 영문으로 저장됨.",
        "domain": "영상",
        "columns": [
            {"physical_name": "imaging_study_id", "business_name": "영상ID", "data_type": "SERIAL", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "영상 고유 식별번호"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "image_filename", "business_name": "이미지파일명", "data_type": "VARCHAR(200)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "PNG 이미지 파일명"},
            {"physical_name": "finding_labels", "business_name": "소견", "data_type": "VARCHAR(500)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "영문 소견 라벨 (Pneumonia, Cardiomegaly 등). 복수 소견은 | 구분."},
            {"physical_name": "view_position", "business_name": "촬영방향", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "PA(후전면) 또는 AP(전후면)"},
            {"physical_name": "patient_age", "business_name": "환자나이", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "촬영 시 환자 나이"},
            {"physical_name": "patient_gender", "business_name": "환자성별", "data_type": "VARCHAR(2)", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "M: 남성, F: 여성"},
            {"physical_name": "image_url", "business_name": "이미지URL", "data_type": "VARCHAR(500)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "이미지 API 경로 (/api/v1/imaging/images/파일명.png)"},
        ],
    },
]

# OMOP CDM FK 관계
TABLE_RELATIONSHIPS = [
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
KEYWORD_TABLE_MAP = {
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
    "ICD": ["condition_occurrence"],
    "SNOMED": ["condition_occurrence"],
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


class ITMetaService:
    """OMOP CDM 스키마 조회 서비스"""

    def __init__(self):
        self.tables = SAMPLE_TABLES
        self.relationships = TABLE_RELATIONSHIPS
        self.keyword_map = KEYWORD_TABLE_MAP

    def get_all_tables(self) -> List[Dict]:
        """모든 테이블 조회"""
        return self.tables

    def get_table(self, table_name: str) -> Optional[Dict]:
        """테이블명으로 조회"""
        table_lower = table_name.lower()
        for table in self.tables:
            if table["physical_name"].lower() == table_lower:
                return table
        return None

    def get_tables_by_keyword(self, keyword: str) -> List[Dict]:
        """키워드로 관련 테이블 조회"""
        table_names = set()

        # 1. 키워드 맵에서 검색
        for kw, tables in self.keyword_map.items():
            if kw in keyword or keyword in kw:
                table_names.update(tables)

        # 2. 테이블/컬럼 이름에서 검색
        keyword_lower = keyword.lower()
        for table in self.tables:
            if (keyword_lower in table["physical_name"].lower() or
                keyword_lower in table["business_name"].lower() or
                keyword_lower in table["description"].lower()):
                table_names.add(table["physical_name"])

            for col in table["columns"]:
                if (keyword_lower in col["physical_name"].lower() or
                    keyword_lower in col["business_name"].lower() or
                    keyword_lower in col.get("description", "").lower()):
                    table_names.add(table["physical_name"])

        # 3. 테이블 객체로 변환
        return [t for t in self.tables if t["physical_name"] in table_names]

    def get_schema_context(self, keywords: List[str]) -> SchemaContext:
        """키워드 기반 스키마 컨텍스트 생성"""
        relevant_tables = set()
        for kw in keywords:
            tables = self.get_tables_by_keyword(kw)
            for t in tables:
                relevant_tables.add(t["physical_name"])

        # 기본 테이블이 없으면 person 테이블 추가
        if not relevant_tables:
            relevant_tables.add("person")

        # person과 함께 사용되는 테이블이 있으면 person 자동 포함
        if relevant_tables and "person" not in relevant_tables:
            for rel in self.relationships:
                if rel["from_table"] in relevant_tables and rel["to_table"] == "person":
                    relevant_tables.add("person")
                    break

        # 테이블 정보 수집
        table_list = []
        column_list = []
        for table in self.tables:
            if table["physical_name"] in relevant_tables:
                table_list.append(table["physical_name"])
                for col in table["columns"]:
                    column_list.append({
                        "table": table["physical_name"],
                        "physical_name": col["physical_name"],
                        "business_name": col["business_name"],
                        "data_type": col["data_type"],
                        "is_pk": col["is_pk"],
                        "description": col.get("description", ""),
                    })

        # 관계 정보 수집
        rel_list = []
        for rel in self.relationships:
            if rel["from_table"] in relevant_tables or rel["to_table"] in relevant_tables:
                rel_list.append(rel)

        # DDL 컨텍스트 생성
        ddl_parts = []
        for table in self.tables:
            if table["physical_name"] in relevant_tables:
                ddl_parts.append(self._generate_ddl(table))

        return SchemaContext(
            tables=table_list,
            columns=column_list,
            relationships=rel_list,
            ddl_context="\n\n".join(ddl_parts)
        )

    def _generate_ddl(self, table: Dict) -> str:
        """테이블 DDL 생성"""
        lines = [f"-- {table['business_name']}: {table['description']}"]
        lines.append(f"CREATE TABLE {table['physical_name']} (")

        col_defs = []
        pk_cols = []
        for col in table["columns"]:
            null_str = "" if col["is_nullable"] else " NOT NULL"
            col_defs.append(f"    {col['physical_name']} {col['data_type']}{null_str}  -- {col['business_name']}")
            if col["is_pk"]:
                pk_cols.append(col["physical_name"])

        lines.extend([f"{c}," for c in col_defs[:-1]])
        lines.append(f"{col_defs[-1]}")

        if pk_cols:
            lines.append(f"    , PRIMARY KEY ({', '.join(pk_cols)})")

        lines.append(");")
        return "\n".join(lines)

    def get_join_path(self, table1: str, table2: str) -> Optional[Dict]:
        """두 테이블 간의 조인 경로 반환"""
        table1_lower = table1.lower()
        table2_lower = table2.lower()

        for rel in self.relationships:
            if ((rel["from_table"].lower() == table1_lower and rel["to_table"].lower() == table2_lower) or
                (rel["from_table"].lower() == table2_lower and rel["to_table"].lower() == table1_lower)):
                return rel

        return None


# Singleton instance
it_meta_service = ITMetaService()
