"""
ITMeta Service - Schema lookup using SAMPLE_TABLES from semantic.py
"""
from typing import List, Dict, Optional
from models.text2sql import SchemaContext


# Import SAMPLE_TABLES from semantic router
SAMPLE_TABLES = [
    {
        "physical_name": "PT_BSNF",
        "business_name": "환자기본정보",
        "description": "환자의 기본 인적사항 및 등록 정보를 관리하는 마스터 테이블",
        "domain": "환자",
        "columns": [
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": True, "is_nullable": False, "sensitivity": "PHI", "description": "환자 고유 식별번호"},
            {"physical_name": "PT_NM", "business_name": "환자성명", "data_type": "VARCHAR(50)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 이름"},
            {"physical_name": "BRTH_DT", "business_name": "생년월일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "환자 생년월일"},
            {"physical_name": "SEX_CD", "business_name": "성별코드", "data_type": "CHAR(1)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "M: 남성, F: 여성"},
            {"physical_name": "RGST_DT", "business_name": "등록일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "최초 등록 일자"},
        ],
    },
    {
        "physical_name": "OPD_RCPT",
        "business_name": "외래접수",
        "description": "외래 진료 접수 정보를 관리하는 테이블",
        "domain": "진료",
        "columns": [
            {"physical_name": "RCPT_NO", "business_name": "접수번호", "data_type": "VARCHAR(15)", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "접수 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자번호 (FK)"},
            {"physical_name": "DEPT_CD", "business_name": "진료과코드", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "진료과 코드"},
            {"physical_name": "DR_ID", "business_name": "담당의ID", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "담당 의사 ID"},
            {"physical_name": "RCPT_DT", "business_name": "접수일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "접수 일자"},
            {"physical_name": "RCPT_TM", "business_name": "접수시간", "data_type": "TIME", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "접수 시간"},
        ],
    },
    {
        "physical_name": "IPD_ADM",
        "business_name": "입원",
        "description": "입원 환자 정보를 관리하는 테이블",
        "domain": "진료",
        "columns": [
            {"physical_name": "ADM_NO", "business_name": "입원번호", "data_type": "VARCHAR(15)", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "입원 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자번호 (FK)"},
            {"physical_name": "ADM_DT", "business_name": "입원일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "입원 일자"},
            {"physical_name": "DSCH_DT", "business_name": "퇴원일자", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "퇴원 일자"},
            {"physical_name": "WARD_CD", "business_name": "병동코드", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "병동 코드"},
            {"physical_name": "ROOM_NO", "business_name": "병실번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "병실 번호"},
        ],
    },
    {
        "physical_name": "LAB_RSLT",
        "business_name": "검사결과",
        "description": "임상검사 결과 정보를 관리하는 테이블",
        "domain": "검사",
        "columns": [
            {"physical_name": "RSLT_NO", "business_name": "결과번호", "data_type": "VARCHAR(20)", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "검사결과 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자번호 (FK)"},
            {"physical_name": "TEST_CD", "business_name": "검사코드", "data_type": "VARCHAR(20)", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "검사 항목 코드"},
            {"physical_name": "TEST_NM", "business_name": "검사명", "data_type": "VARCHAR(100)", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "검사 항목명"},
            {"physical_name": "RSLT_VAL", "business_name": "결과값", "data_type": "VARCHAR(100)", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "검사 결과 값"},
            {"physical_name": "RSLT_UNIT", "business_name": "결과단위", "data_type": "VARCHAR(20)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "결과 단위"},
            {"physical_name": "TEST_DT", "business_name": "검사일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "검사 일자"},
        ],
    },
    {
        "physical_name": "DIAG_INFO",
        "business_name": "진단정보",
        "description": "환자 진단 정보를 관리하는 테이블",
        "domain": "진료",
        "columns": [
            {"physical_name": "DIAG_NO", "business_name": "진단번호", "data_type": "VARCHAR(20)", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "진단 고유번호"},
            {"physical_name": "PT_NO", "business_name": "환자번호", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자번호 (FK)"},
            {"physical_name": "ICD_CD", "business_name": "진단코드", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "ICD-10 진단 코드"},
            {"physical_name": "DIAG_NM", "business_name": "진단명", "data_type": "VARCHAR(200)", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "진단명"},
            {"physical_name": "DIAG_DT", "business_name": "진단일자", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "진단 일자"},
            {"physical_name": "DIAG_TP", "business_name": "진단유형", "data_type": "CHAR(1)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "M: 주진단, S: 부진단"},
        ],
    },
    {
        "physical_name": "imaging_study",
        "business_name": "흉부X-ray영상",
        "description": "NIH Chest X-ray 흉부 영상 데이터 (112,120건). finding_labels는 영문으로 저장됨.",
        "domain": "영상",
        "columns": [
            {"physical_name": "imaging_study_id", "business_name": "영상ID", "data_type": "SERIAL", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "영상 고유 식별번호"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "환자 ID (person 테이블 FK)"},
            {"physical_name": "image_filename", "business_name": "이미지파일명", "data_type": "VARCHAR(200)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "PNG 이미지 파일명"},
            {"physical_name": "finding_labels", "business_name": "소견", "data_type": "VARCHAR(500)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "영문 소견 라벨. 값: Atelectasis, Cardiomegaly, Consolidation, Edema, Effusion, Emphysema, Fibrosis, Hernia, Infiltration, Mass, No Finding, Nodule, Pleural_Thickening, Pneumonia, Pneumothorax. 복수 소견은 | 구분. 한글→영문 매핑 필수: 폐렴=Pneumonia, 심비대=Cardiomegaly, 흉수=Effusion, 폐기종=Emphysema, 침윤=Infiltration, 무기폐=Atelectasis, 기흉=Pneumothorax, 종괴=Mass, 결절=Nodule, 경화=Consolidation, 부종=Edema, 섬유화=Fibrosis, 탈장=Hernia, 흉막비후=Pleural_Thickening"},
            {"physical_name": "view_position", "business_name": "촬영방향", "data_type": "VARCHAR(10)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "PA(후전면) 또는 AP(전후면)"},
            {"physical_name": "patient_age", "business_name": "환자나이", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "촬영 시 환자 나이"},
            {"physical_name": "patient_gender", "business_name": "환자성별", "data_type": "VARCHAR(2)", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "M: 남성, F: 여성"},
            {"physical_name": "image_url", "business_name": "이미지URL", "data_type": "VARCHAR(500)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "이미지 API 경로 (/api/v1/imaging/images/파일명.png)"},
        ],
    },
]

# FK 관계 정의
TABLE_RELATIONSHIPS = [
    {"from_table": "OPD_RCPT", "from_column": "PT_NO", "to_table": "PT_BSNF", "to_column": "PT_NO", "relationship": "many-to-one"},
    {"from_table": "IPD_ADM", "from_column": "PT_NO", "to_table": "PT_BSNF", "to_column": "PT_NO", "relationship": "many-to-one"},
    {"from_table": "LAB_RSLT", "from_column": "PT_NO", "to_table": "PT_BSNF", "to_column": "PT_NO", "relationship": "many-to-one"},
    {"from_table": "DIAG_INFO", "from_column": "PT_NO", "to_table": "PT_BSNF", "to_column": "PT_NO", "relationship": "many-to-one"},
    {"from_table": "imaging_study", "from_column": "person_id", "to_table": "person", "to_column": "person_id", "relationship": "many-to-one"},
]

# 키워드 → 테이블 매핑
KEYWORD_TABLE_MAP = {
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

    # 영상 관련
    "영상": ["imaging_study"],
    "이미지": ["imaging_study"],
    "X-ray": ["imaging_study"],
    "xray": ["imaging_study"],
    "흉부": ["imaging_study"],
    "chest": ["imaging_study"],
    "촬영": ["imaging_study"],
    "방사선": ["imaging_study"],
    "폐렴": ["imaging_study", "DIAG_INFO"],
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
    """스키마 조회 서비스"""

    def __init__(self):
        self.tables = SAMPLE_TABLES
        self.relationships = TABLE_RELATIONSHIPS
        self.keyword_map = KEYWORD_TABLE_MAP

    def get_all_tables(self) -> List[Dict]:
        """모든 테이블 조회"""
        return self.tables

    def get_table(self, table_name: str) -> Optional[Dict]:
        """테이블명으로 조회"""
        table_upper = table_name.upper()
        for table in self.tables:
            if table["physical_name"].upper() == table_upper:
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

        # 기본 테이블이 없으면 환자 테이블 추가
        if not relevant_tables:
            relevant_tables.add("PT_BSNF")

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
        table1_upper = table1.upper()
        table2_upper = table2.upper()

        for rel in self.relationships:
            if ((rel["from_table"].upper() == table1_upper and rel["to_table"].upper() == table2_upper) or
                (rel["from_table"].upper() == table2_upper and rel["to_table"].upper() == table1_upper)):
                return rel

        return None


# Singleton instance
it_meta_service = ITMetaService()
