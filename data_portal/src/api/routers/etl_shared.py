"""
ETL Shared Helpers — data paths, load/save functions, registries, schemas, and utilities
shared across etl_airflow, etl_sources, etl_parallel, etl_mapping, etl_schema, etl_templates.
"""
import json
import re
import uuid
from typing import Optional, List, Dict, Any
from datetime import datetime
from pathlib import Path

from fastapi import HTTPException

# =====================================================================
#  Data file paths
# =====================================================================
_ETL_DATA_DIR = Path(__file__).parent.parent / "etl_data"
_ETL_DATA_DIR.mkdir(parents=True, exist_ok=True)

SOURCES_FILE = _ETL_DATA_DIR / "source_connectors.json"
PARALLEL_CONFIG_FILE = _ETL_DATA_DIR / "parallel_config.json"
SCHEMA_SNAPSHOTS_FILE = _ETL_DATA_DIR / "schema_snapshots.json"
SCHEMA_DIFF_HISTORY_FILE = _ETL_DATA_DIR / "schema_diff_history.json"
TEMPLATES_FILE = _ETL_DATA_DIR / "ingestion_templates.json"

# =====================================================================
#  Connector type registry (DIR-002)
# =====================================================================
CONNECTOR_TYPE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "rdbms": {
        "label": "RDBMS",
        "icon": "DatabaseOutlined",
        "description": "관계형 데이터베이스",
        "subtypes": {
            "postgresql": {
                "label": "PostgreSQL",
                "required_fields": ["host", "port", "database", "username", "password"],
                "optional_fields": ["schema", "ssl_mode"],
                "default_port": 5432,
            },
            "oracle": {
                "label": "Oracle",
                "required_fields": ["host", "port", "database", "username", "password"],
                "optional_fields": ["service_name", "sid"],
                "default_port": 1521,
            },
            "mysql": {
                "label": "MySQL",
                "required_fields": ["host", "port", "database", "username", "password"],
                "optional_fields": ["charset"],
                "default_port": 3306,
            },
            "mssql": {
                "label": "MS SQL Server",
                "required_fields": ["host", "port", "database", "username", "password"],
                "optional_fields": ["driver", "trusted_connection"],
                "default_port": 1433,
            },
        },
    },
    "nosql": {
        "label": "NoSQL",
        "icon": "ClusterOutlined",
        "description": "NoSQL 데이터베이스",
        "subtypes": {
            "mongodb": {
                "label": "MongoDB",
                "required_fields": ["host", "port", "database"],
                "optional_fields": ["username", "password", "auth_source", "replica_set"],
                "default_port": 27017,
            },
            "elasticsearch": {
                "label": "Elasticsearch",
                "required_fields": ["host", "port"],
                "optional_fields": ["username", "password", "index_pattern", "use_ssl"],
                "default_port": 9200,
            },
        },
    },
    "file": {
        "label": "File",
        "icon": "FileOutlined",
        "description": "파일 기반 데이터 소스",
        "subtypes": {
            "csv": {
                "label": "CSV",
                "required_fields": ["path"],
                "optional_fields": ["delimiter", "encoding", "header_row"],
            },
            "json": {
                "label": "JSON",
                "required_fields": ["path"],
                "optional_fields": ["json_path", "encoding"],
            },
            "parquet": {
                "label": "Parquet",
                "required_fields": ["path"],
                "optional_fields": ["compression"],
            },
            "dicom": {
                "label": "DICOM",
                "required_fields": ["path"],
                "optional_fields": ["modality_filter", "recursive"],
            },
            "xml": {
                "label": "XML",
                "required_fields": ["path"],
                "optional_fields": ["xpath_root", "encoding"],
            },
        },
    },
    "api": {
        "label": "API",
        "icon": "ApiOutlined",
        "description": "API 기반 데이터 소스",
        "subtypes": {
            "hl7-fhir": {
                "label": "HL7 FHIR R4",
                "required_fields": ["url"],
                "optional_fields": ["auth_type", "api_key", "username", "password", "fhir_version"],
            },
            "rest-api": {
                "label": "REST API",
                "required_fields": ["url"],
                "optional_fields": ["auth_type", "api_key", "username", "password", "headers"],
            },
            "mdx": {
                "label": "MDX (OLAP)",
                "required_fields": ["host", "port", "database"],
                "optional_fields": ["username", "password", "cube_name"],
                "default_port": 80,
            },
        },
    },
    "cloud": {
        "label": "Cloud Storage",
        "icon": "CloudOutlined",
        "description": "클라우드 스토리지",
        "subtypes": {
            "aws-s3": {
                "label": "AWS S3",
                "required_fields": ["path"],
                "optional_fields": ["aws_access_key", "aws_secret_key", "region", "endpoint_url"],
            },
            "azure-blob": {
                "label": "Azure Blob Storage",
                "required_fields": ["path"],
                "optional_fields": ["connection_string", "account_name", "account_key"],
            },
            "gcs": {
                "label": "Google Cloud Storage",
                "required_fields": ["path"],
                "optional_fields": ["project_id", "credentials_json"],
            },
        },
    },
    "log": {
        "label": "Log",
        "icon": "FileTextOutlined",
        "description": "로그 데이터 소스",
        "subtypes": {
            "syslog": {
                "label": "Syslog",
                "required_fields": ["path"],
                "optional_fields": ["format", "facility_filter"],
            },
            "json-log": {
                "label": "JSON Structured Log",
                "required_fields": ["path"],
                "optional_fields": ["timestamp_field", "level_field"],
            },
        },
    },
}

# =====================================================================
#  Default source connectors
# =====================================================================
DEFAULT_SOURCES: List[Dict[str, Any]] = [
    {
        "id": "omop-cdm",
        "name": "OMOP CDM (PostgreSQL)",
        "type": "rdbms",
        "subtype": "postgresql",
        "host": "localhost",
        "port": 5436,
        "database": "omop_cdm",
        "status": "connected",
        "tables": 18,
        "description": "통합 CDW 데이터베이스 (OMOP CDM V6.0)",
        "last_sync": "2026-02-07T02:00:00",
    },
    {
        "id": "synthea-csv",
        "name": "Synthea 합성 데이터 (CSV)",
        "type": "file",
        "subtype": "csv",
        "path": "/data/synthea/output/csv",
        "status": "available",
        "tables": 16,
        "description": "Synthea 생성 합성 임상 데이터 (CSV 파일셋)",
        "last_sync": "2026-02-07T01:30:00",
    },
    {
        "id": "ehr-oracle",
        "name": "EHR 운영 DB (Oracle)",
        "type": "rdbms",
        "subtype": "oracle",
        "host": "ehr-db.amc.seoul.kr",
        "port": 1521,
        "database": "EHRPRD",
        "status": "configured",
        "tables": 245,
        "description": "서울아산병원 전자의무기록 운영 데이터베이스",
        "last_sync": None,
    },
    {
        "id": "pacs-dicom",
        "name": "PACS 영상 데이터 (DICOM)",
        "type": "file",
        "subtype": "dicom",
        "path": "/data/pacs/dicom",
        "status": "configured",
        "tables": 0,
        "description": "의료 영상 저장/전송 시스템 (DICOM 파일)",
        "last_sync": None,
    },
    {
        "id": "lab-hl7",
        "name": "검사 시스템 (HL7/FHIR)",
        "type": "api",
        "subtype": "hl7-fhir",
        "host": "lab-api.amc.seoul.kr",
        "port": 443,
        "status": "configured",
        "tables": 0,
        "description": "임상검사 시스템 HL7 FHIR R4 인터페이스",
        "last_sync": None,
    },
    {
        "id": "emr-log",
        "name": "EMR 감사 로그 (Syslog)",
        "type": "log",
        "subtype": "syslog",
        "path": "/var/log/emr/audit.log",
        "status": "configured",
        "tables": 0,
        "description": "EMR 시스템 감사 로그 (JSON structured log)",
        "last_sync": None,
    },
]

# =====================================================================
#  OMOP CDM target schema (key tables)
# =====================================================================
OMOP_TARGET_SCHEMA: Dict[str, List[Dict[str, str]]] = {
    "person": [
        {"name": "person_id", "type": "BIGINT", "pk": True},
        {"name": "gender_concept_id", "type": "BIGINT"},
        {"name": "year_of_birth", "type": "INT"},
        {"name": "month_of_birth", "type": "INT"},
        {"name": "day_of_birth", "type": "INT"},
        {"name": "race_concept_id", "type": "BIGINT"},
        {"name": "ethnicity_concept_id", "type": "BIGINT"},
        {"name": "person_source_value", "type": "VARCHAR(50)"},
        {"name": "gender_source_value", "type": "VARCHAR(50)"},
    ],
    "visit_occurrence": [
        {"name": "visit_occurrence_id", "type": "BIGINT", "pk": True},
        {"name": "person_id", "type": "BIGINT"},
        {"name": "visit_concept_id", "type": "BIGINT"},
        {"name": "visit_start_date", "type": "DATE"},
        {"name": "visit_end_date", "type": "DATE"},
        {"name": "visit_type_concept_id", "type": "BIGINT"},
        {"name": "visit_source_value", "type": "VARCHAR(50)"},
    ],
    "condition_occurrence": [
        {"name": "condition_occurrence_id", "type": "BIGINT", "pk": True},
        {"name": "person_id", "type": "BIGINT"},
        {"name": "condition_concept_id", "type": "BIGINT"},
        {"name": "condition_start_date", "type": "DATE"},
        {"name": "condition_end_date", "type": "DATE"},
        {"name": "condition_type_concept_id", "type": "BIGINT"},
        {"name": "condition_source_value", "type": "VARCHAR(50)"},
    ],
    "drug_exposure": [
        {"name": "drug_exposure_id", "type": "BIGINT", "pk": True},
        {"name": "person_id", "type": "BIGINT"},
        {"name": "drug_concept_id", "type": "BIGINT"},
        {"name": "drug_exposure_start_date", "type": "DATE"},
        {"name": "drug_exposure_end_date", "type": "DATE"},
        {"name": "drug_type_concept_id", "type": "BIGINT"},
        {"name": "drug_source_value", "type": "VARCHAR(100)"},
        {"name": "quantity", "type": "NUMERIC"},
        {"name": "days_supply", "type": "INT"},
    ],
    "measurement": [
        {"name": "measurement_id", "type": "BIGINT", "pk": True},
        {"name": "person_id", "type": "BIGINT"},
        {"name": "measurement_concept_id", "type": "BIGINT"},
        {"name": "measurement_date", "type": "DATE"},
        {"name": "value_as_number", "type": "NUMERIC"},
        {"name": "measurement_source_value", "type": "VARCHAR(100)"},
        {"name": "unit_source_value", "type": "VARCHAR(50)"},
    ],
}

# Source column -> target column synonym dictionary
COLUMN_SYNONYMS: Dict[str, List[str]] = {
    "person_id": ["patient_id", "pat_id", "pt_id", "환자id", "환자번호"],
    "gender_source_value": ["sex", "gender", "성별", "sex_cd"],
    "year_of_birth": ["birth_year", "생년", "출생년도", "birth_yr"],
    "visit_occurrence_id": ["encounter_id", "visit_id", "내원id"],
    "visit_start_date": ["admission_date", "adm_date", "입원일", "접수일", "start_dt"],
    "visit_end_date": ["discharge_date", "disch_date", "퇴원일", "end_dt"],
    "condition_concept_id": ["diagnosis_code", "icd_code", "진단코드", "dx_cd"],
    "condition_start_date": ["diagnosis_date", "dx_date", "진단일"],
    "drug_source_value": ["drug_code", "medication", "약품코드", "drug_cd"],
    "drug_exposure_start_date": ["prescription_date", "rx_date", "처방일"],
    "measurement_date": ["test_date", "lab_date", "검사일"],
    "measurement_source_value": ["test_code", "lab_code", "검사코드"],
    "value_as_number": ["result_value", "value", "결과값", "result_num"],
    "unit_source_value": ["unit", "단위", "result_unit"],
}

# Default parallel configuration
DEFAULT_PARALLEL_CONFIG: Dict[str, Any] = {
    "global": {
        "max_workers": 4,
        "batch_size": 10000,
        "chunk_strategy": "id_range",
        "enabled": True,
    },
    "tables": {
        "measurement": {
            "workers": 8,
            "batch_size": 50000,
            "partition_key": "measurement_id",
            "strategy": "id_range",
            "enabled": True,
            "description": "검사 결과 (36.6M rows) — 고병렬 적재",
        },
        "observation": {
            "workers": 4,
            "batch_size": 30000,
            "partition_key": "observation_id",
            "strategy": "id_range",
            "enabled": True,
            "description": "관찰 기록 (21.3M rows)",
        },
        "procedure_occurrence": {
            "workers": 4,
            "batch_size": 20000,
            "partition_key": "procedure_occurrence_id",
            "strategy": "id_range",
            "enabled": True,
            "description": "시술/수술 기록 (12.4M rows)",
        },
        "visit_occurrence": {
            "workers": 2,
            "batch_size": 20000,
            "partition_key": "visit_occurrence_id",
            "strategy": "id_range",
            "enabled": True,
            "description": "방문 기록 (4.5M rows)",
        },
        "drug_exposure": {
            "workers": 2,
            "batch_size": 20000,
            "partition_key": "drug_exposure_id",
            "strategy": "id_range",
            "enabled": True,
            "description": "약물 처방 (3.9M rows)",
        },
        "condition_occurrence": {
            "workers": 2,
            "batch_size": 20000,
            "partition_key": "condition_occurrence_id",
            "strategy": "id_range",
            "enabled": True,
            "description": "진단 기록 (2.8M rows)",
        },
    },
}

# Standard terminology metadata
STANDARD_TERMINOLOGY: Dict[str, Dict[str, Any]] = {
    "person_id": {"standard": "OMOP", "code": "person_id", "domain": "Person", "description": "환자 고유 식별자"},
    "gender_concept_id": {"standard": "OMOP", "code": "8507/8532", "domain": "Gender", "description": "성별 (M=8507, F=8532)"},
    "gender_source_value": {"standard": "HL7", "code": "AdministrativeSex", "domain": "Gender", "description": "원천 성별 값"},
    "year_of_birth": {"standard": "OMOP", "code": "year_of_birth", "domain": "Person", "description": "출생 연도"},
    "visit_occurrence_id": {"standard": "OMOP", "code": "visit_occurrence_id", "domain": "Visit", "description": "방문 고유 식별자"},
    "visit_concept_id": {"standard": "OMOP", "code": "9201/9202/9203", "domain": "Visit", "description": "방문 유형 (입원/외래/응급)"},
    "visit_start_date": {"standard": "OMOP", "code": "visit_start_date", "domain": "Visit", "description": "방문 시작일"},
    "condition_concept_id": {"standard": "SNOMED CT", "code": "condition_concept_id", "domain": "Condition", "description": "진단 표준코드 (SNOMED CT)"},
    "condition_source_value": {"standard": "ICD-10", "code": "condition_source_value", "domain": "Condition", "description": "원천 진단코드 (ICD-10)"},
    "drug_concept_id": {"standard": "RxNorm", "code": "drug_concept_id", "domain": "Drug", "description": "약물 표준코드 (RxNorm)"},
    "drug_source_value": {"standard": "ATC/KD", "code": "drug_source_value", "domain": "Drug", "description": "원천 약물코드"},
    "measurement_concept_id": {"standard": "LOINC", "code": "measurement_concept_id", "domain": "Measurement", "description": "검사 표준코드 (LOINC)"},
    "measurement_source_value": {"standard": "Local", "code": "measurement_source_value", "domain": "Measurement", "description": "원천 검사코드"},
    "value_as_number": {"standard": "OMOP", "code": "value_as_number", "domain": "Measurement", "description": "검사 결과 수치"},
    "unit_source_value": {"standard": "UCUM", "code": "unit_source_value", "domain": "Measurement", "description": "단위 (UCUM)"},
}


# =====================================================================
#  Shared load/save helpers
# =====================================================================

def load_sources() -> List[Dict]:
    if SOURCES_FILE.exists():
        try:
            with open(SOURCES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return list(DEFAULT_SOURCES)


def save_sources(sources: List[Dict]):
    with open(SOURCES_FILE, "w", encoding="utf-8") as f:
        json.dump(sources, f, ensure_ascii=False, indent=2)


def load_parallel_config() -> Dict:
    if PARALLEL_CONFIG_FILE.exists():
        try:
            with open(PARALLEL_CONFIG_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return dict(DEFAULT_PARALLEL_CONFIG)


def save_parallel_config(config: Dict):
    with open(PARALLEL_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)


def load_schema_snapshots() -> List[Dict]:
    if SCHEMA_SNAPSHOTS_FILE.exists():
        try:
            with open(SCHEMA_SNAPSHOTS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return []


def save_schema_snapshots(snapshots: List[Dict]):
    with open(SCHEMA_SNAPSHOTS_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshots, f, ensure_ascii=False, indent=2)


def load_diff_history() -> List[Dict]:
    if SCHEMA_DIFF_HISTORY_FILE.exists():
        try:
            with open(SCHEMA_DIFF_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return []


def save_diff_history(history: List[Dict]):
    with open(SCHEMA_DIFF_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def load_templates() -> List[Dict]:
    from .etl_templates import DEFAULT_TEMPLATES
    if TEMPLATES_FILE.exists():
        try:
            with open(TEMPLATES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return list(DEFAULT_TEMPLATES)


def save_templates(templates: List[Dict]):
    with open(TEMPLATES_FILE, "w", encoding="utf-8") as f:
        json.dump(templates, f, ensure_ascii=False, indent=2)


# =====================================================================
#  Shared schema discovery utilities
# =====================================================================

async def discover_pg_schema(host: str, port: int, user: str, password: str, database: str) -> List[Dict]:
    """PostgreSQL information_schema에서 테이블/컬럼 정보 조회"""
    import asyncpg
    conn = await asyncpg.connect(host=host, port=port, user=user, password=password, database=database)
    try:
        tables_rows = await conn.fetch("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        result = []
        for tr in tables_rows:
            tname = tr["table_name"]
            cols_rows = await conn.fetch("""
                SELECT column_name, data_type, is_nullable,
                       column_default, ordinal_position
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position
            """, tname)
            pk_rows = await conn.fetch("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.table_schema = 'public' AND tc.table_name = $1
                  AND tc.constraint_type = 'PRIMARY KEY'
            """, tname)
            pk_cols = {r["column_name"] for r in pk_rows}

            row_count = await conn.fetchval(
                f"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = $1", tname
            )
            size_bytes = await conn.fetchval(
                f"SELECT pg_total_relation_size($1::regclass)", tname
            )

            columns = []
            for cr in cols_rows:
                columns.append({
                    "name": cr["column_name"],
                    "type": cr["data_type"],
                    "nullable": cr["is_nullable"] == "YES",
                    "is_pk": cr["column_name"] in pk_cols,
                    "position": cr["ordinal_position"],
                })

            result.append({
                "table_name": tname,
                "column_count": len(columns),
                "columns": columns,
                "row_count": int(row_count or 0),
                "size_bytes": int(size_bytes or 0),
                "size_mb": round((size_bytes or 0) / 1024 / 1024, 2),
            })
        return result
    finally:
        await conn.close()


def simulate_schema(source: Dict) -> List[Dict]:
    """비-PostgreSQL 소스에 대한 시뮬레이션 스키마"""
    sim_tables = {
        "oracle": [
            {"table_name": "PATIENTS", "column_count": 12, "columns": [
                {"name": "PAT_ID", "type": "NUMBER", "nullable": False, "is_pk": True, "position": 1},
                {"name": "PAT_NAME", "type": "VARCHAR2", "nullable": False, "is_pk": False, "position": 2},
                {"name": "BIRTH_DATE", "type": "DATE", "nullable": True, "is_pk": False, "position": 3},
                {"name": "SEX_CD", "type": "CHAR", "nullable": True, "is_pk": False, "position": 4},
            ], "row_count": 150000, "size_bytes": 52428800, "size_mb": 50.0},
            {"table_name": "ENCOUNTERS", "column_count": 8, "columns": [
                {"name": "ENC_ID", "type": "NUMBER", "nullable": False, "is_pk": True, "position": 1},
                {"name": "PAT_ID", "type": "NUMBER", "nullable": False, "is_pk": False, "position": 2},
                {"name": "ADM_DATE", "type": "DATE", "nullable": False, "is_pk": False, "position": 3},
                {"name": "DISCH_DATE", "type": "DATE", "nullable": True, "is_pk": False, "position": 4},
            ], "row_count": 500000, "size_bytes": 157286400, "size_mb": 150.0},
        ],
        "csv": [
            {"table_name": "patients.csv", "column_count": 8, "columns": [
                {"name": "Id", "type": "string", "nullable": False, "is_pk": True, "position": 1},
                {"name": "BIRTHDATE", "type": "date", "nullable": True, "is_pk": False, "position": 2},
                {"name": "GENDER", "type": "string", "nullable": True, "is_pk": False, "position": 3},
            ], "row_count": 76074, "size_bytes": 15728640, "size_mb": 15.0},
        ],
    }
    subtype = source.get("subtype", "")
    return sim_tables.get(subtype, [
        {"table_name": f"{source['name']}_table", "column_count": 5, "columns": [
            {"name": "id", "type": "integer", "nullable": False, "is_pk": True, "position": 1},
            {"name": "data", "type": "text", "nullable": True, "is_pk": False, "position": 2},
            {"name": "created_at", "type": "timestamp", "nullable": True, "is_pk": False, "position": 3},
        ], "row_count": 0, "size_bytes": 0, "size_mb": 0.0}
    ])


def match_column(source_col: str, target_cols: List[Dict[str, str]]) -> Optional[Dict]:
    """소스 컬럼과 가장 유사한 타겟 컬럼 매칭"""
    src = source_col.lower().strip()

    # 1. 정확히 같은 이름
    for tc in target_cols:
        if tc["name"].lower() == src:
            return {"target_column": tc["name"], "confidence": 1.0, "method": "exact_match"}

    # 2. 동의어 사전 매칭
    for target_name, synonyms in COLUMN_SYNONYMS.items():
        if src in [s.lower() for s in synonyms]:
            tc_match = next((tc for tc in target_cols if tc["name"] == target_name), None)
            if tc_match:
                return {"target_column": tc_match["name"], "confidence": 0.9, "method": "synonym"}

    # 3. 부분 문자열 매칭
    for tc in target_cols:
        tn = tc["name"].lower()
        if src in tn or tn in src:
            return {"target_column": tc["name"], "confidence": 0.7, "method": "substring"}

    # 4. 키워드 교집합 매칭
    src_parts = set(re.split(r"[_\-\s]+", src))
    for tc in target_cols:
        tn_parts = set(re.split(r"[_\-\s]+", tc["name"].lower()))
        overlap = src_parts & tn_parts
        if overlap and len(overlap) / max(len(src_parts), len(tn_parts)) >= 0.5:
            return {"target_column": tc["name"], "confidence": 0.5, "method": "keyword_overlap"}

    return None
