"""
Semantic Layer API - 검색 엔드포인트 (통합검색, Faceted, 번역, SQL 컨텍스트 등)
"""
from difflib import SequenceMatcher
from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any

router = APIRouter()

# ══════════════════════════════════════════════════════════════════════════════
# Shared data constants (imported by semantic_metadata.py as well)
# ══════════════════════════════════════════════════════════════════════════════

# OMOP CDM 메타데이터 (실제 PostgreSQL DB 연동)
SAMPLE_TABLES = [
    # ── Clinical Data ──
    {
        "physical_name": "person",
        "business_name": "환자 (OMOP)",
        "description": "OMOP CDM 환자 기본 정보 (1,130명). gender_source_value로 성별(M/F) 조회.",
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
            {"physical_name": "person_source_value", "business_name": "원본환자번호", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "원본 환자번호 (비식별화 UUID)"},
            {"physical_name": "gender_source_value", "business_name": "성별", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "성별 원본값 (M/F)"},
        ],
        "usage_count": 3200,
        "tags": ["OMOP", "CDM", "환자", "마스터"],
    },
    {
        "physical_name": "visit_occurrence",
        "business_name": "방문기록 (OMOP)",
        "description": "OMOP CDM 환자 방문(입원/외래/응급) 기록 (32,153건). visit_concept_id로 방문 유형 구분.",
        "domain": "진료",
        "columns": [
            {"physical_name": "visit_occurrence_id", "business_name": "방문기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "방문 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "visit_concept_id", "business_name": "방문유형ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "9201: 입원, 9202: 외래, 9203: 응급"},
            {"physical_name": "visit_start_date", "business_name": "방문시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "방문 시작일"},
            {"physical_name": "visit_end_date", "business_name": "방문종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "방문 종료일"},
            {"physical_name": "visit_source_value", "business_name": "방문원본유형", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "원본 방문 유형"},
        ],
        "usage_count": 2800,
        "tags": ["OMOP", "CDM", "방문", "입원", "외래"],
    },
    {
        "physical_name": "condition_occurrence",
        "business_name": "진단기록 (OMOP)",
        "description": "OMOP CDM 환자 진단/질환 기록 (7,900건). condition_source_value에 SNOMED CT 코드 저장.",
        "domain": "진료",
        "columns": [
            {"physical_name": "condition_occurrence_id", "business_name": "진단기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "진단 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "condition_concept_id", "business_name": "진단개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "진단 SNOMED 개념 ID"},
            {"physical_name": "condition_start_date", "business_name": "진단시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "진단 시작일"},
            {"physical_name": "condition_end_date", "business_name": "진단종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "진단 종료일"},
            {"physical_name": "condition_source_value", "business_name": "진단원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "SNOMED CT 코드 (44054006=당뇨, 38341003=고혈압)"},
            {"physical_name": "visit_occurrence_id", "business_name": "방문ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "방문 ID (FK → visit_occurrence)"},
        ],
        "usage_count": 2500,
        "tags": ["OMOP", "CDM", "진단", "SNOMED"],
    },
    {
        "physical_name": "drug_exposure",
        "business_name": "약물처방 (OMOP)",
        "description": "OMOP CDM 약물 처방 기록 (13,799건). drug_source_value에 원본 약물 코드 저장.",
        "domain": "처방",
        "columns": [
            {"physical_name": "drug_exposure_id", "business_name": "약물처방ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "약물 처방 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "drug_concept_id", "business_name": "약물개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "약물 개념 ID"},
            {"physical_name": "drug_exposure_start_date", "business_name": "처방시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "처방 시작일"},
            {"physical_name": "drug_exposure_end_date", "business_name": "처방종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "처방 종료일"},
            {"physical_name": "drug_source_value", "business_name": "약물원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "원본 약물 코드"},
        ],
        "usage_count": 1900,
        "tags": ["OMOP", "CDM", "약물", "처방"],
    },
    {
        "physical_name": "measurement",
        "business_name": "검사결과 (OMOP)",
        "description": "OMOP CDM 임상 검사 결과 (170,043건). value_as_number에 수치 결과, unit_source_value에 단위.",
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
        "usage_count": 3500,
        "tags": ["OMOP", "CDM", "검사", "임상", "결과"],
    },
    {
        "physical_name": "observation",
        "business_name": "관찰기록 (OMOP)",
        "description": "OMOP CDM 환자 관찰/관측 기록 (7,899건). 사회력, 알레르기, 기타 임상 관찰.",
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
        "usage_count": 980,
        "tags": ["OMOP", "CDM", "관찰", "사회력"],
    },
    {
        "physical_name": "procedure_occurrence",
        "business_name": "시술기록 (OMOP)",
        "description": "OMOP CDM 시술/수술 기록 (17,333건). procedure_source_value에 원본 시술 코드.",
        "domain": "시술",
        "columns": [
            {"physical_name": "procedure_occurrence_id", "business_name": "시술기록ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "시술 기록 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "procedure_concept_id", "business_name": "시술개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "시술 개념 ID"},
            {"physical_name": "procedure_date", "business_name": "시술일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "시술 일자"},
            {"physical_name": "procedure_source_value", "business_name": "시술원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "원본 시술 코드"},
            {"physical_name": "visit_occurrence_id", "business_name": "방문ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "방문 ID (FK → visit_occurrence)"},
        ],
        "usage_count": 1500,
        "tags": ["OMOP", "CDM", "시술", "수술"],
    },
    {
        "physical_name": "imaging_study",
        "business_name": "흉부X-ray영상 (OMOP)",
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
            {"physical_name": "image_url", "business_name": "이미지URL", "data_type": "VARCHAR(500)", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "이미지 API 경로"},
        ],
        "usage_count": 2200,
        "tags": ["OMOP", "CDM", "영상", "X-ray", "흉부"],
    },
    # ── Health System ──
    {
        "physical_name": "provider",
        "business_name": "의료진",
        "description": "OMOP CDM 의료 제공자 정보 (905,492건). 의사, 간호사 등 의료진 마스터.",
        "domain": "의료진",
        "columns": [
            {"physical_name": "provider_id", "business_name": "의료진ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "의료진 고유 ID"},
            {"physical_name": "provider_name", "business_name": "의료진명", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "의료진 이름"},
            {"physical_name": "npi", "business_name": "NPI", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "National Provider Identifier"},
            {"physical_name": "dea", "business_name": "DEA", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "Drug Enforcement Admin 번호"},
            {"physical_name": "specialty_concept_id", "business_name": "전문과목ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "전문 진료과 개념 ID"},
            {"physical_name": "specialty_source_value", "business_name": "전문과목", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "전문과목 원본값"},
            {"physical_name": "gender_source_value", "business_name": "성별", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "의료진 성별"},
        ],
        "usage_count": 1100,
        "tags": ["OMOP", "CDM", "의료진", "마스터"],
    },
    {
        "physical_name": "care_site",
        "business_name": "진료기관",
        "description": "OMOP CDM 진료 기관/부서 정보 (7,100건). 병원, 클리닉, 부서 등.",
        "domain": "의료진",
        "columns": [
            {"physical_name": "care_site_id", "business_name": "진료기관ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "진료기관 고유 ID"},
            {"physical_name": "care_site_name", "business_name": "기관명", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "진료기관 이름"},
            {"physical_name": "place_of_service_concept_id", "business_name": "서비스장소ID", "data_type": "INTEGER", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "진료 장소 유형 개념 ID"},
            {"physical_name": "care_site_source_value", "business_name": "기관원본코드", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "원본 기관 코드"},
        ],
        "usage_count": 450,
        "tags": ["OMOP", "CDM", "기관", "마스터"],
    },
    {
        "physical_name": "location",
        "business_name": "위치정보",
        "description": "OMOP CDM 지리적 위치 정보 (3,787건). 주소, 좌표 등.",
        "domain": "기관",
        "columns": [
            {"physical_name": "location_id", "business_name": "위치ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "위치 고유 ID"},
            {"physical_name": "city", "business_name": "도시", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "도시명"},
            {"physical_name": "state", "business_name": "주/도", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "주/도"},
            {"physical_name": "zip", "business_name": "우편번호", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "우편번호"},
            {"physical_name": "county", "business_name": "군/구", "data_type": "VARCHAR", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "군/구"},
            {"physical_name": "latitude", "business_name": "위도", "data_type": "REAL", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "위도"},
            {"physical_name": "longitude", "business_name": "경도", "data_type": "REAL", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "경도"},
        ],
        "usage_count": 300,
        "tags": ["OMOP", "CDM", "위치", "주소"],
    },
    # ── Observation / Period ──
    {
        "physical_name": "observation_period",
        "business_name": "관찰기간",
        "description": "OMOP CDM 환자 관찰 기간 (76,070건). 환자별 데이터 수집 시작~종료 기간.",
        "domain": "환자",
        "columns": [
            {"physical_name": "observation_period_id", "business_name": "관찰기간ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "관찰 기간 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "observation_period_start_date", "business_name": "관찰시작일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "관찰 기간 시작일"},
            {"physical_name": "observation_period_end_date", "business_name": "관찰종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "관찰 기간 종료일"},
            {"physical_name": "period_type_concept_id", "business_name": "기간유형ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "기간 유형 개념 ID"},
        ],
        "usage_count": 800,
        "tags": ["OMOP", "CDM", "관찰기간", "환자"],
    },
    {
        "physical_name": "visit_detail",
        "business_name": "방문상세",
        "description": "OMOP CDM 방문 상세 기록 (51,027건). 입원 중 이동, 전과 등 세부 방문 정보.",
        "domain": "진료",
        "columns": [
            {"physical_name": "visit_detail_id", "business_name": "방문상세ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "방문 상세 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "visit_detail_concept_id", "business_name": "방문상세유형ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "방문 상세 유형 개념 ID"},
            {"physical_name": "visit_detail_start_date", "business_name": "시작일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "방문 상세 시작일"},
            {"physical_name": "visit_detail_end_date", "business_name": "종료일", "data_type": "DATE", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "방문 상세 종료일"},
            {"physical_name": "visit_occurrence_id", "business_name": "방문ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": False, "sensitivity": "Normal", "description": "방문 ID (FK → visit_occurrence)"},
        ],
        "usage_count": 600,
        "tags": ["OMOP", "CDM", "방문", "상세"],
    },
    # ── Derived (파생 테이블) ──
    {
        "physical_name": "condition_era",
        "business_name": "진단기간",
        "description": "OMOP CDM 진단 에라 (7,072건). 동일 진단의 연속 기간을 집계한 파생 테이블.",
        "domain": "진료",
        "columns": [
            {"physical_name": "condition_era_id", "business_name": "진단기간ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "진단 에라 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "condition_concept_id", "business_name": "진단개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "진단 개념 ID"},
            {"physical_name": "condition_era_start_date", "business_name": "에라시작일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "진단 에라 시작일"},
            {"physical_name": "condition_era_end_date", "business_name": "에라종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "진단 에라 종료일"},
            {"physical_name": "condition_occurrence_count", "business_name": "발생횟수", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "에라 내 진단 발생 횟수"},
        ],
        "usage_count": 500,
        "tags": ["OMOP", "CDM", "진단", "에라", "파생"],
    },
    {
        "physical_name": "drug_era",
        "business_name": "약물기간",
        "description": "OMOP CDM 약물 에라 (5,855건). 동일 약물의 연속 복용 기간을 집계한 파생 테이블.",
        "domain": "처방",
        "columns": [
            {"physical_name": "drug_era_id", "business_name": "약물기간ID", "data_type": "BIGINT", "is_pk": True, "is_nullable": False, "sensitivity": "Normal", "description": "약물 에라 고유 ID"},
            {"physical_name": "person_id", "business_name": "환자ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "PHI", "description": "환자 ID (FK → person)"},
            {"physical_name": "drug_concept_id", "business_name": "약물개념ID", "data_type": "BIGINT", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "약물 개념 ID"},
            {"physical_name": "drug_era_start_date", "business_name": "에라시작일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "약물 에라 시작일"},
            {"physical_name": "drug_era_end_date", "business_name": "에라종료일", "data_type": "DATE", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "약물 에라 종료일"},
            {"physical_name": "drug_exposure_count", "business_name": "처방횟수", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "에라 내 처방 횟수"},
            {"physical_name": "gap_days", "business_name": "갭일수", "data_type": "INTEGER", "is_pk": False, "is_nullable": True, "sensitivity": "Normal", "description": "처방 간 공백 일수"},
        ],
        "usage_count": 420,
        "tags": ["OMOP", "CDM", "약물", "에라", "파생"],
    },
]

DOMAINS = ["환자", "진료", "검사", "처방", "시술", "관찰", "영상", "의료진", "기관"]
TAGS = ["OMOP", "CDM", "마스터", "환자", "방문", "입원", "외래", "진단", "SNOMED", "약물", "처방", "검사", "임상", "결과", "관찰", "사회력", "시술", "수술", "영상", "X-ray", "흉부", "의료진", "기관", "위치", "주소", "관찰기간", "상세", "에라", "파생"]

# ── 의미 확장 검색용 의학 용어 계층 (AAR-001: Semantic Search) ──
MEDICAL_TERM_HIERARCHY: Dict[str, Dict[str, Any]] = {
    "심장 질환": {
        "synonyms": ["심장병", "심질환", "심혈관", "심장질환", "heart disease", "cardiac"],
        "related_terms": ["심근경색", "심방세동", "관상동맥질환", "심부전", "협심증", "부정맥", "심비대", "Cardiomegaly"],
        "snomed_codes": ["22298006", "49436004", "53741008"],
        "related_tables": ["condition_occurrence", "condition_era", "measurement"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
    "당뇨": {
        "synonyms": ["당뇨병", "DM", "diabetes", "제2형 당뇨병"],
        "related_terms": ["혈당", "인슐린", "HbA1c", "당화혈색소", "고혈당"],
        "snomed_codes": ["44054006"],
        "related_tables": ["condition_occurrence", "measurement", "drug_exposure"],
        "related_columns": ["condition_source_value", "value_as_number"],
    },
    "고혈압": {
        "synonyms": ["HTN", "혈압", "hypertension", "고혈압성"],
        "related_terms": ["수축기혈압", "이완기혈압", "혈압강하제", "항고혈압제"],
        "snomed_codes": ["38341003"],
        "related_tables": ["condition_occurrence", "measurement", "drug_exposure"],
        "related_columns": ["condition_source_value", "value_as_number"],
    },
    "호흡기": {
        "synonyms": ["호흡기 질환", "폐", "기관지", "respiratory", "pulmonary"],
        "related_terms": ["폐렴", "기관지염", "인두염", "COPD", "천식", "Pneumonia", "Pneumothorax"],
        "snomed_codes": ["10509002", "195662009"],
        "related_tables": ["condition_occurrence", "imaging_study"],
        "related_columns": ["condition_source_value", "finding_labels"],
    },
    "영상": {
        "synonyms": ["이미지", "방사선", "X-ray", "imaging", "PACS", "흉부"],
        "related_terms": ["흉부X선", "CT", "MRI", "소견", "Pneumonia", "Cardiomegaly", "Effusion"],
        "snomed_codes": [],
        "related_tables": ["imaging_study"],
        "related_columns": ["finding_labels", "view_position", "image_filename"],
    },
    "약물": {
        "synonyms": ["약", "처방", "투약", "drug", "medication", "처방전"],
        "related_terms": ["항생제", "진통제", "인슐린", "항고혈압제", "스타틴"],
        "snomed_codes": [],
        "related_tables": ["drug_exposure", "drug_era"],
        "related_columns": ["drug_concept_id", "drug_source_value"],
    },
    "검사": {
        "synonyms": ["임상검사", "Lab", "혈액검사", "피검사", "laboratory"],
        "related_terms": ["혈당", "HbA1c", "콜레스테롤", "크레아티닌", "백혈구", "헤모글로빈"],
        "snomed_codes": [],
        "related_tables": ["measurement", "observation"],
        "related_columns": ["measurement_concept_id", "value_as_number", "unit_source_value"],
    },
    "입원": {
        "synonyms": ["입원환자", "입퇴원", "재원", "inpatient", "admission"],
        "related_terms": ["외래", "응급", "전과", "퇴원", "재원기간"],
        "snomed_codes": [],
        "related_tables": ["visit_occurrence", "visit_detail"],
        "related_columns": ["visit_concept_id", "visit_start_date", "visit_end_date"],
    },
    "피부과": {
        "synonyms": ["피부", "피부질환", "dermatology", "skin", "피부병"],
        "related_terms": ["무좀", "습진", "건선", "아토피", "두드러기", "대상포진", "백선", "피부염", "여드름", "사마귀", "tinea", "eczema", "psoriasis", "dermatitis"],
        "snomed_codes": ["6020002", "40275004"],
        "related_tables": ["condition_occurrence", "condition_era", "drug_exposure", "observation"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
    "감염": {
        "synonyms": ["감염질환", "감염증", "infection", "infectious", "전염병"],
        "related_terms": ["패혈증", "요로감염", "폐렴", "봉와직염", "결핵", "코로나", "COVID", "세균감염", "바이러스", "항생제"],
        "snomed_codes": ["40733004"],
        "related_tables": ["condition_occurrence", "condition_era", "drug_exposure", "measurement"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
    "소화기": {
        "synonyms": ["위장", "소화기 질환", "GI", "gastro", "gastrointestinal", "위장관"],
        "related_terms": ["위염", "역류성식도염", "궤양", "대장염", "간염", "췌장염", "담석", "장폐색", "크론병"],
        "snomed_codes": [],
        "related_tables": ["condition_occurrence", "condition_era", "procedure_occurrence", "drug_exposure"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
    "신경": {
        "synonyms": ["신경과", "뇌", "신경계", "neurology", "neurological"],
        "related_terms": ["뇌졸중", "간질", "편두통", "치매", "파킨슨", "알츠하이머", "뇌경색", "뇌출혈", "두통"],
        "snomed_codes": [],
        "related_tables": ["condition_occurrence", "condition_era", "drug_exposure", "measurement"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
    "정신": {
        "synonyms": ["정신과", "정신건강", "psychiatry", "mental", "정신질환"],
        "related_terms": ["우울증", "불안장애", "조현병", "PTSD", "불면증", "ADHD", "양극성장애", "공황장애"],
        "snomed_codes": [],
        "related_tables": ["condition_occurrence", "condition_era", "drug_exposure", "observation"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
    "암": {
        "synonyms": ["종양", "cancer", "oncology", "tumor", "악성종양", "신생물"],
        "related_terms": ["폐암", "위암", "대장암", "유방암", "간암", "전립선암", "갑상선암", "백혈병", "림프종", "항암", "화학요법"],
        "snomed_codes": [],
        "related_tables": ["condition_occurrence", "condition_era", "drug_exposure", "procedure_occurrence", "measurement"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
    "근골격": {
        "synonyms": ["근골격계", "정형외과", "관절", "뼈", "orthopedic", "musculoskeletal"],
        "related_terms": ["골절", "관절염", "디스크", "요통", "퇴행성관절", "류마티스", "골다공증", "인대손상", "오십견"],
        "snomed_codes": [],
        "related_tables": ["condition_occurrence", "condition_era", "procedure_occurrence", "drug_exposure"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
    "비뇨기": {
        "synonyms": ["비뇨기과", "신장", "kidney", "urological", "renal"],
        "related_terms": ["요로감염", "신부전", "투석", "신장결석", "방광염", "전립선비대", "혈뇨"],
        "snomed_codes": [],
        "related_tables": ["condition_occurrence", "measurement", "procedure_occurrence", "drug_exposure"],
        "related_columns": ["condition_concept_id", "condition_source_value", "value_as_number"],
    },
    "안과": {
        "synonyms": ["눈", "안과질환", "ophthalmology", "eye"],
        "related_terms": ["백내장", "녹내장", "황반변성", "시력저하", "망막", "안압"],
        "snomed_codes": [],
        "related_tables": ["condition_occurrence", "procedure_occurrence", "measurement"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
    "이비인후과": {
        "synonyms": ["귀", "코", "목", "ENT", "otolaryngology"],
        "related_terms": ["중이염", "비염", "부비동염", "편도염", "난청", "어지럼증", "이명"],
        "snomed_codes": [],
        "related_tables": ["condition_occurrence", "procedure_occurrence", "drug_exposure"],
        "related_columns": ["condition_concept_id", "condition_source_value"],
    },
}


def _expand_search_terms(query: str) -> Dict[str, Any]:
    """검색어 의미 확장 - MEDICAL_TERM_HIERARCHY + BizMeta 동의어 + ICD 코드 매핑"""
    if not query:
        return {"original": query, "expanded_terms": [], "related_tables": [], "expansion_source": []}

    q_lower = query.lower().strip()
    expanded_terms: List[str] = []
    related_tables: List[str] = []
    expansion_source: List[str] = []

    # 1) MEDICAL_TERM_HIERARCHY 매칭 (카테고리명 + synonyms + related_terms)
    q_nospace = q_lower.replace(" ", "")
    for category, info in MEDICAL_TERM_HIERARCHY.items():
        cat_lower = category.lower()
        cat_nospace = cat_lower.replace(" ", "")
        synonyms_lower = [s.lower() for s in info["synonyms"]]
        synonyms_nospace = [s.replace(" ", "") for s in synonyms_lower]
        related_lower = [s.lower() for s in info["related_terms"]]
        related_nospace = [s.replace(" ", "") for s in related_lower]

        # 카테고리명 또는 동의어 매칭
        cat_match = (q_lower == cat_lower or q_nospace == cat_nospace
                or q_lower in synonyms_lower or q_nospace in synonyms_nospace
                or cat_lower in q_lower or cat_nospace in q_nospace
                or q_lower in cat_lower or q_nospace in cat_nospace
                or any(s in q_lower for s in synonyms_lower)
                or any(s in q_nospace for s in synonyms_nospace)
                or any(q_lower in s for s in synonyms_lower)
                or any(q_nospace in s for s in synonyms_nospace))

        # related_terms 매칭 (구체적 질환명 검색 지원: 무좀, 골절, 우울증 등)
        related_match = (q_lower in related_lower or q_nospace in related_nospace
                or any(s in q_lower for s in related_lower if len(s) >= 2)
                or any(q_lower in s for s in related_lower if len(q_lower) >= 2))

        if cat_match or related_match:
            expanded_terms.extend(info["related_terms"])
            expanded_terms.extend(info["synonyms"])
            related_tables.extend(info["related_tables"])
            match_type = "카테고리" if cat_match else "질환명"
            expansion_source.append(f"의학 용어 계층({match_type}): {category}")
            break  # 1 category match is enough

    # 2) BizMeta SYNONYM_MAP 매칭
    try:
        from services.biz_meta import SYNONYM_MAP
        for key, val in SYNONYM_MAP.items():
            if q_lower == key.lower() or q_lower in key.lower():
                expanded_terms.append(val)
                expansion_source.append(f"동의어: {key} → {val}")
    except ImportError:
        pass

    # 3) BizMeta ICD_CODE_MAP 매칭
    try:
        from services.biz_meta import ICD_CODE_MAP
        for key, (code, desc) in ICD_CODE_MAP.items():
            if q_lower == key.lower() or q_lower in key.lower() or key.lower() in q_lower:
                expanded_terms.append(desc)
                expanded_terms.append(code)
                expansion_source.append(f"ICD 코드: {key} → {code} ({desc})")
    except ImportError:
        pass

    # 중복 제거
    expanded_terms = list(dict.fromkeys(expanded_terms))
    related_tables = list(dict.fromkeys(related_tables))

    return {
        "original": query,
        "expanded_terms": expanded_terms,
        "related_tables": related_tables,
        "expansion_source": expansion_source,
    }


def _fuzzy_similarity(query: str, text: str) -> float:
    """토큰 기반 퍼지 유사도 계산 (0.0~1.0). difflib SequenceMatcher 사용."""
    if not query or not text:
        return 0.0
    q = query.lower().strip()
    t = text.lower().strip()
    if q in t:
        return 1.0
    # 전체 문자열 유사도
    ratio = SequenceMatcher(None, q, t).ratio()
    # 토큰별 최대 유사도 (짧은 쿼리가 긴 텍스트의 일부와 매칭될 수 있도록)
    tokens = t.split()
    if tokens:
        token_max = max(SequenceMatcher(None, q, tok).ratio() for tok in tokens)
        ratio = max(ratio, token_max)
    return ratio


def _compute_relevance_score(
    query_lower: str,
    expanded_lower: List[str],
    related_tables: List[str],
    searchable: str,
    physical_name: str,
) -> float:
    """테이블/컬럼에 대한 관련도 점수 계산 (0.0~1.0)."""
    score = 0.0
    # 1) Exact substring match → 1.0
    if query_lower in searchable:
        score = max(score, 1.0)
    # 2) Expanded term match → 0.8
    if any(term in searchable for term in expanded_lower):
        score = max(score, 0.8)
    # 3) Related table match → 0.7
    if physical_name in related_tables:
        score = max(score, 0.7)
    # 4) Fuzzy similarity → 0.0~0.6 (scaled)
    fuzzy = _fuzzy_similarity(query_lower, searchable) * 0.6
    score = max(score, fuzzy)
    return score


# ══════════════════════════════════════════════════════════════════════════════
# Pydantic models
# ══════════════════════════════════════════════════════════════════════════════

class SearchResult(BaseModel):
    success: bool
    data: Dict[str, Any]


# ══════════════════════════════════════════════════════════════════════════════
# Search endpoints
# ══════════════════════════════════════════════════════════════════════════════

@router.get("/search", response_model=SearchResult)
async def search(
    q: str = Query(..., description="검색어"),
    domain: Optional[str] = Query(None, description="도메인 필터"),
    limit: int = Query(20, description="최대 결과 수"),
    min_score: float = Query(0.25, description="최소 유사도 점수 (0.0~1.0)"),
):
    """통합 검색 (의미 확장 + 벡터 유사도 포함)

    AAR-001: Semantic Search — 키워드 매칭 + 의미 확장 + 퍼지 유사도 스코어링.
    결과는 relevance_score 기준 내림차순 정렬됩니다.
    """
    q_lower = q.lower()
    scored_tables: List[tuple] = []  # (score, table)
    scored_columns: List[tuple] = []  # (score, col_dict)

    # 의미 확장
    expansion = _expand_search_terms(q)
    expanded_lower = [t.lower() for t in expansion.get("expanded_terms", [])]
    related_tables = expansion.get("related_tables", [])

    for table in SAMPLE_TABLES:
        # 도메인 필터
        if domain and table["domain"] != domain:
            continue

        searchable = (
            table["physical_name"] + " " +
            table["business_name"] + " " +
            table["description"] + " " +
            " ".join(table.get("tags", []))
        ).lower()

        # 테이블 유사도 점수 계산
        table_score = _compute_relevance_score(
            q_lower, expanded_lower, related_tables, searchable, table["physical_name"]
        )
        if table_score >= min_score:
            scored_tables.append((table_score, table))

        # 컬럼 매칭 (유사도 포함)
        for col in table["columns"]:
            col_searchable = (
                col["physical_name"] + " " +
                col["business_name"] + " " +
                col.get("description", "")
            ).lower()
            col_score = _compute_relevance_score(
                q_lower, expanded_lower, [], col_searchable, ""
            )
            if col_score >= min_score:
                scored_columns.append((col_score, {
                    **col,
                    "table_physical_name": table["physical_name"],
                    "table_business_name": table["business_name"],
                    "relevance_score": round(col_score, 3),
                }))

    # 점수 내림차순 정렬
    scored_tables.sort(key=lambda x: x[0], reverse=True)
    scored_columns.sort(key=lambda x: x[0], reverse=True)

    result_tables = []
    for score, table in scored_tables[:limit]:
        result_tables.append({**table, "relevance_score": round(score, 3)})

    result_columns = [col for _, col in scored_columns[:limit]]

    return SearchResult(
        success=True,
        data={
            "tables": result_tables,
            "columns": result_columns,
            "total": len(result_tables) + len(result_columns),
            "expansion": expansion,
            "search_method": "semantic_fuzzy",
        }
    )


@router.get("/faceted-search")
async def faceted_search(
    q: Optional[str] = None,
    domains: Optional[str] = None,
    tags: Optional[str] = None,
    sensitivity: Optional[str] = None,
    limit: int = 50,
    offset: int = 0
):
    """Faceted Search (의미 확장 지원)"""
    domain_list = domains.split(",") if domains else []
    tag_list = tags.split(",") if tags else []
    sensitivity_list = sensitivity.split(",") if sensitivity else []

    # 의미 확장 (AAR-001)
    expansion = _expand_search_terms(q) if q else None

    results = []
    for table in SAMPLE_TABLES:
        # 도메인 필터
        if domain_list and table["domain"] not in domain_list:
            continue

        # 태그 필터
        if tag_list and not any(t in table["tags"] for t in tag_list):
            continue

        # 민감도 필터
        if sensitivity_list:
            has_sensitivity = any(
                col["sensitivity"] in sensitivity_list
                for col in table["columns"]
            )
            if not has_sensitivity:
                continue

        # 검색어 필터 (불용어 제거 후 AND 매칭 + 의미 확장)
        if q:
            _stop = {"테이블", "데이터", "정보", "목록", "조회", "보여줘", "알려줘", "찾아줘", "뭐야", "table", "data", "list", "show"}
            tokens = [t for t in q.lower().split() if t not in _stop]
            if not tokens:
                pass  # 불용어만 입력한 경우 필터 건너뜀 (전체 반환)
            else:
                searchable = (table["physical_name"] + " " + table["business_name"] + " " + table["description"] + " " + " ".join(table.get("tags", []))).lower()
                col_text = " ".join(c["physical_name"] + " " + c["business_name"] for c in table["columns"]).lower()
                searchable += " " + col_text

                # 원본 매칭
                original_match = all(token in searchable for token in tokens)

                # 확장 용어 매칭
                expanded_match = False
                if expansion and expansion["expanded_terms"]:
                    for term in expansion["expanded_terms"]:
                        if term.lower() in searchable:
                            expanded_match = True
                            break

                # related_tables 매칭
                related_match = False
                if expansion and expansion["related_tables"]:
                    if table["physical_name"] in expansion["related_tables"]:
                        related_match = True

                if not (original_match or expanded_match or related_match):
                    continue

        results.append(table)

    total = len(results)
    resp: Dict[str, Any] = {
        "success": True,
        "data": {
            "tables": results[offset:offset + limit],
            "total": total,
            "facets": {
                "domains": DOMAINS,
                "tags": TAGS,
                "sensitivity": ["Normal", "PHI", "Restricted"],
            }
        }
    }
    # 의미 확장 정보 추가
    if expansion and expansion["expanded_terms"]:
        resp["data"]["expansion"] = expansion
    return resp


@router.post("/translate/physical-to-business")
async def translate_physical_to_business(body: Dict[str, List[str]]):
    """물리명 → 비즈니스명 변환"""
    physical_names = body.get("physical_names", [])
    result = {}

    for pname in physical_names:
        pname_upper = pname.upper()
        # 테이블 검색
        for table in SAMPLE_TABLES:
            if table["physical_name"].upper() == pname_upper:
                result[pname] = table["business_name"]
                break
            # 컬럼 검색
            for col in table["columns"]:
                if col["physical_name"].upper() == pname_upper:
                    result[pname] = col["business_name"]
                    break

    return {"success": True, "translations": result}


@router.get("/translate/business-to-physical")
async def translate_business_to_physical(q: str):
    """비즈니스명 → 물리명 변환"""
    q_lower = q.lower()
    results = []

    for table in SAMPLE_TABLES:
        if q_lower in table["business_name"].lower():
            results.append({
                "business_name": table["business_name"],
                "physical_name": table["physical_name"],
                "type": "table"
            })

        for col in table["columns"]:
            if q_lower in col["business_name"].lower():
                results.append({
                    "business_name": col["business_name"],
                    "physical_name": col["physical_name"],
                    "type": "column",
                    "table": table["physical_name"]
                })

    return {"success": True, "results": results}


@router.get("/sql-context")
async def get_sql_context(q: str):
    """SQL 생성용 컨텍스트 조회"""
    search_result = await search(q=q, domain=None, limit=5)
    tables = search_result.data.get("tables", [])

    context_parts = []
    for table in tables:
        cols = ", ".join([f"{c['physical_name']} ({c['business_name']})" for c in table["columns"][:5]])
        context_parts.append(f"테이블: {table['physical_name']} ({table['business_name']})\n컬럼: {cols}")

    return {
        "success": True,
        "context": "\n\n".join(context_parts),
        "tables": tables
    }


@router.get("/popular")
async def get_popular_data(top_n: int = 10):
    """인기 데이터"""
    sorted_tables = sorted(SAMPLE_TABLES, key=lambda x: x["usage_count"], reverse=True)
    return {"success": True, "data": {"items": [{"name": t["physical_name"], "business_name": t["business_name"], "count": t["usage_count"]} for t in sorted_tables[:top_n]]}}


@router.get("/related/{table_name}")
async def get_related_data(table_name: str, limit: int = 5):
    """연관 데이터"""
    # 같은 도메인의 테이블 반환
    target_table = None
    for t in SAMPLE_TABLES:
        if t["physical_name"].upper() == table_name.upper():
            target_table = t
            break

    if not target_table:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Table not found")

    related = [
        t for t in SAMPLE_TABLES
        if t["domain"] == target_table["domain"] and t["physical_name"] != target_table["physical_name"]
    ]

    return {"success": True, "related": related[:limit]}
