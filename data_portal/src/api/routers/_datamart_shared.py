"""
데이터마트 공유 유틸리티 — DB 설정, 테이블 메타데이터, 연결 관리
datamart.py / datamart_analytics.py 양쪽에서 임포트
"""
import os
from fastapi import HTTPException

from services.db_pool import get_pool

# ═══════════════════════════════════════════════════
#  OMOP CDM DB 연결 설정
# ═══════════════════════════════════════════════════

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}

# ═══════════════════════════════════════════════════
#  OMOP CDM 테이블 메타데이터
# ═══════════════════════════════════════════════════

# OMOP CDM 테이블 설명 (한국어)
TABLE_DESCRIPTIONS = {
    "person": "환자 인구통계학적 정보 (성별, 생년, 인종 등)",
    "visit_occurrence": "내원/입퇴원 이력 정보",
    "visit_detail": "내원 세부 정보 (병동 이동, 진료과 등)",
    "condition_occurrence": "진단/상병 발생 기록",
    "condition_era": "진단 기간 요약 (연속된 진단의 집약)",
    "drug_exposure": "약물 처방/투약 기록",
    "drug_era": "약물 투여 기간 요약",
    "procedure_occurrence": "시술/수술/검사 수행 기록",
    "measurement": "검사 결과 (Lab, Vital signs 등)",
    "observation": "관찰 기록 (진단 외 임상 소견)",
    "observation_period": "환자 관찰 기간 (데이터 수집 기간)",
    "device_exposure": "의료기기 사용 기록",
    "care_site": "진료 장소 (병원, 병동, 외래 등)",
    "provider": "의료진 정보",
    "location": "지역/주소 정보",
    "location_history": "지역 변경 이력",
    "cost": "의료비용 정보",
    "payer_plan_period": "보험 가입 기간 정보",
    "note": "임상 노트/기록 텍스트",
    "note_nlp": "임상 노트 NLP 처리 결과",
    "specimen_id": "검체 정보",
    "survey_conduct": "설문 수행 기록",
    "imaging_study": "영상 검사 기록 (CT, MRI 등)",
}

# OMOP CDM 테이블 카테고리
TABLE_CATEGORIES = {
    "Clinical Data": ["person", "visit_occurrence", "visit_detail", "condition_occurrence",
                      "drug_exposure", "procedure_occurrence", "measurement", "observation",
                      "device_exposure", "imaging_study"],
    "Health System": ["care_site", "provider", "location", "location_history"],
    "Derived": ["condition_era", "drug_era", "observation_period"],
    "Cost & Payer": ["cost", "payer_plan_period"],
    "Unstructured": ["note", "note_nlp"],
    "Other": ["specimen_id", "survey_conduct"],
}

# 허용된 테이블 이름 (SQL injection 방지)
ALLOWED_TABLES = set(TABLE_DESCRIPTIONS.keys())


# ═══════════════════════════════════════════════════
#  DB 연결 헬퍼
# ═══════════════════════════════════════════════════

async def get_connection():
    """OMOP CDM DB 연결 (풀에서 가져옴)"""
    try:
        pool = await get_pool()
        return await pool.acquire()
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"OMOP CDM 데이터베이스 연결 실패: {str(e)}",
        )


async def release_connection(conn):
    """연결을 풀에 반환"""
    try:
        pool = await get_pool()
        await pool.release(conn)
    except Exception:
        pass


def validate_table_name(table_name: str) -> str:
    """테이블 이름 검증 (SQL injection 방지)"""
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail=f"테이블을 찾을 수 없습니다: {table_name}")
    return table_name


def _serialize_value(val):
    """DB 값을 JSON-serializable 형태로 변환"""
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    # datetime, date, Decimal 등
    return str(val)
