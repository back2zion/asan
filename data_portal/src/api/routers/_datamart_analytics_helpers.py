"""
데이터마트 Analytics 헬퍼 — 상수 & Airflow 유틸
datamart_analytics.py 에서 import 해서 사용
"""
import os
import base64
import asyncio
import json as _json
import urllib.request


# ═══════════════════════════════════════════════════
#  SNOMED CT 코드 → 한글명 매핑 (canonical, 단일 정의)
# ═══════════════════════════════════════════════════

SNOMED_NAMES: dict[str, str] = {
    "314529007": "비만 (BMI 30 이상)",
    "73595000": "스트레스 관련 장애",
    "66383009": "정상 임신 경과",
    "160903007": "풀타임 근로자",
    "160904001": "파트타임 근로자",
    "444814009": "바이러스성 부비동염",
    "423315002": "연간 의료 검진",
    "422650009": "사회적 고립",
    "741062008": "자궁내 피임장치 사용",
    "18718003": "약물 알레르기",
    "706893006": "위험 활동 종사",
    "109570002": "비정상 소견",
    "195662009": "급성 바이러스성 인두염",
    "424393004": "주거 불안정",
    "10509002": "급성 기관지염",
    "72892002": "정상 임신",
    "162864005": "체질량지수 30+ (비만)",
    "15777000": "사전 치료 필요 (Prediabetes)",
    "38341003": "고혈압성 장애",
    "40055000": "만성 부비동염",
    "19169002": "빈혈을 동반한 장애",
    "65363002": "이염 (중이염)",
    "44054006": "제2형 당뇨병",
    "55822004": "고지혈증",
    "230690007": "뇌졸중",
    "49436004": "심방세동",
    "53741008": "관상동맥 죽상경화증",
    "22298006": "심근경색",
    "59621000": "본태성 고혈압",
    "36971009": "부비동염",
    "233604007": "폐렴",
    "68496003": "다발성 장기 장애",
    "26929004": "알츠하이머병",
    "87433001": "폐색전증",
}

# ═══════════════════════════════════════════════════
#  방문 유형 코드 → 한글명
# ═══════════════════════════════════════════════════

VISIT_TYPE_NAMES: dict[int, str] = {9201: "입원", 9202: "외래", 9203: "응급"}

# ═══════════════════════════════════════════════════
#  품질 체크 설정 (Full = 5 도메인, Lite = 3 도메인)
# ═══════════════════════════════════════════════════

QUALITY_CHECKS_FULL: dict[str, tuple[str, str, int]] = {
    "Clinical": (
        "condition_occurrence",
        "COUNT(condition_source_value)+COUNT(condition_start_date)+COUNT(condition_concept_id)",
        3,
    ),
    "Imaging": (
        "imaging_study",
        "COUNT(finding_labels)+COUNT(view_position)+COUNT(patient_age)",
        3,
    ),
    "Admin": (
        "visit_occurrence",
        "COUNT(visit_start_date)+COUNT(visit_end_date)+COUNT(visit_concept_id)",
        3,
    ),
    "Lab": (
        "measurement",
        "COUNT(measurement_source_value)+COUNT(measurement_date)+COUNT(value_as_number)",
        3,
    ),
    "Drug": (
        "drug_exposure",
        "COUNT(drug_source_value)+COUNT(drug_exposure_start_date)+COUNT(quantity)",
        3,
    ),
}

QUALITY_CHECKS_LITE: dict[str, tuple[str, str, int]] = {
    "Clinical": (
        "condition_occurrence",
        "COUNT(condition_source_value)+COUNT(condition_start_date)+COUNT(condition_concept_id)",
        3,
    ),
    "Admin": (
        "visit_occurrence",
        "COUNT(visit_start_date)+COUNT(visit_end_date)+COUNT(visit_concept_id)",
        3,
    ),
    "Drug": (
        "drug_exposure",
        "COUNT(drug_source_value)+COUNT(drug_exposure_start_date)+COUNT(quantity)",
        3,
    ),
}


# ═══════════════════════════════════════════════════
#  Airflow 헬퍼
# ═══════════════════════════════════════════════════

def _fetch_airflow_sync() -> dict:
    """Airflow REST API 호출 (동기, executor에서 실행)"""
    info = {"total_dags": 0, "active": 0, "paused": 0,
            "recent_success": 0, "recent_failed": 0, "recent_running": 0}
    airflow_url = os.getenv("AIRFLOW_API_URL", "http://localhost:18080")
    creds = base64.b64encode(
        f"{os.getenv('AIRFLOW_USER', 'admin')}:{os.getenv('AIRFLOW_PASSWORD', 'admin')}".encode()
    ).decode()
    headers = {"Authorization": f"Basic {creds}"}

    req1 = urllib.request.Request(f"{airflow_url}/api/v1/dags", headers=headers)
    with urllib.request.urlopen(req1, timeout=3) as resp:
        dags = _json.loads(resp.read())
        info["total_dags"] = dags.get("total_entries", 0)
        for d in dags.get("dags", []):
            if d.get("is_paused"):
                info["paused"] += 1
            else:
                info["active"] += 1

    req2 = urllib.request.Request(
        f"{airflow_url}/api/v1/dags/~/dagRuns?limit=50&order_by=-start_date",
        headers=headers,
    )
    with urllib.request.urlopen(req2, timeout=3) as resp:
        runs = _json.loads(resp.read())
        for r in runs.get("dag_runs", []):
            st = r.get("state", "")
            if st == "success":
                info["recent_success"] += 1
            elif st == "failed":
                info["recent_failed"] += 1
            elif st == "running":
                info["recent_running"] += 1
    return info


async def _fetch_airflow_status() -> dict:
    """비동기 래퍼 — thread executor에서 Airflow API 호출"""
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _fetch_airflow_sync), timeout=5
        )
    except Exception:
        return {"total_dags": 0, "active": 0, "paused": 0,
                "recent_success": 0, "recent_failed": 0, "recent_running": 0}
