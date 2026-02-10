"""
카탈로그 확장 API 헬퍼 — Pydantic 모델, 정적 데이터, 유틸리티 함수
catalog_ext.py 에서 추출 (리팩터링)
"""
import uuid
import difflib
from typing import Optional
from pydantic import BaseModel, Field


# ───── Pydantic Models ─────

class CommentCreate(BaseModel):
    author: str = Field(default="관리자", max_length=64)
    content: str = Field(..., min_length=1, max_length=2000)


class SnapshotCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    creator: str = Field(default="관리자", max_length=64)
    table_name: Optional[str] = None
    query_logic: Optional[str] = None
    filters: Optional[dict] = None
    columns: Optional[list[str]] = None
    shared: bool = False
    share_scope: str = Field(default="private", pattern=r"^(private|group|public)$")


# ───── Version History Data ─────

# 테이블별 버전 이력 시뮬레이션 (실 환경에서는 CDC/감사 로그 기반)
_VERSION_TEMPLATES: dict[str, list[dict]] = {
    "person": [
        {"version": "v3.2", "date": "2026-02-07", "type": "data_update", "author": "ETL Pipeline", "summary": "Synthea 합성 데이터 적재 완료 (76,074명)", "changes": ["76,074 rows inserted via ETL step 1", "gender_source_value: M(37,796)/F(38,278)"]},
        {"version": "v3.1", "date": "2026-02-05", "type": "schema_change", "author": "DBA 관리자", "summary": "person_source_value 인덱스 추가", "changes": ["CREATE INDEX idx_person_source ON person(person_source_value)"]},
        {"version": "v3.0", "date": "2026-01-15", "type": "schema_change", "author": "OMOP CDM ETL", "summary": "OMOP CDM v5.4 스키마 생성", "changes": ["CREATE TABLE person (13 columns)", "Primary key: person_id"]},
    ],
    "measurement": [
        {"version": "v2.4", "date": "2026-02-07", "type": "data_update", "author": "ETL Pipeline", "summary": "Synthea measurement 적재 (36.6M rows)", "changes": ["36,608,082 rows inserted", "measurement_concept_id 분포: 3,200+ distinct"]},
        {"version": "v2.3", "date": "2026-02-06", "type": "data_update", "author": "CDC Sync", "summary": "실시간 CDC 동기화 (12,500 rows)", "changes": ["12,500 rows incremental sync", "Source: Lab system HL7 feed"]},
        {"version": "v2.2", "date": "2026-02-01", "type": "schema_change", "author": "DBA 관리자", "summary": "value_as_number 인덱스 추가", "changes": ["CREATE INDEX idx_meas_value ON measurement(value_as_number)"]},
        {"version": "v2.1", "date": "2026-01-20", "type": "quality_check", "author": "품질 관리자", "summary": "NULL 비율 품질 검사 완료", "changes": ["value_as_number NULL 비율: 15.2%", "measurement_source_value 완전성: 99.8%"]},
        {"version": "v2.0", "date": "2026-01-15", "type": "schema_change", "author": "OMOP CDM ETL", "summary": "OMOP CDM v5.4 스키마 생성", "changes": ["CREATE TABLE measurement (18 columns)", "Primary key: measurement_id"]},
    ],
    "visit_occurrence": [
        {"version": "v2.1", "date": "2026-02-07", "type": "data_update", "author": "ETL Pipeline", "summary": "방문 데이터 적재 완료 (4.5M rows)", "changes": ["4,508,707 rows inserted", "입원(9201): 830K, 외래(9202): 3.2M, 응급(9203): 470K"]},
        {"version": "v2.0", "date": "2026-01-15", "type": "schema_change", "author": "OMOP CDM ETL", "summary": "OMOP CDM v5.4 스키마 생성", "changes": ["CREATE TABLE visit_occurrence (16 columns)"]},
    ],
}

# 기본 템플릿 (매핑에 없는 테이블용)
_DEFAULT_VERSIONS = [
    {"version": "v1.1", "date": "2026-02-07", "type": "data_update", "author": "ETL Pipeline", "summary": "Synthea 합성 데이터 적재 완료", "changes": ["Full data load via Synthea ETL pipeline"]},
    {"version": "v1.0", "date": "2026-01-15", "type": "schema_change", "author": "OMOP CDM ETL", "summary": "OMOP CDM v5.4 스키마 생성", "changes": ["Initial table creation"]},
]


# ───── Table / Page Aliases ─────

# 테이블 이름 → 한글 설명/별칭 매핑 (fuzzy matching 대상)
_TABLE_ALIASES: dict[str, dict] = {
    "person": {"label": "환자 인구통계", "aliases": ["환자", "인구", "사람", "patient", "demographic", "persom", "perso"]},
    "visit_occurrence": {"label": "내원/방문 이력", "aliases": ["방문", "내원", "입원", "외래", "응급", "visit", "visist"]},
    "condition_occurrence": {"label": "진단/상병 기록", "aliases": ["진단", "상병", "질환", "condition", "diagnosis", "condtion"]},
    "measurement": {"label": "검사결과/활력징후", "aliases": ["검사", "측정", "lab", "vital", "mesurement", "measurment"]},
    "drug_exposure": {"label": "약물 처방/투약", "aliases": ["약물", "처방", "투약", "drug", "medication", "drgu"]},
    "observation": {"label": "관찰/기타 임상", "aliases": ["관찰", "observaton", "obsrvation"]},
    "procedure_occurrence": {"label": "시술/수술 이력", "aliases": ["시술", "수술", "procedure", "procedur"]},
    "visit_detail": {"label": "방문 상세(병동이동)", "aliases": ["병동", "이동", "상세방문"]},
    "condition_era": {"label": "진단 기간 요약", "aliases": ["진단기간", "질환기간"]},
    "drug_era": {"label": "약물 기간 요약", "aliases": ["약물기간", "처방기간"]},
    "observation_period": {"label": "관찰 기간", "aliases": ["관찰기간"]},
    "cost": {"label": "비용 정보", "aliases": ["비용", "수가", "cost"]},
    "payer_plan_period": {"label": "보험/급여 기간", "aliases": ["보험", "급여", "payer"]},
    "care_site": {"label": "진료 부서/기관", "aliases": ["부서", "기관", "진료과"]},
    "provider": {"label": "의료진 정보", "aliases": ["의료진", "의사", "provider"]},
    "location": {"label": "지역 정보", "aliases": ["지역", "주소"]},
}

# 페이지 바로가기 (한글 + 영문 별칭)
_PAGE_ALIASES: dict[str, dict] = {
    "/dashboard": {"label": "홈 (대시보드)", "aliases": ["대시보드", "홈", "dashboard", "home", "dashboad"]},
    "/etl": {"label": "ETL 파이프라인", "aliases": ["ETL", "파이프라인", "etl", "pipeline"]},
    "/governance": {"label": "데이터 거버넌스", "aliases": ["거버넌스", "governance", "품질", "governace"]},
    "/catalog": {"label": "데이터 카탈로그", "aliases": ["카탈로그", "catalog", "catlog"]},
    "/datamart": {"label": "데이터마트", "aliases": ["데이터마트", "마트", "datamart"]},
    "/bi": {"label": "BI 대시보드", "aliases": ["BI", "비아이", "차트", "시각화"]},
    "/ai-environment": {"label": "AI 분석환경", "aliases": ["AI", "분석환경", "주피터", "jupyter"]},
    "/cdw": {"label": "CDW 연구지원", "aliases": ["CDW", "연구", "research", "코호트"]},
    "/ner": {"label": "비정형 구조화", "aliases": ["NER", "비정형", "구조화", "개체명"]},
    "/ontology": {"label": "의료 온톨로지", "aliases": ["온톨로지", "ontology", "SNOMED"]},
}


# ───── Table Domain Mapping (대시보드 레이크하우스 현황) ─────

TABLE_DOMAINS: dict[str, str] = {
    "person": "Demographics", "visit_occurrence": "Clinical",
    "visit_detail": "Clinical", "condition_occurrence": "Clinical",
    "condition_era": "Derived", "drug_exposure": "Clinical",
    "drug_era": "Derived", "procedure_occurrence": "Clinical",
    "measurement": "Lab/Vital", "observation": "Clinical",
    "observation_period": "Derived", "device_exposure": "Clinical",
    "care_site": "Health System", "provider": "Health System",
    "location": "Health System", "location_history": "Health System",
    "cost": "Financial", "payer_plan_period": "Financial",
    "note": "Unstructured", "note_nlp": "Unstructured",
}


# ───── Fuzzy Matching Functions ─────

def _fuzzy_match_tables(query: str, cutoff: float = 0.55) -> list[dict]:
    """테이블 이름/별칭 fuzzy matching"""
    q = query.lower().strip()
    results = []
    all_candidates = []
    candidate_to_table = {}

    for tname, info in _TABLE_ALIASES.items():
        candidates = [tname] + info["aliases"]
        for c in candidates:
            all_candidates.append(c.lower())
            candidate_to_table[c.lower()] = tname

    # difflib로 유사 후보 추출
    matches = difflib.get_close_matches(q, all_candidates, n=5, cutoff=cutoff)
    seen = set()
    for m in matches:
        tname = candidate_to_table[m]
        if tname not in seen:
            seen.add(tname)
            results.append({
                "table_name": tname,
                "label": _TABLE_ALIASES[tname]["label"],
                "matched_on": m,
                "score": round(difflib.SequenceMatcher(None, q, m).ratio(), 2),
            })

    return results


def _fuzzy_match_pages(query: str, cutoff: float = 0.5) -> list[dict]:
    """페이지 바로가기 fuzzy matching"""
    q = query.lower().strip()
    results = []
    all_candidates = []
    candidate_to_path = {}

    for path, info in _PAGE_ALIASES.items():
        candidates = [info["label"]] + info["aliases"]
        for c in candidates:
            all_candidates.append(c.lower())
            candidate_to_path[c.lower()] = path

    matches = difflib.get_close_matches(q, all_candidates, n=3, cutoff=cutoff)
    seen = set()
    for m in matches:
        path = candidate_to_path[m]
        if path not in seen:
            seen.add(path)
            results.append({
                "path": path,
                "label": _PAGE_ALIASES[path]["label"],
                "matched_on": m,
            })

    return results


def _generate_ai_summary(query: str, table_matches: list[dict]) -> str:
    """검색 결과 기반 AI 요약 생성 (템플릿 기반)"""
    if not table_matches:
        return ""

    table_descs = [f"{t['table_name']}({t['label']})" for t in table_matches[:3]]
    tables_str = ", ".join(table_descs)

    if len(table_matches) == 1:
        t = table_matches[0]
        return f"'{query}' 관련 테이블: {t['table_name']} — {t['label']}. 데이터 카탈로그에서 상세 조회하세요."
    else:
        return f"'{query}' 관련 테이블 {len(table_matches)}건 — {tables_str}. 카탈로그에서 상세 비교할 수 있습니다."


# ───── Seed Data Function ─────

async def _ensure_seed(conn):
    """시드 데이터 삽입 (댓글 + 스냅샷). 이미 데이터가 있으면 skip.
    conn: asyncpg connection (caller가 제공)
    """
    cnt = await conn.fetchval("SELECT COUNT(*) FROM catalog_comment")
    if cnt > 0:
        return
    await conn.execute("""
        INSERT INTO catalog_comment (table_name, author, content, created_at) VALUES
        ('person', '김연구원', 'person 테이블은 OMOP CDM의 핵심 테이블입니다. 다른 모든 테이블이 person_id로 조인됩니다.', NOW() - INTERVAL '3 days'),
        ('person', '박데이터', 'gender_source_value 컬럼은 M/F 텍스트 값입니다. 분석 시 참고하세요.', NOW() - INTERVAL '1 day'),
        ('measurement', '이분석가', 'measurement 테이블은 3,600만 건으로 대용량입니다. WHERE 조건 없이 전체 조회하지 마세요.', NOW() - INTERVAL '5 days'),
        ('measurement', '김연구원', 'value_as_number NULL 비율이 높은 concept들이 있으니 주의 바랍니다.', NOW() - INTERVAL '2 days'),
        ('visit_occurrence', '정임상', '방문유형: 9201=입원, 9202=외래, 9203=응급. 필터링에 참고하세요.', NOW() - INTERVAL '4 days'),
        ('condition_occurrence', '박데이터', 'SNOMED CT 코드 기준입니다. 당뇨=44054006, 고혈압=38341003', NOW() - INTERVAL '6 days'),
        ('drug_exposure', '이분석가', '약물 처방 데이터이며, days_supply 컬럼으로 처방일수를 확인할 수 있습니다.', NOW() - INTERVAL '1 day'),
        ('observation', '정임상', 'observation 테이블은 2,100만 건입니다. 배치 분석 시 파티션 사용을 권장합니다.', NOW() - INTERVAL '3 days')
    """)
    # Seed snapshots
    await conn.execute("""
        INSERT INTO catalog_snapshot (snapshot_id, name, description, creator, table_name, query_logic, filters, columns, shared, share_scope, created_at) VALUES
        ($1, '당뇨 환자 코호트', '당뇨(44054006) 진단 환자의 인구통계학적 정보', '김연구원', 'person',
         'SELECT p.* FROM person p WHERE p.person_id IN (SELECT DISTINCT person_id FROM condition_occurrence WHERE condition_concept_id = 44054006)',
         '{"condition_concept_id": 44054006}'::jsonb, ARRAY['person_id','gender_source_value','year_of_birth'], TRUE, 'public', NOW() - INTERVAL '7 days'),
        ($2, '2024년 입원 환자', '2024년 입원(9201) 내원 기록', '박데이터', 'visit_occurrence',
         'SELECT * FROM visit_occurrence WHERE visit_concept_id = 9201 AND visit_start_date >= ''2024-01-01''',
         '{"visit_concept_id": 9201, "year": 2024}'::jsonb, ARRAY['visit_occurrence_id','person_id','visit_start_date','visit_end_date'], TRUE, 'group', NOW() - INTERVAL '3 days'),
        ($3, '검사결과 이상치', 'value_as_number가 정상 범위를 벗어난 검사 결과', '이분석가', 'measurement',
         'SELECT * FROM measurement WHERE value_as_number > range_high OR value_as_number < range_low',
         '{"anomaly": true}'::jsonb, ARRAY['measurement_id','person_id','measurement_concept_id','value_as_number'], FALSE, 'private', NOW() - INTERVAL '1 day')
    """, str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()))
