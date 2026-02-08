"""
카탈로그 확장 API — 샘플 데이터, 커뮤니티 댓글, 데이터셋 스냅샷/레시피, 검색 보조
DPR-001 포털 UI/UX 요구사항: 상세 조회 및 데이터 재현성(Reproducibility) 확보
"""
import os
import uuid
import difflib
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import asyncpg

router = APIRouter(prefix="/catalog-ext", tags=["CatalogExt"])

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}

ALLOWED_TABLES = {
    "person", "visit_occurrence", "visit_detail", "condition_occurrence",
    "condition_era", "drug_exposure", "drug_era", "procedure_occurrence",
    "measurement", "observation", "observation_period", "device_exposure",
    "care_site", "provider", "location", "location_history", "cost",
    "payer_plan_period", "note", "note_nlp", "specimen_id",
    "survey_conduct", "imaging_study",
}

_tables_ensured = False


async def get_connection():
    return await asyncpg.connect(**OMOP_DB_CONFIG)


async def _ensure_tables():
    global _tables_ensured
    if _tables_ensured:
        return
    conn = await get_connection()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS catalog_comment (
                comment_id SERIAL PRIMARY KEY,
                table_name VARCHAR(128) NOT NULL,
                author VARCHAR(64) NOT NULL DEFAULT '관리자',
                content TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_catalog_comment_table ON catalog_comment(table_name);

            CREATE TABLE IF NOT EXISTS catalog_snapshot (
                snapshot_id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                description TEXT,
                creator VARCHAR(64) NOT NULL DEFAULT '관리자',
                table_name VARCHAR(128),
                query_logic TEXT,
                filters JSONB DEFAULT '{}',
                columns TEXT[],
                shared BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_catalog_snapshot_table ON catalog_snapshot(table_name);
            CREATE INDEX IF NOT EXISTS idx_catalog_snapshot_creator ON catalog_snapshot(creator);
        """)
        _tables_ensured = True
    finally:
        await conn.close()


async def _ensure_seed():
    conn = await get_connection()
    try:
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
            INSERT INTO catalog_snapshot (snapshot_id, name, description, creator, table_name, query_logic, filters, columns, shared, created_at) VALUES
            ($1, '당뇨 환자 코호트', '당뇨(44054006) 진단 환자의 인구통계학적 정보', '김연구원', 'person',
             'SELECT p.* FROM person p WHERE p.person_id IN (SELECT DISTINCT person_id FROM condition_occurrence WHERE condition_concept_id = 44054006)',
             '{"condition_concept_id": 44054006}'::jsonb, ARRAY['person_id','gender_source_value','year_of_birth'], TRUE, NOW() - INTERVAL '7 days'),
            ($2, '2024년 입원 환자', '2024년 입원(9201) 내원 기록', '박데이터', 'visit_occurrence',
             'SELECT * FROM visit_occurrence WHERE visit_concept_id = 9201 AND visit_start_date >= ''2024-01-01''',
             '{"visit_concept_id": 9201, "year": 2024}'::jsonb, ARRAY['visit_occurrence_id','person_id','visit_start_date','visit_end_date'], TRUE, NOW() - INTERVAL '3 days'),
            ($3, '검사결과 이상치', 'value_as_number가 정상 범위를 벗어난 검사 결과', '이분석가', 'measurement',
             'SELECT * FROM measurement WHERE value_as_number > range_high OR value_as_number < range_low',
             '{"anomaly": true}'::jsonb, ARRAY['measurement_id','person_id','measurement_concept_id','value_as_number'], FALSE, NOW() - INTERVAL '1 day')
        """, str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()))
    finally:
        await conn.close()


# ───── Sample Data ─────

@router.get("/tables/{table_name}/sample-data")
async def get_sample_data(table_name: str, limit: int = Query(default=10, le=50)):
    """테이블의 실제 샘플 데이터 Top-N 행 조회 (SQL injection 방지)"""
    if table_name not in ALLOWED_TABLES:
        raise HTTPException(status_code=400, detail=f"허용되지 않은 테이블: {table_name}")
    conn = await get_connection()
    try:
        # 컬럼 목록 조회
        col_rows = await conn.fetch(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position",
            table_name,
        )
        if not col_rows:
            raise HTTPException(status_code=404, detail=f"테이블 '{table_name}'을 찾을 수 없습니다")

        columns = [r["column_name"] for r in col_rows]
        col_types = {r["column_name"]: r["data_type"] for r in col_rows}

        # 최대 10개 컬럼만 (너무 넓은 테이블 대응)
        display_cols = columns[:10]
        col_list = ", ".join(display_cols)

        rows = await conn.fetch(f"SELECT {col_list} FROM {table_name} LIMIT $1", limit)

        return {
            "table_name": table_name,
            "columns": display_cols,
            "column_types": {c: col_types[c] for c in display_cols},
            "rows": [dict(r) for r in rows],
            "total_columns": len(columns),
            "displayed_columns": len(display_cols),
            "row_count": len(rows),
        }
    finally:
        await conn.close()


# ───── Community Comments ─────

class CommentCreate(BaseModel):
    author: str = Field(default="관리자", max_length=64)
    content: str = Field(..., min_length=1, max_length=2000)


@router.get("/tables/{table_name}/comments")
async def get_comments(table_name: str, limit: int = Query(default=50, le=200)):
    """테이블별 커뮤니티 댓글 조회"""
    await _ensure_tables()
    await _ensure_seed()
    conn = await get_connection()
    try:
        rows = await conn.fetch(
            "SELECT comment_id, table_name, author, content, created_at "
            "FROM catalog_comment WHERE table_name = $1 ORDER BY created_at DESC LIMIT $2",
            table_name, limit,
        )
        return {
            "table_name": table_name,
            "comments": [
                {
                    "id": str(r["comment_id"]),
                    "author": r["author"],
                    "content": r["content"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ],
            "total": len(rows),
        }
    finally:
        await conn.close()


@router.post("/tables/{table_name}/comments")
async def create_comment(table_name: str, body: CommentCreate):
    """커뮤니티 댓글 등록"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        row = await conn.fetchrow(
            "INSERT INTO catalog_comment (table_name, author, content) "
            "VALUES ($1, $2, $3) RETURNING comment_id, created_at",
            table_name, body.author, body.content,
        )
        return {
            "id": str(row["comment_id"]),
            "table_name": table_name,
            "author": body.author,
            "content": body.content,
            "created_at": row["created_at"].isoformat(),
        }
    finally:
        await conn.close()


@router.delete("/tables/{table_name}/comments/{comment_id}")
async def delete_comment(table_name: str, comment_id: int):
    """커뮤니티 댓글 삭제"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        result = await conn.execute(
            "DELETE FROM catalog_comment WHERE comment_id = $1 AND table_name = $2",
            comment_id, table_name,
        )
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="댓글을 찾을 수 없습니다")
        return {"deleted": True, "comment_id": comment_id}
    finally:
        await conn.close()


# ───── Dataset Snapshots / Recipes ─────

class SnapshotCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = None
    creator: str = Field(default="관리자", max_length=64)
    table_name: Optional[str] = None
    query_logic: Optional[str] = None
    filters: Optional[dict] = None
    columns: Optional[list[str]] = None
    shared: bool = False


@router.get("/snapshots")
async def list_snapshots(
    table_name: Optional[str] = None,
    creator: Optional[str] = None,
    shared_only: bool = False,
    limit: int = Query(default=50, le=200),
):
    """데이터셋 스냅샷/레시피 목록 조회"""
    await _ensure_tables()
    await _ensure_seed()
    conn = await get_connection()
    try:
        query = "SELECT * FROM catalog_snapshot WHERE TRUE"
        params: list = []
        idx = 1
        if table_name:
            query += f" AND table_name = ${idx}"
            params.append(table_name)
            idx += 1
        if creator:
            query += f" AND creator = ${idx}"
            params.append(creator)
            idx += 1
        if shared_only:
            query += " AND shared = TRUE"
        query += f" ORDER BY created_at DESC LIMIT ${idx}"
        params.append(limit)

        rows = await conn.fetch(query, *params)
        return {
            "snapshots": [
                {
                    "snapshot_id": r["snapshot_id"],
                    "name": r["name"],
                    "description": r["description"],
                    "creator": r["creator"],
                    "table_name": r["table_name"],
                    "query_logic": r["query_logic"],
                    "filters": dict(r["filters"]) if r["filters"] else {},
                    "columns": list(r["columns"]) if r["columns"] else [],
                    "shared": r["shared"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ],
            "total": len(rows),
        }
    finally:
        await conn.close()


@router.get("/snapshots/{snapshot_id}")
async def get_snapshot(snapshot_id: str):
    """스냅샷 상세 조회"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        r = await conn.fetchrow("SELECT * FROM catalog_snapshot WHERE snapshot_id = $1", snapshot_id)
        if not r:
            raise HTTPException(status_code=404, detail="스냅샷을 찾을 수 없습니다")
        return {
            "snapshot_id": r["snapshot_id"],
            "name": r["name"],
            "description": r["description"],
            "creator": r["creator"],
            "table_name": r["table_name"],
            "query_logic": r["query_logic"],
            "filters": dict(r["filters"]) if r["filters"] else {},
            "columns": list(r["columns"]) if r["columns"] else [],
            "shared": r["shared"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
    finally:
        await conn.close()


@router.post("/snapshots")
async def create_snapshot(body: SnapshotCreate):
    """데이터셋 스냅샷/레시피 생성 (고유 ID 자동 부여)"""
    await _ensure_tables()
    import json
    sid = str(uuid.uuid4())
    conn = await get_connection()
    try:
        await conn.execute(
            "INSERT INTO catalog_snapshot (snapshot_id, name, description, creator, table_name, query_logic, filters, columns, shared) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)",
            sid, body.name, body.description, body.creator, body.table_name,
            body.query_logic, json.dumps(body.filters or {}),
            body.columns, body.shared,
        )
        return {"snapshot_id": sid, "name": body.name, "created": True}
    finally:
        await conn.close()


@router.delete("/snapshots/{snapshot_id}")
async def delete_snapshot(snapshot_id: str):
    """스냅샷 삭제"""
    await _ensure_tables()
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM catalog_snapshot WHERE snapshot_id = $1", snapshot_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="스냅샷을 찾을 수 없습니다")
        return {"deleted": True, "snapshot_id": snapshot_id}
    finally:
        await conn.close()


# ───── Dashboard Lakehouse Overview Data ─────

@router.get("/lakehouse-overview")
async def lakehouse_overview():
    """CDW/EDW 통합 데이터 레이크하우스 현황 (대시보드 메인화면용)"""
    conn = await get_connection()
    try:
        # 테이블별 행 수 + 도메인 정보
        table_stats = await conn.fetch("""
            SELECT relname AS table_name, n_live_tup AS row_count
            FROM pg_stat_user_tables
            WHERE schemaname = 'public' AND n_live_tup > 0
            ORDER BY n_live_tup DESC
        """)

        TABLE_DOMAINS = {
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

        assets = []
        total_rows = 0
        domain_counts: dict[str, int] = {}
        for r in table_stats:
            tname = r["table_name"]
            if tname.startswith("etl_") or tname.startswith("sec_") or tname.startswith("perm_") or tname.startswith("catalog_"):
                continue
            domain = TABLE_DOMAINS.get(tname, "Other")
            rows = r["row_count"]
            total_rows += rows
            domain_counts[domain] = domain_counts.get(domain, 0) + rows
            assets.append({
                "table_name": tname,
                "domain": domain,
                "row_count": rows,
                "size_label": f"{rows / 1_000_000:.1f}M" if rows >= 1_000_000 else f"{rows / 1_000:.1f}K" if rows >= 1_000 else str(rows),
            })

        # 환자 수
        patient_count = await conn.fetchval("SELECT COUNT(*) FROM person") or 0

        # 도메인 분포
        domain_distribution = [
            {"domain": k, "row_count": v, "percentage": round(v / total_rows * 100, 1) if total_rows > 0 else 0}
            for k, v in sorted(domain_counts.items(), key=lambda x: -x[1])
        ]

        # 추천 데이터셋 (활용도 높은 핵심 테이블)
        recommended = [
            {"table_name": "person", "reason": "모든 분석의 기본 — 환자 인구통계", "usage_score": 98},
            {"table_name": "visit_occurrence", "reason": "내원/입퇴원 이력 — 코호트 정의 필수", "usage_score": 95},
            {"table_name": "condition_occurrence", "reason": "진단/상병 — 질환 기반 연구", "usage_score": 92},
            {"table_name": "measurement", "reason": "검사결과 — Lab/Vital 분석", "usage_score": 90},
            {"table_name": "drug_exposure", "reason": "처방/투약 — 약물 연구", "usage_score": 88},
            {"table_name": "procedure_occurrence", "reason": "시술/수술 — 중재 연구", "usage_score": 85},
        ]

        return {
            "total_tables": len(assets),
            "total_rows": total_rows,
            "total_rows_label": f"{total_rows / 1_000_000:.1f}M" if total_rows >= 1_000_000 else str(total_rows),
            "patient_count": patient_count,
            "assets": assets[:30],
            "domain_distribution": domain_distribution,
            "recommended_datasets": recommended,
            "cdw_tables": len([a for a in assets if a["domain"] in ("Clinical", "Lab/Vital", "Demographics", "Derived")]),
            "edw_tables": len([a for a in assets if a["domain"] in ("Financial", "Health System")]),
            "unstructured_tables": len([a for a in assets if a["domain"] == "Unstructured"]),
        }
    finally:
        await conn.close()


@router.get("/recent-searches")
async def get_recent_searches():
    """최근 검색 이력 (서버사이드 — 데모용 정적 데이터)"""
    return {
        "searches": [
            {"query": "당뇨 환자", "time": "5분 전", "results": 3},
            {"query": "measurement", "time": "15분 전", "results": 1},
            {"query": "입원 방문", "time": "1시간 전", "results": 2},
            {"query": "약물 처방 기록", "time": "3시간 전", "results": 4},
            {"query": "person 테이블", "time": "어제", "results": 1},
        ]
    }


# ───── Search Suggest (오타 보정 + AI 요약) ─────

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


@router.get("/search-suggest")
async def search_suggest(q: str = Query(..., min_length=1, max_length=200)):
    """GNB 통합 검색 보조 — 오타 보정 + AI 요약 (DPR-001)"""
    query = q.strip()

    # 1) exact match 확인
    exact_tables = [t for t in _TABLE_ALIASES if query.lower() in t or t in query.lower()]
    exact_aliases = []
    for tname, info in _TABLE_ALIASES.items():
        for alias in info["aliases"]:
            if query.lower() in alias.lower() or alias.lower() in query.lower():
                if tname not in exact_tables:
                    exact_tables.append(tname)

    # 2) fuzzy match (exact이 없을 때만 corrections 반환)
    corrections = []
    if not exact_tables:
        table_fuzzy = _fuzzy_match_tables(query)
        page_fuzzy = _fuzzy_match_pages(query)
        corrections = {
            "tables": table_fuzzy,
            "pages": page_fuzzy,
        }
    else:
        corrections = {"tables": [], "pages": []}

    # 3) AI 요약 생성 (exact 또는 fuzzy 기반)
    summary_targets = (
        [{"table_name": t, "label": _TABLE_ALIASES[t]["label"]} for t in exact_tables]
        if exact_tables
        else corrections.get("tables", [])
    )
    ai_summary = _generate_ai_summary(query, summary_targets)

    return {
        "query": query,
        "ai_summary": ai_summary,
        "corrections": corrections,
        "has_corrections": bool(corrections.get("tables") or corrections.get("pages")),
    }
