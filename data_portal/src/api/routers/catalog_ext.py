"""
카탈로그 확장 API — 샘플 데이터, 커뮤니티 댓글, 데이터셋 스냅샷/레시피, 검색 보조
DPR-001 포털 UI/UX 요구사항: 상세 조회 및 데이터 재현성(Reproducibility) 확보
"""
import os
import json
import uuid
from fastapi import APIRouter, HTTPException, Query
import asyncpg

from ._catalog_ext_data import (
    CommentCreate, SnapshotCreate,
    _VERSION_TEMPLATES, _DEFAULT_VERSIONS,
    _TABLE_ALIASES, _PAGE_ALIASES, TABLE_DOMAINS,
    _fuzzy_match_tables, _fuzzy_match_pages, _generate_ai_summary,
    _ensure_seed,
)

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
                share_scope VARCHAR(20) DEFAULT 'private',
                created_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_catalog_snapshot_table ON catalog_snapshot(table_name);
            CREATE INDEX IF NOT EXISTS idx_catalog_snapshot_creator ON catalog_snapshot(creator);
        """)
        # share_scope 컬럼 마이그레이션 (기존 테이블에 없을 수 있음)
        await conn.execute("""
            ALTER TABLE catalog_snapshot ADD COLUMN IF NOT EXISTS share_scope VARCHAR(20) DEFAULT 'private'
        """)
        _tables_ensured = True
    finally:
        await conn.close()


async def _do_ensure_seed():
    """_ensure_seed wrapper: connection을 열어서 헬퍼에 전달"""
    conn = await get_connection()
    try:
        await _ensure_seed(conn)
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

@router.get("/tables/{table_name}/comments")
async def get_comments(table_name: str, limit: int = Query(default=50, le=200)):
    """테이블별 커뮤니티 댓글 조회"""
    await _ensure_tables()
    await _do_ensure_seed()
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

@router.get("/snapshots")
async def list_snapshots(
    table_name: str | None = None,
    creator: str | None = None,
    shared_only: bool = False,
    limit: int = Query(default=50, le=200),
):
    """데이터셋 스냅샷/레시피 목록 조회"""
    await _ensure_tables()
    await _do_ensure_seed()
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

        def _snap_dict(r):
            scope = r.get("share_scope", "private") if "share_scope" in r.keys() else ("public" if r["shared"] else "private")
            raw_filters = r["filters"]
            if isinstance(raw_filters, str):
                filters = json.loads(raw_filters)
            elif raw_filters:
                filters = dict(raw_filters)
            else:
                filters = {}
            return {
                "snapshot_id": r["snapshot_id"],
                "name": r["name"],
                "description": r["description"],
                "creator": r["creator"],
                "table_name": r["table_name"],
                "query_logic": r["query_logic"],
                "filters": filters,
                "columns": list(r["columns"]) if r["columns"] else [],
                "shared": r["shared"],
                "share_scope": scope,
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }

        return {
            "snapshots": [_snap_dict(r) for r in rows],
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
        scope = r.get("share_scope", "private") if "share_scope" in r.keys() else ("public" if r["shared"] else "private")
        raw_filters = r["filters"]
        if isinstance(raw_filters, str):
            filters = json.loads(raw_filters)
        elif raw_filters:
            filters = dict(raw_filters)
        else:
            filters = {}
        return {
            "snapshot_id": r["snapshot_id"],
            "name": r["name"],
            "description": r["description"],
            "creator": r["creator"],
            "table_name": r["table_name"],
            "query_logic": r["query_logic"],
            "filters": filters,
            "columns": list(r["columns"]) if r["columns"] else [],
            "shared": r["shared"],
            "share_scope": scope,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
    finally:
        await conn.close()


@router.post("/snapshots")
async def create_snapshot(body: SnapshotCreate):
    """데이터셋 스냅샷/레시피 생성 (고유 ID 자동 부여)"""
    await _ensure_tables()
    sid = str(uuid.uuid4())
    conn = await get_connection()
    try:
        await conn.execute(
            "INSERT INTO catalog_snapshot (snapshot_id, name, description, creator, table_name, query_logic, filters, columns, shared, share_scope) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10)",
            sid, body.name, body.description, body.creator, body.table_name,
            body.query_logic, json.dumps(body.filters or {}),
            body.columns, body.share_scope != "private", body.share_scope,
        )
        return {"snapshot_id": sid, "name": body.name, "share_scope": body.share_scope, "created": True}
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


# ───── Version History ─────

@router.get("/tables/{table_name}/versions")
async def get_table_versions(table_name: str):
    """테이블 버전 이력 조회 (DPR-001: 버전 관리)"""
    versions = _VERSION_TEMPLATES.get(table_name, _DEFAULT_VERSIONS)

    # 실제 DB에서 현재 행 수 + 컬럼 수 조회
    current_info = {}
    if table_name in ALLOWED_TABLES:
        try:
            conn = await get_connection()
            try:
                row_count = await conn.fetchval(
                    "SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = $1",
                    table_name,
                )
                col_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = $1",
                    table_name,
                )
                current_info = {
                    "row_count": row_count or 0,
                    "column_count": col_count or 0,
                }
            finally:
                await conn.close()
        except Exception:
            pass

    return {
        "table_name": table_name,
        "current": current_info,
        "versions": versions,
        "total": len(versions),
    }


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

@router.get("/search-suggest")
async def search_suggest(q: str = Query(..., min_length=1, max_length=200)):
    """GNB 통합 검색 보조 — 오타 보정 + AI 요약 (DPR-001)"""
    query = q.strip()

    # 1) exact match 확인
    exact_tables = [t for t in _TABLE_ALIASES if query.lower() in t or t in query.lower()]
    for tname, info in _TABLE_ALIASES.items():
        for alias in info["aliases"]:
            if query.lower() in alias.lower() or alias.lower() in query.lower():
                if tname not in exact_tables:
                    exact_tables.append(tname)

    # 2) fuzzy match (exact이 없을 때만 corrections 반환)
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
