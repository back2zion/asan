"""
마-5: 데이터 마트 추천 / 템플릿 / SLA 관리 API
Task #24: 데이터 마트 72->90% — 자동 마트 추천, 사전 구축 템플릿, SLA 추적
"""
import json
from datetime import datetime, timezone
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/mart-ext", tags=["MartExtended"])

# ── DB helpers (pool-based) ──────────────────────────────────────────

async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()


async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)


# ── Pydantic models ──────────────────────────────────────────────────

class TemplateCreate(BaseModel):
    name: str = Field(..., max_length=100)
    category: str = Field(..., pattern=r"^(disease|research|management)$")
    description: Optional[str] = Field(None, max_length=1000)
    sql_definition: str = Field(..., min_length=5)
    target_tables: Optional[List[str]] = Field(default_factory=list)


class RecommendRequest(BaseModel):
    focus_area: str = Field(..., pattern=r"^(disease|research|management)$")
    conditions: Optional[List[str]] = None


class SLACreate(BaseModel):
    mart_name: str = Field(..., max_length=100)
    refresh_interval_min: int = Field(default=60, ge=1)
    max_stale_min: int = Field(default=120, ge=1)
    alert_threshold_sec: int = Field(default=300, ge=1)


class SLAUpdate(BaseModel):
    mart_name: Optional[str] = Field(None, max_length=100)
    refresh_interval_min: Optional[int] = Field(None, ge=1)
    max_stale_min: Optional[int] = Field(None, ge=1)
    status: Optional[str] = Field(None, pattern=r"^(active|paused|disabled)$")
    alert_threshold_sec: Optional[int] = Field(None, ge=1)


# ── Lazy table creation ──────────────────────────────────────────────

_tbl_ok = False


async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS mart_template (
            template_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            category VARCHAR(50) NOT NULL,
            description TEXT,
            sql_definition TEXT NOT NULL,
            target_tables JSONB DEFAULT '[]',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS mart_sla (
            sla_id SERIAL PRIMARY KEY,
            mart_name VARCHAR(100) NOT NULL,
            refresh_interval_min INTEGER DEFAULT 60,
            max_stale_min INTEGER DEFAULT 120,
            last_refreshed_at TIMESTAMPTZ,
            next_refresh_at TIMESTAMPTZ,
            status VARCHAR(20) DEFAULT 'active',
            alert_threshold_sec INTEGER DEFAULT 300,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS mart_sla_log (
            log_id BIGSERIAL PRIMARY KEY,
            sla_id INTEGER,
            event_type VARCHAR(30),
            duration_sec DOUBLE PRECISION,
            row_count INTEGER,
            status VARCHAR(20),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS mart_usage_log (
            log_id BIGSERIAL PRIMARY KEY,
            user_id VARCHAR(100) DEFAULT 'anonymous',
            template_id INTEGER,
            action VARCHAR(30),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_mart_usage_log_template ON mart_usage_log(template_id, created_at);
        CREATE INDEX IF NOT EXISTS idx_mart_usage_log_user ON mart_usage_log(user_id, created_at);
    """)
    # Seed templates if empty
    cnt = await conn.fetchval("SELECT COUNT(*) FROM mart_template")
    if cnt == 0:
        await _seed_templates(conn)
    _tbl_ok = True


async def _seed_templates(conn):
    """9개 사전 구축 마트 템플릿 시드 데이터"""
    templates = [
        # ── Disease ──
        (
            "당뇨 코호트 마트",
            "disease",
            "제2형 당뇨병(SNOMED 44054006) 환자 코호트 마트. 인구통계+진단+방문 통합.",
            (
                "SELECT p.person_id, p.gender_source_value, p.year_of_birth, "
                "co.condition_start_date, co.condition_end_date, "
                "COUNT(vo.visit_occurrence_id) AS visit_count "
                "FROM person p "
                "JOIN condition_occurrence co ON p.person_id = co.person_id "
                "LEFT JOIN visit_occurrence vo ON p.person_id = vo.person_id "
                "WHERE co.condition_source_value = '44054006' "
                "GROUP BY p.person_id, p.gender_source_value, p.year_of_birth, "
                "co.condition_start_date, co.condition_end_date"
            ),
            '["person","condition_occurrence","visit_occurrence"]',
        ),
        (
            "고혈압 환자 마트",
            "disease",
            "고혈압성 장애(SNOMED 38341003) 환자 코호트 마트. 인구통계+진단+약물 통합.",
            (
                "SELECT p.person_id, p.gender_source_value, p.year_of_birth, "
                "co.condition_start_date, co.condition_end_date, "
                "COUNT(de.drug_exposure_id) AS drug_count "
                "FROM person p "
                "JOIN condition_occurrence co ON p.person_id = co.person_id "
                "LEFT JOIN drug_exposure de ON p.person_id = de.person_id "
                "WHERE co.condition_source_value = '38341003' "
                "GROUP BY p.person_id, p.gender_source_value, p.year_of_birth, "
                "co.condition_start_date, co.condition_end_date"
            ),
            '["person","condition_occurrence","drug_exposure"]',
        ),
        (
            "다제약물 사용 마트",
            "disease",
            "5개 이상 약물을 처방받은 다약제(polypharmacy) 환자 마트.",
            (
                "SELECT person_id, COUNT(DISTINCT drug_source_value) AS drug_count "
                "FROM drug_exposure "
                "GROUP BY person_id "
                "HAVING COUNT(DISTINCT drug_source_value) >= 5"
            ),
            '["drug_exposure"]',
        ),
        # ── Research ──
        (
            "입원 재원일수 분석 마트",
            "research",
            "입원(visit_concept_id=9201) 환자의 재원일수 분석 마트.",
            (
                "SELECT person_id, visit_occurrence_id, visit_start_date, visit_end_date, "
                "EXTRACT(DAY FROM visit_end_date - visit_start_date) AS los_days "
                "FROM visit_occurrence "
                "WHERE visit_concept_id = 9201 "
                "AND visit_end_date IS NOT NULL"
            ),
            '["visit_occurrence"]',
        ),
        (
            "검사치 이상 환자 마트",
            "research",
            "검사 결과가 정상 범위를 벗어난 환자 마트.",
            (
                "SELECT person_id, measurement_source_value, value_as_number, "
                "range_low, range_high, measurement_date "
                "FROM measurement "
                "WHERE value_as_number IS NOT NULL "
                "AND (value_as_number < range_low OR value_as_number > range_high)"
            ),
            '["measurement"]',
        ),
        (
            "응급실 방문 패턴 마트",
            "research",
            "응급실(visit_concept_id=9203) 방문 패턴 분석 마트.",
            (
                "SELECT person_id, visit_occurrence_id, visit_start_date, "
                "EXTRACT(DOW FROM visit_start_date) AS day_of_week, "
                "EXTRACT(HOUR FROM visit_start_date) AS hour_of_day "
                "FROM visit_occurrence "
                "WHERE visit_concept_id = 9203"
            ),
            '["visit_occurrence"]',
        ),
        # ── Management ──
        (
            "환자 방문 현황 마트",
            "management",
            "방문 유형별 방문 건수 및 환자 수 현황 마트.",
            (
                "SELECT visit_concept_id, "
                "COUNT(*) AS visit_count, "
                "COUNT(DISTINCT person_id) AS patient_count "
                "FROM visit_occurrence "
                "GROUP BY visit_concept_id"
            ),
            '["visit_occurrence"]',
        ),
        (
            "데이터 품질 현황 마트",
            "management",
            "주요 테이블 핵심 컬럼의 NULL 비율 분석 마트.",
            (
                "SELECT 'condition_occurrence' AS table_name, "
                "COUNT(*) AS total_rows, "
                "ROUND(COUNT(condition_source_value)::numeric / NULLIF(COUNT(*),0) * 100, 2) AS source_value_fill_pct, "
                "ROUND(COUNT(condition_start_date)::numeric / NULLIF(COUNT(*),0) * 100, 2) AS start_date_fill_pct "
                "FROM condition_occurrence "
                "UNION ALL "
                "SELECT 'drug_exposure', COUNT(*), "
                "ROUND(COUNT(drug_source_value)::numeric / NULLIF(COUNT(*),0) * 100, 2), "
                "ROUND(COUNT(drug_exposure_start_date)::numeric / NULLIF(COUNT(*),0) * 100, 2) "
                "FROM drug_exposure "
                "UNION ALL "
                "SELECT 'measurement', COUNT(*), "
                "ROUND(COUNT(measurement_source_value)::numeric / NULLIF(COUNT(*),0) * 100, 2), "
                "ROUND(COUNT(value_as_number)::numeric / NULLIF(COUNT(*),0) * 100, 2) "
                "FROM measurement"
            ),
            '["condition_occurrence","drug_exposure","measurement"]',
        ),
        (
            "테이블 사용량 현황 마트",
            "management",
            "PostgreSQL pg_stat_user_tables 기반 테이블 사용량/크기 분석 마트.",
            (
                "SELECT relname AS table_name, "
                "n_live_tup AS live_rows, "
                "n_dead_tup AS dead_rows, "
                "seq_scan, idx_scan, "
                "pg_size_pretty(pg_total_relation_size(relid)) AS total_size "
                "FROM pg_stat_user_tables "
                "WHERE schemaname = 'public' "
                "ORDER BY n_live_tup DESC"
            ),
            '["pg_stat_user_tables"]',
        ),
    ]
    for name, category, description, sql_def, target_tables in templates:
        await conn.execute(
            "INSERT INTO mart_template (name, category, description, sql_definition, target_tables) "
            "VALUES ($1, $2, $3, $4, $5::jsonb)",
            name, category, description, sql_def, target_tables,
        )


def _serialize(val):
    if val is None:
        return None
    if isinstance(val, (int, float, str, bool)):
        return val
    if isinstance(val, datetime):
        return val.isoformat()
    return str(val)


# ── Template endpoints ───────────────────────────────────────────────

@router.get("/templates")
async def list_templates(category: Optional[str] = Query(None, pattern=r"^(disease|research|management)$")):
    """마트 템플릿 목록 조회 (카테고리 필터 가능)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        if category:
            rows = await conn.fetch(
                "SELECT * FROM mart_template WHERE category = $1 ORDER BY template_id",
                category,
            )
        else:
            rows = await conn.fetch("SELECT * FROM mart_template ORDER BY template_id")
        return {
            "templates": [
                {k: _serialize(v) for k, v in dict(r).items()} for r in rows
            ],
            "total": len(rows),
        }
    finally:
        await _rel(conn)


@router.get("/templates/{template_id}")
async def get_template(template_id: int):
    """마트 템플릿 상세 조회"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow(
            "SELECT * FROM mart_template WHERE template_id = $1", template_id
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"템플릿 ID {template_id}를 찾을 수 없습니다")
        # Log usage
        await conn.execute(
            "INSERT INTO mart_usage_log (template_id, action) VALUES ($1, 'view')",
            template_id)
        return {k: _serialize(v) for k, v in dict(row).items()}
    finally:
        await _rel(conn)


@router.post("/templates")
async def create_template(body: TemplateCreate):
    """커스텀 마트 템플릿 생성"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow(
            "INSERT INTO mart_template (name, category, description, sql_definition, target_tables) "
            "VALUES ($1, $2, $3, $4, $5::jsonb) RETURNING template_id, created_at",
            body.name,
            body.category,
            body.description,
            body.sql_definition,
            json.dumps(body.target_tables) if body.target_tables else "[]",
        )
        return {
            "template_id": row["template_id"],
            "created_at": row["created_at"].isoformat(),
        }
    finally:
        await _rel(conn)


@router.post("/recommend")
async def recommend_marts(body: RecommendRequest):
    """사용 패턴 기반 마트 자동 추천 — 인기도 + 협업필터링 + 카테고리"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)

        # Step 1: Get all templates in the requested category
        all_templates = await conn.fetch(
            "SELECT * FROM mart_template WHERE category = $1 ORDER BY template_id",
            body.focus_area)

        if not all_templates:
            return {"focus_area": body.focus_area, "recommendations": [], "total": 0, "custom_sql": None}

        template_scores = {}
        for t in all_templates:
            template_scores[t["template_id"]] = {
                "template": {k: _serialize(v) for k, v in dict(t).items()},
                "popularity_score": 0.0,
                "collab_score": 0.0,
                "category_score": 1.0,  # All match category
                "total_score": 0.0,
            }

        # Step 2: Popularity — usage count in last 30 days
        popularity = await conn.fetch("""
            SELECT template_id, COUNT(*) AS usage_count
            FROM mart_usage_log
            WHERE created_at > NOW() - INTERVAL '30 days'
                AND template_id = ANY($1::int[])
            GROUP BY template_id
            ORDER BY usage_count DESC
        """, [t["template_id"] for t in all_templates])

        max_usage = max((r["usage_count"] for r in popularity), default=1)
        for r in popularity:
            tid = r["template_id"]
            if tid in template_scores:
                template_scores[tid]["popularity_score"] = r["usage_count"] / max_usage

        # Step 3: Collaborative filtering — "users who used template X also used template Y"
        # Find most popular template in this category, then find co-used templates
        if popularity:
            top_template_id = popularity[0]["template_id"]
            collab = await conn.fetch("""
                SELECT ul2.template_id, COUNT(DISTINCT ul2.user_id) AS co_users
                FROM mart_usage_log ul1
                JOIN mart_usage_log ul2 ON ul1.user_id = ul2.user_id
                    AND ul1.template_id != ul2.template_id
                WHERE ul1.template_id = $1
                    AND ul2.template_id = ANY($2::int[])
                    AND ul1.created_at > NOW() - INTERVAL '30 days'
                    AND ul2.created_at > NOW() - INTERVAL '30 days'
                GROUP BY ul2.template_id
                ORDER BY co_users DESC
            """, top_template_id, [t["template_id"] for t in all_templates])

            max_co = max((r["co_users"] for r in collab), default=1)
            for r in collab:
                tid = r["template_id"]
                if tid in template_scores:
                    template_scores[tid]["collab_score"] = r["co_users"] / max_co

        # Step 4: Calculate total score = popularity*0.4 + collab*0.4 + category*0.2
        for tid, scores in template_scores.items():
            scores["total_score"] = round(
                scores["popularity_score"] * 0.4 +
                scores["collab_score"] * 0.4 +
                scores["category_score"] * 0.2, 3)

        # Sort by total_score descending, take top 5
        sorted_templates = sorted(template_scores.values(), key=lambda x: x["total_score"], reverse=True)[:5]

        recommendations = []
        for s in sorted_templates:
            rec = s["template"]
            rec["recommendation_score"] = s["total_score"]
            rec["popularity_score"] = round(s["popularity_score"], 3)
            rec["collab_score"] = round(s["collab_score"], 3)
            recommendations.append(rec)

        # Fix SQL injection: use parameterized query for conditions
        custom_sql = None
        if body.conditions:
            # Build parameterized WHERE clause
            placeholders = [f"${i+1}" for i in range(len(body.conditions))]
            where_clause = "co.condition_source_value IN (" + ", ".join(placeholders) + ")"
            custom_sql = {
                "query": (
                    f"SELECT p.person_id, p.gender_source_value, p.year_of_birth, "
                    f"co.condition_source_value, co.condition_start_date "
                    f"FROM person p "
                    f"JOIN condition_occurrence co ON p.person_id = co.person_id "
                    f"WHERE {where_clause}"
                ),
                "params": body.conditions,
                "note": "파라미터화된 쿼리 - SQL injection 방지"
            }

        return {
            "focus_area": body.focus_area,
            "recommendations": recommendations,
            "total": len(recommendations),
            "custom_sql": custom_sql,
            "algorithm": "popularity(0.4) + collaborative_filtering(0.4) + category(0.2)",
        }
    finally:
        await _rel(conn)


@router.post("/templates/{template_id}/preview")
async def preview_template(template_id: int):
    """템플릿 SQL 미리보기 실행 (LIMIT 100)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        tpl = await conn.fetchrow(
            "SELECT sql_definition FROM mart_template WHERE template_id = $1",
            template_id,
        )
        if not tpl:
            raise HTTPException(status_code=404, detail=f"템플릿 ID {template_id}를 찾을 수 없습니다")

        sql_def = tpl["sql_definition"].rstrip().rstrip(";")
        preview_sql = f"SELECT * FROM ({sql_def}) _t LIMIT 100"

        # Estimate total (pg_class for single-table, or EXPLAIN for complex)
        total_estimate = None
        try:
            explain_rows = await conn.fetch(f"EXPLAIN {sql_def}")
            for r in explain_rows:
                line = r[0] if r else ""
                if "rows=" in line:
                    # Extract last rows= value from top plan node
                    import re
                    match = re.search(r"rows=(\d+)", line)
                    if match:
                        total_estimate = int(match.group(1))
                    break
        except Exception:
            pass

        rows = await conn.fetch(preview_sql)
        # Log usage
        await conn.execute(
            "INSERT INTO mart_usage_log (template_id, action) VALUES ($1, 'preview')",
            template_id)

        if not rows:
            return {
                "template_id": template_id,
                "columns": [],
                "rows": [],
                "row_count": 0,
                "total_estimate": total_estimate,
            }

        columns = list(rows[0].keys())
        data = [{col: _serialize(row[col]) for col in columns} for row in rows]

        return {
            "template_id": template_id,
            "columns": columns,
            "rows": data,
            "row_count": len(data),
            "total_estimate": total_estimate,
        }
    finally:
        await _rel(conn)


# ── Usage stats endpoint ─────────────────────────────────────────────

@router.get("/usage-stats")
async def usage_stats(days: int = Query(30, ge=1, le=365)):
    """마트 사용 통계"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        stats = await conn.fetch("""
            SELECT mt.template_id, mt.name, mt.category,
                   COUNT(mul.log_id) AS usage_count,
                   COUNT(DISTINCT mul.user_id) AS unique_users
            FROM mart_template mt
            LEFT JOIN mart_usage_log mul ON mt.template_id = mul.template_id
                AND mul.created_at > NOW() - ($1 || ' days')::interval
            GROUP BY mt.template_id, mt.name, mt.category
            ORDER BY usage_count DESC
        """, str(days))
        return {
            "period_days": days,
            "templates": [dict(r) for r in stats],
            "total_templates": len(stats),
        }
    finally:
        await _rel(conn)


# ── SLA endpoints ────────────────────────────────────────────────────

@router.get("/sla")
async def list_sla(status: Optional[str] = Query(None, pattern=r"^(active|paused|disabled)$")):
    """SLA 설정 목록 조회"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        if status:
            rows = await conn.fetch(
                "SELECT * FROM mart_sla WHERE status = $1 ORDER BY sla_id", status
            )
        else:
            rows = await conn.fetch("SELECT * FROM mart_sla ORDER BY sla_id")
        return {
            "sla_configs": [
                {k: _serialize(v) for k, v in dict(r).items()} for r in rows
            ],
            "total": len(rows),
        }
    finally:
        await _rel(conn)


@router.post("/sla")
async def create_sla(body: SLACreate):
    """마트 SLA 설정 생성"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        now = datetime.now(timezone.utc)
        next_refresh = datetime.fromtimestamp(
            now.timestamp() + body.refresh_interval_min * 60, tz=timezone.utc
        )
        row = await conn.fetchrow(
            "INSERT INTO mart_sla (mart_name, refresh_interval_min, max_stale_min, "
            "last_refreshed_at, next_refresh_at, alert_threshold_sec) "
            "VALUES ($1, $2, $3, $4, $5, $6) RETURNING sla_id, created_at",
            body.mart_name,
            body.refresh_interval_min,
            body.max_stale_min,
            now,
            next_refresh,
            body.alert_threshold_sec,
        )
        # Log creation event
        await conn.execute(
            "INSERT INTO mart_sla_log (sla_id, event_type, status) VALUES ($1, 'created', 'ok')",
            row["sla_id"],
        )
        return {
            "sla_id": row["sla_id"],
            "next_refresh_at": next_refresh.isoformat(),
            "created_at": row["created_at"].isoformat(),
        }
    finally:
        await _rel(conn)


@router.put("/sla/{sla_id}")
async def update_sla(sla_id: int, body: SLAUpdate):
    """SLA 설정 수정"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        existing = await conn.fetchrow(
            "SELECT * FROM mart_sla WHERE sla_id = $1", sla_id
        )
        if not existing:
            raise HTTPException(status_code=404, detail=f"SLA ID {sla_id}를 찾을 수 없습니다")

        updates, params = [], []
        idx = 1
        if body.mart_name is not None:
            params.append(body.mart_name)
            updates.append(f"mart_name = ${idx}")
            idx += 1
        if body.refresh_interval_min is not None:
            params.append(body.refresh_interval_min)
            updates.append(f"refresh_interval_min = ${idx}")
            idx += 1
        if body.max_stale_min is not None:
            params.append(body.max_stale_min)
            updates.append(f"max_stale_min = ${idx}")
            idx += 1
        if body.status is not None:
            params.append(body.status)
            updates.append(f"status = ${idx}")
            idx += 1
        if body.alert_threshold_sec is not None:
            params.append(body.alert_threshold_sec)
            updates.append(f"alert_threshold_sec = ${idx}")
            idx += 1

        if not updates:
            raise HTTPException(status_code=400, detail="수정할 필드가 없습니다")

        params.append(sla_id)
        sql = f"UPDATE mart_sla SET {', '.join(updates)} WHERE sla_id = ${idx}"
        await conn.execute(sql, *params)

        # Log update event
        await conn.execute(
            "INSERT INTO mart_sla_log (sla_id, event_type, status) VALUES ($1, 'updated', 'ok')",
            sla_id,
        )

        updated = await conn.fetchrow(
            "SELECT * FROM mart_sla WHERE sla_id = $1", sla_id
        )
        return {k: _serialize(v) for k, v in dict(updated).items()}
    finally:
        await _rel(conn)


@router.get("/sla/{sla_id}/history")
async def sla_history(
    sla_id: int,
    limit: int = Query(default=50, ge=1, le=500),
):
    """SLA 실행 이력 조회"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        # SLA 존재 확인
        exists = await conn.fetchval(
            "SELECT COUNT(*) FROM mart_sla WHERE sla_id = $1", sla_id
        )
        if not exists:
            raise HTTPException(status_code=404, detail=f"SLA ID {sla_id}를 찾을 수 없습니다")

        rows = await conn.fetch(
            "SELECT * FROM mart_sla_log WHERE sla_id = $1 ORDER BY created_at DESC LIMIT $2",
            sla_id, limit,
        )
        return {
            "sla_id": sla_id,
            "logs": [
                {k: _serialize(v) for k, v in dict(r).items()} for r in rows
            ],
            "total": len(rows),
        }
    finally:
        await _rel(conn)


@router.post("/sla/{sla_id}/check")
async def check_sla(sla_id: int):
    """SLA 상태 점검 — stale 여부 + 마지막 갱신 시각 확인"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        sla = await conn.fetchrow(
            "SELECT * FROM mart_sla WHERE sla_id = $1", sla_id
        )
        if not sla:
            raise HTTPException(status_code=404, detail=f"SLA ID {sla_id}를 찾을 수 없습니다")

        now = datetime.now(timezone.utc)
        last_refreshed = sla["last_refreshed_at"]
        max_stale_min = sla["max_stale_min"]

        minutes_since_refresh = None
        is_stale = False
        if last_refreshed:
            # Ensure last_refreshed is timezone-aware
            if last_refreshed.tzinfo is None:
                last_refreshed = last_refreshed.replace(tzinfo=timezone.utc)
            delta = now - last_refreshed
            minutes_since_refresh = round(delta.total_seconds() / 60, 1)
            is_stale = minutes_since_refresh > max_stale_min
        else:
            is_stale = True

        check_status = "stale" if is_stale else "fresh"

        # Log check event
        await conn.execute(
            "INSERT INTO mart_sla_log (sla_id, event_type, duration_sec, status) "
            "VALUES ($1, 'check', $2, $3)",
            sla_id,
            minutes_since_refresh * 60 if minutes_since_refresh else 0,
            check_status,
        )

        return {
            "sla_id": sla_id,
            "mart_name": sla["mart_name"],
            "status": sla["status"],
            "is_stale": is_stale,
            "minutes_since_refresh": minutes_since_refresh,
            "next_refresh_at": _serialize(sla["next_refresh_at"]),
        }
    finally:
        await _rel(conn)


@router.get("/sla/dashboard")
async def sla_dashboard():
    """SLA 대시보드 요약 — 전체 현황 + 최근 로그"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)

        total = await conn.fetchval("SELECT COUNT(*) FROM mart_sla")
        active = await conn.fetchval(
            "SELECT COUNT(*) FROM mart_sla WHERE status = 'active'"
        )
        avg_interval = await conn.fetchval(
            "SELECT COALESCE(AVG(refresh_interval_min), 0) FROM mart_sla"
        )

        # Stale count: active SLAs whose last_refreshed_at + max_stale_min < NOW
        stale_count = await conn.fetchval("""
            SELECT COUNT(*) FROM mart_sla
            WHERE status = 'active'
              AND (
                  last_refreshed_at IS NULL
                  OR last_refreshed_at + (max_stale_min || ' minutes')::interval < NOW()
              )
        """)

        recent_logs = await conn.fetch(
            "SELECT * FROM mart_sla_log ORDER BY created_at DESC LIMIT 20"
        )

        return {
            "total_slas": total,
            "active": active,
            "stale_count": stale_count,
            "avg_refresh_interval_min": round(float(avg_interval), 1),
            "recent_logs": [
                {k: _serialize(v) for k, v in dict(r).items()} for r in recent_logs
            ],
        }
    finally:
        await _rel(conn)
