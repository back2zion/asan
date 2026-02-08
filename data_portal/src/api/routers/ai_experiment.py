"""
AAR-003: AI 실험 + A/B 테스트 + 피드백
실험 관리, 변형 트래픽 분할, 사용자 피드백 수집
"""
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/experiments", tags=["AIExperiment"])

async def _get_conn():
    from services.db_pool import get_pool
    return await (await get_pool()).acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    await (await get_pool()).release(conn)

_tbl_ok = False

async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_experiment (
            experiment_id SERIAL PRIMARY KEY,
            name VARCHAR(200) NOT NULL,
            description TEXT,
            status VARCHAR(20) DEFAULT 'draft',
            variants JSONB DEFAULT '[]',
            traffic_split JSONB DEFAULT '{}',
            start_date TIMESTAMPTZ,
            end_date TIMESTAMPTZ,
            created_by VARCHAR(50) DEFAULT 'system',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS ai_experiment_variant (
            variant_id SERIAL PRIMARY KEY,
            experiment_id INTEGER REFERENCES ai_experiment(experiment_id),
            variant_name VARCHAR(100) NOT NULL,
            model_id VARCHAR(50),
            config JSONB DEFAULT '{}',
            request_count BIGINT DEFAULT 0,
            avg_latency_ms DOUBLE PRECISION DEFAULT 0,
            avg_rating DOUBLE PRECISION DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS ai_feedback (
            feedback_id SERIAL PRIMARY KEY,
            experiment_id INTEGER,
            variant_name VARCHAR(100),
            response_id VARCHAR(100),
            rating INTEGER CHECK (rating >= 1 AND rating <= 5),
            category VARCHAR(50),
            comment TEXT,
            user_id VARCHAR(50),
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_ai_exp_status ON ai_experiment(status);
        CREATE INDEX IF NOT EXISTS idx_ai_feedback_exp ON ai_feedback(experiment_id);
        CREATE INDEX IF NOT EXISTS idx_ai_feedback_rating ON ai_feedback(rating);
    """)
    _tbl_ok = True


class ExperimentCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=2000)
    variants: List[Dict[str, Any]] = Field(default_factory=lambda: [{"name": "control"}, {"name": "treatment"}])
    traffic_split: Optional[Dict[str, float]] = None

class StatusUpdate(BaseModel):
    status: str = Field(..., pattern=r"^(draft|running|completed|archived)$")

class FeedbackCreate(BaseModel):
    experiment_id: Optional[int] = None
    variant_name: Optional[str] = None
    response_id: Optional[str] = Field(None, max_length=100)
    rating: int = Field(..., ge=1, le=5)
    category: Optional[str] = Field(None, pattern=r"^(accuracy|relevance|speed|safety|overall)$")
    comment: Optional[str] = Field(None, max_length=2000)
    user_id: Optional[str] = Field(None, max_length=50)


@router.post("/")
async def create_experiment(body: ExperimentCreate):
    """실험 생성"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        # 기본 트래픽 분할
        if not body.traffic_split and body.variants:
            n = len(body.variants)
            split = {v.get("name", f"v{i}"): round(1.0/n, 2) for i, v in enumerate(body.variants)}
        else:
            split = body.traffic_split or {}

        exp_id = await conn.fetchval("""
            INSERT INTO ai_experiment (name, description, variants, traffic_split)
            VALUES ($1,$2,$3::jsonb,$4::jsonb) RETURNING experiment_id
        """, body.name, body.description, json.dumps(body.variants), json.dumps(split))

        # 변형 레코드 생성
        for v in body.variants:
            vname = v.get("name", "default")
            await conn.execute("""
                INSERT INTO ai_experiment_variant (experiment_id, variant_name, model_id, config)
                VALUES ($1,$2,$3,$4::jsonb)
            """, exp_id, vname, v.get("model_id"), json.dumps(v.get("config", {})))

        return {"experiment_id": exp_id, "name": body.name, "variants": len(body.variants), "traffic_split": split}
    finally:
        await _rel(conn)


@router.get("/")
async def list_experiments(status: Optional[str] = None, limit: int = Query(20, le=100)):
    """실험 목록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT * FROM ai_experiment"
        params, idx = [], 1
        if status:
            q += f" WHERE status = ${idx}"; params.append(status); idx += 1
        q += f" ORDER BY created_at DESC LIMIT ${idx}"
        params.append(limit)
        rows = await conn.fetch(q, *params)
        return {"experiments": [dict(r) for r in rows], "total": len(rows)}
    finally:
        await _rel(conn)


@router.get("/{experiment_id}")
async def get_experiment(experiment_id: int):
    """실험 상세 + 결과 통계"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        exp = await conn.fetchrow("SELECT * FROM ai_experiment WHERE experiment_id=$1", experiment_id)
        if not exp:
            raise HTTPException(404, "실험을 찾을 수 없습니다")

        variants = await conn.fetch(
            "SELECT * FROM ai_experiment_variant WHERE experiment_id=$1", experiment_id)
        feedback_stats = await conn.fetch("""
            SELECT variant_name, COUNT(*) as count, AVG(rating) as avg_rating,
                   COUNT(CASE WHEN rating >= 4 THEN 1 END) as positive_count
            FROM ai_feedback WHERE experiment_id=$1
            GROUP BY variant_name
        """, experiment_id)

        return {
            **dict(exp),
            "variant_details": [dict(v) for v in variants],
            "feedback_stats": [dict(f) for f in feedback_stats],
        }
    finally:
        await _rel(conn)


@router.put("/{experiment_id}/status")
async def update_status(experiment_id: int, body: StatusUpdate):
    """실험 상태 변경"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        exp = await conn.fetchrow("SELECT * FROM ai_experiment WHERE experiment_id=$1", experiment_id)
        if not exp:
            raise HTTPException(404, "실험을 찾을 수 없습니다")

        update_fields = {"status": body.status, "updated_at": datetime.now()}
        if body.status == "running" and not exp["start_date"]:
            update_fields["start_date"] = datetime.now()
        elif body.status in ("completed", "archived") and not exp["end_date"]:
            update_fields["end_date"] = datetime.now()

        await conn.execute("""
            UPDATE ai_experiment SET status=$1, updated_at=NOW(),
                start_date=COALESCE(start_date, CASE WHEN $1='running' THEN NOW() ELSE NULL END),
                end_date=CASE WHEN $1 IN ('completed','archived') THEN COALESCE(end_date, NOW()) ELSE end_date END
            WHERE experiment_id=$2
        """, body.status, experiment_id)

        return {"experiment_id": experiment_id, "status": body.status}
    finally:
        await _rel(conn)


@router.post("/feedback")
async def submit_feedback(body: FeedbackCreate):
    """사용자 피드백 제출"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        fid = await conn.fetchval("""
            INSERT INTO ai_feedback (experiment_id, variant_name, response_id, rating, category, comment, user_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING feedback_id
        """, body.experiment_id, body.variant_name, body.response_id,
           body.rating, body.category, body.comment, body.user_id)

        # 변형 통계 업데이트
        if body.experiment_id and body.variant_name:
            await conn.execute("""
                UPDATE ai_experiment_variant
                SET avg_rating = (SELECT AVG(rating) FROM ai_feedback WHERE experiment_id=$1 AND variant_name=$2),
                    request_count = request_count + 1
                WHERE experiment_id=$1 AND variant_name=$2
            """, body.experiment_id, body.variant_name)

        return {"feedback_id": fid, "rating": body.rating}
    finally:
        await _rel(conn)


@router.get("/feedback/stats")
async def feedback_stats(experiment_id: Optional[int] = None):
    """피드백 통계"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        where = ""
        params = []
        if experiment_id:
            where = " WHERE experiment_id = $1"
            params = [experiment_id]

        total = await conn.fetchval(f"SELECT COUNT(*) FROM ai_feedback{where}", *params) or 0
        avg_rating = await conn.fetchval(f"SELECT AVG(rating) FROM ai_feedback{where}", *params)

        by_category = await conn.fetch(f"""
            SELECT category, COUNT(*) as count, AVG(rating) as avg_rating
            FROM ai_feedback{where}
            GROUP BY category ORDER BY count DESC
        """, *params)

        by_rating = await conn.fetch(f"""
            SELECT rating, COUNT(*) as count
            FROM ai_feedback{where}
            GROUP BY rating ORDER BY rating
        """, *params)

        return {
            "total_feedback": total,
            "avg_rating": round(avg_rating, 2) if avg_rating else 0,
            "by_category": [dict(r) for r in by_category],
            "by_rating": [dict(r) for r in by_rating],
            "satisfaction_rate": round(
                sum(1 for r in by_rating if r["rating"] >= 4) / total * 100, 1
            ) if total > 0 else 0,
        }
    finally:
        await _rel(conn)


@router.post("/{experiment_id}/analyze")
async def analyze_experiment(experiment_id: int):
    """실험 결과 분석"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        exp = await conn.fetchrow("SELECT * FROM ai_experiment WHERE experiment_id=$1", experiment_id)
        if not exp:
            raise HTTPException(404, "실험을 찾을 수 없습니다")

        variants = await conn.fetch(
            "SELECT * FROM ai_experiment_variant WHERE experiment_id=$1", experiment_id)
        feedback = await conn.fetch(
            "SELECT * FROM ai_feedback WHERE experiment_id=$1", experiment_id)

        analysis = []
        for v in variants:
            vf = [f for f in feedback if f["variant_name"] == v["variant_name"]]
            ratings = [f["rating"] for f in vf]
            analysis.append({
                "variant_name": v["variant_name"],
                "total_feedback": len(vf),
                "avg_rating": round(sum(ratings) / len(ratings), 2) if ratings else 0,
                "min_rating": min(ratings) if ratings else 0,
                "max_rating": max(ratings) if ratings else 0,
                "positive_rate": round(sum(1 for r in ratings if r >= 4) / len(ratings) * 100, 1) if ratings else 0,
            })

        # 승자 결정
        winner = max(analysis, key=lambda x: x["avg_rating"]) if analysis else None

        return {
            "experiment_id": experiment_id,
            "experiment_name": exp["name"],
            "status": exp["status"],
            "variant_analysis": analysis,
            "winner": winner["variant_name"] if winner and winner["avg_rating"] > 0 else None,
            "total_feedback": len(feedback),
        }
    finally:
        await _rel(conn)
