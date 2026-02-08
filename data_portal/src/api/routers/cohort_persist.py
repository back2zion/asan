"""
코호트 저장/로드/내보내기/비교
코호트 정의 영속화 + CSV 내보내기 + 비교 분석
"""
import csv
import io
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

router = APIRouter(prefix="/persist", tags=["CohortPersist"])

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
        CREATE TABLE IF NOT EXISTS cohort_definition (
            cohort_id SERIAL PRIMARY KEY,
            cohort_name VARCHAR(200) NOT NULL,
            description TEXT,
            criteria JSONB DEFAULT '{}',
            patient_count INTEGER DEFAULT 0,
            created_by VARCHAR(50) DEFAULT 'system',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS cohort_patient (
            id SERIAL PRIMARY KEY,
            cohort_id INTEGER REFERENCES cohort_definition(cohort_id) ON DELETE CASCADE,
            person_id BIGINT NOT NULL,
            added_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(cohort_id, person_id)
        );
        CREATE INDEX IF NOT EXISTS idx_cohort_def_name ON cohort_definition(cohort_name);
        CREATE INDEX IF NOT EXISTS idx_cohort_pat_cohort ON cohort_patient(cohort_id);
        CREATE INDEX IF NOT EXISTS idx_cohort_pat_person ON cohort_patient(person_id);
    """)
    _tbl_ok = True


class CohortSave(BaseModel):
    cohort_name: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=2000)
    criteria: Dict[str, Any] = Field(default_factory=dict)
    person_ids: Optional[List[int]] = None


@router.post("/save")
async def save_cohort(body: CohortSave):
    """코호트 정의 저장"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)

        # 코호트 정의 저장
        cohort_id = await conn.fetchval("""
            INSERT INTO cohort_definition (cohort_name, description, criteria)
            VALUES ($1,$2,$3::jsonb) RETURNING cohort_id
        """, body.cohort_name, body.description, json.dumps(body.criteria))

        # person_ids가 제공되면 코호트 환자 저장
        patient_count = 0
        if body.person_ids:
            for pid in body.person_ids[:100000]:  # 최대 10만명
                try:
                    await conn.execute("""
                        INSERT INTO cohort_patient (cohort_id, person_id) VALUES ($1,$2)
                        ON CONFLICT DO NOTHING
                    """, cohort_id, pid)
                    patient_count += 1
                except Exception:
                    pass
        elif body.criteria:
            # criteria 기반 자동 코호트 생성
            patient_count = await _build_cohort_from_criteria(conn, cohort_id, body.criteria)

        await conn.execute(
            "UPDATE cohort_definition SET patient_count=$1 WHERE cohort_id=$2",
            patient_count, cohort_id)

        return {"cohort_id": cohort_id, "cohort_name": body.cohort_name, "patient_count": patient_count}
    finally:
        await _rel(conn)


async def _build_cohort_from_criteria(conn, cohort_id: int, criteria: dict) -> int:
    """criteria JSON으로 코호트 빌드"""
    conditions = []
    params = []
    idx = 1

    if "condition_code" in criteria:
        conditions.append(f"""
            person_id IN (
                SELECT DISTINCT person_id FROM condition_occurrence
                WHERE condition_source_value = ${idx}
            )
        """)
        params.append(str(criteria["condition_code"]))
        idx += 1

    if "gender" in criteria:
        conditions.append(f"gender_source_value = ${idx}")
        params.append(criteria["gender"])
        idx += 1

    if "min_age" in criteria:
        conditions.append(f"year_of_birth <= EXTRACT(YEAR FROM NOW()) - ${idx}")
        params.append(criteria["min_age"])
        idx += 1

    if "max_age" in criteria:
        conditions.append(f"year_of_birth >= EXTRACT(YEAR FROM NOW()) - ${idx}")
        params.append(criteria["max_age"])
        idx += 1

    if not conditions:
        return 0

    where = " AND ".join(conditions)
    sql = f"SELECT person_id FROM person WHERE {where} LIMIT 100000"
    rows = await conn.fetch(sql, *params)

    for r in rows:
        await conn.execute("""
            INSERT INTO cohort_patient (cohort_id, person_id) VALUES ($1,$2)
            ON CONFLICT DO NOTHING
        """, cohort_id, r["person_id"])

    return len(rows)


@router.get("/saved")
async def list_saved_cohorts(limit: int = Query(50, le=200)):
    """저장된 코호트 목록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("""
            SELECT cohort_id, cohort_name, description, patient_count, created_by, created_at
            FROM cohort_definition ORDER BY created_at DESC LIMIT $1
        """, limit)
        return {"cohorts": [dict(r) for r in rows], "total": len(rows)}
    finally:
        await _rel(conn)


@router.get("/saved/{cohort_id}")
async def get_saved_cohort(cohort_id: int):
    """저장된 코호트 로드"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        cohort = await conn.fetchrow("SELECT * FROM cohort_definition WHERE cohort_id=$1", cohort_id)
        if not cohort:
            raise HTTPException(404, "코호트를 찾을 수 없습니다")

        patients = await conn.fetch("""
            SELECT cp.person_id, p.gender_source_value, p.year_of_birth
            FROM cohort_patient cp
            JOIN person p ON cp.person_id = p.person_id
            WHERE cp.cohort_id = $1
            LIMIT 1000
        """, cohort_id)

        return {
            **dict(cohort),
            "patients_sample": [dict(r) for r in patients],
            "sample_size": len(patients),
        }
    finally:
        await _rel(conn)


@router.delete("/saved/{cohort_id}")
async def delete_cohort(cohort_id: int):
    """코호트 삭제"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        existing = await conn.fetchval("SELECT cohort_id FROM cohort_definition WHERE cohort_id=$1", cohort_id)
        if not existing:
            raise HTTPException(404, "코호트를 찾을 수 없습니다")
        await conn.execute("DELETE FROM cohort_patient WHERE cohort_id=$1", cohort_id)
        await conn.execute("DELETE FROM cohort_definition WHERE cohort_id=$1", cohort_id)
        return {"message": f"코호트 {cohort_id} 삭제됨"}
    finally:
        await _rel(conn)


@router.post("/saved/{cohort_id}/export")
async def export_cohort(cohort_id: int):
    """코호트 CSV 내보내기 (person_id + demographics)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        cohort = await conn.fetchrow("SELECT * FROM cohort_definition WHERE cohort_id=$1", cohort_id)
        if not cohort:
            raise HTTPException(404, "코호트를 찾을 수 없습니다")

        rows = await conn.fetch("""
            SELECT cp.person_id, p.gender_source_value, p.year_of_birth, p.month_of_birth, p.day_of_birth,
                   p.race_source_value, p.ethnicity_source_value
            FROM cohort_patient cp
            JOIN person p ON cp.person_id = p.person_id
            WHERE cp.cohort_id = $1
            ORDER BY cp.person_id
        """, cohort_id)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["person_id", "gender", "year_of_birth", "month_of_birth", "day_of_birth", "race", "ethnicity"])
        for r in rows:
            writer.writerow([r["person_id"], r["gender_source_value"], r["year_of_birth"],
                           r["month_of_birth"], r["day_of_birth"], r["race_source_value"], r["ethnicity_source_value"]])

        return Response(
            content=output.getvalue().encode("utf-8-sig"),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=cohort_{cohort_id}_{datetime.now().strftime('%Y%m%d')}.csv"},
        )
    finally:
        await _rel(conn)


@router.post("/saved/{cohort_id}/compare")
async def compare_cohorts(cohort_id: int, other_cohort_id: int = Query(...)):
    """두 코호트 비교 (겹침, 차이)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        c1 = await conn.fetchrow("SELECT * FROM cohort_definition WHERE cohort_id=$1", cohort_id)
        c2 = await conn.fetchrow("SELECT * FROM cohort_definition WHERE cohort_id=$1", other_cohort_id)
        if not c1 or not c2:
            raise HTTPException(404, "코호트를 찾을 수 없습니다")

        # 환자 집합
        p1 = await conn.fetch("SELECT person_id FROM cohort_patient WHERE cohort_id=$1", cohort_id)
        p2 = await conn.fetch("SELECT person_id FROM cohort_patient WHERE cohort_id=$1", other_cohort_id)
        set1 = {r["person_id"] for r in p1}
        set2 = {r["person_id"] for r in p2}

        overlap = set1 & set2
        only_a = set1 - set2
        only_b = set2 - set1
        union_size = len(set1 | set2)
        jaccard = round(len(overlap) / union_size, 4) if union_size > 0 else 0

        # 인구통계 비교
        async def _demographics(person_ids):
            if not person_ids:
                return {"male": 0, "female": 0, "avg_birth_year": 0}
            pids = list(person_ids)[:100000]
            males = await conn.fetchval(
                f"SELECT COUNT(*) FROM person WHERE person_id = ANY($1::bigint[]) AND gender_source_value='M'", pids) or 0
            avg_year = await conn.fetchval(
                f"SELECT AVG(year_of_birth) FROM person WHERE person_id = ANY($1::bigint[])", pids)
            return {"male": males, "female": len(pids) - males, "avg_birth_year": round(avg_year) if avg_year else 0}

        demo_a = await _demographics(set1)
        demo_b = await _demographics(set2)

        return {
            "cohort_a": {"id": cohort_id, "name": c1["cohort_name"], "size": len(set1), "demographics": demo_a},
            "cohort_b": {"id": other_cohort_id, "name": c2["cohort_name"], "size": len(set2), "demographics": demo_b},
            "overlap": {"count": len(overlap), "percent_of_a": round(len(overlap)/len(set1)*100, 1) if set1 else 0,
                        "percent_of_b": round(len(overlap)/len(set2)*100, 1) if set2 else 0},
            "only_in_a": len(only_a),
            "only_in_b": len(only_b),
            "jaccard_similarity": jaccard,
        }
    finally:
        await _rel(conn)
