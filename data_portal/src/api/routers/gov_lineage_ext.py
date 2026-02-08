"""
거버넌스 확장 - 리니지 시각화, 영향 분석, 동적 비식별화 미들웨어
Task #23: 라. 거버넌스 72->90%
"""
import hashlib, random, re
from datetime import date as _date, datetime as _dt
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(prefix="/gov-ext", tags=["GovernanceExt"])

# ── DB helpers (services.db_pool 패턴) ──
async def _get_conn():
    from services.db_pool import get_pool
    return await (await get_pool()).acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    await (await get_pool()).release(conn)

# ── OMOP CDM 리니지 상수 ──
TABLE_CATEGORY: Dict[str, str] = {
    "person": "patient", "death": "patient", "observation_period": "patient",
    "visit_occurrence": "admin", "visit_detail": "admin", "care_site": "admin",
    "provider": "admin", "location": "admin", "cost": "admin", "payer_plan_period": "admin",
    "condition_occurrence": "clinical", "condition_era": "clinical",
    "drug_exposure": "clinical", "drug_era": "clinical",
    "procedure_occurrence": "clinical", "measurement": "clinical",
    "observation": "clinical", "device_exposure": "clinical",
    "note": "clinical", "note_nlp": "clinical", "specimen": "clinical",
}

OMOP_EDGES = [
    ("person", "condition_occurrence", "person_id"),
    ("person", "drug_exposure", "person_id"),
    ("person", "measurement", "person_id"),
    ("person", "visit_occurrence", "person_id"),
    ("person", "procedure_occurrence", "person_id"),
    ("person", "observation", "person_id"),
    ("person", "death", "person_id"),
    ("person", "observation_period", "person_id"),
    ("person", "device_exposure", "person_id"),
    ("person", "drug_era", "person_id"),
    ("person", "condition_era", "person_id"),
    ("person", "payer_plan_period", "person_id"),
    ("person", "note", "person_id"),
    ("person", "cost", "person_id"),
    ("visit_occurrence", "condition_occurrence", "visit_occurrence_id"),
    ("visit_occurrence", "drug_exposure", "visit_occurrence_id"),
    ("visit_occurrence", "measurement", "visit_occurrence_id"),
    ("visit_occurrence", "procedure_occurrence", "visit_occurrence_id"),
    ("visit_occurrence", "observation", "visit_occurrence_id"),
    ("visit_occurrence", "device_exposure", "visit_occurrence_id"),
    ("visit_occurrence", "note", "visit_occurrence_id"),
    ("visit_occurrence", "cost", "visit_occurrence_id"),
]

SOURCE_VALUE_COLUMNS: Dict[str, List[str]] = {
    "condition_occurrence": ["condition_source_value", "condition_status_source_value"],
    "drug_exposure": ["drug_source_value", "route_source_value"],
    "procedure_occurrence": ["procedure_source_value", "modifier_source_value"],
    "measurement": ["measurement_source_value", "unit_source_value", "value_source_value"],
    "observation": ["observation_source_value", "unit_source_value", "qualifier_source_value"],
    "person": ["person_source_value", "gender_source_value", "race_source_value", "ethnicity_source_value"],
    "visit_occurrence": ["visit_source_value", "admitting_source_value", "discharge_to_source_value"],
}

# ── Pydantic 모델 ──
class DeidentRule(BaseModel):
    column: str
    method: str = Field(..., pattern=r"^(mask|hash|generalize|suppress|noise)$")

class DeidentApplyRequest(BaseModel):
    sql: str
    rules: List[DeidentRule]

class ColumnTraceRequest(BaseModel):
    table_name: str
    column_name: str

# ── 비식별화 함수 ──
_FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|COPY|EXECUTE)\b", re.I)

def _apply_mask(v: Any) -> Any:
    if v is None: return None
    s = str(v)
    return s[0] + "***" if len(s) > 1 else "***"

def _apply_hash(v: Any) -> Optional[str]:
    return hashlib.sha256(str(v).encode()).hexdigest() if v is not None else None

def _apply_generalize(v: Any) -> Any:
    if v is None: return None
    if isinstance(v, (_date, _dt)): return v.year
    if isinstance(v, (int, float)):
        return (int(v) // 10) * 10 if 0 < abs(v) < 200 else round(v, -1)
    s = str(v)
    return s[:4] if re.match(r"^\d{4}-\d{2}-\d{2}", s) else v

def _apply_noise(v: Any) -> Any:
    if v is None or not isinstance(v, (int, float)): return v
    r = v * (1 + random.uniform(-0.05, 0.05))
    return round(r, 2) if isinstance(v, float) else int(round(r))

DEIDENT_FN = {
    "mask": _apply_mask, "hash": _apply_hash, "generalize": _apply_generalize,
    "suppress": lambda v: None, "noise": _apply_noise,
}

def _serialize(val: Any) -> Any:
    """datetime/date -> isoformat string"""
    return val.isoformat() if isinstance(val, (_date, _dt)) else val

# ═══════ 1. GET /lineage/graph ═══════

@router.get("/lineage/graph")
async def lineage_graph():
    """OMOP CDM 테이블 리니지 그래프 (노드 + 엣지)"""
    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            "SELECT relname AS t, n_live_tup AS c FROM pg_stat_user_tables WHERE schemaname='public'")
        cmap = {r["t"]: r["c"] for r in rows}
        nodes, seen = [], set()
        for tbl, cat in TABLE_CATEGORY.items():
            seen.add(tbl)
            nodes.append({"id": tbl, "label": tbl, "row_count": cmap.get(tbl, 0), "category": cat})
        for tbl, rc in cmap.items():
            if tbl not in seen:
                nodes.append({"id": tbl, "label": tbl, "row_count": rc, "category": "admin"})
        edges = [{"source": s, "target": t, "relationship": r} for s, t, r in OMOP_EDGES]
        return {"nodes": nodes, "edges": edges, "total_tables": len(nodes), "total_edges": len(edges)}
    finally:
        await _rel(conn)

# ═══════ 2. GET /lineage/impact/{table_name} ═══════

@router.get("/lineage/impact/{table_name}")
async def lineage_impact(table_name: str):
    """테이블 영향 분석 - 상류/하류 의존성"""
    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            "SELECT relname AS t, n_live_tup AS c FROM pg_stat_user_tables WHERE schemaname='public'")
        cmap = {r["t"]: r["c"] for r in rows}
        tl = table_name.lower()
        if tl not in cmap and tl not in TABLE_CATEGORY:
            raise HTTPException(404, f"테이블을 찾을 수 없습니다: {table_name}")
        upstream, downstream = [], []
        for s, t, r in OMOP_EDGES:
            if t == tl:
                upstream.append({"table": s, "relationship": r, "row_count": cmap.get(s, 0)})
            if s == tl:
                downstream.append({"table": t, "relationship": r, "row_count": cmap.get(t, 0)})
        affected = {d["table"] for d in downstream} | {u["table"] for u in upstream}
        return {"table_name": tl, "upstream": upstream, "downstream": downstream,
                "total_impacted_rows": sum(cmap.get(t, 0) for t in affected)}
    finally:
        await _rel(conn)

# ═══════ 3. POST /deident/apply ═══════

@router.post("/deident/apply")
async def deident_apply(body: DeidentApplyRequest):
    """SQL 실행 후 동적 비식별화 적용"""
    sql = body.sql.strip().rstrip(";")
    if not sql.upper().startswith("SELECT"):
        raise HTTPException(400, "SELECT 문만 허용됩니다")
    if _FORBIDDEN_SQL.search(sql):
        raise HTTPException(400, "허용되지 않는 SQL 명령어가 포함되어 있습니다")
    rule_map = {r.column.lower(): r.method for r in body.rules}
    conn = await _get_conn()
    try:
        rows = await conn.fetch(sql)
        if not rows:
            return {"columns": [], "rows": [], "row_count": 0, "applied_rules": rule_map}
        columns = list(rows[0].keys())
        result = []
        for row in rows:
            nr: Dict[str, Any] = {}
            for c in columns:
                v = row[c]
                m = rule_map.get(c.lower())
                if m and m in DEIDENT_FN:
                    v = DEIDENT_FN[m](v)
                nr[c] = _serialize(v)
            result.append(nr)
        return {"columns": columns, "rows": result, "row_count": len(result), "applied_rules": rule_map}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"SQL 실행 오류: {e}")
    finally:
        await _rel(conn)

# ═══════ 4. GET /deident/methods ═══════

@router.get("/deident/methods")
async def deident_methods():
    """비식별화 방법 목록 및 설명"""
    return [
        {"method": "mask", "name": "마스킹",
         "description": "첫 글자만 남기고 나머지를 ***로 대체", "example": "홍길동 -> 홍***",
         "applicable_types": ["string", "name", "identifier"]},
        {"method": "hash", "name": "해시",
         "description": "SHA-256 단방향 해시 변환 (복원 불가)", "example": "ABC123 -> a1b2c3...",
         "applicable_types": ["string", "identifier"]},
        {"method": "generalize", "name": "일반화",
         "description": "날짜→연도, 나이→10세 단위, 숫자→10 단위 라운딩", "example": "2024-03-15 -> 2024",
         "applicable_types": ["date", "number", "age"]},
        {"method": "suppress", "name": "삭제",
         "description": "값을 null로 완전 삭제", "example": "any -> null",
         "applicable_types": ["any"]},
        {"method": "noise", "name": "노이즈",
         "description": "숫자에 +-5% 랜덤 노이즈 추가", "example": "100 -> 97~103",
         "applicable_types": ["number", "measurement"]},
    ]

# ═══════ 5. POST /lineage/column-trace ═══════

@router.post("/lineage/column-trace")
async def column_trace(body: ColumnTraceRequest):
    """컬럼 레벨 리니지 추적 - FK 및 source_value 매핑"""
    table, column = body.table_name.lower(), body.column_name.lower()
    related: List[Dict[str, str]] = []

    if column in ("person_id", "visit_occurrence_id"):
        for s, t, r in OMOP_EDGES:
            if r == column:
                if table == s:
                    related.append({"table": t, "column": column, "relationship": "FK (downstream)"})
                elif table == t:
                    related.append({"table": s, "column": column, "relationship": "FK (upstream)"})
    elif column.endswith("_source_value"):
        base = column.replace("_source_value", "")
        related.append({"table": table, "column": f"{base}_concept_id",
                         "relationship": "source_value -> concept_id 매핑"})
        for ot, sv_cols in SOURCE_VALUE_COLUMNS.items():
            if ot != table and column in sv_cols:
                related.append({"table": ot, "column": column, "relationship": "동일 source_value"})
    elif column.endswith("_concept_id"):
        base = column.replace("_concept_id", "")
        related.append({"table": table, "column": f"{base}_source_value",
                         "relationship": "concept_id -> source_value 역매핑"})

    if not related:
        conn = await _get_conn()
        try:
            matches = await conn.fetch("""
                SELECT table_name, column_name FROM information_schema.columns
                WHERE table_schema='public' AND column_name=$1 AND table_name!=$2
                ORDER BY table_name
            """, column, table)
            related = [{"table": m["table_name"], "column": m["column_name"],
                        "relationship": "동일 컬럼명"} for m in matches]
        finally:
            await _rel(conn)

    return {"column": column, "table": table, "related": related}

# ═══════ 6. GET /governance/dashboard ═══════

@router.get("/governance/dashboard")
async def governance_dashboard():
    """거버넌스 확장 대시보드"""
    conn = await _get_conn()
    try:
        total_tables = await conn.fetchval(
            "SELECT COUNT(DISTINCT table_name) FROM information_schema.tables "
            "WHERE table_schema='public' AND table_type='BASE TABLE'") or 0
        total_columns = await conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public'") or 0

        sensitivity_summary: Dict[str, int] = {}
        try:
            for r in await conn.fetch(
                    "SELECT override_level, COUNT(*) AS c FROM sensitivity_override GROUP BY override_level"):
                sensitivity_summary[r["override_level"]] = r["c"]
        except Exception:
            sensitivity_summary = {"극비": 0, "민감": 0, "일반": 0}

        deident_rules_count = 0
        try:
            deident_rules_count = await conn.fetchval("SELECT COUNT(*) FROM deident_rule") or 0
        except Exception:
            pass

        row_stats = await conn.fetch(
            "SELECT relname AS t, n_live_tup AS c FROM pg_stat_user_tables "
            "WHERE schemaname='public' ORDER BY n_live_tup DESC LIMIT 10")
        total_rows = await conn.fetchval(
            "SELECT COALESCE(SUM(n_live_tup),0)::bigint FROM pg_stat_user_tables WHERE schemaname='public'") or 0

        return {
            "total_tables": total_tables, "total_columns": total_columns, "total_rows": total_rows,
            "sensitivity_summary": sensitivity_summary,
            "deident_rules_count": deident_rules_count,
            "lineage_nodes_count": len(TABLE_CATEGORY),
            "lineage_edges_count": len(OMOP_EDGES),
            "top_tables_by_rows": [{"table": r["t"], "row_count": r["c"]} for r in row_stats],
        }
    finally:
        await _rel(conn)
