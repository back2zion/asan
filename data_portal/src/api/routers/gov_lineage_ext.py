"""
거버넌스 확장 - 리니지 시각화, 영향 분석, 동적 비식별화 미들웨어
Task #23: 라. 거버넌스 72->90%
+ sqlglot 기반 SQL 리니지 파싱 (2026-02-10)
"""
import hashlib, json, logging, random, re
from datetime import date as _date, datetime as _dt
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

try:
    import sqlglot
    from sqlglot import exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False

logger = logging.getLogger(__name__)

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

class ParseSqlRequest(BaseModel):
    sql: str = Field(..., min_length=5)
    dialect: str = Field(default="postgres", pattern=r"^(postgres|mysql|bigquery|generic)$")

class QueryImpactRequest(BaseModel):
    sql: str = Field(..., min_length=5)

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


# ── lineage_log 테이블 보장 ──
async def _ensure_lineage_tables(conn):
    """lineage_log 테이블이 없으면 생성"""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS lineage_log (
            log_id BIGSERIAL PRIMARY KEY,
            sql_text TEXT,
            parsed_tables JSONB DEFAULT '[]',
            parsed_columns JSONB DEFAULT '[]',
            lineage_edges JSONB DEFAULT '[]',
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lineage_log_time ON lineage_log(created_at)
    """)


# ── FK 기반 동적 엣지 조회 ──
async def _fetch_fk_edges(conn) -> List[tuple]:
    """information_schema에서 실제 FK 관계를 조회하여 (source, target, fk_col) 리스트 반환"""
    try:
        rows = await conn.fetch("""
            SELECT
                kcu.table_name AS source_table,
                ccu.table_name AS target_table,
                kcu.column_name AS fk_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
                AND tc.table_schema = ccu.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
        """)
        return [(r["source_table"], r["target_table"], r["fk_column"]) for r in rows]
    except Exception as e:
        logger.warning("FK 조회 실패 (fallback to OMOP_EDGES): %s", e)
        return []


def _merge_edges(fk_edges: List[tuple], static_edges: List[tuple]) -> List[tuple]:
    """FK 기반 엣지 + 정적 OMOP_EDGES 병합 (중복 제거)"""
    seen = set()
    merged = []
    for edge in fk_edges + static_edges:
        key = (edge[0], edge[1], edge[2])
        if key not in seen:
            seen.add(key)
            merged.append(edge)
    return merged


# ═══════ 1. GET /lineage/graph ═══════

@router.get("/lineage/graph")
async def lineage_graph():
    """OMOP CDM 테이블 리니지 그래프 (노드 + 엣지) - FK 동적 조회 + OMOP_EDGES 병합"""
    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            "SELECT relname AS t, n_live_tup AS c FROM pg_stat_user_tables WHERE schemaname='public'")
        cmap = {r["t"]: r["c"] for r in rows}

        # 동적 FK 엣지 조회 + 정적 OMOP_EDGES 병합
        fk_edges = await _fetch_fk_edges(conn)
        all_edges = _merge_edges(fk_edges, list(OMOP_EDGES))

        nodes, seen = [], set()
        for tbl, cat in TABLE_CATEGORY.items():
            seen.add(tbl)
            nodes.append({"id": tbl, "label": tbl, "row_count": cmap.get(tbl, 0), "category": cat})
        for tbl, rc in cmap.items():
            if tbl not in seen:
                nodes.append({"id": tbl, "label": tbl, "row_count": rc, "category": "admin"})
        edges = [{"source": s, "target": t, "relationship": r} for s, t, r in all_edges]
        return {
            "nodes": nodes,
            "edges": edges,
            "total_tables": len(nodes),
            "total_edges": len(edges),
            "fk_edges_count": len(fk_edges),
            "static_edges_count": len(OMOP_EDGES),
        }
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


# ═══════ 7. POST /lineage/parse-sql  (NEW - sqlglot 기반) ═══════

@router.post("/lineage/parse-sql")
async def parse_sql(body: ParseSqlRequest):
    """sqlglot 기반 SQL 파싱 - 테이블/컬럼/리니지 엣지 추출"""
    if not SQLGLOT_AVAILABLE:
        raise HTTPException(501, "sqlglot 라이브러리가 설치되어 있지 않습니다")

    sql_text = body.sql.strip()
    dialect = body.dialect

    # 1) Parse the SQL AST
    try:
        parsed = sqlglot.parse_one(sql_text, read=dialect)
    except sqlglot.errors.ParseError as e:
        raise HTTPException(400, f"SQL 파싱 오류: {e}")
    except Exception as e:
        raise HTTPException(400, f"SQL 파싱 실패: {e}")

    # 2) Extract tables
    tables = []
    try:
        for tbl in parsed.find_all(exp.Table):
            tbl_name = tbl.name
            if tbl_name:
                schema_name = tbl.db if hasattr(tbl, "db") and tbl.db else None
                alias = tbl.alias if tbl.alias else None
                entry = {"name": tbl_name}
                if schema_name:
                    entry["schema"] = schema_name
                if alias:
                    entry["alias"] = alias
                # Avoid duplicates
                if entry not in tables:
                    tables.append(entry)
    except Exception as e:
        logger.warning("테이블 추출 실패: %s", e)

    # 3) Extract columns
    columns = []
    try:
        for col in parsed.find_all(exp.Column):
            col_entry = {"name": col.name}
            if col.table:
                col_entry["table"] = col.table
            if col_entry not in columns:
                columns.append(col_entry)
    except Exception as e:
        logger.warning("컬럼 추출 실패: %s", e)

    # 4) Determine statement type
    type_name = type(parsed).__name__  # e.g. Select, Insert, Create, etc.

    # 5) Try sqlglot.lineage() for SELECT columns to get lineage edges
    lineage_edges = []
    if isinstance(parsed, exp.Select):
        # Get output column names from the SELECT clause
        try:
            select_expressions = parsed.expressions
            for sel_expr in select_expressions:
                # Determine the output column name
                out_name = None
                if isinstance(sel_expr, exp.Alias):
                    out_name = sel_expr.alias
                elif isinstance(sel_expr, exp.Column):
                    out_name = sel_expr.name
                else:
                    continue

                if not out_name:
                    continue

                try:
                    lineage_node = sqlglot.lineage(out_name, sql_text, dialect=dialect)
                    # Walk downstream nodes to find source columns
                    for downstream in lineage_node.walk():
                        if downstream.expression and downstream != lineage_node:
                            src_expr = downstream.expression
                            if isinstance(src_expr, exp.Column):
                                edge = {
                                    "output_column": out_name,
                                    "source_column": src_expr.name,
                                    "source_table": src_expr.table or None,
                                }
                                if edge not in lineage_edges:
                                    lineage_edges.append(edge)
                except Exception:
                    # lineage() can fail on complex queries - skip silently
                    pass
        except Exception as e:
            logger.warning("sqlglot.lineage 추출 실패: %s", e)

    # 6) Log to lineage_log table
    conn = None
    try:
        conn = await _get_conn()
        await _ensure_lineage_tables(conn)
        await conn.execute(
            """
            INSERT INTO lineage_log (sql_text, parsed_tables, parsed_columns, lineage_edges)
            VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb)
            """,
            sql_text,
            json.dumps(tables, ensure_ascii=False),
            json.dumps(columns, ensure_ascii=False),
            json.dumps(lineage_edges, ensure_ascii=False),
        )
    except Exception as e:
        logger.warning("lineage_log 기록 실패: %s", e)
    finally:
        if conn:
            await _rel(conn)

    return {
        "tables": tables,
        "columns": columns,
        "lineage_edges": lineage_edges,
        "parsed_type": type_name,
        "dialect": dialect,
        "sqlglot_version": sqlglot.__version__ if SQLGLOT_AVAILABLE else None,
    }


# ═══════ 8. POST /lineage/query-impact  (NEW - sqlglot 기반) ═══════

@router.post("/lineage/query-impact")
async def query_impact(body: QueryImpactRequest):
    """SQL 쿼리가 참조하는 테이블/컬럼 및 하류 영향 분석"""
    if not SQLGLOT_AVAILABLE:
        raise HTTPException(501, "sqlglot 라이브러리가 설치되어 있지 않습니다")

    sql_text = body.sql.strip()

    # 1) Parse SQL
    try:
        parsed = sqlglot.parse_one(sql_text, read="postgres")
    except sqlglot.errors.ParseError as e:
        raise HTTPException(400, f"SQL 파싱 오류: {e}")
    except Exception as e:
        raise HTTPException(400, f"SQL 파싱 실패: {e}")

    # 2) Extract referenced tables
    referenced_tables = []
    try:
        for tbl in parsed.find_all(exp.Table):
            tbl_name = tbl.name
            if tbl_name and tbl_name not in referenced_tables:
                referenced_tables.append(tbl_name)
    except Exception as e:
        logger.warning("쿼리 영향 분석 - 테이블 추출 실패: %s", e)

    # 3) Extract referenced columns
    referenced_columns = []
    try:
        for col in parsed.find_all(exp.Column):
            entry = {"name": col.name}
            if col.table:
                entry["table"] = col.table
            if entry not in referenced_columns:
                referenced_columns.append(entry)
    except Exception as e:
        logger.warning("쿼리 영향 분석 - 컬럼 추출 실패: %s", e)

    # 4) Build edge graph for downstream lookups (dynamic FK + static OMOP_EDGES)
    conn = await _get_conn()
    try:
        fk_edges = await _fetch_fk_edges(conn)
        all_edges = _merge_edges(fk_edges, list(OMOP_EDGES))
    finally:
        await _rel(conn)

    # Build adjacency: source -> list of downstream tables
    downstream_map: Dict[str, List[Dict[str, str]]] = {}
    for src, tgt, rel in all_edges:
        downstream_map.setdefault(src, []).append({"table": tgt, "relationship": rel})

    # 5) For each referenced table, find downstream impact (BFS 1-hop)
    downstream_impact = []
    visited = set()
    for tbl in referenced_tables:
        tbl_lower = tbl.lower()
        if tbl_lower in downstream_map:
            for dep in downstream_map[tbl_lower]:
                dep_key = (tbl_lower, dep["table"])
                if dep_key not in visited:
                    visited.add(dep_key)
                    downstream_impact.append({
                        "source_table": tbl_lower,
                        "impacted_table": dep["table"],
                        "relationship": dep["relationship"],
                    })

    return {
        "referenced_tables": referenced_tables,
        "referenced_columns": referenced_columns,
        "downstream_impact": downstream_impact,
        "total_impacted_tables": len({d["impacted_table"] for d in downstream_impact}),
        "parsed_type": type(parsed).__name__,
    }
