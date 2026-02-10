"""
gov_lineage_ext.py helpers -- constants, models, DB helpers, de-identification
functions, and utility functions extracted for readability.
"""
import hashlib, json, logging, random, re
from datetime import date as _date, datetime as _dt
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field

try:
    import sqlglot
    from sqlglot import exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    sqlglot = None          # type: ignore[assignment]
    exp = None              # type: ignore[assignment]
    SQLGLOT_AVAILABLE = False

logger = logging.getLogger(__name__)

# ── DB helpers (services.db_pool 패턴) ──────────────────────────────

async def _get_conn():
    from services.db_pool import get_pool
    return await (await get_pool()).acquire()


async def _rel(conn):
    from services.db_pool import get_pool
    await (await get_pool()).release(conn)


# ── OMOP CDM 리니지 상수 ────────────────────────────────────────────

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

# ── Pydantic 모델 ───────────────────────────────────────────────────

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


# ── 비식별화 함수 ───────────────────────────────────────────────────

_FORBIDDEN_SQL = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|GRANT|REVOKE|COPY|EXECUTE)\b", re.I)


def _apply_mask(v: Any) -> Any:
    if v is None:
        return None
    s = str(v)
    return s[0] + "***" if len(s) > 1 else "***"


def _apply_hash(v: Any) -> Optional[str]:
    return hashlib.sha256(str(v).encode()).hexdigest() if v is not None else None


def _apply_generalize(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (_date, _dt)):
        return v.year
    if isinstance(v, (int, float)):
        return (int(v) // 10) * 10 if 0 < abs(v) < 200 else round(v, -1)
    s = str(v)
    return s[:4] if re.match(r"^\d{4}-\d{2}-\d{2}", s) else v


def _apply_noise(v: Any) -> Any:
    if v is None or not isinstance(v, (int, float)):
        return v
    r = v * (1 + random.uniform(-0.05, 0.05))
    return round(r, 2) if isinstance(v, float) else int(round(r))


DEIDENT_FN = {
    "mask": _apply_mask,
    "hash": _apply_hash,
    "generalize": _apply_generalize,
    "suppress": lambda v: None,
    "noise": _apply_noise,
}


def _serialize(val: Any) -> Any:
    """datetime/date -> isoformat string"""
    return val.isoformat() if isinstance(val, (_date, _dt)) else val


# ── lineage_log 테이블 보장 ─────────────────────────────────────────

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


# ── FK 기반 동적 엣지 조회 ──────────────────────────────────────────

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
    seen: set = set()
    merged: List[tuple] = []
    for edge in fk_edges + static_edges:
        key = (edge[0], edge[1], edge[2])
        if key not in seen:
            seen.add(key)
            merged.append(edge)
    return merged
