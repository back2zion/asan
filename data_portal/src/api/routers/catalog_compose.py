"""
카탈로그 데이터 조합 API — FK 감지 + 조인 경로 탐색 + SQL 생성
DPR-002: 지능형 데이터 카탈로그 및 탐색 환경 구축
"""
import os
from collections import deque
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
import asyncpg

router = APIRouter(prefix="/catalog-compose", tags=["CatalogCompose"])

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}

# OMOP CDM FK 관계 그래프 (16 테이블, ~18 FK 관계)
FK_RELATIONSHIPS: list[dict] = [
    # person 중심
    {"from_table": "visit_occurrence", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "condition_occurrence", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "drug_exposure", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "measurement", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "observation", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "procedure_occurrence", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "visit_detail", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "observation_period", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "condition_era", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "drug_era", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "cost", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    {"from_table": "payer_plan_period", "from_column": "person_id", "to_table": "person", "to_column": "person_id"},
    # visit_occurrence 중심
    {"from_table": "condition_occurrence", "from_column": "visit_occurrence_id", "to_table": "visit_occurrence", "to_column": "visit_occurrence_id"},
    {"from_table": "measurement", "from_column": "visit_occurrence_id", "to_table": "visit_occurrence", "to_column": "visit_occurrence_id"},
    {"from_table": "procedure_occurrence", "from_column": "visit_occurrence_id", "to_table": "visit_occurrence", "to_column": "visit_occurrence_id"},
    {"from_table": "visit_detail", "from_column": "visit_occurrence_id", "to_table": "visit_occurrence", "to_column": "visit_occurrence_id"},
    # care_site / provider / location
    {"from_table": "provider", "from_column": "care_site_id", "to_table": "care_site", "to_column": "care_site_id"},
    {"from_table": "care_site", "from_column": "location_id", "to_table": "location", "to_column": "location_id"},
]

# 테이블별 주요 컬럼 (메타데이터)
TABLE_COLUMNS: dict[str, list[dict]] = {
    "person": [
        {"name": "person_id", "type": "bigint", "pk": True},
        {"name": "gender_concept_id", "type": "integer"},
        {"name": "year_of_birth", "type": "integer"},
        {"name": "month_of_birth", "type": "integer"},
        {"name": "race_concept_id", "type": "integer"},
        {"name": "ethnicity_concept_id", "type": "integer"},
        {"name": "gender_source_value", "type": "varchar"},
        {"name": "race_source_value", "type": "varchar"},
    ],
    "visit_occurrence": [
        {"name": "visit_occurrence_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "visit_concept_id", "type": "integer"},
        {"name": "visit_start_date", "type": "date"},
        {"name": "visit_end_date", "type": "date"},
        {"name": "visit_type_concept_id", "type": "integer"},
        {"name": "care_site_id", "type": "bigint"},
    ],
    "condition_occurrence": [
        {"name": "condition_occurrence_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "condition_concept_id", "type": "integer"},
        {"name": "condition_start_date", "type": "date"},
        {"name": "condition_end_date", "type": "date"},
        {"name": "condition_type_concept_id", "type": "integer"},
        {"name": "visit_occurrence_id", "type": "bigint"},
    ],
    "drug_exposure": [
        {"name": "drug_exposure_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "drug_concept_id", "type": "integer"},
        {"name": "drug_exposure_start_date", "type": "date"},
        {"name": "drug_exposure_end_date", "type": "date"},
        {"name": "quantity", "type": "numeric"},
        {"name": "days_supply", "type": "integer"},
    ],
    "measurement": [
        {"name": "measurement_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "measurement_concept_id", "type": "integer"},
        {"name": "measurement_date", "type": "date"},
        {"name": "value_as_number", "type": "numeric"},
        {"name": "value_as_concept_id", "type": "integer"},
        {"name": "unit_concept_id", "type": "integer"},
        {"name": "visit_occurrence_id", "type": "bigint"},
    ],
    "observation": [
        {"name": "observation_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "observation_concept_id", "type": "integer"},
        {"name": "observation_date", "type": "date"},
        {"name": "value_as_number", "type": "numeric"},
        {"name": "value_as_string", "type": "varchar"},
    ],
    "procedure_occurrence": [
        {"name": "procedure_occurrence_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "procedure_concept_id", "type": "integer"},
        {"name": "procedure_date", "type": "date"},
        {"name": "procedure_type_concept_id", "type": "integer"},
        {"name": "visit_occurrence_id", "type": "bigint"},
    ],
    "visit_detail": [
        {"name": "visit_detail_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "visit_occurrence_id", "type": "bigint"},
        {"name": "visit_detail_concept_id", "type": "integer"},
        {"name": "visit_detail_start_date", "type": "date"},
        {"name": "visit_detail_end_date", "type": "date"},
        {"name": "care_site_id", "type": "bigint"},
    ],
    "condition_era": [
        {"name": "condition_era_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "condition_concept_id", "type": "integer"},
        {"name": "condition_era_start_date", "type": "date"},
        {"name": "condition_era_end_date", "type": "date"},
        {"name": "condition_occurrence_count", "type": "integer"},
    ],
    "drug_era": [
        {"name": "drug_era_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "drug_concept_id", "type": "integer"},
        {"name": "drug_era_start_date", "type": "date"},
        {"name": "drug_era_end_date", "type": "date"},
        {"name": "drug_exposure_count", "type": "integer"},
    ],
    "observation_period": [
        {"name": "observation_period_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "observation_period_start_date", "type": "date"},
        {"name": "observation_period_end_date", "type": "date"},
    ],
    "cost": [
        {"name": "cost_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "cost_event_id", "type": "bigint"},
        {"name": "total_charge", "type": "numeric"},
        {"name": "total_cost", "type": "numeric"},
        {"name": "total_paid", "type": "numeric"},
    ],
    "payer_plan_period": [
        {"name": "payer_plan_period_id", "type": "bigint", "pk": True},
        {"name": "person_id", "type": "bigint"},
        {"name": "contract_person_id", "type": "bigint"},
        {"name": "payer_plan_period_start_date", "type": "date"},
        {"name": "payer_plan_period_end_date", "type": "date"},
    ],
    "care_site": [
        {"name": "care_site_id", "type": "bigint", "pk": True},
        {"name": "care_site_name", "type": "varchar"},
        {"name": "place_of_service_concept_id", "type": "integer"},
        {"name": "location_id", "type": "bigint"},
    ],
    "provider": [
        {"name": "provider_id", "type": "bigint", "pk": True},
        {"name": "provider_name", "type": "varchar"},
        {"name": "specialty_concept_id", "type": "integer"},
        {"name": "care_site_id", "type": "bigint"},
    ],
    "location": [
        {"name": "location_id", "type": "bigint", "pk": True},
        {"name": "address_1", "type": "varchar"},
        {"name": "city", "type": "varchar"},
        {"name": "state", "type": "varchar"},
        {"name": "zip", "type": "varchar"},
    ],
}

# 테이블 별칭 매핑
TABLE_ALIASES: dict[str, str] = {
    "person": "p",
    "visit_occurrence": "vo",
    "visit_detail": "vd",
    "condition_occurrence": "co",
    "condition_era": "ce",
    "drug_exposure": "de",
    "drug_era": "dre",
    "procedure_occurrence": "po",
    "measurement": "m",
    "observation": "obs",
    "observation_period": "op",
    "cost": "c",
    "payer_plan_period": "pp",
    "care_site": "cs",
    "provider": "pv",
    "location": "loc",
}


def _build_adjacency():
    """양방향 인접 리스트 구축"""
    adj: dict[str, list[dict]] = {}
    for fk in FK_RELATIONSHIPS:
        ft, fc, tt, tc = fk["from_table"], fk["from_column"], fk["to_table"], fk["to_column"]
        adj.setdefault(ft, []).append({"table": tt, "from_col": fc, "to_col": tc})
        adj.setdefault(tt, []).append({"table": ft, "from_col": tc, "to_col": fc})
    return adj


ADJ = _build_adjacency()


def _find_join_path(tables: list[str]) -> list[dict]:
    """BFS로 선택한 테이블들 간 최적 조인 경로 탐색"""
    if len(tables) < 2:
        return []

    joins = []
    connected = {tables[0]}

    for target in tables[1:]:
        if target in connected:
            continue
        # BFS from connected set to target
        queue: deque[list[str]] = deque()
        visited: set[str] = set()
        for src in connected:
            queue.append([src])
            visited.add(src)

        found_path = None
        while queue:
            path = queue.popleft()
            current = path[-1]
            if current == target:
                found_path = path
                break
            for edge in ADJ.get(current, []):
                next_table = edge["table"]
                if next_table not in visited:
                    visited.add(next_table)
                    queue.append(path + [next_table])

        if found_path:
            for i in range(len(found_path) - 1):
                a, b = found_path[i], found_path[i + 1]
                # Find the FK edge
                for edge in ADJ.get(a, []):
                    if edge["table"] == b:
                        joins.append({
                            "left_table": a,
                            "left_column": edge["from_col"],
                            "right_table": b,
                            "right_column": edge["to_col"],
                            "join_type": "LEFT JOIN",
                        })
                        connected.add(b)
                        break
            # Also add intermediate tables
            for t in found_path:
                connected.add(t)
        else:
            # No path found — still return what we have
            pass

    return joins


async def get_connection():
    return await asyncpg.connect(**OMOP_DB_CONFIG)


# ───── Endpoints ─────

@router.get("/fk-relationships")
async def get_fk_relationships():
    """전체 FK 관계 그래프"""
    all_tables = set()
    for fk in FK_RELATIONSHIPS:
        all_tables.add(fk["from_table"])
        all_tables.add(fk["to_table"])

    return {
        "relationships": FK_RELATIONSHIPS,
        "tables": sorted(all_tables),
        "table_columns": TABLE_COLUMNS,
        "total_relationships": len(FK_RELATIONSHIPS),
        "total_tables": len(all_tables),
    }


class DetectJoinsRequest(BaseModel):
    tables: list[str] = Field(..., min_length=2, max_length=8)


@router.post("/detect-joins")
async def detect_joins(body: DetectJoinsRequest):
    """선택한 테이블들 간 최적 조인 경로 탐색 (BFS)"""
    valid_tables = set(TABLE_COLUMNS.keys())
    invalid = [t for t in body.tables if t not in valid_tables]
    if invalid:
        raise HTTPException(status_code=400, detail=f"유효하지 않은 테이블: {invalid}")

    joins = _find_join_path(body.tables)
    # 중간 테이블 포함 여부
    all_tables_in_path = set(body.tables)
    for j in joins:
        all_tables_in_path.add(j["left_table"])
        all_tables_in_path.add(j["right_table"])

    return {
        "requested_tables": body.tables,
        "joins": joins,
        "all_tables": sorted(all_tables_in_path),
        "joinable": len(joins) > 0,
    }


class GenerateSqlRequest(BaseModel):
    tables: list[str] = Field(..., min_length=1, max_length=8)
    selected_columns: dict[str, list[str]] = Field(default_factory=dict)
    joins: Optional[list[dict]] = None
    limit: int = Field(default=100, ge=1, le=10000)


@router.post("/generate-sql")
async def generate_sql(body: GenerateSqlRequest):
    """테이블+조인+컬럼 → SELECT JOIN SQL 생성"""
    # 조인이 제공되지 않으면 자동 탐색
    joins = body.joins
    if not joins and len(body.tables) >= 2:
        joins = _find_join_path(body.tables)

    # 컬럼 선택 (비어있으면 주요 컬럼 자동 선택)
    selected = body.selected_columns
    if not selected:
        for t in body.tables:
            cols = TABLE_COLUMNS.get(t, [])
            selected[t] = [c["name"] for c in cols[:5]]

    # SQL 생성
    primary = body.tables[0]
    alias_p = TABLE_ALIASES.get(primary, primary[:2])

    # SELECT 절
    select_parts = []
    for t in body.tables:
        alias = TABLE_ALIASES.get(t, t[:2])
        for col in selected.get(t, []):
            select_parts.append(f"{alias}.{col}")

    if not select_parts:
        select_parts = [f"{alias_p}.*"]

    select_clause = ",\n       ".join(select_parts)

    # FROM / JOIN 절
    from_clause = f"{primary} {alias_p}"
    join_clauses = []
    if joins:
        joined_tables = {primary}
        for j in joins:
            lt = j["left_table"]
            rt = j["right_table"]
            lc = j["left_column"]
            rc = j["right_column"]
            jt = j.get("join_type", "LEFT JOIN")

            if rt not in joined_tables:
                ra = TABLE_ALIASES.get(rt, rt[:2])
                la = TABLE_ALIASES.get(lt, lt[:2])
                join_clauses.append(f"{jt} {rt} {ra} ON {la}.{lc} = {ra}.{rc}")
                joined_tables.add(rt)
            elif lt not in joined_tables:
                la = TABLE_ALIASES.get(lt, lt[:2])
                ra = TABLE_ALIASES.get(rt, rt[:2])
                join_clauses.append(f"{jt} {lt} {la} ON {ra}.{rc} = {la}.{lc}")
                joined_tables.add(lt)

    sql = f"SELECT {select_clause}\nFROM {from_clause}"
    if join_clauses:
        sql += "\n" + "\n".join(join_clauses)
    sql += f"\nLIMIT {body.limit};"

    return {
        "sql": sql,
        "tables": body.tables,
        "joins": joins or [],
        "column_count": len(select_parts),
    }


class PreviewRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    limit: int = Field(default=20, ge=1, le=100)


@router.post("/preview")
async def preview_sql(body: PreviewRequest):
    """생성된 SQL 실행 (LIMIT 강제 적용) + 결과 반환"""
    sql = body.sql.strip().rstrip(";")

    # 안전성: SELECT만 허용
    sql_upper = sql.upper().strip()
    if not sql_upper.startswith("SELECT"):
        raise HTTPException(status_code=400, detail="SELECT 쿼리만 실행할 수 있습니다")
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE", "GRANT", "REVOKE"]
    for keyword in forbidden:
        if keyword in sql_upper.split("SELECT", 1)[0]:
            raise HTTPException(status_code=400, detail=f"허용되지 않은 키워드: {keyword}")

    # LIMIT 강제 적용
    if "LIMIT" in sql_upper:
        # 기존 LIMIT 교체
        import re
        sql = re.sub(r'LIMIT\s+\d+', f'LIMIT {body.limit}', sql, flags=re.IGNORECASE)
    else:
        sql += f" LIMIT {body.limit}"

    conn = await get_connection()
    try:
        rows = await conn.fetch(sql)
        if not rows:
            return {"columns": [], "rows": [], "row_count": 0, "sql": sql}

        columns = list(rows[0].keys())
        result_rows = [dict(r) for r in rows]

        return {
            "columns": columns,
            "rows": result_rows,
            "row_count": len(result_rows),
            "sql": sql,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"SQL 실행 오류: {str(e)}")
    finally:
        await conn.close()
