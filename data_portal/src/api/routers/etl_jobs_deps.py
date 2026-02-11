"""
ETL Jobs Dependencies: table dependency CRUD, graph visualization, auto-detect, execution order.
"""
import json
from collections import defaultdict, deque
from typing import List

from fastapi import APIRouter, HTTPException
import asyncpg

from routers.etl_jobs_shared import (
    get_connection, release_connection, _ensure_tables,
    DependencyCreate,
)

router = APIRouter()


# ═══════════════════════════════════════════════════
#  Table Dependencies
# ═══════════════════════════════════════════════════

@router.get("/dependencies")
async def list_dependencies():
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("SELECT * FROM etl_table_dependency ORDER BY dep_id")
        return {
            "dependencies": [
                {
                    "dep_id": r["dep_id"],
                    "source_table": r["source_table"],
                    "target_table": r["target_table"],
                    "relationship": r["relationship"],
                    "dep_type": r["dep_type"],
                }
                for r in rows
            ]
        }
    finally:
        await release_connection(conn)


@router.get("/dependencies/graph")
async def get_dependency_graph():
    """ReactFlow 포맷으로 종속관계 그래프 반환"""
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        deps = await conn.fetch("SELECT * FROM etl_table_dependency ORDER BY dep_id")

        # Collect unique tables
        table_set = set()
        for d in deps:
            table_set.add(d["source_table"])
            table_set.add(d["target_table"])

        # Row counts for sizing
        row_counts = {}
        if table_set:
            rc = await conn.fetch("""
                SELECT relname, n_live_tup FROM pg_stat_user_tables
                WHERE schemaname='public'
            """)
            row_counts = {r["relname"]: r["n_live_tup"] for r in rc}

        # Domain color mapping
        domain_colors = {
            "person": "#005BAC", "visit_occurrence": "#005BAC", "visit_detail": "#005BAC",
            "condition_occurrence": "#E53E3E", "condition_era": "#E53E3E",
            "drug_exposure": "#38A169", "drug_era": "#38A169",
            "procedure_occurrence": "#D69E2E",
            "measurement": "#805AD5", "observation": "#805AD5",
            "device_exposure": "#DD6B20",
            "death": "#718096", "observation_period": "#718096",
            "cost": "#319795", "payer_plan_period": "#319795",
            "care_site": "#B7791F", "provider": "#B7791F", "location": "#B7791F",
        }

        # Layout: topological layers
        sorted_tables = _topological_sort_tables(deps)
        layer_map = {}
        for i, tbl in enumerate(sorted_tables):
            layer_map[tbl] = i
        # Remaining tables not in sort
        for tbl in table_set:
            if tbl not in layer_map:
                layer_map[tbl] = len(sorted_tables)

        nodes = []
        for tbl in sorted(table_set):
            layer = layer_map.get(tbl, 0)
            rc = row_counts.get(tbl, 0)
            nodes.append({
                "id": tbl,
                "type": "default",
                "position": {"x": layer * 250, "y": list(sorted(table_set)).index(tbl) * 80},
                "data": {
                    "label": tbl,
                    "rowCount": rc,
                    "color": domain_colors.get(tbl, "#718096"),
                },
            })

        edges = []
        for d in deps:
            edges.append({
                "id": f"e-{d['dep_id']}",
                "source": d["source_table"],
                "target": d["target_table"],
                "label": d["relationship"] or "",
                "type": "smoothstep",
                "animated": d["dep_type"] == "fk",
                "style": {
                    "stroke": "#005BAC" if d["dep_type"] == "fk" else "#A0AEC0",
                    "strokeDasharray": "none" if d["dep_type"] == "fk" else "5 5",
                },
            })

        return {"nodes": nodes, "edges": edges}
    finally:
        await release_connection(conn)


def _topological_sort_tables(deps) -> List[str]:
    """Kahn's algorithm for topological sorting"""
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    all_nodes = set()

    for d in deps:
        src = d["source_table"]
        tgt = d["target_table"]
        all_nodes.add(src)
        all_nodes.add(tgt)
        graph[src].append(tgt)
        in_degree[tgt] = in_degree.get(tgt, 0) + 1
        if src not in in_degree:
            in_degree[src] = 0

    queue = deque([n for n in all_nodes if in_degree.get(n, 0) == 0])
    result = []

    while queue:
        node = queue.popleft()
        result.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    return result


@router.post("/dependencies/auto-detect")
async def auto_detect_dependencies():
    """OMOP CDM FK 자동 감지 + _id 컬럼명 패턴 매칭"""
    conn = await get_connection()
    try:
        await _ensure_tables(conn)

        # 1) information_schema FK constraints
        fk_rows = await conn.fetch("""
            SELECT
                kcu.table_name AS source_table,
                ccu.table_name AS target_table,
                kcu.column_name AS fk_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
        """)

        detected = []
        for r in fk_rows:
            if r["source_table"] != r["target_table"]:
                detected.append({
                    "source_table": r["target_table"],  # FK target = parent
                    "target_table": r["source_table"],   # FK source = child
                    "relationship": f"FK: {r['fk_column']}",
                    "dep_type": "fk",
                })

        # 2) OMOP CDM convention: *_id columns referencing other tables
        omop_relations = [
            ("person", "visit_occurrence", "person_id"),
            ("person", "condition_occurrence", "person_id"),
            ("person", "drug_exposure", "person_id"),
            ("person", "procedure_occurrence", "person_id"),
            ("person", "measurement", "person_id"),
            ("person", "observation", "person_id"),
            ("person", "device_exposure", "person_id"),
            ("person", "death", "person_id"),
            ("person", "observation_period", "person_id"),
            ("person", "payer_plan_period", "person_id"),
            ("visit_occurrence", "condition_occurrence", "visit_occurrence_id"),
            ("visit_occurrence", "drug_exposure", "visit_occurrence_id"),
            ("visit_occurrence", "procedure_occurrence", "visit_occurrence_id"),
            ("visit_occurrence", "measurement", "visit_occurrence_id"),
            ("visit_occurrence", "observation", "visit_occurrence_id"),
            ("visit_occurrence", "device_exposure", "visit_occurrence_id"),
            ("condition_occurrence", "condition_era", "derived"),
            ("drug_exposure", "drug_era", "derived"),
            ("care_site", "visit_occurrence", "care_site_id"),
            ("provider", "visit_occurrence", "provider_id"),
            ("location", "care_site", "location_id"),
            ("person", "cost", "person_id"),
        ]

        # Merge: skip duplicates
        existing_pairs = {(d["source_table"], d["target_table"]) for d in detected}
        for src, tgt, col in omop_relations:
            if (src, tgt) not in existing_pairs:
                detected.append({
                    "source_table": src,
                    "target_table": tgt,
                    "relationship": f"OMOP: {col}" if col != "derived" else "derived aggregation",
                    "dep_type": "fk" if col != "derived" else "derived",
                })
                existing_pairs.add((src, tgt))

        # Upsert into DB
        inserted = 0
        for d in detected:
            try:
                await conn.execute("""
                    INSERT INTO etl_table_dependency (source_table, target_table, relationship, dep_type)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (source_table, target_table) DO UPDATE SET relationship=$3, dep_type=$4
                """, d["source_table"], d["target_table"], d["relationship"], d["dep_type"])
                inserted += 1
            except Exception:
                pass

        return {
            "success": True,
            "detected_count": len(detected),
            "inserted_count": inserted,
            "dependencies": detected,
        }
    finally:
        await release_connection(conn)


@router.post("/dependencies")
async def create_dependency(body: DependencyCreate):
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        dep_id = await conn.fetchval("""
            INSERT INTO etl_table_dependency (source_table, target_table, relationship, dep_type)
            VALUES ($1, $2, $3, $4) RETURNING dep_id
        """, body.source_table, body.target_table, body.relationship, body.dep_type)
        return {"success": True, "dep_id": dep_id}
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="이미 동일 종속관계가 존재합니다")
    finally:
        await release_connection(conn)


@router.delete("/dependencies/{dep_id}")
async def delete_dependency(dep_id: int):
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM etl_table_dependency WHERE dep_id=$1", dep_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="종속관계를 찾을 수 없습니다")
        return {"success": True}
    finally:
        await release_connection(conn)


@router.get("/dependencies/execution-order")
async def get_execution_order():
    """위상정렬 기반 실행 순서"""
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        deps = await conn.fetch("SELECT * FROM etl_table_dependency")
        sorted_tables = _topological_sort_tables(deps)

        # Check for cycles
        all_nodes = set()
        for d in deps:
            all_nodes.add(d["source_table"])
            all_nodes.add(d["target_table"])

        has_cycle = len(sorted_tables) < len(all_nodes)

        return {
            "execution_order": [
                {"order": i + 1, "table": tbl}
                for i, tbl in enumerate(sorted_tables)
            ],
            "total_tables": len(all_nodes),
            "sorted_count": len(sorted_tables),
            "has_cycle": has_cycle,
            "cycle_warning": "순환 의존성이 감지되었습니다. 일부 테이블이 정렬에서 제외되었습니다." if has_cycle else None,
        }
    finally:
        await release_connection(conn)
