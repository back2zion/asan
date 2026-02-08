"""
데이터 카탈로그 - 오너쉽/공유, 연관 관계, 작업 이력, 리소스
"""
import json
from typing import Optional

from fastapi import APIRouter

from .data_catalog_shared import (
    get_connection, _init,
    OwnershipCreate, RelationCreate, WorkHistoryCreate, ResourceCreate,
)

router = APIRouter()


# ══════════════════════════════════════════════
#  2. Ownership & Sharing
# ══════════════════════════════════════════════

@router.get("/ownerships")
async def list_ownerships(entry_id: Optional[int] = None, owner_type: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = ["o.status='active'"], []
        if entry_id:
            params.append(entry_id)
            where.append(f"o.entry_id = ${len(params)}")
        if owner_type:
            params.append(owner_type)
            where.append(f"o.owner_type = ${len(params)}")
        rows = await conn.fetch(f"""
            SELECT o.*, e.entry_name, e.business_name, e.entry_type
            FROM catalog_ownership o JOIN catalog_entry e ON e.entry_id = o.entry_id
            WHERE {' AND '.join(where)} ORDER BY e.entry_name, o.owner_type
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/ownerships")
async def create_ownership(body: OwnershipCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO catalog_ownership (entry_id, owner_type, dept, person, access_scope, share_purpose, expiry_date)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING ownership_id
        """, body.entry_id, body.owner_type, body.dept, body.person, body.access_scope,
            body.share_purpose, body.expiry_date)
        # Log sharing history
        await conn.execute("""
            INSERT INTO catalog_work_history (entry_id, action_type, action_detail, actor)
            VALUES ($1, 'share', $2::jsonb, $3)
        """, body.entry_id, json.dumps({"dept": body.dept, "person": body.person, "scope": body.access_scope}),
            body.person or body.dept)
        return {"ownership_id": row["ownership_id"]}
    finally:
        await conn.close()


@router.delete("/ownerships/{ownership_id}")
async def revoke_ownership(ownership_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("UPDATE catalog_ownership SET status='revoked' WHERE ownership_id=$1", ownership_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/ownerships/matrix")
async def ownership_matrix():
    """오너쉽 매트릭스: 엔트리별 소유/공유 현황"""
    conn = await get_connection()
    try:
        await _init(conn)
        rows = await conn.fetch("""
            SELECT e.entry_id, e.entry_name, e.business_name, e.entry_type, e.domain, e.access_level,
                array_agg(DISTINCT CASE WHEN o.owner_type='primary' THEN o.dept END) FILTER (WHERE o.owner_type='primary') AS primary_depts,
                array_agg(DISTINCT CASE WHEN o.owner_type='steward' THEN o.dept END) FILTER (WHERE o.owner_type='steward') AS steward_depts,
                array_agg(DISTINCT CASE WHEN o.owner_type='consumer' THEN o.dept END) FILTER (WHERE o.owner_type='consumer') AS consumer_depts,
                array_agg(DISTINCT CASE WHEN o.owner_type='shared' THEN o.dept END) FILTER (WHERE o.owner_type='shared') AS shared_depts,
                COUNT(DISTINCT o.ownership_id) FILTER (WHERE o.status='active') AS total_stakeholders
            FROM catalog_entry e
            LEFT JOIN catalog_ownership o ON o.entry_id = e.entry_id AND o.status='active'
            WHERE e.status='active'
            GROUP BY e.entry_id ORDER BY e.entry_type, e.entry_id
        """)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


# ══════════════════════════════════════════════
#  3. Relations
# ══════════════════════════════════════════════

@router.get("/relations")
async def list_relations(entry_id: Optional[int] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        if entry_id:
            rows = await conn.fetch("""
                SELECT r.*, s.entry_name AS source_name, s.business_name AS source_biz, s.entry_type AS source_type,
                       t.entry_name AS target_name, t.business_name AS target_biz, t.entry_type AS target_type
                FROM catalog_relation r
                JOIN catalog_entry s ON s.entry_id = r.source_entry_id
                JOIN catalog_entry t ON t.entry_id = r.target_entry_id
                WHERE r.source_entry_id=$1 OR r.target_entry_id=$1
            """, entry_id)
        else:
            rows = await conn.fetch("""
                SELECT r.*, s.entry_name AS source_name, s.business_name AS source_biz, s.entry_type AS source_type,
                       t.entry_name AS target_name, t.business_name AS target_biz, t.entry_type AS target_type
                FROM catalog_relation r
                JOIN catalog_entry s ON s.entry_id = r.source_entry_id
                JOIN catalog_entry t ON t.entry_id = r.target_entry_id
            """)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.get("/relations/graph")
async def relation_graph():
    """연관 데이터 그래프 (ReactFlow 포맷)"""
    conn = await get_connection()
    try:
        await _init(conn)
        entries = await conn.fetch("SELECT entry_id, entry_name, business_name, entry_type, domain, row_count FROM catalog_entry WHERE status='active'")
        relations = await conn.fetch("SELECT * FROM catalog_relation")

        type_colors = {"table": "#006241", "view": "#805AD5", "api": "#1890ff",
                       "sql_query": "#DD6B20", "json_schema": "#D69E2E", "xml_schema": "#E53E3E",
                       "file": "#38A169", "stream": "#722ed1"}
        nodes, edges = [], []
        col_count = 4
        for i, e in enumerate(entries):
            d = dict(e)
            nodes.append({
                "id": str(d["entry_id"]),
                "type": "default",
                "position": {"x": (i % col_count) * 280, "y": (i // col_count) * 160},
                "data": {"label": d["business_name"], "entry_type": d["entry_type"],
                         "domain": d["domain"], "row_count": d["row_count"]},
                "style": {"background": f"{type_colors.get(d['entry_type'], '#999')}15",
                           "border": f"2px solid {type_colors.get(d['entry_type'], '#999')}",
                           "borderRadius": 8, "padding": 10, "fontSize": 12},
            })
        edge_styles = {
            "feeds_into": {"stroke": "#006241", "animated": True},
            "derives_from": {"stroke": "#DD6B20", "strokeDasharray": "5,5"},
            "references": {"stroke": "#1890ff"},
            "same_entity": {"stroke": "#722ed1", "strokeWidth": 2},
            "aggregates": {"stroke": "#D69E2E"},
            "supplements": {"stroke": "#38A169", "strokeDasharray": "3,3"},
        }
        for r in relations:
            style = edge_styles.get(r["relation_type"], {})
            edges.append({
                "id": f"r-{r['relation_id']}",
                "source": str(r["source_entry_id"]),
                "target": str(r["target_entry_id"]),
                "label": r["relation_type"],
                "style": style,
                "animated": style.get("animated", False),
            })
        return {"nodes": nodes, "edges": edges}
    finally:
        await conn.close()


@router.post("/relations")
async def create_relation(body: RelationCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO catalog_relation (source_entry_id, target_entry_id, relation_type, description)
            VALUES ($1,$2,$3,$4) RETURNING relation_id
        """, body.source_entry_id, body.target_entry_id, body.relation_type, body.description)
        return {"relation_id": row["relation_id"]}
    finally:
        await conn.close()


@router.delete("/relations/{relation_id}")
async def delete_relation(relation_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("DELETE FROM catalog_relation WHERE relation_id=$1", relation_id)
        return {"ok": True}
    finally:
        await conn.close()


# ══════════════════════════════════════════════
#  4. Work History
# ══════════════════════════════════════════════

@router.get("/history")
async def list_history(entry_id: Optional[int] = None, action_type: Optional[str] = None,
                       cohort_id: Optional[str] = None, job_id: Optional[str] = None,
                       limit: int = 50):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if entry_id:
            params.append(entry_id)
            where.append(f"h.entry_id = ${len(params)}")
        if action_type:
            params.append(action_type)
            where.append(f"h.action_type = ${len(params)}")
        if cohort_id:
            params.append(cohort_id)
            where.append(f"h.cohort_id = ${len(params)}")
        if job_id:
            params.append(job_id)
            where.append(f"h.job_id = ${len(params)}")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        params.append(limit)
        rows = await conn.fetch(f"""
            SELECT h.*, e.entry_name, e.business_name FROM catalog_work_history h
            JOIN catalog_entry e ON e.entry_id = h.entry_id
            {w} ORDER BY h.created_at DESC LIMIT ${len(params)}
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/history")
async def create_history(body: WorkHistoryCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO catalog_work_history (entry_id, action_type, action_detail, actor, job_id, cohort_id, row_count)
            VALUES ($1,$2,$3::jsonb,$4,$5,$6,$7) RETURNING history_id
        """, body.entry_id, body.action_type,
            json.dumps(body.action_detail) if body.action_detail else None,
            body.actor, body.job_id, body.cohort_id, body.row_count)
        return {"history_id": row["history_id"]}
    finally:
        await conn.close()


# ══════════════════════════════════════════════
#  5. Resources (API, View, SQL, JSON, XML, etc.)
# ══════════════════════════════════════════════

@router.get("/resources")
async def list_resources(entry_id: Optional[int] = None, resource_type: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = ["rs.status='active'"], []
        if entry_id:
            params.append(entry_id)
            where.append(f"rs.entry_id = ${len(params)}")
        if resource_type:
            params.append(resource_type)
            where.append(f"rs.resource_type = ${len(params)}")
        rows = await conn.fetch(f"""
            SELECT rs.*, e.entry_name, e.business_name FROM catalog_resource rs
            JOIN catalog_entry e ON e.entry_id = rs.entry_id
            WHERE {' AND '.join(where)} ORDER BY rs.resource_type, rs.resource_id
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/resources")
async def create_resource(body: ResourceCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO catalog_resource (entry_id, resource_type, endpoint_url, method, request_schema,
                response_schema, query_text, file_path, format_spec, description)
            VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9::jsonb,$10) RETURNING resource_id
        """, body.entry_id, body.resource_type, body.endpoint_url, body.method,
            json.dumps(body.request_schema) if body.request_schema else None,
            json.dumps(body.response_schema) if body.response_schema else None,
            body.query_text, body.file_path,
            json.dumps(body.format_spec) if body.format_spec else None, body.description)
        return {"resource_id": row["resource_id"]}
    finally:
        await conn.close()


@router.delete("/resources/{resource_id}")
async def delete_resource(resource_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("UPDATE catalog_resource SET status='archived' WHERE resource_id=$1", resource_id)
        return {"ok": True}
    finally:
        await conn.close()
