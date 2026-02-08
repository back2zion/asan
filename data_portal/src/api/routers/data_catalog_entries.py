"""
데이터 카탈로그 - 엔트리 CRUD, 개요, 스키마 동기화
"""
import json
import hashlib
from typing import Optional

from fastapi import APIRouter, HTTPException

from .data_catalog_shared import (
    get_connection, _init, CatalogEntryCreate,
)

router = APIRouter()


# ══════════════════════════════════════════════
#  1. Catalog Entries
# ══════════════════════════════════════════════

@router.get("/entries")
async def list_entries(entry_type: Optional[str] = None, domain: Optional[str] = None,
                       access_level: Optional[str] = None, search: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if entry_type:
            params.append(entry_type)
            where.append(f"entry_type = ${len(params)}")
        if domain:
            params.append(domain)
            where.append(f"domain = ${len(params)}")
        if access_level:
            params.append(access_level)
            where.append(f"access_level = ${len(params)}")
        if search:
            params.append(f"%{search}%")
            n = len(params)
            where.append(f"(entry_name ILIKE ${n} OR business_name ILIKE ${n} OR description ILIKE ${n})")
        w = f"WHERE status='active' AND {' AND '.join(where)}" if where else "WHERE status='active'"
        rows = await conn.fetch(f"""
            SELECT e.*,
                (SELECT COUNT(*) FROM catalog_ownership o WHERE o.entry_id = e.entry_id AND o.status='active') AS share_count,
                (SELECT COUNT(*) FROM catalog_relation r WHERE r.source_entry_id = e.entry_id OR r.target_entry_id = e.entry_id) AS relation_count,
                (SELECT COUNT(*) FROM catalog_resource rs WHERE rs.entry_id = e.entry_id) AS resource_count
            FROM catalog_entry e {w} ORDER BY e.entry_type, e.entry_id
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.get("/entries/{entry_id}")
async def get_entry_detail(entry_id: int):
    """종합 정보: 엔트리 + 오너쉽 + 연관 + 리소스 + 이력 + 품질"""
    conn = await get_connection()
    try:
        await _init(conn)
        entry = await conn.fetchrow("SELECT * FROM catalog_entry WHERE entry_id=$1", entry_id)
        if not entry:
            raise HTTPException(404, "엔트리를 찾을 수 없습니다")
        data = dict(entry)

        data["ownerships"] = [dict(r) for r in await conn.fetch(
            "SELECT * FROM catalog_ownership WHERE entry_id=$1 AND status='active' ORDER BY owner_type", entry_id)]
        data["relations"] = [dict(r) for r in await conn.fetch("""
            SELECT r.*, s.entry_name AS source_name, s.business_name AS source_biz,
                   t.entry_name AS target_name, t.business_name AS target_biz
            FROM catalog_relation r
            JOIN catalog_entry s ON s.entry_id = r.source_entry_id
            JOIN catalog_entry t ON t.entry_id = r.target_entry_id
            WHERE r.source_entry_id=$1 OR r.target_entry_id=$1
            ORDER BY r.relation_type
        """, entry_id)]
        data["resources"] = [dict(r) for r in await conn.fetch(
            "SELECT * FROM catalog_resource WHERE entry_id=$1 ORDER BY resource_type", entry_id)]
        data["history"] = [dict(r) for r in await conn.fetch(
            "SELECT * FROM catalog_work_history WHERE entry_id=$1 ORDER BY created_at DESC LIMIT 20", entry_id)]
        return data
    finally:
        await conn.close()


@router.post("/entries")
async def create_entry(body: CatalogEntryCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        schema_json = json.dumps(body.schema_definition) if body.schema_definition else None
        schema_hash = hashlib.sha256(schema_json.encode()).hexdigest()[:16] if schema_json else None
        row = await conn.fetchrow("""
            INSERT INTO catalog_entry (entry_name, entry_type, physical_name, business_name, domain,
                description, source_system, owner_dept, owner_person, tags, access_level,
                usage_guide, policy_notes, connection_info, schema_definition, schema_hash)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14::jsonb,$15::jsonb,$16)
            RETURNING entry_id
        """, body.entry_name, body.entry_type, body.physical_name, body.business_name, body.domain,
            body.description, body.source_system, body.owner_dept, body.owner_person,
            json.dumps(body.tags), body.access_level, body.usage_guide, body.policy_notes,
            json.dumps(body.connection_info) if body.connection_info else None, schema_json, schema_hash)
        # Auto-add work history
        await conn.execute("""
            INSERT INTO catalog_work_history (entry_id, action_type, action_detail, actor)
            VALUES ($1, 'create', '{"auto":"catalog entry created"}'::jsonb, $2)
        """, row["entry_id"], body.owner_person or "system")
        return {"entry_id": row["entry_id"]}
    finally:
        await conn.close()


@router.put("/entries/{entry_id}")
async def update_entry(entry_id: int, body: CatalogEntryCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        schema_json = json.dumps(body.schema_definition) if body.schema_definition else None
        schema_hash = hashlib.sha256(schema_json.encode()).hexdigest()[:16] if schema_json else None
        await conn.execute("""
            UPDATE catalog_entry SET entry_name=$2, entry_type=$3, physical_name=$4, business_name=$5,
                domain=$6, description=$7, source_system=$8, owner_dept=$9, owner_person=$10,
                tags=$11::jsonb, access_level=$12, usage_guide=$13, policy_notes=$14,
                connection_info=$15::jsonb, schema_definition=$16::jsonb, schema_hash=$17, updated_at=NOW()
            WHERE entry_id=$1
        """, entry_id, body.entry_name, body.entry_type, body.physical_name, body.business_name,
            body.domain, body.description, body.source_system, body.owner_dept, body.owner_person,
            json.dumps(body.tags), body.access_level, body.usage_guide, body.policy_notes,
            json.dumps(body.connection_info) if body.connection_info else None, schema_json, schema_hash)
        return {"ok": True}
    finally:
        await conn.close()


@router.delete("/entries/{entry_id}")
async def archive_entry(entry_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("UPDATE catalog_entry SET status='archived', updated_at=NOW() WHERE entry_id=$1", entry_id)
        return {"ok": True}
    finally:
        await conn.close()


# ══════════════════════════════════════════════
#  Schema Sync
# ══════════════════════════════════════════════

@router.post("/sync/detect-changes")
async def detect_schema_changes():
    """OMOP CDM 실제 스키마와 카탈로그 비교 -> 변경 감지"""
    conn = await get_connection()
    try:
        await _init(conn)
        # Get actual tables from information_schema
        actual = await conn.fetch("""
            SELECT table_name, array_agg(column_name ORDER BY ordinal_position) AS columns
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name NOT LIKE 'catalog_%' AND table_name NOT LIKE 'meta_%'
                AND table_name NOT LIKE 'dm_%' AND table_name NOT LIKE 'etl_%' AND table_name NOT LIKE 'cdc_%'
                AND table_name NOT LIKE 'data_%' AND table_name NOT LIKE 'sensitivity_%'
                AND table_name NOT LIKE 'governance_%' AND table_name NOT LIKE 'deident_%'
                AND table_name NOT LIKE 'column_%' AND table_name NOT LIKE 'metadata_%'
            GROUP BY table_name ORDER BY table_name
        """)
        # Get catalog entries
        catalog = await conn.fetch(
            "SELECT entry_id, physical_name FROM catalog_entry WHERE entry_type='table' AND status='active' AND physical_name IS NOT NULL")
        catalog_map = {r["physical_name"]: r["entry_id"] for r in catalog}

        changes = []
        for tbl in actual:
            name = tbl["table_name"]
            if name not in catalog_map:
                changes.append({"type": "new_table", "table": name, "columns": tbl["columns"]})

        for phys, eid in catalog_map.items():
            found = [t for t in actual if t["table_name"] == phys]
            if not found:
                changes.append({"type": "missing_table", "table": phys, "entry_id": eid})

        # Log sync
        await conn.execute("""
            INSERT INTO catalog_sync_log (sync_type, changes_detected, status)
            VALUES ('schema_diff', $1::jsonb, 'success')
        """, json.dumps(changes))

        return {"changes": changes, "total": len(changes)}
    finally:
        await conn.close()


@router.get("/sync/logs")
async def list_sync_logs(limit: int = 20):
    conn = await get_connection()
    try:
        await _init(conn)
        rows = await conn.fetch("SELECT * FROM catalog_sync_log ORDER BY synced_at DESC LIMIT $1", limit)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


# ══════════════════════════════════════════════
#  Overview
# ══════════════════════════════════════════════

@router.get("/overview")
async def get_overview():
    conn = await get_connection()
    try:
        await _init(conn)
        total = await conn.fetchval("SELECT COUNT(*) FROM catalog_entry WHERE status='active'")
        by_type = await conn.fetch("SELECT entry_type, COUNT(*) cnt FROM catalog_entry WHERE status='active' GROUP BY entry_type")
        by_domain = await conn.fetch("SELECT domain, COUNT(*) cnt FROM catalog_entry WHERE status='active' GROUP BY domain")
        owners = await conn.fetchval("SELECT COUNT(DISTINCT dept) FROM catalog_ownership WHERE status='active'")
        shared = await conn.fetchval("SELECT COUNT(*) FROM catalog_ownership WHERE owner_type='shared' AND status='active'")
        relations = await conn.fetchval("SELECT COUNT(*) FROM catalog_relation")
        resources = await conn.fetchval("SELECT COUNT(*) FROM catalog_resource WHERE status='active'")
        history_count = await conn.fetchval("SELECT COUNT(*) FROM catalog_work_history")
        return {
            "total_entries": total,
            "by_type": {r["entry_type"]: r["cnt"] for r in by_type},
            "by_domain": {r["domain"]: r["cnt"] for r in by_domain},
            "owner_depts": owners,
            "shared_count": shared,
            "relations": relations,
            "resources": resources,
            "history_records": history_count,
        }
    finally:
        await conn.close()
