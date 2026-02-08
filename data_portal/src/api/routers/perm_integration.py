"""
DGR-007 권한 관리 — EAM 연계, EDW 이관, 감사 로그, 개요
"""
import json as _json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from .permission_mgmt_shared import (
    EamMappingCreate, EdwMigrationCreate,
    get_conn, ensure_tables, ensure_seed,
)

router = APIRouter()


# ═══════════════════════════════════════════════════════════
# 5. EAM Mapping (AMIS 3.0 역할 연계)
# ═══════════════════════════════════════════════════════════

@router.get("/eam-mappings")
async def get_eam_mappings(sync_status: Optional[str] = None):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM perm_eam_mapping WHERE 1=1"
        args = []
        if sync_status:
            args.append(sync_status); q += f" AND sync_status=${len(args)}"
        q += " ORDER BY eam_role_code"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/eam-mappings")
async def create_eam_mapping(body: EamMappingCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO perm_eam_mapping (eam_role_code, eam_role_name, eam_department, platform_role_id, mapping_rule, auto_sync)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING *
        """, body.eam_role_code, body.eam_role_name, body.eam_department, body.platform_role_id,
            _json.dumps(body.mapping_rule) if body.mapping_rule else None, body.auto_sync)
        return dict(row)
    finally:
        await conn.close()


@router.put("/eam-mappings/{mapping_id}")
async def update_eam_mapping(mapping_id: int, body: EamMappingCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE perm_eam_mapping SET eam_role_code=$2, eam_role_name=$3, eam_department=$4,
            platform_role_id=$5, mapping_rule=$6, auto_sync=$7, updated_at=NOW()
            WHERE mapping_id=$1 RETURNING *
        """, mapping_id, body.eam_role_code, body.eam_role_name, body.eam_department,
            body.platform_role_id, _json.dumps(body.mapping_rule) if body.mapping_rule else None, body.auto_sync)
        if not row:
            raise HTTPException(404, "Mapping not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/eam-mappings/{mapping_id}")
async def delete_eam_mapping(mapping_id: int):
    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM perm_eam_mapping WHERE mapping_id=$1", mapping_id)
        return {"deleted": True}
    finally:
        await conn.close()


@router.post("/eam-mappings/sync-all")
async def sync_eam_roles():
    """전체 EAM 역할 동기화 (시뮬레이션)"""
    conn = await get_conn()
    try:
        mappings = await conn.fetch("SELECT * FROM perm_eam_mapping WHERE auto_sync=TRUE")
        results = []
        for m in mappings:
            rule = _json.loads(m["mapping_rule"]) if m["mapping_rule"] and isinstance(m["mapping_rule"], str) else (m["mapping_rule"] or {})
            sync_params = rule.get("sync_params", {})
            # Simulate: check if role assignment exists with these params
            existing = await conn.fetchval("""
                SELECT COUNT(*) FROM perm_role_assignment
                WHERE role_id=$1 AND assignment_type='primary'
            """, m["platform_role_id"])
            await conn.execute("""
                UPDATE perm_eam_mapping SET sync_status='synced', last_synced_at=NOW(),
                sync_count=sync_count+1, updated_at=NOW() WHERE mapping_id=$1
            """, m["mapping_id"])
            results.append({
                "eam_code": m["eam_role_code"],
                "eam_name": m["eam_role_name"],
                "platform_role": m["platform_role_id"],
                "existing_assignments": existing,
                "sync_params": sync_params,
                "status": "synced",
            })
        await conn.execute("""
            INSERT INTO perm_audit (action, actor, target_type, target_id, details)
            VALUES ('EAM_SYNC', 'system', 'eam_mapping', 'all', $1)
        """, _json.dumps({"synced_count": len(results)}))
        return {"synced": len(results), "results": results}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 6. EDW Migration (EDW 권한 이관)
# ═══════════════════════════════════════════════════════════

@router.get("/edw-migrations")
async def get_edw_migrations(status: Optional[str] = None, edw_source: Optional[str] = None):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT m.*, d.name as dataset_name FROM perm_edw_migration m LEFT JOIN perm_dataset d ON m.platform_dataset_id=d.dataset_id WHERE 1=1"
        args = []
        if status:
            args.append(status); q += f" AND m.status=${len(args)}"
        if edw_source:
            args.append(edw_source); q += f" AND m.edw_source=${len(args)}"
        q += " ORDER BY m.created_at DESC"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/edw-migrations")
async def create_edw_migration(body: EdwMigrationCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO perm_edw_migration (edw_source, edw_permission_type, edw_object_name, edw_grantee,
            platform_dataset_id, platform_role_id, platform_grant_type)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *
        """, body.edw_source, body.edw_permission_type, body.edw_object_name, body.edw_grantee,
            body.platform_dataset_id, body.platform_role_id, body.platform_grant_type)
        return dict(row)
    finally:
        await conn.close()


@router.put("/edw-migrations/{migration_id}/migrate")
async def execute_edw_migration(migration_id: int):
    """EDW 권한 이관 실행 — grant 레코드를 자동 생성"""
    conn = await get_conn()
    try:
        m = await conn.fetchrow("SELECT * FROM perm_edw_migration WHERE migration_id=$1", migration_id)
        if not m:
            raise HTTPException(404, "Migration record not found")
        if m["status"] not in ("pending", "mapped"):
            raise HTTPException(400, f"Cannot migrate from status: {m['status']}")
        if not m["platform_dataset_id"] or not m["platform_role_id"]:
            raise HTTPException(400, "Dataset and role must be mapped before migration")
        # Create grant
        grant = await conn.fetchrow("""
            INSERT INTO perm_grant (dataset_id, grant_type, grantee_type, grantee_id, source_role_id, granted_by)
            VALUES ($1, $2, 'role', $3, $4, 'edw_migration') RETURNING grant_id
        """, m["platform_dataset_id"], m["platform_grant_type"] or "select",
            str(m["platform_role_id"]), m["platform_role_id"])
        await conn.execute("""
            UPDATE perm_edw_migration SET status='migrated', migrated_at=NOW(),
            migration_note=COALESCE(migration_note,'') || ' [자동이관 grant_id=' || $2 || ']'
            WHERE migration_id=$1
        """, migration_id, str(grant["grant_id"]))
        await conn.execute("""
            INSERT INTO perm_audit (action, actor, target_type, target_id, details)
            VALUES ('EDW_MIGRATE', 'system', 'edw_permission', $1, $2)
        """, str(migration_id), _json.dumps({
            "edw_object": m["edw_object_name"],
            "grant_id": grant["grant_id"],
            "status": "migrated",
        }))
        return {"migrated": True, "grant_id": grant["grant_id"]}
    finally:
        await conn.close()


@router.put("/edw-migrations/{migration_id}/verify")
async def verify_edw_migration(migration_id: int):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE perm_edw_migration SET status='verified'
            WHERE migration_id=$1 AND status='migrated' RETURNING *
        """, migration_id)
        if not row:
            raise HTTPException(400, "Only migrated records can be verified")
        return dict(row)
    finally:
        await conn.close()


@router.put("/edw-migrations/{migration_id}/skip")
async def skip_edw_migration(migration_id: int, reason: str = "건너뜀"):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE perm_edw_migration SET status='skipped', migration_note=$2
            WHERE migration_id=$1 RETURNING *
        """, migration_id, reason)
        if not row:
            raise HTTPException(404, "Migration not found")
        return dict(row)
    finally:
        await conn.close()


@router.get("/edw-migrations/stats")
async def get_edw_migration_stats():
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        stats = {}
        stats["total"] = await conn.fetchval("SELECT COUNT(*) FROM perm_edw_migration")
        stats["by_status"] = [dict(r) for r in await conn.fetch(
            "SELECT status, COUNT(*) as cnt FROM perm_edw_migration GROUP BY status ORDER BY cnt DESC")]
        stats["by_source"] = [dict(r) for r in await conn.fetch(
            "SELECT edw_source, COUNT(*) as cnt FROM perm_edw_migration GROUP BY edw_source ORDER BY cnt DESC")]
        stats["by_type"] = [dict(r) for r in await conn.fetch(
            "SELECT edw_permission_type, COUNT(*) as cnt FROM perm_edw_migration GROUP BY edw_permission_type")]
        return stats
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 7. Audit Log (권한 감사)
# ═══════════════════════════════════════════════════════════

@router.get("/audit")
async def get_permission_audit(
    action: Optional[str] = None,
    actor: Optional[str] = None,
    limit: int = Query(default=50, le=200),
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM perm_audit WHERE 1=1"
        args = []
        if action:
            args.append(action); q += f" AND action=${len(args)}"
        if actor:
            args.append(actor); q += f" AND actor=${len(args)}"
        args.append(limit); q += f" ORDER BY created_at DESC LIMIT ${len(args)}"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.get("/audit/stats")
async def get_audit_stats():
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        stats = {}
        stats["total"] = await conn.fetchval("SELECT COUNT(*) FROM perm_audit")
        stats["by_action"] = [dict(r) for r in await conn.fetch(
            "SELECT action, COUNT(*) as cnt FROM perm_audit GROUP BY action ORDER BY cnt DESC")]
        stats["by_actor"] = [dict(r) for r in await conn.fetch(
            "SELECT actor, COUNT(*) as cnt FROM perm_audit GROUP BY actor ORDER BY cnt DESC")]
        return stats
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 8. Overview
# ═══════════════════════════════════════════════════════════

@router.get("/overview")
async def get_permission_overview():
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        ov = {}
        ov["datasets"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM perm_dataset WHERE active=TRUE"),
            "by_type": [dict(r) for r in await conn.fetch(
                "SELECT dataset_type, COUNT(*) as cnt FROM perm_dataset WHERE active=TRUE GROUP BY dataset_type")],
        }
        ov["grants"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM perm_grant WHERE active=TRUE"),
            "by_type": [dict(r) for r in await conn.fetch(
                "SELECT grant_type, COUNT(*) as cnt FROM perm_grant WHERE active=TRUE GROUP BY grant_type ORDER BY cnt DESC")],
        }
        ov["role_assignments"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM perm_role_assignment WHERE active=TRUE"),
            "multi_role_users": await conn.fetchval(
                "SELECT COUNT(DISTINCT user_id) FROM perm_role_assignment WHERE active=TRUE GROUP BY user_id HAVING COUNT(*) > 1"),
            "by_type": [dict(r) for r in await conn.fetch(
                "SELECT assignment_type, COUNT(*) as cnt FROM perm_role_assignment WHERE active=TRUE GROUP BY assignment_type")],
        }
        ov["role_params"] = {
            "total_defs": await conn.fetchval("SELECT COUNT(*) FROM perm_role_param_def"),
        }
        ov["eam_mappings"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM perm_eam_mapping"),
            "synced": await conn.fetchval("SELECT COUNT(*) FROM perm_eam_mapping WHERE sync_status='synced'"),
        }
        ov["edw_migration"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM perm_edw_migration"),
            "migrated": await conn.fetchval("SELECT COUNT(*) FROM perm_edw_migration WHERE status='migrated'"),
            "pending": await conn.fetchval("SELECT COUNT(*) FROM perm_edw_migration WHERE status='pending'"),
        }
        ov["audit"] = {
            "total": await conn.fetchval("SELECT COUNT(*) FROM perm_audit"),
        }
        return ov
    finally:
        await conn.close()
