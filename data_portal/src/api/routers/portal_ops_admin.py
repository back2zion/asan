"""
DPR-005: 포털 운영 — 시스템 관리
공지사항, 메뉴 관리, 시스템 설정, 운영 개요
"""
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ._portal_ops_shared import (
    get_connection, portal_ops_init,
    AnnouncementCreate, AnnouncementUpdate,
    MenuItemCreate, MenuItemUpdate,
)

router = APIRouter(tags=["PortalOps-Admin"])


# ── Announcements ──

@router.post("/announcements")
async def create_announcement(body: AnnouncementCreate):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        row = await conn.fetchrow(
            "INSERT INTO po_announcement (title, content, ann_type, priority, is_pinned, creator, start_date, end_date) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7::timestamptz,$8::timestamptz) RETURNING ann_id, created_at",
            body.title, body.content, body.ann_type, body.priority,
            body.is_pinned, body.creator, body.start_date, body.end_date,
        )
        return {"ann_id": row["ann_id"], "created_at": row["created_at"].isoformat()}
    finally:
        await conn.close()


@router.get("/announcements")
async def list_announcements(
    status: Optional[str] = None,
    ann_type: Optional[str] = None,
    limit: int = Query(default=50, ge=1, le=200),
):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        conditions = []
        params = []
        idx = 1
        if status:
            conditions.append(f"status = ${idx}")
            params.append(status)
            idx += 1
        if ann_type:
            conditions.append(f"ann_type = ${idx}")
            params.append(ann_type)
            idx += 1
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        params.append(limit)
        rows = await conn.fetch(
            f"SELECT * FROM po_announcement {where} ORDER BY is_pinned DESC, created_at DESC LIMIT ${idx}",
            *params,
        )
        return [
            {
                "ann_id": r["ann_id"], "title": r["title"], "content": r["content"],
                "ann_type": r["ann_type"], "priority": r["priority"], "status": r["status"],
                "start_date": r["start_date"].isoformat() if r["start_date"] else None,
                "end_date": r["end_date"].isoformat() if r["end_date"] else None,
                "is_pinned": r["is_pinned"], "view_count": r["view_count"],
                "creator": r["creator"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.get("/announcements/active")
async def list_active_announcements():
    """활성 공지 (배너/팝업 포함)"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        rows = await conn.fetch(
            "SELECT * FROM po_announcement WHERE status = 'published' "
            "AND (end_date IS NULL OR end_date > NOW()) "
            "ORDER BY is_pinned DESC, priority DESC, created_at DESC LIMIT 20"
        )
        return [
            {
                "ann_id": r["ann_id"], "title": r["title"], "content": r["content"],
                "ann_type": r["ann_type"], "priority": r["priority"],
                "is_pinned": r["is_pinned"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.get("/announcements/{ann_id}")
async def get_announcement(ann_id: int):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        r = await conn.fetchrow("SELECT * FROM po_announcement WHERE ann_id = $1", ann_id)
        if not r:
            raise HTTPException(status_code=404, detail="공지를 찾을 수 없습니다")
        await conn.execute("UPDATE po_announcement SET view_count = view_count + 1 WHERE ann_id = $1", ann_id)
        return {
            "ann_id": r["ann_id"], "title": r["title"], "content": r["content"],
            "ann_type": r["ann_type"], "priority": r["priority"], "status": r["status"],
            "start_date": r["start_date"].isoformat() if r["start_date"] else None,
            "end_date": r["end_date"].isoformat() if r["end_date"] else None,
            "is_pinned": r["is_pinned"], "view_count": r["view_count"],
            "creator": r["creator"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
    finally:
        await conn.close()


@router.put("/announcements/{ann_id}")
async def update_announcement(ann_id: int, body: AnnouncementUpdate):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        existing = await conn.fetchrow("SELECT * FROM po_announcement WHERE ann_id = $1", ann_id)
        if not existing:
            raise HTTPException(status_code=404, detail="공지를 찾을 수 없습니다")
        await conn.execute(
            "UPDATE po_announcement SET title=COALESCE($2,title), content=COALESCE($3,content), "
            "ann_type=COALESCE($4,ann_type), priority=COALESCE($5,priority), status=COALESCE($6,status), "
            "start_date=COALESCE($7::timestamptz,start_date), end_date=COALESCE($8::timestamptz,end_date), "
            "is_pinned=COALESCE($9,is_pinned), updated_at=NOW() WHERE ann_id=$1",
            ann_id, body.title, body.content, body.ann_type, body.priority,
            body.status, body.start_date, body.end_date, body.is_pinned,
        )
        return {"updated": True, "ann_id": ann_id}
    finally:
        await conn.close()


@router.delete("/announcements/{ann_id}")
async def delete_announcement(ann_id: int):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        result = await conn.execute("DELETE FROM po_announcement WHERE ann_id = $1", ann_id)
        if "DELETE 0" in result:
            raise HTTPException(status_code=404, detail="공지를 찾을 수 없습니다")
        return {"deleted": True, "ann_id": ann_id}
    finally:
        await conn.close()


# ── Menu Management ──

@router.post("/menus")
async def create_menu_item(body: MenuItemCreate):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        row = await conn.fetchrow(
            "INSERT INTO po_menu_item (menu_key, label, icon, path, parent_key, sort_order, visible, roles) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb) RETURNING item_id",
            body.menu_key, body.label, body.icon, body.path, body.parent_key,
            body.sort_order, body.visible, json.dumps(body.roles),
        )
        return {"item_id": row["item_id"], "menu_key": body.menu_key}
    finally:
        await conn.close()


@router.get("/menus")
async def list_menu_items(role: Optional[str] = None):
    """메뉴 목록 (역할 필터 가능)"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        rows = await conn.fetch("SELECT * FROM po_menu_item ORDER BY sort_order")
        items = []
        for r in rows:
            roles = r["roles"] if isinstance(r["roles"], list) else json.loads(r["roles"]) if r["roles"] else []
            if role and role not in roles:
                continue
            items.append({
                "item_id": r["item_id"], "menu_key": r["menu_key"], "label": r["label"],
                "icon": r["icon"], "path": r["path"], "parent_key": r["parent_key"],
                "sort_order": r["sort_order"], "visible": r["visible"], "roles": roles,
            })
        return items
    finally:
        await conn.close()


@router.put("/menus/{menu_key}")
async def update_menu_item(menu_key: str, body: MenuItemUpdate):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        existing = await conn.fetchrow("SELECT * FROM po_menu_item WHERE menu_key = $1", menu_key)
        if not existing:
            raise HTTPException(status_code=404, detail="메뉴를 찾을 수 없습니다")
        await conn.execute(
            "UPDATE po_menu_item SET label=COALESCE($2,label), icon=COALESCE($3,icon), "
            "path=COALESCE($4,path), sort_order=COALESCE($5,sort_order), "
            "visible=COALESCE($6,visible), roles=COALESCE($7::jsonb,roles), "
            "updated_at=NOW() WHERE menu_key=$1",
            menu_key, body.label, body.icon, body.path, body.sort_order,
            body.visible, json.dumps(body.roles) if body.roles is not None else None,
        )
        return {"updated": True, "menu_key": menu_key}
    finally:
        await conn.close()


@router.delete("/menus/{menu_key}")
async def delete_menu_item(menu_key: str):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        result = await conn.execute("DELETE FROM po_menu_item WHERE menu_key = $1", menu_key)
        if "DELETE 0" in result:
            raise HTTPException(status_code=404, detail="메뉴를 찾을 수 없습니다")
        return {"deleted": True, "menu_key": menu_key}
    finally:
        await conn.close()


# ── System Settings ──

@router.get("/settings")
async def list_settings():
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        rows = await conn.fetch("SELECT * FROM po_system_setting ORDER BY setting_key")
        return [
            {
                "key": r["setting_key"], "value": r["setting_value"],
                "description": r["description"],
                "updated_by": r["updated_by"],
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.put("/settings/{key}")
async def update_setting(key: str, body: dict):
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        value = body.get("value")
        if value is None:
            raise HTTPException(status_code=400, detail="value 필드가 필요합니다")
        result = await conn.execute(
            "UPDATE po_system_setting SET setting_value=$2::jsonb, updated_by=$3, updated_at=NOW() WHERE setting_key=$1",
            key, json.dumps(value), body.get("updated_by", "admin"),
        )
        if "UPDATE 0" in result:
            # Insert if not exists
            await conn.execute(
                "INSERT INTO po_system_setting (setting_key, setting_value, description) VALUES ($1,$2::jsonb,$3)",
                key, json.dumps(value), body.get("description", ""),
            )
        return {"updated": True, "key": key}
    finally:
        await conn.close()


# ── Overview ──

@router.get("/overview")
async def portal_ops_overview():
    """포털 운영 전체 요약"""
    conn = await get_connection()
    try:
        await portal_ops_init(conn)
        log_count = await conn.fetchval("SELECT COUNT(*) FROM po_access_log")
        today_logs = await conn.fetchval("SELECT COUNT(*) FROM po_access_log WHERE created_at >= CURRENT_DATE")
        active_alerts = await conn.fetchval("SELECT COUNT(*) FROM po_alert WHERE status = 'active'")
        critical_alerts = await conn.fetchval("SELECT COUNT(*) FROM po_alert WHERE status = 'active' AND severity IN ('error','critical')")
        ann_count = await conn.fetchval("SELECT COUNT(*) FROM po_announcement WHERE status = 'published'")
        menu_count = await conn.fetchval("SELECT COUNT(*) FROM po_menu_item WHERE visible = TRUE")
        quality_rules = await conn.fetchval("SELECT COUNT(*) FROM po_quality_rule WHERE status = 'active'")
        quality_passed = await conn.fetchval(
            "SELECT COUNT(*) FROM po_quality_rule WHERE status='active' AND last_score IS NOT NULL AND last_score >= threshold"
        )

        unique_users = await conn.fetchval("SELECT COUNT(DISTINCT user_id) FROM po_access_log")

        return {
            "access_logs": {"total": log_count, "today": today_logs, "unique_users": unique_users},
            "alerts": {"active": active_alerts, "critical": critical_alerts},
            "announcements": {"published": ann_count},
            "menus": {"visible": menu_count},
            "quality": {"total_rules": quality_rules, "passed": quality_passed or 0},
        }
    finally:
        await conn.close()
