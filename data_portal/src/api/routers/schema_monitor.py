"""
DIR-003: 원천데이터 변화 대응 — 스키마 변경 탐지 & CDC 관리
- Section A: 스키마 변경 탐지 & 로깅
- Section B: CDC 스트리밍 관리
- Section C: 자동 반영 정책
"""
import json
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query

from ._schema_monitor_helpers import (
    get_connection,
    CDCSwitchMode, CDCResetWatermark, PolicyUpdate,
    _ensure_schema_change_log, _ensure_cdc_table_config, _ensure_schema_auto_policy,
    _capture_current_schema, _compute_diff, _get_policies_map,
    CDC_CLINICAL_TABLES,
)

router = APIRouter(prefix="/schema-monitor", tags=["SchemaMonitor"])


# ── Section A: 스키마 변경 엔드포인트 ──

@router.post("/snapshot")
async def capture_snapshot(source_label: Optional[str] = Query(None)):
    """스키마 스냅샷 캡처 + diff 산출 + 저장"""
    conn = await get_connection()
    try:
        await _ensure_schema_change_log(conn)
        policies = await _get_policies_map(conn)

        current_schema = await _capture_current_schema(conn)

        # 이전 스냅샷 조회
        prev_row = await conn.fetchrow("""
            SELECT snapshot_version, snapshot_data
            FROM schema_change_log
            ORDER BY id DESC LIMIT 1
        """)

        new_version = (prev_row["snapshot_version"] + 1) if prev_row else 1

        # Diff 산출
        detected_changes = []
        if prev_row:
            prev_data = prev_row["snapshot_data"]
            if isinstance(prev_data, str):
                prev_data = json.loads(prev_data)
            detected_changes = _compute_diff(prev_data, current_schema, policies)

        # 변경 요약
        change_summary = {
            "total_changes": len(detected_changes),
            "table_added": sum(1 for c in detected_changes if c["type"] == "table_added"),
            "table_removed": sum(1 for c in detected_changes if c["type"] == "table_removed"),
            "column_added": sum(1 for c in detected_changes if c["type"] == "column_added"),
            "column_removed": sum(1 for c in detected_changes if c["type"] == "column_removed"),
            "type_changed": sum(1 for c in detected_changes if c["type"] == "column_type_changed"),
            "nullable_changed": sum(1 for c in detected_changes if c["type"] == "column_nullable_changed"),
            "high_risk": sum(1 for c in detected_changes if c["risk_level"] == "high"),
            "medium_risk": sum(1 for c in detected_changes if c["risk_level"] == "medium"),
            "low_risk": sum(1 for c in detected_changes if c["risk_level"] == "low"),
            "auto_applicable": sum(1 for c in detected_changes if c.get("auto_applicable")),
        }

        label = source_label or f"v{new_version} snapshot"

        await conn.execute("""
            INSERT INTO schema_change_log (snapshot_version, snapshot_data, detected_changes, change_summary, source_label)
            VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb, $5)
        """, new_version, json.dumps(current_schema), json.dumps(detected_changes),
            json.dumps(change_summary), label)

        return {
            "success": True,
            "version": new_version,
            "label": label,
            "total_tables": len(current_schema),
            "total_columns": sum(len(t["columns"]) for t in current_schema.values()),
            "changes": change_summary,
            "detected_changes": detected_changes,
        }
    finally:
        await conn.close()


@router.get("/diff")
async def get_current_diff():
    """현재 vs 마지막 스냅샷 diff (dry-run, 저장하지 않음)"""
    conn = await get_connection()
    try:
        await _ensure_schema_change_log(conn)
        policies = await _get_policies_map(conn)

        current_schema = await _capture_current_schema(conn)

        prev_row = await conn.fetchrow("""
            SELECT snapshot_version, snapshot_data
            FROM schema_change_log
            ORDER BY id DESC LIMIT 1
        """)

        if not prev_row:
            return {
                "has_previous": False,
                "message": "이전 스냅샷이 없습니다. 먼저 스냅샷을 캡처하세요.",
                "current_tables": len(current_schema),
                "current_columns": sum(len(t["columns"]) for t in current_schema.values()),
                "changes": [],
            }

        prev_data = prev_row["snapshot_data"]
        if isinstance(prev_data, str):
            prev_data = json.loads(prev_data)

        changes = _compute_diff(prev_data, current_schema, policies)

        return {
            "has_previous": True,
            "previous_version": prev_row["snapshot_version"],
            "current_tables": len(current_schema),
            "current_columns": sum(len(t["columns"]) for t in current_schema.values()),
            "total_changes": len(changes),
            "changes": changes,
            "summary": {
                "high_risk": sum(1 for c in changes if c["risk_level"] == "high"),
                "medium_risk": sum(1 for c in changes if c["risk_level"] == "medium"),
                "low_risk": sum(1 for c in changes if c["risk_level"] == "low"),
                "auto_applicable": sum(1 for c in changes if c.get("auto_applicable")),
            },
        }
    finally:
        await conn.close()


@router.get("/history")
async def get_history(limit: int = Query(20, ge=1, le=100)):
    """스키마 변경 이력 목록"""
    conn = await get_connection()
    try:
        await _ensure_schema_change_log(conn)
        rows = await conn.fetch("""
            SELECT id, snapshot_version, change_summary, source_label, created_at
            FROM schema_change_log
            ORDER BY id DESC
            LIMIT $1
        """, limit)

        return [
            {
                "id": r["id"],
                "version": r["snapshot_version"],
                "summary": r["change_summary"] if isinstance(r["change_summary"], dict) else json.loads(r["change_summary"]) if r["change_summary"] else None,
                "label": r["source_label"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]
    finally:
        await conn.close()


@router.get("/history/{log_id}")
async def get_history_detail(log_id: int):
    """특정 이력 상세 (full diff)"""
    conn = await get_connection()
    try:
        await _ensure_schema_change_log(conn)
        row = await conn.fetchrow("""
            SELECT id, snapshot_version, snapshot_data, detected_changes, change_summary, source_label, created_at
            FROM schema_change_log
            WHERE id = $1
        """, log_id)

        if not row:
            raise HTTPException(status_code=404, detail="이력을 찾을 수 없습니다")

        snapshot_data = row["snapshot_data"]
        if isinstance(snapshot_data, str):
            snapshot_data = json.loads(snapshot_data)

        detected = row["detected_changes"]
        if isinstance(detected, str):
            detected = json.loads(detected)

        summary = row["change_summary"]
        if isinstance(summary, str):
            summary = json.loads(summary)

        return {
            "id": row["id"],
            "version": row["snapshot_version"],
            "label": row["source_label"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "total_tables": len(snapshot_data) if snapshot_data else 0,
            "total_columns": sum(len(t.get("columns", {})) for t in snapshot_data.values()) if snapshot_data else 0,
            "changes": detected or [],
            "summary": summary,
            "tables": [
                {
                    "table_name": tname,
                    "row_count": tdata.get("row_count", 0),
                    "column_count": len(tdata.get("columns", {})),
                    "columns": [
                        {"name": cname, **cdata}
                        for cname, cdata in tdata.get("columns", {}).items()
                    ],
                }
                for tname, tdata in (snapshot_data or {}).items()
            ],
        }
    finally:
        await conn.close()


# ── Section B: CDC 스트리밍 관리 ──

@router.get("/cdc/status")
async def get_cdc_status():
    """CDC 워터마크 + 모드 현황"""
    conn = await get_connection()
    try:
        await _ensure_cdc_table_config(conn)

        # _etl_watermarks 존재 확인
        wm_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = '_etl_watermarks'
            )
        """)

        # cdc_table_config에 시드 데이터 삽입 (비어있으면)
        cfg_count = await conn.fetchval("SELECT COUNT(*) FROM cdc_table_config")
        if cfg_count == 0:
            for tname, mode, enabled, threshold, interval in CDC_CLINICAL_TABLES:
                await conn.execute("""
                    INSERT INTO cdc_table_config (table_name, mode, enabled, batch_threshold, sync_interval_minutes)
                    VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING
                """, tname, mode, enabled, threshold, interval)

        # CDC 설정 조회
        configs = await conn.fetch("SELECT * FROM cdc_table_config ORDER BY table_name")

        # 워터마크 데이터 조회
        watermarks = {}
        if wm_exists:
            wm_rows = await conn.fetch("SELECT * FROM _etl_watermarks")
            for w in wm_rows:
                watermarks[w["table_name"]] = {
                    "watermark_id": w.get("watermark_id"),
                    "last_sync": w.get("last_sync_at"),
                }

        # 테이블별 max PK 조회 (pending rows 계산용)
        results = []
        streaming_count = 0
        batch_count = 0
        disabled_count = 0
        total_pending = 0

        for cfg in configs:
            tname = cfg["table_name"]
            mode = cfg["mode"]

            if mode == "streaming":
                streaming_count += 1
            elif mode == "batch":
                batch_count += 1
            else:
                disabled_count += 1

            # row count from pg_stat
            row_count = await conn.fetchval("""
                SELECT COALESCE(n_live_tup, 0)
                FROM pg_stat_user_tables
                WHERE schemaname = 'public' AND relname = $1
            """, tname) or 0

            wm = watermarks.get(tname, {})
            wm_id = wm.get("watermark_id", 0) or 0
            pending = max(0, row_count - wm_id) if wm_id > 0 else 0
            total_pending += pending

            lag_minutes = None
            last_sync = wm.get("last_sync")
            if last_sync:
                try:
                    if isinstance(last_sync, datetime):
                        lag_minutes = int((datetime.now() - last_sync).total_seconds() / 60)
                    elif isinstance(last_sync, str):
                        dt = datetime.fromisoformat(last_sync.replace("Z", "+00:00"))
                        lag_minutes = int((datetime.now() - dt.replace(tzinfo=None)).total_seconds() / 60)
                except Exception:
                    pass

            results.append({
                "table_name": tname,
                "mode": mode,
                "enabled": cfg["enabled"],
                "batch_threshold": cfg["batch_threshold"],
                "sync_interval_minutes": cfg["sync_interval_minutes"],
                "row_count": row_count,
                "watermark_id": wm_id,
                "pending_rows": pending,
                "lag_minutes": lag_minutes,
                "last_sync": last_sync.isoformat() if isinstance(last_sync, datetime) else last_sync,
                "last_mode_change": cfg["last_mode_change"].isoformat() if cfg["last_mode_change"] else None,
                "updated_at": cfg["updated_at"].isoformat() if cfg["updated_at"] else None,
            })

        return {
            "has_watermarks": wm_exists,
            "summary": {
                "total_tables": len(results),
                "streaming": streaming_count,
                "batch": batch_count,
                "disabled": disabled_count,
                "total_pending_rows": total_pending,
            },
            "tables": results,
        }
    finally:
        await conn.close()


@router.post("/cdc/switch-mode")
async def switch_cdc_mode(body: CDCSwitchMode):
    """streaming/batch/disabled 전환"""
    conn = await get_connection()
    try:
        await _ensure_cdc_table_config(conn)
        result = await conn.execute("""
            UPDATE cdc_table_config
            SET mode = $1, enabled = $2, last_mode_change = NOW(), updated_at = NOW()
            WHERE table_name = $3
        """, body.mode, body.mode != "disabled", body.table_name)

        if result == "UPDATE 0":
            # 새 행 삽입
            await conn.execute("""
                INSERT INTO cdc_table_config (table_name, mode, enabled)
                VALUES ($1, $2, $3)
            """, body.table_name, body.mode, body.mode != "disabled")

        return {
            "success": True,
            "message": f"{body.table_name} → {body.mode} 전환 완료",
            "table_name": body.table_name,
            "mode": body.mode,
        }
    finally:
        await conn.close()


@router.post("/cdc/reset-watermark")
async def reset_watermark(body: CDCResetWatermark):
    """워터마크 리셋 (벌크 재적재용)"""
    conn = await get_connection()
    try:
        wm_exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = '_etl_watermarks'
            )
        """)

        if wm_exists:
            result = await conn.execute("""
                UPDATE _etl_watermarks
                SET watermark_id = 0, last_sync_at = NOW()
                WHERE table_name = $1
            """, body.table_name)
            if result == "UPDATE 0":
                return {
                    "success": True,
                    "message": f"{body.table_name}: 워터마크 테이블에 항목 없음 (이미 초기 상태)",
                }
            return {
                "success": True,
                "message": f"{body.table_name} 워터마크 리셋 완료 (0으로 초기화)",
            }
        else:
            return {
                "success": True,
                "message": "_etl_watermarks 테이블이 없습니다 (Airflow DAG 미설정 상태)",
            }
    finally:
        await conn.close()


# ── Section C: 자동 반영 정책 ──

@router.get("/policies")
async def get_policies():
    """자동 반영 정책 조회 (7개 시드)"""
    conn = await get_connection()
    try:
        await _ensure_schema_auto_policy(conn)
        rows = await conn.fetch("SELECT * FROM schema_auto_policy ORDER BY policy_key")

        policies = []
        for r in rows:
            val = r["policy_value"]
            if isinstance(val, str):
                val = json.loads(val)
            policies.append({
                "key": r["policy_key"],
                "value": val,
                "description": r["description"],
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            })

        # 통계 산출
        auto_count = sum(1 for p in policies if p["value"].get("action") == "auto")
        manual_count = sum(1 for p in policies if p["value"].get("action") == "manual")
        config_count = len(policies) - auto_count - manual_count

        return {
            "policies": policies,
            "summary": {
                "total": len(policies),
                "auto": auto_count,
                "manual": manual_count,
                "config": config_count,
            },
        }
    finally:
        await conn.close()


@router.put("/policies/{key}")
async def update_policy(key: str, body: PolicyUpdate):
    """정책 수정"""
    conn = await get_connection()
    try:
        await _ensure_schema_auto_policy(conn)
        result = await conn.execute("""
            UPDATE schema_auto_policy
            SET policy_value = $1::jsonb, description = COALESCE($2, description), updated_at = NOW()
            WHERE policy_key = $3
        """, json.dumps(body.policy_value), body.description, key)

        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail=f"정책 '{key}'를 찾을 수 없습니다")

        return {"success": True, "message": f"정책 '{key}' 수정 완료", "key": key}
    finally:
        await conn.close()


@router.get("/policies/audit")
async def get_policy_audit():
    """자동/수동 적용 감사 통계"""
    conn = await get_connection()
    try:
        await _ensure_schema_change_log(conn)
        await _ensure_schema_auto_policy(conn)

        # 전체 변경 이력에서 자동/수동 적용 통계 산출
        rows = await conn.fetch("""
            SELECT detected_changes, change_summary, created_at
            FROM schema_change_log
            WHERE detected_changes IS NOT NULL
            ORDER BY id DESC
            LIMIT 50
        """)

        total_changes = 0
        auto_applied = 0
        manual_required = 0
        high_risk = 0
        by_type: Dict[str, int] = {}

        for r in rows:
            changes = r["detected_changes"]
            if isinstance(changes, str):
                changes = json.loads(changes)
            if not changes:
                continue
            for c in changes:
                total_changes += 1
                ctype = c.get("type", "unknown")
                by_type[ctype] = by_type.get(ctype, 0) + 1
                if c.get("auto_applicable"):
                    auto_applied += 1
                else:
                    manual_required += 1
                if c.get("risk_level") == "high":
                    high_risk += 1

        return {
            "total_changes": total_changes,
            "auto_applied": auto_applied,
            "manual_required": manual_required,
            "high_risk": high_risk,
            "by_type": by_type,
            "audit_period": {
                "entries_scanned": len(rows),
                "latest": rows[0]["created_at"].isoformat() if rows else None,
                "oldest": rows[-1]["created_at"].isoformat() if rows else None,
            },
        }
    finally:
        await conn.close()
