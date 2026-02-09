"""
DIR-003: 원천데이터 변화 대응 — 스키마 변경 탐지 & CDC 관리
- Section A: 스키마 변경 탐지 & 로깅
- Section B: CDC 스트리밍 관리
- Section C: 자동 반영 정책
"""
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import asyncpg

router = APIRouter(prefix="/schema-monitor", tags=["SchemaMonitor"])

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}


async def get_connection():
    try:
        return await asyncpg.connect(**OMOP_DB_CONFIG)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")


# ── Pydantic 모델 ──

class CDCSwitchMode(BaseModel):
    table_name: str = Field(..., max_length=100)
    mode: str = Field(..., pattern=r"^(streaming|batch|disabled)$")

class CDCResetWatermark(BaseModel):
    table_name: str = Field(..., max_length=100)

class PolicyUpdate(BaseModel):
    policy_value: Dict[str, Any]
    description: Optional[str] = Field(None, max_length=200)


# ── 테이블 자동 생성 ──

async def _ensure_schema_change_log(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_change_log (
            id SERIAL PRIMARY KEY,
            snapshot_version INTEGER NOT NULL,
            snapshot_data JSONB NOT NULL,
            detected_changes JSONB,
            change_summary JSONB,
            source_label VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)


async def _ensure_cdc_table_config(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cdc_table_config (
            table_name VARCHAR(100) PRIMARY KEY,
            mode VARCHAR(20) NOT NULL DEFAULT 'streaming',
            enabled BOOLEAN DEFAULT TRUE,
            batch_threshold INTEGER DEFAULT 100000,
            sync_interval_minutes INTEGER DEFAULT 30,
            last_mode_change TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)


async def _ensure_schema_auto_policy(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_auto_policy (
            policy_key VARCHAR(50) PRIMARY KEY,
            policy_value JSONB NOT NULL,
            description VARCHAR(200),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    count = await conn.fetchval("SELECT COUNT(*) FROM schema_auto_policy")
    if count == 0:
        seeds = [
            ("column_addition", {"action": "auto", "notify": True}, "컬럼 추가 시 자동 반영"),
            ("column_removal", {"action": "manual", "notify": True, "require_approval": True}, "컬럼 삭제 시 수동 확인 필요"),
            ("type_widening", {"action": "auto", "notify": True}, "타입 확장 (varchar 길이 증가 등) 자동 반영"),
            ("type_narrowing", {"action": "manual", "notify": True, "require_approval": True}, "타입 축소 시 수동 확인 필요"),
            ("table_addition", {"action": "manual", "notify": True}, "테이블 추가 시 수동 확인"),
            ("batch_switch_threshold", {"threshold": 100000, "auto_switch": True}, "배치 전환 임계값 (pending rows)"),
            ("notification", {"channel": "dashboard", "email": False, "slack": False}, "알림 채널 설정"),
        ]
        for key, value, desc in seeds:
            await conn.execute("""
                INSERT INTO schema_auto_policy (policy_key, policy_value, description)
                VALUES ($1, $2::jsonb, $3) ON CONFLICT DO NOTHING
            """, key, json.dumps(value), desc)


# ── 핵심 로직 ──

async def _capture_current_schema(conn) -> Dict[str, Any]:
    """information_schema에서 현재 스키마 스냅샷 캡처"""
    rows = await conn.fetch("""
        SELECT c.table_name, c.column_name, c.data_type,
               c.is_nullable, c.ordinal_position,
               c.character_maximum_length, c.numeric_precision
        FROM information_schema.columns c
        WHERE c.table_schema = 'public'
        ORDER BY c.table_name, c.ordinal_position
    """)

    row_counts = await conn.fetch("""
        SELECT relname AS table_name, n_live_tup AS row_count
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
    """)
    count_map = {r["table_name"]: r["row_count"] for r in row_counts}

    tables: Dict[str, Any] = {}
    for r in rows:
        tname = r["table_name"]
        if tname not in tables:
            tables[tname] = {
                "table_name": tname,
                "row_count": count_map.get(tname, 0),
                "columns": {},
            }
        tables[tname]["columns"][r["column_name"]] = {
            "data_type": r["data_type"],
            "is_nullable": r["is_nullable"],
            "ordinal_position": r["ordinal_position"],
            "char_max_length": r["character_maximum_length"],
            "numeric_precision": r["numeric_precision"],
        }

    return tables


def _compute_diff(prev: Dict[str, Any], curr: Dict[str, Any], policies: Dict[str, Any]) -> List[Dict[str, Any]]:
    """이전 스냅샷 vs 현재 스냅샷 diff 산출"""
    changes = []

    prev_tables = set(prev.keys())
    curr_tables = set(curr.keys())

    # 테이블 추가
    for t in curr_tables - prev_tables:
        col_count = len(curr[t].get("columns", {}))
        changes.append({
            "type": "table_added",
            "table": t,
            "column": None,
            "detail": f"{col_count}개 컬럼",
            "risk_level": "medium",
            "auto_applicable": policies.get("table_addition", {}).get("action") == "auto",
        })

    # 테이블 삭제
    for t in prev_tables - curr_tables:
        changes.append({
            "type": "table_removed",
            "table": t,
            "column": None,
            "detail": "테이블 삭제됨",
            "risk_level": "high",
            "auto_applicable": False,
        })

    # 공통 테이블 — 컬럼 비교
    for t in prev_tables & curr_tables:
        prev_cols = set(prev[t].get("columns", {}).keys())
        curr_cols = set(curr[t].get("columns", {}).keys())

        # 컬럼 추가
        for c in curr_cols - prev_cols:
            changes.append({
                "type": "column_added",
                "table": t,
                "column": c,
                "detail": f"타입: {curr[t]['columns'][c]['data_type']}",
                "risk_level": "low",
                "auto_applicable": policies.get("column_addition", {}).get("action") == "auto",
            })

        # 컬럼 삭제
        for c in prev_cols - curr_cols:
            changes.append({
                "type": "column_removed",
                "table": t,
                "column": c,
                "detail": f"이전 타입: {prev[t]['columns'][c]['data_type']}",
                "risk_level": "high",
                "auto_applicable": policies.get("column_removal", {}).get("action") == "auto",
            })

        # 공통 컬럼 — 타입/nullable 변경
        for c in prev_cols & curr_cols:
            pc = prev[t]["columns"][c]
            cc = curr[t]["columns"][c]

            if pc["data_type"] != cc["data_type"]:
                # 타입 확장 vs 축소 판별 (간단 휴리스틱)
                is_widening = (
                    cc.get("char_max_length") is not None
                    and pc.get("char_max_length") is not None
                    and cc["char_max_length"] > pc["char_max_length"]
                )
                risk = "low" if is_widening else "high"
                policy_key = "type_widening" if is_widening else "type_narrowing"
                changes.append({
                    "type": "column_type_changed",
                    "table": t,
                    "column": c,
                    "detail": f"{pc['data_type']} → {cc['data_type']}",
                    "risk_level": risk,
                    "auto_applicable": policies.get(policy_key, {}).get("action") == "auto",
                })

            if pc["is_nullable"] != cc["is_nullable"]:
                changes.append({
                    "type": "column_nullable_changed",
                    "table": t,
                    "column": c,
                    "detail": f"nullable: {pc['is_nullable']} → {cc['is_nullable']}",
                    "risk_level": "medium",
                    "auto_applicable": False,
                })

    return changes


async def _get_policies_map(conn) -> Dict[str, Any]:
    """정책 키-값 맵 반환"""
    await _ensure_schema_auto_policy(conn)
    rows = await conn.fetch("SELECT policy_key, policy_value FROM schema_auto_policy")
    result = {}
    for r in rows:
        val = r["policy_value"]
        if isinstance(val, str):
            val = json.loads(val)
        result[r["policy_key"]] = val
    return result


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
            # 주요 OMOP 테이블을 CDC 설정에 등록
            clinical_tables = [
                ("person", "streaming", True, 50000, 30),
                ("visit_occurrence", "streaming", True, 100000, 15),
                ("condition_occurrence", "streaming", True, 100000, 15),
                ("drug_exposure", "streaming", True, 100000, 15),
                ("procedure_occurrence", "streaming", True, 100000, 15),
                ("measurement", "batch", True, 200000, 60),
                ("observation", "batch", True, 200000, 60),
                ("death", "streaming", True, 10000, 30),
                ("cost", "batch", True, 150000, 60),
                ("condition_era", "disabled", False, 100000, 120),
            ]
            for tname, mode, enabled, threshold, interval in clinical_tables:
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
