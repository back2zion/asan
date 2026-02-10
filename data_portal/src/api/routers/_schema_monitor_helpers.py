"""
Schema monitor helpers — DB table creation, schema capture, diff computation, policies.
Extracted from schema_monitor.py to reduce file size.
"""
import os
import json
from typing import Dict, Any, List
from pydantic import BaseModel, Field
from typing import Optional
import asyncpg
from fastapi import HTTPException


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


# CDC seed data
CDC_CLINICAL_TABLES = [
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
