"""
나. 통합 Pipeline — CDC 실행 엔진 + 파이프라인 모니터링
RFP 요구: 다양한 형태의 데이터 수집/적재 및 변환 프로세스 통합 Pipeline
PostgreSQL Logical Replication 기반 CDC + 이벤트 로그 + 모니터링
"""
import os
import json
import time
import asyncio
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/cdc-exec", tags=["CDC Executor"])

async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)

# ── 테이블 ──
_tbl_ok = False

async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS cdc_capture_config (
            config_id SERIAL PRIMARY KEY,
            source_table VARCHAR(100) NOT NULL,
            capture_mode VARCHAR(20) DEFAULT 'trigger',
            is_active BOOLEAN DEFAULT TRUE,
            tracked_columns JSONB DEFAULT '[]',
            filter_condition TEXT,
            batch_size INTEGER DEFAULT 1000,
            poll_interval_sec INTEGER DEFAULT 60,
            last_captured_at TIMESTAMPTZ,
            total_events BIGINT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS cdc_change_event (
            event_id BIGSERIAL PRIMARY KEY,
            config_id INTEGER,
            source_table VARCHAR(100) NOT NULL,
            operation VARCHAR(10) NOT NULL,
            primary_key_value TEXT,
            old_data JSONB,
            new_data JSONB,
            changed_columns JSONB DEFAULT '[]',
            captured_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_cdc_event_table ON cdc_change_event(source_table, captured_at);
        CREATE INDEX IF NOT EXISTS idx_cdc_event_time ON cdc_change_event(captured_at);

        CREATE TABLE IF NOT EXISTS cdc_pipeline_run (
            run_id SERIAL PRIMARY KEY,
            pipeline_name VARCHAR(100) NOT NULL,
            pipeline_type VARCHAR(30) DEFAULT 'batch',
            status VARCHAR(20) DEFAULT 'running',
            source_table VARCHAR(100),
            target_table VARCHAR(100),
            rows_read BIGINT DEFAULT 0,
            rows_written BIGINT DEFAULT 0,
            rows_errored BIGINT DEFAULT 0,
            started_at TIMESTAMPTZ DEFAULT NOW(),
            finished_at TIMESTAMPTZ,
            error_message TEXT,
            metadata JSONB DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_cdc_run_status ON cdc_pipeline_run(status);
    """)
    _tbl_ok = True


# ── 모델 ──

class CaptureConfigCreate(BaseModel):
    source_table: str = Field(..., pattern=r"^[a-z_][a-z0-9_]{0,99}$")
    capture_mode: str = Field(default="trigger", pattern=r"^(trigger|polling|log)$")
    tracked_columns: list = Field(default_factory=list)
    filter_condition: Optional[str] = Field(None, max_length=500)
    batch_size: int = Field(default=1000, ge=100, le=50000)
    poll_interval_sec: int = Field(default=60, ge=10, le=3600)

class PipelineRunCreate(BaseModel):
    pipeline_name: str = Field(..., max_length=100)
    pipeline_type: str = Field(default="batch", pattern=r"^(batch|streaming|micro_batch)$")
    source_table: Optional[str] = None
    target_table: Optional[str] = None


# ══════════════════════════════════════════
# CDC 캡처 설정
# ══════════════════════════════════════════

@router.post("/configs")
async def create_capture_config(body: CaptureConfigCreate):
    """CDC 캡처 설정 생성 + 트리거 자동 설치"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)

        # 테이블 존재 확인
        exists = await conn.fetchval(
            "SELECT 1 FROM pg_stat_user_tables WHERE relname=$1", body.source_table)
        if not exists:
            raise HTTPException(404, f"테이블 없음: {body.source_table}")

        cid = await conn.fetchval("""
            INSERT INTO cdc_capture_config (source_table, capture_mode, tracked_columns, filter_condition, batch_size, poll_interval_sec)
            VALUES ($1,$2,$3::jsonb,$4,$5,$6) RETURNING config_id
        """, body.source_table, body.capture_mode, json.dumps(body.tracked_columns),
            body.filter_condition, body.batch_size, body.poll_interval_sec)

        # 트리거 모드: audit trigger 설치
        trigger_installed = False
        if body.capture_mode == "trigger":
            try:
                await conn.execute(f"""
                    CREATE OR REPLACE FUNCTION cdc_audit_{body.source_table}()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        INSERT INTO cdc_change_event (config_id, source_table, operation, primary_key_value, old_data, new_data)
                        VALUES (
                            {cid},
                            '{body.source_table}',
                            TG_OP,
                            CASE WHEN TG_OP='DELETE' THEN row_to_json(OLD)::jsonb->>'person_id'
                                 ELSE row_to_json(NEW)::jsonb->>'person_id' END,
                            CASE WHEN TG_OP IN ('UPDATE','DELETE') THEN row_to_json(OLD)::jsonb ELSE NULL END,
                            CASE WHEN TG_OP IN ('INSERT','UPDATE') THEN row_to_json(NEW)::jsonb ELSE NULL END
                        );
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;

                    DROP TRIGGER IF EXISTS cdc_trigger_{body.source_table} ON {body.source_table};
                    CREATE TRIGGER cdc_trigger_{body.source_table}
                        AFTER INSERT OR UPDATE OR DELETE ON {body.source_table}
                        FOR EACH ROW EXECUTE FUNCTION cdc_audit_{body.source_table}();
                """)
                trigger_installed = True
            except Exception as e:
                trigger_installed = False

        return {"config_id": cid, "source_table": body.source_table,
                "capture_mode": body.capture_mode, "trigger_installed": trigger_installed}
    finally:
        await _rel(conn)


@router.get("/configs")
async def list_capture_configs():
    """CDC 캡처 설정 목록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("SELECT * FROM cdc_capture_config ORDER BY created_at DESC")
        return [dict(r) for r in rows]
    finally:
        await _rel(conn)


@router.post("/configs/{config_id}/capture")
async def manual_capture(config_id: int):
    """수동 폴링 캡처 실행 (polling 모드)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        cfg = await conn.fetchrow("SELECT * FROM cdc_capture_config WHERE config_id=$1", config_id)
        if not cfg:
            raise HTTPException(404, "설정 없음")

        table = cfg["source_table"]
        last = cfg["last_captured_at"]

        # 마지막 캡처 이후 변경된 행 감지 (created_at 기반)
        q = f"SELECT * FROM {table}"
        if last:
            # created_at 컬럼이 있으면 사용
            has_ts = await conn.fetchval(
                "SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name='created_at'",
                table)
            if has_ts:
                q += f" WHERE created_at > $1"
                rows = await conn.fetch(q + f" LIMIT {cfg['batch_size']}", last)
            else:
                rows = []
        else:
            rows = await conn.fetch(q + f" LIMIT {cfg['batch_size']}")

        # 이벤트 기록
        count = 0
        for r in rows:
            await conn.execute("""
                INSERT INTO cdc_change_event (config_id, source_table, operation, new_data)
                VALUES ($1,$2,'SNAPSHOT',$3::jsonb)
            """, config_id, table, json.dumps(dict(r), default=str))
            count += 1

        await conn.execute("""
            UPDATE cdc_capture_config SET last_captured_at=NOW(), total_events=total_events+$1
            WHERE config_id=$2
        """, count, config_id)

        return {"config_id": config_id, "captured_events": count}
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# 변경 이벤트 조회
# ══════════════════════════════════════════

@router.get("/events")
async def list_events(
    source_table: Optional[str] = None,
    operation: Optional[str] = None,
    limit: int = Query(50, le=500),
):
    """CDC 변경 이벤트 조회"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT event_id, config_id, source_table, operation, primary_key_value, changed_columns, captured_at FROM cdc_change_event WHERE 1=1"
        params, idx = [], 1
        if source_table:
            q += f" AND source_table=${idx}"; params.append(source_table); idx += 1
        if operation:
            q += f" AND operation=${idx}"; params.append(operation.upper()); idx += 1
        q += f" ORDER BY captured_at DESC LIMIT ${idx}"; params.append(limit)
        rows = await conn.fetch(q, *params)
        return [dict(r) for r in rows]
    finally:
        await _rel(conn)


@router.get("/events/stats")
async def event_stats(days: int = Query(7, ge=1, le=90)):
    """CDC 이벤트 통계"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        stats = await conn.fetch("""
            SELECT source_table, operation, COUNT(*) as count,
                   MIN(captured_at) as first_event, MAX(captured_at) as last_event
            FROM cdc_change_event
            WHERE captured_at > NOW() - ($1 || ' days')::interval
            GROUP BY source_table, operation
            ORDER BY count DESC
        """, str(days))
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM cdc_change_event WHERE captured_at > NOW() - ($1 || ' days')::interval",
            str(days))
        return {"period_days": days, "total_events": total, "by_table_operation": [dict(r) for r in stats]}
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# 파이프라인 실행 모니터링
# ══════════════════════════════════════════

@router.post("/pipelines/start")
async def start_pipeline(body: PipelineRunCreate):
    """파이프라인 실행 시작 기록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rid = await conn.fetchval("""
            INSERT INTO cdc_pipeline_run (pipeline_name, pipeline_type, source_table, target_table)
            VALUES ($1,$2,$3,$4) RETURNING run_id
        """, body.pipeline_name, body.pipeline_type, body.source_table, body.target_table)
        return {"run_id": rid, "status": "running"}
    finally:
        await _rel(conn)

@router.put("/pipelines/{run_id}/complete")
async def complete_pipeline(run_id: int, rows_read: int = 0, rows_written: int = 0, rows_errored: int = 0, error: Optional[str] = None):
    """파이프라인 실행 완료 기록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        status = "failed" if error else "completed"
        await conn.execute("""
            UPDATE cdc_pipeline_run SET status=$1, rows_read=$2, rows_written=$3, rows_errored=$4,
                   finished_at=NOW(), error_message=$5
            WHERE run_id=$6
        """, status, rows_read, rows_written, rows_errored, error, run_id)
        return {"run_id": run_id, "status": status}
    finally:
        await _rel(conn)

@router.get("/pipelines")
async def list_pipeline_runs(status: Optional[str] = None, limit: int = Query(50, le=200)):
    """파이프라인 실행 이력"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT * FROM cdc_pipeline_run"
        params = []
        if status:
            q += " WHERE status=$1"; params.append(status)
        q += " ORDER BY started_at DESC LIMIT " + str(limit)
        rows = await conn.fetch(q, *params)
        return [dict(r) for r in rows]
    finally:
        await _rel(conn)

@router.get("/pipelines/dashboard")
async def pipeline_dashboard():
    """파이프라인 모니터링 대시보드"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        summary = await conn.fetchrow("""
            SELECT
                COUNT(*) as total_runs,
                COUNT(*) FILTER (WHERE status='running') as running,
                COUNT(*) FILTER (WHERE status='completed') as completed,
                COUNT(*) FILTER (WHERE status='failed') as failed,
                COALESCE(SUM(rows_written) FILTER (WHERE status='completed'), 0) as total_rows_written,
                AVG(EXTRACT(EPOCH FROM (finished_at - started_at))) FILTER (WHERE finished_at IS NOT NULL) as avg_duration_sec
            FROM cdc_pipeline_run
        """)

        # CDC 설정 현황
        cdc_configs = await conn.fetchval("SELECT COUNT(*) FROM cdc_capture_config WHERE is_active=true") or 0
        cdc_events_24h = await conn.fetchval(
            "SELECT COUNT(*) FROM cdc_change_event WHERE captured_at > NOW() - INTERVAL '24 hours'") or 0

        # 최근 실행
        recent = await conn.fetch("SELECT * FROM cdc_pipeline_run ORDER BY started_at DESC LIMIT 5")

        return {
            "pipeline_summary": dict(summary) if summary else {},
            "cdc": {"active_configs": cdc_configs, "events_last_24h": cdc_events_24h},
            "recent_runs": [dict(r) for r in recent],
        }
    finally:
        await _rel(conn)
