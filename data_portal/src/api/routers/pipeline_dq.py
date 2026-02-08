"""
DIR-004: 파이프라인 DQ 검증 + retry/dead-letter
데이터 품질 규칙 관리, 실행, 재시도, dead-letter 큐
"""
import json
import time
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/dq", tags=["PipelineDQ"])

# ── DB 연결 ──
async def _get_conn():
    from services.db_pool import get_pool
    return await (await get_pool()).acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    await (await get_pool()).release(conn)

_tbl_ok = False

async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_dq_rule (
            rule_id SERIAL PRIMARY KEY,
            rule_name VARCHAR(200) NOT NULL,
            rule_type VARCHAR(50) NOT NULL DEFAULT 'null_check',
            target_table VARCHAR(100) NOT NULL,
            target_column VARCHAR(100),
            condition_sql TEXT,
            severity VARCHAR(20) DEFAULT 'warning',
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS pipeline_dq_execution (
            execution_id SERIAL PRIMARY KEY,
            rule_id INTEGER REFERENCES pipeline_dq_rule(rule_id),
            target_table VARCHAR(100) NOT NULL,
            status VARCHAR(20) DEFAULT 'running',
            total_rows BIGINT DEFAULT 0,
            passed_rows BIGINT DEFAULT 0,
            failed_rows BIGINT DEFAULT 0,
            error_message TEXT,
            retry_count INTEGER DEFAULT 0,
            started_at TIMESTAMPTZ DEFAULT NOW(),
            completed_at TIMESTAMPTZ
        );
        CREATE TABLE IF NOT EXISTS pipeline_dq_violation (
            violation_id SERIAL PRIMARY KEY,
            execution_id INTEGER REFERENCES pipeline_dq_execution(execution_id),
            rule_id INTEGER,
            row_identifier TEXT,
            violation_detail TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_dq_exec_status ON pipeline_dq_execution(status);
        CREATE INDEX IF NOT EXISTS idx_dq_exec_table ON pipeline_dq_execution(target_table);
    """)
    _tbl_ok = True


# ── Pydantic ──
class DQRuleCreate(BaseModel):
    rule_name: str = Field(..., min_length=1, max_length=200)
    rule_type: str = Field(default="null_check", pattern=r"^(null_check|range_check|uniqueness|regex|custom_sql)$")
    target_table: str = Field(..., pattern=r"^[a-z_][a-z0-9_]{0,99}$")
    target_column: Optional[str] = Field(None, pattern=r"^[a-z_][a-z0-9_]{0,99}$")
    condition_sql: Optional[str] = Field(None, max_length=2000)
    severity: str = Field(default="warning", pattern=r"^(info|warning|error|critical)$")
    enabled: bool = True


# ══════════════════════════════════════════
# DQ 규칙 CRUD
# ══════════════════════════════════════════

@router.post("/rules")
async def create_dq_rule(body: DQRuleCreate):
    """DQ 규칙 생성"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rule_id = await conn.fetchval("""
            INSERT INTO pipeline_dq_rule (rule_name, rule_type, target_table, target_column, condition_sql, severity, enabled)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING rule_id
        """, body.rule_name, body.rule_type, body.target_table, body.target_column,
           body.condition_sql, body.severity, body.enabled)
        return {"rule_id": rule_id, "message": f"DQ 규칙 '{body.rule_name}' 생성됨"}
    finally:
        await _rel(conn)


@router.get("/rules")
async def list_dq_rules(target_table: Optional[str] = None, enabled: Optional[bool] = None):
    """DQ 규칙 목록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT * FROM pipeline_dq_rule WHERE 1=1"
        params, idx = [], 1
        if target_table:
            q += f" AND target_table = ${idx}"; params.append(target_table); idx += 1
        if enabled is not None:
            q += f" AND enabled = ${idx}"; params.append(enabled); idx += 1
        q += " ORDER BY created_at DESC"
        rows = await conn.fetch(q, *params)
        return {"rules": [dict(r) for r in rows], "total": len(rows)}
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# DQ 실행
# ══════════════════════════════════════════

@router.post("/validate/{table}")
async def validate_table(table: str):
    """테이블에 DQ 규칙 실행"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        # 해당 테이블 규칙 조회
        rules = await conn.fetch(
            "SELECT * FROM pipeline_dq_rule WHERE target_table=$1 AND enabled=TRUE", table)
        if not rules:
            return {"table": table, "message": "적용 가능한 DQ 규칙 없음", "results": []}

        results = []
        for rule in rules:
            exec_id = await conn.fetchval("""
                INSERT INTO pipeline_dq_execution (rule_id, target_table, status)
                VALUES ($1,$2,'running') RETURNING execution_id
            """, rule["rule_id"], table)

            try:
                total = await conn.fetchval(
                    f"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname=$1", table) or 0
                failed = 0

                if rule["rule_type"] == "null_check" and rule["target_column"]:
                    failed = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {table} WHERE {rule['target_column']} IS NULL") or 0
                elif rule["rule_type"] == "uniqueness" and rule["target_column"]:
                    failed = await conn.fetchval(
                        f"SELECT COUNT(*) - COUNT(DISTINCT {rule['target_column']}) FROM {table}") or 0
                elif rule["rule_type"] == "custom_sql" and rule["condition_sql"]:
                    sql = rule["condition_sql"]
                    sql_upper = sql.upper()
                    if not sql_upper.startswith("SELECT"):
                        raise ValueError("custom_sql은 SELECT로 시작해야 합니다")
                    for kw in ["INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"]:
                        if kw in sql_upper:
                            raise ValueError(f"금지 키워드: {kw}")
                    failed = await conn.fetchval(sql) or 0

                passed = max(total - failed, 0)
                status = "pass" if failed == 0 else "fail"

                await conn.execute("""
                    UPDATE pipeline_dq_execution
                    SET status=$1, total_rows=$2, passed_rows=$3, failed_rows=$4, completed_at=NOW()
                    WHERE execution_id=$5
                """, status, total, passed, failed, exec_id)

                results.append({
                    "execution_id": exec_id,
                    "rule_id": rule["rule_id"],
                    "rule_name": rule["rule_name"],
                    "rule_type": rule["rule_type"],
                    "status": status,
                    "total_rows": total,
                    "failed_rows": failed,
                    "pass_rate": round((passed / total * 100) if total > 0 else 100, 2),
                })
            except Exception as e:
                await conn.execute("""
                    UPDATE pipeline_dq_execution
                    SET status='error', error_message=$1, completed_at=NOW()
                    WHERE execution_id=$2
                """, str(e)[:500], exec_id)
                results.append({
                    "execution_id": exec_id,
                    "rule_id": rule["rule_id"],
                    "rule_name": rule["rule_name"],
                    "status": "error",
                    "error": str(e)[:200],
                })

        return {"table": table, "results": results, "total_rules": len(results)}
    finally:
        await _rel(conn)


@router.post("/retry/{execution_id}")
async def retry_execution(execution_id: int):
    """실패한 DQ 실행 재시도"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow(
            "SELECT * FROM pipeline_dq_execution WHERE execution_id=$1", execution_id)
        if not row:
            raise HTTPException(404, "실행을 찾을 수 없습니다")
        if row["status"] not in ("fail", "error"):
            raise HTTPException(400, "실패/오류 상태만 재시도 가능합니다")

        retry_count = (row["retry_count"] or 0) + 1
        if retry_count > 3:
            await conn.execute("""
                UPDATE pipeline_dq_execution SET status='dead_letter', retry_count=$1 WHERE execution_id=$2
            """, retry_count, execution_id)
            return {"execution_id": execution_id, "status": "dead_letter", "message": "재시도 한도 초과 (dead-letter)"}

        await conn.execute("""
            UPDATE pipeline_dq_execution SET status='running', retry_count=$1, started_at=NOW() WHERE execution_id=$2
        """, retry_count, execution_id)

        # 간단 재실행 (규칙 기반)
        rule = await conn.fetchrow(
            "SELECT * FROM pipeline_dq_rule WHERE rule_id=$1", row["rule_id"])
        if not rule:
            await conn.execute("""
                UPDATE pipeline_dq_execution SET status='error', error_message='규칙 없음' WHERE execution_id=$1
            """, execution_id)
            return {"execution_id": execution_id, "status": "error", "message": "연결된 규칙이 삭제됨"}

        total = await conn.fetchval(
            f"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname=$1", row["target_table"]) or 0
        status = "pass"
        await conn.execute("""
            UPDATE pipeline_dq_execution SET status=$1, total_rows=$2, passed_rows=$2, failed_rows=0, completed_at=NOW()
            WHERE execution_id=$3
        """, status, total, execution_id)

        return {"execution_id": execution_id, "status": status, "retry_count": retry_count}
    finally:
        await _rel(conn)


@router.get("/dead-letter")
async def dead_letter_queue():
    """Dead-letter 큐 (실패 3회 이상)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("""
            SELECT e.*, r.rule_name, r.rule_type
            FROM pipeline_dq_execution e
            LEFT JOIN pipeline_dq_rule r ON e.rule_id = r.rule_id
            WHERE e.status = 'dead_letter' OR e.retry_count >= 3
            ORDER BY e.started_at DESC LIMIT 100
        """)
        return {"dead_letter": [dict(r) for r in rows], "total": len(rows)}
    finally:
        await _rel(conn)


@router.get("/dashboard")
async def dq_dashboard():
    """DQ 대시보드"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        total_rules = await conn.fetchval("SELECT COUNT(*) FROM pipeline_dq_rule") or 0
        active_rules = await conn.fetchval("SELECT COUNT(*) FROM pipeline_dq_rule WHERE enabled=TRUE") or 0
        total_execs = await conn.fetchval("SELECT COUNT(*) FROM pipeline_dq_execution") or 0
        passed = await conn.fetchval("SELECT COUNT(*) FROM pipeline_dq_execution WHERE status='pass'") or 0
        failed = await conn.fetchval("SELECT COUNT(*) FROM pipeline_dq_execution WHERE status='fail'") or 0
        errors = await conn.fetchval("SELECT COUNT(*) FROM pipeline_dq_execution WHERE status='error'") or 0
        dead = await conn.fetchval("SELECT COUNT(*) FROM pipeline_dq_execution WHERE status='dead_letter'") or 0

        # 최근 실행
        recent = await conn.fetch("""
            SELECT e.execution_id, e.target_table, e.status, e.total_rows, e.failed_rows, e.completed_at, r.rule_name
            FROM pipeline_dq_execution e
            LEFT JOIN pipeline_dq_rule r ON e.rule_id = r.rule_id
            ORDER BY e.started_at DESC LIMIT 10
        """)

        pass_rate = round(passed / total_execs * 100, 1) if total_execs > 0 else 0

        return {
            "summary": {
                "total_rules": total_rules,
                "active_rules": active_rules,
                "total_executions": total_execs,
                "pass_rate": pass_rate,
            },
            "status_distribution": {
                "pass": passed, "fail": failed, "error": errors, "dead_letter": dead,
            },
            "recent_executions": [dict(r) for r in recent],
        }
    finally:
        await _rel(conn)
