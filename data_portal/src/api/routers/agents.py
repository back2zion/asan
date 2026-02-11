"""
AI 에이전트 관리 라우터 — HumanLayer 스타일 작업 승인/실행
AIAgents.tsx 연동: 작업 CRUD, 승인/거부, Claude Code 세션 관리
"""
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ._portal_ops_shared import get_connection, release_connection

router = APIRouter(prefix="/agents", tags=["Agents"])

# ── Pydantic Models ──

class AgentTaskCreate(BaseModel):
    task_type: str = Field(..., pattern=r"^(data_analysis|etl_pipeline|query_generation|report_generation)$")
    description: str = Field(..., min_length=1, max_length=2000)
    require_approval: bool = True
    priority: str = Field(default="medium", pattern=r"^(low|medium|high|critical)$")


class AgentApprove(BaseModel):
    task_id: str = Field(..., max_length=50)
    approved: bool
    feedback: str = Field(default="", max_length=1000)


class ClaudeLaunch(BaseModel):
    project_path: str = Field(..., min_length=1, max_length=500)
    instructions: Optional[str] = Field(default="", max_length=5000)


# ── DB 초기화 ──

_AGENT_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS agent_task (
    task_id       VARCHAR(50) PRIMARY KEY,
    task_type     VARCHAR(30) NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    priority      VARCHAR(10) NOT NULL DEFAULT 'medium',
    status        VARCHAR(20) NOT NULL DEFAULT 'pending_approval',
    require_approval BOOLEAN NOT NULL DEFAULT TRUE,
    feedback      TEXT DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS agent_session (
    session_id    VARCHAR(50) PRIMARY KEY,
    project_path  VARCHAR(500) NOT NULL,
    instructions  TEXT DEFAULT '',
    status        VARCHAR(20) NOT NULL DEFAULT 'running',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    stopped_at    TIMESTAMPTZ
);
"""

_initialized = False


async def _ensure_tables(conn):
    global _initialized
    if _initialized:
        return
    for stmt in _AGENT_TABLES_SQL.strip().split(";"):
        stmt = stmt.strip()
        if stmt:
            await conn.execute(stmt)
    _initialized = True


# ── 엔드포인트 ──

@router.get("/pending")
async def get_pending_tasks():
    """승인 대기 중인 작업 목록"""
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch(
            "SELECT task_id, task_type, description, priority, status, created_at "
            "FROM agent_task WHERE status = 'pending_approval' "
            "ORDER BY created_at DESC"
        )
        return {
            "pending_tasks": [
                {
                    "task_id": r["task_id"],
                    "task_type": r["task_type"],
                    "description": r["description"],
                    "priority": r["priority"],
                    "status": r["status"],
                    "created_at": r["created_at"].isoformat(),
                }
                for r in rows
            ]
        }
    finally:
        await release_connection(conn)


@router.get("/stats")
async def get_agent_stats():
    """에이전트 통계 — 실행 중/완료 카운트"""
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow("""
            SELECT
                (SELECT COUNT(*) FROM agent_session WHERE status = 'running') AS running_agents,
                (SELECT COUNT(*) FROM agent_task WHERE status = 'completed') AS completed_tasks,
                (SELECT COUNT(*) FROM agent_task) AS total_tasks,
                (SELECT COUNT(*) FROM agent_task WHERE status = 'pending_approval') AS pending_tasks
        """)
        return {
            "running_agents": row["running_agents"],
            "completed_tasks": row["completed_tasks"],
            "total_tasks": row["total_tasks"],
            "pending_tasks": row["pending_tasks"],
        }
    finally:
        await release_connection(conn)


@router.get("/claude-code/sessions")
async def get_claude_sessions():
    """활성 Claude Code 세션 목록"""
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch(
            "SELECT session_id, project_path, instructions, status, created_at, stopped_at "
            "FROM agent_session ORDER BY created_at DESC LIMIT 50"
        )
        return {
            "sessions": [
                {
                    "session_id": r["session_id"],
                    "project_path": r["project_path"],
                    "instructions": r["instructions"],
                    "status": r["status"],
                    "created_at": r["created_at"].isoformat(),
                    "stopped_at": r["stopped_at"].isoformat() if r["stopped_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await release_connection(conn)


@router.post("/execute")
async def execute_task(body: AgentTaskCreate):
    """새 에이전트 작업 생성"""
    task_id = f"task-{uuid.uuid4().hex[:8]}"
    status = "pending_approval" if body.require_approval else "approved"
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await conn.execute(
            "INSERT INTO agent_task (task_id, task_type, description, priority, status, require_approval) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            task_id, body.task_type, body.description, body.priority, status, body.require_approval,
        )
        return {"task_id": task_id, "status": status}
    finally:
        await release_connection(conn)


@router.post("/approve")
async def approve_task(body: AgentApprove):
    """작업 승인/거부"""
    new_status = "approved" if body.approved else "rejected"
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        result = await conn.execute(
            "UPDATE agent_task SET status = $1, feedback = $2, updated_at = NOW() "
            "WHERE task_id = $3 AND status = 'pending_approval'",
            new_status, body.feedback, body.task_id,
        )
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="작업을 찾을 수 없거나 이미 처리되었습니다.")
        return {"task_id": body.task_id, "status": new_status}
    finally:
        await release_connection(conn)


@router.post("/claude-code/launch")
async def launch_claude_session(body: ClaudeLaunch):
    """Claude Code 세션 시작"""
    session_id = f"claude-{uuid.uuid4().hex[:8]}"
    conn = await get_connection()
    try:
        await _ensure_tables(conn)
        await conn.execute(
            "INSERT INTO agent_session (session_id, project_path, instructions, status) "
            "VALUES ($1, $2, $3, 'running')",
            session_id, body.project_path, body.instructions or "",
        )
        return {"session_id": session_id, "status": "running"}
    finally:
        await release_connection(conn)
