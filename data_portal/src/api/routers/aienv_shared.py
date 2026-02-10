"""
AI 분석환경 API - 공유 상수, Docker 클라이언트, 경로, 유틸리티
PostgreSQL-backed (sync_history, permissions)
"""
import logging
from pathlib import Path
from datetime import datetime

import docker

logger = logging.getLogger(__name__)

# Docker 클라이언트 (lazy init)
_docker_client = None


def get_docker_client():
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


# 인프라 컨테이너 (삭제/중지 보호)
PROTECTED_CONTAINERS = {
    "asan-jupyterlab",
    "asan-mlflow",
    "asan-redis",
    "milvus-standalone",
    "milvus-etcd",
    "asan-minio",
    "asan-xiyan-sql",
    "infra-omop-db-1",
    "infra-nginx-1",
}

# 포트 범위 (새 컨테이너용)
PORT_RANGE_START = 19000
PORT_RANGE_END = 19099

# 노트북 디렉토리 (공유용)
NOTEBOOKS_DIR = Path(__file__).parent.parent / "notebooks"

# JupyterLab 워크스페이스 디렉토리 (asan-jupyterlab 컨테이너의 /home/jovyan/work)
JUPYTER_WORKSPACE = Path(__file__).resolve().parent.parent.parent.parent.parent / "notebooks"


# ─── DB helpers ────────────────────────────────

async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)


# ─── Lazy table creation ──────────────────────

_tbl_ok = False

async def _ensure_tables():
    global _tbl_ok
    if _tbl_ok:
        return
    conn = await _get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS aienv_sync_history (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(500) NOT NULL,
                action VARCHAR(50) NOT NULL,
                username VARCHAR(100) DEFAULT 'anonymous',
                cell_count INT DEFAULT 0,
                size_kb REAL DEFAULT 0,
                source VARCHAR(50) DEFAULT 'system',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_aienv_sync_filename ON aienv_sync_history(filename);
            CREATE TABLE IF NOT EXISTS aienv_permission (
                filename VARCHAR(500) PRIMARY KEY,
                level VARCHAR(20) DEFAULT 'public',
                owner VARCHAR(100) DEFAULT 'anonymous',
                grp VARCHAR(50) DEFAULT '전체',
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        _tbl_ok = True
    finally:
        await _rel(conn)


# --- 감사 로그 헬퍼 (PostgreSQL) ---

async def load_sync_history(limit: int = 500) -> list:
    """감사 로그 로드"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM aienv_sync_history ORDER BY created_at DESC LIMIT $1",
            limit,
        )
        return [
            {
                "filename": r["filename"],
                "action": r["action"],
                "user": r["username"],
                "timestamp": r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r["created_at"] else "",
                "cell_count": r["cell_count"],
                "size_kb": r["size_kb"],
                "source": r["source"],
            }
            for r in rows
        ]
    finally:
        await _rel(conn)


async def add_sync_log(filename: str, action: str, user: str = "anonymous",
                       cell_count: int = 0, size_kb: float = 0, source: str = "system"):
    """감사 로그 항목 추가"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        await conn.execute("""
            INSERT INTO aienv_sync_history
                (filename, action, username, cell_count, size_kb, source)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, filename, action, user, cell_count, size_kb, source)
    finally:
        await _rel(conn)


async def get_last_modifier(filename: str) -> str:
    """특정 파일의 마지막 수정자 조회"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT username FROM aienv_sync_history WHERE filename=$1 ORDER BY created_at DESC LIMIT 1",
            filename,
        )
        return row["username"] if row else ""
    finally:
        await _rel(conn)


# --- 권한 관리 헬퍼 (PostgreSQL) ---

async def load_permissions() -> dict:
    """노트북별 공유 권한 설정 로드"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        rows = await conn.fetch("SELECT * FROM aienv_permission")
        return {
            r["filename"]: {
                "level": r["level"],
                "owner": r["owner"],
                "group": r["grp"],
                "updated": r["updated_at"].strftime("%Y-%m-%d %H:%M:%S") if r["updated_at"] else "",
            }
            for r in rows
        }
    finally:
        await _rel(conn)


async def save_permissions(perms: dict):
    """노트북별 공유 권한 설정 저장 (upsert)"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        for filename, perm in perms.items():
            await conn.execute("""
                INSERT INTO aienv_permission (filename, level, owner, grp, updated_at)
                VALUES ($1, $2, $3, $4, NOW())
                ON CONFLICT (filename) DO UPDATE SET
                    level = EXCLUDED.level,
                    owner = EXCLUDED.owner,
                    grp = EXCLUDED.grp,
                    updated_at = NOW()
            """, filename, perm.get("level", "public"),
                perm.get("owner", "anonymous"), perm.get("group", "전체"))
    finally:
        await _rel(conn)


async def get_permission(filename: str) -> dict:
    """특정 노트북의 권한 정보 조회"""
    await _ensure_tables()
    conn = await _get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT * FROM aienv_permission WHERE filename=$1", filename
        )
        if row:
            return {
                "level": row["level"],
                "owner": row["owner"],
                "group": row["grp"],
                "updated": row["updated_at"].strftime("%Y-%m-%d %H:%M:%S") if row["updated_at"] else "",
            }
        return {
            "level": "public",
            "owner": "anonymous",
            "group": "전체",
            "updated": "",
        }
    finally:
        await _rel(conn)


