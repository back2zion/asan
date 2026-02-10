"""
AI 분석환경 API helpers — Pydantic models, DB helpers, OMOP pool, table definitions.
Extracted from aienv_projects.py to reduce file size.
"""
from typing import Optional

from pydantic import BaseModel


# --- Pydantic Models ---

class AnalysisRequestCreate(BaseModel):
    title: str
    description: str
    requester: Optional[str] = "anonymous"
    priority: Optional[str] = "medium"


class AnalysisRequestUpdate(BaseModel):
    status: Optional[str] = None
    response: Optional[str] = None
    assignee: Optional[str] = None
    result_notebook: Optional[str] = None


class ProjectCreate(BaseModel):
    name: str
    description: Optional[str] = ""
    owner: Optional[str] = "anonymous"
    cpu_quota: Optional[float] = 4.0
    memory_quota_gb: Optional[float] = 8.0
    storage_quota_gb: Optional[float] = 50.0


class ProjectUpdate(BaseModel):
    description: Optional[str] = None
    cpu_quota: Optional[float] = None
    memory_quota_gb: Optional[float] = None
    storage_quota_gb: Optional[float] = None
    status: Optional[str] = None


class DatasetExportRequest(BaseModel):
    table_name: str
    limit: Optional[int] = 10000
    columns: Optional[list[str]] = None


# ─── DB helpers ────────────────────────────────────

async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)


# ─── Lazy table creation ──────────────────────────

_tbl_ok = False

async def _ensure_tables():
    global _tbl_ok
    if _tbl_ok:
        return
    conn = await _get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS aienv_project (
                id VARCHAR(100) PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                description TEXT DEFAULT '',
                owner VARCHAR(100) DEFAULT 'anonymous',
                status VARCHAR(20) DEFAULT 'active',
                cpu_quota REAL DEFAULT 4.0,
                memory_quota_gb REAL DEFAULT 8.0,
                storage_quota_gb REAL DEFAULT 50.0,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS aienv_analysis_request (
                id VARCHAR(20) PRIMARY KEY,
                title VARCHAR(500) NOT NULL,
                description TEXT DEFAULT '',
                requester VARCHAR(100) DEFAULT 'anonymous',
                priority VARCHAR(20) DEFAULT 'medium',
                status VARCHAR(20) DEFAULT 'pending',
                assignee VARCHAR(100) DEFAULT '',
                response TEXT DEFAULT '',
                result_notebook VARCHAR(500) DEFAULT '',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_aienv_req_status ON aienv_analysis_request(status);
            CREATE TABLE IF NOT EXISTS aienv_export_history (
                id SERIAL PRIMARY KEY,
                filename VARCHAR(300) NOT NULL,
                table_name VARCHAR(100) NOT NULL,
                row_count INT DEFAULT 0,
                columns JSONB DEFAULT '[]',
                size_kb REAL DEFAULT 0,
                export_limit INT DEFAULT 10000,
                exported_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_aienv_export_table ON aienv_export_history(table_name);
        """)
        _tbl_ok = True
    finally:
        await _rel(conn)


# ─── OMOP CDM 연결 (asyncpg) ─────────────────────

_omop_pool = None

async def _get_omop_pool():
    global _omop_pool
    if _omop_pool is None:
        import asyncpg
        _omop_pool = await asyncpg.create_pool(
            host="localhost",
            port=5436,
            database="omop_cdm",
            user="omopuser",
            password="omop",
            min_size=1,
            max_size=3,
        )
    return _omop_pool


# ─── 테이블 목록 캐시 (OMOP) ─────────────────────

OMOP_TABLE_DESCRIPTIONS = {
    "person": "환자 기본 인구통계 정보",
    "visit_occurrence": "방문(입원/외래/응급) 기록",
    "condition_occurrence": "진단/상병 기록",
    "drug_exposure": "약물 처방/투약 기록",
    "procedure_occurrence": "시술/수술 기록",
    "measurement": "검사 결과 (검체/생체 측정)",
    "observation": "관찰 기록 (증상/징후/가족력 등)",
    "observation_period": "환자별 관찰 기간",
    "condition_era": "진단 에피소드 (연속 진단 기간)",
    "drug_era": "약물 에피소드 (연속 투약 기간)",
    "cost": "의료비용 정보",
    "payer_plan_period": "보험 가입 기간",
    "death": "사망 기록",
    "specimen": "검체 정보",
    "device_exposure": "의료기기 사용 기록",
    "note": "임상 노트/문서",
    "care_site": "진료 장소 정보",
    "provider": "의료 제공자 정보",
    "location": "위치 정보",
}
