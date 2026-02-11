"""
포털 데이터 패브릭 관리 API — 소스/흐름 CRUD + 통계
DataFabric.tsx의 데이터 소스 관리 엔드포인트
"""
import json
import asyncio
from typing import Optional, Dict, Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ._portal_ops_shared import get_connection, release_connection, cached_get, cached_set, cached_pop
from ._portal_ops_health import run_single_check

router = APIRouter(tags=["PortalOps-Fabric"])


# ── Pydantic Models ──

class FabricSourceCreate(BaseModel):
    source_id: str = Field(..., max_length=50)
    name: str = Field(..., max_length=100)
    source_type: str = Field(..., max_length=50)
    host: str = Field(default="localhost", max_length=200)
    port: int
    enabled: bool = True
    check_method: str = Field(default="http")
    check_url: str = Field(default="", max_length=500)
    description: str = Field(default="", max_length=1000)
    config: Dict[str, Any] = Field(default_factory=dict)


class FabricSourceUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    source_type: Optional[str] = Field(None, max_length=50)
    host: Optional[str] = Field(None, max_length=200)
    port: Optional[int] = None
    enabled: Optional[bool] = None
    check_method: Optional[str] = None
    check_url: Optional[str] = Field(None, max_length=500)
    description: Optional[str] = Field(None, max_length=1000)
    config: Optional[Dict[str, Any]] = None


class FabricFlowCreate(BaseModel):
    source_from: str = Field(..., max_length=50)
    source_to: str = Field(..., max_length=50)
    label: str = Field(..., max_length=100)
    enabled: bool = True
    description: str = Field(default="", max_length=1000)


class FabricFlowUpdate(BaseModel):
    source_from: Optional[str] = Field(None, max_length=50)
    source_to: Optional[str] = Field(None, max_length=50)
    label: Optional[str] = Field(None, max_length=100)
    enabled: Optional[bool] = None
    description: Optional[str] = Field(None, max_length=1000)


# ── DB 테이블 + 시드 ──

_FABRIC_INIT_DONE = False


async def _ensure_fabric_tables(conn):
    """패브릭 소스/흐름 관리 테이블 생성 + 시드"""
    global _FABRIC_INIT_DONE
    if _FABRIC_INIT_DONE:
        return

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS po_fabric_source (
            source_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            source_type VARCHAR(50) NOT NULL,
            host VARCHAR(200) DEFAULT 'localhost',
            port INTEGER NOT NULL,
            enabled BOOLEAN DEFAULT TRUE,
            check_method VARCHAR(20) DEFAULT 'http',
            check_url VARCHAR(500) DEFAULT '',
            description TEXT DEFAULT '',
            config JSONB DEFAULT '{}',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS po_fabric_flow (
            flow_id SERIAL PRIMARY KEY,
            source_from VARCHAR(50) NOT NULL,
            source_to VARCHAR(50) NOT NULL,
            label VARCHAR(100) NOT NULL,
            enabled BOOLEAN DEFAULT TRUE,
            description TEXT DEFAULT '',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cnt = await conn.fetchval("SELECT COUNT(*) FROM po_fabric_source")
    if cnt == 0:
        _sources = [
            ("omop-cdm", "OMOP CDM", "PostgreSQL", "localhost", 5436, "asyncpg", "",
             "OMOP CDM 임상 데이터 웨어하우스", '{"db":"omop_cdm","user":"omopuser"}'),
            ("milvus", "Milvus Vector DB", "Vector Store", "localhost", 19530, "http",
             "http://localhost:9091/healthz", "벡터 임베딩 DB (RAG)", '{}'),
            ("minio", "MinIO S3", "Object Storage", "localhost", 19000, "http",
             "http://localhost:19000/minio/health/live", "S3 호환 오브젝트 스토리지", '{}'),
            ("xiyan-sql", "XiYanSQL", "Text-to-SQL LLM", "localhost", 8001, "http",
             "http://localhost:8001/v1/models", "Text-to-SQL 변환 모델",
             '{"model":"QwenCoder-7B","gpu_mb":6200}'),
            ("qwen3", "Qwen3-32B", "General LLM", "localhost", 28888, "http",
             "http://localhost:28888/v1/models", "범용 대규모 언어 모델",
             '{"model":"Qwen3-32B-AWQ","gpu_mb":22400}'),
            ("ner", "BioClinicalBERT", "Medical NER", "localhost", 28100, "http",
             "http://localhost:28100/ner/health", "의료 개체명 인식 모델",
             '{"model":"biomedical-ner-all","gpu_mb":287}'),
            ("superset", "Superset DB", "BI Analytics", "localhost", 15432, "psycopg2", "",
             "Superset BI 분석 데이터베이스",
             '{"user":"superset","password":"superset","dbname":"superset","charts":6,"dashboards":1,"datasets":27}'),
            ("jupyterlab", "JupyterLab", "Analysis Env", "localhost", 18888, "http",
             "http://localhost:18888/api/status", "JupyterLab 분석 환경", '{}'),
        ]
        for s in _sources:
            await conn.execute(
                "INSERT INTO po_fabric_source "
                "(source_id,name,source_type,host,port,check_method,check_url,description,config) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)",
                *s,
            )

    cnt = await conn.fetchval("SELECT COUNT(*) FROM po_fabric_flow")
    if cnt == 0:
        _flows = [
            ("omop-cdm", "milvus", "Embedding", "CDM 스키마/데이터 → 벡터 임베딩"),
            ("omop-cdm", "xiyan-sql", "Schema", "테이블 스키마 정보 제공"),
            ("omop-cdm", "qwen3", "Context", "질의 컨텍스트 제공"),
            ("omop-cdm", "superset", "Analytics", "분석 데이터 소스"),
            ("xiyan-sql", "qwen3", "SQL→NL", "생성 SQL → 자연어 변환"),
            ("qwen3", "ner", "의료NER", "텍스트 → 의료 개체명 추출"),
            ("minio", "omop-cdm", "ETL", "오브젝트 → DB 적재"),
            ("milvus", "qwen3", "RAG", "유사 문서 검색 결과 제공"),
        ]
        for f in _flows:
            await conn.execute(
                "INSERT INTO po_fabric_flow (source_from,source_to,label,description) "
                "VALUES ($1,$2,$3,$4)",
                *f,
            )

    _FABRIC_INIT_DONE = True


# ═══════════════════════════════════════════════════
#  GET /fabric-stats
# ═══════════════════════════════════════════════════

@router.get("/fabric-stats")
async def fabric_stats():
    cached = cached_get("fabric-stats")
    if cached:
        return cached

    conn = await get_connection()
    try:
        await _ensure_fabric_tables(conn)

        source_rows = await conn.fetch(
            "SELECT * FROM po_fabric_source ORDER BY created_at"
        )

        sources = []
        for row in source_rows:
            cfg = row["config"] if isinstance(row["config"], dict) else json.loads(row["config"] or "{}")
            sources.append({
                "id": row["source_id"],
                "name": row["name"],
                "type": row["source_type"],
                "host": row["host"],
                "port": row["port"],
                "enabled": row["enabled"],
                "check_method": row["check_method"] or "http",
                "check_url": row["check_url"] or "",
                "description": row["description"] or "",
                "config": cfg,
                "status": "disabled",
                "latency_ms": 0,
                "stats": {},
            })

        enabled = [s for s in sources if s["enabled"]]
        if enabled:
            check_tasks = [
                run_single_check({
                    "source_id": s["id"], "name": s["name"], "source_type": s["type"],
                    "host": s["host"], "port": s["port"],
                    "check_method": s["check_method"], "check_url": s["check_url"],
                })
                for s in enabled
            ]
            results = await asyncio.gather(*check_tasks, return_exceptions=True)
            result_map = {}
            for r in results:
                if isinstance(r, dict):
                    result_map[r["id"]] = r
            for s in sources:
                if s["id"] in result_map:
                    r = result_map[s["id"]]
                    s["status"] = r["status"]
                    s["latency_ms"] = r["latency_ms"]
                    s["stats"] = r.get("stats", {})

        active = [s for s in sources if s["enabled"]]
        healthy = sum(1 for s in active if s["status"] == "healthy")
        degraded = sum(1 for s in active if s["status"] == "degraded")
        error = sum(1 for s in active if s["status"] == "error")

        flow_rows = await conn.fetch(
            "SELECT * FROM po_fabric_flow ORDER BY flow_id"
        )
        flows = [
            {
                "id": r["flow_id"],
                "from": r["source_from"],
                "to": r["source_to"],
                "label": r["label"],
                "enabled": r["enabled"],
                "description": r["description"] or "",
            }
            for r in flow_rows
        ]

        quality_data = []
        domain_tables = {
            "임상": ["person", "visit_occurrence", "condition_occurrence"],
            "검사": ["measurement"],
            "영상": ["observation"],
            "원무": ["cost", "payer_plan_period"],
            "약물": ["drug_exposure", "drug_era"],
        }
        for domain, tables in domain_tables.items():
            total_cols = 0
            filled_cols = 0
            issues = 0
            for tbl in tables:
                try:
                    cols = await conn.fetch(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema='public' AND table_name=$1", tbl
                    )
                    if not cols:
                        continue
                    total_cols += len(cols)
                    col_exprs = ", ".join(
                        f"COUNT({c['column_name']}) AS c{i}"
                        for i, c in enumerate(cols)
                    )
                    row = await conn.fetchrow(
                        f"SELECT COUNT(*) AS total, {col_exprs} "
                        f"FROM (SELECT * FROM {tbl} LIMIT 100) sub"
                    )
                    if row and row["total"] > 0:
                        tc = row["total"]
                        for i in range(len(cols)):
                            ratio = row[f"c{i}"] / tc
                            if ratio >= 0.9:
                                filled_cols += 1
                            elif ratio < 0.5:
                                issues += 1
                except Exception:
                    pass
            score = round(filled_cols / max(total_cols, 1) * 100, 1)
            quality_data.append({"domain": domain, "score": score, "issues": issues})

        source_count = await conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='public' AND table_type='BASE TABLE'"
        ) or 0

        result = {
            "sources": sources,
            "flows": flows,
            "summary": {
                "total": len(active),
                "healthy": healthy,
                "degraded": degraded,
                "error": error,
            },
            "quality_data": quality_data,
            "source_count": source_count,
        }
        cached_set("fabric-stats", result)
        return result
    finally:
        await release_connection(conn)


# ── 소스 CRUD ──

@router.post("/fabric-source")
async def create_fabric_source(body: FabricSourceCreate):
    conn = await get_connection()
    try:
        await _ensure_fabric_tables(conn)
        exists = await conn.fetchval(
            "SELECT 1 FROM po_fabric_source WHERE source_id=$1", body.source_id
        )
        if exists:
            raise HTTPException(409, f"소스 ID '{body.source_id}'가 이미 존재합니다")
        await conn.execute(
            "INSERT INTO po_fabric_source "
            "(source_id,name,source_type,host,port,enabled,check_method,check_url,description,config) "
            "VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)",
            body.source_id, body.name, body.source_type, body.host, body.port,
            body.enabled, body.check_method, body.check_url, body.description,
            json.dumps(body.config),
        )
        cached_pop("fabric-stats")
        return {"ok": True, "source_id": body.source_id}
    finally:
        await release_connection(conn)


@router.put("/fabric-source/{source_id}")
async def update_fabric_source(source_id: str, body: FabricSourceUpdate):
    conn = await get_connection()
    try:
        await _ensure_fabric_tables(conn)
        exists = await conn.fetchval(
            "SELECT 1 FROM po_fabric_source WHERE source_id=$1", source_id
        )
        if not exists:
            raise HTTPException(404, f"소스 '{source_id}'을 찾을 수 없습니다")
        updates = []
        params = []
        idx = 1
        for field in ["name", "source_type", "host", "port", "enabled",
                       "check_method", "check_url", "description"]:
            val = getattr(body, field, None)
            if val is not None:
                updates.append(f"{field}=${idx}")
                params.append(val)
                idx += 1
        if body.config is not None:
            updates.append(f"config=${idx}::jsonb")
            params.append(json.dumps(body.config))
            idx += 1
        if not updates:
            return {"ok": True, "changed": 0}
        updates.append("updated_at=NOW()")
        params.append(source_id)
        await conn.execute(
            f"UPDATE po_fabric_source SET {', '.join(updates)} WHERE source_id=${idx}",
            *params,
        )
        cached_pop("fabric-stats")
        return {"ok": True, "source_id": source_id}
    finally:
        await release_connection(conn)


@router.delete("/fabric-source/{source_id}")
async def delete_fabric_source(source_id: str):
    conn = await get_connection()
    try:
        await _ensure_fabric_tables(conn)
        deleted = await conn.execute(
            "DELETE FROM po_fabric_source WHERE source_id=$1", source_id
        )
        if deleted == "DELETE 0":
            raise HTTPException(404, f"소스 '{source_id}'을 찾을 수 없습니다")
        await conn.execute(
            "DELETE FROM po_fabric_flow WHERE source_from=$1 OR source_to=$1", source_id
        )
        cached_pop("fabric-stats")
        return {"ok": True, "source_id": source_id}
    finally:
        await release_connection(conn)


# ── 개별 소스 연결 테스트 ──

@router.post("/fabric-test/{source_id}")
async def test_fabric_source(source_id: str):
    """캐시 무시하고 단일 소스 라이브 헬스체크"""
    conn = await get_connection()
    try:
        await _ensure_fabric_tables(conn)
        row = await conn.fetchrow(
            "SELECT * FROM po_fabric_source WHERE source_id=$1", source_id
        )
        if not row:
            raise HTTPException(404, f"소스 '{source_id}'을 찾을 수 없습니다")
        return await run_single_check(dict(row))
    finally:
        await release_connection(conn)


@router.post("/fabric-test-all")
async def test_all_fabric_sources():
    """캐시 무시하고 모든 활성 소스 라이브 헬스체크"""
    conn = await get_connection()
    try:
        await _ensure_fabric_tables(conn)
        rows = await conn.fetch(
            "SELECT * FROM po_fabric_source WHERE enabled=TRUE ORDER BY created_at"
        )
        tasks = [run_single_check(dict(r)) for r in rows]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        sources = [r for r in results if isinstance(r, dict)]
        return {"sources": sources}
    finally:
        await release_connection(conn)


@router.post("/fabric-cache-clear")
async def clear_fabric_cache():
    """패브릭 통계 캐시 강제 삭제"""
    cached_pop("fabric-stats")
    return {"ok": True, "message": "캐시가 삭제되었습니다"}


# ── 흐름 CRUD ──

@router.post("/fabric-flow")
async def create_fabric_flow(body: FabricFlowCreate):
    conn = await get_connection()
    try:
        await _ensure_fabric_tables(conn)
        flow_id = await conn.fetchval(
            "INSERT INTO po_fabric_flow (source_from,source_to,label,enabled,description) "
            "VALUES ($1,$2,$3,$4,$5) RETURNING flow_id",
            body.source_from, body.source_to, body.label, body.enabled, body.description,
        )
        cached_pop("fabric-stats")
        return {"ok": True, "flow_id": flow_id}
    finally:
        await release_connection(conn)


@router.put("/fabric-flow/{flow_id}")
async def update_fabric_flow(flow_id: int, body: FabricFlowUpdate):
    conn = await get_connection()
    try:
        await _ensure_fabric_tables(conn)
        exists = await conn.fetchval(
            "SELECT 1 FROM po_fabric_flow WHERE flow_id=$1", flow_id
        )
        if not exists:
            raise HTTPException(404, f"흐름 ID {flow_id}을 찾을 수 없습니다")
        updates = []
        params = []
        idx = 1
        for field in ["source_from", "source_to", "label", "enabled", "description"]:
            val = getattr(body, field, None)
            if val is not None:
                updates.append(f"{field}=${idx}")
                params.append(val)
                idx += 1
        if not updates:
            return {"ok": True, "changed": 0}
        params.append(flow_id)
        await conn.execute(
            f"UPDATE po_fabric_flow SET {', '.join(updates)} WHERE flow_id=${idx}",
            *params,
        )
        cached_pop("fabric-stats")
        return {"ok": True, "flow_id": flow_id}
    finally:
        await release_connection(conn)


@router.delete("/fabric-flow/{flow_id}")
async def delete_fabric_flow(flow_id: int):
    conn = await get_connection()
    try:
        await _ensure_fabric_tables(conn)
        deleted = await conn.execute(
            "DELETE FROM po_fabric_flow WHERE flow_id=$1", flow_id
        )
        if deleted == "DELETE 0":
            raise HTTPException(404, f"흐름 ID {flow_id}을 찾을 수 없습니다")
        cached_pop("fabric-stats")
        return {"ok": True, "flow_id": flow_id}
    finally:
        await release_connection(conn)
