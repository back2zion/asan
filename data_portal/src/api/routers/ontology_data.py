"""
Medical Ontology Knowledge Graph — Data layer

DB queries, graph construction, cache management, Neo4j/RDF export.
Constants and reference data are in ontology_constants.py.
"""

import os
import json
import time
from datetime import datetime
from typing import Optional, Dict, Any
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import asyncpg

from services.db_pool import get_pool
from services.redis_cache import cache_get as redis_get, cache_set as redis_set

from ._ontology_graph_builder import (
    _build_full_ontology, _generate_cypher, _generate_rdf_triples,
)

router = APIRouter()

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}


async def _get_conn():
    try:
        pool = await get_pool()
        return await pool.acquire()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")


async def _release_conn(conn):
    try:
        pool = await get_pool()
        await pool.release(conn)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════
#  GRAPH CACHE
# ══════════════════════════════════════════════════════════════════════

_GRAPH_CACHE: Dict[str, Any] = {"data": None, "built_at": 0}
_CACHE_TTL = 600  # 10 minutes

# Persistent disk cache — survives server restarts
_CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "etl_data")
_DISK_CACHE_FILE = os.path.join(_CACHE_DIR, "ontology_cache.json")


def _save_disk_cache(graph: Dict[str, Any]) -> None:
    """Save built graph to disk for persistence across restarts"""
    try:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(_DISK_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(graph, f, ensure_ascii=False)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Disk cache save failed: {e}")


def _load_disk_cache() -> Optional[Dict[str, Any]]:
    """Load graph from disk cache if available and not too old (24h)"""
    try:
        if not os.path.exists(_DISK_CACHE_FILE):
            return None
        age = time.time() - os.path.getmtime(_DISK_CACHE_FILE)
        if age > 86400:
            return None
        with open(_DISK_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


async def warm_ontology_cache() -> None:
    """Pre-build ontology cache (call on startup). Safe to call multiple times."""
    global _GRAPH_CACHE
    import logging
    log = logging.getLogger(__name__)

    # 1) Try disk cache first (instant)
    if not _GRAPH_CACHE["data"]:
        disk = _load_disk_cache()
        if disk:
            _GRAPH_CACHE["data"] = disk
            _GRAPH_CACHE["built_at"] = time.time()
            log.info("Ontology cache loaded from disk (instant)")

    # 2) Build fresh in background regardless (updates disk + memory)
    try:
        log.info("Ontology cache warming: building full graph...")
        t0 = time.time()
        graph = await _build_full_ontology(_get_conn, _release_conn)
        elapsed = time.time() - t0
        _GRAPH_CACHE["data"] = graph
        _GRAPH_CACHE["built_at"] = time.time()
        _save_disk_cache(graph)
        await redis_set("ontology-graph", graph, 600)  # 10분 TTL
        log.info(f"Ontology cache warmed in {elapsed:.1f}s ({len(graph['nodes'])} nodes, {len(graph['links'])} links)")
    except Exception as e:
        log.warning(f"Ontology cache warming failed: {e}")


async def get_or_build_graph(force_refresh: bool = False) -> Dict[str, Any]:
    """Get cached graph or build fresh one. Shared by all endpoints."""
    global _GRAPH_CACHE

    now = time.time()
    # 1) 모듈 인메모리 캐시
    if not force_refresh and _GRAPH_CACHE["data"] and (now - _GRAPH_CACHE["built_at"]) < _CACHE_TTL:
        return _GRAPH_CACHE["data"]

    # 2) Redis 캐시 (멀티 워커 공유, 서버 재시작 후에도 유지)
    if not force_refresh:
        redis_data = await redis_get("ontology-graph")
        if redis_data:
            _GRAPH_CACHE["data"] = redis_data
            _GRAPH_CACHE["built_at"] = now
            return redis_data

    # 3) 디스크 캐시
    if not force_refresh:
        disk = _load_disk_cache()
        if disk:
            _GRAPH_CACHE["data"] = disk
            _GRAPH_CACHE["built_at"] = now
            await redis_set("ontology-graph", disk, 600)  # 10분 TTL
            return disk

    # 4) DB에서 새로 빌드
    graph = await _build_full_ontology(_get_conn, _release_conn)
    _GRAPH_CACHE["data"] = graph
    _GRAPH_CACHE["built_at"] = now
    _save_disk_cache(graph)
    await redis_set("ontology-graph", graph, 600)  # 10분 TTL
    return graph


# ══════════════════════════════════════════════════════════════════════
#  ENDPOINT: Neo4j export
# ══════════════════════════════════════════════════════════════════════

@router.get("/neo4j-export")
async def export_neo4j_cypher():
    """Neo4j Cypher 내보내기 — 온톨로지를 Neo4j로 가져오기 위한 Cypher 스크립트"""
    graph = await get_or_build_graph()
    cypher = _generate_cypher(graph)

    return {
        "cypher": cypher,
        "nodes_count": len(graph["nodes"]),
        "relationships_count": len(graph["links"]),
        "instructions": {
            "step1": "Neo4j Desktop 또는 Aura에서 새 데이터베이스 생성",
            "step2": "Neo4j Browser에서 위 Cypher 스크립트 실행",
            "step3": "CALL db.schema.visualization() 으로 그래프 스키마 확인",
            "step4": "MATCH (n)-[r]->(m) RETURN n,r,m LIMIT 200 으로 시각화",
        },
        "size_estimate": f"{len(cypher) / 1024:.1f} KB",
    }


# ══════════════════════════════════════════════════════════════════════
#  NEO4J LIVE PUSH / STATUS
# ══════════════════════════════════════════════════════════════════════

@router.post("/neo4j-push")
async def neo4j_push():
    """OMOP CDM 온톨로지 그래프를 Neo4j에 실제 적재"""
    from core.config import settings

    try:
        from neo4j import GraphDatabase
    except ImportError:
        raise HTTPException(status_code=503, detail="neo4j Python 드라이버 미설치 (pip install neo4j)")

    # Get the existing graph data
    graph = await get_or_build_graph()

    driver = GraphDatabase.driver(settings.NEO4J_URI, auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD))

    try:
        with driver.session() as session:
            # Clear existing
            session.run("MATCH (n) DETACH DELETE n")

            # Create nodes
            node_count = 0
            for node in graph.get("nodes", []):
                session.run(
                    "CREATE (n:OmopTable {name: $name, category: $category, row_count: $row_count})",
                    name=node.get("id", ""),
                    category=node.get("type", ""),
                    row_count=node.get("row_count", 0),
                )
                node_count += 1

            # Create edges
            edge_count = 0
            for link in graph.get("links", []):
                session.run(
                    """
                    MATCH (a:OmopTable {name: $source}), (b:OmopTable {name: $target})
                    CREATE (a)-[:FK {column: $column}]->(b)
                    """,
                    source=link.get("source", ""),
                    target=link.get("target", ""),
                    column=link.get("label", ""),
                )
                edge_count += 1

            return {"status": "success", "nodes_created": node_count, "edges_created": edge_count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Neo4j push failed: {e}")
    finally:
        driver.close()


@router.get("/neo4j-status")
async def neo4j_status():
    """Neo4j 연결 상태 확인"""
    from core.config import settings
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(settings.NEO4J_URI, auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD))
        with driver.session() as session:
            result = session.run("RETURN 1 AS ok")
            record = result.single()
            node_count = session.run("MATCH (n) RETURN count(n) AS cnt").single()["cnt"]
        driver.close()
        return {"status": "connected", "uri": settings.NEO4J_URI, "node_count": node_count}
    except ImportError:
        return {"status": "driver_missing", "detail": "pip install neo4j"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


# ══════════════════════════════════════════════════════════════════════
#  MANAGEMENT ENDPOINTS
# ══════════════════════════════════════════════════════════════════════

@router.post("/cache-refresh")
async def refresh_ontology_cache():
    """온톨로지 캐시 강제 갱신 (메모리 + 디스크)"""
    global _GRAPH_CACHE
    import time as _t
    t0 = _t.time()
    graph = await _build_full_ontology(_get_conn, _release_conn)
    elapsed = _t.time() - t0
    _GRAPH_CACHE["data"] = graph
    _GRAPH_CACHE["built_at"] = _t.time()
    _save_disk_cache(graph)
    await redis_set("ontology-graph", graph, 600)  # 10분 TTL
    return {
        "success": True,
        "elapsed_seconds": round(elapsed, 1),
        "nodes": len(graph["nodes"]),
        "links": len(graph["links"]),
    }


@router.post("/cache-clear")
async def clear_ontology_cache():
    """온톨로지 메모리 캐시 삭제 (다음 요청 시 재구축)"""
    global _GRAPH_CACHE
    _GRAPH_CACHE = {"data": None, "built_at": 0}
    return {"success": True, "message": "캐시가 초기화되었습니다"}


# Custom annotation table
async def _ensure_annotation_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS ontology_annotation (
            annotation_id SERIAL PRIMARY KEY,
            node_id VARCHAR(200) NOT NULL,
            note TEXT NOT NULL,
            author VARCHAR(100) DEFAULT 'admin',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)


class AnnotationCreate(BaseModel):
    node_id: str = Field(..., max_length=200)
    note: str = Field(..., max_length=2000)
    author: str = Field(default="admin", max_length=100)


class AnnotationUpdate(BaseModel):
    note: str = Field(..., max_length=2000)


@router.get("/annotations")
async def list_annotations(node_id: Optional[str] = Query(None)):
    """노드 주석 목록 조회"""
    conn = await _get_conn()
    try:
        await _ensure_annotation_table(conn)
        if node_id:
            rows = await conn.fetch(
                "SELECT * FROM ontology_annotation WHERE node_id=$1 ORDER BY created_at DESC", node_id
            )
        else:
            rows = await conn.fetch("SELECT * FROM ontology_annotation ORDER BY created_at DESC LIMIT 100")
        return {
            "annotations": [
                {
                    "annotation_id": r["annotation_id"],
                    "node_id": r["node_id"],
                    "note": r["note"],
                    "author": r["author"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await _release_conn(conn)


@router.post("/annotations")
async def create_annotation(body: AnnotationCreate):
    """노드 주석 추가"""
    conn = await _get_conn()
    try:
        await _ensure_annotation_table(conn)
        aid = await conn.fetchval("""
            INSERT INTO ontology_annotation (node_id, note, author)
            VALUES ($1, $2, $3) RETURNING annotation_id
        """, body.node_id, body.note, body.author)
        return {"success": True, "annotation_id": aid}
    finally:
        await _release_conn(conn)


@router.put("/annotations/{annotation_id}")
async def update_annotation(annotation_id: int, body: AnnotationUpdate):
    """노드 주석 수정"""
    conn = await _get_conn()
    try:
        result = await conn.execute(
            "UPDATE ontology_annotation SET note=$1 WHERE annotation_id=$2",
            body.note, annotation_id,
        )
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="주석을 찾을 수 없습니다")
        return {"success": True, "annotation_id": annotation_id}
    finally:
        await _release_conn(conn)


@router.delete("/annotations/{annotation_id}")
async def delete_annotation(annotation_id: int):
    """노드 주석 삭제"""
    conn = await _get_conn()
    try:
        result = await conn.execute(
            "DELETE FROM ontology_annotation WHERE annotation_id=$1", annotation_id
        )
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="주석을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await _release_conn(conn)
