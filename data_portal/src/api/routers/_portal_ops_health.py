"""
포털 데이터 패브릭 — 서비스 헬스체크 함수 모음
Built-in 8개 서비스 + 범용 HTTP/TCP 체커
"""
import time
import asyncio

import asyncpg
import httpx
import psycopg2

from ._portal_ops_shared import OMOP_DB_CONFIG

HEALTH_TIMEOUT = 3.0


async def check_omop() -> dict:
    """OMOP CDM PostgreSQL health check"""
    t0 = time.time()
    try:
        conn = await asyncio.wait_for(
            asyncpg.connect(**OMOP_DB_CONFIG), timeout=HEALTH_TIMEOUT
        )
        try:
            await conn.fetchval("SELECT 1")
            latency = round((time.time() - t0) * 1000)
            stats_rows = await conn.fetch(
                "SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE schemaname='public'"
            )
            table_count = len(stats_rows)
            total_rows = sum(r["n_live_tup"] for r in stats_rows)
            patient_count = 0
            for r in stats_rows:
                if r["relname"] == "person":
                    patient_count = r["n_live_tup"]
                    break
            if total_rows >= 1_000_000:
                rows_str = f"{round(total_rows / 1_000_000, 1)}M"
            else:
                rows_str = f"{total_rows:,}"
            return {
                "id": "omop-cdm", "name": "OMOP CDM", "type": "PostgreSQL",
                "port": OMOP_DB_CONFIG["port"], "status": "healthy",
                "latency_ms": latency,
                "stats": {"tables": table_count, "patients": patient_count, "total_rows": rows_str},
            }
        finally:
            await conn.close()
    except Exception:
        return {
            "id": "omop-cdm", "name": "OMOP CDM", "type": "PostgreSQL",
            "port": OMOP_DB_CONFIG["port"], "status": "error",
            "latency_ms": 0, "stats": {},
        }


async def check_milvus() -> dict:
    """Milvus Vector DB health check"""
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT) as c:
            r = await c.get("http://localhost:9091/healthz")
            latency = round((time.time() - t0) * 1000)
            status = "healthy" if r.status_code == 200 else "degraded"
            return {
                "id": "milvus", "name": "Milvus Vector DB", "type": "Vector Store",
                "port": 19530, "status": status, "latency_ms": latency,
                "stats": {"collections": 1, "dimension": 384},
            }
    except Exception:
        return {
            "id": "milvus", "name": "Milvus Vector DB", "type": "Vector Store",
            "port": 19530, "status": "error", "latency_ms": 0, "stats": {},
        }


async def check_minio() -> dict:
    """MinIO S3 health check"""
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT) as c:
            r = await c.get("http://localhost:19000/minio/health/live")
            latency = round((time.time() - t0) * 1000)
            status = "healthy" if r.status_code == 200 else "degraded"
            return {
                "id": "minio", "name": "MinIO S3", "type": "Object Storage",
                "port": 19000, "status": status, "latency_ms": latency,
                "stats": {"buckets": 5},
            }
    except Exception:
        return {
            "id": "minio", "name": "MinIO S3", "type": "Object Storage",
            "port": 19000, "status": "error", "latency_ms": 0, "stats": {},
        }


async def check_llm(name: str, src_id: str, port: int, model_name: str, gpu_mb: int) -> dict:
    """vLLM-style LLM health check (GET /v1/models)"""
    t0 = time.time()
    stype = "Text-to-SQL LLM" if "SQL" in name else "General LLM"
    try:
        async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT) as c:
            r = await c.get(f"http://localhost:{port}/v1/models")
            latency = round((time.time() - t0) * 1000)
            status = "healthy" if r.status_code == 200 else "degraded"
            return {
                "id": src_id, "name": name, "type": stype,
                "port": port, "status": status, "latency_ms": latency,
                "stats": {"model": model_name, "gpu_mb": gpu_mb},
            }
    except Exception:
        return {
            "id": src_id, "name": name, "type": stype,
            "port": port, "status": "error", "latency_ms": 0,
            "stats": {"model": model_name, "gpu_mb": gpu_mb},
        }


async def check_ner() -> dict:
    """Medical NER (BioClinicalBERT) health check"""
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT) as c:
            r = await c.get("http://localhost:28100/ner/health")
            latency = round((time.time() - t0) * 1000)
            status = "healthy" if r.status_code == 200 else "degraded"
            return {
                "id": "ner", "name": "BioClinicalBERT", "type": "Medical NER",
                "port": 28100, "status": status, "latency_ms": latency,
                "stats": {"model": "biomedical-ner-all", "gpu_mb": 287},
            }
    except Exception:
        return {
            "id": "ner", "name": "BioClinicalBERT", "type": "Medical NER",
            "port": 28100, "status": "error", "latency_ms": 0,
            "stats": {"model": "biomedical-ner-all", "gpu_mb": 287},
        }


async def check_superset() -> dict:
    """Superset PostgreSQL health check (psycopg2 — sync, via to_thread)"""
    t0 = time.time()
    try:
        def _sync_check():
            conn = psycopg2.connect(
                host="localhost", port=15432,
                user="superset", password="superset", dbname="superset",
                connect_timeout=3,
            )
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
            finally:
                conn.close()

        await asyncio.wait_for(asyncio.to_thread(_sync_check), timeout=HEALTH_TIMEOUT)
        latency = round((time.time() - t0) * 1000)
        return {
            "id": "superset", "name": "Superset DB", "type": "BI Analytics",
            "port": 15432, "status": "healthy", "latency_ms": latency,
            "stats": {"charts": 6, "dashboards": 1, "datasets": 27},
        }
    except Exception:
        return {
            "id": "superset", "name": "Superset DB", "type": "BI Analytics",
            "port": 15432, "status": "error", "latency_ms": 0, "stats": {},
        }


async def check_jupyterlab() -> dict:
    """JupyterLab health check"""
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT) as c:
            r = await c.get("http://localhost:18888/api/status")
            latency = round((time.time() - t0) * 1000)
            status = "healthy" if r.status_code == 200 else "degraded"
            return {
                "id": "jupyterlab", "name": "JupyterLab", "type": "Analysis Env",
                "port": 18888, "status": status, "latency_ms": latency,
                "stats": {},
            }
    except Exception:
        return {
            "id": "jupyterlab", "name": "JupyterLab", "type": "Analysis Env",
            "port": 18888, "status": "error", "latency_ms": 0, "stats": {},
        }


# ── Built-in 헬스체커 레지스트리 ──

BUILTIN_CHECKERS = {
    "omop-cdm": check_omop,
    "milvus": check_milvus,
    "minio": check_minio,
    "xiyan-sql": lambda: check_llm("XiYanSQL", "xiyan-sql", 8001, "QwenCoder-7B", 6200),
    "qwen3": lambda: check_llm("Qwen3-32B", "qwen3", 28888, "Qwen3-32B-AWQ", 22400),
    "ner": check_ner,
    "superset": check_superset,
    "jupyterlab": check_jupyterlab,
}


async def generic_http_check(source_id, name, stype, port, url) -> dict:
    """사용자 추가 소스용 범용 HTTP 헬스체크"""
    t0 = time.time()
    try:
        async with httpx.AsyncClient(timeout=HEALTH_TIMEOUT) as c:
            r = await c.get(url)
            latency = round((time.time() - t0) * 1000)
            status = "healthy" if r.status_code < 400 else "degraded"
            return {
                "id": source_id, "name": name, "type": stype,
                "port": port, "status": status, "latency_ms": latency, "stats": {},
            }
    except Exception:
        return {
            "id": source_id, "name": name, "type": stype,
            "port": port, "status": "error", "latency_ms": 0, "stats": {},
        }


async def generic_tcp_check(source_id, name, stype, host, port) -> dict:
    """사용자 추가 소스용 범용 TCP 연결 체크"""
    t0 = time.time()
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=HEALTH_TIMEOUT
        )
        writer.close()
        await writer.wait_closed()
        latency = round((time.time() - t0) * 1000)
        return {
            "id": source_id, "name": name, "type": stype,
            "port": port, "status": "healthy", "latency_ms": latency, "stats": {},
        }
    except Exception:
        return {
            "id": source_id, "name": name, "type": stype,
            "port": port, "status": "error", "latency_ms": 0, "stats": {},
        }


async def run_single_check(row: dict) -> dict:
    """DB 행 기반으로 적절한 헬스체커 실행"""
    sid = row["source_id"]
    if sid in BUILTIN_CHECKERS:
        return await BUILTIN_CHECKERS[sid]()
    elif row["check_method"] == "http" and row.get("check_url"):
        return await generic_http_check(
            sid, row["name"], row["source_type"], row["port"], row["check_url"],
        )
    else:
        return await generic_tcp_check(
            sid, row["name"], row["source_type"],
            row.get("host", "localhost"), row["port"],
        )
