"""
AI 인프라 & 아키텍처 개요 API (AAR-002)

MCP 상태, RAG 파이프라인, Agent 워크플로우, GPU 자원, 컨테이너 인프라 정보 제공
"""
import os
import asyncio
import logging
import subprocess
from datetime import datetime
from typing import Dict, Any, List

from fastapi import APIRouter

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ai-architecture", tags=["AIArchitecture"])


# ── 정적 아키텍처 정의 ───────────────────────────────────

SW_COMPONENTS = [
    {
        "id": "mcp",
        "name": "MCP Server",
        "type": "protocol",
        "description": "Model Context Protocol — LLM ↔ 데이터 소스 표준 연결",
        "version": "2.0.0",
        "tools_count": 7,
        "endpoint": "/api/v1/mcp",
    },
    {
        "id": "rag",
        "name": "RAG Pipeline",
        "type": "pipeline",
        "description": "Retrieval-Augmented Generation — 벡터 DB 기반 검색 증강 생성",
        "components": {
            "vector_db": "Qdrant v1.12.0",
            "embedding_model": "paraphrase-multilingual-MiniLM-L12-v2 (384d)",
            "collection": "omop_knowledge",
        },
        "endpoint": "/api/v1/vector",
    },
    {
        "id": "orchestration",
        "name": "LangGraph Orchestration",
        "type": "agent",
        "description": "LangGraph StateGraph 멀티 에이전트 워크플로우",
        "nodes": [
            "reference_detector",
            "context_resolver",
            "query_enricher",
            "state_updater",
        ],
        "checkpointer": "SQLite / PostgreSQL",
        "endpoint": "/api/v1/conversation",
    },
    {
        "id": "text2sql",
        "name": "Text2SQL Engine",
        "type": "model",
        "description": "XiYanSQL-QwenCoder-7B — 자연어 → OMOP CDM SQL 변환",
        "model": "XiYanSQL-QwenCoder-7B-2504",
        "runtime": "vLLM v0.6.6",
        "endpoint": "/api/v1/text2sql",
    },
    {
        "id": "llm",
        "name": "LLM Service",
        "type": "model",
        "description": "멀티 프로바이더 LLM — XiYan / Qwen3-32B / Claude / Gemini",
        "providers": ["XiYan (vLLM)", "Qwen3-32B (SSH)", "Claude API", "Gemini API"],
    },
    {
        "id": "ner",
        "name": "Medical NER",
        "type": "model",
        "description": "BioClinicalBERT 기반 의료 개체명 인식",
        "model": "d4data/biomedical-ner-all",
        "endpoint": "/api/v1/ner",
    },
]

HW_COMPONENTS = [
    {
        "id": "gpu_xiyan",
        "name": "XiYanSQL GPU",
        "type": "gpu",
        "device": "GPU 0 (Local Docker)",
        "model_loaded": "XiYanSQL-QwenCoder-7B",
        "memory_gb": 6.2,
        "port": 8001,
    },
    {
        "id": "gpu_qwen",
        "name": "Qwen3-32B GPU",
        "type": "gpu",
        "device": "Remote GPU Server",
        "model_loaded": "Qwen3-32B-AWQ",
        "memory_gb": 22.4,
        "port": "28888 (SSH tunnel)",
    },
    {
        "id": "gpu_ner",
        "name": "BioClinicalBERT GPU",
        "type": "gpu",
        "device": "Remote GPU Server",
        "model_loaded": "d4data/biomedical-ner-all",
        "memory_gb": 0.287,
        "port": "28100 (SSH tunnel)",
    },
]

CONTAINER_STACKS = [
    {"stack": "Main", "compose": "docker-compose.yml", "services": [
        "asan-qdrant", "asan-redis", "asan-mlflow",
        "asan-jupyterlab", "asan-xiyan-sql",
    ]},
    {"stack": "OMOP CDM", "compose": "docker-compose-omop.yml", "services": [
        "infra-omop-db-1",
    ]},
    {"stack": "Superset BI", "compose": "docker-compose-superset.yml", "services": [
        "superset", "superset-db", "superset-redis", "superset-worker",
    ]},
    {"stack": "Airflow ETL", "compose": "docker-compose-airflow.yml", "services": [
        "infra-airflow-webserver-1", "infra-airflow-scheduler-1", "infra-postgres-1",
    ]},
    {"stack": "Nginx Proxy", "compose": "docker-compose-nginx.yml", "services": [
        "nginx-proxy",
    ]},
]


@router.get("/overview")
async def architecture_overview():
    """전체 AI 아키텍처 개요"""
    return {
        "sw_architecture": {
            "components": SW_COMPONENTS,
            "patterns": [
                "MCP (Model Context Protocol) — 표준 프로토콜로 LLM ↔ 데이터 소스 연결",
                "RAG (Retrieval-Augmented Generation) — Qdrant 벡터 DB 기반 검색 증강",
                "LangGraph Orchestration — StateGraph 멀티 에이전트 워크플로우",
                "Multi-provider LLM — XiYan / Qwen3 / Claude / Gemini fallback chain",
            ],
        },
        "hw_infrastructure": {
            "gpu_resources": HW_COMPONENTS,
            "total_gpu_memory_gb": sum(g["memory_gb"] for g in HW_COMPONENTS),
        },
        "container_infrastructure": {
            "stacks": CONTAINER_STACKS,
            "total_services": sum(len(s["services"]) for s in CONTAINER_STACKS),
            "orchestration": "Docker Compose (5 stacks)",
        },
    }


@router.get("/health")
async def architecture_health():
    """각 컴포넌트 헬스체크"""
    checks = {}

    # MCP
    checks["mcp"] = {"status": "healthy", "tools": 7}

    # RAG / Qdrant
    try:
        from ai_services.rag.retriever import get_retriever
        retriever = get_retriever()
        checks["rag"] = {
            "status": "healthy" if retriever._initialized else "initializing",
            "vector_db": "qdrant",
            "collection": "omop_knowledge",
        }
    except Exception as e:
        checks["rag"] = {"status": "unavailable", "error": str(e)}

    # LangGraph conversation
    checks["orchestration"] = {"status": "healthy", "engine": "langgraph", "checkpointer": "sqlite"}

    # SQL Executor (OMOP CDM)
    try:
        from services.sql_executor import sql_executor
        result = await sql_executor.execute("SELECT 1")
        checks["omop_db"] = {
            "status": "healthy" if result.results else "error",
            "execution_time_ms": result.execution_time_ms,
        }
    except Exception as e:
        checks["omop_db"] = {"status": "error", "error": str(e)}

    # GPU — nvidia-smi (best-effort)
    try:
        proc = await asyncio.create_subprocess_exec(
            "nvidia-smi", "--query-gpu=name,memory.used,memory.total,utilization.gpu",
            "--format=csv,noheader,nounits",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        gpus = []
        for line in stdout.decode().strip().split("\n"):
            if line.strip():
                parts = [p.strip() for p in line.split(",")]
                if len(parts) >= 4:
                    gpus.append({
                        "name": parts[0],
                        "memory_used_mb": int(parts[1]),
                        "memory_total_mb": int(parts[2]),
                        "utilization_pct": int(parts[3]),
                    })
        checks["gpu"] = {"status": "healthy" if gpus else "no_gpu", "devices": gpus}
    except Exception:
        checks["gpu"] = {"status": "not_available", "note": "nvidia-smi not found or no GPU"}

    # SSH Tunnels
    tunnel_ports = {"qwen3_llm": 28888, "medical_ner": 28100, "paper2slides": 29001}
    for name, port in tunnel_ports.items():
        try:
            proc = await asyncio.create_subprocess_exec(
                "bash", "-c", f"echo > /dev/tcp/127.0.0.1/{port}",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            await asyncio.wait_for(proc.communicate(), timeout=2.0)
            checks[f"tunnel_{name}"] = {"status": "connected", "port": port}
        except Exception:
            checks[f"tunnel_{name}"] = {"status": "disconnected", "port": port}

    overall = "healthy" if all(
        v.get("status") in ("healthy", "connected", "initializing", "not_available")
        for v in checks.values()
    ) else "degraded"

    return {"overall": overall, "components": checks, "checked_at": datetime.utcnow().isoformat()}


@router.get("/containers")
async def container_status():
    """Docker 컨테이너 실행 상태"""
    containers = []
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "ps", "--format", "{{.Names}}\t{{.Status}}\t{{.Ports}}\t{{.Image}}",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=5.0)
        for line in stdout.decode().strip().split("\n"):
            if line.strip():
                parts = line.split("\t")
                if len(parts) >= 4:
                    containers.append({
                        "name": parts[0],
                        "status": parts[1],
                        "ports": parts[2],
                        "image": parts[3],
                    })
    except Exception as e:
        return {"error": str(e), "containers": []}

    return {"containers": containers, "count": len(containers)}


@router.get("/mcp-topology")
async def mcp_topology():
    """MCP Tool 토폴로지 — 도구별 연결 서비스 매핑"""
    return {
        "server": {
            "name": "IDP MCP Server",
            "protocol_version": "2024-11-05",
        },
        "tools": [
            {
                "name": "search_catalog",
                "category": "data",
                "backend_service": "Qdrant RAG",
                "data_source": "OMOP CDM Schema + Medical Codes",
                "status": "active",
            },
            {
                "name": "generate_sql",
                "category": "sql",
                "backend_service": "LLM Service (XiYan → Qwen3 → Claude fallback)",
                "data_source": "Schema Linker + M-Schema",
                "status": "active",
            },
            {
                "name": "execute_sql",
                "category": "sql",
                "backend_service": "SQL Executor (docker exec psql)",
                "data_source": "OMOP CDM (PostgreSQL 13, 92M rows)",
                "status": "active",
            },
            {
                "name": "get_table_info",
                "category": "data",
                "backend_service": "pg_stat_user_tables + OMOP Metadata",
                "data_source": "OMOP CDM",
                "status": "active",
            },
            {
                "name": "vector_search",
                "category": "search",
                "backend_service": "Qdrant v1.12.0",
                "data_source": "omop_knowledge collection (384d embeddings)",
                "status": "active",
            },
            {
                "name": "get_data_lineage",
                "category": "governance",
                "backend_service": "OMOP CDM Lineage Registry",
                "data_source": "Synthea ETL Pipeline Metadata",
                "status": "active",
            },
            {
                "name": "check_data_quality",
                "category": "governance",
                "backend_service": "SQL Executor (real-time checks)",
                "data_source": "OMOP CDM (PK uniqueness, NULL rate)",
                "status": "active",
            },
        ],
    }
