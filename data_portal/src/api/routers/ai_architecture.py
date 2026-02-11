"""
AI 인프라 & 아키텍처 운영 API (AAR-002)

MCP 도구 CRUD, 컨테이너 관리, 서비스 설정, 헬스체크
"""
import asyncio
import logging
import time as _time
from datetime import datetime

import aiohttp

from fastapi import APIRouter, HTTPException

from . import _ai_architecture_data as _data
from ._ai_architecture_data import (
    McpToolCreate,
    McpToolUpdate,
    ServiceConfigUpdate,
)
from services.redis_cache import cached

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ai-architecture", tags=["AIArchitecture"])


@router.get("/overview")
async def architecture_overview():
    """전체 AI 아키텍처 개요"""
    # Update MCP tools count dynamically
    mcp_comp = next((c for c in _data.SW_COMPONENTS if c["id"] == "mcp"), None)
    if mcp_comp:
        mcp_comp["tools_count"] = len(_data._mcp_tools)
    return {
        "sw_architecture": {
            "components": _data.SW_COMPONENTS,
            "patterns": [
                "MCP (Model Context Protocol) — 표준 프로토콜로 LLM ↔ 데이터 소스 연결",
                "RAG (Retrieval-Augmented Generation) — Milvus 벡터 DB 기반 검색 증강",
                "LangGraph Orchestration — StateGraph 멀티 에이전트 워크플로우",
                "Multi-provider LLM — XiYan / Qwen3 / Claude / Gemini fallback chain",
            ],
        },
        "hw_infrastructure": {
            "gpu_resources": _data.HW_COMPONENTS,
            "total_gpu_memory_gb": sum(g["memory_gb"] for g in _data.HW_COMPONENTS),
        },
        "container_infrastructure": {
            "stacks": _data.CONTAINER_STACKS,
            "total_services": sum(len(s["services"]) for s in _data.CONTAINER_STACKS),
            "orchestration": "Docker Compose (5 stacks)",
        },
    }


@router.get("/health")
async def architecture_health():
    """각 컴포넌트 헬스체크 (30초 캐시)"""
    now = _time.time()
    if _data._health_cache["data"] and (now - _data._health_cache["ts"]) < _data._HEALTH_TTL:
        return _data._health_cache["data"]
    checks = {}

    # MCP
    enabled_count = sum(1 for t in _data._mcp_tools if t.get("enabled"))
    checks["mcp"] = {"status": "healthy", "tools": len(_data._mcp_tools), "enabled": enabled_count}

    # RAG / Milvus
    try:
        from ai_services.rag.retriever import get_retriever
        retriever = get_retriever()
        checks["rag"] = {
            "status": "healthy" if retriever._initialized else "initializing",
            "vector_db": "milvus",
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

    result = {"overall": overall, "components": checks, "checked_at": datetime.utcnow().isoformat()}
    _data._health_cache["data"] = result
    _data._health_cache["ts"] = _time.time()
    return result


@router.get("/containers")
async def container_status():
    """Docker 컨테이너 실행 상태 (30초 캐시)"""
    now = _time.time()
    if _data._container_cache["data"] and (now - _data._container_cache["ts"]) < _data._CONTAINER_TTL:
        return _data._container_cache["data"]
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

    result = {"containers": containers, "count": len(containers)}
    _data._container_cache["data"] = result
    _data._container_cache["ts"] = _time.time()
    return result


@router.get("/mcp-topology")
@cached("mcp-topology", ttl=300)
async def mcp_topology():
    """MCP Tool 토폴로지 — 도구별 연결 서비스 매핑"""
    total = len(_data._mcp_tools)
    enabled = sum(1 for t in _data._mcp_tools if t.get("enabled"))
    by_phase = {}
    for t in _data._mcp_tools:
        ph = t.get("phase", "unknown")
        by_phase.setdefault(ph, 0)
        by_phase[ph] += 1
    by_category = {}
    for t in _data._mcp_tools:
        cat = t.get("category", "custom")
        by_category.setdefault(cat, 0)
        by_category[cat] += 1
    return {
        "server": {"name": "IDP MCP Server", "protocol_version": "2024-11-05"},
        "tools": list(_data._mcp_tools),
        "summary": {
            "total": total,
            "enabled": enabled,
            "by_phase": by_phase,
            "by_category": by_category,
        },
    }


@router.post("/mcp-tools")
async def create_mcp_tool(body: McpToolCreate):
    """MCP 도구 추가"""
    tool = {"id": _data._mcp_next_id, **body.model_dump(), "status": "active"}
    _data._mcp_next_id += 1
    _data._mcp_tools.append(tool)
    return tool


@router.put("/mcp-tools/{tool_id}")
async def update_mcp_tool(tool_id: int, body: McpToolUpdate):
    """MCP 도구 설정 수정"""
    tool = next((t for t in _data._mcp_tools if t["id"] == tool_id), None)
    if not tool:
        raise HTTPException(404, "Tool not found")
    for k, v in body.model_dump(exclude_none=True).items():
        tool[k] = v
    return tool


@router.delete("/mcp-tools/{tool_id}")
async def delete_mcp_tool(tool_id: int):
    """MCP 도구 삭제"""
    before = len(_data._mcp_tools)
    _data._mcp_tools[:] = [t for t in _data._mcp_tools if t["id"] != tool_id]
    if len(_data._mcp_tools) == before:
        raise HTTPException(404, "Tool not found")
    return {"success": True}


@router.post("/mcp-tools/{tool_id}/test")
async def test_mcp_tool(tool_id: int):
    """MCP 도구 연결 테스트"""
    tool = next((t for t in _data._mcp_tools if t["id"] == tool_id), None)
    if not tool:
        raise HTTPException(404, "Tool not found")
    endpoint = tool.get("endpoint", "")
    # 실제 엔드포인트에 간이 연결 테스트
    start = datetime.utcnow()
    try:
        async with aiohttp.ClientSession() as session:
            url = f"http://127.0.0.1:8000{endpoint}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                latency = (datetime.utcnow() - start).total_seconds() * 1000
                return {"success": resp.status < 500, "status_code": resp.status, "latency_ms": round(latency, 1), "tool": tool["name"]}
    except Exception as e:
        latency = (datetime.utcnow() - start).total_seconds() * 1000
        return {"success": False, "error": str(e), "latency_ms": round(latency, 1), "tool": tool["name"]}


@router.post("/services/{service_id}/test")
async def test_service(service_id: str):
    """서비스 연결 테스트"""
    comp = next((c for c in _data.SW_COMPONENTS if c["id"] == service_id), None)
    if not comp:
        raise HTTPException(404, "Service not found")
    endpoint = comp.get("endpoint")
    if not endpoint:
        return {"success": True, "message": "No endpoint to test", "service": service_id}
    start = datetime.utcnow()
    try:
        async with aiohttp.ClientSession() as session:
            url = f"http://127.0.0.1:8000{endpoint}"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                latency = (datetime.utcnow() - start).total_seconds() * 1000
                return {"success": resp.status < 500, "status_code": resp.status, "latency_ms": round(latency, 1), "service": service_id}
    except Exception as e:
        latency = (datetime.utcnow() - start).total_seconds() * 1000
        return {"success": False, "error": str(e), "latency_ms": round(latency, 1), "service": service_id}


@router.put("/services/{service_id}")
async def update_service(service_id: str, body: ServiceConfigUpdate):
    """서비스 설정 수정"""
    comp = next((c for c in _data.SW_COMPONENTS if c["id"] == service_id), None)
    if not comp:
        raise HTTPException(404, "Service not found")
    for k, v in body.model_dump(exclude_none=True).items():
        comp[k] = v
    return comp


@router.post("/containers/{name}/action")
async def container_action(name: str, action: str = "restart"):
    """컨테이너 작업 (restart/stop/start)"""
    if action not in ("restart", "stop", "start"):
        raise HTTPException(400, "Invalid action")
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", action, name,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30.0)
        return {"success": proc.returncode == 0, "container": name, "action": action, "output": stdout.decode().strip() or stderr.decode().strip()}
    except asyncio.TimeoutError:
        return {"success": False, "container": name, "action": action, "error": "Timeout (30s)"}
    except Exception as e:
        return {"success": False, "container": name, "action": action, "error": str(e)}


@router.get("/containers/{name}/logs")
async def container_logs(name: str, tail: int = 100):
    """컨테이너 최근 로그 조회"""
    try:
        proc = await asyncio.create_subprocess_exec(
            "docker", "logs", "--tail", str(tail), name,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10.0)
        logs = stdout.decode() + stderr.decode()
        return {"container": name, "logs": logs[-5000:], "lines": tail}
    except Exception as e:
        raise HTTPException(500, str(e))
