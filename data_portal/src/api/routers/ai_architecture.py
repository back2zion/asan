"""
AI 인프라 & 아키텍처 운영 API (AAR-002)

MCP 도구 CRUD, 컨테이너 관리, 서비스 설정, 헬스체크
"""
import os
import asyncio
import logging
import subprocess
import copy
from datetime import datetime
from typing import Dict, Any, List, Optional

import aiohttp

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/ai-architecture", tags=["AIArchitecture"])


# ── Pydantic Models ──────────────────────────────────────

class McpToolCreate(BaseModel):
    name: str
    category: str = "custom"
    description: str = ""
    backend_service: str = ""
    data_source: str = ""
    endpoint: str = ""
    enabled: bool = True
    priority: str = "medium"       # high / medium / low
    reference: str = ""            # 참고 URL
    phase: str = ""                # 도입 단계: phase1 / phase2 / future

class McpToolUpdate(BaseModel):
    name: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    backend_service: Optional[str] = None
    data_source: Optional[str] = None
    endpoint: Optional[str] = None
    enabled: Optional[bool] = None
    priority: Optional[str] = None
    reference: Optional[str] = None
    phase: Optional[str] = None

class ServiceConfigUpdate(BaseModel):
    endpoint: Optional[str] = None
    model: Optional[str] = None
    description: Optional[str] = None


# ── 정적 아키텍처 정의 ───────────────────────────────────

SW_COMPONENTS = [
    {
        "id": "mcp",
        "name": "MCP Server",
        "type": "protocol",
        "description": "Model Context Protocol — LLM ↔ 데이터 소스 표준 연결",
        "version": "2.0.0",
        "tools_count": 16,  # updated at runtime in overview endpoint
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
    # Update MCP tools count dynamically
    mcp_comp = next((c for c in SW_COMPONENTS if c["id"] == "mcp"), None)
    if mcp_comp:
        mcp_comp["tools_count"] = len(_mcp_tools)
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
    enabled_count = sum(1 for t in _mcp_tools if t.get("enabled"))
    checks["mcp"] = {"status": "healthy", "tools": len(_mcp_tools), "enabled": enabled_count}

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


# ── Mutable MCP Tools (in-memory) ────────────────────────

_mcp_tools: List[Dict[str, Any]] = [
    # ── 기존 데이터/SQL/거버넌스 도구 (7개) ──
    {"id": 1, "name": "search_catalog", "category": "data", "description": "OMOP CDM 스키마 & 의료 코드 시맨틱 검색",
     "backend_service": "Qdrant RAG", "data_source": "OMOP CDM Schema + Medical Codes",
     "endpoint": "/api/v1/vector/search", "enabled": True,
     "priority": "high", "phase": "deployed", "reference": ""},
    {"id": 2, "name": "generate_sql", "category": "sql", "description": "자연어 → OMOP CDM SQL 변환",
     "backend_service": "LLM Service (XiYan → Qwen3 → Claude fallback)", "data_source": "Schema Linker + M-Schema",
     "endpoint": "/api/v1/text2sql/generate", "enabled": True,
     "priority": "high", "phase": "deployed", "reference": ""},
    {"id": 3, "name": "execute_sql", "category": "sql", "description": "OMOP CDM SQL 실행 및 결과 반환",
     "backend_service": "SQL Executor (docker exec psql)", "data_source": "OMOP CDM (PostgreSQL 13, 92M rows)",
     "endpoint": "/api/v1/text2sql/execute", "enabled": True,
     "priority": "high", "phase": "deployed", "reference": ""},
    {"id": 4, "name": "get_table_info", "category": "data", "description": "테이블 메타데이터 및 통계 조회",
     "backend_service": "pg_stat_user_tables + OMOP Metadata", "data_source": "OMOP CDM",
     "endpoint": "/api/v1/datamart/tables", "enabled": True,
     "priority": "high", "phase": "deployed", "reference": ""},
    {"id": 5, "name": "vector_search", "category": "search", "description": "벡터 유사도 기반 지식 검색",
     "backend_service": "Qdrant v1.12.0", "data_source": "omop_knowledge collection (384d embeddings)",
     "endpoint": "/api/v1/vector/search", "enabled": True,
     "priority": "high", "phase": "deployed", "reference": ""},
    {"id": 6, "name": "get_data_lineage", "category": "governance", "description": "데이터 리니지 추적",
     "backend_service": "OMOP CDM Lineage Registry", "data_source": "Synthea ETL Pipeline Metadata",
     "endpoint": "/api/v1/governance/lineage", "enabled": True,
     "priority": "medium", "phase": "deployed", "reference": ""},
    {"id": 7, "name": "check_data_quality", "category": "governance", "description": "데이터 품질 실시간 검사",
     "backend_service": "SQL Executor (real-time checks)", "data_source": "OMOP CDM (PK uniqueness, NULL rate)",
     "endpoint": "/api/v1/governance/quality", "enabled": True,
     "priority": "medium", "phase": "deployed", "reference": ""},

    # ── 1단계: PACS/EMR 실시간 연동 (즉시) ──
    {"id": 8, "name": "dicom_mcp", "category": "imaging", "description": "DICOM/PACS 쿼리 — 영상 메타데이터 추출, 환자 영상 검색, 시리즈/스터디 조회",
     "backend_service": "dicom-mcp (Orthanc/DICOMweb)", "data_source": "PACS (DICOM RT, CT, MRI, PET)",
     "endpoint": "/dicom/query_patients", "enabled": False,
     "priority": "high", "phase": "phase1", "reference": "https://github.com/christianhinge/dicom-mcp"},
    {"id": 9, "name": "fhir_mcp", "category": "interop", "description": "FHIR R4 환자/처방/관찰/진단 데이터 접근 — EMR 실시간 통합",
     "backend_service": "FHIR MCP Server (HAPI FHIR)", "data_source": "EMR (HL7 FHIR R4 리소스)",
     "endpoint": "/fhir/patient_search", "enabled": False,
     "priority": "high", "phase": "phase1", "reference": "https://mcpmarket.com/server/fhir"},

    # ── 2단계: 연구/데이터 강화 (중기) ──
    {"id": 10, "name": "healthcare_mcp", "category": "knowledge", "description": "PubMed 논문 검색, FDA 약물 정보, 임상시험(ClinicalTrials.gov) 통합 검색",
     "backend_service": "Healthcare MCP Public", "data_source": "PubMed + FDA + ClinicalTrials.gov",
     "endpoint": "/healthcare/pubmed_search", "enabled": False,
     "priority": "medium", "phase": "phase2", "reference": "https://glama.ai/mcp/servers/@Cicatriiz/healthcare-mcp-public"},
    {"id": 11, "name": "omcp", "category": "data", "description": "OMOP CDM 코호트 쿼리, 인구통계 통계, 용어 매핑(SNOMED↔ICD↔KCD)",
     "backend_service": "OMCP Server", "data_source": "OMOP CDM (코호트/Atlas 연동)",
     "endpoint": "/omop/cohort_query", "enabled": False,
     "priority": "medium", "phase": "phase2", "reference": "https://mcpmarket.com/server/omcp-1"},
    {"id": 12, "name": "keragon_healthcare", "category": "workflow", "description": "EHR/청구/스케줄링 300+ 의료 시스템 통합 — 행정 자동화 워크플로",
     "backend_service": "Keragon Healthcare API", "data_source": "EHR + 청구 시스템 + 예약 시스템",
     "endpoint": "/healthcare/ehr_query", "enabled": False,
     "priority": "medium", "phase": "phase2", "reference": "https://www.keragon.com/blog/best-mcp-servers"},

    # ── 향후 확장 (낮음) ──
    {"id": 13, "name": "agentcare_mcp", "category": "emr", "description": "FHIR EMR 기록 요약 — Cerner/Epic 환자 히스토리 자동 분석",
     "backend_service": "AgentCare MCP", "data_source": "FHIR EMR (Cerner, Epic)",
     "endpoint": "/fhir/emr_patient_history", "enabled": False,
     "priority": "low", "phase": "future", "reference": "https://github.com/sunanhe/awesome-medical-mcp-servers"},
    {"id": 14, "name": "suncture_healthcare", "category": "clinical", "description": "증상/질병/약물 분석 — 환자 상담 지원 임상 의사결정",
     "backend_service": "Suncture Healthcare MCP", "data_source": "의약품/질병 지식베이스",
     "endpoint": "/clinical/symptom_checker", "enabled": False,
     "priority": "low", "phase": "future", "reference": "https://mcpmarket.com/server/suncture-healthcare"},
    {"id": 15, "name": "medrxiv_mcp", "category": "research", "description": "medRxiv/bioRxiv 프리프린트 검색 및 요약 — 최신 연구 동향 추적",
     "backend_service": "medRxiv MCP Server", "data_source": "medRxiv + bioRxiv preprints",
     "endpoint": "/medrxiv/search", "enabled": False,
     "priority": "low", "phase": "future", "reference": "https://github.com/sunanhe/awesome-medical-mcp-servers"},
    {"id": 16, "name": "medical_query_mcp", "category": "clinical", "description": "바이탈 사인, 검사 결과, 약물 준수 모니터링 — 실시간 환자 데이터 조회",
     "backend_service": "Medical Query MCP Server", "data_source": "EMR 바이탈/랩/약물 데이터",
     "endpoint": "/patient/get_vitals", "enabled": False,
     "priority": "low", "phase": "future", "reference": "https://lobehub.com/ko/mcp/sp2learn-medical-mcp-server"},
]
_mcp_next_id = 17


@router.get("/mcp-topology")
async def mcp_topology():
    """MCP Tool 토폴로지 — 도구별 연결 서비스 매핑"""
    total = len(_mcp_tools)
    enabled = sum(1 for t in _mcp_tools if t.get("enabled"))
    by_phase = {}
    for t in _mcp_tools:
        ph = t.get("phase", "unknown")
        by_phase.setdefault(ph, 0)
        by_phase[ph] += 1
    by_category = {}
    for t in _mcp_tools:
        cat = t.get("category", "custom")
        by_category.setdefault(cat, 0)
        by_category[cat] += 1
    return {
        "server": {"name": "IDP MCP Server", "protocol_version": "2024-11-05"},
        "tools": list(_mcp_tools),
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
    global _mcp_next_id
    tool = {"id": _mcp_next_id, **body.model_dump(), "status": "active"}
    _mcp_next_id += 1
    _mcp_tools.append(tool)
    return tool


@router.put("/mcp-tools/{tool_id}")
async def update_mcp_tool(tool_id: int, body: McpToolUpdate):
    """MCP 도구 설정 수정"""
    tool = next((t for t in _mcp_tools if t["id"] == tool_id), None)
    if not tool:
        raise HTTPException(404, "Tool not found")
    for k, v in body.model_dump(exclude_none=True).items():
        tool[k] = v
    return tool


@router.delete("/mcp-tools/{tool_id}")
async def delete_mcp_tool(tool_id: int):
    """MCP 도구 삭제"""
    global _mcp_tools
    before = len(_mcp_tools)
    _mcp_tools = [t for t in _mcp_tools if t["id"] != tool_id]
    if len(_mcp_tools) == before:
        raise HTTPException(404, "Tool not found")
    return {"success": True}


@router.post("/mcp-tools/{tool_id}/test")
async def test_mcp_tool(tool_id: int):
    """MCP 도구 연결 테스트"""
    tool = next((t for t in _mcp_tools if t["id"] == tool_id), None)
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
    comp = next((c for c in SW_COMPONENTS if c["id"] == service_id), None)
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
    comp = next((c for c in SW_COMPONENTS if c["id"] == service_id), None)
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
