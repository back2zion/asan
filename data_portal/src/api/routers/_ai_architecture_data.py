"""
AI Architecture - shared data constants, Pydantic models, and MCP tool definitions.
Imported by ai_architecture.py.
"""
from typing import Dict, Any, List, Optional

from pydantic import BaseModel


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
            "vector_db": "Milvus v2.4.0",
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
        "milvus-standalone", "milvus-etcd", "asan-minio",
        "asan-redis", "asan-mlflow",
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


# ── Mutable MCP Tools (in-memory) ────────────────────────

_mcp_tools: List[Dict[str, Any]] = [
    # ── 기존 데이터/SQL/거버넌스 도구 (7개) ──
    {"id": 1, "name": "search_catalog", "category": "data", "description": "OMOP CDM 스키마 & 의료 코드 시맨틱 검색",
     "backend_service": "Milvus RAG", "data_source": "OMOP CDM Schema + Medical Codes",
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
     "backend_service": "Milvus v2.4.0", "data_source": "omop_knowledge collection (384d embeddings)",
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


# ── Cache state ───────────────────────────────────────────

_health_cache: dict = {"data": None, "ts": 0}
_HEALTH_TTL = 30

_container_cache: dict = {"data": None, "ts": 0}
_CONTAINER_TTL = 30
