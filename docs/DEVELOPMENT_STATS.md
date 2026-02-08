# 서울아산병원 통합 데이터 플랫폼 — 개발 통계

**작성일**: 2026-02-08
**개발 기간**: 2026-02-01 ~ 2026-02-08 (8일)
**개발 방식**: Claude Code (AI Agent) 기반 자율 개발

---

## 1. AI Agent 세션 통계

| 날짜 | 세션 수 | 대화 로그 |
|------|---------|----------|
| 02/01 | 2 | 3.9 MB |
| 02/03 | 5 | 22.8 MB |
| 02/04 | 1 | 0.8 MB |
| 02/05 | 3 | 8.6 MB |
| 02/06 | 9 | 56.4 MB |
| 02/07 | 19 | 64.6 MB |
| 02/08 | 15 | 61.5 MB |
| **합계** | **54 세션** | **219 MB** |

- **모델**: Claude Opus 4.6
- **도구**: Claude Code CLI (Bash, Read, Edit, Write, Grep, Glob, WebFetch 등)
- 1세션 평균 약 4MB의 대화 로그 생성

---

## 2. 코드베이스 현황

### 2.1 전체 규모

| 항목 | 수치 |
|------|------|
| **총 파일 수** | 440개 |
| **총 코드 줄 수** | 209,924줄 |
| **Git 커밋 수** | 12개 |

### 2.2 언어별 분포

| 언어 | 파일 수 | 줄 수 | 비율 |
|------|---------|------|------|
| Python (.py) | 172 | 42,827 | 20.4% |
| React/TSX (.tsx) | 126 | 35,681 | 17.0% |
| TypeScript (.ts) | 35 | 3,919 | 1.9% |
| JSON (.json) | 15 | 13,142 | 6.3% |
| Markdown (.md) | 9 | 1,073 | 0.5% |
| YAML (.yml) | 8 | 844 | 0.4% |
| Shell (.sh) | 8 | 596 | 0.3% |
| Config (.conf) | 2 | 159 | 0.1% |
| 기타 (txt, pdf 등) | 65 | 111,683 | 53.2% |
| **합계** | **440** | **209,924** | **100%** |

### 2.3 모듈별 구성

| 모듈 | 파일 수 | 역할 |
|------|---------|------|
| Backend API 라우터 | 104 | FastAPI REST API 엔드포인트 |
| Frontend 컴포넌트 | 92 | React UI 컴포넌트 |
| AI 서비스 모듈 | 44 | Text2SQL, 대화, RAG, 프롬프트 |
| Frontend 페이지 | 27 | 화면 단위 페이지 |
| Frontend 서비스 | 24 | API 클라이언트 레이어 |
| 인프라 설정 | 29 | Docker, Nginx, Airflow, 모니터링 |
| Backend 서비스 | 7 | LLM, 메타데이터, DB 풀 등 |
| 문서 | 32 | PRD, 설계 문서, 가이드 |
| GPU 서비스 | 5 | Medical NER 서버 |

---

## 3. 인프라 — Docker 컨테이너 (57개)

### 3.1 컨테이너 현황 요약

| 분류 | 컨테이너 수 | 주요 서비스 |
|------|------------|------------|
| 데이터 플랫폼 (Core) | 8 | API 서버, Nginx, Airflow, Superset, JupyterLab |
| AI/LLM 서비스 | 12 | vLLM, MLflow, Embedding, Reranker, Whisper STT, Milvus, Qdrant, LiteLLM |
| LLMOps 플랫폼 | 9 | LibreChat, Dify, Langfuse, RAG API |
| 데이터베이스 | 12 | PostgreSQL(7), Redis(6), MongoDB, ClickHouse |
| 오브젝트 스토리지 | 2 | MinIO (Langfuse, Milvus) |
| 모니터링 | 6 | Prometheus, Grafana, cAdvisor, Node/Postgres/Redis Exporter |
| 보안/인증 | 2 | KeyCloak, KeyCloak DB |
| 유틸리티 | 6 | SearXNG, Firecrawl, Docling, Gotenberg, Cloudflared |
| **합계** | **57** | |

### 3.2 전체 컨테이너 목록

| # | 컨테이너 | 이미지 | 역할 |
|---|---------|--------|------|
| | **데이터 플랫폼** | | |
| 1 | asan-api | asan-api (custom) | FastAPI 백엔드 API 서버 |
| 2 | nginx-proxy | nginx:alpine | 리버스 프록시 |
| 3 | infra-airflow-webserver-1 | apache/airflow:2.8.1 | ETL 파이프라인 웹서버 |
| 4 | infra-airflow-scheduler-1 | apache/airflow:2.8.1 | ETL 파이프라인 스케줄러 |
| 5 | superset | apache/superset:3.1.0 | BI 대시보드 |
| 6 | superset-worker | apache/superset:3.1.0 | Superset 비동기 워커 |
| 7 | superset-beat | apache/superset:3.1.0 | Superset 스케줄러 |
| 8 | asan-jupyterlab | jupyter/datascience-notebook | AI 분석 환경 |
| | **AI/LLM 서비스** | | |
| 9 | asan-xiyan-sql | vllm/vllm-openai:v0.6.6 | Text2SQL LLM 서빙 (vLLM) |
| 10 | asan-qdrant | qdrant/qdrant:v1.12.0 | Vector DB (RAG 임베딩) |
| 11 | asan-mlflow | ghcr.io/mlflow/mlflow:v2.10.0 | ML 모델 추적/관리 |
| 12 | milvus-standalone | milvusdb/milvus:v2.4.0 | Vector DB (Milvus) |
| 13 | embedding-server | huggingface/text-embeddings-inference | 텍스트 임베딩 서버 |
| 14 | reranker-server | huggingface/text-embeddings-inference | 리랭커 서버 |
| 15 | whisper-stt | faster-whisper-server:latest-cuda | 음성→텍스트 변환 (GPU) |
| 16 | litellm-proxy | ghcr.io/berriai/litellm | LLM 프록시/로드밸런싱 |
| 17 | metabase | metabase/metabase | 데이터 시각화/분석 |
| 18 | docling-serve | docling-project/docling-serve | 문서 파싱/구조화 |
| 19 | ai-stream-converter | thecodingmachine/gotenberg:8 | 문서 포맷 변환 |
| 20 | file-proxy | llmops-file-proxy (custom) | 파일 프록시 서버 |
| | **LLMOps 플랫폼** | | |
| 21 | librechat | danny-avila/librechat-dev | 멀티 LLM 채팅 인터페이스 |
| 22 | rag-api | danny-avila/librechat-rag-api | RAG API 서버 |
| 23 | langfuse-web | langfuse/langfuse:3 | LLM 관측/추적 웹 |
| 24 | langfuse-worker | langfuse/langfuse-worker:3 | LLM 관측 워커 |
| 25 | dify-web | langgenius/dify-web:0.15.3 | AI 워크플로우 빌더 |
| 26 | dify-sandbox | langgenius/dify-sandbox:0.2.10 | Dify 코드 샌드박스 |
| 27 | firecrawl-playwright | firecrawl/playwright-service | 웹 크롤링/스크래핑 |
| 28 | searxng | searxng/searxng | 메타 검색 엔진 |
| 29 | cloudflared-tunnel | cloudflare/cloudflared | 클라우드 터널 |
| | **데이터베이스** | | |
| 30 | infra-omop-db-1 | postgres:13 | OMOP CDM DB (92M rows) |
| 31 | infra-postgres-1 | postgres:15 | Airflow 메타 DB |
| 32 | superset-db | postgres:15 | Superset 메타 DB |
| 33 | rag-postgres | pgvector/pgvector:pg16 | RAG Vector DB |
| 34 | langfuse-postgres | postgres:17 | Langfuse DB |
| 35 | keycloak-postgres | postgres:15-alpine | KeyCloak DB |
| 36 | metabase-postgres | postgres:15-alpine | Metabase DB |
| 37 | dify-postgres | postgres:15-alpine | Dify DB |
| 38 | firecrawl-postgres | postgres (custom) | Firecrawl DB |
| 39 | asan-redis | redis:7-alpine | 메인 캐시 |
| 40 | superset-redis | redis:7-alpine | Superset 캐시 |
| 41 | langfuse-redis | redis:7 | Langfuse 캐시 |
| 42 | dify-redis | redis:7-alpine | Dify 캐시 |
| 43 | firecrawl-redis | redis:7-alpine | Firecrawl 캐시 |
| 44 | searxng-redis | redis:alpine | SearXNG 캐시 |
| 45 | chat-mongodb | mongo:latest | LibreChat 대화 저장소 |
| 46 | langfuse-clickhouse | clickhouse/clickhouse-server | Langfuse 분석 DB |
| | **오브젝트 스토리지** | | |
| 47 | langfuse-minio | minio/minio | Langfuse 파일 저장소 |
| 48 | milvus-minio | minio/minio | Milvus 파일 저장소 |
| 49 | milvus-etcd | coreos/etcd:v3.5.5 | Milvus 메타데이터 |
| | **모니터링** | | |
| 50 | asan-prometheus | prom/prometheus:v2.51.0 | 메트릭 수집 |
| 51 | asan-grafana | grafana/grafana:11.0.0 | 대시보드/시각화 |
| 52 | asan-cadvisor | cadvisor:v0.49.1 | 컨테이너 메트릭 |
| 53 | asan-node-exporter | node-exporter:v1.8.0 | 호스트 메트릭 |
| 54 | asan-postgres-exporter | postgres-exporter:v0.15.0 | PostgreSQL 메트릭 |
| 55 | asan-redis-exporter | redis_exporter:v1.61.0 | Redis 메트릭 |
| | **보안/인증** | | |
| 56 | keycloak | keycloak:26.0 | SSO/인증 서버 |
| 57 | firecrawl-rabbitmq | rabbitmq:3-alpine | 메시지 큐 |

---

## 4. OMOP CDM 데이터

Synthea 합성 데이터 기반 OMOP CDM 표준 변환 완료.

| 항목 | 수치 |
|------|------|
| **총 레코드** | **92,260,027건** |
| **환자 수** | **76,074명** (M: 37,796 / F: 38,278) |
| **임상 테이블** | 18개 |
| **ETL 단계** | 13단계 전체 완료 |

### 주요 테이블 데이터 분포

| 테이블 | 건수 | 설명 |
|--------|------|------|
| measurement | 36,610,971 | 검사결과 |
| observation | 21,289,693 | 관찰 데이터 |
| procedure_occurrence | 12,413,965 | 시술/수술 |
| visit_occurrence | 4,514,630 | 방문 (입원/외래/응급) |
| cost | 4,389,991 | 의료비용 |
| drug_exposure | 3,928,575 | 투약 |
| payer_plan_period | 2,820,208 | 보험 기간 |
| condition_occurrence | 2,822,068 | 진단 |
| condition_era | 1,511,513 | 진단 Era (30일 gap 병합) |
| drug_era | 1,241,133 | 투약 Era (30일 gap 병합) |
| person | 76,074 | 환자 마스터 |

---

## 5. 플랫폼 기능 (15개 모듈)

| # | 기능 | 설명 |
|---|------|------|
| 1 | 대시보드 | 시스템 현황, CDM 통계, 서비스 모니터링 |
| 2 | 데이터 카탈로그 | 테이블 검색, 스키마/샘플, AI 요약, 데이터 조합 |
| 3 | 데이터마트 | OMOP CDM 테이블 탐색, Text2SQL 자연어 쿼리 |
| 4 | 데이터 거버넌스 | 품질, 비식별화, 리니지, 표준/보안 관리 |
| 5 | 데이터 설계 | ERD, 명명 규칙, 비정형 데이터, 존 설계 |
| 6 | ETL 파이프라인 | Airflow 기반 OMOP CDM ETL, CDC, 스키마 변경 |
| 7 | BI 대시보드 | 차트 빌더, SQL 에디터, 리포트 내보내기 |
| 8 | AI 분석환경 | JupyterLab, 프로젝트/노트북/리소스 관리 |
| 9 | CDW 연구지원 | 코호트 빌더, Text2SQL 대화형 임상 데이터 조회 |
| 10 | 비정형 구조화 | Medical NER (BioClinicalBERT + 한국어 의학사전) |
| 11 | 의료 온톨로지 | OMOP CDM Knowledge Graph, RDF Triple, Neo4j Export |
| 12 | AI 운영관리 | 모델 배포, 모니터링, 감사 로그 |
| 13 | 보안/권한관리 | RBAC, 동적 마스킹, 데이터셋 권한 |
| 14 | 메타데이터 관리 | 변경 이력, 매핑, 품질 관리 |
| 15 | CDC 스트리밍 | 변경 데이터 캡처, 실시간 모니터링 |

---

## 6. 기술 스택

| 레이어 | 기술 |
|--------|------|
| Frontend | React 18, TypeScript, Ant Design, Recharts, react-force-graph-2d, Vite |
| Backend | FastAPI, Python 3.11, Uvicorn |
| Database | PostgreSQL 13 (OMOP CDM, 92M rows) |
| ETL | Apache Airflow 2.8, Synthea ETL 파이프라인 (13단계) |
| BI | Apache Superset 3.1, Metabase |
| AI/LLM | Qwen3-235B FP8 (vLLM PagedAttention) |
| Text2SQL | XiYan SQL + 스키마 링킹 + LangGraph 대화 메모리 |
| Medical NER | BioClinicalBERT + 한국어 의학사전 (67개 용어) |
| Vector DB | Qdrant, Milvus, pgvector |
| Embedding | HuggingFace Text Embeddings Inference |
| LLMOps | Langfuse (추적), LiteLLM (프록시), MLflow (모델 관리) |
| 채팅 | LibreChat, Dify (AI 워크플로우) |
| 분석환경 | JupyterLab (Docker) |
| 인증 | KeyCloak 26.0 (SSO) |
| 모니터링 | Prometheus, Grafana, cAdvisor, Node/Postgres/Redis Exporter |
| Proxy | Nginx |
| 인프라 | Docker Compose (57 컨테이너), Systemd |

---

## 7. 요약

> **8일간 54개 AI Agent 세션**으로
> **440개 파일, 209,924줄** 코드베이스,
> **57개 Docker 컨테이너** 인프라,
> **9,200만건** OMOP CDM 임상 데이터를 갖춘
> **15개 모듈**의 통합 데이터 플랫폼을 구축했습니다.

---

*Generated by Claude Code (Claude Opus 4.6) — 2026-02-08*
