# 서울아산병원 통합 데이터 플랫폼 — 개발 통계

**작성일**: 2026-02-10
**개발 기간**: 2026-02-01 ~ 2026-02-10 (10일)
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
| 02/08 | 17 | 85.7 MB |
| 02/09 | 5 | 34.1 MB |
| 02/10 | 5 | 52.4 MB |
| **합계** | **66 세션** | **347.2 MB** |

- **모델**: Claude Opus 4.6
- **도구**: Claude Code CLI (Bash, Read, Edit, Write, Grep, Glob, WebFetch 등)
- 1세션 평균 약 5.3MB의 대화 로그 생성

---

## 2. 코드베이스 현황

### 2.1 전체 규모

| 항목 | 수치 |
|------|------|
| **총 소스 파일 수** | 462개 |
| **총 코드 줄 수** | 129,699줄 |
| **Git 커밋 수** | 25개 |
| **API 엔드포인트** | 692개 (129개 라우터 파일) |

### 2.2 언어별 분포

| 언어 | 파일 수 | 줄 수 | 비율 |
|------|---------|------|------|
| Python (.py) | 210 | 56,205 | 43.3% |
| React/TSX (.tsx) | 133 | 37,629 | 29.0% |
| TypeScript (.ts) | 39 | 4,467 | 3.4% |
| JSON (.json) | 17 | 14,950 | 11.5% |
| Markdown (.md) | 20 | 4,169 | 3.2% |
| YAML (.yml/.yaml) | 8 | 1,297 | 1.0% |
| Shell (.sh) | 9 | 675 | 0.5% |
| Notebook (.ipynb) | 9 | 3,798 | 2.9% |
| Image (.png/.svg/.jpg/.ico) | 6 | — | — |
| PDF (.pdf) | 5 | — | — |
| Config/CSS/HTML/기타 | 6 | 394 | 0.3% |
| **합계** | **462** | **129,699+** | **100%** |

### 2.3 모듈별 구성

| 모듈 | 파일 수 | 줄 수 | 역할 |
|------|---------|------|------|
| Backend API 라우터 | 129 | 41,037 | FastAPI REST API 엔드포인트 (692개) |
| Frontend 컴포넌트 | 96 | 27,037 | React UI 컴포넌트 |
| AI 서비스 모듈 | 31 | 7,934 | Text2SQL, 대화, RAG, 프롬프트, 스키마 |
| Frontend 페이지 | 31 | 10,144 | 화면 단위 페이지 |
| Frontend 서비스 | 24 | 3,494 | API 클라이언트 레이어 |
| Backend 서비스 | 9 | 2,105 | LLM, 메타데이터, DB 풀, S3, 비즈메타 |
| Backend 미들웨어 | 5 | 382 | 보안헤더, 레이트리밋, CORS, CSRF |
| 인프라 설정 | 43 | — | Docker Compose, Nginx, Airflow, 모니터링 |
| Synthea ETL | 4 | 1,469 | OMOP CDM ETL 파이프라인 |
| 문서 | 20 | 4,169 | PRD, 설계 문서, 가이드, 일별 로그 |

---

## 3. RFP 자체평가 (2026-02-10 갱신)

### 3.1 종합 점수

| 영역 | 점수 | API 엔드포인트 | 프론트엔드 |
|------|:----:|:----------:|:----------:|
| **가. 데이터 레이크하우스** | **90%** | 34 | Lakehouse 대시보드 |
| **나. 통합 Pipeline** | **92%** | 89 | ETL 16컴포넌트 |
| **다. CDW-EDW 임상연구** | **93%** | 32 | 코호트+CONSORT+VennDiagram |
| **라. 데이터 거버넌스** | **91%** | 59 | 거버넌스 17컴포넌트 |
| **마. 데이터 마트** | **91%** | 56 | DataMart 634줄 |
| **바. 연동 인터페이스** | **89%** | 41 | NER+Presentation+BI |
| **사. AI 어시스턴트** | **92%** | 105 | AIOps+AIArch+AIEnv+AIExperiment |
| **SER. 보안** | **87%** | — | CSRF+Audit+Auth+RateLimit |
| **PER. 성능** | **84%** | — | 캐싱+모니터링+DB풀 |
| **종합** | **90%** | **692** | **133 TSX, 31페이지** |

### 3.2 가. 데이터 레이크하우스 (90%)

| 세부 요구사항 | 구현 | 점수 |
|-------------|------|:----:|
| DuckDB OLAP 엔진 | `lakehouse.py` — 쿼리 실행, 상태 모니터링 | 90 |
| Parquet 변환/저장 | `lakehouse.py` — export endpoint | 90 |
| 테이블 버전 관리 | `lakehouse.py` — 스냅샷, 롤백 | 85 |
| Landing Zone | `pipeline_lz.py` — 11 endpoints, 템플릿 | 90 |
| 데이터 Export | `pipeline_export.py` — 8 endpoints | 85 |
| 데이터 품질 | `lakehouse_quality.py` — 6 endpoints (신규) | 90 |
| MinIO S3 스토리지 | Docker 컨테이너 + `s3_service.py` | 88 |
| 프론트엔드 | `LakehouseView.tsx`, `DataFabric.tsx` | 88 |

### 3.3 나. 통합 Pipeline (92%)

| 세부 요구사항 | 구현 | 점수 |
|-------------|------|:----:|
| CDC 트리거/폴링 캡처 | `cdc_executor.py` — 10 endpoints, 동적 PK + pg_notify + SSE | 95 |
| CDC 스트림 커넥터 | `cdc_streams.py` — 15 endpoints | 90 |
| CDC 모니터링 | `cdc_monitoring.py` — 3 endpoints | 88 |
| ETL 작업 관리 | `etl_jobs_core.py` — 11 endpoints | 92 |
| ETL 의존성 관리 | `etl_jobs_deps.py` — 6 endpoints | 90 |
| ETL 알림 | `etl_jobs_alerts.py` — 10 endpoints | 90 |
| ETL 스키마 매핑 | `etl_schema.py` — 9 endpoints | 90 |
| ETL 소스 관리 | `etl_sources.py` — 6 endpoints | 90 |
| ETL 템플릿 | `etl_templates.py` — 5 endpoints | 90 |
| 파이프라인 품질 | `pipeline_dq.py` — 6 endpoints (신규) | 90 |
| Airflow 연동 | `etl_airflow.py` — 4 endpoints + Docker | 88 |
| 파이프라인 대시보드 | `cdc_executor.py /dashboard` | 92 |
| 프론트엔드 | ETL 16컴포넌트, TableDependencyGraph | 92 |

### 3.4 다. CDW-EDW 임상연구 분석 (93%)

| 세부 요구사항 | 구현 | 점수 |
|-------------|------|:----:|
| Text2SQL 자연어 질의 | `text2sql.py` — 7 endpoints, LLM 연동 | 95 |
| 멀티턴 대화 | `conversation.py` — 7 endpoints, 스레드 관리 | 92 |
| GUI 코호트 빌더 | `cohort_core.py` — count, execute-flow, set-operation | 95 |
| 코호트 영속화 | `cohort_persist.py` — 6 endpoints (신규) | 92 |
| CONSORT Diagram | `CONSORTFlow.tsx` — ReactFlow 기반 | 95 |
| Venn Diagram 집합연산 | `VennDiagram.tsx` — SVG, 3영역 표시 | 92 |
| Drill-down 환자목록 | `cohort_review.py /drill-down` | 92 |
| 차트 리뷰 타임라인 | `ChartReview.tsx` — 6도메인 컬러코딩 | 90 |
| 요약 통계 | `cohort_review.py /summary-stats` — 성별/연령/진단Top10 | 92 |
| 실데이터 검증 | OMOP CDM 76,074명, 당뇨 5,819명 확인 | 95 |
| 프론트엔드 | CDWResearch 3탭, 5컴포넌트 | 93 |

### 3.5 라. 데이터 거버넌스 (91%)

| 세부 요구사항 | 구현 | 점수 |
|-------------|------|:----:|
| 비식별화 규칙 관리 | `gov_deident.py` — 12 endpoints | 92 |
| 동적 비식별화 적용 | `gov_lineage_ext.py /deident/apply` — 5가지 방법 | 90 |
| 민감도 분류 | `gov_sensitivity.py` — 9 endpoints | 90 |
| 데이터 리니지 | `gov_lineage_ext.py /lineage/graph` — 163노드, 60엣지 (FK 38 + static 22) | 95 |
| SQL 리니지 파싱 | `gov_lineage_ext.py /lineage/parse-sql` — sqlglot AST 기반 | 95 |
| 영향 분석 | `gov_lineage_ext.py /lineage/query-impact` — SQL 의존성 + 하류 BFS | 92 |
| 컬럼 레벨 추적 | `gov_lineage_ext.py /lineage/column-trace` | 90 |
| 컴플라이언스 | `gov_compliance.py` — 7 endpoints (신규) | 90 |
| 스키마 모니터링 | `schema_monitor.py` — 10 endpoints | 90 |
| RBAC | `gov_rbac.py` — 4 endpoints | 85 |
| 표준 관리 | `gov_standards.py` — 8 endpoints | 88 |
| 프론트엔드 | 거버넌스 17컴포넌트, LineageTab, DeidentTab | 92 |

### 3.6 마. 데이터 마트 (91%)

| 세부 요구사항 | 구현 | 점수 |
|-------------|------|:----:|
| 마트 생성/관리 | `mart_ops_core.py` — 20 endpoints | 92 |
| 마트 실행 흐름 | `mart_ops_flow.py` — 10 endpoints, 스케줄링 | 90 |
| 자동 추천 | `mart_recommend.py /recommend` — 인기도×0.4 + 협업필터링×0.4 + 카테고리×0.2 | 93 |
| 사전 정의 템플릿 9종 | 질환3 + 연구3 + 관리3 | 92 |
| SLA 관리 | `mart_recommend.py /sla/*` — 6 endpoints | 88 |
| 고급 마트 기능 | `mart_advanced.py` — 7 endpoints (신규) | 90 |
| 데이터마트 쿼리 | `datamart.py` — 11 endpoints | 90 |
| 프론트엔드 | DataMart.tsx (752줄) | 88 |

### 3.7 바. 연동 인터페이스 (89%)

| 세부 요구사항 | 구현 | 점수 |
|-------------|------|:----:|
| FHIR R4 리소스 서버 | `fhir.py` — 11 endpoints (기존6→확장) | 92 |
| 데이터 반출 (IRB) | `data_export.py` — IRB 승인 워크플로 | 88 |
| 외부 API 게이트웨이 | `external_api.py` — API키 인증, 레이트리밋 | 88 |
| MCP 프로토콜 | `mcp.py` — 3 endpoints + ai_safety 확장 | 85 |
| Paper2Slides | `presentation.py` — 5 endpoints, 비동기 | 90 |
| Medical NER | `ner.py` — GPU 프록시, 한영 지원 | 90 |
| Superset 연동 | `superset.py` — psycopg2 프록시 | 85 |
| 의료영상 | `imaging.py` — 2 endpoints (메타데이터) | 80 |
| 프론트엔드 | Presentation, MedicalNER, BI 페이지 | 88 |

### 3.8 사. AI 어시스턴트 (92%)

| 세부 요구사항 | 구현 | 점수 |
|-------------|------|:----:|
| LLM 모델 관리 | `ai_ops.py` — 18 endpoints | 92 |
| 프롬프트 인젝션 탐지 | `ai_safety.py` — 37패턴 (OWASP) + n-gram + 엔트로피 이상탐지 | 95 |
| 응답 검증 (PII/환각) | `ai_safety.py /validate-response` | 90 |
| 프롬프트 템플릿 | `ai_safety.py /prompt-templates` — CRUD+렌더링 | 88 |
| 입력 새니타이징 | `ai_safety.py /sanitize` — 3단계 모드 | 90 |
| 통합 안전성 스캔 | `ai_safety.py /scan` — injection + PII + harmful SQL 통합 | 93 |
| MCP 도구 확장 | `ai_safety.py /mcp/tools-extended` — 4 도구 | 88 |
| AI 아키텍처 | `ai_architecture.py` — 12 endpoints | 90 |
| AI 실험 관리 | `ai_experiment.py` — 7 endpoints (신규) | 90 |
| BI 차트/쿼리 | `bi_chart.py`(14) + `bi_query.py`(11) | 92 |
| AI 환경 관리 | `aienv_*.py` — 노트북/프로젝트/컨테이너/리소스 | 92 |
| MLflow 연동 | Docker 컨테이너, 모델 추적 | 88 |
| 프론트엔드 | AIOps(970줄), AIArchitecture(794줄), AIEnv | 92 |

### 3.9 SER. 보안 (87%)

| 세부 요구사항 | 구현 | 점수 |
|-------------|------|:----:|
| CSRF 보호 | HMAC-SHA256, 1시간 만료, SameSite=strict | 90 |
| 감사 로그 | `audit.py` — DB 저장, 비밀번호 마스킹 | 90 |
| JWT 인증 | `auth_core.py` + `auth_admin.py` — 16 endpoints | 85 |
| RBAC 권한 관리 | `permission_mgmt.py` + `security_mgmt.py` | 85 |
| SQL 인젝션 방지 | Pydantic + `_safe_*` 이중 검증, asyncpg 파라미터 | 90 |
| Rate Limiting | `rate_limit.py` — 미들웨어 기반 (신규) | 88 |
| Security Headers | `security_headers.py` — X-Frame, CSP 등 (신규) | 85 |
| CORS 제한 | 명시적 origin 목록 (settings.CORS_ORIGINS) | 85 |
| 시크릿 관리 | ENV 지원 있으나 기본값 하드코딩 존재 | 70 |

### 3.10 PER. 성능 (84%)

| 세부 요구사항 | 구현 | 점수 |
|-------------|------|:----:|
| DB 커넥션 풀링 | asyncpg pool (2-10), 30s timeout | 90 |
| Redis 캐싱 | `redis_cache.py`, 5분 캐시 + 백그라운드 갱신 | 88 |
| Prometheus 메트릭 | `MetricsMiddleware`, REQUEST_COUNT/LATENCY | 85 |
| Watchdog 자동 복구 | 2분 주기, 3회 재시도, SSH 터널 관리 | 85 |
| DB 백업 자동화 | 매일 02시, 3개 DB + Milvus, 7일 보관 | 85 |
| 벡터 DB | Milvus 2.4 + etcd + MinIO | 82 |
| 대용량 테이블 최적화 | measurement 36M 캐싱, ANALYZE 완료 | 80 |
| 멀티워커 프로덕션 | uvicorn 4 workers (PRODUCTION=true) | 75 |

---

## 4. 인프라 — Docker 컨테이너 (58개)

### 4.1 컨테이너 현황 요약

| 분류 | 컨테이너 수 | 주요 서비스 |
|------|------------|------------|
| 데이터 플랫폼 (Core) | 8 | API 서버, Nginx, Airflow, Superset, JupyterLab |
| AI/LLM 서비스 | 11 | vLLM, MLflow, Embedding, Reranker, Whisper STT, Milvus, LiteLLM |
| LLMOps 플랫폼 | 9 | LibreChat, Dify, Langfuse, RAG API |
| 데이터베이스 | 11 | PostgreSQL(7), Redis(6), MongoDB, ClickHouse |
| 오브젝트 스토리지 | 4 | asan-minio (IDP), milvus-minio, langfuse-minio, milvus-etcd |
| 모니터링 | 6 | Prometheus, Grafana, cAdvisor, Node/Postgres/Redis Exporter |
| 보안/인증 | 2 | KeyCloak, KeyCloak DB |
| 유틸리티 | 7 | SearXNG, Firecrawl, Docling, Gotenberg, Cloudflared |
| **합계** | **58** | |

### 4.2 전체 컨테이너 목록

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
| 10 | asan-mlflow | ghcr.io/mlflow/mlflow:v2.10.0 | ML 모델 추적/관리 |
| 11 | milvus-standalone | milvusdb/milvus:v2.4.0 | Vector DB (Milvus) |
| 12 | asan-qdrant | qdrant/qdrant:v1.12.0 | Vector DB (Qdrant, 레거시) |
| 13 | embedding-server | huggingface/text-embeddings-inference | 텍스트 임베딩 서버 |
| 14 | reranker-server | huggingface/text-embeddings-inference | 리랭커 서버 |
| 15 | whisper-stt | faster-whisper-server:latest-cuda | 음성→텍스트 변환 (GPU) |
| 16 | litellm-proxy | ghcr.io/berriai/litellm | LLM 프록시/로드밸런싱 |
| 17 | metabase | metabase/metabase | 데이터 시각화/분석 |
| 18 | docling-serve | docling-project/docling-serve | 문서 파싱/구조화 |
| 19 | ai-stream-converter | thecodingmachine/gotenberg:8 | 문서 포맷 변환 |
| | **LLMOps 플랫폼** | | |
| 20 | librechat | danny-avila/librechat-dev | 멀티 LLM 채팅 인터페이스 |
| 21 | rag-api | danny-avila/librechat-rag-api | RAG API 서버 |
| 22 | langfuse-web | langfuse/langfuse:3 | LLM 관측/추적 웹 |
| 23 | langfuse-worker | langfuse/langfuse-worker:3 | LLM 관측 워커 |
| 24 | dify-web | langgenius/dify-web:0.15.3 | AI 워크플로우 빌더 |
| 25 | dify-sandbox | langgenius/dify-sandbox:0.2.10 | Dify 코드 샌드박스 |
| 26 | firecrawl-playwright | firecrawl/playwright-service | 웹 크롤링/스크래핑 |
| 27 | file-proxy | llmops-file-proxy (custom) | 파일 프록시 서버 |
| 28 | searxng | searxng/searxng | 메타 검색 엔진 |
| | **데이터베이스** | | |
| 29 | infra-omop-db-1 | postgres:13 | OMOP CDM DB (92M rows) |
| 30 | infra-postgres-1 | postgres:15 | Airflow 메타 DB |
| 31 | superset-db | postgres:15 | Superset 메타 DB |
| 32 | rag-postgres | pgvector/pgvector:pg16 | RAG Vector DB |
| 33 | langfuse-postgres | postgres:17 | Langfuse DB |
| 34 | keycloak-postgres | postgres:15-alpine | KeyCloak DB |
| 35 | metabase-postgres | postgres:15-alpine | Metabase DB |
| 36 | dify-postgres | postgres:15-alpine | Dify DB |
| 37 | firecrawl-postgres | postgres (custom) | Firecrawl DB |
| 38 | asan-redis | redis:7-alpine | 메인 캐시 |
| 39 | superset-redis | redis:7-alpine | Superset 캐시 |
| 40 | langfuse-redis | redis:7 | Langfuse 캐시 |
| 41 | dify-redis | redis:7-alpine | Dify 캐시 |
| 42 | firecrawl-redis | redis:7-alpine | Firecrawl 캐시 |
| 43 | searxng-redis | redis:alpine | SearXNG 캐시 |
| 44 | chat-mongodb | mongo:latest | LibreChat 대화 저장소 |
| 45 | langfuse-clickhouse | clickhouse/clickhouse-server | Langfuse 분석 DB |
| | **오브젝트 스토리지** | | |
| 46 | asan-minio | minio/minio | IDP 오브젝트 스토리지 (S3 API 19000, Console 19001) |
| 47 | milvus-minio | minio/minio | Milvus S3 백엔드 |
| 48 | langfuse-minio | minio/minio | Langfuse 파일 저장소 |
| 49 | milvus-etcd | coreos/etcd:v3.5.5 | Milvus 메타데이터 |
| | **모니터링** | | |
| 50 | asan-prometheus | prom/prometheus:v2.51.0 | 메트릭 수집 |
| 51 | asan-grafana | grafana/grafana:11.0.0 | 대시보드/시각화 |
| 52 | asan-cadvisor | cadvisor:v0.49.1 | 컨테이너 메트릭 |
| 53 | asan-node-exporter | node-exporter:v1.8.0 | 호스트 메트릭 |
| 54 | asan-postgres-exporter | postgres-exporter:v0.15.0 | PostgreSQL 메트릭 |
| 55 | asan-redis-exporter | redis_exporter:v1.61.0 | Redis 메트릭 |
| | **보안/인증** | | |
| 56 | keycloak | keycloak:26.0 | SSO/인증 서버 (코드 연동 미완 — Docker만 운영) |
| | **유틸리티** | | |
| 57 | firecrawl-rabbitmq | rabbitmq:3-alpine | 메시지 큐 |
| 58 | cloudflared-tunnel | cloudflare/cloudflared | 클라우드 터널 |

---

## 5. 데이터 자산

### 5.1 전체 데이터 규모

| 데이터셋 | 용량 | 건수 | 설명 |
|----------|------|------|------|
| **NIH Chest X-ray** | **43 GB** | **112,120장** | 흉부 X선 PNG 이미지 (12 폴더), BBox/메타 CSV |
| **OMOP CDM (DB)** | — | **92,260,027건** | Synthea 합성 → OMOP 변환, 18 테이블, PostgreSQL |
| **AI Hub 전문의학지식** | 198 MB | 37 파일 | 진료과별 라벨링 데이터 (ZIP), 학회 가이드라인, 논문 |
| **AI Hub 필수의료지식** | 137 MB | 16 파일 | 내과/외과/산부인과/응급의학과 (ZIP) |
| **CMS Synthetic OMOP** | 126 MB | 1 SQL | CMS 합성 OMOP 데이터 (116만 줄 SQL) |
| **Eunomia (Synthea 1K)** | 45 MB | 13 CSV | OMOP ETL 검증용 소규모 합성 데이터 |
| **OMOP CDM 스키마** | 176 MB | — | CommonDataModel DDL, 문서, ERD |
| **기타** | 20 KB | 1 파일 | `aihub_healthcare_datasets.xlsx` (AI Hub 목록) |
| **합계** | **~44 GB** | **112,120 이미지 + 9,200만 레코드** | |

### 5.2 OMOP CDM 데이터 상세

Synthea 합성 데이터 기반 OMOP CDM 표준 변환 완료.

| 항목 | 수치 |
|------|------|
| **총 레코드** | **92,260,027건** |
| **환자 수** | **76,074명** (M: 37,796 / F: 38,278) |
| **임상 테이블** | 18개 |
| **ETL 단계** | 13단계 전체 완료 |

### 5.3 주요 테이블 데이터 분포

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

### 5.4 NIH Chest X-ray 데이터

| 항목 | 수치 |
|------|------|
| **총 이미지** | **112,120장** (PNG) |
| **용량** | 43 GB (12 폴더) |
| **메타데이터** | `Data_Entry_2017.csv` (112,120행) |
| **Bounding Box** | `BBox_List_2017.csv` (984행) |
| **출처** | NIH Clinical Center (KaggleHub) |
| **연동** | `imaging.py` — 2 endpoints (메타데이터 조회) |

---

## 6. 플랫폼 기능 (17개 모듈)

| # | 기능 | 설명 |
|---|------|------|
| 1 | 대시보드 | 시스템 현황, CDM 통계, 서비스 모니터링 |
| 2 | 데이터 카탈로그 | 테이블 검색, 스키마/샘플, AI 요약, 데이터 조합 |
| 3 | 데이터마트 | OMOP CDM 테이블 탐색, Text2SQL 자연어 쿼리, 자동 추천 |
| 4 | 데이터 거버넌스 | 품질, 비식별화, 리니지/영향분석, 표준/보안/컴플라이언스 관리 |
| 5 | 데이터 설계 | ERD, 명명 규칙, 비정형 데이터, 존 설계 |
| 6 | ETL 파이프라인 | Airflow 기반 OMOP CDM ETL, CDC 실행엔진, 스키마 변경, DQ 검증 |
| 7 | BI 대시보드 | 차트 빌더, SQL 에디터, 리포트 내보내기 |
| 8 | AI 분석환경 | JupyterLab, 프로젝트/노트북/리소스 관리 |
| 9 | CDW 연구지원 | 코호트 빌더 (CONSORT/Venn), Text2SQL 대화형 임상 데이터 조회 |
| 10 | 비정형 구조화 | Medical NER (BioClinicalBERT + 한국어 의학사전) |
| 11 | 의료 온톨로지 | OMOP CDM Knowledge Graph, Neo4j Cypher 스크립트 생성 (Neo4j 서버 미포함) |
| 12 | AI 운영관리 | 모델 배포, 모니터링, 감사 로그, AI 안전장치, AI 실험 관리 |
| 13 | 보안/권한관리 | RBAC, 동적 마스킹, 데이터셋 권한, CSRF, JWT, Rate Limiting |
| 14 | 메타데이터 관리 | 변경 이력, 매핑, 품질 관리 |
| 15 | CDC 스트리밍 | 변경 데이터 캡처, 실시간 모니터링, 파이프라인 대시보드 |
| 16 | 데이터 레이크하우스 | DuckDB OLAP, Parquet 변환, Landing Zone, 테이블 버전, 데이터 품질 |
| 17 | 연동 인터페이스 | FHIR R4, IRB 데이터 반출, 외부 API 게이트웨이, MCP |

---

## 7. 기술 스택

| 레이어 | 기술 |
|--------|------|
| Frontend | React 18, TypeScript, Ant Design, Recharts, ReactFlow, Vite |
| Backend | FastAPI, Python 3.11, Uvicorn (692 API endpoints, 129 라우터) |
| Database | PostgreSQL 13 (OMOP CDM, 92M rows) |
| ETL | Apache Airflow 2.8, Synthea ETL 파이프라인 (13단계) |
| BI | Apache Superset 3.1, Metabase |
| AI/LLM | Qwen3-32B-AWQ (vLLM, SSH 터널 28888→GPU:8000) |
| Text2SQL | XiYanSQL-QwenCoder-7B (vLLM, port 8001) + 스키마 링킹 + LangGraph 대화 메모리 |
| Medical NER | BioClinicalBERT + 한국어 의학사전 (67개 용어) |
| Vector DB | Milvus v2.4.0, Qdrant v1.12, pgvector |
| Object Storage | MinIO (Milvus S3 백엔드) |
| Embedding | HuggingFace Text Embeddings Inference |
| LLMOps | Langfuse (추적), LiteLLM (프록시), MLflow (모델 관리) |
| 채팅 | LibreChat, Dify (AI 워크플로우) |
| 분석환경 | JupyterLab (Docker) |
| 인증 | JWT + CSRF (KeyCloak 26.0 Docker 운영 중, 코드 연동 미완) |
| 모니터링 | Prometheus, Grafana, cAdvisor, Node/Postgres/Redis Exporter |
| SQL 파서 | sqlglot v28.10.1 (AST 기반 SQL 리니지 분석) |
| 보안 | CSRF HMAC-SHA256, Rate Limiting, Security Headers, Audit Logging, SQL Injection 이중 방어, 37 OWASP 패턴 |
| Proxy | Nginx |
| 인프라 | Docker Compose (58 컨테이너), Watchdog, 자동 백업 |

---

## 8. 최근 주요 변경사항 (02/08 ~ 02/10)

| 변경 | 내용 |
|------|------|
| **Shell 라우터 실기능 전환 (02/10)** | 4개 라우터 전면 교체 — 껍데기 0개 확인 |
| — gov_lineage_ext | sqlglot SQL AST 파싱, information_schema FK 동적 탐색 (60 edges), lineage_log DB 영속화 |
| — cdc_executor | 동적 PK 탐지, pg_notify 실시간 CDC, SSE 스트리밍 리스너 |
| — mart_recommend | mart_usage_log 테이블, 3단계 추천 알고리즘, SQL injection 수정 (parameterized query) |
| — ai_safety | 37개 OWASP 패턴, n-gram + Shannon 엔트로피 이상탐지, 통합 /scan 엔드포인트, 동적 의료코드 캐시 |
| 신규 라우터 10개 | `ai_experiment`, `cohort_persist`, `gov_compliance`, `lakehouse_quality`, `mart_advanced`, `pipeline_dq`, `_portal_ops_health`, `s3_service`, `chat_db`, `unstructured` |
| 신규 서비스 1개 | `biz_meta_generator.py` — 비즈니스 메타 자동 생성 |
| API 엔드포인트 확장 | 620 → 692개 (+72) |
| 전체 라우터 감사 (02/10) | 129개 파일 전수 조사 — 실제 동작 76개, Umbrella 22개, Shared 31개, 껍데기 **0개** |
| 보안 미들웨어 | `rate_limit.py`, `security_headers.py` 추가 |
| FHIR 확장 | 6 → 11 endpoints |
| Milvus 도입 | Qdrant에서 Milvus v2.4.0으로 전환 (etcd + MinIO 백엔드) |
| 벡터 검색 | `vector.py` 265줄 리팩토링, Milvus 통합 |
| 포털 운영 | `portal_ops_home.py` 709줄 확장, 헬스체크 분리 |
| 대화 이력 DB화 | chat_history 테이블 (PostgreSQL), 세션/메시지 영속 저장 |
| 프론트엔드 리팩토링 (02/10) | MainLayout/AIAssistantPanel/TableDetailModal 분리, MedicalNER 대폭 개선 |
| 데이터 패브릭 | DB 기반 소스/플로우 CRUD + 8개 서비스 실시간 헬스체크 |
| Vite 메모리 강건화 | NODE_OPTIONS 4GB 힙, watch 제외, 청크 분리 (3D/에디터/차트) |
| 전체 시스템 검증 | 20개 프론트엔드 라우트 + 43개 백엔드 API 전수 검사 통과 |
| 성능 최적화 | ETL dags 병렬화 + 캐시, Dashboard 서브쿼리 샘플링, Docker/psutil 캐시 |
| 서버 시작 워밍업 | dashboard-stats, containers, system, gpu, dags 등 9개 API 자동 워밍업 |
| sqlglot 의존성 추가 | Pure Python SQL 파서 (v28.10.1), 리니지 분석용 |

---

## 9. 요약

> **10일간 66개 AI Agent 세션** (대화 로그 347MB)으로
> **462개 소스 파일, 129,699줄** 코드베이스,
> **API 엔드포인트: 692개 (129개 라우터, 껍데기 0개)**,
> **133개 프론트엔드 컴포넌트**, **31개 페이지**,
> **58개 Docker 컨테이너** 인프라,
> **44GB 데이터 자산** (NIH 흉부 X선 112,120장 + OMOP CDM 9,200만건)을 갖춘
> **17개 모듈**의 통합 데이터 플랫폼을 구축했습니다.
>
> **RFP 종합 자체평가: 90%** (7개 사업 영역 + 보안/성능)

---

*Generated by Claude Code (Claude Opus 4.6) — 2026-02-10*
