# 서울아산병원 통합 데이터 플랫폼 (IDP)

## 개요

서울아산병원 **통합 데이터 플랫폼(Integrated Data Platform)** 구축 프로젝트입니다.

OMOP CDM 기반 임상 데이터 웨어하우스와 AI 지능형 서비스를 통합하여, 데이터 수집부터 분석/연구까지 전 과정을 지원하는 플랫폼입니다.

---

## 주요 기능

| 영역 | 기능 | 설명 |
|------|------|------|
| **데이터 수집** | ETL 파이프라인 | Airflow 기반 OMOP CDM ETL (13단계 전체 완료) |
| | CDC 스트리밍 | 변경 데이터 캡처, 실시간 모니터링 |
| | 이기종 소스 연동 | REST API, JDBC, 파일, 스트림 통합 수집 |
| **데이터 관리** | 데이터 거버넌스 | 품질 검증, 비식별화, 리니지, 표준/보안/컴플라이언스 관리 |
| | 데이터 카탈로그 | 데이터 자산 검색, 스키마/샘플, AI 요약, 데이터 조합 |
| | 데이터 설계 | ERD, 명명 규칙, 비정형 데이터, 존 설계 |
| | 메타데이터 관리 | 변경 이력, 매핑, 온톨로지 기반 스키마 링킹 |
| **데이터 활용** | 데이터마트 | OMOP CDM 테이블 탐색, 마트 물리화(MV), 자동 스케줄링 |
| | Text2SQL 분석 | 자연어 → SQL 변환, 대화형 임상 데이터 조회 |
| | BI 대시보드 | Apache Superset 내장 BI 분석 환경 |
| | 코호트 분석 | 환자 코호트 정의/저장/비교/내보내기 |
| | 레이크하우스 | CDW/EDW 통합 뷰, 데이터 품질 프로파일링, 스키마 진화 |
| **AI 서비스** | AI 분석환경 | JupyterLab 컨테이너, 노트북/프로젝트/리소스 관리 |
| | 비정형 구조화 | Medical NER (BioClinicalBERT + 한국어 의학사전 67개 용어) |
| | 의료 온톨로지 | OMOP CDM Knowledge Graph, RDF Triple, Neo4j Export |
| | AI 운영관리 | 모델 관리, A/B 실험, 감사 로그, AI 안전성 평가 |
| | RAG 검색 | Milvus 벡터 DB 기반 시맨틱 검색 |
| **보안/인프라** | 보안 미들웨어 | CSRF, Rate Limit, Security Headers, GZip, 감사 로그 |
| | 권한관리 | RBAC, 동적 마스킹, 데이터셋 권한 |
| | FHIR R4 연동 | Patient/$everything, Bundle 배치, 고급 검색 |
| | 파이프라인 DQ | DQ 규칙 엔진, 검증/재시도, Dead-letter 큐 |

---

## 시스템 아키텍처

```
┌──────────────────────────────────────────────────────────────────┐
│                       Frontend (React 18)                         │
│           Vite + Ant Design + Recharts + ReactFlow               │
│                        localhost:5173                              │
│                  31 Pages · 14 Component Groups                   │
└───────────────────────────┬──────────────────────────────────────┘
                            │
┌───────────────────────────▼──────────────────────────────────────┐
│                      Backend (FastAPI)                             │
│     127 API Routers · 4 Middleware (CSRF/Audit/RateLimit/GZip)   │
│  Text2SQL · DataMart · Governance · ETL · Catalog · CDC · FHIR   │
│  AI Ops · Cohort · Lakehouse · Pipeline DQ · Compliance · RAG    │
│                        localhost:8000                              │
└──┬────────┬────────┬────────┬────────┬────────┬────────┬────────┘
   │        │        │        │        │        │        │
┌──▼──┐ ┌──▼──┐ ┌──▼───┐ ┌──▼───┐ ┌──▼───┐ ┌──▼────┐ ┌▼───────┐
│OMOP │ │Milvus│ │MinIO │ │Airfl.│ │Super.│ │Qwen3  │ │Med NER │
│PG   │ │Vector│ │  S3  │ │ ETL  │ │  BI  │ │ LLM   │ │BioBERT │
│:5436│ │:19530│ │:19000│ │:18080│ │:18088│ │:28888 │ │:28100  │
└─────┘ └─────┘ └──────┘ └──────┘ └──────┘ └───────┘ └────────┘
```

---

## 서비스 포트

| 서비스 | 포트 | 용도 |
|--------|------|------|
| Frontend (Vite) | 5173 | React 개발 서버 |
| Backend API | 8000 | FastAPI 백엔드 |
| Nginx Proxy | 80 | 리버스 프록시 |
| OMOP CDM DB | 5436 | PostgreSQL 13 (OMOP CDM) |
| Superset DB | 15432 | PostgreSQL 15 (Superset 메타데이터) |
| Milvus Vector DB | 19530 | 벡터 검색 (gRPC) |
| MinIO S3 | 19000 / 19001 | 오브젝트 스토리지 (API / Console) |
| Airflow | 18080 | ETL 파이프라인 관리 |
| Superset | 18088 | BI 대시보드 |
| JupyterLab | 18888 | 데이터 분석 노트북 |
| Prometheus | 19090 | 메트릭 수집 |
| Grafana | 13000 | 모니터링 대시보드 |
| Qwen3 LLM | 28888 | GPU 서버 SSH 터널 |
| XiYan SQL | 8001 | GPU 서버 Text2SQL 모델 |
| Medical NER | 28100 | GPU 서버 SSH 터널 |

---

## 디렉토리 구조

```
asan/
├── ai_services/               # AI 서비스 모듈
│   ├── xiyan_sql/             # Text2SQL (XiYan 기반 스키마 링킹)
│   ├── conversation_memory/   # 대화 이력 관리 (LangGraph)
│   ├── prompt_enhancement/    # 프롬프트 최적화
│   ├── rag/                   # RAG 임베딩/검색 (Milvus)
│   └── knowledge_data/        # 의학 지식 데이터
├── data_portal/               # 데이터 포털
│   └── src/
│       ├── api/               # FastAPI 백엔드
│       │   ├── middleware/    # 보안 미들웨어 (CSRF, Audit, RateLimit, SecurityHeaders)
│       │   ├── routers/       # API 라우터 (127개 모듈)
│       │   │   ├── chat*.py           # 대화형 AI
│       │   │   ├── datamart*.py       # 데이터마트/분석 API
│       │   │   ├── etl*.py            # ETL 파이프라인
│       │   │   ├── ontology*.py       # 온톨로지 Knowledge Graph
│       │   │   ├── gov*.py            # 거버넌스/컴플라이언스
│       │   │   ├── data_catalog*.py   # 데이터 카탈로그
│       │   │   ├── cdc*.py            # CDC 스트리밍
│       │   │   ├── fhir.py            # FHIR R4 연동
│       │   │   ├── pipeline_dq.py     # 파이프라인 DQ 검증
│       │   │   ├── ai_experiment.py   # AI A/B 실험
│       │   │   ├── cohort_persist.py  # 코호트 저장/비교
│       │   │   ├── lakehouse*.py      # 레이크하우스 DQ/스키마
│       │   │   ├── mart_advanced.py   # 마트 물리화/스케줄링
│       │   │   └── ...
│       │   ├── services/      # 비즈니스 로직 (llm, db_pool, s3_service)
│       │   └── core/          # 설정 (config.py)
│       └── portal/            # React 프론트엔드
│           └── src/
│               ├── pages/     # 페이지 (31개)
│               ├── components/
│               │   ├── dashboard/     # 대시보드 (레이크하우스/운영/아키텍처 뷰)
│               │   ├── ontology/      # 온톨로지 그래프
│               │   ├── catalog/       # 카탈로그
│               │   ├── governance/    # 거버넌스
│               │   ├── etl/           # ETL 파이프라인
│               │   ├── cohort/        # 코호트/Text2SQL
│               │   ├── aienv/         # AI 환경
│               │   ├── lineage/       # 데이터 리니지
│               │   ├── datadesign/    # 데이터 설계
│               │   └── ai/            # AI 어시스턴트
│               └── services/  # API 서비스 레이어
├── gpu-services/              # GPU 서버 배포 서비스
│   └── medical-ner-service/   # BioClinicalBERT NER 서버
├── infra/                     # 인프라 설정
│   ├── airflow/dags/          # Airflow DAG (OMOP ETL 파이프라인)
│   ├── docker-compose*.yml    # Docker Compose 설정 (6개)
│   ├── nginx/                 # Nginx 리버스 프록시
│   ├── superset/              # Superset 설정
│   ├── systemd/               # Systemd 서비스 설정
│   ├── watchdog.sh            # 서비스 모니터링 (2분 crontab)
│   └── demo_start.sh          # 데모 일괄 시작 스크립트
├── synthea/                   # Synthea → OMOP CDM ETL 스크립트
├── docs/                      # PRD 및 설계 문서
└── README.md
```

---

## OMOP CDM 데이터베이스

Synthea 합성 OMOP CDM 데이터 기반 — **76,074명 환자, 92,260,027건**.

| 테이블 | 건수 | 비고 |
|--------|------|------|
| measurement | 36,610,971 | 검사결과 (Lab/Vital) |
| observation | 21,289,693 | 관찰 데이터 |
| procedure_occurrence | 12,413,965 | 시술/수술 |
| visit_occurrence | 4,514,630 | 입원(9201)/외래(9202)/응급(9203) |
| cost | 4,389,991 | 의료비용 |
| drug_exposure | 3,928,575 | 투약 |
| payer_plan_period | 2,820,208 | 보험 기간 |
| condition_occurrence | 2,822,068 | 진단 (SNOMED CT) |
| condition_era | 1,511,513 | ETL 생성 (30일 gap 병합) |
| drug_era | 1,241,133 | ETL 생성 (30일 gap 병합) |
| person | 76,074 | M:37,796 / F:38,278 |

---

## ETL 파이프라인 (Airflow DAGs)

OMOP CDM 표준 ETL을 수행하는 4개 DAG:

| DAG | 스케줄 | 역할 |
|-----|--------|------|
| `omop_condition_era_builder` | 매일 02:00 | condition_occurrence → condition_era 변환 (30일 gap 병합) |
| `omop_drug_era_builder` | 매일 02:30 | drug_exposure → drug_era 변환 (30일 gap 병합) |
| `omop_data_quality_check` | 매일 03:00 | row count, NULL 비율, 날짜 유효성, FK 무결성 검증 |
| `omop_stats_summary` | 매일 04:00 | 테이블 통계, demographics, 상위 condition/drug → cdm_summary |

Synthea → OMOP CDM ETL: 13단계 전체 완료 (`synthea/etl_synthea_to_omop.py`)

---

## 보안 아키텍처

| 계층 | 구현 |
|------|------|
| **인증** | JWT 토큰 + Refresh 토큰 |
| **CSRF** | Double-submit cookie 패턴 (전체 POST/PUT/DELETE 적용) |
| **Rate Limit** | IP별 100 req/min, 1000 req/hour (Token Bucket) |
| **보안 헤더** | X-Content-Type-Options, X-Frame-Options, HSTS, CSP |
| **감사 로그** | 전체 API 호출 기록 (actor, action, resource, timestamp) |
| **데이터 보호** | PII 비식별화, 동적 마스킹, 접근 레벨 (public/internal/restricted/confidential) |
| **압축** | GZip (1KB 이상 응답 자동 압축) |

---

## 빠른 시작

### 1. 인프라 실행

```bash
cd infra

# 전체 스택 (OMOP DB + Milvus + MinIO + Airflow + Superset + JupyterLab)
docker compose up -d

# 또는 개별 실행
docker compose -f docker-compose-omop.yml up -d      # OMOP DB
docker compose -f docker-compose-airflow.yml up -d    # Airflow
docker compose -f docker-compose-superset.yml up -d   # Superset
docker compose -f docker-compose-monitoring.yml up -d  # Prometheus + Grafana
docker compose -f docker-compose-nginx.yml up -d       # Nginx
```

### 2. GPU 서버 SSH 터널

```bash
# Qwen3 LLM
ssh -p 20022 -L 28888:localhost:8000 aigen@1.215.235.250

# XiYan SQL (Text2SQL)
ssh -p 20022 -L 8001:localhost:8001 aigen@1.215.235.250

# Medical NER
ssh -p 20022 -L 28100:localhost:8100 aigen@1.215.235.250
```

### 3. 백엔드 실행

```bash
cd data_portal/src/api
pip install -r requirements.txt
python main.py    # http://localhost:8000
```

### 4. 프론트엔드 실행

```bash
cd data_portal/src/portal
npm install
npm run dev    # http://localhost:5173
```

### 접속 정보

- **포털**: http://localhost:5173
- **API 문서**: http://localhost:8000/docs
- **Airflow**: http://localhost:18080 (admin / admin)
- **Superset**: http://localhost:18088 (admin / admin)
- **JupyterLab**: http://localhost:18888
- **MinIO Console**: http://localhost:19001 (minioadmin / minioadmin)
- **Grafana**: http://localhost:13000

---

## 기술 스택

| 계층 | 기술 |
|------|------|
| **Frontend** | React 18, TypeScript, Ant Design, Recharts, ReactFlow, Vite |
| **Backend** | FastAPI, Python 3.11, asyncpg, Pydantic |
| **Database** | PostgreSQL 13 (OMOP CDM) |
| **Vector DB** | Milvus 2.4 (IVF_FLAT, COSINE, 384d) |
| **Object Storage** | MinIO S3 |
| **ETL** | Apache Airflow 2.8, Synthea ETL 파이프라인 |
| **BI** | Apache Superset |
| **AI/LLM** | Qwen3-32B-AWQ (vLLM), XiYan SQL 7B |
| **Medical NLP** | BioClinicalBERT (Medical NER), 한국어 의학사전 67용어 |
| **Text2SQL** | XiYan SQL + 스키마 링킹 + LangGraph 대화 메모리 |
| **RAG** | Sentence-Transformers (384d) + Milvus |
| **분석환경** | JupyterLab (Docker) |
| **모니터링** | Prometheus, Grafana, cAdvisor |
| **Proxy** | Nginx |

---

## 프로젝트 규모

| 항목 | 수치 |
|------|------|
| Backend API 라우터 | 127개 |
| Frontend 페이지 | 31개 |
| Frontend 컴포넌트 그룹 | 14개 |
| 보안 미들웨어 | 4개 (CSRF, Audit, RateLimit, SecurityHeaders) |
| Docker Compose 파일 | 6개 |
| OMOP CDM 레코드 | 92,260,027건 |
| 환자 수 | 76,074명 |

---

**Last Updated**: 2026-02-09
