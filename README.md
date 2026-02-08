# 서울아산병원 통합 데이터 플랫폼 (IDP)

## 개요

서울아산병원 **통합 데이터 플랫폼(Integrated Data Platform)** 구축 프로젝트입니다.

OMOP CDM 기반 임상 데이터 웨어하우스와 AI 지능형 서비스를 통합하여, 데이터 수집부터 분석/연구까지 전 과정을 지원하는 플랫폼입니다.

---

## 주요 기능

| 기능 | 설명 | 상태 |
|------|------|------|
| ETL 파이프라인 | Airflow 기반 OMOP CDM ETL (13단계 전체 완료) | **완료** |
| 데이터 거버넌스 | 데이터 품질, 비식별화, 리니지, 표준/보안 관리 | **완료** |
| 데이터 카탈로그 | 데이터마트 검색, 스키마/샘플 확인, AI 요약, 데이터 조합 | **완료** |
| 데이터마트 | OMOP CDM 테이블 탐색 및 Text2SQL 자연어 쿼리 | **완료** |
| 데이터 설계 | ERD, 명명 규칙, 비정형 데이터, 존 설계 | **완료** |
| BI 대시보드 | Apache Superset 내장 BI 분석 환경 | **완료** |
| AI 분석환경 | JupyterLab 컨테이너, 노트북/프로젝트/리소스 관리 | **완료** |
| CDW 연구지원 | Text2SQL 대화형 임상 데이터 조회 | **완료** |
| 비정형 구조화 | Medical NER (BioClinicalBERT + 한국어 의학사전 67개 용어) | **완료** |
| 의료 온톨로지 | OMOP CDM Knowledge Graph, RDF Triple, Neo4j Export | **완료** |
| AI 운영관리 | 모델 배포, 모니터링, 감사 로그 | **완료** |
| 보안/권한관리 | RBAC, 동적 마스킹, 데이터셋 권한 | **완료** |
| 메타데이터 관리 | 변경 이력, 매핑, 품질 관리 | **완료** |
| CDC 스트리밍 | 변경 데이터 캡처, 실시간 모니터링 | **완료** |

---

## 시스템 아키텍처

```
┌─────────────────────────────────────────────────────────┐
│                    Frontend (React)                       │
│              Vite + Ant Design + Recharts                │
│                    localhost:5173                         │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────┐
│                  Backend (FastAPI)                        │
│  Text2SQL · Conversation · DataMart · NER · ETL          │
│  Ontology · Governance · Catalog · CDC · AI Ops          │
│                    localhost:8000                         │
└───┬──────────┬──────────┬──────────┬───────────────┬────┘
    │          │          │          │               │
┌───▼───┐ ┌───▼───┐ ┌───▼────┐ ┌──▼──────┐ ┌──────▼─────┐
│OMOP DB│ │Airflow│ │Superset│ │Qwen3 LLM│ │Medical NER │
│PG:5436│ │ :18080│ │ :18088 │ │ :28888  │ │   :28100   │
└───────┘ └───────┘ └────────┘ └─────────┘ └────────────┘
```

---

## 서비스 포트

| 서비스 | 포트 | 용도 |
|--------|------|------|
| Frontend (Vite) | 5173 | React 개발 서버 |
| Backend API | 8000 | FastAPI 백엔드 |
| OMOP CDM DB | 5436 | PostgreSQL 13 (OMOP CDM) |
| Superset DB | 15432 | PostgreSQL 15 (Superset 메타데이터) |
| Airflow | 18080 | ETL 파이프라인 관리 |
| Superset | 18088 | BI 대시보드 |
| JupyterLab | 18888 | 데이터 분석 노트북 |
| Nginx Proxy | 80 | 리버스 프록시 |
| Qwen3 LLM | 28888 | GPU 서버 SSH 터널 |
| Medical NER | 28100 | GPU 서버 SSH 터널 |

---

## 디렉토리 구조

```
asan/
├── ai_services/               # AI 서비스 모듈
│   ├── xiyan_sql/             # Text2SQL (XiYan 기반 스키마 링킹)
│   ├── conversation_memory/   # 대화 이력 관리 (LangGraph)
│   ├── prompt_enhancement/    # 프롬프트 최적화
│   ├── rag/                   # RAG 임베딩/검색
│   └── knowledge_data/        # 의학 지식 데이터
├── data_portal/               # 데이터 포털
│   └── src/
│       ├── api/               # FastAPI 백엔드
│       │   ├── routers/       # API 라우터 (60+ 모듈)
│       │   │   ├── chat*.py           # 대화형 AI
│       │   │   ├── datamart.py        # 데이터마트 API
│       │   │   ├── etl*.py            # ETL 파이프라인
│       │   │   ├── ontology*.py       # 온톨로지 Knowledge Graph
│       │   │   ├── gov*.py            # 거버넌스
│       │   │   ├── data_catalog*.py   # 데이터 카탈로그
│       │   │   ├── cdc*.py            # CDC 스트리밍
│       │   │   ├── security_mgmt*.py  # 보안 관리
│       │   │   ├── permission_mgmt*.py # 권한 관리
│       │   │   └── ...
│       │   └── services/      # 비즈니스 로직 (llm, meta, sql_executor)
│       └── portal/            # React 프론트엔드
│           └── src/
│               ├── pages/     # 페이지 (15+)
│               ├── components/
│               │   ├── ontology/      # 온톨로지 그래프 컴포넌트
│               │   ├── catalog/       # 카탈로그 컴포넌트
│               │   ├── governance/    # 거버넌스 컴포넌트
│               │   ├── etl/           # ETL 컴포넌트
│               │   ├── aienv/         # AI 환경 컴포넌트
│               │   ├── dashboard/     # 대시보드 컴포넌트
│               │   └── datadesign/    # 데이터 설계 컴포넌트
│               └── services/  # API 서비스 레이어 (15+ 모듈)
├── gpu-services/              # GPU 서버 배포 서비스
│   └── medical-ner-service/   # BioClinicalBERT NER 서버
├── infra/                     # 인프라 설정
│   ├── airflow/dags/          # Airflow DAG (OMOP ETL 파이프라인)
│   ├── docker-compose*.yml    # Docker Compose 설정
│   ├── nginx/                 # Nginx 리버스 프록시 설정
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
| measurement | 36,610,971 | 검사결과 |
| observation | 21,289,693 | 관찰 데이터 |
| procedure_occurrence | 12,413,965 | 시술/수술 |
| visit_occurrence | 4,514,630 | 입원(9201)/외래(9202)/응급(9203) |
| cost | 4,389,991 | 의료비용 |
| drug_exposure | 3,928,575 | 투약 |
| payer_plan_period | 2,820,208 | 보험 기간 |
| condition_occurrence | 2,822,068 | 진단 |
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

## 빠른 시작

### 1. 인프라 실행

```bash
cd infra

# OMOP DB
docker compose -f docker-compose-omop.yml up -d

# Airflow
docker compose -f docker-compose-airflow.yml up -d

# Superset
docker compose -f docker-compose-superset.yml up -d

# JupyterLab
docker compose -f docker-compose-jupyter.yml up -d

# Nginx
docker compose -f docker-compose-nginx.yml up -d
```

### 2. GPU 서버 SSH 터널

```bash
# Qwen3 LLM
ssh -p 20022 -L 28888:localhost:8000 aigen@1.215.235.250

# Medical NER
ssh -p 20022 -L 28100:localhost:8100 aigen@1.215.235.250
```

### 3. 백엔드 실행

```bash
cd data_portal/src/api
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 4. 프론트엔드 실행

```bash
cd data_portal/src/portal
npm install
npm run dev    # http://localhost:5173
```

### 접속 정보

- **포털**: http://localhost:5173
- **Airflow**: http://localhost:18080 (admin / admin)
- **Superset**: http://localhost:18088 (admin / admin)
- **JupyterLab**: http://localhost:18888

---

## 기술 스택

- **Frontend**: React 18, TypeScript, Ant Design, Recharts, react-force-graph-2d, Vite
- **Backend**: FastAPI, Python 3.11
- **Database**: PostgreSQL 13 (OMOP CDM)
- **ETL**: Apache Airflow 2.8, Synthea ETL 파이프라인
- **BI**: Apache Superset
- **AI/LLM**: Qwen3-32B-AWQ (vLLM), BioClinicalBERT (Medical NER)
- **Text2SQL**: XiYan SQL + 스키마 링킹 + LangGraph 대화 메모리
- **분석환경**: JupyterLab (Docker)
- **Proxy**: Nginx

---

**Last Updated**: 2026-02-08
