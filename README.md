# 서울아산병원 통합 데이터 플랫폼 (IDP)

## 개요

서울아산병원 **통합 데이터 플랫폼(Integrated Data Platform)** 구축 프로젝트입니다.

데이터 레이크하우스 아키텍처 기반으로 CDW(Clinical Data Warehouse)와 EDW(Enterprise Data Warehouse)를 통합하고, AI 기반 지능형 데이터 서비스를 제공합니다.

---

## 주요 기능 (SFR 요구사항)

| 요구사항 ID | 기능명 | 설명 | 구현 상태 |
|------------|--------|------| --- |
| DPR-002 | 데이터마트 카탈로그 | 데이터마트를 검색, 탐색하고 스키마/샘플을 확인하는 UI | **완료** |
| SFR-003 | ETL 대시보드 | Airflow 파이프라인의 상태를 모니터링하는 UI | **완료** |
| DPR-004 | 셀프서비스 BI | Apache Superset을 내장한 BI 대시보드 생성/분석 환경 | **완료** |
| SFR-002 | OLAP 직접 분석 | SQL 편집기를 통해 데이터마트에 직접 OLAP 쿼리 수행 | **완료** |
| SFR-006 | AI 분석환경 | 컨테이너 기반 JupyterLab 분석 환경 제공 및 관리 | **진행중** |
| AAR-001 | AI 지능형 서비스 | Text2SQL, RAG 기반 데이터 탐색 | **진행중** |


---

## 시스템 구성

### Docker 서비스

```bash
docker compose up -d --build
```

| 서비스 | 포트 | 용도 |
|--------|------|------|
| Portal | 18000 | 메인 데이터 포털 (React) |
| API Server | 8000 | 백엔드 API (FastAPI) |
| PostgreSQL | 15432 | 메타데이터 저장소 |
| Redis | 16379 | 캐시 |
| Qdrant | 16333, 16334 | 벡터 데이터베이스 |
| Airflow | 18080 | ETL 파이프라인 (DAG) |
| MLflow | 5000 | ML 모델 추적 |
| JupyterLab | 18888 | 데이터 분석 노트북 |
| Superset | 18088 | BI 대시보드 |

### 접속 정보

- **메인 포털**: http://localhost:18000
- **Airflow**: http://localhost:18080 (admin / admin)
- **Superset**: http://localhost:18088 (admin / admin)
- **JupyterLab**: http://localhost:18888 (토큰 없음)
- **MLflow**: http://localhost:5000

---

## 디렉토리 구조

```
asan/
├── src/                    # 소스 코드
│   ├── api/               # API 서버 (FastAPI)
│   └── portal/            # 데이터 포털 UI (React)
├── data/                   # 데이터 저장소 (DB, VectorDB 등)
├── docker/                 # Docker 설정
├── doc/                    # 문서
├── notebooks/              # Jupyter 노트북
├── SFR-005_etl/            # Airflow DAGs
├── docker-compose.yml      # Docker Compose 설정
└── README.md               # 프로젝트 안내
```

---

## 빠른 시작

### 1. 환경 설정

```bash
# .env 파일 생성
cp .env.example .env

# 필요한 API 키 설정
# OPENAI_API_KEY=sk-...
```

### 2. 백엔드 서비스 실행

```bash
# Docker 컨테이너 빌드 및 실행
docker compose up -d --build

# 상태 확인
docker compose ps
```

### 3. 프론트엔드 개발 서버 실행

```bash
# 프론트엔드 디렉토리로 이동
cd src/portal

# 종속성 설치
npm install

# 개발 서버 실행 (http://localhost:3000)
npm run dev
```

---

## 기술 스택

- **Frontend**: React, TypeScript, Ant Design, Vite
- **Backend**: FastAPI, Python
- **데이터 레이크하우스**: Apache Iceberg / Delta Lake
- **ETL/ELT**: Apache Airflow
- **벡터DB**: Qdrant
- **OLAP**: DuckDB
- **BI**: Apache Superset
- **ML 추적**: MLflow
- **AI/LLM**: OpenAI GPT-4, Claude, Multi-LLM
- **MCP**: Model Context Protocol

---

**Last Updated**: 2026-02-02
