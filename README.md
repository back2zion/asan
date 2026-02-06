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

### Docker 서비스 실행

```bash
# infra 디렉토리에서 Docker Compose 실행
cd infra

# Docker Compose V2 (권장)
docker compose up -d --build

# 또는 Docker Compose V1 (레거시)
docker-compose up -d --build

# 상태 확인
docker compose ps  # 또는 docker-compose ps
```

| 서비스 | 포트 | 용도 |
|--------|------|------|
| Portal | 18000 | 메인 데이터 포털 (React + Nginx) |
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
├── ai_services/            # AI 서비스 관련 코드 및 데이터
│   ├── analysis/          # AI 분석 모듈
│   └── knowledge_data/    # 의학 지식 데이터
├── data/                   # 데이터 저장소 (DB, VectorDB 등)
├── data_lakehouse/         # 데이터 레이크하우스 연구
│   └── research/          # CDW 연구 자료
├── data_pipeline/          # 데이터 파이프라인
│   ├── etl_airflow/       # Airflow DAGs
│   └── scripts/           # ETL 스크립트
├── data_portal/            # 데이터 포털 UI
│   └── src/portal/        # React 프론트엔드
├── docs/                   # 프로젝트 문서
│   └── prd_and_design/    # PRD 및 설계 문서
├── governance/             # 데이터 거버넌스
├── infra/                  # 인프라 설정
│   ├── docker/            # Docker 설정 파일
│   │   ├── duckdb/       # DuckDB 설정
│   │   └── nginx/        # Nginx 설정
│   └── docker-compose.yml # Docker Compose 설정
├── notebooks/              # Jupyter 노트북
├── .env.example            # 환경 변수 예시
└── README.md               # 프로젝트 안내
```

---

## 빠른 시작

### 1. 환경 설정

```bash
# .env 파일 생성
cp .env.example .env

# 필요한 API 키 설정 (선택사항)
# LLM_API_URL=http://host.docker.internal:8888/v1
# LLM_MODEL=Qwen3-32B-AWQ
# OPENAI_API_KEY=sk-...
```

### 2. 백엔드 서비스 실행

```bash
# infra 디렉토리로 이동
cd infra

# Docker 컨테이너 빌드 및 실행 (V2 또는 V1)
docker compose up -d --build
# 또는: docker-compose up -d --build

# 상태 확인
docker compose ps

# 로그 확인
docker compose logs -f
```

### 3. 프론트엔드 개발 서버 실행

```bash
# 프론트엔드 디렉토리로 이동
cd data_portal/src/portal

# 종속성 설치
npm install

# 개발 서버 실행 (http://localhost:5173)
npm run dev

# 프로덕션 빌드 (Nginx 배포용)
npm run build
```

### 4. 서비스 중지

```bash
cd infra
docker compose down
# 또는: docker-compose down
```

---

## 트러블슈팅

### Docker Compose 플러그인 오류

`docker compose` 명령이 안 될 경우:

```bash
# Docker Compose 플러그인 재설치
sudo apt-get update && sudo apt-get install -y docker-compose-plugin

# 또는 독립 실행형 docker-compose 설치
sudo curl -L "https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-linux-x86_64" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

---

## 기술 스택

- **Frontend**: React 18, TypeScript, Ant Design, Vite, TailwindCSS
- **Backend**: FastAPI, Python 3.11
- **데이터 레이크하우스**: Apache Iceberg / Delta Lake
- **ETL/ELT**: Apache Airflow 2.8
- **벡터DB**: Qdrant
- **OLAP**: DuckDB
- **BI**: Apache Superset 3.1
- **ML 추적**: MLflow 2.10
- **AI/LLM**: OpenAI GPT-4, Claude, Qwen, Multi-LLM
- **MCP**: Model Context Protocol

---

**Last Updated**: 2026-02-03
