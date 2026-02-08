# 프로젝트 포트 사용 현황

이 문서는 서울아산병원 통합 데이터 플랫폼(IDP) 프로젝트에서 사용되는 주요 서비스 및 애플리케이션의 네트워크 포트 정보를 정리합니다.

## 1. Docker Compose 서비스 포트

`docker-compose.yml` 파일에 정의된 서비스별 포트 매핑은 다음과 같습니다. 호스트 머신에서 접근 가능한 외부 포트를 기준으로 작성되었습니다.

| 서비스 이름 | 내부 컨테이너 포트 | 호스트 포트 | 설명 |
| :---------- | :----------------- | :---------- | :------------------------------------------- |
| `portal`    | `80`               | `18000`     | 메인 데이터 포털 웹 UI (Nginx)              |
| `api`       | `8000`             | `8000`      | 백엔드 API 서버 (FastAPI)                   |
| `postgres`  | `5432`             | `15432`     | PostgreSQL 메타데이터 데이터베이스          |
| `omop-db`   | `5432`             | `5436`      | **OMOP CDM 데이터베이스 (Synthea 합성 데이터, 92M rows)** ⚠️ |
| `superset-db` | `5432`           | `15432`     | Superset 메타데이터 DB (postgres:15)        |
| `redis`     | `6379`             | `16379`     | Redis 캐시 및 메시지 브로커                 |
| `qdrant`    | `6333`, `6334`     | `16333`, `16334` | Qdrant 벡터 데이터베이스 (gRPC, HTTP)        |
| `airflow-webserver` | `8080`     | `18080`     | Apache Airflow 웹 UI                        |
| `mlflow`    | `5000`             | `5000`      | MLflow 트래킹 서버 (ML 모델 관리)           |
| `jupyterlab` | `8888`            | `18888`     | JupyterLab 분석 환경 (로컬 Docker 컨테이너) |
| `superset`  | `8088`             | `18088`     | Apache Superset BI 대시보드                |

⚠️ **포트 충돌 주의**: 로컬 PostgreSQL 14가 기본 5432 포트를 사용 중이므로, OMOP CDM DB는 **5436**으로 리매핑되어 있습니다.

## 2. 프론트엔드 개발 서버 포트

프론트엔드 개발 환경에서 사용되는 포트입니다.

| 서비스 이름 | 포트  | 설명                                       |
| :---------- | :---- | :----------------------------------------- |
| Vite 개발 서버 | `5173` | `data_portal/src/portal` 애플리케이션 개발 시 사용 |

## 3. GPU 서버 SSH 터널 포트

GPU 서버(`ssh -p 20022 aigen@1.215.235.250`)에서 실행 중인 서비스를 로컬 머신에서 접근하기 위한 SSH 터널 매핑입니다.

| 로컬 포트 | GPU 서버 포트 | 서비스 이름 | 설명 |
| :-------- | :------------ | :---------- | :----------------------------------------- |
| `28888`   | `8000`        | Qwen3 LLM   | LLM API 서버 (텍스트 생성, Text2SQL 등)   |
| `29001`   | `9001`        | Paper2Slides | 논문 -> PPT 변환 서비스 (비동기 작업)     |
| `28100`   | `8100`        | Medical NER  | 의료 개체명 인식 서비스 (BioClinicalBERT) |
| `18888`   | `8888`        | JupyterLab (GPU) | GPU 서버의 JupyterLab (로컬 Docker와 별도) |

**접근 예시**:
- LLM API: `http://localhost:28888/v1`
- Paper2Slides: `http://localhost:29001`
- Medical NER: `http://localhost:28100/ner/analyze`

## 4. 백엔드 API 내부 통신 포트

`api` 서비스 컨테이너 내부에서 다른 서비스와 통신하는 데 사용되는 포트입니다. 이 포트들은 일반적으로 호스트 머신에 직접 노출되지 않습니다.

| 서비스 이름 | 포트 | 설명                                       |
| :---------- | :--- | :----------------------------------------- |
| `postgres`  | `5432` | `api` 서비스에서 `postgres` 컨테이너 접근 |
| `omop-db`   | `5432` | `api` 서비스에서 `omop-db` 컨테이너 접근 (실제 호스트 포트는 5436) |
| `redis`     | `6379` | `api` 서비스에서 `redis` 컨테이너 접근    |
| `qdrant`    | `6333` | `api` 서비스에서 `qdrant` 컨테이너 접근   |

## 5. 기타 연동 서비스 포트

LLM, 임베딩 모델 등 외부 또는 내부망에서 연동될 수 있는 서비스의 포트입니다. 이들은 `api` 서비스의 환경 변수에 의해 설정됩니다.

| 환경 변수           | 기본값                               | 설명                                 |
| :------------------ | :----------------------------------- | :----------------------------------- |
| `LLM_API_URL`       | `http://host.docker.internal:28888/v1` | Qwen3 LLM API 서버 (GPU SSH 터널)  |
| `EMBEDDING_API_URL` | `http://host.docker.internal:8082`    | 임베딩 모델 API 서버 엔드포인트  |

---

## 6. 주요 포트 충돌 및 주의사항

### 포트 충돌
- **5432 포트**: 로컬 PostgreSQL 14와 충돌 방지를 위해 OMOP CDM DB는 **5436**으로 리매핑
- **18888 포트**: 로컬 JupyterLab Docker 컨테이너 사용 (GPU 서버 JupyterLab과는 별도)

### SSH 터널 체크리스트
백엔드 API 서버가 GPU 서비스와 통신하려면 다음 SSH 터널이 활성화되어 있어야 합니다:
```bash
# LLM 서비스 (필수)
ssh -p 20022 -L 28888:localhost:8000 aigen@1.215.235.250

# Paper2Slides (Presentation 기능 사용 시)
ssh -p 20022 -L 29001:localhost:9001 aigen@1.215.235.250

# Medical NER (NER 기능 사용 시)
ssh -p 20022 -L 28100:localhost:8100 aigen@1.215.235.250
```

### 모니터링
- **Watchdog 스크립트**: `infra/watchdog.sh`가 2분마다 crontab으로 실행되어 SSH 터널 상태 확인
- **서비스 헬스체크**: 각 서비스는 `/health` 엔드포인트 제공

---

**참고**: `host.docker.internal`은 Docker 컨테이너 내부에서 호스트 머신에 접근할 수 있도록 해주는 특별한 DNS 이름입니다. SSH 터널로 매핑된 GPU 서비스에 접근할 때 유용합니다.
