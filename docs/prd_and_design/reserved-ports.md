# AI Stream 예약 포트

> 이 포트들은 AI Stream 프로젝트에서 사용 중입니다. 다른 시스템 구축 시 충돌을 피해주세요.

## 사용 중인 포트 요약

```
3000-3002, 3100, 3200, 4000, 8020, 8081-8083, 8088, 8090-8091, 8180, 8888-8889, 9091, 10300, 19530, 27017
```

---

## 상세 포트 맵

### 외부 노출 (0.0.0.0)

| 포트 | 서비스 | 용도 |
|------|--------|------|
| 3000 | librechat | AI 채팅 UI (메인) |
| 3001 | hwp-converter | HWP→PDF 변환 (Gotenberg) |
| 3002 | firecrawl-api | 웹 스크래핑 API |
| 3100 | langfuse-web | LLM 관측성/트레이싱 UI |
| 3200 | dify-nginx | Dify Low-code AI 플랫폼 |
| 4000 | litellm-proxy | LLM 게이트웨이 |
| 8081 | docling-serve | 문서 파싱 (PDF, DOCX, PPTX) |
| 8082 | embedding-server | bge-m3 임베딩 |
| 8083 | reranker-server | bge-reranker-v2-m3 |
| 8088 | searxng | 메타 검색엔진 |
| 9091 | milvus | 벡터 DB Health |
| 8090 | langgraph-agent | LangGraph 에이전트 API |
| 8091 | guardrails | NeMo Guardrails 필터링 |
| 8180 | keycloak | Keycloak SSO |
| 10300 | whisper-stt | Whisper 음성인식 |
| 19530 | milvus | 벡터 DB gRPC |
| 27017 | mongodb | LibreChat DB |

### SSH 터널 (H100 원격)

| 포트 | 원격 | 용도 |
|------|------|------|
| 8888 | H100:8000 | Qwen3-32B-AWQ LLM API (텍스트) |
| 8889 | H100:8003 | Qwen2.5-VL-32B-AWQ VLM API (비전) |
| 8020 | H100:8020 | XTTS v2 TTS API |

### 내부 전용 (컨테이너 간)

| 포트 | 서비스 | 용도 |
|------|--------|------|
| 2379-2380 | milvus-etcd | etcd 클러스터 |
| 5432 | *-postgres | PostgreSQL (여러 인스턴스) |
| 6379 | *-redis | Redis (여러 인스턴스) |
| 9000 | milvus-minio | MinIO 오브젝트 스토리지 |

---

## 권장 피해야 할 범위

| 범위 | 사유 |
|------|------|
| 3000-3999 | 웹 UI 서비스 |
| 4000-4999 | API 게이트웨이 |
| 8000-8999 | AI/ML 서비스 |
| 9000-9999 | 인프라 (DB, 모니터링) |
| 10000-10999 | 음성 서비스 |
| 19530 | Milvus 고정 포트 |
| 27017 | MongoDB 고정 포트 |

---

## 사용 가능한 포트 (권장)

```
6000-6999  # 커스텀 서비스
7000-7999  # 커스텀 서비스
20000+  # 고포트 대역
```

---

## IDP (통합 데이터 플랫폼) 예약 포트

> ASAN IDP 프로젝트에서 사용 중인 포트

### 외부 노출 (0.0.0.0)

| 포트 | 서비스 | 용도 |
|------|--------|------|
| 5000 | asan-mlflow | MLflow 모델 추적 서버 |
| 15432 | asan-postgres | PostgreSQL (메타데이터) |
| 19000 | asan-minio | MinIO S3 API |
| 19001 | asan-minio | MinIO Console |
| 19091 | milvus-standalone | Milvus Health |
| 19530 | milvus-standalone | Milvus gRPC |
| 16379 | asan-redis | Redis 캐시 |
| 18080 | asan-airflow | Airflow ETL 웹 UI |
| 18088 | asan-superset | Superset BI 대시보드 |
| 18888 | asan-jupyterlab | JupyterLab 분석 환경 |
| 18000 | asan-portal | IDP 사용자 포털 UI |
| 18001 | asan-api | IDP 백엔드 API 서버 |

### IDP 권장 피해야 할 범위

| 범위 | 사유 |
|------|------|
| 5000 | MLflow 고정 |
| 15000-15999 | 데이터베이스 서비스 |
| 16000-16999 | 인프라 서비스 (Redis) |
| 19000-19999 | Milvus, MinIO 서비스 |
| 18000-18999 | 웹 UI 서비스 (Airflow, Superset, Jupyter) |

---

*마지막 업데이트: 2026-02-01*
