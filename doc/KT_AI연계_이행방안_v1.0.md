# 서울아산병원 IDP 구축 - AI 연계 이행 방안

## 작성일: 2026-02-01
## 작성자: 데이터스트림즈
## 대상: KT AI 솔루션팀 서찬미님

---

## 1. 요구사항별 이행 방안 총괄표

| 요구사항 분류 | 고유번호 | 요구사항 세부내용 | D/S 이행 | 제공 형상(이행 방안) | AI 연계 필요사항 |
|-------------|---------|-----------------|---------|-------------------|-----------------|
| 데이터 설계 요구사항 | DIT-001 | 비정형 데이터 구조화∙정형화 대상 데이터 분석, 정형화 방안 제시 (암병원 데이터 대상 – 병리검사, 영상검사, 수술기록, 경과기록 등) | O | **[방안 2.1 참조]**<br>- 비정형 데이터 분석 파이프라인 구축<br>- NLP 기반 정형화 엔진 개발<br>- 메타데이터 자동 추출 모듈 | **AI 모델 API 연동 필요**<br>- 의료 NLP 모델 (텍스트→구조화)<br>- 개체명 인식(NER) 모델 호출 API |
| 데이터 설계 요구사항 | DIT-001 | 비정형 데이터를 포함 분석하여, 데이터의 용도가 명확한 체계 구성 방안 제시 | O | **[방안 2.1 참조]**<br>- 용도별 데이터 분류 체계 수립<br>- 데이터 카탈로그 메타 정보 구성 | 분류 모델 결과 수신 API |
| 데이터 설계 요구사항 | DIT-002 | "비정형 데이터→정형 데이터", "비표준 용어→표준 용어" 적용의 Flow 설계 시 단계별 데이터 스토리지 구성과 각 단계별 사용자 서비스를 위한 데이터의 선처리, 메타 정보 구성 방안 제시 | O | **[방안 2.2 참조]**<br>- 3-Stage 스토리지 아키텍처<br>- 표준용어 매핑 엔진<br>- Vector DB 적재 파이프라인 | **Vector DB 형태로 제공**<br>- Embedding 저장소 연동 API<br>- 표준용어 매핑 결과 API |
| 데이터 포털 요구사항 | DPR-001 | 우측 AI Assistant (보조 도구) | O | **[방안 3.1 참조]**<br>- AI Assistant 연동 UI 컴포넌트<br>- 사이드바 패널 구현 | **AI Assistant API 형태 제공 필요**<br>- Chat Completion API 연동 |
| 데이터 포털 요구사항 | DPR-001 | 화면 우측에는 언제든지 호출/접기가 가능한 AI Assistant 패널을 Side-bar 형태로 배치 | O | **[방안 3.1 참조]**<br>- 토글 가능한 Side-bar UI<br>- 세션 상태 관리 | AI 세션 관리 API |
| 데이터 포털 요구사항 | DPR-001 | 사용자는 중앙 화면을 보면서 우측 AI Assistant에게 자연어로 데이터 검색, 필터링 조건 설정, 쿼리 생성 요청 등을 수행 | O | **[방안 3.2 참조]**<br>- Semantic Layer API 제공<br>- Text2SQL 컨텍스트 API<br>- 필터 조건 생성 API | **MCP(Model Context Protocol) 연동**<br>- 데이터 스키마 컨텍스트 제공<br>- SQL 생성용 메타데이터 API |
| 데이터 포털 요구사항 | DPR-001 | AI와의 대화 내용은 타임라인 형태로 기록되며, 클릭 시 해당 시점의 데이터 탐색 화면 상태로 복원 | O | **[방안 3.3 참조]**<br>- 대화 이력 저장소<br>- 화면 상태 스냅샷 관리<br>- 복원 API | 대화 이력 저장 API (KT 측 제공 또는 D/S 자체 구현) |
| 데이터 포털 요구사항 | DPR-001 | 데이터 카탈로그 및 시각화 (Main View 연동) | O | **[방안 3.4 참조]**<br>- 카탈로그 Tree/Graph View API<br>- 시각화 연동 인터페이스 | 시각화 추천 AI API (선택) |
| 데이터 포털 요구사항 | DPR-001 | 데이터 간의 상하/포함/연관 관계를 보여주는 계층 구조(Tree/Graph View)는 메인 화면 내에서 직관적으로 조작 가능 | O | **[방안 3.4 참조]**<br>- Data Lineage API<br>- 계층 구조 조회 API<br>- 인터랙티브 Graph 컴포넌트 | - |
| 데이터 포털 요구사항 | DPR-001 | 카탈로그 탐색 중 AI Assistant를 통해 "이 테이블과 연관된 진료과 데이터 찾아줘"와 같은 맥락 기반 명령 가능 | O | **[방안 3.5 참조]**<br>- 컨텍스트 기반 검색 API<br>- 현재 탐색 상태 전달 API | **컨텍스트 전달 API**<br>- 현재 선택된 테이블/컬럼 정보<br>- 사용자 탐색 히스토리 |
| 데이터 포털 요구사항 | DPR-001 | 검색 결과 최상단에는 AI 요약 답변을 제시하고, 하단에는 근거가 되는 Raw-data, 연관 다빈도 활용 데이터(마스터 분석 모델), 관련 문건을 리스트로 제공 | O | **[방안 3.6 참조]**<br>- 검색 결과 구조화 API<br>- 근거 데이터 연결 API<br>- 다빈도 데이터 추천 API | **AI 요약 API 연동 필요**<br>- 검색 결과 요약 생성 API<br>- RAG 기반 답변 생성 |
| 데이터 포털 요구사항 | DPR-001 | 검색 목록에는 제목, 요약 설명, 최종 수정일, 활용 횟수(인기도), 태그 정보와 함께 좌측에 다차원 필터링(Faceted Search) 기능 제공 | O | **[방안 3.7 참조]**<br>- Faceted Search API<br>- 활용 통계 API<br>- 태그 관리 API | - |
| 데이터 포털 요구사항 | DPR-002 | 메타데이터 기반 통합 검색 환경 제공 | O | **[방안 4.1 참조]**<br>- Semantic Layer 통합 검색 API<br>- 비즈니스 용어 해석기 | **검색 의도 파악 AI 연동**<br>- 자연어 쿼리 해석 API |
| 데이터 포털 요구사항 | DPR-002 | 비즈니스 시맨틱 계층(Semantic Layer)을 적용하여 복잡한 테이블명을 직관적인 비즈니스 용어로 제공 | O | **[방안 4.2 참조]**<br>- 물리명↔비즈니스명 변환 API<br>- 동의어/약어 확장 API<br>- 도메인별 검색 API | - |

---

## 2. 데이터 설계 요구사항 상세 이행 방안

### 2.1 비정형 데이터 구조화·정형화 (DIT-001)

#### 대상 데이터
| 데이터 유형 | 원천 시스템 | 데이터 형태 | 정형화 대상 |
|-----------|-----------|-----------|-----------|
| 병리검사 소견 | LIS | CLOB/TEXT | 진단명, 조직형태, 분화도, 마진상태 등 |
| 영상검사 판독문 | PACS/RIS | CLOB/TEXT | 소견, 결론, 권고사항, 병변위치/크기 |
| 수술기록 | EMR | CLOB/TEXT | 수술명, 수술소견, 출혈량, 합병증 |
| 경과기록 | EMR | CLOB/TEXT | 주호소, 현병력, 계획 |

#### 이행 방안
```
┌─────────────────────────────────────────────────────────────────────┐
│                    비정형 데이터 정형화 파이프라인                      │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  [원천 시스템]     [전처리 단계]      [정형화 단계]     [적재 단계]     │
│                                                                     │
│  ┌─────────┐     ┌─────────────┐   ┌─────────────┐   ┌───────────┐ │
│  │  PACS   │────▶│ 텍스트 추출  │──▶│  NLP 처리   │──▶│정형 테이블│ │
│  │ 판독문  │     │ 노이즈 제거  │   │ (AI 연동)  │   │ (Bronze)  │ │
│  └─────────┘     └─────────────┘   └─────────────┘   └───────────┘ │
│                                           │                        │
│  ┌─────────┐     ┌─────────────┐   ┌─────────────┐   ┌───────────┐ │
│  │  LIS    │────▶│ 섹션 분리   │──▶│ 개체명 추출 │──▶│ 메타 정보 │ │
│  │병리소견 │     │ 구조화      │   │ (NER)      │   │ (Silver)  │ │
│  └─────────┘     └─────────────┘   └─────────────┘   └───────────┘ │
│                                           │                        │
│  ┌─────────┐     ┌─────────────┐   ┌─────────────┐   ┌───────────┐ │
│  │  EMR    │────▶│ 비식별화    │──▶│ 표준용어   │──▶│ Vector DB │ │
│  │수술기록 │     │ 마스킹      │   │   매핑     │   │ (Gold)    │ │
│  └─────────┘     └─────────────┘   └─────────────┘   └───────────┘ │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

#### AI 연동 인터페이스 (D/S → KT)
```python
# 정형화 요청 API (D/S가 KT AI로 호출)
POST /api/v1/ai/extract-entities
Request:
{
    "document_type": "pathology_report",  # pathology_report, radiology_report, op_note
    "text": "소견: 좌측 폐 하엽에 2.3cm 크기의 결절이 관찰됨...",
    "extract_fields": ["finding", "conclusion", "lesion_size", "lesion_location"]
}

Response:
{
    "entities": [
        {"field": "lesion_location", "value": "좌측 폐 하엽", "confidence": 0.95},
        {"field": "lesion_size", "value": "2.3cm", "confidence": 0.92},
        {"field": "finding", "value": "결절 관찰", "confidence": 0.88}
    ],
    "structured_data": {
        "location": "left_lower_lobe",
        "size_cm": 2.3,
        "finding_type": "nodule"
    }
}
```

---

### 2.2 단계별 데이터 스토리지 구성 (DIT-002)

#### 3-Stage 스토리지 아키텍처
```
┌───────────────────────────────────────────────────────────────────────────┐
│                        데이터 레이크하우스 아키텍처                          │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  Stage 1: Bronze (원천)        Stage 2: Silver (정제)    Stage 3: Gold    │
│  ┌────────────────────┐       ┌────────────────────┐   ┌───────────────┐ │
│  │ 원본 비정형 데이터   │       │ 정형화된 데이터     │   │ 분석용 마트   │ │
│  │ - Raw Text         │──────▶│ - 구조화 테이블     │──▶│ - 집계 테이블 │ │
│  │ - DICOM Metadata   │       │ - 표준용어 매핑     │   │ - 코호트 마트 │ │
│  │ - 비식별 처리 전    │       │ - 메타데이터       │   │ - 연구 마트   │ │
│  └────────────────────┘       └────────────────────┘   └───────────────┘ │
│           │                            │                       │         │
│           ▼                            ▼                       ▼         │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                        Vector DB (AI 연동용)                        │ │
│  │  - Text Embedding 저장                                             │ │
│  │  - 유사 문서 검색 지원                                              │ │
│  │  - RAG 기반 질의응답 지원                                           │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

#### Vector DB 적재 형상 (D/S → KT AI)
```python
# Vector DB 적재 API (D/S가 제공)
POST /api/v1/vectordb/upsert
Request:
{
    "collection": "pathology_reports",
    "documents": [
        {
            "id": "PATH_2026_00001",
            "text": "좌측 폐 하엽 결절, 선암 의심",
            "metadata": {
                "patient_id_hash": "a1b2c3d4...",
                "report_date": "2026-01-15",
                "department": "병리과",
                "document_type": "pathology",
                "standard_terms": ["선암", "폐암", "결절"],
                "icd_codes": ["C34.3"]
            }
        }
    ],
    "embedding_model": "medical-ko-bert"  # 사용할 임베딩 모델 지정
}

# Vector 검색 API (KT AI가 호출)
POST /api/v1/vectordb/search
Request:
{
    "collection": "pathology_reports",
    "query": "폐암 3기 환자의 병리 소견",
    "top_k": 10,
    "filters": {
        "department": "병리과",
        "report_date": {"gte": "2025-01-01"}
    }
}
```

---

## 3. 데이터 포털 요구사항 상세 이행 방안

### 3.1 AI Assistant 연동 UI (DPR-001)

#### 제공 형상
```
┌─────────────────────────────────────────────────────────────────────┐
│                      데이터 포털 메인 화면                            │
├─────────────────────────────────────────┬───────────────────────────┤
│                                         │    AI Assistant Panel     │
│                                         │   ┌───────────────────┐   │
│         Main Content Area               │   │ 🤖 AI Assistant   │   │
│                                         │   ├───────────────────┤   │
│  ┌─────────────────────────────────┐   │   │ [대화 이력]        │   │
│  │                                 │   │   │                   │   │
│  │   데이터 카탈로그 / 검색 결과     │   │   │ User: 진단 테이블 │   │
│  │                                 │   │   │       찾아줘      │   │
│  │                                 │   │   │                   │   │
│  │                                 │   │   │ AI: TB_DX_HST     │   │
│  │                                 │   │   │     테이블을...   │   │
│  │                                 │   │   │                   │   │
│  └─────────────────────────────────┘   │   ├───────────────────┤   │
│                                         │   │ [입력창]          │   │
│                                         │   │ ┌───────────────┐ │   │
│                                         │   │ │ 질문 입력...   │ │   │
│                                         │   │ └───────────────┘ │   │
│                                         │   └───────────────────┘   │
│                                         │   [접기/펼치기 버튼]      │
└─────────────────────────────────────────┴───────────────────────────┘
```

#### AI Assistant 연동 API (KT가 제공해야 할 API)
```python
# Chat Completion API (포털 → KT AI)
POST /api/v1/ai/chat
Request:
{
    "session_id": "sess_12345",
    "message": "진단 테이블과 연관된 환자 데이터 찾아줘",
    "context": {
        "current_table": "TB_DX_HST",
        "current_columns": ["PAT_NO", "DX_CD", "DX_NM"],
        "user_role": "researcher",
        "data_permissions": ["CDW", "EDW"]
    },
    "tools": [
        {
            "type": "function",
            "function": {
                "name": "search_catalog",
                "description": "데이터 카탈로그 검색"
            }
        },
        {
            "type": "function",
            "function": {
                "name": "generate_sql",
                "description": "SQL 쿼리 생성"
            }
        }
    ]
}

Response:
{
    "session_id": "sess_12345",
    "response": "TB_DX_HST(진단 이력) 테이블은 TB_PAT_MST(환자 마스터)와 PAT_NO 컬럼으로 연결됩니다.",
    "tool_calls": [
        {
            "function": "search_catalog",
            "arguments": {"query": "환자 마스터", "related_to": "TB_DX_HST"}
        }
    ],
    "suggested_actions": [
        {"action": "view_table", "target": "TB_PAT_MST"},
        {"action": "generate_join_sql", "tables": ["TB_DX_HST", "TB_PAT_MST"]}
    ]
}
```

### 3.2 Semantic Layer API (D/S가 제공)

#### API 엔드포인트 목록
| 엔드포인트 | 메서드 | 설명 | AI 연동 용도 |
|-----------|-------|------|-------------|
| `/api/v1/semantic/search` | GET | 통합 검색 | 자연어 질의 해석 |
| `/api/v1/semantic/translate/physical-to-business` | POST | 물리명→비즈니스명 | 결과 표시 |
| `/api/v1/semantic/translate/business-to-physical` | GET | 비즈니스명→물리명 | SQL 생성 |
| `/api/v1/semantic/sql-context` | GET | SQL 생성 컨텍스트 | Text2SQL 지원 |
| `/api/v1/semantic/lineage/{table}` | GET | 데이터 계보 | 연관 데이터 탐색 |
| `/api/v1/semantic/catalog/tree` | GET | 카탈로그 트리 | 네비게이션 |

#### SQL 생성 컨텍스트 API 예시
```python
# D/S가 제공하는 API (KT AI가 호출)
GET /api/v1/semantic/sql-context?q=고혈압 환자의 CT 검사 결과

Response:
{
    "success": true,
    "data": {
        "original_query": "고혈압 환자의 CT 검사 결과",
        "understanding": "'고혈압' → '진단' 도메인, 'CT 검사' → '영상' 도메인으로 분류",
        "relevant_tables": [
            {
                "physical_name": "CDW.TB_DX_HST",
                "business_name": "진단 이력",
                "columns": [
                    {"physical": "PAT_NO", "business": "환자번호", "type": "VARCHAR(10)"},
                    {"physical": "DX_CD", "business": "진단코드", "type": "VARCHAR(10)"},
                    {"physical": "DX_NM", "business": "진단명", "type": "VARCHAR(200)"}
                ]
            },
            {
                "physical_name": "CDW.TB_IMG_RST",
                "business_name": "영상검사 결과",
                "columns": [
                    {"physical": "PAT_NO", "business": "환자번호", "type": "VARCHAR(10)"},
                    {"physical": "IMG_TP_CD", "business": "영상검사유형", "type": "VARCHAR(10)"},
                    {"physical": "RPT_TXT", "business": "판독소견", "type": "CLOB"}
                ]
            }
        ],
        "suggested_joins": [
            {
                "from": "TB_DX_HST.PAT_NO",
                "to": "TB_IMG_RST.PAT_NO",
                "type": "INNER JOIN"
            }
        ],
        "filters": [
            {"table": "TB_DX_HST", "column": "DX_NM", "condition": "LIKE '%고혈압%'"},
            {"table": "TB_IMG_RST", "column": "IMG_TP_CD", "condition": "= 'CT'"}
        ]
    }
}
```

---

## 4. AI 연계 인터페이스 정의

### 4.1 D/S → KT AI 호출 API (D/S가 필요로 하는 API)

| API | 용도 | 호출 시점 | 요청 형식 |
|-----|-----|---------|---------|
| 개체명 추출 (NER) | 비정형 텍스트에서 의료 개체 추출 | 데이터 정형화 시 | `POST /ai/extract-entities` |
| 텍스트 분류 | 문서 유형/도메인 분류 | 메타데이터 생성 시 | `POST /ai/classify` |
| 텍스트 임베딩 | Vector DB 적재용 | 데이터 적재 시 | `POST /ai/embedding` |
| 요약 생성 | 검색 결과 요약 | 검색 시 | `POST /ai/summarize` |

### 4.2 KT AI → D/S 호출 API (D/S가 제공하는 API)

| API | 용도 | 제공 형식 |
|-----|-----|---------|
| Semantic Search | 자연어 기반 카탈로그 검색 | `GET /semantic/search` |
| SQL Context | Text2SQL용 스키마 컨텍스트 | `GET /semantic/sql-context` |
| Vector Search | 유사 문서 검색 | `POST /vectordb/search` |
| Data Lineage | 테이블 관계 정보 | `GET /semantic/lineage/{table}` |
| Table Metadata | 테이블 상세 정보 | `GET /semantic/table/{name}` |

### 4.3 MCP (Model Context Protocol) 연동

```yaml
# MCP Server 설정 (D/S가 제공)
mcp_server:
  name: "asan-idp-semantic-layer"
  version: "1.0.0"
  tools:
    - name: "search_data_catalog"
      description: "데이터 카탈로그에서 테이블/컬럼 검색"
      parameters:
        query: { type: "string", description: "검색어" }
        domain: { type: "string", description: "도메인 필터", optional: true }

    - name: "get_table_schema"
      description: "테이블 스키마 정보 조회"
      parameters:
        table_name: { type: "string", description: "물리적 테이블명" }

    - name: "translate_term"
      description: "물리명/비즈니스명 상호 변환"
      parameters:
        term: { type: "string" }
        direction: { type: "string", enum: ["p2b", "b2p"] }

    - name: "get_join_path"
      description: "두 테이블 간 JOIN 경로 조회"
      parameters:
        table1: { type: "string" }
        table2: { type: "string" }

  resources:
    - name: "catalog://tables"
      description: "전체 테이블 목록"
    - name: "catalog://domains"
      description: "도메인 목록"
```

---

## 5. 추가 논의 필요 사항

### 5.1 확인 필요 사항 (KT → D/S)

| 항목 | 질문 | 비고 |
|-----|-----|-----|
| AI 모델 호스팅 | AI 모델은 KT Cloud에서 호스팅? On-premise? | 네트워크 구성 영향 |
| API 인증 | API 인증 방식 (API Key, OAuth2, mTLS?) | 보안 설계 영향 |
| 응답 시간 요구사항 | AI API 호출 시 예상 응답 시간? | 비동기 처리 여부 결정 |
| 배치 처리 | 대량 데이터 정형화 시 배치 API 제공 여부? | 파이프라인 설계 |
| 에러 처리 | AI 모델 오류 시 Fallback 정책? | 장애 대응 |

### 5.2 추가 연계 필요 사항 (D/S → KT)

| 항목 | 설명 | 필요 이유 |
|-----|-----|---------|
| **RAG 파이프라인** | Vector DB 기반 검색 증강 생성 | 검색 결과 요약/답변 생성 |
| **Fine-tuning 데이터** | 의료 도메인 학습 데이터 제공 | 모델 정확도 향상 |
| **피드백 수집** | 사용자 피드백 → 모델 개선 | 지속적 품질 향상 |
| **모델 버전 관리** | 모델 업데이트 시 호환성 | 서비스 안정성 |

---

## 6. 일정 및 마일스톤

| Phase | 기간 | 주요 산출물 | AI 연계 항목 |
|-------|-----|-----------|-------------|
| Phase 1 | M+1 ~ M+3 | Semantic Layer 기본 구현 | API 인터페이스 정의 |
| Phase 2 | M+4 ~ M+6 | 비정형 데이터 파이프라인 | NER/분류 모델 연동 |
| Phase 3 | M+7 ~ M+9 | Vector DB 구축 | Embedding 연동 |
| Phase 4 | M+10 ~ M+12 | 포털 UI 통합 | AI Assistant 연동 |
| Phase 5 | M+13 ~ M+15 | 통합 테스트 | 전체 연동 검증 |

---

## 7. 첨부

### 7.1 구현 완료된 코드 (POC)

- `src/semantic_layer/semantic_metadata.py`: 시맨틱 메타데이터 저장소
- `src/semantic_layer/term_resolver.py`: 비즈니스 용어 해석기
- `src/semantic_layer/semantic_api.py`: REST API 인터페이스

### 7.2 API 명세서 (별도 문서)

- Swagger/OpenAPI 스펙 제공 예정

---

**문의사항**: 데이터스트림즈 [담당자명] / [연락처]
