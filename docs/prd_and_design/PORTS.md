# Seoyon E-Hwa AI Platform 포트 정보

## 사용 중인 포트

| 포트 | 서비스 | 설명 |
|------|--------|------|
| 5500 | Backend (FastAPI) | AI 분석/생성 API 서버 |
| 5510 | Frontend (Vite) | React 웹 UI |
| 8889 | H100 VLM (SSH 터널) | Qwen2.5-VL-32B-Instruct-AWQ |

## 서비스별 상세

### Backend - Port 5500

- **파일**: `backend/main.py`
- **프레임워크**: FastAPI + Uvicorn
- **GPU 사용**: RTX 3090 (Flux.1-dev 이미지 생성만)
- **주요 엔드포인트**:
  - `POST /api/analyze` - 도면 분석
  - `POST /api/refine` - 문서 데이터 추출
  - `POST /api/generate-image` - AI 도면 생성
  - `POST /api/search-similar` - 유사 도면 검색
  - `GET /api/search-stats` - 벡터 검색 통계

### Frontend - Port 5510

- **파일**: `vite.config.ts`
- **프레임워크**: React + Vite + Tailwind CSS
- **접속 URL**: http://localhost:5510

### H100 VLM - Port 8889 (SSH 터널)

- **원격 서버**: `1.215.235.250:20022`
- **모델**: Qwen2.5-VL-32B-Instruct-AWQ
- **API 형식**: OpenAI 호환 (`/v1/chat/completions`)

## 실행 방법

### 1. SSH 터널 설정 (필수)

```bash
# H100 VLM 터널 (먼저 실행)
ssh -L 8889:localhost:8889 -p 20022 aigen@1.215.235.250

# 또는 백그라운드 실행 (autossh 권장)
autossh -M 0 -f -N -L 8889:localhost:8889 -p 20022 aigen@1.215.235.250
```

### 2. 서비스 시작

```bash
# 전체 시작 (백엔드 + 프론트엔드)
./start_all.sh

# 개별 실행
cd backend && uv run python main.py  # Backend
npm run dev                           # Frontend
```

## AI 엔진 Fallback 순서

1. **Claude AI** (Anthropic API) - Primary
2. **Gemini AI** (Google API) - Secondary
3. **H100 VLM** (Qwen2.5-VL-32B) - Final fallback

## 아키텍처

```
┌─────────────────┐     ┌─────────────────┐
│  Frontend       │────▶│  Backend        │
│  (Port 5510)    │     │  (Port 5500)    │
└─────────────────┘     └────────┬────────┘
                                 │
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
┌───────────────┐    ┌───────────────┐    ┌───────────────────┐
│  Claude API   │    │  Gemini API   │    │  H100 VLM         │
│  (External)   │    │  (External)   │    │  (SSH:8889)       │
└───────────────┘    └───────────────┘    │  Qwen2.5-VL-32B   │
                                          └───────────────────┘
                                                   │
                                          ┌───────────────────┐
                                          │  RTX 3090 (로컬)  │
                                          │  Flux.1-dev 전용  │
                                          └───────────────────┘
```

---

*포트 범위 5000-5999는 AI Stream 예약 포트를 피한 사용 가능 대역입니다.*
