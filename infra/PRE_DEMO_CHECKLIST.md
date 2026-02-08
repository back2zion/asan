# 시연 전 체크리스트

## 🎯 시연 30분 전

### 1. 전체 시스템 시작
```bash
cd /home/babelai/datastreams-work/datastreams/asan/infra
./demo_start.sh
```

### 2. 헬스체크 확인
```bash
/tmp/service_check.sh
```

모든 항목이 ✅ 녹색이어야 합니다.

### 3. 주요 기능 테스트

#### ✅ 대시보드 (http://localhost:5173)
- [ ] 페이지 로딩
- [ ] 차트 표시
- [ ] 통계 숫자 정확

#### ✅ CDW 연구 (http://localhost:5173/cdw)
- [ ] Enhancement 버튼 클릭
- [ ] SQL 생성 확인
- [ ] 결과 조회 확인

#### ✅ NER 분석 (http://localhost:5173/ner)
- [ ] 한글 텍스트 분석 ("당뇨병과 고혈압")
- [ ] 영문 텍스트 분석 ("diabetes and hypertension")
- [ ] 결과 하이라이팅 확인

#### ✅ BI 대시보드 (http://localhost:5173/bi)
- [ ] Superset 통계 표시
- [ ] 차트/대시보드 개수 확인

#### ✅ AI 분석환경 (http://localhost:5173/ai-environment)
- [ ] 컨테이너 목록 표시
- [ ] JupyterLab 링크 클릭 (http://localhost:18888/lab)
- [ ] 노트북 생성/실행

---

## 🚨 시연 중 문제 발생 시

### 긴급 복구
```bash
cd /home/babelai/datastreams-work/datastreams/asan/infra
./emergency_fix.sh
```

### 개별 서비스 재시작

**API 서버 (CORS 에러, 500 에러):**
```bash
pkill -f "uvicorn main:app"
cd /home/babelai/datastreams-work/datastreams/asan/data_portal/src/api
source ../../venv/bin/activate
PYTHONPATH=/home/babelai/datastreams-work/datastreams/asan \
  python -m uvicorn main:app --host 0.0.0.0 --port 8000 &
```

**프론트엔드 (페이지 로딩 안됨):**
```bash
pkill -f "vite.*5173"
cd /home/babelai/datastreams-work/datastreams/asan/data_portal/src/portal
npm run dev -- --port 5173 &
```

**NER 서비스 (503 에러):**
```bash
# SSH 터널 재시작
pkill -f "ssh.*28100"
nohup ssh -o StrictHostKeyChecking=no -N -L 28100:localhost:8100 -p 20022 aigen@1.215.235.250 &
```

---

## 📊 시스템 상태 모니터링

### 실시간 로그 확인
```bash
# API 서버 로그
tail -f /tmp/api.log

# 프론트엔드 로그
tail -f /tmp/vite.log

# Watchdog 로그
tail -f /tmp/watchdog.log

# 긴급 알림 로그
tail -f /tmp/watchdog_alerts.log
```

### 서비스 상태 확인
```bash
# 전체 포트 확인
ss -tlnp | grep -E "8000|5173|28888|29001|28100|18888"

# 프로세스 확인
ps aux | grep -E "uvicorn|vite|ssh.*aigen"

# Docker 컨테이너
docker ps --format "table {{.Names}}\t{{.Status}}"
```

---

## 🔧 알려진 이슈 및 해결책

### ❌ CORS 에러
- **증상**: `No 'Access-Control-Allow-Origin' header`
- **해결**: API 서버 재시작 (위 참조)

### ❌ NER 서비스 503
- **증상**: `Service Unavailable`
- **해결**: SSH 터널 재시작 (위 참조)

### ❌ Superset 연결 실패
- **증상**: `Connection reset by peer`
- **해결**:
  ```bash
  docker restart superset-db
  sleep 5
  # API 서버 재시작
  ```

### ❌ JupyterLab 접속 안됨
- **증상**: `Connection refused`
- **해결**:
  ```bash
  docker restart asan-jupyterlab
  ```

---

## ✅ 시연 성공 포인트

1. **대시보드**: 실시간 통계, 깔끔한 차트
2. **CDW 연구**: AI 기반 SQL 자동 생성
3. **NER 분석**: 한/영 의료 용어 자동 인식
4. **BI**: Superset 통합 (차트 6개, 대시보드 1개, 데이터셋 27개)
5. **AI 환경**: JupyterLab 즉시 접근

---

## 📞 비상 연락망

문제 발생 시:
1. 먼저 `./emergency_fix.sh` 실행
2. 안되면 개별 서비스 재시작
3. 그래도 안되면... 침착하게 재부팅

**자신감 있게 시연하세요!** 💪
