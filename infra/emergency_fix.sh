#!/bin/bash
###############################################
# 긴급 복구 스크립트 - 시연 중 문제 발생 시
# 사용법: ./emergency_fix.sh
###############################################

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}=== 긴급 복구 시작 ===${NC}"

# 1. 모든 프로세스 종료
echo "1. 기존 프로세스 정리..."
pkill -9 -f "uvicorn main:app.*8000" 2>/dev/null || true
pkill -9 -f "vite.*5173" 2>/dev/null || true
sleep 2

# 2. 포트 확인
echo "2. 포트 상태 확인..."
if lsof -i :8000 > /dev/null 2>&1; then
    echo -e "${RED}Port 8000 still occupied!${NC}"
    lsof -i :8000 -t | xargs -r kill -9
fi

if lsof -i :5173 > /dev/null 2>&1; then
    echo -e "${RED}Port 5173 still occupied!${NC}"
    lsof -i :5173 -t | xargs -r kill -9
fi

sleep 2

# 3. API 서버 재시작
echo "3. API 서버 재시작..."
cd /home/babelai/datastreams-work/datastreams/asan/data_portal/src/api
source /home/babelai/datastreams-work/datastreams/asan/venv/bin/activate
PYTHONPATH=/home/babelai/datastreams-work/datastreams/asan \
    python -m uvicorn main:app --host 0.0.0.0 --port 8000 \
    >> /tmp/api_emergency.log 2>&1 &

# 4. 프론트엔드 재시작
echo "4. 프론트엔드 재시작..."
cd /home/babelai/datastreams-work/datastreams/asan/data_portal/src/portal
npm run dev -- --port 5173 >> /tmp/vite_emergency.log 2>&1 &

# 5. 대기
echo "5. 서비스 시작 대기 중..."
sleep 10

# 6. 확인
echo "6. 헬스체크..."
if curl -s http://localhost:8000/api/v1/health > /dev/null 2>&1; then
    echo -e "${GREEN}✓ API 서버 정상${NC}"
else
    echo -e "${RED}✗ API 서버 실패${NC}"
    tail -20 /tmp/api_emergency.log
fi

if curl -s http://localhost:5173 > /dev/null 2>&1; then
    echo -e "${GREEN}✓ 프론트엔드 정상${NC}"
else
    echo -e "${RED}✗ 프론트엔드 실패${NC}"
    tail -20 /tmp/vite_emergency.log
fi

echo -e "${GREEN}=== 복구 완료 ===${NC}"
