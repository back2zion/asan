#!/bin/bash
###############################################
# 시연용 전체 시스템 시작 스크립트
# 사용법: ./demo_start.sh
###############################################

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}  서울아산병원 IDP 시스템 시작${NC}"
echo -e "${BLUE}================================================${NC}"
echo ""

# 1. SSH Tunnels
echo -e "${YELLOW}[1/6] SSH 터널 확인 중...${NC}"
/home/babelai/datastreams-work/datastreams/asan/infra/watchdog.sh
sleep 2

# 2. Docker 컨테이너
echo -e "${YELLOW}[2/6] Docker 컨테이너 확인 중...${NC}"
for container in infra-omop-db-1 superset-db asan-jupyterlab; do
    if ! docker ps | grep -q "$container"; then
        echo -e "  ${RED}✗${NC} $container 시작 중..."
        docker start "$container" 2>/dev/null || echo "  Already running or not found"
    else
        echo -e "  ${GREEN}✓${NC} $container"
    fi
done

# 3. API Server
echo -e "${YELLOW}[3/6] API 서버 확인 중...${NC}"
if ! curl -s --max-time 3 http://localhost:8000/api/v1/health > /dev/null 2>&1; then
    echo "  API 서버 시작 중..."
    cd /home/babelai/datastreams-work/datastreams/asan/data_portal/src/api
    source /home/babelai/datastreams-work/datastreams/asan/venv/bin/activate
    pkill -f "uvicorn main:app.*8000" 2>/dev/null || true
    sleep 2
    PYTHONPATH=/home/babelai/datastreams-work/datastreams/asan \
        nohup python -m uvicorn main:app --host 0.0.0.0 --port 8000 \
        >> /tmp/api.log 2>&1 &
    sleep 5
    if curl -s --max-time 3 http://localhost:8000/api/v1/health > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} API 서버 실행 중"
    else
        echo -e "  ${RED}✗${NC} API 서버 시작 실패!"
        exit 1
    fi
else
    echo -e "  ${GREEN}✓${NC} API 서버 실행 중"
fi

# 4. Frontend
echo -e "${YELLOW}[4/6] 프론트엔드 확인 중...${NC}"
if ! curl -s --max-time 3 http://localhost:5173 > /dev/null 2>&1; then
    echo "  프론트엔드 시작 중..."
    cd /home/babelai/datastreams-work/datastreams/asan/data_portal/src/portal
    pkill -f "vite.*5173" 2>/dev/null || true
    sleep 2
    nohup npm run dev -- --port 5173 >> /tmp/vite.log 2>&1 &
    sleep 5
    if curl -s --max-time 3 http://localhost:5173 > /dev/null 2>&1; then
        echo -e "  ${GREEN}✓${NC} 프론트엔드 실행 중"
    else
        echo -e "  ${RED}✗${NC} 프론트엔드 시작 실패!"
        exit 1
    fi
else
    echo -e "  ${GREEN}✓${NC} 프론트엔드 실행 중"
fi

# 5. 헬스체크
echo -e "${YELLOW}[5/6] 전체 시스템 헬스체크...${NC}"
/tmp/service_check.sh

# 6. 최종 확인
echo ""
echo -e "${YELLOW}[6/6] 최종 확인 중...${NC}"
sleep 3

HEALTHY=true

# API 엔드포인트 테스트
if ! curl -s http://localhost:8000/api/v1/health > /dev/null 2>&1; then
    echo -e "${RED}✗ API 서버 응답 없음${NC}"
    HEALTHY=false
fi

# CORS 테스트
if ! curl -s -I -X OPTIONS http://localhost:8000/api/v1/health -H "Origin: http://localhost:5173" 2>&1 | grep -q "access-control-allow-origin"; then
    echo -e "${RED}✗ CORS 설정 문제${NC}"
    HEALTHY=false
fi

# NER 서비스
if ! curl -s http://localhost:8000/api/v1/ner/health > /dev/null 2>&1; then
    echo -e "${RED}✗ NER 서비스 응답 없음${NC}"
    HEALTHY=false
fi

# Superset
if ! curl -s http://localhost:8000/api/v1/superset/stats > /dev/null 2>&1; then
    echo -e "${RED}✗ Superset 통계 조회 실패${NC}"
    HEALTHY=false
fi

echo ""
if [ "$HEALTHY" = true ]; then
    echo -e "${GREEN}================================================${NC}"
    echo -e "${GREEN}  ✓ 시스템 준비 완료!${NC}"
    echo -e "${GREEN}================================================${NC}"
    echo ""
    echo -e "  📊 대시보드: ${BLUE}http://localhost:5173${NC}"
    echo -e "  🔬 CDW 연구: ${BLUE}http://localhost:5173/cdw${NC}"
    echo -e "  🧬 NER 분석: ${BLUE}http://localhost:5173/ner${NC}"
    echo -e "  📈 BI 대시보드: ${BLUE}http://localhost:5173/bi${NC}"
    echo -e "  📚 API 문서: ${BLUE}http://localhost:8000/api/docs${NC}"
    echo ""
else
    echo -e "${RED}================================================${NC}"
    echo -e "${RED}  ✗ 시스템에 문제가 있습니다!${NC}"
    echo -e "${RED}================================================${NC}"
    echo ""
    echo "로그 확인:"
    echo "  tail -50 /tmp/api.log"
    echo "  tail -50 /tmp/vite.log"
    exit 1
fi
