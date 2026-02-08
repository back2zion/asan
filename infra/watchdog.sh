#!/bin/bash
###############################################
# Service Watchdog - 강건한 서비스 자동 복구
# cron: */2 * * * * (2분마다 실행)
# 만료: 2026-02-12 자동 비활성화
###############################################

LOGFILE="/tmp/watchdog.log"
ALERT_LOG="/tmp/watchdog_alerts.log"
EXPIRE_DATE="2026-02-12"
MAX_RESTART_ATTEMPTS=3

# 만료 체크
if [[ "$(date +%Y-%m-%d)" > "$EXPIRE_DATE" ]]; then
    echo "$(date) [WATCHDOG] Expired. Removing cron job." >> "$LOGFILE"
    crontab -l 2>/dev/null | grep -v watchdog.sh | crontab -
    exit 0
fi

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [WATCHDOG] $1" >> "$LOGFILE"
}

alert() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [ALERT] $1" >> "$ALERT_LOG"
    log "ALERT: $1"
}

# 프로세스 재시작 카운터 파일
restart_count_file() {
    echo "/tmp/watchdog_restart_$1.count"
}

get_restart_count() {
    local service=$1
    local file=$(restart_count_file "$service")
    if [[ -f "$file" ]]; then
        cat "$file"
    else
        echo 0
    fi
}

increment_restart_count() {
    local service=$1
    local file=$(restart_count_file "$service")
    local count=$(get_restart_count "$service")
    echo $((count + 1)) > "$file"
}

reset_restart_count() {
    local service=$1
    local file=$(restart_count_file "$service")
    echo 0 > "$file"
}

# --- 1. SSH Tunnel: Qwen3 LLM (localhost:28888 -> GPU:8000) ---
if ! curl -s --max-time 3 http://localhost:28888/v1/models > /dev/null 2>&1; then
    log "LLM tunnel DOWN - restarting..."
    pkill -f "ssh.*28888.*aigen" 2>/dev/null
    sleep 1
    nohup ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ServerAliveCountMax=3 \
        -N -L 28888:localhost:8000 -p 20022 aigen@1.215.235.250 \
        >> /tmp/ssh_llm.log 2>&1 &
    log "LLM tunnel restarted (PID: $!)"
else
    log "LLM tunnel OK"
fi

# --- 2. SSH Tunnel: Paper2Slides (localhost:29001 -> GPU:9001) ---
if ! curl -s --max-time 3 http://localhost:29001/health > /dev/null 2>&1; then
    log "P2S tunnel DOWN - restarting..."
    pkill -f "ssh.*29001.*aigen" 2>/dev/null
    sleep 1
    nohup ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ServerAliveCountMax=3 \
        -N -L 29001:localhost:9001 -p 20022 aigen@1.215.235.250 \
        >> /tmp/ssh_p2s.log 2>&1 &
    log "P2S tunnel restarted (PID: $!)"
else
    log "P2S tunnel OK"
fi

# --- 3. SSH Tunnel: Medical NER (localhost:28100 -> GPU:8100) ---
if ! curl -s --max-time 3 http://localhost:28100/api/v1/ner/health > /dev/null 2>&1; then
    log "NER tunnel DOWN - restarting..."
    pkill -f "ssh.*28100.*aigen" 2>/dev/null
    sleep 1
    nohup ssh -o StrictHostKeyChecking=no -o ServerAliveInterval=60 -o ServerAliveCountMax=3 \
        -N -L 28100:localhost:8100 -p 20022 aigen@1.215.235.250 \
        >> /tmp/ssh_ner.log 2>&1 &
    log "NER tunnel restarted (PID: $!)"
else
    log "NER tunnel OK"
fi

# --- 4. JupyterLab (local Docker container on port 18888) ---
# Note: JupyterLab runs locally as 'asan-jupyterlab' Docker container, not via SSH tunnel
# Watchdog skips this service - managed by Docker restart policy

# --- 5. FastAPI Backend (port 8000) ---
if ! curl -s --max-time 5 http://localhost:8000/api/v1/health > /dev/null 2>&1; then
    count=$(get_restart_count "api")
    if [[ $count -ge $MAX_RESTART_ATTEMPTS ]]; then
        alert "API server 재시작 한계 초과 ($count회) - 수동 개입 필요!"
        log "API server restart limit reached - skipping"
    else
        log "API server DOWN - restarting (attempt $((count + 1))/$MAX_RESTART_ATTEMPTS)..."
        # 기존 프로세스 강제 종료
        pkill -9 -f "uvicorn main:app.*8000" 2>/dev/null
        sleep 3
        # 포트 확인
        if lsof -i :8000 > /dev/null 2>&1; then
            alert "Port 8000 still in use after kill!"
        fi
        # 재시작
        cd /home/babelai/datastreams-work/datastreams/asan/data_portal/src/api
        source /home/babelai/datastreams-work/datastreams/asan/venv/bin/activate
        PYTHONPATH=/home/babelai/datastreams-work/datastreams/asan \
            nohup python -m uvicorn main:app --host 0.0.0.0 --port 8000 \
            >> /tmp/api.log 2>&1 &
        log "API server restarted (PID: $!)"
        increment_restart_count "api"
    fi
else
    log "API server OK"
    reset_restart_count "api"
fi

# --- 6. Vite Frontend (port 5173) ---
if ! curl -s --max-time 5 http://localhost:5173 > /dev/null 2>&1; then
    log "Vite dev server DOWN - restarting..."
    pkill -f "vite.*5173" 2>/dev/null
    sleep 2
    cd /home/babelai/datastreams-work/datastreams/asan/data_portal/src/portal
    nohup npm run dev -- --port 5173 >> /tmp/vite.log 2>&1 &
    log "Vite dev server restarted (PID: $!)"
else
    log "Vite dev server OK"
fi
