# ============================================================
# 서울아산병원 IDP — 통합 Makefile
# 사용법: make help
# ============================================================

.PHONY: help dev staging up down restart status health logs \
        backup restore tunnels db-shell api frontend \
        build clean monitoring monitoring-down \
        env-check env-diff

# ── 기본 설정 ──
SHELL         := /bin/bash
PROJECT_ROOT  := $(shell pwd)
INFRA_DIR     := $(PROJECT_ROOT)/infra
API_DIR       := $(PROJECT_ROOT)/data_portal/src/api
PORTAL_DIR    := $(PROJECT_ROOT)/data_portal/src/portal
VENV          := $(PROJECT_ROOT)/venv/bin/activate

# 환경 (ENV=dev|staging|prod, 기본 dev)
ENV           ?= dev
ENV_FILE      := $(INFRA_DIR)/.env.$(ENV)

# Docker Compose 파일들
DC            := docker compose -f $(INFRA_DIR)/docker-compose.yml --env-file $(ENV_FILE)
DC_OMOP       := docker compose -f $(INFRA_DIR)/docker-compose-omop.yml
DC_MON        := docker compose -f $(INFRA_DIR)/docker-compose-monitoring.yml
DC_NGINX      := docker compose -f $(INFRA_DIR)/docker-compose-nginx.yml

# 색상
GREEN         := \033[0;32m
YELLOW        := \033[1;33m
RED           := \033[0;31m
BLUE          := \033[0;34m
NC            := \033[0m

# ============================================================
# 도움말
# ============================================================

help: ## 사용 가능한 명령어 목록
	@echo ""
	@echo -e "$(BLUE)서울아산병원 IDP — Makefile 명령어$(NC)"
	@echo -e "$(BLUE)============================================$(NC)"
	@echo ""
	@echo -e "$(YELLOW)환경 선택:$(NC) ENV=dev|staging|prod (기본: dev)"
	@echo -e "  예: make up ENV=staging"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""

# ============================================================
# 환경 관리
# ============================================================

env-check: ## 현재 환경 설정 확인
	@echo -e "$(BLUE)현재 환경: $(ENV)$(NC)"
	@if [ -f "$(ENV_FILE)" ]; then \
		echo -e "$(GREEN)환경 파일: $(ENV_FILE) (존재)$(NC)"; \
		echo "---"; \
		grep -E '^(ENVIRONMENT|LOG_LEVEL|DEBUG|AUTH_REQUIRED|API_WORKERS)=' "$(ENV_FILE)" 2>/dev/null || true; \
	else \
		echo -e "$(RED)환경 파일 없음: $(ENV_FILE)$(NC)"; \
		echo "  cp $(INFRA_DIR)/.env.example $(ENV_FILE)"; \
	fi

env-diff: ## dev vs staging 설정 차이 비교
	@echo -e "$(BLUE)환경 설정 차이 (dev vs staging)$(NC)"
	@diff --color=always \
		<(grep -v '^#' $(INFRA_DIR)/.env.dev | grep -v '^$$' | sort) \
		<(grep -v '^#' $(INFRA_DIR)/.env.staging | grep -v '^$$' | sort) \
		|| true

# ============================================================
# 개발 모드 (venv 직접 실행, Docker 없이)
# ============================================================

dev: env-check ## [개발] API + Frontend 직접 실행 (venv)
	@echo -e "$(YELLOW)개발 모드 시작 (ENV=$(ENV))...$(NC)"
	@# API 서버
	@if ! curl -s --max-time 2 http://localhost:8000/api/v1/health > /dev/null 2>&1; then \
		echo "  API 서버 시작..."; \
		cd $(API_DIR) && \
		source $(VENV) && \
		set -a && source $(ENV_FILE) && set +a && \
		PYTHONPATH=$(PROJECT_ROOT) \
			nohup python -m uvicorn main:app --host 0.0.0.0 --port 8000 \
			>> /tmp/api.log 2>&1 & \
		sleep 3; \
	fi
	@# Frontend
	@if ! curl -s --max-time 2 http://localhost:5173 > /dev/null 2>&1; then \
		echo "  프론트엔드 시작..."; \
		cd $(PORTAL_DIR) && \
		nohup npm run dev -- --port 5173 >> /tmp/vite.log 2>&1 & \
		sleep 3; \
	fi
	@echo -e "$(GREEN)개발 서버 시작 완료$(NC)"
	@echo -e "  포털: $(BLUE)http://localhost:5173$(NC)"
	@echo -e "  API:  $(BLUE)http://localhost:8000/api/docs$(NC)"

dev-stop: ## [개발] API + Frontend 중지
	@echo "개발 서버 중지..."
	@pkill -f "uvicorn main:app.*8000" 2>/dev/null || true
	@pkill -f "vite.*5173" 2>/dev/null || true
	@echo -e "$(GREEN)중지 완료$(NC)"

# ============================================================
# Docker Compose 통합 (staging/prod)
# ============================================================

up: env-check ## Docker 서비스 전체 시작
	@echo -e "$(YELLOW)Docker 서비스 시작 (ENV=$(ENV))...$(NC)"
	$(DC) up -d
	@echo -e "$(GREEN)서비스 시작 완료$(NC)"

down: ## Docker 서비스 전체 중지
	@echo "Docker 서비스 중지..."
	$(DC) down
	@echo -e "$(GREEN)중지 완료$(NC)"

restart: down up ## Docker 서비스 재시작

build: ## Docker 이미지 빌드
	@echo "Docker 이미지 빌드 (ENV=$(ENV))..."
	$(DC) build --no-cache

ps: ## Docker 컨테이너 상태
	@echo -e "$(BLUE)=== Main Services ===$(NC)"
	@$(DC) ps 2>/dev/null || true
	@echo ""
	@echo -e "$(BLUE)=== OMOP DB ===$(NC)"
	@$(DC_OMOP) ps 2>/dev/null || true

# ============================================================
# OMOP CDM DB
# ============================================================

omop-up: ## OMOP CDM DB 시작
	$(DC_OMOP) up -d
	@echo -e "$(GREEN)OMOP DB 시작 (port 5436)$(NC)"

omop-down: ## OMOP CDM DB 중지
	$(DC_OMOP) down

db-shell: ## OMOP DB 쉘 접속
	@docker exec -it infra-omop-db-1 psql -U omopuser -d omop_cdm

# ============================================================
# 모니터링 스택
# ============================================================

monitoring: ## 모니터링 스택 시작 (Prometheus + Grafana)
	$(DC_MON) up -d
	@echo -e "$(GREEN)모니터링 시작$(NC)"
	@echo -e "  Prometheus: $(BLUE)http://localhost:19090$(NC)"
	@echo -e "  Grafana:    $(BLUE)http://localhost:13000$(NC)"

monitoring-down: ## 모니터링 스택 중지
	$(DC_MON) down

# ============================================================
# Nginx 리버스 프록시
# ============================================================

proxy: ## Nginx 리버스 프록시 시작
	$(DC_NGINX) up -d
	@echo -e "$(GREEN)Nginx 프록시 시작 (443, 18081, 18082)$(NC)"

proxy-down: ## Nginx 리버스 프록시 중지
	$(DC_NGINX) down

# ============================================================
# SSH 터널 & Watchdog
# ============================================================

tunnels: ## SSH 터널 복구 (watchdog 실행)
	@echo "SSH 터널 체크..."
	@bash $(INFRA_DIR)/watchdog.sh
	@echo -e "$(GREEN)터널 체크 완료$(NC)"

# ============================================================
# 헬스체크
# ============================================================

health: ## 전체 시스템 헬스체크
	@echo -e "$(BLUE)시스템 헬스체크$(NC)"
	@echo -e "$(BLUE)============================================$(NC)"
	@# API
	@printf "  %-25s" "API Server (8000)"
	@curl -sf --max-time 3 http://localhost:8000/api/v1/health > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"
	@# Frontend
	@printf "  %-25s" "Frontend (5173)"
	@curl -sf --max-time 3 http://localhost:5173 > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"
	@# OMOP DB
	@printf "  %-25s" "OMOP DB (5436)"
	@docker exec infra-omop-db-1 pg_isready -U omopuser -d omop_cdm > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"
	@# Redis
	@printf "  %-25s" "Redis (16379)"
	@docker exec asan-redis redis-cli ping > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"
	@# Milvus
	@printf "  %-25s" "Milvus (19530)"
	@curl -sf --max-time 3 http://localhost:9091/healthz > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"
	@# LLM Tunnel
	@printf "  %-25s" "LLM Tunnel (28888)"
	@curl -sf --max-time 3 http://localhost:28888/v1/models > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"
	@# NER Tunnel
	@printf "  %-25s" "NER Tunnel (28100)"
	@curl -sf --max-time 3 http://localhost:28100/ner/health > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"
	@# Neo4j
	@printf "  %-25s" "Neo4j (7474)"
	@curl -sf --max-time 3 http://localhost:7474 > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"
	@# Prometheus
	@printf "  %-25s" "Prometheus (19090)"
	@curl -sf --max-time 3 http://localhost:19090/-/healthy > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"
	@# Grafana
	@printf "  %-25s" "Grafana (13000)"
	@curl -sf --max-time 3 http://localhost:13000/api/health > /dev/null 2>&1 \
		&& echo -e "$(GREEN)OK$(NC)" || echo -e "$(RED)DOWN$(NC)"

# ============================================================
# 로그
# ============================================================

logs: ## Docker 서비스 로그 (tail -f)
	$(DC) logs -f --tail=50

logs-api: ## API 서버 로그
	@tail -100f /tmp/api.log

logs-vite: ## Frontend 로그
	@tail -100f /tmp/vite.log

# ============================================================
# 백업 & 복구
# ============================================================

backup: ## DB 전체 백업 실행
	@echo "백업 시작..."
	@bash $(INFRA_DIR)/backup.sh
	@echo -e "$(GREEN)백업 완료$(NC)"

backup-list: ## 백업 파일 목록
	@echo -e "$(BLUE)백업 파일 목록$(NC)"
	@ls -lh $(INFRA_DIR)/backups/*.gz 2>/dev/null || echo "  백업 파일 없음"

# ============================================================
# 전체 시작/중지
# ============================================================

all-up: omop-up up monitoring proxy tunnels ## 전체 인프라 시작 (DB + Docker + 모니터링 + 프록시 + 터널)
	@sleep 5
	@$(MAKE) health
	@echo ""
	@echo -e "$(GREEN)============================================$(NC)"
	@echo -e "$(GREEN)  전체 시스템 시작 완료 (ENV=$(ENV))$(NC)"
	@echo -e "$(GREEN)============================================$(NC)"

all-down: proxy-down monitoring-down down omop-down ## 전체 인프라 중지
	@echo -e "$(GREEN)전체 시스템 중지 완료$(NC)"

# ============================================================
# 긴급 복구
# ============================================================

emergency: ## 긴급 복구 (API + Frontend 강제 재시작)
	@bash $(INFRA_DIR)/emergency_fix.sh

# ============================================================
# Ansible 프로비저닝
# ============================================================

provision: ## Ansible 프로비저닝 실행
	@echo -e "$(YELLOW)Ansible 프로비저닝 (ENV=$(ENV))...$(NC)"
	cd $(INFRA_DIR)/ansible && \
		ansible-playbook -i inventory/local.ini \
		playbook.yml \
		-e "env=$(ENV)" \
		-e "project_root=$(PROJECT_ROOT)"

provision-check: ## Ansible 프로비저닝 (dry-run)
	cd $(INFRA_DIR)/ansible && \
		ansible-playbook -i inventory/local.ini \
		playbook.yml \
		-e "env=$(ENV)" \
		-e "project_root=$(PROJECT_ROOT)" \
		--check --diff
