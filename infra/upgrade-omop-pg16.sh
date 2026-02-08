#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  OMOP CDM PostgreSQL 13 → 16 업그레이드 스크립트
#  ⚠️ 주의: 서비스 중단 필요 — 수동 실행 전용
#
#  순서:
#    1) 기존 DB dump
#    2) PG13 컨테이너 중단
#    3) PG16 컨테이너 시작 (새 볼륨)
#    4) dump restore
#    5) 검증
#
#  사용법: bash upgrade-omop-pg16.sh
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKUP_DIR="$SCRIPT_DIR/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DUMP_FILE="$BACKUP_DIR/omop_cdm_pre_upgrade_${TIMESTAMP}.sql"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

log "=== OMOP CDM PostgreSQL 13→16 업그레이드 ==="

# 1) 사전 검증
log "현재 DB 버전 확인..."
docker exec infra-omop-db-1 psql -U omopuser -d omop_cdm -c "SELECT version();" -t

read -p "업그레이드를 진행합니까? (y/N): " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    log "취소됨."
    exit 0
fi

# 2) Dump
mkdir -p "$BACKUP_DIR"
log "DB 덤프 시작 (약 5~10분 소요)..."
docker exec infra-omop-db-1 pg_dump -U omopuser -d omop_cdm --no-owner --no-acl -Fc \
    > "$BACKUP_DIR/omop_cdm_pre_upgrade_${TIMESTAMP}.dump"
DUMP_SIZE=$(du -h "$BACKUP_DIR/omop_cdm_pre_upgrade_${TIMESTAMP}.dump" | cut -f1)
log "덤프 완료: $DUMP_SIZE"

# 3) PG13 중단
log "PG13 컨테이너 중단..."
cd "$SCRIPT_DIR"
docker compose -f docker-compose-omop.yml down

# 4) 볼륨 백업
log "기존 볼륨 백업 (omop_data → omop_data_pg13_backup)..."
docker volume create omop_data_pg13_backup 2>/dev/null || true
docker run --rm \
    -v infra_omop_data:/source:ro \
    -v omop_data_pg13_backup:/dest \
    alpine sh -c "cp -a /source/. /dest/"

# 5) 기존 볼륨 삭제 후 PG16으로 시작
log "기존 볼륨 삭제..."
docker volume rm infra_omop_data 2>/dev/null || true

# docker-compose-omop.yml을 PG16으로 변경
log "compose 파일을 PG16으로 업데이트..."
sed -i 's/image: postgres:13/image: postgres:16-alpine/' docker-compose-omop.yml

log "PG16 컨테이너 시작..."
docker compose -f docker-compose-omop.yml up -d
sleep 10

# DB 준비 대기
for i in $(seq 1 30); do
    if docker exec infra-omop-db-1 pg_isready -U omopuser -d omop_cdm 2>/dev/null; then
        break
    fi
    log "DB 대기 중... ($i/30)"
    sleep 2
done

# 6) Restore
log "DB 복원 시작 (약 10~20분 소요)..."
docker exec -i infra-omop-db-1 pg_restore -U omopuser -d omop_cdm --no-owner --no-acl \
    < "$BACKUP_DIR/omop_cdm_pre_upgrade_${TIMESTAMP}.dump"
log "복원 완료!"

# 7) ANALYZE
log "ANALYZE 실행..."
docker exec infra-omop-db-1 psql -U omopuser -d omop_cdm -c "ANALYZE;"

# 8) 검증
log "업그레이드 후 검증..."
docker exec infra-omop-db-1 psql -U omopuser -d omop_cdm -c "SELECT version();" -t
docker exec infra-omop-db-1 psql -U omopuser -d omop_cdm -c "
    SELECT relname AS table_name, n_live_tup AS row_count
    FROM pg_stat_user_tables WHERE schemaname = 'public' AND n_live_tup > 0
    ORDER BY n_live_tup DESC LIMIT 10;" -t

log "=== 업그레이드 완료! ==="
log "백업 볼륨: omop_data_pg13_backup (문제 시 복원용)"
log "덤프 파일: $BACKUP_DIR/omop_cdm_pre_upgrade_${TIMESTAMP}.dump"
