#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  서울아산병원 IDP — 자동 DB 백업 스크립트
#  크론탭: 0 2 * * * /home/babelai/datastreams-work/datastreams/asan/infra/backup.sh
#  (매일 02:00 실행)
# ═══════════════════════════════════════════════════════════════════
set -euo pipefail

BACKUP_DIR="/home/babelai/datastreams-work/datastreams/asan/infra/backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RETENTION_DAYS=7
LOG_FILE="/tmp/idp_backup.log"

mkdir -p "$BACKUP_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

log "=== IDP DB Backup 시작 ==="

# 1) OMOP CDM (postgres:13, port 5436)
OMOP_FILE="$BACKUP_DIR/omop_cdm_${TIMESTAMP}.sql.gz"
log "OMOP CDM 백업 시작..."
if docker exec infra-omop-db-1 pg_dump -U omopuser -d omop_cdm --no-owner --no-acl \
    | gzip > "$OMOP_FILE" 2>>"$LOG_FILE"; then
    SIZE=$(du -h "$OMOP_FILE" | cut -f1)
    log "OMOP CDM 백업 완료: $OMOP_FILE ($SIZE)"
else
    log "ERROR: OMOP CDM 백업 실패"
    rm -f "$OMOP_FILE"
fi

# 2) Main Postgres (postgres:16, port 15432 — Airflow, MLflow 등)
MAIN_FILE="$BACKUP_DIR/main_db_${TIMESTAMP}.sql.gz"
log "Main DB 백업 시작..."
if docker exec asan-postgres pg_dumpall -U asan \
    | gzip > "$MAIN_FILE" 2>>"$LOG_FILE"; then
    SIZE=$(du -h "$MAIN_FILE" | cut -f1)
    log "Main DB 백업 완료: $MAIN_FILE ($SIZE)"
else
    log "ERROR: Main DB 백업 실패"
    rm -f "$MAIN_FILE"
fi

# 3) Superset DB (postgres:15, port 15432)
SUPERSET_FILE="$BACKUP_DIR/superset_db_${TIMESTAMP}.sql.gz"
log "Superset DB 백업 시작..."
if docker exec superset-db pg_dump -U superset -d superset --no-owner --no-acl \
    | gzip > "$SUPERSET_FILE" 2>>"$LOG_FILE"; then
    SIZE=$(du -h "$SUPERSET_FILE" | cut -f1)
    log "Superset DB 백업 완료: $SUPERSET_FILE ($SIZE)"
else
    log "ERROR: Superset DB 백업 실패"
    rm -f "$SUPERSET_FILE"
fi

# 4) Qdrant 스냅샷
QDRANT_FILE="$BACKUP_DIR/qdrant_snapshot_${TIMESTAMP}.tar.gz"
log "Qdrant 스냅샷 시작..."
if tar czf "$QDRANT_FILE" -C /home/babelai/datastreams-work/datastreams/asan/infra/data qdrant_storage 2>>"$LOG_FILE"; then
    SIZE=$(du -h "$QDRANT_FILE" | cut -f1)
    log "Qdrant 스냅샷 완료: $QDRANT_FILE ($SIZE)"
else
    log "ERROR: Qdrant 스냅샷 실패"
    rm -f "$QDRANT_FILE"
fi

# 5) 오래된 백업 삭제
log "오래된 백업 정리 (${RETENTION_DAYS}일 이상)..."
DELETED=$(find "$BACKUP_DIR" -name "*.gz" -mtime +"$RETENTION_DAYS" -delete -print | wc -l)
log "삭제된 파일: ${DELETED}개"

# 6) 요약
TOTAL_SIZE=$(du -sh "$BACKUP_DIR" | cut -f1)
FILE_COUNT=$(find "$BACKUP_DIR" -name "*.gz" | wc -l)
log "=== 백업 완료 — ${FILE_COUNT}개 파일, 총 ${TOTAL_SIZE} ==="
