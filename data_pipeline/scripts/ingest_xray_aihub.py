#!/usr/bin/env python3
"""
AI Hub 주요질환 이미지 합성데이터(X-ray) 적재 스크립트
- /mnt/c/projects/ 하위 AI Hub 다운로드 폴더 스캔
- 5개 부위별 폴더 자동 감지: ChestPA, Mammography, PNS, FacialBone, Foot
- PNG → MinIO idp-documents 버킷에 업로드
- OMOP imaging_study 테이블에 메타데이터 INSERT
- progress JSON으로 재시작 가능
"""
import argparse
import io
import json
import logging
import os
import sys
import time
from pathlib import Path

import psycopg2
from minio import Minio
from minio.error import S3Error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── 부위 목록 ──
BODY_PARTS = ["ChestPA", "Mammography", "PNS", "FacialBone", "Foot"]

# ── MinIO 설정 ──
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:19000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "idp-documents"
S3_PREFIX = "medical-imaging/xray-synthetic"

# ── OMOP DB 설정 ──
DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "dbname": os.getenv("OMOP_DB_NAME", "omop_cdm"),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
}

# ── Progress 파일 ──
PROGRESS_FILE = Path(__file__).parent / ".xray_ingest_progress.json"


def _load_progress() -> set:
    """처리 완료된 파일 목록을 로드합니다."""
    if PROGRESS_FILE.exists():
        try:
            data = json.loads(PROGRESS_FILE.read_text())
            return set(data.get("completed_files", []))
        except Exception:
            pass
    return set()


def _save_progress(completed: set):
    """처리 완료된 파일 목록을 저장합니다."""
    PROGRESS_FILE.write_text(json.dumps(
        {"completed_files": sorted(completed)},
        ensure_ascii=False,
    ))


def _get_s3_client() -> Minio:
    return Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def _ensure_bucket(s3: Minio):
    if not s3.bucket_exists(MINIO_BUCKET):
        s3.make_bucket(MINIO_BUCKET)
        logger.info(f"Created bucket: {MINIO_BUCKET}")


def _ensure_imaging_table(conn):
    """imaging_study 테이블 및 합성 데이터용 컬럼이 존재하는지 확인합니다."""
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS imaging_study (
            imaging_study_id SERIAL PRIMARY KEY,
            modality VARCHAR(20),
            body_part VARCHAR(100),
            study_description VARCHAR(500),
            study_date DATE,
            s3_key VARCHAR(500),
            dicom_meta JSONB,
            image_filename VARCHAR(200),
            image_url VARCHAR(500)
        )
    """)
    # 합성 데이터셋 전용 컬럼 추가 (idempotent)
    for ddl in [
        "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS dataset VARCHAR(100)",
        "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS image_width INT",
        "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS image_height INT",
        "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT",
        "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
    ]:
        try:
            cur.execute(ddl)
        except Exception:
            conn.rollback()
            continue
    conn.commit()
    cur.close()


def _scan_images(source: Path) -> list[tuple[str, Path]]:
    """소스 디렉토리에서 부위별 PNG 파일을 스캔합니다.

    Returns:
        list of (body_part, file_path) tuples
    """
    results = []
    for bp in BODY_PARTS:
        bp_dir = source / bp
        if not bp_dir.is_dir():
            # 대소문자 다른 폴더도 찾기
            for d in source.iterdir():
                if d.is_dir() and d.name.lower() == bp.lower():
                    bp_dir = d
                    break
            else:
                continue
        for png in sorted(bp_dir.rglob("*.png")):
            results.append((bp, png))
        for png in sorted(bp_dir.rglob("*.PNG")):
            if (bp, png) not in results:
                results.append((bp, png))
        logger.info(f"  {bp}: {sum(1 for b, _ in results if b == bp)} files found")
    return results


def run_ingest(source: Path, batch_size: int = 100, limit: int = 0, reset: bool = False):
    """메인 ETL 실행 함수."""
    start_time = time.time()

    # S3 연결
    s3 = _get_s3_client()
    _ensure_bucket(s3)

    # DB 연결
    conn = psycopg2.connect(**DB_CONFIG)
    _ensure_imaging_table(conn)

    # Progress
    if reset:
        completed = set()
        if PROGRESS_FILE.exists():
            PROGRESS_FILE.unlink()
        logger.info("Reset mode: progress cleared")
    else:
        completed = _load_progress()
        if completed:
            logger.info(f"Resuming: {len(completed)} files already processed")

    # 이미지 스캔
    logger.info(f"Scanning source: {source}")
    all_images = _scan_images(source)
    total_found = len(all_images)
    logger.info(f"Total images found: {total_found}")

    # 이미 완료된 파일 필터링
    pending = [(bp, p) for bp, p in all_images if str(p) not in completed]
    logger.info(f"Pending: {len(pending)} (skipping {total_found - len(pending)} already done)")

    if limit > 0:
        pending = pending[:limit]
        logger.info(f"Limit applied: processing {len(pending)} files")

    if not pending:
        logger.info("Nothing to process. Done.")
        conn.close()
        return

    # 배치 처리
    cur = conn.cursor()
    total_uploaded = 0
    total_inserted = 0
    errors = []

    for i in range(0, len(pending), batch_size):
        batch = pending[i:i + batch_size]
        batch_start = time.time()

        for body_part, filepath in batch:
            rel_key = f"{S3_PREFIX}/{body_part}/{filepath.name}"
            try:
                # 파일 크기
                file_size = filepath.stat().st_size

                # S3 업로드
                with open(filepath, "rb") as f:
                    data = f.read()
                s3.put_object(
                    MINIO_BUCKET,
                    rel_key,
                    io.BytesIO(data),
                    len(data),
                    content_type="image/png",
                )
                total_uploaded += 1

                # DB INSERT
                cur.execute(
                    """INSERT INTO imaging_study
                       (modality, body_part, study_description, s3_key,
                        image_filename, image_url, dataset,
                        image_width, image_height, file_size_bytes)
                       VALUES ('CR', %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        body_part,
                        f"AI Hub 합성 X-ray — {body_part}",
                        rel_key,
                        filepath.name,
                        rel_key,
                        "aihub-xray-synthetic",
                        1024,  # AI Hub spec: 1024x1024
                        1024,
                        file_size,
                    ),
                )
                total_inserted += 1

                completed.add(str(filepath))

            except Exception as e:
                errors.append(f"{filepath}: {e}")
                logger.error(f"Error processing {filepath}: {e}")

        conn.commit()
        _save_progress(completed)

        batch_elapsed = time.time() - batch_start
        progress_pct = min(100, int((i + len(batch)) / len(pending) * 100))
        logger.info(
            f"[{progress_pct:3d}%] Batch {i // batch_size + 1}: "
            f"{len(batch)} files in {batch_elapsed:.1f}s | "
            f"uploaded={total_uploaded}, inserted={total_inserted}"
        )

    cur.close()
    conn.close()

    elapsed = time.time() - start_time
    logger.info(f"{'='*60}")
    logger.info(f"ETL completed in {elapsed:.1f}s")
    logger.info(f"  Uploaded to S3: {total_uploaded}")
    logger.info(f"  Inserted to DB: {total_inserted}")
    logger.info(f"  Errors: {len(errors)}")
    if errors:
        for e in errors[:10]:
            logger.error(f"  - {e}")

    return {
        "total_found": total_found,
        "uploaded": total_uploaded,
        "inserted": total_inserted,
        "errors": len(errors),
        "elapsed_seconds": round(elapsed, 1),
    }


def main():
    parser = argparse.ArgumentParser(description="AI Hub X-ray 합성데이터 적재")
    parser.add_argument(
        "--source",
        type=str,
        default="/mnt/c/projects",
        help="AI Hub 다운로드 폴더 경로 (default: /mnt/c/projects)",
    )
    parser.add_argument("--batch-size", type=int, default=100, help="배치 크기 (default: 100)")
    parser.add_argument("--limit", type=int, default=0, help="최대 처리 건수 (0=전체)")
    parser.add_argument("--reset", action="store_true", help="진행 상태 초기화 후 전체 재처리")
    args = parser.parse_args()

    source = Path(args.source)
    if not source.is_dir():
        logger.error(f"Source directory not found: {source}")
        sys.exit(1)

    result = run_ingest(
        source=source,
        batch_size=args.batch_size,
        limit=args.limit,
        reset=args.reset,
    )
    if result:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
