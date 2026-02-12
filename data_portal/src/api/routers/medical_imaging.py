"""
의료영상 데이터셋 브라우저 API
- AI Hub X-ray 합성데이터 등록/조회/통계
- MinIO에서 이미지 서빙 및 썸네일 생성
"""
import io
import logging

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from services.redis_cache import cache_get, cache_set

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/medical-imaging", tags=["MedicalImaging"])

# ── 테이블 확인 플래그 ──
_tbl_ok = False


async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()


async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)


async def _ensure_table():
    global _tbl_ok
    if _tbl_ok:
        return
    conn = await _get_conn()
    try:
        await conn.execute("""
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
        for ddl in [
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS dataset VARCHAR(100)",
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS image_width INT",
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS image_height INT",
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT",
            "ALTER TABLE imaging_study ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()",
        ]:
            try:
                await conn.execute(ddl)
            except Exception:
                pass
        _tbl_ok = True
    finally:
        await _rel(conn)


# ── 1. 데이터셋 목록 ──

DATASETS = [
    {
        "dataset_id": "xray-synthetic",
        "name": "AI Hub 주요질환 이미지 합성데이터 (X-ray)",
        "source": "AI Hub",
        "modality": "CR",
        "body_parts": ["ChestPA", "Mammography", "PNS", "FacialBone", "Foot"],
        "resolution": "1024x1024",
        "format": "PNG",
        "description": "AI Hub 제공 주요질환 X-ray 합성 영상 데이터셋 (105,000장, ~38GB)",
    },
]

# ── MinIO 기반 영상 데이터셋 통계 ──
_MINIO_BUCKET = "medical-images"
_BODY_PART_INFO = [
    {"key": "ChestPA", "label": "ChestPA (흉부)", "prefixes": ["aihub-xray/training/images/ChestPA_"], "source": "AI Hub", "modality": "X선"},
    {"key": "Mammography", "label": "Mammography (유방)", "prefixes": ["aihub-xray/training/images/Mammography_"], "source": "AI Hub", "modality": "유방촬영"},
    {"key": "PNS", "label": "PNS (부비동)", "prefixes": ["aihub-xray/training/images/PNS_"], "source": "AI Hub", "modality": "X선"},
    {"key": "FacialBone", "label": "Facial bone (안면골)", "prefixes": ["aihub-xray/training/images/Facial_bone_"], "source": "AI Hub", "modality": "X선"},
    {"key": "Foot", "label": "Foot (족부)", "prefixes": ["aihub-xray/training/images/Foot_"], "source": "AI Hub", "modality": "X선"},
    {"key": "NIH", "label": "NIH Chest X-ray", "prefixes": ["nih-chest-xray/images/"], "source": "NIH", "modality": "X선"},
]
_LABEL_PREFIXES = [
    {"key": "ChestPA", "prefixes": ["aihub-xray/training/labels/ChestPA_"]},
    {"key": "Mammography", "prefixes": ["aihub-xray/training/labels/Mammography_"]},
    {"key": "PNS", "prefixes": ["aihub-xray/training/labels/PNS_"]},
    {"key": "FacialBone", "prefixes": ["aihub-xray/training/labels/Facial_bone_"]},
    {"key": "Foot", "prefixes": ["aihub-xray/training/labels/Foot_"]},
]

# 캐시 (MinIO list는 느림)
import time as _time
_stats_cache: dict = {}
_STATS_CACHE_TTL = 300  # 5분


def _get_minio():
    from minio import Minio
    return Minio("localhost:19000", access_key="minioadmin", secret_key="minioadmin", secure=False)


def _count_objects(client, bucket: str, prefix: str) -> int:
    return sum(1 for _ in client.list_objects(bucket, prefix=prefix, recursive=True))


_REDIS_IMAGING_KEY = "idp:imaging_minio_stats"
_REDIS_IMAGING_TTL = 1800  # 30분 (MinIO 데이터는 잘 안 변함)


@router.get("/minio-stats")
async def minio_image_stats():
    """MinIO medical-images 버킷의 부위별 이미지/라벨 통계"""
    import asyncio

    now = _time.time()
    # 1) 메모리 캐시
    if _stats_cache.get("data") and now - _stats_cache.get("ts", 0) < _STATS_CACHE_TTL:
        return _stats_cache["data"]

    # 2) Redis 캐시
    redis_data = await cache_get(_REDIS_IMAGING_KEY)
    if redis_data:
        _stats_cache["data"] = redis_data
        _stats_cache["ts"] = now
        return redis_data

    # 3) MinIO 직접 조회
    def _collect():
        client = _get_minio()
        if not client.bucket_exists(_MINIO_BUCKET):
            return {"error": "medical-images 버킷이 없습니다", "datasets": []}

        datasets = []
        total_images = 0
        total_labels = 0

        for bp in _BODY_PART_INFO:
            img_count = sum(_count_objects(client, _MINIO_BUCKET, p) for p in bp["prefixes"])
            # 라벨 수
            lbl_count = 0
            lbl_info = next((l for l in _LABEL_PREFIXES if l["key"] == bp["key"]), None)
            if lbl_info:
                lbl_count = sum(_count_objects(client, _MINIO_BUCKET, p) for p in lbl_info["prefixes"])

            # NIH는 CSV 메타데이터가 라벨 역할
            label_desc = ""
            if bp["key"] == "NIH":
                label_desc = "CSV 2건 (14질환 multi-label + BBox)"
            elif lbl_count > 0:
                label_desc = f"JSON {lbl_count:,}건"

            datasets.append({
                "key": bp["key"],
                "label": bp["label"],
                "source": bp["source"],
                "modality": bp["modality"],
                "image_count": img_count,
                "label_count": lbl_count,
                "label_desc": label_desc,
                "status": "uploaded" if img_count > 0 else "pending",
            })
            total_images += img_count
            total_labels += lbl_count

        return {
            "bucket": _MINIO_BUCKET,
            "total_images": total_images,
            "total_labels": total_labels,
            "total_files": total_images + total_labels,
            "datasets": datasets,
        }

    result = await asyncio.to_thread(_collect)
    _stats_cache["data"] = result
    _stats_cache["ts"] = now
    await cache_set(_REDIS_IMAGING_KEY, result, _REDIS_IMAGING_TTL)
    return result


@router.get("/datasets")
async def list_datasets():
    """등록된 의료영상 데이터셋 목록"""
    await _ensure_table()
    conn = await _get_conn()
    try:
        # 등록된 이미지 수 조회
        cnt = await conn.fetchval(
            "SELECT COUNT(*) FROM imaging_study WHERE dataset = 'aihub-xray-synthetic'"
        ) or 0
        result = []
        for ds in DATASETS:
            result.append({**ds, "image_count": cnt})
        return result
    except Exception:
        return [{**ds, "image_count": 0} for ds in DATASETS]
    finally:
        await _rel(conn)


# ── 2. 이미지 목록 ──

@router.get("/datasets/{dataset_id}/images")
async def list_images(
    dataset_id: str,
    body_part: str | None = Query(None, description="부위 필터"),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
):
    """데이터셋 내 이미지 목록 (페이지네이션)"""
    if dataset_id != "xray-synthetic":
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    await _ensure_table()
    conn = await _get_conn()
    try:
        offset = (page - 1) * size
        base_where = "WHERE dataset = 'aihub-xray-synthetic'"
        params: list = []

        if body_part:
            base_where += " AND body_part = $1"
            params.append(body_part)

        # 총 건수
        total = await conn.fetchval(
            f"SELECT COUNT(*) FROM imaging_study {base_where}", *params
        ) or 0

        # 이미지 목록
        idx = len(params) + 1
        rows = await conn.fetch(
            f"""SELECT imaging_study_id, body_part, modality, image_filename,
                       s3_key, image_width, image_height, file_size_bytes, created_at
                FROM imaging_study {base_where}
                ORDER BY imaging_study_id
                LIMIT ${idx} OFFSET ${idx + 1}""",
            *params, size, offset,
        )

        items = [
            {
                "imaging_study_id": r["imaging_study_id"],
                "body_part": r["body_part"],
                "modality": r["modality"],
                "filename": r["image_filename"],
                "s3_key": r["s3_key"],
                "width": r["image_width"],
                "height": r["image_height"],
                "file_size_bytes": r["file_size_bytes"],
                "thumbnail_url": f"/api/v1/medical-imaging/image/{r['imaging_study_id']}/thumbnail",
                "image_url": f"/api/v1/medical-imaging/image/{r['imaging_study_id']}",
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            }
            for r in rows
        ]

        return {
            "dataset_id": dataset_id,
            "body_part": body_part,
            "page": page,
            "size": size,
            "total": total,
            "total_pages": (total + size - 1) // size if total > 0 else 0,
            "items": items,
        }
    finally:
        await _rel(conn)


# ── 3. 부위별 통계 ──

@router.get("/datasets/{dataset_id}/stats")
async def dataset_stats(dataset_id: str):
    """데이터셋 부위별/modality별 통계"""
    if dataset_id != "xray-synthetic":
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_id}' not found")

    await _ensure_table()
    conn = await _get_conn()
    try:
        # 부위별 통계
        bp_rows = await conn.fetch(
            """SELECT body_part, COUNT(*) AS cnt,
                      SUM(file_size_bytes) AS total_bytes
               FROM imaging_study
               WHERE dataset = 'aihub-xray-synthetic'
               GROUP BY body_part
               ORDER BY cnt DESC"""
        )
        by_body_part = [
            {
                "body_part": r["body_part"],
                "count": r["cnt"],
                "total_size_mb": round((r["total_bytes"] or 0) / 1024 / 1024, 1),
            }
            for r in bp_rows
        ]

        # modality별 통계
        mod_rows = await conn.fetch(
            """SELECT modality, COUNT(*) AS cnt
               FROM imaging_study
               WHERE dataset = 'aihub-xray-synthetic'
               GROUP BY modality"""
        )
        by_modality = {r["modality"]: r["cnt"] for r in mod_rows}

        total = sum(r["cnt"] for r in bp_rows)
        total_size_mb = sum(r["total_size_mb"] for r in by_body_part)

        return {
            "dataset_id": dataset_id,
            "total_images": total,
            "total_size_mb": round(total_size_mb, 1),
            "by_body_part": by_body_part,
            "by_modality": by_modality,
        }
    finally:
        await _rel(conn)


# ── 4. 이미지 서빙 ──

async def _get_image_record(conn, imaging_study_id: int):
    row = await conn.fetchrow(
        "SELECT s3_key, image_filename FROM imaging_study WHERE imaging_study_id = $1",
        imaging_study_id,
    )
    if not row:
        raise HTTPException(status_code=404, detail=f"Image {imaging_study_id} not found")
    return row


def _get_s3():
    from services.s3_service import get_s3_client
    return get_s3_client()


@router.get("/image/{imaging_study_id}")
async def serve_image(imaging_study_id: int):
    """원본 이미지 서빙 (MinIO에서 읽어 반환)"""
    await _ensure_table()
    conn = await _get_conn()
    try:
        row = await _get_image_record(conn, imaging_study_id)
    finally:
        await _rel(conn)

    s3_key = row["s3_key"]
    try:
        s3 = _get_s3()
        response = s3.get_object("idp-documents", s3_key)
        data = response.read()
        response.close()
        response.release_conn()
        return StreamingResponse(
            io.BytesIO(data),
            media_type="image/png",
            headers={
                "Cache-Control": "public, max-age=86400",
                "Content-Disposition": f'inline; filename="{row["image_filename"]}"',
            },
        )
    except Exception as e:
        logger.error(f"S3 fetch failed for {s3_key}: {e}")
        raise HTTPException(status_code=502, detail="이미지를 가져올 수 없습니다")


@router.get("/image/{imaging_study_id}/thumbnail")
async def serve_thumbnail(imaging_study_id: int, width: int = Query(256, ge=64, le=512)):
    """썸네일 이미지 서빙 (리사이즈 반환)"""
    await _ensure_table()
    conn = await _get_conn()
    try:
        row = await _get_image_record(conn, imaging_study_id)
    finally:
        await _rel(conn)

    s3_key = row["s3_key"]
    try:
        s3 = _get_s3()
        response = s3.get_object("idp-documents", s3_key)
        data = response.read()
        response.close()
        response.release_conn()
    except Exception as e:
        logger.error(f"S3 fetch failed for {s3_key}: {e}")
        raise HTTPException(status_code=502, detail="이미지를 가져올 수 없습니다")

    try:
        from PIL import Image
        img = Image.open(io.BytesIO(data))
        img.thumbnail((width, width), Image.LANCZOS)
        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        buf.seek(0)
        return StreamingResponse(
            buf,
            media_type="image/png",
            headers={
                "Cache-Control": "public, max-age=86400",
            },
        )
    except ImportError:
        # Pillow 미설치 시 원본 반환
        return StreamingResponse(
            io.BytesIO(data),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )
    except Exception as e:
        logger.warning(f"Thumbnail generation failed, returning original: {e}")
        return StreamingResponse(
            io.BytesIO(data),
            media_type="image/png",
            headers={"Cache-Control": "public, max-age=86400"},
        )
