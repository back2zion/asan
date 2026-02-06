"""
Imaging API Router

- GET /imaging/images/{filename} — 흉부 X-ray 이미지 서빙
- GET /imaging/studies — imaging_study 테이블 조회 (person JOIN)
"""
import os
import asyncio
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

router = APIRouter(prefix="/imaging")

# 이미지 파일 디렉토리 (다운로드된 NIH Chest X-ray 데이터셋)
DATASET_BASE = os.path.expanduser("~/.cache/kagglehub/datasets/nih-chest-xrays/data/versions/3")
IMAGE_DIRS = [
    os.path.join(DATASET_BASE, d, "images")
    for d in sorted(os.listdir(DATASET_BASE))
    if d.startswith("images_") and os.path.isdir(os.path.join(DATASET_BASE, d, "images"))
] if os.path.isdir(DATASET_BASE) else []
# Fallback: 기존 static 디렉토리
STATIC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "static", "chest_xray")

# OMOP DB 설정
OMOP_CONTAINER = os.getenv("OMOP_CONTAINER", "infra-omop-db-1")
OMOP_USER = os.getenv("OMOP_USER", "omopuser")
OMOP_DB = os.getenv("OMOP_DB", "omop_cdm")


@router.get("/images/{filename}")
async def get_image(filename: str):
    """흉부 X-ray 이미지 파일 서빙"""
    # 경로 순회 공격 방지
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(status_code=400, detail="Invalid filename")

    # 실제 NIH 데이터셋 폴더에서 검색
    filepath = None
    for img_dir in IMAGE_DIRS:
        candidate = os.path.join(img_dir, filename)
        if os.path.isfile(candidate):
            filepath = candidate
            break
    # fallback: static 디렉토리
    if filepath is None:
        candidate = os.path.join(STATIC_DIR, filename)
        if os.path.isfile(candidate):
            filepath = candidate
    if filepath is None:
        raise HTTPException(status_code=404, detail=f"Image not found: {filename}")

    # 확장자에 따른 media type
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    media_types = {
        "png": "image/png",
        "jpg": "image/jpeg",
        "jpeg": "image/jpeg",
        "dicom": "application/dicom",
    }
    media_type = media_types.get(ext, "application/octet-stream")

    return FileResponse(filepath, media_type=media_type)


@router.get("/studies")
async def get_studies(
    person_id: Optional[int] = Query(None, description="특정 환자 ID로 필터"),
    finding: Optional[str] = Query(None, description="소견 키워드 필터"),
    limit: int = Query(50, ge=1, le=200),
):
    """imaging_study 테이블 목록 조회 (person 정보 JOIN)"""
    where_clauses = []
    if person_id is not None:
        where_clauses.append(f"i.person_id = {int(person_id)}")
    if finding:
        # SQL injection 방지: 알파벳, 숫자, 공백, 언더스코어만 허용
        safe_finding = "".join(c for c in finding if c.isalnum() or c in (" ", "_"))
        where_clauses.append(f"i.finding_labels ILIKE '%{safe_finding}%'")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    sql = f"""
    SELECT
        i.imaging_study_id,
        i.person_id,
        p.year_of_birth,
        CASE p.gender_concept_id WHEN 8507 THEN 'M' WHEN 8532 THEN 'F' ELSE 'U' END as gender,
        i.image_filename,
        i.finding_labels,
        i.view_position,
        i.patient_age,
        i.image_url,
        i.created_at
    FROM imaging_study i
    LEFT JOIN person p ON i.person_id = p.person_id
    {where_sql}
    ORDER BY i.imaging_study_id
    LIMIT {int(limit)};
    """

    try:
        cmd = [
            "docker", "exec", OMOP_CONTAINER,
            "psql", "-U", OMOP_USER, "-d", OMOP_DB,
            "-t", "-A", "-F", "\t", "-c", sql
        ]
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10.0)

        if process.returncode != 0:
            raise HTTPException(status_code=500, detail=f"DB error: {stderr.decode().strip()}")

        output = stdout.decode().strip()
        if not output:
            return {"studies": [], "count": 0}

        columns = [
            "imaging_study_id", "person_id", "year_of_birth", "gender",
            "image_filename", "finding_labels", "view_position",
            "patient_age", "image_url", "created_at"
        ]

        studies = []
        for line in output.split("\n"):
            if not line.strip():
                continue
            values = line.split("\t")
            row = {}
            for j, col in enumerate(columns):
                val = values[j] if j < len(values) else None
                if col in ("imaging_study_id", "person_id", "year_of_birth", "patient_age"):
                    try:
                        val = int(val) if val else None
                    except (ValueError, TypeError):
                        pass
                row[col] = val
            studies.append(row)

        return {"studies": studies, "count": len(studies)}

    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="DB query timeout")
