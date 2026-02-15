"""
디지털 병리 API — AI-Hub 위암/유방암 데이터 서빙
"""
import json
import logging
import os
import re
import threading
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/pathology")

# ── 데이터 경로 ──────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[4]  # data_portal/src/api/routers → asan/
DATA_DIRS = {
    "stomach": BASE_DIR / "data" / "10.위암_병리_이미지_및_판독문_합성데이터" / "3.개방데이터" / "1.데이터" / "Validation" / "extracted",
    "breast": BASE_DIR / "data" / "11.유방암_병리_이미지_및_판독문_합성데이터" / "3.개방데이터" / "1.데이터" / "Validation" / "extracted",
}

CATEGORY_LABELS = {
    "STNT": "정상 (Normal)",
    "STDI": "이형성 (Dysplasia)",
    "STIN": "장상피화생 (Intestinal Metaplasia)",
    "STMX": "혼합형 (Mixed)",
    "BRNT": "정상 (Normal)",
    "BRDC": "유관암 (Ductal Carcinoma)",
    "BRID": "침윤성 유관암 (Invasive Ductal)",
    "BRIL": "침윤성 소엽암 (Invasive Lobular)",
    "BRLC": "소엽암 (Lobular Carcinoma)",
}

# 파일명 패턴: NIA6_S_<CATEGORY>_<NUMBER>
_FNAME_RE = re.compile(r"^NIA6_S_([A-Z]{4})_(\d+)$")

# ── 인메모리 인덱스 (파일명 기반 — 빠른 빌드) ─────────────────
_index: list[dict] = []
_index_by_id: dict[str, dict] = {}
_index_ready = threading.Event()


def _build_index():
    """파일명 패턴으로 즉시 인덱스 구축 (JSON 파싱 불필요)"""
    global _index, _index_by_id
    items = []
    for cancer_type, data_dir in DATA_DIRS.items():
        if not data_dir.exists():
            logger.warning(f"Pathology data dir not found: {data_dir}")
            continue
        json_files = sorted(data_dir.glob("*.json"))
        png_stems = {f.stem for f in data_dir.glob("*.png")}
        tumor_code = "STOP" if cancer_type == "stomach" else "BRCA"
        for jf in json_files:
            stem = jf.stem
            m = _FNAME_RE.match(stem)
            category = m.group(1) if m else ""
            tumor_category = "normal" if category.endswith("NT") else "abnormal"
            has_image = stem in png_stems
            items.append({
                "case_id": stem,
                "cancer_type": cancer_type,
                "tumor_code": tumor_code,
                "category": category,
                "category_label": CATEGORY_LABELS.get(category, category),
                "tumor_category": tumor_category,
                "has_image": has_image,
                "file_name": f"{stem}.png",
                "json_path": str(jf),
            })
    # 이미지 있는 케이스가 먼저 표시되도록 정렬
    items.sort(key=lambda x: (not x["has_image"], x["case_id"]))
    _index = items
    _index_by_id = {c["case_id"]: c for c in items}
    _index_ready.set()
    logger.info(f"Pathology index built: {len(items)} cases "
                f"(stomach={sum(1 for i in items if i['cancer_type']=='stomach')}, "
                f"breast={sum(1 for i in items if i['cancer_type']=='breast')})")


# 백그라운드에서 인덱스 구축
_build_thread = threading.Thread(target=_build_index, daemon=True)
_build_thread.start()


def _wait_index():
    if not _index_ready.wait(timeout=30):
        raise HTTPException(503, "Pathology index is still building")


def _validate_filename(filename: str):
    """경로 순회 공격 방지"""
    if ".." in filename or "/" in filename or "\\" in filename:
        raise HTTPException(400, "Invalid filename")


# ── 엔드포인트 ───────────────────────────────────────────────
@router.get("/stats")
async def get_stats():
    """암종별 통계 요약"""
    _wait_index()
    stats = {}
    for cancer_type in ("stomach", "breast"):
        cases = [c for c in _index if c["cancer_type"] == cancer_type]
        categories: dict = {}
        for c in cases:
            cat = c["category"]
            if cat not in categories:
                categories[cat] = {"count": 0, "label": c["category_label"], "with_image": 0}
            categories[cat]["count"] += 1
            if c["has_image"]:
                categories[cat]["with_image"] += 1
        stats[cancer_type] = {
            "total": len(cases),
            "with_image": sum(1 for c in cases if c["has_image"]),
            "categories": categories,
        }
    stats["total"] = len(_index)
    return stats


@router.get("/cases")
async def list_cases(
    cancer_type: Optional[str] = Query(None, description="stomach or breast"),
    category: Optional[str] = Query(None, description="e.g. STNT, BRDC"),
    tumor_category: Optional[str] = Query(None, description="normal or abnormal"),
    search: Optional[str] = Query(None, description="case_id keyword search"),
    has_image: Optional[bool] = Query(None),
    offset: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """케이스 목록 (필터 + 페이지네이션)"""
    _wait_index()
    filtered = _index
    if cancer_type:
        filtered = [c for c in filtered if c["cancer_type"] == cancer_type]
    if category:
        filtered = [c for c in filtered if c["category"] == category]
    if tumor_category:
        filtered = [c for c in filtered if c["tumor_category"] == tumor_category]
    if search:
        kw = search.upper()
        filtered = [c for c in filtered if kw in c["case_id"].upper()]
    if has_image is not None:
        filtered = [c for c in filtered if c["has_image"] == has_image]
    total = len(filtered)
    page = filtered[offset:offset + limit]
    # json_path는 내부용 — 클라이언트에 노출하지 않음
    items = [{k: v for k, v in c.items() if k != "json_path"} for c in page]
    return {"total": total, "offset": offset, "limit": limit, "items": items}


@router.get("/cases/{case_id}")
async def get_case(case_id: str):
    """개별 케이스 상세 (clinical + file + annotations)"""
    _wait_index()
    _validate_filename(case_id)
    entry = _index_by_id.get(case_id)
    if not entry:
        raise HTTPException(404, f"Case not found: {case_id}")
    try:
        with open(entry["json_path"], "r", encoding="utf-8-sig") as f:
            raw = json.load(f)
        content = raw.get("content", raw)
        return {
            "case_id": entry["case_id"],
            "cancer_type": entry["cancer_type"],
            "category": entry["category"],
            "category_label": entry["category_label"],
            "tumor_category": entry["tumor_category"],
            "has_image": entry["has_image"],
            "clinical": content.get("clinical", {}),
            "file": content.get("file", {}),
        }
    except Exception as e:
        raise HTTPException(500, f"Failed to load case data: {e}")


@router.get("/images/{cancer_type}/{filename}")
async def get_image(cancer_type: str, filename: str):
    """PNG 이미지 서빙"""
    _validate_filename(filename)
    if cancer_type not in DATA_DIRS:
        raise HTTPException(400, f"Invalid cancer_type: {cancer_type}")
    if not filename.lower().endswith(".png"):
        raise HTTPException(400, "Only PNG files are supported")
    filepath = DATA_DIRS[cancer_type] / filename
    if not filepath.is_file():
        raise HTTPException(404, f"Image not found: {filename}")
    return FileResponse(str(filepath), media_type="image/png")
