"""
AI 분석환경 API - 공유 노트북 CRUD, 워크스페이스 동기화, 권한 관리, 내보내기
PostgreSQL-backed (sync_history, permissions via aienv_shared async functions)
"""
import json
import csv
import io
import html as html_module
from typing import Optional
from datetime import datetime
from urllib.parse import quote
from pathlib import Path
import shutil

from fastapi import APIRouter, HTTPException, Query, Header, UploadFile, File
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from .aienv_shared import (
    logger,
    NOTEBOOKS_DIR,
    JUPYTER_WORKSPACE,
    load_sync_history,
    add_sync_log,
    get_last_modifier,
    load_permissions,
    save_permissions,
    get_permission,
)

router = APIRouter()


# --- 워크스페이스 동기화 ---

async def _sync_workspace_to_shared():
    """JupyterLab 워크스페이스의 shared/ 폴더에서 변경된 노트북을 공유 라이브러리로 동기화"""
    shared_dir = JUPYTER_WORKSPACE / "shared"
    if not shared_dir.exists():
        return
    synced = []
    for ws_file in shared_dir.glob("*.ipynb"):
        shared_file = NOTEBOOKS_DIR / ws_file.name
        if not shared_file.exists():
            shutil.copy2(str(ws_file), str(shared_file))
            cell_count = 0
            try:
                with open(shared_file, "r", encoding="utf-8") as fp:
                    nb = json.load(fp)
                    cell_count = len(nb.get("cells", []))
            except (json.JSONDecodeError, OSError):
                pass
            await add_sync_log(
                ws_file.name, "sync_back", "jupyterlab",
                cell_count=cell_count,
                size_kb=round(ws_file.stat().st_size / 1024, 1),
                source="jupyterlab",
            )
            synced.append(ws_file.name)
        else:
            ws_mtime = ws_file.stat().st_mtime
            shared_mtime = shared_file.stat().st_mtime
            if ws_mtime > shared_mtime + 1:
                shutil.copy2(str(ws_file), str(shared_file))
                cell_count = 0
                try:
                    with open(shared_file, "r", encoding="utf-8") as fp:
                        nb = json.load(fp)
                        cell_count = len(nb.get("cells", []))
                except (json.JSONDecodeError, OSError):
                    pass
                await add_sync_log(
                    ws_file.name, "sync_back", "jupyterlab",
                    cell_count=cell_count,
                    size_kb=round(ws_file.stat().st_size / 1024, 1),
                    source="jupyterlab",
                )
                synced.append(ws_file.name)
    if synced:
        logger.info(f"Synced {len(synced)} notebook(s) from workspace: {synced}")


# --- Pydantic Models ---

class PermissionUpdate(BaseModel):
    level: str  # "private", "group", "public"
    group: Optional[str] = "전체"


# =============================================
# 공유 노트북 엔드포인트
# =============================================

@router.get("/shared")
async def list_shared_notebooks():
    """공유 노트북 파일 목록 (워크스페이스 -> 공유 자동 동기화 포함)"""
    try:
        await _sync_workspace_to_shared()
    except Exception as e:
        logger.warning(f"Workspace sync failed: {e}")

    notebooks = []
    if NOTEBOOKS_DIR.exists():
        for f in NOTEBOOKS_DIR.glob("*.ipynb"):
            try:
                stat = f.stat()
                cell_count = 0
                try:
                    with open(f, "r", encoding="utf-8") as fp:
                        nb = json.load(fp)
                        cell_count = len(nb.get("cells", []))
                except (json.JSONDecodeError, KeyError):
                    pass

                perm = await get_permission(f.name)
                notebooks.append({
                    "filename": f.name,
                    "name": f.stem.replace("_", " ").replace("-", " "),
                    "size_kb": round(stat.st_size / 1024, 1),
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                    "cell_count": cell_count,
                    "last_modified_by": await get_last_modifier(f.name),
                    "permission": perm.get("level", "public"),
                    "permission_group": perm.get("group", "전체"),
                })
            except OSError:
                continue
    notebooks.sort(key=lambda x: x["modified"], reverse=True)
    return {"notebooks": notebooks, "total": len(notebooks)}


@router.get("/shared/history")
async def get_sync_history_endpoint(limit: int = Query(50, ge=1, le=500)):
    """전체 노트북 수정 이력 조회"""
    history = await load_sync_history(limit=limit)
    return {"history": history, "total": len(history)}


@router.get("/shared/{filename}/history")
async def get_notebook_history(filename: str, limit: int = Query(20, ge=1, le=100)):
    """특정 노트북 수정 이력 조회"""
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    # Load all history for this filename via DB query
    from .aienv_shared import _get_conn, _rel, _ensure_tables
    await _ensure_tables()
    conn = await _get_conn()
    try:
        rows = await conn.fetch(
            "SELECT * FROM aienv_sync_history WHERE filename=$1 ORDER BY created_at DESC LIMIT $2",
            filename, limit,
        )
        filtered = [
            {
                "filename": r["filename"],
                "action": r["action"],
                "user": r["username"],
                "timestamp": r["created_at"].strftime("%Y-%m-%d %H:%M:%S") if r["created_at"] else "",
                "cell_count": r["cell_count"],
                "size_kb": r["size_kb"],
                "source": r["source"],
            }
            for r in rows
        ]
    finally:
        await _rel(conn)
    return {"filename": filename, "history": filtered, "total": len(filtered)}


@router.get("/shared/{filename}/download")
async def download_notebook(filename: str):
    """노트북 파일 다운로드"""
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    file_path = NOTEBOOKS_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다")
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type="application/x-ipynb+json",
    )


@router.get("/shared/{filename}/preview")
async def preview_notebook(filename: str):
    """노트북 셀 내용 미리보기"""
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    file_path = NOTEBOOKS_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다")
    try:
        with open(file_path, "r", encoding="utf-8") as fp:
            nb = json.load(fp)
        cells = []
        for cell in nb.get("cells", []):
            cells.append({
                "cell_type": cell.get("cell_type", "code"),
                "source": "".join(cell.get("source", [])) if isinstance(cell.get("source"), list) else cell.get("source", ""),
            })
        metadata = nb.get("metadata", {})
        kernel = metadata.get("kernelspec", {}).get("display_name", "")
        lang = metadata.get("language_info", {}).get("name", "")
        return {
            "filename": filename,
            "cells": cells,
            "cell_count": len(cells),
            "kernel": kernel,
            "language": lang,
        }
    except (json.JSONDecodeError, KeyError) as e:
        raise HTTPException(status_code=400, detail=f"노트북 파싱 실패: {str(e)}")


@router.post("/shared/upload")
async def upload_notebook(
    file: UploadFile = File(...),
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
):
    """노트북 파일 업로드"""
    if not file.filename or not file.filename.endswith(".ipynb"):
        raise HTTPException(status_code=400, detail=".ipynb 파일만 업로드할 수 있습니다")
    safe_name = file.filename.replace("..", "").replace("/", "").replace("\\", "")
    if not safe_name:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)
    dest = NOTEBOOKS_DIR / safe_name
    content = await file.read()
    try:
        nb = json.loads(content)
        if "cells" not in nb or "nbformat" not in nb:
            raise ValueError("유효한 Jupyter 노트북이 아닙니다")
    except (json.JSONDecodeError, ValueError) as e:
        raise HTTPException(status_code=400, detail=f"유효하지 않은 노트북 파일: {str(e)}")
    with open(dest, "wb") as fp:
        fp.write(content)
    stat = dest.stat()
    cell_count = len(nb.get("cells", []))
    await add_sync_log(
        safe_name, "upload", x_user_name,
        cell_count=cell_count,
        size_kb=round(stat.st_size / 1024, 1),
        source="upload",
    )
    return {
        "message": f"'{safe_name}' 업로드 완료",
        "notebook": {
            "filename": safe_name,
            "name": dest.stem.replace("_", " ").replace("-", " "),
            "size_kb": round(stat.st_size / 1024, 1),
            "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
            "cell_count": cell_count,
        },
    }


@router.delete("/shared/{filename}")
async def delete_notebook(
    filename: str,
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
):
    """공유 노트북 삭제"""
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    file_path = NOTEBOOKS_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다")
    await add_sync_log(filename, "delete", x_user_name, source="portal")
    file_path.unlink()
    return {"message": f"'{filename}' 삭제 완료"}


@router.post("/shared/{filename}/open-in-jupyter")
async def open_in_jupyter(filename: str):
    """공유 노트북을 JupyterLab 워크스페이스로 복사 후 열기 URL 반환"""
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    src = NOTEBOOKS_DIR / filename
    if not src.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다")
    shared_dir = JUPYTER_WORKSPACE / "shared"
    shared_dir.mkdir(parents=True, exist_ok=True)
    dest = shared_dir / filename
    shutil.copy2(str(src), str(dest))
    jupyter_path = f"/jupyter/lab/tree/work/shared/{filename}"
    return {
        "message": f"'{filename}'을(를) JupyterLab 워크스페이스로 복사했습니다",
        "jupyter_url": jupyter_path,
    }


@router.get("/jupyter/workspace")
async def list_jupyter_workspace():
    """JupyterLab 워크스페이스의 노트북 목록"""
    notebooks = []
    if JUPYTER_WORKSPACE.exists():
        for f in JUPYTER_WORKSPACE.rglob("*.ipynb"):
            if ".ipynb_checkpoints" in str(f):
                continue
            try:
                stat = f.stat()
                rel_path = f.relative_to(JUPYTER_WORKSPACE)
                cell_count = 0
                try:
                    with open(f, "r", encoding="utf-8") as fp:
                        nb = json.load(fp)
                        cell_count = len(nb.get("cells", []))
                except (json.JSONDecodeError, KeyError):
                    pass
                notebooks.append({
                    "filename": str(rel_path),
                    "name": f.stem.replace("_", " ").replace("-", " "),
                    "size_kb": round(stat.st_size / 1024, 1),
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                    "cell_count": cell_count,
                    "jupyter_url": f"/jupyter/lab/tree/work/{rel_path}",
                })
            except OSError:
                continue
    notebooks.sort(key=lambda x: x["modified"], reverse=True)
    return {"notebooks": notebooks, "total": len(notebooks)}


# =============================================
# 권한 관리 엔드포인트
# =============================================

@router.get("/shared/{filename}/permissions")
async def get_permissions(filename: str):
    """노트북 공유 권한 조회"""
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    return await get_permission(filename)


@router.put("/shared/{filename}/permissions")
async def update_permissions(
    filename: str,
    req: PermissionUpdate,
    x_user_name: str = Header("anonymous", alias="X-User-Name"),
):
    """노트북 공유 권한 변경 (개인/그룹/전체)"""
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    file_path = NOTEBOOKS_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다")
    if req.level not in ("private", "group", "public"):
        raise HTTPException(status_code=400, detail="level은 private, group, public 중 하나여야 합니다")

    perm_data = {
        filename: {
            "level": req.level,
            "owner": x_user_name,
            "group": req.group or "전체",
        }
    }
    await save_permissions(perm_data)

    await add_sync_log(filename, f"permission_change:{req.level}", x_user_name, source="portal")

    return {
        "message": f"'{filename}' 공유 설정이 변경되었습니다",
        "permission": {
            "level": req.level,
            "owner": x_user_name,
            "group": req.group or "전체",
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        },
    }


# =============================================
# 다양한 포맷 내보내기
# =============================================

@router.get("/shared/{filename}/export/{fmt}")
async def export_notebook(filename: str, fmt: str):
    """노트북을 다양한 포맷으로 내보내기 (ipynb, html, csv)"""
    if ".." in filename or "/" in filename:
        raise HTTPException(status_code=400, detail="잘못된 파일명입니다")
    file_path = NOTEBOOKS_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다")

    if fmt == "ipynb":
        return FileResponse(
            path=str(file_path), filename=filename,
            media_type="application/x-ipynb+json",
        )

    try:
        with open(file_path, "r", encoding="utf-8") as fp:
            nb = json.load(fp)
    except (json.JSONDecodeError, OSError) as e:
        raise HTTPException(status_code=400, detail=f"노트북 파싱 실패: {str(e)}")

    cells = nb.get("cells", [])
    base_name = Path(filename).stem

    if fmt == "html":
        html_parts = [
            "<!DOCTYPE html><html><head><meta charset='utf-8'>",
            f"<title>{html_module.escape(base_name)}</title>",
            "<style>",
            "body{font-family:'Malgun Gothic',sans-serif;max-width:960px;margin:0 auto;padding:24px;background:#fafafa}",
            "h1{color:#006241;border-bottom:2px solid #006241;padding-bottom:8px}",
            ".cell{margin-bottom:16px;background:#fff;border-radius:8px;padding:16px;box-shadow:0 1px 3px rgba(0,0,0,0.1)}",
            ".cell-label{color:#999;font-size:11px;margin-bottom:6px;font-weight:600}",
            "pre{background:#f5f5f5;padding:12px;border-radius:6px;overflow:auto;font-size:13px;line-height:1.5}",
            ".md-cell pre{background:#fafff5}",
            "</style></head><body>",
            f"<h1>{html_module.escape(base_name)}</h1>",
            f"<p style='color:#888;font-size:13px'>셀 {len(cells)}개 | 서울아산병원 통합 데이터 플랫폼</p>",
        ]
        for i, cell in enumerate(cells):
            source = "".join(cell.get("source", [])) if isinstance(cell.get("source"), list) else cell.get("source", "")
            ct = cell.get("cell_type", "code")
            escaped = html_module.escape(source)
            if ct == "markdown":
                html_parts.append(f'<div class="cell md-cell"><div class="cell-label">Markdown [{i+1}]</div><pre>{escaped}</pre></div>')
            else:
                html_parts.append(f'<div class="cell"><div class="cell-label">Code [{i+1}]</div><pre>{escaped}</pre></div>')
        html_parts.append("</body></html>")
        html_content = "\n".join(html_parts)
        tmp_path = NOTEBOOKS_DIR / f".export_{base_name}.html"
        with open(tmp_path, "w", encoding="utf-8") as fp:
            fp.write(html_content)
        safe_fn = quote(f"{base_name}.html")
        return FileResponse(
            path=str(tmp_path),
            filename=safe_fn,
            media_type="text/html",
        )

    elif fmt == "csv":
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Cell", "Type", "Source"])
        for i, cell in enumerate(cells):
            source = "".join(cell.get("source", [])) if isinstance(cell.get("source"), list) else cell.get("source", "")
            ct = cell.get("cell_type", "code")
            writer.writerow([i + 1, ct, source])
        tmp_path = NOTEBOOKS_DIR / f".export_{base_name}.csv"
        with open(tmp_path, "w", encoding="utf-8-sig") as fp:
            fp.write(output.getvalue())
        safe_fn = quote(f"{base_name}.csv")
        return FileResponse(
            path=str(tmp_path),
            filename=safe_fn,
            media_type="text/csv",
        )

    else:
        raise HTTPException(
            status_code=400,
            detail=f"지원하지 않는 형식: {fmt}. ipynb, html, csv 중 선택하세요",
        )
