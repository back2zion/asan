"""
AI 분석환경 API - 공유 상수, Docker 클라이언트, 경로, 유틸리티
"""
import json
import logging
from pathlib import Path
from datetime import datetime

import docker

logger = logging.getLogger(__name__)

# Docker 클라이언트 (lazy init)
_docker_client = None


def get_docker_client():
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


# 인프라 컨테이너 (삭제/중지 보호)
PROTECTED_CONTAINERS = {
    "asan-jupyterlab",
    "asan-mlflow",
    "asan-redis",
    "asan-qdrant",
    "asan-xiyan-sql",
    "infra-omop-db-1",
    "infra-nginx-1",
}

# 포트 범위 (새 컨테이너용)
PORT_RANGE_START = 19000
PORT_RANGE_END = 19099

# 노트북 디렉토리 (공유용)
NOTEBOOKS_DIR = Path(__file__).parent.parent / "notebooks"

# JupyterLab 워크스페이스 디렉토리 (asan-jupyterlab 컨테이너의 /home/jovyan/work)
JUPYTER_WORKSPACE = Path(__file__).resolve().parent.parent.parent.parent.parent / "notebooks"

# 감사 로그 파일
SYNC_HISTORY_FILE = NOTEBOOKS_DIR / ".sync_history.json"

# 권한 파일
PERMISSIONS_FILE = NOTEBOOKS_DIR / ".permissions.json"


# --- 감사 로그 헬퍼 ---

def _load_sync_history() -> list:
    """감사 로그 로드"""
    if SYNC_HISTORY_FILE.exists():
        try:
            with open(SYNC_HISTORY_FILE, "r", encoding="utf-8") as fp:
                return json.load(fp)
        except (json.JSONDecodeError, OSError):
            pass
    return []


def _save_sync_history(history: list):
    """감사 로그 저장 (최근 500건 유지)"""
    history = history[-500:]
    NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)
    with open(SYNC_HISTORY_FILE, "w", encoding="utf-8") as fp:
        json.dump(history, fp, ensure_ascii=False, indent=2)


def _add_sync_log(filename: str, action: str, user: str = "anonymous",
                  cell_count: int = 0, size_kb: float = 0, source: str = "system"):
    """감사 로그 항목 추가"""
    history = _load_sync_history()
    history.append({
        "filename": filename,
        "action": action,
        "user": user,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "cell_count": cell_count,
        "size_kb": size_kb,
        "source": source,
    })
    _save_sync_history(history)


def _get_last_modifier(filename: str) -> str:
    """특정 파일의 마지막 수정자 조회"""
    history = _load_sync_history()
    for entry in reversed(history):
        if entry["filename"] == filename:
            return entry.get("user", "anonymous")
    return ""


# --- 권한 관리 헬퍼 ---

def _load_permissions() -> dict:
    """노트북별 공유 권한 설정 로드"""
    if PERMISSIONS_FILE.exists():
        try:
            with open(PERMISSIONS_FILE, "r", encoding="utf-8") as fp:
                return json.load(fp)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_permissions(perms: dict):
    """노트북별 공유 권한 설정 저장"""
    NOTEBOOKS_DIR.mkdir(parents=True, exist_ok=True)
    with open(PERMISSIONS_FILE, "w", encoding="utf-8") as fp:
        json.dump(perms, fp, ensure_ascii=False, indent=2)


def _get_permission(filename: str) -> dict:
    """특정 노트북의 권한 정보 조회"""
    perms = _load_permissions()
    return perms.get(filename, {
        "level": "public",
        "owner": "anonymous",
        "group": "전체",
        "updated": "",
    })
