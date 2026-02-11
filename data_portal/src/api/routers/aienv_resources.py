"""
AI 분석환경 API - 시스템/GPU 모니터링, 템플릿, 헬스체크, 언어 목록
"""
import subprocess
import time

from fastapi import APIRouter, HTTPException
import docker
import psutil

from .aienv_shared import get_docker_client
from services.redis_cache import cached

router = APIRouter()


# --- 분석 템플릿 ---

TEMPLATES = [
    {
        "id": "eda",
        "name": "탐색적 데이터 분석",
        "description": "EDA를 위한 기본 환경 (pandas, matplotlib, seaborn 등)",
        "libraries": ["pandas", "numpy", "matplotlib", "seaborn", "scikit-learn"],
        "icon": "FundProjectionScreenOutlined",
        "default_memory": "4g",
        "default_cpu": 2.0,
    },
    {
        "id": "deeplearning",
        "name": "딥러닝 연구",
        "description": "GPU 가속 딥러닝 환경 (PyTorch, TensorFlow)",
        "libraries": ["tensorflow", "pytorch", "keras", "cuda", "torchvision"],
        "icon": "ThunderboltOutlined",
        "default_memory": "8g",
        "default_cpu": 4.0,
    },
    {
        "id": "nlp",
        "name": "자연어 처리",
        "description": "NLP 연구를 위한 환경 (Transformers, spaCy)",
        "libraries": ["transformers", "spacy", "nltk", "gensim", "sentencepiece"],
        "icon": "RobotOutlined",
        "default_memory": "8g",
        "default_cpu": 4.0,
    },
    {
        "id": "medical-imaging",
        "name": "의료 영상 분석",
        "description": "의료 이미지 처리 환경 (DICOM, OpenCV)",
        "libraries": ["pydicom", "nibabel", "opencv-python", "scikit-image", "SimpleITK"],
        "icon": "ExperimentOutlined",
        "default_memory": "8g",
        "default_cpu": 4.0,
    },
    {
        "id": "spatial",
        "name": "공간 분석",
        "description": "지리·공간 데이터 분석 환경 (GeoPandas, Folium)",
        "libraries": ["geopandas", "folium", "shapely", "pyproj", "contextily"],
        "icon": "EnvironmentOutlined",
        "default_memory": "4g",
        "default_cpu": 2.0,
    },
    {
        "id": "r-statistics",
        "name": "R 통계 분석",
        "description": "R 기반 통계 분석 환경 (tidyverse, ggplot2)",
        "libraries": ["tidyverse", "ggplot2", "dplyr", "caret", "survival"],
        "icon": "LineChartOutlined",
        "language": "r",
        "default_memory": "4g",
        "default_cpu": 2.0,
    },
]


# --- 리소스 모니터링 엔드포인트 ---

_system_cache: dict = {"data": None, "ts": 0}
_SYSTEM_CACHE_TTL = 10  # 10초


@router.get("/resources/system")
@cached("sys-resources", ttl=30)
async def system_resources():
    """호스트 시스템 리소스 실측치"""
    now = time.time()
    if _system_cache["data"] and now - _system_cache["ts"] < _SYSTEM_CACHE_TTL:
        return _system_cache["data"]

    cpu = psutil.cpu_percent(interval=0.1)  # 0.5→0.1초 (충분한 정확도)
    cpu_count = psutil.cpu_count()
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    result = {
        "cpu": {
            "percent": cpu,
            "cores": cpu_count,
            "used_cores": round(cpu * cpu_count / 100, 1),
        },
        "memory": {
            "total_gb": round(mem.total / (1024 ** 3), 1),
            "used_gb": round(mem.used / (1024 ** 3), 1),
            "available_gb": round(mem.available / (1024 ** 3), 1),
            "percent": mem.percent,
        },
        "disk": {
            "total_gb": round(disk.total / (1024 ** 3), 1),
            "used_gb": round(disk.used / (1024 ** 3), 1),
            "free_gb": round(disk.free / (1024 ** 3), 1),
            "percent": disk.percent,
        },
    }
    _system_cache["data"] = result
    _system_cache["ts"] = time.time()
    return result


@router.get("/resources/gpu")
@cached("gpu-resources", ttl=60)
async def gpu_resources():
    """GPU 리소스 정보 (nvidia-smi)"""
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return {"gpus": [], "available": False}

        gpus = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split(",")]
            if len(parts) >= 6:
                gpus.append({
                    "index": int(parts[0]),
                    "name": parts[1],
                    "utilization_percent": float(parts[2]),
                    "memory_used_mb": float(parts[3]),
                    "memory_total_mb": float(parts[4]),
                    "memory_percent": round(float(parts[3]) / float(parts[4]) * 100, 1) if float(parts[4]) > 0 else 0,
                    "temperature": float(parts[5]),
                })
        return {"gpus": gpus, "available": True}
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return {"gpus": [], "available": False}


# --- 분석 템플릿 엔드포인트 ---

@router.get("/templates")
async def list_templates():
    """사전 정의 분석 템플릿 목록"""
    return {"templates": TEMPLATES}


# --- 분석 환경 지원 언어 ---

@router.get("/languages")
async def list_languages():
    """지원 언어 및 버전 목록"""
    return {
        "languages": [
            {
                "id": "python",
                "name": "Python",
                "version": "3.11",
                "description": "데이터 분석, 머신러닝, 딥러닝",
                "kernels": ["python3"],
                "available": True,
            },
            {
                "id": "r",
                "name": "R",
                "version": "4.3",
                "description": "통계 분석, 생존 분석, 임상 연구",
                "kernels": ["ir"],
                "available": True,
            },
            {
                "id": "julia",
                "name": "Julia",
                "version": "1.10",
                "description": "고성능 수치 계산, 시뮬레이션",
                "kernels": ["julia-1.10"],
                "available": True,
            },
        ]
    }


# --- 헬스체크 ---

@router.get("/health")
async def health_check():
    """AI 분석환경 헬스체크"""
    docker_ok = False
    container_count = 0
    try:
        client = get_docker_client()
        client.ping()
        docker_ok = True
        container_count = len(client.containers.list())
    except docker.errors.DockerException:
        pass

    mem = psutil.virtual_memory()
    cpu = psutil.cpu_percent(interval=0.1)

    return {
        "status": "healthy" if docker_ok else "degraded",
        "docker_connected": docker_ok,
        "running_containers": container_count,
        "system": {
            "cpu_percent": cpu,
            "memory_percent": mem.percent,
            "memory_total_gb": round(mem.total / (1024 ** 3), 1),
        },
    }
