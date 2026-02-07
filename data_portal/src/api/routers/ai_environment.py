"""
AI 분석환경 API - Docker 기반 컨테이너 관리 및 리소스 모니터링
"""
import os
import json
import subprocess
from pathlib import Path
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
import docker
import psutil

router = APIRouter(prefix="/ai-environment", tags=["AIEnvironment"])

# Docker 클라이언트 (lazy init)
_docker_client = None

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

# 노트북 디렉토리
NOTEBOOKS_DIR = Path(__file__).parent.parent / "notebooks"


def get_docker_client():
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


# --- Pydantic Models ---

class ContainerCreateRequest(BaseModel):
    name: str
    cpu_limit: Optional[float] = 2.0  # CPU cores
    memory_limit: Optional[str] = "4g"  # e.g. "4g", "8g"
    template_id: Optional[str] = None


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
]


# --- Helper functions ---

def _find_available_port() -> int:
    """PORT_RANGE에서 사용 가능한 포트 찾기"""
    client = get_docker_client()
    used_ports = set()
    for c in client.containers.list(all=True):
        ports = c.attrs.get("NetworkSettings", {}).get("Ports") or {}
        for bindings in ports.values():
            if bindings:
                for b in bindings:
                    try:
                        used_ports.add(int(b["HostPort"]))
                    except (KeyError, ValueError, TypeError):
                        pass
    for port in range(PORT_RANGE_START, PORT_RANGE_END + 1):
        if port not in used_ports:
            return port
    raise HTTPException(status_code=503, detail="사용 가능한 포트가 없습니다 (19000-19099 모두 사용 중)")


def _container_to_dict(container) -> dict:
    """Docker 컨테이너 객체를 API 응답 dict로 변환"""
    attrs = container.attrs
    state = attrs.get("State", {})
    config = attrs.get("Config", {})
    host_config = attrs.get("HostConfig", {})

    # 포트 매핑 추출
    ports = attrs.get("NetworkSettings", {}).get("Ports") or {}
    port_mappings = {}
    access_url = None
    for container_port, bindings in ports.items():
        if bindings:
            host_port = bindings[0].get("HostPort")
            if host_port:
                port_mappings[container_port] = host_port
                # JupyterLab 포트 (8888/tcp)
                if "8888" in container_port:
                    access_url = f"http://localhost:{host_port}"

    # 메모리/CPU 제한
    memory_limit = host_config.get("Memory", 0)
    memory_str = f"{memory_limit // (1024**3)}GB" if memory_limit else "제한 없음"
    cpu_period = host_config.get("CpuPeriod", 0)
    cpu_quota = host_config.get("CpuQuota", 0)
    cpu_cores = f"{cpu_quota / cpu_period:.1f} cores" if cpu_period and cpu_quota else "제한 없음"

    # 이미지명
    image = config.get("Image", "")
    if not image:
        image_tags = attrs.get("Image", "")
        image = image_tags

    # 생성 시간
    created = attrs.get("Created", "")
    if created:
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            created = dt.strftime("%Y-%m-%d %H:%M")
        except (ValueError, TypeError):
            pass

    name = container.name
    return {
        "id": container.short_id,
        "full_id": container.id,
        "name": name,
        "status": state.get("Status", "unknown"),
        "image": image,
        "cpu": cpu_cores,
        "memory": memory_str,
        "ports": port_mappings,
        "access_url": access_url,
        "created": created,
        "is_protected": name in PROTECTED_CONTAINERS,
    }


def _calc_cpu_percent(stats: dict) -> float:
    """Docker stats에서 CPU 사용률 계산"""
    cpu_delta = stats["cpu_stats"]["cpu_usage"]["total_usage"] - \
                stats["precpu_stats"]["cpu_usage"]["total_usage"]
    system_delta = stats["cpu_stats"]["system_cpu_usage"] - \
                   stats["precpu_stats"]["system_cpu_usage"]
    num_cpus = stats["cpu_stats"].get("online_cpus", 1)
    if system_delta > 0 and cpu_delta >= 0:
        return round((cpu_delta / system_delta) * num_cpus * 100, 1)
    return 0.0


# --- 컨테이너 관리 엔드포인트 ---

@router.get("/containers")
async def list_containers(prefix: str = Query("asan-", description="컨테이너 이름 필터 prefix")):
    """실행 중인 Docker 컨테이너 목록 (asan- prefix 필터링)"""
    try:
        client = get_docker_client()
        containers = client.containers.list(all=True)
        result = []
        for c in containers:
            if prefix and not c.name.startswith(prefix):
                continue
            result.append(_container_to_dict(c))
        result.sort(key=lambda x: x["name"])
        return {"containers": result, "total": len(result)}
    except docker.errors.DockerException as e:
        raise HTTPException(status_code=503, detail=f"Docker 연결 실패: {str(e)}")


@router.post("/containers")
async def create_container(req: ContainerCreateRequest):
    """새 JupyterLab 컨테이너 생성"""
    try:
        client = get_docker_client()

        # 이름 중복 체크
        container_name = f"asan-{req.name}" if not req.name.startswith("asan-") else req.name
        try:
            existing = client.containers.get(container_name)
            raise HTTPException(status_code=409, detail=f"컨테이너 '{container_name}'이(가) 이미 존재합니다")
        except docker.errors.NotFound:
            pass

        # 포트 할당
        host_port = _find_available_port()

        # 메모리 제한 파싱
        mem_limit = req.memory_limit or "4g"

        # 컨테이너 생성
        container = client.containers.run(
            image="jupyter/datascience-notebook:latest",
            name=container_name,
            detach=True,
            ports={"8888/tcp": host_port},
            environment={
                "JUPYTER_ENABLE_LAB": "yes",
                "GRANT_SUDO": "yes",
                "NB_UID": "1000",
            },
            mem_limit=mem_limit,
            nano_cpus=int(req.cpu_limit * 1e9),
            network="asan-network",
            restart_policy={"Name": "unless-stopped"},
            labels={
                "managed_by": "asan-idp",
                "template": req.template_id or "custom",
            },
        )

        return {
            "message": f"컨테이너 '{container_name}' 생성 완료",
            "container": _container_to_dict(client.containers.get(container_name)),
        }
    except docker.errors.ImageNotFound:
        raise HTTPException(status_code=404, detail="jupyter/datascience-notebook 이미지를 찾을 수 없습니다. docker pull을 실행해주세요.")
    except docker.errors.DockerException as e:
        raise HTTPException(status_code=500, detail=f"컨테이너 생성 실패: {str(e)}")


@router.post("/containers/{container_id}/start")
async def start_container(container_id: str):
    """컨테이너 시작"""
    try:
        client = get_docker_client()
        container = client.containers.get(container_id)
        container.start()
        return {"message": f"컨테이너 '{container.name}' 시작됨", "status": "running"}
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="컨테이너를 찾을 수 없습니다")
    except docker.errors.DockerException as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/containers/{container_id}/stop")
async def stop_container(container_id: str):
    """컨테이너 중지"""
    try:
        client = get_docker_client()
        container = client.containers.get(container_id)
        if container.name in PROTECTED_CONTAINERS:
            raise HTTPException(status_code=403, detail=f"'{container.name}'은(는) 인프라 컨테이너로 중지할 수 없습니다")
        container.stop(timeout=10)
        return {"message": f"컨테이너 '{container.name}' 중지됨", "status": "exited"}
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="컨테이너를 찾을 수 없습니다")
    except docker.errors.DockerException as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/containers/{container_id}")
async def delete_container(container_id: str):
    """컨테이너 삭제 (인프라 컨테이너 보호)"""
    try:
        client = get_docker_client()
        container = client.containers.get(container_id)
        if container.name in PROTECTED_CONTAINERS:
            raise HTTPException(status_code=403, detail=f"'{container.name}'은(는) 인프라 컨테이너로 삭제할 수 없습니다")
        container.remove(force=True)
        return {"message": f"컨테이너 '{container.name}' 삭제됨"}
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="컨테이너를 찾을 수 없습니다")
    except docker.errors.DockerException as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/containers/{container_id}/stats")
async def container_stats(container_id: str):
    """컨테이너 실시간 리소스 사용량"""
    try:
        client = get_docker_client()
        container = client.containers.get(container_id)
        if container.status != "running":
            return {"cpu_percent": 0, "memory_usage_mb": 0, "memory_limit_mb": 0, "memory_percent": 0}
        stats = container.stats(stream=False)

        cpu_percent = _calc_cpu_percent(stats)

        mem_usage = stats["memory_stats"].get("usage", 0)
        mem_limit = stats["memory_stats"].get("limit", 0)
        mem_usage_mb = round(mem_usage / (1024 ** 2), 1)
        mem_limit_mb = round(mem_limit / (1024 ** 2), 1)
        mem_percent = round((mem_usage / mem_limit) * 100, 1) if mem_limit else 0

        return {
            "cpu_percent": cpu_percent,
            "memory_usage_mb": mem_usage_mb,
            "memory_limit_mb": mem_limit_mb,
            "memory_percent": mem_percent,
        }
    except docker.errors.NotFound:
        raise HTTPException(status_code=404, detail="컨테이너를 찾을 수 없습니다")
    except docker.errors.DockerException as e:
        raise HTTPException(status_code=500, detail=str(e))


# --- 리소스 모니터링 엔드포인트 ---

@router.get("/resources/system")
async def system_resources():
    """호스트 시스템 리소스 실측치"""
    cpu = psutil.cpu_percent(interval=0.5)
    cpu_count = psutil.cpu_count()
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")

    return {
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


@router.get("/resources/gpu")
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


# --- 분석 작업 공유 엔드포인트 ---

@router.get("/shared")
async def list_shared_notebooks():
    """공유 노트북 파일 목록"""
    notebooks = []
    if NOTEBOOKS_DIR.exists():
        for f in NOTEBOOKS_DIR.glob("*.ipynb"):
            try:
                stat = f.stat()
                # 셀 수 계산
                cell_count = 0
                try:
                    with open(f, "r", encoding="utf-8") as fp:
                        nb = json.load(fp)
                        cell_count = len(nb.get("cells", []))
                except (json.JSONDecodeError, KeyError):
                    pass

                notebooks.append({
                    "filename": f.name,
                    "name": f.stem.replace("_", " ").replace("-", " "),
                    "size_kb": round(stat.st_size / 1024, 1),
                    "modified": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                    "cell_count": cell_count,
                })
            except OSError:
                continue
    notebooks.sort(key=lambda x: x["modified"], reverse=True)
    return {"notebooks": notebooks, "total": len(notebooks)}


@router.get("/shared/{filename}/download")
async def download_notebook(filename: str):
    """노트북 파일 다운로드"""
    # 경로 탐색 방지
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
