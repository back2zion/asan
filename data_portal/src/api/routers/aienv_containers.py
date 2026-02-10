"""
AI 분석환경 API - 컨테이너 CRUD + 실시간 리소스 사용량
"""
import time
from typing import Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
import docker

from .aienv_shared import (
    get_docker_client,
    PROTECTED_CONTAINERS,
    PORT_RANGE_START,
    PORT_RANGE_END,
)

router = APIRouter()


# --- Pydantic Models ---

class ContainerCreateRequest(BaseModel):
    name: str
    cpu_limit: Optional[float] = 2.0  # CPU cores
    memory_limit: Optional[str] = "4g"  # e.g. "4g", "8g"
    template_id: Optional[str] = None
    language: Optional[str] = "python"  # "python", "r", "julia"


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
                # JupyterLab 포트 (8888/tcp) - Vite proxy 경로 사용 (WSL2 호환)
                if "8888" in container_port:
                    access_url = "/jupyter/lab"

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

_containers_cache: dict = {"data": None, "ts": 0, "prefix": None}
_CONTAINERS_CACHE_TTL = 30  # 30초


@router.get("/containers")
async def list_containers(prefix: str = Query("asan-", description="컨테이너 이름 필터 prefix")):
    """실행 중인 Docker 컨테이너 목록 (asan- prefix 필터링)"""
    now = time.time()
    if (_containers_cache["data"] and _containers_cache["prefix"] == prefix
            and now - _containers_cache["ts"] < _CONTAINERS_CACHE_TTL):
        return _containers_cache["data"]

    try:
        client = get_docker_client()
        containers = client.containers.list(all=True)
        result = []
        for c in containers:
            if prefix and not c.name.startswith(prefix):
                continue
            result.append(_container_to_dict(c))
        result.sort(key=lambda x: x["name"])
        resp = {"containers": result, "total": len(result)}
        _containers_cache["data"] = resp
        _containers_cache["ts"] = time.time()
        _containers_cache["prefix"] = prefix
        return resp
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
                "language": req.language or "python",
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
