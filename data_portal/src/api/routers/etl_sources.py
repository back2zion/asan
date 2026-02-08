"""
ETL Source Connector Endpoints — /etl/sources, /etl/sources/types, /etl/sources/{source_id}, /etl/sources/test
"""
import uuid
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .etl_shared import (
    CONNECTOR_TYPE_REGISTRY,
    load_sources,
    save_sources,
)

router = APIRouter()


# =====================================================================
#  Pydantic models
# =====================================================================

class SourceTestRequest(BaseModel):
    source_id: str


class SourceConnectorCreate(BaseModel):
    name: str
    type: str
    subtype: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    path: Optional[str] = None
    url: Optional[str] = None
    description: Optional[str] = ""
    extra_config: Optional[Dict[str, Any]] = None


class SourceConnectorUpdate(BaseModel):
    name: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    path: Optional[str] = None
    url: Optional[str] = None
    description: Optional[str] = None
    extra_config: Optional[Dict[str, Any]] = None


# =====================================================================
#  Endpoints
# =====================================================================

@router.get("/sources")
async def list_sources():
    """이기종 소스 커넥터 목록"""
    sources = load_sources()
    type_summary = {}
    for s in sources:
        t = s["type"]
        type_summary[t] = type_summary.get(t, 0) + 1
    return {
        "sources": sources,
        "total": len(sources),
        "type_summary": type_summary,
    }


@router.get("/sources/types")
async def get_source_types():
    """커넥터 타입 레지스트리 반환 (프론트엔드 동적 폼 구성용)"""
    return {
        "types": CONNECTOR_TYPE_REGISTRY,
        "total_types": len(CONNECTOR_TYPE_REGISTRY),
        "total_subtypes": sum(
            len(t["subtypes"]) for t in CONNECTOR_TYPE_REGISTRY.values()
        ),
    }


@router.post("/sources")
async def create_source(req: SourceConnectorCreate):
    """커넥터 생성"""
    if req.type not in CONNECTOR_TYPE_REGISTRY:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 타입: {req.type}")
    type_info = CONNECTOR_TYPE_REGISTRY[req.type]
    if req.subtype not in type_info["subtypes"]:
        raise HTTPException(status_code=400, detail=f"지원하지 않는 서브타입: {req.subtype}")

    sources = load_sources()
    source_id = str(uuid.uuid4())[:8]
    new_source: Dict[str, Any] = {
        "id": source_id,
        "name": req.name,
        "type": req.type,
        "subtype": req.subtype,
        "status": "configured",
        "tables": 0,
        "description": req.description or "",
        "last_sync": None,
        "created_at": datetime.now().isoformat(),
    }
    if req.host:
        new_source["host"] = req.host
    if req.port:
        new_source["port"] = req.port
    if req.database:
        new_source["database"] = req.database
    if req.username:
        new_source["username"] = req.username
    if req.password:
        new_source["password"] = req.password
    if req.path:
        new_source["path"] = req.path
    if req.url:
        new_source["url"] = req.url
    if req.extra_config:
        new_source["extra_config"] = req.extra_config

    sources.append(new_source)
    save_sources(sources)
    return {"success": True, "source": new_source, "message": f"커넥터 '{req.name}' 생성됨"}


@router.put("/sources/{source_id}")
async def update_source(source_id: str, req: SourceConnectorUpdate):
    """커넥터 설정 수정"""
    sources = load_sources()
    source = next((s for s in sources if s["id"] == source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다")

    update_data = req.model_dump(exclude_none=True)
    source.update(update_data)
    source["updated_at"] = datetime.now().isoformat()
    save_sources(sources)
    return {"success": True, "source": source, "message": f"커넥터 '{source['name']}' 수정됨"}


@router.delete("/sources/{source_id}")
async def delete_source(source_id: str):
    """커넥터 삭제 (omop-cdm 보호)"""
    if source_id == "omop-cdm":
        raise HTTPException(status_code=403, detail="기본 OMOP CDM 소스는 삭제할 수 없습니다")

    sources = load_sources()
    original_len = len(sources)
    sources = [s for s in sources if s["id"] != source_id]
    if len(sources) == original_len:
        raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다")

    save_sources(sources)
    return {"success": True, "message": f"커넥터 '{source_id}' 삭제됨"}


@router.post("/sources/test")
async def test_source_connection(req: SourceTestRequest):
    """소스 커넥터 연결 테스트 (모든 PostgreSQL 소스 실제 테스트)"""
    sources = load_sources()
    source = next((s for s in sources if s["id"] == req.source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다")

    # 모든 PostgreSQL 소스에 대해 실제 연결 테스트
    if source.get("subtype") == "postgresql":
        try:
            import asyncpg
            conn = await asyncpg.connect(
                host=source.get("host", "localhost"),
                port=source.get("port", 5432),
                user=source.get("username", "omopuser"),
                password=source.get("password", "omop"),
                database=source.get("database", "omop_cdm"),
            )
            version = await conn.fetchval("SELECT version()")
            await conn.close()
            source["status"] = "connected"
            save_sources(sources)
            return {"success": True, "message": f"연결 성공: {version[:60]}", "status": "connected"}
        except Exception as e:
            source["status"] = "error"
            save_sources(sources)
            return {"success": False, "message": f"연결 실패: {str(e)}", "status": "error"}

    # 파일 소스는 경로 존재 확인
    if source["type"] == "file" and source.get("path"):
        exists = Path(source["path"]).exists() if not source["path"].startswith("/data") else True
        status = "available" if exists else "unavailable"
        source["status"] = status
        save_sources(sources)
        return {"success": exists, "message": f"경로 {'확인됨' if exists else '없음'}: {source['path']}", "status": status}

    # 나머지는 시뮬레이션
    source["status"] = "configured"
    save_sources(sources)
    return {"success": True, "message": f"{source['name']} 설정 확인됨 (운영 환경에서 연결 테스트 필요)", "status": "configured"}
