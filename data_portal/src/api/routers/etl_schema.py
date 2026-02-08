"""
ETL Schema Versioning Endpoints — /etl/schema/*
"""
import uuid
from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .etl_shared import (
    OMOP_TARGET_SCHEMA,
    COLUMN_SYNONYMS,
    load_sources,
    save_sources,
    load_parallel_config,
    load_schema_snapshots,
    save_schema_snapshots,
    load_diff_history,
    save_diff_history,
    discover_pg_schema,
    simulate_schema,
)

router = APIRouter()


# =====================================================================
#  Pydantic models
# =====================================================================

class SchemaDiscoverRequest(BaseModel):
    source_id: str


class SchemaSnapshotRequest(BaseModel):
    source_id: str
    label: Optional[str] = None


class SchemaCompareRequest(BaseModel):
    snapshot_id_a: str
    snapshot_id_b: str


class ImpactAnalysisRequest(BaseModel):
    source_id: str
    diffs: List[Dict[str, Any]]


# =====================================================================
#  Internal helpers
# =====================================================================

def _compute_schema_diff(old_tables: List[Dict], new_tables: List[Dict]) -> List[Dict]:
    """두 스키마 간 diff 계산"""
    old_map = {t["table_name"]: t for t in old_tables}
    new_map = {t["table_name"]: t for t in new_tables}
    diffs = []

    for tname in new_map:
        if tname not in old_map:
            diffs.append({"type": "table_added", "table": tname, "detail": f"테이블 추가됨 ({new_map[tname]['column_count']} 컬럼)"})
    for tname in old_map:
        if tname not in new_map:
            diffs.append({"type": "table_removed", "table": tname, "detail": f"테이블 삭제됨"})
    for tname in old_map:
        if tname in new_map:
            old_cols = {c["name"]: c for c in old_map[tname]["columns"]}
            new_cols = {c["name"]: c for c in new_map[tname]["columns"]}
            for cname in new_cols:
                if cname not in old_cols:
                    diffs.append({"type": "column_added", "table": tname, "column": cname,
                                  "detail": f"컬럼 추가됨: {new_cols[cname]['type']}"})
            for cname in old_cols:
                if cname not in new_cols:
                    diffs.append({"type": "column_removed", "table": tname, "column": cname,
                                  "detail": f"컬럼 삭제됨: {old_cols[cname]['type']}"})
            for cname in old_cols:
                if cname in new_cols:
                    if old_cols[cname]["type"] != new_cols[cname]["type"]:
                        diffs.append({
                            "type": "column_type_changed", "table": tname, "column": cname,
                            "old_type": old_cols[cname]["type"], "new_type": new_cols[cname]["type"],
                            "detail": f"타입 변경: {old_cols[cname]['type']} → {new_cols[cname]['type']}",
                        })
    return diffs


# =====================================================================
#  Endpoints
# =====================================================================

@router.post("/schema/discover")
async def discover_schema(req: SchemaDiscoverRequest):
    """소스의 스키마 자동 탐색"""
    sources = load_sources()
    source = next((s for s in sources if s["id"] == req.source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다")

    if source.get("subtype") == "postgresql":
        try:
            tables = await discover_pg_schema(
                host=source.get("host", "localhost"),
                port=source.get("port", 5432),
                user=source.get("username", "omopuser"),
                password=source.get("password", "omop"),
                database=source.get("database", "omop_cdm"),
            )
            source["tables"] = len(tables)
            source["last_sync"] = datetime.now().isoformat()
            save_sources(sources)
            return {
                "success": True,
                "source_id": req.source_id,
                "source_name": source["name"],
                "tables": tables,
                "total_tables": len(tables),
                "total_columns": sum(t["column_count"] for t in tables),
                "total_rows": sum(t["row_count"] for t in tables),
                "discovered_at": datetime.now().isoformat(),
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"스키마 탐색 실패: {str(e)}")
    else:
        tables = simulate_schema(source)
        return {
            "success": True,
            "source_id": req.source_id,
            "source_name": source["name"],
            "tables": tables,
            "total_tables": len(tables),
            "total_columns": sum(t["column_count"] for t in tables),
            "total_rows": sum(t["row_count"] for t in tables),
            "discovered_at": datetime.now().isoformat(),
            "simulated": True,
        }


@router.post("/schema/snapshot")
async def create_schema_snapshot(req: SchemaSnapshotRequest):
    """현재 스키마를 버전 스냅샷으로 저장"""
    sources = load_sources()
    source = next((s for s in sources if s["id"] == req.source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다")

    if source.get("subtype") == "postgresql":
        try:
            tables = await discover_pg_schema(
                host=source.get("host", "localhost"),
                port=source.get("port", 5432),
                user=source.get("username", "omopuser"),
                password=source.get("password", "omop"),
                database=source.get("database", "omop_cdm"),
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"스키마 탐색 실패: {str(e)}")
    else:
        tables = simulate_schema(source)

    snapshots = load_schema_snapshots()
    existing = [s for s in snapshots if s["source_id"] == req.source_id]
    version = len(existing) + 1

    snapshot = {
        "id": str(uuid.uuid4())[:8],
        "source_id": req.source_id,
        "source_name": source["name"],
        "version": version,
        "label": req.label or f"v{version}",
        "tables": tables,
        "total_tables": len(tables),
        "total_columns": sum(t["column_count"] for t in tables),
        "created_at": datetime.now().isoformat(),
        "is_baseline": version == 1,
    }
    snapshots.append(snapshot)
    save_schema_snapshots(snapshots)

    return {"success": True, "snapshot": {k: v for k, v in snapshot.items() if k != "tables"},
            "message": f"스냅샷 v{version} 저장됨 ({len(tables)} 테이블)"}


@router.get("/schema/snapshots")
async def list_schema_snapshots(source_id: Optional[str] = None):
    """스냅샷 목록 조회"""
    snapshots = load_schema_snapshots()
    if source_id:
        snapshots = [s for s in snapshots if s["source_id"] == source_id]
    summary = [{k: v for k, v in s.items() if k != "tables"} for s in snapshots]
    return {"snapshots": summary, "total": len(summary)}


@router.get("/schema/snapshots/{snapshot_id}")
async def get_schema_snapshot(snapshot_id: str):
    """스냅샷 상세 조회"""
    snapshots = load_schema_snapshots()
    snapshot = next((s for s in snapshots if s["id"] == snapshot_id), None)
    if not snapshot:
        raise HTTPException(status_code=404, detail="스냅샷을 찾을 수 없습니다")
    return snapshot


@router.post("/schema/detect-changes/{source_id}")
async def detect_schema_changes(source_id: str):
    """최신 스냅샷과 현재 스키마 비교하여 변경 감지"""
    sources = load_sources()
    source = next((s for s in sources if s["id"] == source_id), None)
    if not source:
        raise HTTPException(status_code=404, detail="소스를 찾을 수 없습니다")

    snapshots = load_schema_snapshots()
    source_snaps = [s for s in snapshots if s["source_id"] == source_id]
    if not source_snaps:
        raise HTTPException(status_code=404, detail="이전 스냅샷이 없습니다. 먼저 스냅샷을 저장하세요.")

    latest = max(source_snaps, key=lambda s: s["version"])

    if source.get("subtype") == "postgresql":
        try:
            current_tables = await discover_pg_schema(
                host=source.get("host", "localhost"),
                port=source.get("port", 5432),
                user=source.get("username", "omopuser"),
                password=source.get("password", "omop"),
                database=source.get("database", "omop_cdm"),
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"스키마 탐색 실패: {str(e)}")
    else:
        current_tables = simulate_schema(source)

    diffs = _compute_schema_diff(latest["tables"], current_tables)

    if diffs:
        history = load_diff_history()
        entry = {
            "id": str(uuid.uuid4())[:8],
            "source_id": source_id,
            "source_name": source["name"],
            "baseline_snapshot": latest["id"],
            "baseline_version": latest["version"],
            "diffs": diffs,
            "detected_at": datetime.now().isoformat(),
        }
        history.append(entry)
        save_diff_history(history)

    return {
        "source_id": source_id,
        "baseline_version": latest["version"],
        "baseline_snapshot_id": latest["id"],
        "diffs": diffs,
        "total_changes": len(diffs),
        "has_changes": len(diffs) > 0,
        "detected_at": datetime.now().isoformat(),
    }


@router.post("/schema/compare")
async def compare_snapshots(req: SchemaCompareRequest):
    """두 스냅샷 간 비교"""
    snapshots = load_schema_snapshots()
    snap_a = next((s for s in snapshots if s["id"] == req.snapshot_id_a), None)
    snap_b = next((s for s in snapshots if s["id"] == req.snapshot_id_b), None)
    if not snap_a or not snap_b:
        raise HTTPException(status_code=404, detail="스냅샷을 찾을 수 없습니다")

    diffs = _compute_schema_diff(snap_a["tables"], snap_b["tables"])
    return {
        "snapshot_a": {"id": snap_a["id"], "version": snap_a["version"], "source": snap_a["source_name"]},
        "snapshot_b": {"id": snap_b["id"], "version": snap_b["version"], "source": snap_b["source_name"]},
        "diffs": diffs,
        "total_changes": len(diffs),
    }


@router.get("/schema/diff-history")
async def get_diff_history(source_id: Optional[str] = None):
    """변경 이력 조회"""
    history = load_diff_history()
    if source_id:
        history = [h for h in history if h["source_id"] == source_id]
    return {"history": history, "total": len(history)}


@router.post("/schema/impact-analysis")
async def schema_impact_analysis(req: ImpactAnalysisRequest):
    """diff 기반 영향도 분석"""
    impacts = []
    for diff in req.diffs:
        diff_type = diff.get("type", "")
        table = diff.get("table", "")
        column = diff.get("column", "")

        # 영향 받는 매핑 규칙 검사
        affected_mappings = []
        for target_table, target_cols in OMOP_TARGET_SCHEMA.items():
            target_col_names = [c["name"] for c in target_cols]
            if column in target_col_names or table == target_table:
                affected_mappings.append(target_table)
            for syn_target, syn_list in COLUMN_SYNONYMS.items():
                if column.lower() in [s.lower() for s in syn_list]:
                    if syn_target in target_col_names:
                        affected_mappings.append(target_table)

        # 영향 받는 병렬 적재 설정
        parallel_config = load_parallel_config()
        affected_parallel = []
        if table in parallel_config.get("tables", {}):
            affected_parallel.append(table)

        # 위험도 평가
        if diff_type in ("table_removed", "column_removed"):
            is_pk = diff.get("is_pk", False)
            if is_pk or diff_type == "table_removed":
                risk = "high"
            else:
                risk = "medium"
        elif diff_type == "column_type_changed":
            risk = "medium"
        else:
            risk = "low"

        # 권장 조치
        recommendations = []
        if diff_type == "table_removed":
            recommendations.append("관련 ETL 파이프라인 비활성화 필요")
            recommendations.append("매핑 규칙 재설정 필요")
        elif diff_type == "column_removed":
            recommendations.append("매핑에서 해당 컬럼 제거 필요")
        elif diff_type == "column_type_changed":
            recommendations.append("타입 변환 로직 검토 필요")
            recommendations.append("데이터 정합성 검증 권장")
        elif diff_type == "table_added":
            recommendations.append("신규 테이블 매핑 설정 권장")
        elif diff_type == "column_added":
            recommendations.append("추가 매핑 설정 검토 권장")

        impacts.append({
            "diff": diff,
            "risk": risk,
            "affected_mappings": list(set(affected_mappings)),
            "affected_parallel_configs": affected_parallel,
            "recommendations": recommendations,
        })

    risk_counts = {"high": 0, "medium": 0, "low": 0}
    for imp in impacts:
        risk_counts[imp["risk"]] = risk_counts.get(imp["risk"], 0) + 1

    return {
        "source_id": req.source_id,
        "impacts": impacts,
        "summary": {
            "total_impacts": len(impacts),
            "risk_counts": risk_counts,
            "affected_mapping_tables": list(set(
                t for imp in impacts for t in imp["affected_mappings"]
            )),
        },
        "analyzed_at": datetime.now().isoformat(),
    }


@router.post("/schema/rollback/{snapshot_id}")
async def rollback_to_snapshot(snapshot_id: str):
    """특정 스냅샷을 기준선으로 설정"""
    snapshots = load_schema_snapshots()
    target_snap = next((s for s in snapshots if s["id"] == snapshot_id), None)
    if not target_snap:
        raise HTTPException(status_code=404, detail="스냅샷을 찾을 수 없습니다")

    for s in snapshots:
        if s["source_id"] == target_snap["source_id"]:
            s["is_baseline"] = s["id"] == snapshot_id
    save_schema_snapshots(snapshots)

    return {
        "success": True,
        "message": f"스냅샷 v{target_snap['version']}이 기준선으로 설정됨",
        "snapshot_id": snapshot_id,
        "version": target_snap["version"],
    }
