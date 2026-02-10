"""
Source Mappings + Compliance Rules & Monitoring + Pipeline Biz Metadata
"""
import json
from typing import Optional, Dict

from fastapi import APIRouter

from .metamgmt_shared import (
    get_connection, _init,
    SourceMappingCreate, ComplianceRuleCreate, PipelineBizCreate,
)

router = APIRouter()


# ══════════════════════════════════════════════
#  3. Source <-> Integrated Metadata Mapping
# ══════════════════════════════════════════════

@router.get("/source-mappings")
async def list_source_mappings(source_system: Optional[str] = None, target_table: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if source_system:
            params.append(source_system)
            where.append(f"source_system = ${len(params)}")
        if target_table:
            params.append(target_table)
            where.append(f"target_table = ${len(params)}")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"SELECT * FROM meta_source_mapping {w} ORDER BY source_system, source_table, mapping_id", *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/source-mappings")
async def create_source_mapping(body: SourceMappingCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO meta_source_mapping (source_system, source_table, source_column, source_type,
                target_table, target_column, target_type, transform_rule, description)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING mapping_id
        """, body.source_system, body.source_table, body.source_column, body.source_type,
            body.target_table, body.target_column, body.target_type, body.transform_rule, body.description)
        return {"mapping_id": row["mapping_id"]}
    finally:
        await conn.close()


@router.delete("/source-mappings/{mapping_id}")
async def delete_source_mapping(mapping_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("DELETE FROM meta_source_mapping WHERE mapping_id=$1", mapping_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/source-mappings/overview")
async def source_mapping_overview():
    """원천<->통합 매핑 전체 현황"""
    conn = await get_connection()
    try:
        await _init(conn)
        total = await conn.fetchval("SELECT COUNT(*) FROM meta_source_mapping")
        by_system = await conn.fetch("""
            SELECT source_system, COUNT(*) cnt, COUNT(DISTINCT source_table) AS src_tables,
                   COUNT(DISTINCT target_table) AS tgt_tables
            FROM meta_source_mapping GROUP BY source_system ORDER BY source_system
        """)
        by_target = await conn.fetch("""
            SELECT target_table, COUNT(*) cnt, array_agg(DISTINCT source_system) systems
            FROM meta_source_mapping GROUP BY target_table ORDER BY target_table
        """)
        return {
            "total_mappings": total,
            "by_source_system": [dict(r) for r in by_system],
            "by_target_table": [dict(r) for r in by_target],
        }
    finally:
        await conn.close()


# ══════════════════════════════════════════════
#  4. Compliance Rules & Monitoring
# ══════════════════════════════════════════════

@router.get("/compliance-rules")
async def list_compliance_rules(category: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if category:
            params.append(category)
            where.append(f"cr.category = ${len(params)}")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"""
            SELECT cr.*, cs.result AS last_result, cs.actual_value AS last_actual, cs.detail AS last_detail,
                   cs.check_time AS last_check_time
            FROM meta_compliance_rule cr
            LEFT JOIN LATERAL (
                SELECT * FROM meta_compliance_status WHERE rule_id = cr.rule_id ORDER BY check_time DESC LIMIT 1
            ) cs ON TRUE
            {w} ORDER BY cr.severity DESC, cr.rule_id
        """, *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/compliance-rules")
async def create_compliance_rule(body: ComplianceRuleCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO meta_compliance_rule (rule_name, regulation, category, description, check_query, threshold, severity, enabled)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING rule_id
        """, body.rule_name, body.regulation, body.category, body.description,
            body.check_query, body.threshold, body.severity, body.enabled)
        return {"rule_id": row["rule_id"]}
    finally:
        await conn.close()


@router.delete("/compliance-rules/{rule_id}")
async def delete_compliance_rule(rule_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("DELETE FROM meta_compliance_rule WHERE rule_id=$1", rule_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/compliance-dashboard")
async def compliance_dashboard():
    """규정 준수 대시보드"""
    conn = await get_connection()
    try:
        await _init(conn)
        rules = await conn.fetch("""
            SELECT cr.*, cs.result, cs.actual_value, cs.detail, cs.check_time
            FROM meta_compliance_rule cr
            LEFT JOIN LATERAL (
                SELECT * FROM meta_compliance_status WHERE rule_id = cr.rule_id ORDER BY check_time DESC LIMIT 1
            ) cs ON TRUE
            WHERE cr.enabled = TRUE
            ORDER BY cr.rule_id
        """)
        total = len(rules)
        passed = sum(1 for r in rules if r["result"] == "pass")
        warnings = sum(1 for r in rules if r["result"] == "warning")
        failed = sum(1 for r in rules if r["result"] in ("fail", "error", None))

        by_category: Dict[str, Dict[str, int]] = {}
        for r in rules:
            cat = r["category"]
            if cat not in by_category:
                by_category[cat] = {"total": 0, "pass": 0, "warning": 0, "fail": 0}
            by_category[cat]["total"] += 1
            if r["result"] == "pass":
                by_category[cat]["pass"] += 1
            elif r["result"] == "warning":
                by_category[cat]["warning"] += 1
            else:
                by_category[cat]["fail"] += 1

        return {
            "total": total, "passed": passed, "warnings": warnings, "failed": failed,
            "compliance_rate": round(passed / max(total, 1) * 100, 1),
            "by_category": by_category,
            "rules": [dict(r) for r in rules],
        }
    finally:
        await conn.close()


# ══════════════════════════════════════════════
#  5. Pipeline Biz Metadata Status
# ══════════════════════════════════════════════

@router.get("/pipeline-biz")
async def list_pipeline_biz(job_type: Optional[str] = None, last_status: Optional[str] = None):
    conn = await get_connection()
    try:
        await _init(conn)
        where, params = [], []
        if job_type:
            params.append(job_type)
            where.append(f"job_type = ${len(params)}")
        if last_status:
            params.append(last_status)
            where.append(f"last_status = ${len(params)}")
        w = f"WHERE {' AND '.join(where)}" if where else ""
        rows = await conn.fetch(f"SELECT * FROM meta_pipeline_biz {w} ORDER BY link_id", *params)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/pipeline-biz")
async def create_pipeline_biz(body: PipelineBizCreate):
    conn = await get_connection()
    try:
        await _init(conn)
        row = await conn.fetchrow("""
            INSERT INTO meta_pipeline_biz (pipeline_name, job_type, biz_metadata_ids, source_tables,
                target_table, schedule, owner, description)
            VALUES ($1,$2,$3::jsonb,$4::jsonb,$5,$6,$7,$8) RETURNING link_id
        """, body.pipeline_name, body.job_type, json.dumps(body.biz_metadata_ids),
            json.dumps(body.source_tables), body.target_table, body.schedule,
            body.owner, body.description)
        return {"link_id": row["link_id"]}
    finally:
        await conn.close()


@router.delete("/pipeline-biz/{link_id}")
async def delete_pipeline_biz(link_id: int):
    conn = await get_connection()
    try:
        await _init(conn)
        await conn.execute("DELETE FROM meta_pipeline_biz WHERE link_id=$1", link_id)
        return {"ok": True}
    finally:
        await conn.close()


@router.get("/pipeline-biz/dashboard")
async def pipeline_biz_dashboard():
    """Biz 메타데이터 적용 Pipeline 대시보드"""
    conn = await get_connection()
    try:
        await _init(conn)
        rows = await conn.fetch("SELECT * FROM meta_pipeline_biz ORDER BY link_id")
        total = len(rows)
        running = sum(1 for r in rows if r["last_status"] == "running")
        success = sum(1 for r in rows if r["last_status"] == "success")
        idle = sum(1 for r in rows if r["last_status"] == "idle")
        failed = sum(1 for r in rows if r["last_status"] == "failed")
        total_rows = sum(r["row_count"] for r in rows)

        # Biz metadata coverage
        all_biz = set()
        for r in rows:
            biz = r["biz_metadata_ids"] if isinstance(r["biz_metadata_ids"], list) else json.loads(r["biz_metadata_ids"])
            all_biz.update(biz)

        by_type: Dict[str, int] = {}
        for r in rows:
            t = r["job_type"]
            if t not in by_type:
                by_type[t] = 0
            by_type[t] += 1

        return {
            "total_pipelines": total, "running": running, "success": success,
            "idle": idle, "failed": failed,
            "total_rows_processed": total_rows,
            "unique_biz_metadata": len(all_biz),
            "biz_metadata_list": sorted(all_biz),
            "by_job_type": by_type,
            "pipelines": [dict(r) for r in rows],
        }
    finally:
        await conn.close()
