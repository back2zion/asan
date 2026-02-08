"""
Pipeline — Landing Zone 템플릿 + CDW 조회 이력 분석 + 마트 설계 추천
"""
import json
import random
from datetime import datetime
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, HTTPException, Query

from .pipeline_shared import (
    get_conn, ensure_tables, seed_query_history,
    LandingZoneTemplate, MartDesignCreate,
    builtin_lz_templates, load_custom_templates, save_custom_templates,
)

router = APIRouter()


# ════════════════════════════════════════════════════════════
#  A. Landing Zone 템플릿
# ════════════════════════════════════════════════════════════

@router.get("/landing-zone/templates")
async def list_lz_templates():
    """Landing Zone 템플릿 전체 조회 (Built-in + Custom)"""
    builtin = builtin_lz_templates()
    custom = load_custom_templates()
    return {"templates": builtin + custom, "builtin_count": len(builtin), "custom_count": len(custom)}


@router.post("/landing-zone/templates")
async def create_lz_template(body: LandingZoneTemplate):
    """커스텀 Landing Zone 템플릿 생성"""
    custom = load_custom_templates()
    new_id = f"lz-custom-{len(custom) + 1}"
    template = {"id": new_id, **body.model_dump(), "captured_count": 0, "created_at": datetime.now().isoformat()}
    custom.append(template)
    save_custom_templates(custom)
    return {"status": "created", "template": template}


@router.put("/landing-zone/templates/{template_id}")
async def update_lz_template(template_id: str, body: LandingZoneTemplate):
    """커스텀 Landing Zone 템플릿 수정"""
    custom = load_custom_templates()
    for i, t in enumerate(custom):
        if t["id"] == template_id:
            custom[i] = {"id": template_id, **body.model_dump(), "captured_count": t.get("captured_count", 0), "created_at": t.get("created_at")}
            save_custom_templates(custom)
            return {"status": "updated", "template": custom[i]}
    raise HTTPException(status_code=404, detail=f"템플릿 '{template_id}' 없음 (Built-in 템플릿은 수정 불가)")


@router.delete("/landing-zone/templates/{template_id}")
async def delete_lz_template(template_id: str):
    """커스텀 Landing Zone 템플릿 삭제"""
    custom = load_custom_templates()
    orig_len = len(custom)
    custom = [t for t in custom if t["id"] != template_id]
    if len(custom) == orig_len:
        raise HTTPException(status_code=404, detail=f"템플릿 '{template_id}' 없음 (Built-in 템플릿은 삭제 불가)")
    save_custom_templates(custom)
    return {"status": "deleted", "id": template_id}


@router.post("/landing-zone/templates/auto-generate")
async def auto_generate_lz_templates():
    """소스 커넥터 기반 Landing Zone 템플릿 자동 생성"""
    conn = await get_conn()
    try:
        tables = await conn.fetch("""
            SELECT tablename FROM pg_tables WHERE schemaname = 'public'
            AND tablename IN ('person', 'visit_occurrence', 'condition_occurrence', 'drug_exposure',
                              'measurement', 'procedure_occurrence', 'observation', 'payer_plan_period',
                              'condition_era', 'cost', 'death', 'device_exposure', 'note', 'specimen')
        """)
        existing_builtin = {t["id"] for t in builtin_lz_templates()}
        existing_custom = {t["id"] for t in load_custom_templates()}
        table_id_map = {t["target_tables"][0]: t["id"] for t in builtin_lz_templates()}

        generated = []
        custom = load_custom_templates()
        for row in tables:
            tbl = row["tablename"]
            if tbl in table_id_map:
                continue  # built-in 이미 있음
            new_id = f"lz-auto-{tbl}"
            if new_id in existing_custom:
                continue
            cols = await conn.fetch("""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position
            """, tbl)
            schema_cols = [{"source": c["column_name"], "target": c["column_name"], "type": c["data_type"].upper(), "transform": "none"} for c in cols[:10]]
            template = {
                "id": new_id, "name": f"{tbl} Auto Landing Zone", "source_system": "OMOP CDM",
                "target_tables": [tbl],
                "cdc_config": {"capture_mode": "incremental", "watermark_column": cols[0]["column_name"] if cols else "id", "batch_interval": "6h"},
                "landing_schema": {"columns": schema_cols},
                "schedule": "0 */6 * * *",
                "validation_rules": [{"type": "not_null", "column": cols[0]["column_name"]}] if cols else [],
                "enabled": False, "captured_count": 0, "created_at": datetime.now().isoformat(),
            }
            custom.append(template)
            generated.append(template)

        save_custom_templates(custom)
        return {"status": "generated", "count": len(generated), "templates": generated}
    finally:
        await conn.close()


@router.post("/landing-zone/preview/{template_id}")
async def preview_lz_template(template_id: str):
    """OMOP 실제 데이터로 Landing Zone 미리보기"""
    all_templates = builtin_lz_templates() + load_custom_templates()
    template = next((t for t in all_templates if t["id"] == template_id), None)
    if not template:
        raise HTTPException(status_code=404, detail=f"템플릿 '{template_id}' 없음")

    target_table = template["target_tables"][0] if template["target_tables"] else None
    if not target_table:
        return {"preview": [], "total_count": 0}

    conn = await get_conn()
    try:
        count = await conn.fetchval(f"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = $1", target_table)
        rows = await conn.fetch(f"SELECT * FROM {target_table} LIMIT 10")  # noqa: S608
        preview = [dict(r) for r in rows]
        # datetime serialization
        for row in preview:
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()
                elif hasattr(v, 'isoformat'):
                    row[k] = str(v)
        return {"preview": preview, "total_count": count or 0, "template_name": template["name"]}
    finally:
        await conn.close()


# ════════════════════════════════════════════════════════════
#  B. CDW 조회 이력 분석
# ════════════════════════════════════════════════════════════

@router.get("/query-history")
async def list_query_history(page: int = Query(1, ge=1), size: int = Query(20, ge=1, le=100)):
    """CDW 조회 이력 조회 (페이지네이션)"""
    conn = await get_conn()
    try:
        await ensure_tables(conn)
        await seed_query_history(conn)
        total = await conn.fetchval("SELECT COUNT(*) FROM cdw_query_history")
        rows = await conn.fetch(
            "SELECT * FROM cdw_query_history ORDER BY execution_count DESC LIMIT $1 OFFSET $2",
            size, (page - 1) * size
        )
        return {
            "items": [dict(r) for r in rows],
            "total": total,
            "page": page,
            "size": size,
        }
    finally:
        await conn.close()


@router.post("/query-history/analyze")
async def analyze_query_history(period_days: int = Query(30, ge=1, le=365)):
    """CDW 조회 이력 분석: 테이블/조인/필터 패턴"""
    conn = await get_conn()
    try:
        await ensure_tables(conn)
        await seed_query_history(conn)

        # Hot tables: 테이블별 접근 빈도
        rows = await conn.fetch("""
            SELECT unnest(tables_accessed) AS table_name, SUM(execution_count) AS total_access
            FROM cdw_query_history
            WHERE last_seen >= NOW() - MAKE_INTERVAL(days => $1)
            GROUP BY 1 ORDER BY 2 DESC
        """, period_days)
        hot_tables = [{"table": r["table_name"], "access_count": r["total_access"]} for r in rows]

        # Hot joins: 조인 패턴별 빈도
        rows = await conn.fetch("""
            SELECT unnest(join_tables) AS join_pattern, SUM(execution_count) AS total_joins
            FROM cdw_query_history
            WHERE last_seen >= NOW() - MAKE_INTERVAL(days => $1) AND array_length(join_tables, 1) > 0
            GROUP BY 1 ORDER BY 2 DESC
        """, period_days)
        hot_joins = [{"join_pattern": r["join_pattern"], "join_count": r["total_joins"]} for r in rows]

        # Hot filters: 필터 컬럼별 빈도
        rows = await conn.fetch("""
            SELECT unnest(filter_columns) AS filter_column, SUM(execution_count) AS filter_count
            FROM cdw_query_history
            WHERE last_seen >= NOW() - MAKE_INTERVAL(days => $1) AND array_length(filter_columns, 1) > 0
            GROUP BY 1 ORDER BY 2 DESC
        """, period_days)
        hot_filters = [{"column": r["filter_column"], "filter_count": r["filter_count"]} for r in rows]

        # Summary stats
        total_queries = await conn.fetchval("SELECT COUNT(*) FROM cdw_query_history WHERE last_seen >= NOW() - MAKE_INTERVAL(days => $1)", period_days)
        total_executions = await conn.fetchval("SELECT COALESCE(SUM(execution_count), 0) FROM cdw_query_history WHERE last_seen >= NOW() - MAKE_INTERVAL(days => $1)", period_days)
        avg_time = await conn.fetchval("SELECT COALESCE(AVG(total_time_ms / GREATEST(execution_count, 1)), 0) FROM cdw_query_history WHERE last_seen >= NOW() - MAKE_INTERVAL(days => $1)", period_days)

        return {
            "period_days": period_days,
            "total_unique_queries": total_queries,
            "total_executions": total_executions,
            "avg_query_time_ms": round(float(avg_time), 1),
            "hot_tables": hot_tables,
            "hot_joins": hot_joins,
            "hot_filters": hot_filters,
        }
    finally:
        await conn.close()


@router.post("/mart-suggestions")
async def suggest_mart_designs():
    """CDW 조회 이력 기반 마트 설계 자동 추천"""
    conn = await get_conn()
    try:
        await ensure_tables(conn)
        await seed_query_history(conn)

        # 고빈도 조인 패턴 수집
        join_rows = await conn.fetch("""
            SELECT join_tables, SUM(execution_count) AS total, array_agg(DISTINCT unnest_col) AS all_columns
            FROM cdw_query_history, LATERAL unnest(columns_accessed) AS unnest_col
            WHERE array_length(join_tables, 1) > 0
            GROUP BY join_tables ORDER BY total DESC LIMIT 10
        """)

        suggestions = []
        for i, r in enumerate(join_rows):
            joins = list(r["join_tables"])
            all_tables = set()
            for j in joins:
                parts = j.split("-")
                all_tables.update(parts)
            all_tables = sorted(all_tables)
            cols = list(r["all_columns"]) if r["all_columns"] else []
            total_access = r["total"]

            # confidence score: 접근 빈도 기반
            max_access = 500
            confidence = min(total_access / max_access, 1.0)

            mart_name = f"mart_{'_'.join(t[:4] for t in all_tables)}"
            join_desc = ", ".join(joins)

            # 컬럼 정의 생성
            mart_columns = []
            for col in cols[:15]:
                mart_columns.append({"name": col, "type": "inferred", "source": "auto"})

            # 인덱스 추천
            filter_rows = await conn.fetch("""
                SELECT unnest(filter_columns) AS fc, SUM(execution_count) AS cnt
                FROM cdw_query_history
                WHERE join_tables && $1
                GROUP BY 1 ORDER BY 2 DESC LIMIT 5
            """, joins)
            indexes = [{"columns": [fr["fc"]], "type": "btree", "reason": f"필터 빈도 {fr['cnt']}회"} for fr in filter_rows]

            # Build SQL 생성
            join_clauses = []
            base_table = all_tables[0]
            for j in joins:
                parts = j.split("-")
                if len(parts) == 2:
                    t1, t2 = parts
                    if t1 == base_table:
                        join_clauses.append(f"JOIN {t2} ON {t1}.person_id = {t2}.person_id")
                    elif t2 == base_table:
                        join_clauses.append(f"JOIN {t1} ON {t2}.person_id = {t1}.person_id")
                    else:
                        join_clauses.append(f"JOIN {t2} ON {t1}.person_id = {t2}.person_id")

            col_list = ", ".join(cols[:10])
            join_sql = "\n".join(join_clauses)
            build_sql = f"CREATE TABLE {mart_name} AS\nSELECT {col_list}\nFROM {base_table}\n{join_sql};"

            suggestions.append({
                "rank": i + 1,
                "name": mart_name,
                "description": f"고빈도 조인 패턴 ({join_desc}) 기반 자동 추천 마트. 총 {total_access}회 조회.",
                "source_tables": all_tables,
                "join_patterns": joins,
                "columns": mart_columns,
                "indexes": indexes,
                "estimated_rows": random.randint(50000, 5000000),
                "confidence": round(confidence, 2),
                "total_access_count": total_access,
                "build_sql": build_sql,
            })

        return {"suggestions": suggestions, "total": len(suggestions)}
    finally:
        await conn.close()


@router.post("/mart-designs")
async def save_mart_design(body: MartDesignCreate):
    """마트 설계 저장"""
    conn = await get_conn()
    try:
        await ensure_tables(conn)
        row = await conn.fetchrow("""
            INSERT INTO pipeline_mart_design (name, description, source_tables, columns, indexes, estimated_rows, build_sql)
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, $7)
            RETURNING mart_id, name, status, created_at
        """, body.name, body.description, body.source_tables,
            json.dumps(body.columns), json.dumps(body.indexes),
            body.estimated_rows, body.build_sql)
        return {"status": "saved", "mart": dict(row)}
    finally:
        await conn.close()


@router.get("/mart-designs")
async def list_mart_designs():
    """저장된 마트 설계 조회"""
    conn = await get_conn()
    try:
        await ensure_tables(conn)
        rows = await conn.fetch("SELECT * FROM pipeline_mart_design ORDER BY created_at DESC")
        items = []
        for r in rows:
            d = dict(r)
            if isinstance(d.get("columns"), str):
                d["columns"] = json.loads(d["columns"])
            if isinstance(d.get("indexes"), str):
                d["indexes"] = json.loads(d["indexes"])
            items.append(d)
        return {"mart_designs": items, "total": len(items)}
    finally:
        await conn.close()
