"""
거버넌스 - 민감도 분류, 메타데이터, 리니지 상세, Auto-Tagging, Usage Lineage
"""
import re
from typing import List, Dict
from fastapi import APIRouter

from routers.gov_shared import (
    get_connection, classify_column,
    SensitivityUpdate, TableMetadataUpdate, ColumnMetadataUpdate, SuggestTagsRequest,
    DOMAIN_TAG_DEFAULTS,
    _fetch_airflow_dag, _parse_usage_data,
)
from services.redis_cache import cached

router = APIRouter()


# ── 민감도 ──

async def _ensure_sensitivity_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS sensitivity_override (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            column_name VARCHAR(100) NOT NULL,
            override_level VARCHAR(20) NOT NULL,
            reason VARCHAR(200),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(table_name, column_name)
        )
    """)


@router.get("/sensitivity")
@cached("gov-sensitivity", ttl=300)
async def get_sensitivity():
    """민감도 분류 현황 - information_schema 기반 + override 적용"""
    conn = await get_connection()
    try:
        await _ensure_sensitivity_table(conn)

        rows = await conn.fetch("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)

        overrides = await conn.fetch("SELECT table_name, column_name, override_level FROM sensitivity_override")
        override_map = {f"{r['table_name']}.{r['column_name']}": r["override_level"] for r in overrides}

        groups = {"극비": [], "민감": [], "일반": []}
        for r in rows:
            full = f"{r['table_name']}.{r['column_name']}"
            level = override_map.get(full, classify_column(r["table_name"], r["column_name"]))
            groups[level].append(full)

        colors = {"극비": "red", "민감": "orange", "일반": "green"}
        result = []
        for level in ["극비", "민감", "일반"]:
            cols = groups[level]
            result.append({
                "level": level,
                "color": colors[level],
                "count": len(cols),
                "columns": cols,
            })
        return result
    finally:
        await conn.close()


@router.put("/sensitivity")
async def update_sensitivity(body: SensitivityUpdate):
    """컬럼 민감도 등급 수동 변경 (UPSERT)"""
    conn = await get_connection()
    try:
        await _ensure_sensitivity_table(conn)
        await conn.execute("""
            INSERT INTO sensitivity_override (table_name, column_name, override_level, reason, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (table_name, column_name)
            DO UPDATE SET override_level = $3, reason = $4, updated_at = NOW()
        """, body.table_name, body.column_name, body.level, body.reason)
        return {"success": True, "message": f"{body.table_name}.{body.column_name} → {body.level}"}
    finally:
        await conn.close()


# ── 메타데이터 ──

async def _ensure_metadata_tables(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS metadata_override (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL UNIQUE,
            business_name VARCHAR(200),
            description VARCHAR(500),
            domain VARCHAR(50),
            tags TEXT[],
            owner VARCHAR(100),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS column_metadata_override (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(100) NOT NULL,
            column_name VARCHAR(100) NOT NULL,
            business_name VARCHAR(200),
            description VARCHAR(500),
            updated_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(table_name, column_name)
        )
    """)


@router.get("/metadata")
async def get_metadata_overrides():
    """메타데이터 override 목록 (테이블 + 컬럼)"""
    conn = await get_connection()
    try:
        await _ensure_metadata_tables(conn)
        table_rows = await conn.fetch("SELECT * FROM metadata_override ORDER BY table_name")
        col_rows = await conn.fetch("SELECT * FROM column_metadata_override ORDER BY table_name, column_name")
        return {
            "tables": [
                {
                    "table_name": r["table_name"],
                    "business_name": r["business_name"],
                    "description": r["description"],
                    "domain": r["domain"],
                    "tags": r["tags"] or [],
                    "owner": r["owner"],
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                }
                for r in table_rows
            ],
            "columns": [
                {
                    "table_name": r["table_name"],
                    "column_name": r["column_name"],
                    "business_name": r["business_name"],
                    "description": r["description"],
                    "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
                }
                for r in col_rows
            ],
        }
    finally:
        await conn.close()


@router.put("/metadata/table")
async def update_table_metadata(body: TableMetadataUpdate):
    """테이블 메타데이터 수정 (비즈니스명, 설명, 도메인, 태그, 담당자)"""
    conn = await get_connection()
    try:
        await _ensure_metadata_tables(conn)
        await conn.execute("""
            INSERT INTO metadata_override (table_name, business_name, description, domain, tags, owner, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (table_name)
            DO UPDATE SET
                business_name = COALESCE($2, metadata_override.business_name),
                description = COALESCE($3, metadata_override.description),
                domain = COALESCE($4, metadata_override.domain),
                tags = COALESCE($5, metadata_override.tags),
                owner = COALESCE($6, metadata_override.owner),
                updated_at = NOW()
        """, body.table_name, body.business_name, body.description, body.domain, body.tags, body.owner)
        return {"success": True, "message": f"{body.table_name} 메타데이터 수정 완료"}
    finally:
        await conn.close()


@router.put("/metadata/column")
async def update_column_metadata(body: ColumnMetadataUpdate):
    """컬럼 메타데이터 수정 (비즈니스명, 설명)"""
    conn = await get_connection()
    try:
        await _ensure_metadata_tables(conn)
        await conn.execute("""
            INSERT INTO column_metadata_override (table_name, column_name, business_name, description, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (table_name, column_name)
            DO UPDATE SET
                business_name = COALESCE($3, column_metadata_override.business_name),
                description = COALESCE($4, column_metadata_override.description),
                updated_at = NOW()
        """, body.table_name, body.column_name, body.business_name, body.description)
        return {"success": True, "message": f"{body.table_name}.{body.column_name} 메타데이터 수정 완료"}
    finally:
        await conn.close()


# ── 리니지 상세 ──

_lineage_detail_cache: dict = {"data": None, "ts": 0}

@router.get("/lineage-detail/{node_id}")
async def get_lineage_detail(node_id: str):
    """리니지 노드 상세 - 실제 DB row count + Airflow 상태 (5분 캐시)"""
    import time as _time
    now = _time.time()
    if _lineage_detail_cache["data"] and now - _lineage_detail_cache["ts"] < 300:
        cached = _lineage_detail_cache["data"]
    else:
        conn = await get_connection()
        try:
            row_counts = await conn.fetch("""
                SELECT relname AS table_name, n_live_tup AS row_count
                FROM pg_stat_user_tables
                WHERE schemaname = 'public'
            """)
            count_map = {r["table_name"]: r["row_count"] for r in row_counts}
            total_rows = sum(count_map.values())

            latest_date = await conn.fetchval("""
                SELECT MAX(measurement_date) FROM measurement LIMIT 1
            """)
            latest_str = latest_date.strftime("%Y-%m-%d %H:%M:%S") if latest_date else "N/A"

            quality_score = 99.7  # pre-computed: condition_occurrence 3컬럼 완성도
            cached = {"count_map": count_map, "total_rows": total_rows, "latest_str": latest_str, "quality_score": quality_score}
            _lineage_detail_cache["data"] = cached
            _lineage_detail_cache["ts"] = now
        finally:
            await conn.close()

    count_map = cached["count_map"]
    total_rows = cached["total_rows"]
    latest_str = cached["latest_str"]
    quality_score = cached["quality_score"]

    base_info = {
        "source": {
            "schedule": "실시간 (CDC)",
            "format": "Oracle / EMR",
            "sla": "< 5초",
            "owner": "의료정보실",
            "retention": "영구",
        },
        "cdc": {
            "schedule": "실시간",
            "format": "Kafka Avro",
            "sla": "< 10초",
            "owner": "빅데이터연구센터",
            "retention": "7일 (토픽)",
        },
        "bronze": {
            "schedule": "실시간 적재",
            "format": "Delta Lake (Parquet)",
            "sla": "< 30초",
            "owner": "융합연구지원센터",
            "retention": "1년",
        },
        "etl": {
            "schedule": "매일 02:00 KST",
            "format": "Spark SQL",
            "sla": "< 2시간",
            "owner": "의공학연구소",
            "retention": "-",
        },
        "silver": {
            "schedule": "매일 04:00 KST",
            "format": "Delta Lake (Parquet)",
            "sla": "< 4시간",
            "owner": "융합연구지원센터",
            "retention": "3년",
        },
        "gold": {
            "schedule": "매일 05:00 KST",
            "format": "Delta Lake (Parquet)",
            "sla": "< 6시간",
            "owner": "임상의학연구소",
            "retention": "5년",
        },
    }

    info = base_info.get(node_id, base_info["source"])

    airflow_info = {}
    if node_id in ("cdc", "etl"):
        dag_map = {"cdc": "omop_cdc_ingest", "etl": "omop_etl_transform"}
        airflow_info = await _fetch_airflow_dag(dag_map.get(node_id, ""))

    last_run = airflow_info.get("lastRun", latest_str)

    return {
        "schedule": info["schedule"],
        "lastRun": last_run,
        "rowCount": total_rows,
        "format": info["format"],
        "sla": info["sla"],
        "owner": info["owner"],
        "qualityScore": quality_score,
        "retention": info["retention"],
        "tableCount": len(count_map),
        "tables": count_map,
    }


# ── Auto-Tagging ──

@router.post("/suggest-tags")
async def suggest_tags(body: SuggestTagsRequest):
    """AI 기반 태그 자동 추천 - LLM 분석 + 도메인 규칙 폴백"""
    from routers.semantic import SAMPLE_TABLES, TAGS

    target = None
    for t in SAMPLE_TABLES:
        if t["physical_name"].lower() == body.table_name.lower():
            target = t
            break

    if not target:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"테이블 '{body.table_name}'를 찾을 수 없습니다")

    table_desc = (
        f"테이블: {target['physical_name']} ({target['business_name']})\n"
        f"도메인: {target['domain']}\n"
        f"설명: {target['description']}\n"
        f"컬럼: {', '.join(c['physical_name'] + '(' + c['business_name'] + ')' for c in target['columns'][:10])}\n"
        f"기존태그: {', '.join(target.get('tags', []))}"
    )

    recommended_tags: List[str] = []
    new_tags: List[str] = []
    recommended_domain = target["domain"]
    reasoning = ""
    source = "rule"

    try:
        from core.config import settings
        import httpx

        prompt = f"""다음 OMOP CDM 테이블 메타데이터를 분석하여 비즈니스 태그를 추천하세요.

{table_desc}

사용 가능한 기존 태그 목록: {', '.join(TAGS)}

다음 JSON 형식으로 응답하세요 (JSON만, 다른 텍스트 없이):
{{
  "recommended_tags": ["기존 태그 중 적합한 것 3~5개"],
  "new_tags": ["새로 제안하는 태그 0~2개 (한글)"],
  "recommended_domain": "가장 적합한 도메인",
  "reasoning": "추천 이유 (1~2문장)"
}}"""

        llm_base = settings.LLM_API_URL.rstrip("/")
        if llm_base.endswith("/v1"):
            llm_url = f"{llm_base}/chat/completions"
        else:
            llm_url = f"{llm_base}/v1/chat/completions"

        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.post(
                llm_url,
                json={
                    "model": settings.LLM_MODEL,
                    "messages": [
                        {"role": "system", "content": "/no_think"},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": 0.3,
                    "max_tokens": 500,
                },
            )
            if resp.status_code == 200:
                content = resp.json()["choices"][0]["message"]["content"]
                import json
                json_match = re.search(r'\{[\s\S]*\}', content)
                if json_match:
                    result = json.loads(json_match.group())
                    recommended_tags = result.get("recommended_tags", [])
                    new_tags = result.get("new_tags", [])
                    recommended_domain = result.get("recommended_domain", target["domain"])
                    reasoning = result.get("reasoning", "")
                    source = "llm"
    except Exception:
        pass

    if not recommended_tags:
        domain_defaults = DOMAIN_TAG_DEFAULTS.get(target["domain"], [])
        recommended_tags = list(dict.fromkeys(["OMOP", "CDM"] + domain_defaults))
        col_names = " ".join(c["physical_name"] for c in target["columns"])
        if "person_id" in col_names:
            recommended_tags.append("환자연계")
        if "source_value" in col_names:
            recommended_tags.append("원본코드")
        if "concept_id" in col_names:
            recommended_tags.append("SNOMED")
        recommended_tags = list(dict.fromkeys(recommended_tags))[:8]
        reasoning = f"도메인 '{target['domain']}' 기반 규칙 및 컬럼 패턴 분석 결과"
        source = "rule"

    return {
        "success": True,
        "table_name": target["physical_name"],
        "recommended_tags": recommended_tags,
        "recommended_domain": recommended_domain,
        "new_tags": new_tags,
        "reasoning": reasoning,
        "source": source,
    }


# ── Usage Lineage ──

@router.get("/usage-lineage")
async def get_usage_lineage():
    """쿼리 로그 기반 활용 리니지 (전체)"""
    data = _parse_usage_data()
    return {"success": True, **data}


@router.get("/usage-lineage/{table_name}")
async def get_usage_lineage_for_table(table_name: str):
    """특정 테이블의 활용 리니지"""
    data = _parse_usage_data()
    table_lower = table_name.lower()

    related_edges = [
        e for e in data["co_occurrence_edges"]
        if e["source"] == table_lower or e["target"] == table_lower
    ]

    related_tables: Dict[str, int] = {}
    for edge in related_edges:
        other = edge["target"] if edge["source"] == table_lower else edge["source"]
        related_tables[other] = edge["weight"]

    return {
        "success": True,
        "table_name": table_name,
        "query_count": data["table_frequency"].get(table_lower, 0),
        "related_tables": dict(sorted(related_tables.items(), key=lambda x: -x[1])),
        "co_occurrence_edges": related_edges,
        "total_queries": data["total_queries"],
        "analysis_period": data["analysis_period"],
    }
