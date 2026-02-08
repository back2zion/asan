"""
ETL Mapping Endpoints — /etl/mapping/target-tables, /etl/mapping/generate
"""
from typing import List, Dict, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from .etl_shared import OMOP_TARGET_SCHEMA, match_column

router = APIRouter()


class MappingGenerateRequest(BaseModel):
    source_table: str
    source_columns: List[str]
    target_table: str


def _generate_etl_code(source_table: str, target_table: str, mappings: List[Dict]) -> str:
    """매핑 기반 ETL Python 코드 자동 생성"""
    col_map_lines = []
    for m in mappings:
        col_map_lines.append(f'    "{m["source_column"]}": "{m["target_column"]}",')

    col_map_str = "\n".join(col_map_lines)

    select_cols = ", ".join(m["source_column"] for m in mappings)
    insert_cols = ", ".join(m["target_column"] for m in mappings)
    placeholders = ", ".join(f"%s" for _ in mappings)

    code = f'''"""
Auto-generated ETL: {source_table} → {target_table}
Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Coverage: {len(mappings)} columns mapped
"""
import psycopg2
from psycopg2.extras import execute_values

# 컬럼 매핑 정의
COLUMN_MAP = {{
{col_map_str}
}}

# 소스 DB 연결
src_conn = psycopg2.connect(
    host="SOURCE_HOST", port=5432,
    dbname="SOURCE_DB", user="USER", password="PASSWORD",
)

# 타겟 DB 연결 (OMOP CDM)
tgt_conn = psycopg2.connect(
    host="localhost", port=5436,
    dbname="omop_cdm", user="omopuser", password="omop",
)

BATCH_SIZE = 10000

try:
    src_cur = src_conn.cursor("etl_cursor")  # server-side cursor
    src_cur.itersize = BATCH_SIZE

    # 소스 데이터 추출
    src_cur.execute("""
        SELECT {select_cols}
        FROM {source_table}
    """)

    tgt_cur = tgt_conn.cursor()
    total = 0

    while True:
        rows = src_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        # 배치 삽입
        execute_values(
            tgt_cur,
            """INSERT INTO {target_table} ({insert_cols})
               VALUES %s
               ON CONFLICT DO NOTHING""",
            rows,
        )
        tgt_conn.commit()
        total += len(rows)
        print(f"  Loaded {{total:,}} rows...")

    print(f"ETL complete: {{total:,}} rows loaded into {target_table}")

finally:
    src_conn.close()
    tgt_conn.close()
'''
    return code


@router.get("/mapping/target-tables")
async def list_target_tables():
    """매핑 대상 OMOP CDM 타겟 테이블 목록"""
    tables = []
    for name, cols in OMOP_TARGET_SCHEMA.items():
        tables.append({
            "name": name,
            "column_count": len(cols),
            "columns": cols,
        })
    return {"tables": tables, "total": len(tables)}


@router.post("/mapping/generate")
async def generate_mapping(req: MappingGenerateRequest):
    """소스→타겟 1:1 매핑 자동 생성"""
    if req.target_table not in OMOP_TARGET_SCHEMA:
        raise HTTPException(status_code=404, detail=f"타겟 테이블 '{req.target_table}'을 찾을 수 없습니다")

    target_cols = OMOP_TARGET_SCHEMA[req.target_table]
    mappings = []
    unmapped_source = []
    unmapped_target = [tc["name"] for tc in target_cols]

    for src_col in req.source_columns:
        match = match_column(src_col, target_cols)
        if match:
            mappings.append({
                "source_column": src_col,
                "target_column": match["target_column"],
                "confidence": match["confidence"],
                "method": match["method"],
                "transform": None,
            })
            if match["target_column"] in unmapped_target:
                unmapped_target.remove(match["target_column"])
        else:
            unmapped_source.append(src_col)

    # 매핑 통계
    total_src = len(req.source_columns)
    matched = len(mappings)
    coverage = round(matched / total_src * 100, 1) if total_src else 0

    # ETL 코드 자동 생성
    etl_code = _generate_etl_code(req.source_table, req.target_table, mappings)

    return {
        "source_table": req.source_table,
        "target_table": req.target_table,
        "mappings": mappings,
        "unmapped_source": unmapped_source,
        "unmapped_target": unmapped_target,
        "stats": {
            "total_source_columns": total_src,
            "matched_columns": matched,
            "coverage_percent": coverage,
        },
        "generated_etl_code": etl_code,
    }


@router.get("/mapping/presets")
async def list_mapping_presets():
    """매핑 프리셋 — OMOP CDM 대상 테이블 기반 동적 생성"""
    presets = {}
    for name, cols in OMOP_TARGET_SCHEMA.items():
        col_names = [c["name"] for c in cols[:5]]
        presets[name] = {
            "table": name,
            "columns": ", ".join(col_names),
        }
    return {"presets": presets}
