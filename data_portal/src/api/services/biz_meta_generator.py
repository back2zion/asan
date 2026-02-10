"""
IT메타 기반 비즈메타 자동 생성 서비스

1. information_schema에서 IT메타(테이블·컬럼 물리 정보) 추출
2. LLM에 IT메타를 전달하여 한글 업무 의미(비즈메타) 자동 추론
3. 결과를 메모리 + DB에 캐시하여 재사용
"""
import asyncio
import json
import logging
import os
import re
import time
from typing import Dict, List, Any, Optional

import httpx

logger = logging.getLogger(__name__)

OMOP_CONTAINER = os.getenv("OMOP_CONTAINER", "infra-omop-db-1")
OMOP_USER = os.getenv("OMOP_USER", "omopuser")
OMOP_DB = os.getenv("OMOP_DB", "omop_cdm")

TARGET_TABLES = [
    "person", "condition_occurrence", "visit_occurrence",
    "drug_exposure", "measurement", "observation", "imaging_study",
]

# ── 캐시 ──
_it_meta_cache: Dict[str, dict] = {}
_biz_meta_cache: Dict[str, dict] = {}
_cache_ready = False
_cache_generated_at: float = 0

# ── 폴백 (LLM 실패 시) ──
_FALLBACK_BIZ_META: Dict[str, dict] = {
    "person": {
        "business_name": "환자 기본정보",
        "description": "환자 인구통계 (성별, 출생년도 등)",
        "key_columns": {
            "person_id": "환자 고유 ID",
            "gender_source_value": "성별 (M=남성, F=여성)",
            "year_of_birth": "출생연도 (나이 산출 기준)",
        },
    },
    "condition_occurrence": {
        "business_name": "진단 이력",
        "description": "환자 진단(질병) 발생 기록",
        "key_columns": {
            "condition_occurrence_id": "진단 기록 ID",
            "person_id": "환자 ID",
            "condition_source_value": "SNOMED CT 진단코드",
            "condition_start_date": "진단 시작일",
        },
    },
    "visit_occurrence": {
        "business_name": "방문 이력",
        "description": "환자 병원 방문 기록 (입원/외래/응급)",
        "key_columns": {
            "visit_occurrence_id": "방문 기록 ID",
            "person_id": "환자 ID",
            "visit_concept_id": "방문유형 (9201=입원, 9202=외래, 9203=응급)",
            "visit_start_date": "방문 시작일",
        },
    },
    "drug_exposure": {
        "business_name": "약물 처방",
        "description": "환자 약물 처방·투약 기록",
        "key_columns": {
            "drug_exposure_id": "처방 기록 ID",
            "person_id": "환자 ID",
            "drug_source_value": "약물 코드",
            "days_supply": "처방 일수",
        },
    },
    "measurement": {
        "business_name": "검사 결과",
        "description": "임상 검사(혈액, 생체 등) 측정 결과",
        "key_columns": {
            "measurement_id": "검사 기록 ID",
            "person_id": "환자 ID",
            "measurement_source_value": "검사항목 코드",
            "value_as_number": "측정 수치",
        },
    },
    "observation": {
        "business_name": "관찰 기록",
        "description": "환자 관찰 소견 기록",
        "key_columns": {
            "observation_id": "관찰 기록 ID",
            "person_id": "환자 ID",
            "observation_source_value": "관찰항목 코드",
        },
    },
    "imaging_study": {
        "business_name": "흉부 X-ray 영상",
        "description": "흉부 X-ray 영상 및 판독 소견",
        "key_columns": {
            "imaging_study_id": "영상 기록 ID",
            "person_id": "환자 ID",
            "finding_labels": "판독 소견 (Pneumonia, Cardiomegaly 등)",
            "view_position": "촬영 방향 (AP=전후, PA=후전)",
            "patient_age": "환자 나이",
        },
    },
}


async def _run_sql(sql: str) -> str:
    """docker exec로 SQL 실행"""
    cmd = [
        "docker", "exec", OMOP_CONTAINER,
        "psql", "-U", OMOP_USER, "-d", OMOP_DB,
        "-t", "-A", "-F", "\t", "-c", sql,
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10.0)
    if proc.returncode != 0:
        raise RuntimeError(f"SQL error: {stderr.decode().strip()}")
    return stdout.decode().strip()


async def extract_it_meta() -> Dict[str, dict]:
    """DB information_schema에서 IT메타 추출"""
    global _it_meta_cache, _cache_ready

    tables_str = ",".join(f"'{t}'" for t in TARGET_TABLES)

    col_sql = f"""
    SELECT table_name, column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name IN ({tables_str})
    ORDER BY table_name, ordinal_position;
    """
    count_sql = """
    SELECT relname, n_live_tup
    FROM pg_stat_user_tables
    WHERE schemaname = 'public';
    """

    col_out, count_out = await asyncio.gather(
        _run_sql(col_sql),
        _run_sql(count_sql),
    )

    row_counts: Dict[str, int] = {}
    for line in count_out.split("\n"):
        if line.strip():
            parts = line.split("\t")
            if len(parts) >= 2:
                try:
                    row_counts[parts[0]] = int(parts[1])
                except ValueError:
                    pass

    result: Dict[str, dict] = {}
    for line in col_out.split("\n"):
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 4:
            continue
        table, col, dtype, nullable = parts[0], parts[1], parts[2], parts[3]
        if table not in result:
            result[table] = {
                "table_name": table,
                "row_count": row_counts.get(table, 0),
                "columns": [],
            }
        result[table]["columns"].append({
            "name": col,
            "type": dtype,
            "nullable": nullable == "YES",
        })

    _it_meta_cache = result
    logger.info(f"[BizMeta] IT meta extracted: {len(result)} tables")
    return result


async def _generate_via_llm(it_meta: Dict[str, dict]) -> Dict[str, dict]:
    """LLM으로 IT메타에서 비즈메타 생성"""
    from core.config import settings

    meta_lines = []
    for table_name, info in it_meta.items():
        cols = ", ".join(f"{c['name']}({c['type']})" for c in info["columns"])
        meta_lines.append(f"- {table_name} ({info['row_count']:,}행): {cols}")

    prompt = f"""당신은 의료 데이터 전문가입니다.
아래 OMOP CDM 데이터베이스의 IT메타(물리 테이블·컬럼 정보)를 보고,
각 테이블과 주요 컬럼의 한글 업무 의미(비즈메타)를 JSON으로 생성하세요.

## IT메타
{chr(10).join(meta_lines)}

## 출력 형식 (반드시 순수 JSON만 출력)
```json
{{
  "테이블명": {{
    "business_name": "한글 업무명",
    "description": "한글 설명 (1줄)",
    "key_columns": {{
      "컬럼명": "한글 업무 의미 설명"
    }}
  }}
}}
```
모든 테이블에 대해 생성하세요. key_columns는 PK와 주요 업무 컬럼만 포함 (테이블당 3-5개)."""

    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(
            f"{settings.LLM_API_URL}/chat/completions",
            json={
                "model": settings.LLM_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 2048,
                "temperature": 0.2,
                "chat_template_kwargs": {"enable_thinking": False},
            },
            headers=(
                {"Authorization": f"Bearer {settings.OPENAI_API_KEY}"}
                if settings.OPENAI_API_KEY else {}
            ),
        )
        if resp.status_code != 200:
            raise RuntimeError(f"LLM HTTP {resp.status_code}")

        content = resp.json()["choices"][0]["message"]["content"]
        content = re.sub(r"<think>[\s\S]*?</think>\s*", "", content).strip()

        json_match = re.search(r"```json\s*([\s\S]*?)```", content)
        raw = json_match.group(1) if json_match else content
        return json.loads(raw)


async def _load_from_db() -> Dict[str, dict]:
    """DB 캐시에서 비즈메타 로드"""
    try:
        out = await _run_sql(
            "SELECT table_name, biz_meta FROM biz_meta_cache;"
        )
        result = {}
        for line in out.split("\n"):
            if not line.strip():
                continue
            parts = line.split("\t", 1)
            if len(parts) == 2:
                result[parts[0]] = json.loads(parts[1])
        return result
    except Exception:
        return {}


async def _save_to_db(table_name: str, it_meta: dict, biz_meta: dict):
    """비즈메타를 DB에 영속화"""
    try:
        it_json = json.dumps(it_meta, ensure_ascii=False).replace("'", "''")
        biz_json = json.dumps(biz_meta, ensure_ascii=False).replace("'", "''")
        await _run_sql(f"""
            INSERT INTO biz_meta_cache (table_name, it_meta, biz_meta)
            VALUES ('{table_name}', '{it_json}'::jsonb, '{biz_json}'::jsonb)
            ON CONFLICT (table_name) DO UPDATE
            SET it_meta = EXCLUDED.it_meta,
                biz_meta = EXCLUDED.biz_meta,
                generated_at = NOW();
        """)
    except Exception as e:
        logger.warning(f"[BizMeta] DB save failed for {table_name}: {e}")


async def warm_cache():
    """서버 시작 시 캐시 워밍: DB캐시 → 없으면 LLM 생성"""
    global _biz_meta_cache, _it_meta_cache, _cache_ready, _cache_generated_at

    # 1) DB 캐시 테이블 생성
    try:
        await _run_sql("""
            CREATE TABLE IF NOT EXISTS biz_meta_cache (
                table_name VARCHAR(100) PRIMARY KEY,
                it_meta JSONB NOT NULL DEFAULT '{}',
                biz_meta JSONB NOT NULL DEFAULT '{}',
                generated_at TIMESTAMP DEFAULT NOW()
            );
        """)
    except Exception as e:
        logger.warning(f"[BizMeta] Table creation failed: {e}")

    # 2) IT메타 추출
    try:
        it_meta = await extract_it_meta()
    except Exception as e:
        logger.warning(f"[BizMeta] IT meta extraction failed: {e}")
        _biz_meta_cache = dict(_FALLBACK_BIZ_META)
        _cache_ready = True
        return

    # 3) DB 캐시 확인
    db_cache = await _load_from_db()
    if len(db_cache) >= len(TARGET_TABLES) - 1:
        logger.info(f"[BizMeta] Loaded {len(db_cache)} tables from DB cache")
        _biz_meta_cache = db_cache
        _it_meta_cache = it_meta  # IT메타도 설정
        _cache_ready = True
        _cache_generated_at = time.time()
        return

    # 4) LLM으로 생성
    try:
        logger.info("[BizMeta] Generating via LLM...")
        t0 = time.time()
        biz_meta = await _generate_via_llm(it_meta)
        elapsed = time.time() - t0
        logger.info(f"[BizMeta] LLM generated {len(biz_meta)} tables in {elapsed:.1f}s")

        _biz_meta_cache = biz_meta
        _cache_generated_at = time.time()

        # DB에 영속화
        for tbl, bm in biz_meta.items():
            it = it_meta.get(tbl, {})
            await _save_to_db(tbl, it, bm)

    except Exception as e:
        logger.warning(f"[BizMeta] LLM generation failed, using fallback: {e}")
        _biz_meta_cache = dict(_FALLBACK_BIZ_META)

    _cache_ready = True


def get_thinking_process(tables: List[str]) -> Optional[Dict[str, Any]]:
    """채팅 응답에 포함할 사고 과정 데이터 반환"""
    if not tables:
        return None

    it_items = []
    biz_items = []

    for t in tables:
        # IT메타
        it = _it_meta_cache.get(t)
        if it:
            it_items.append({
                "table": t,
                "row_count": it.get("row_count", 0),
                "columns": [c["name"] for c in it.get("columns", [])],
            })
        # 비즈메타
        bm = _biz_meta_cache.get(t) or _FALLBACK_BIZ_META.get(t)
        if bm:
            biz_items.append({
                "table": t,
                "business_name": bm.get("business_name", t),
                "description": bm.get("description", ""),
                "key_columns": bm.get("key_columns", {}),
            })

    if not it_items and not biz_items:
        return None

    return {
        "it_meta": it_items,
        "biz_meta": biz_items,
        "generated_at": _cache_generated_at,
    }


def detect_tables_from_sql(sql: str) -> List[str]:
    """SQL에서 사용된 테이블명 추출"""
    pattern = re.findall(r"\bFROM\s+(\w+)|\bJOIN\s+(\w+)", sql, re.IGNORECASE)
    found = set(t for pair in pattern for t in pair if t)
    return [t for t in found if t in TARGET_TABLES or t in _it_meta_cache]
