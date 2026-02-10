"""
AI Ops 공유 유틸리티, 상수, 모델 정의
PostgreSQL-backed (asyncpg via services.db_pool)

ai_ops.py, ai_ops_safety.py, ai_ops_audit.py 에서 공통으로 사용
"""
import re
import json
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# ─── DB helpers (shared pool) ────────────────────────

async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)


# ─── Lazy table creation ────────────────────────────

_tbl_ok = False

async def _ensure_tables():
    global _tbl_ok
    if _tbl_ok:
        return
    conn = await _get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_model_config (
                model_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                type VARCHAR(50),
                version VARCHAR(100),
                parameters VARCHAR(20),
                gpu_memory_mb INT DEFAULT 0,
                description TEXT DEFAULT '',
                health_url TEXT DEFAULT '',
                test_url TEXT DEFAULT '',
                config JSONB DEFAULT '{}',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS ai_pii_pattern (
                pattern_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                pattern TEXT NOT NULL,
                replacement VARCHAR(100) DEFAULT '***',
                enabled BOOLEAN DEFAULT TRUE,
                description TEXT DEFAULT '',
                sort_order INT DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS ai_audit_log (
                log_id BIGSERIAL PRIMARY KEY,
                log_entry_id VARCHAR(20),
                ts TIMESTAMPTZ DEFAULT NOW(),
                user_id VARCHAR(100),
                model VARCHAR(50),
                query_type VARCHAR(50),
                query TEXT,
                response TEXT,
                latency_ms FLOAT,
                tokens INT,
                pii_count INT DEFAULT 0,
                hallucination_status VARCHAR(20),
                injection_blocked BOOLEAN DEFAULT FALSE,
                error TEXT,
                metadata JSONB DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_ai_audit_ts ON ai_audit_log(ts DESC);
            CREATE INDEX IF NOT EXISTS idx_ai_audit_model ON ai_audit_log(model);
        """)
        _tbl_ok = True
    finally:
        await _rel(conn)


# ─── 모델 레지스트리 기본값 ──────────────────────────

_DEFAULT_MODELS: List[Dict[str, Any]] = [
    {
        "id": "xiyan-sql",
        "name": "XiYanSQL-QWen2.5-3B",
        "type": "Text-to-SQL",
        "version": "2.5-3B-instruct",
        "parameters": "3B",
        "gpu_memory_mb": 6200,
        "description": "자연어 질의를 SQL로 변환하는 경량 모델",
        "health_url": "http://localhost:8001/v1/models",
        "test_url": "http://localhost:8001/v1/chat/completions",
        "config": {
            "temperature": 0.1,
            "max_tokens": 2048,
            "system_prompt": "你是一名PostgreSQL专家，现在需要阅读并理解下面的【数据库schema】描述，然后回答【用户问题】并生成对应的SQL查询语句。\n\n【数据库schema】\n【DB_ID】 omop_cdm\n【표(Table)】 person (person_id BIGINT PK, gender_source_value VARCHAR 'M/F', year_of_birth INT, month_of_birth INT, day_of_birth INT)\n【표(Table)】 condition_occurrence (condition_occurrence_id BIGINT PK, person_id BIGINT FK, condition_concept_id BIGINT, condition_start_date DATE, condition_end_date DATE, condition_source_value VARCHAR 'SNOMED CT코드')\n【표(Table)】 visit_occurrence (visit_occurrence_id BIGINT PK, person_id BIGINT FK, visit_concept_id BIGINT '9201:입원,9202:외래,9203:응급', visit_start_date DATE, visit_end_date DATE)\n【표(Table)】 drug_exposure (drug_exposure_id BIGINT PK, person_id BIGINT FK, drug_concept_id BIGINT, drug_exposure_start_date DATE, drug_source_value VARCHAR)\n【표(Table)】 measurement (measurement_id BIGINT PK, person_id BIGINT FK, measurement_date DATE, value_as_number NUMERIC, measurement_source_value VARCHAR, unit_source_value VARCHAR)\n【표(Table)】 imaging_study (imaging_study_id SERIAL PK, person_id INT FK, finding_labels VARCHAR, image_url VARCHAR)\n【외래키(FK)】 condition_occurrence.person_id = person.person_id\n【외래키(FK)】 visit_occurrence.person_id = person.person_id\n\n【参考信息】\n당뇨=44054006, 고혈압=38341003, 심방세동=49436004, 심근경색=22298006, 뇌졸중=230690007",
        },
    },
    {
        "id": "qwen3-32b",
        "name": "Qwen3-32B",
        "type": "General LLM",
        "version": "3.0-32B-AWQ",
        "parameters": "32B",
        "gpu_memory_mb": 22400,
        "description": "범용 대화형 LLM (한국어 최적화)",
        "health_url": "http://localhost:28888/v1/models",
        "test_url": "http://localhost:28888/v1/chat/completions",
        "config": {
            "temperature": 0.7,
            "max_tokens": 4096,
            "system_prompt": "당신은 서울아산병원 통합 데이터 플랫폼(IDP)의 AI 어시스턴트입니다.\n사용자의 자연어 질문을 SQL로 변환하고 실행하여 결과를 알려줍니다.\n\n## 중요: SQL 생성 규칙\n- 데이터 질문에는 반드시 실행 가능한 PostgreSQL SQL을 ```sql 블록으로 작성하세요\n- SQL은 시스템이 자동 실행하여 결과를 사용자에게 보여줍니다\n- concept 테이블은 존재하지 않습니다. 절대 JOIN하지 마세요\n- 진단 필터링: condition_occurrence.condition_source_value = 'SNOMED코드' 사용\n- 성별 필터링: person.gender_source_value = 'M' 또는 'F' 사용\n- 컬럼 별칭(alias)은 반드시 영문으로 작성하세요 (예: AS patient_count). 한글 별칭 금지\n\n## 데이터베이스 스키마 (OMOP CDM, PostgreSQL)\n### person (환자)\nperson_id BIGINT PK, gender_concept_id BIGINT, year_of_birth INT, month_of_birth INT, day_of_birth INT,\ngender_source_value VARCHAR(50) -- 'M' 또는 'F'\n### condition_occurrence (진단)\ncondition_occurrence_id BIGINT PK, person_id BIGINT FK, condition_concept_id BIGINT,\ncondition_start_date DATE, condition_end_date DATE, condition_source_value VARCHAR(50) -- SNOMED CT 코드\n### visit_occurrence (방문)\nvisit_occurrence_id BIGINT PK, person_id BIGINT FK, visit_concept_id BIGINT,\nvisit_start_date DATE, visit_end_date DATE\n### drug_exposure (약물)\ndrug_exposure_id BIGINT PK, person_id BIGINT FK, drug_concept_id BIGINT,\ndrug_exposure_start_date DATE, drug_exposure_end_date DATE, drug_source_value VARCHAR(100), quantity NUMERIC, days_supply INT\n### measurement (검사)\nmeasurement_id BIGINT PK, person_id BIGINT FK, measurement_concept_id BIGINT,\nmeasurement_date DATE, value_as_number NUMERIC, measurement_source_value VARCHAR(100), unit_source_value VARCHAR(50)\n### imaging_study (흉부X-ray)\nimaging_study_id SERIAL PK, person_id INT FK, image_filename VARCHAR(200),\nfinding_labels VARCHAR(500), view_position VARCHAR(10), patient_age INT, patient_gender VARCHAR(2), image_url VARCHAR(500)\n\n## SNOMED CT 코드 매핑\n당뇨=44054006, 고혈압=38341003, 심방세동=49436004, 심근경색=22298006, 뇌졸중=230690007\n\n## 이미지 표시\nimaging_study 조회 시 마크다운 이미지: ![소견](image_url값)\n\n답변은 항상 한국어로 해주세요.",
        },
    },
    {
        "id": "bioclinical-bert",
        "name": "BioClinicalBERT",
        "type": "Medical NER",
        "version": "d4data/biomedical-ner-all",
        "parameters": "110M",
        "gpu_memory_mb": 287,
        "description": "의료 텍스트 개체명 인식 (NER)",
        "health_url": "http://localhost:28100/ner/health",
        "test_url": "http://localhost:28100/ner/analyze",
        "config": {
            "min_confidence": 0.7,
            "max_length": 512,
            "system_prompt": "",
        },
    },
]

_DEFAULT_PII_PATTERNS: List[Dict[str, Any]] = [
    {"id": "rrn", "name": "주민등록번호", "pattern": r"(?<![.\d])\d{6}[-\s]?[1-4]\d{6}(?!\d)", "replacement": "******-*******", "enabled": True, "description": "한국 주민등록번호 (YYMMDD-GNNNNNN)"},
    {"id": "phone", "name": "전화번호", "pattern": r"01[016789][-\s]?\d{3,4}[-\s]?\d{4}", "replacement": "***-****-****", "enabled": True, "description": "한국 휴대전화 번호"},
    {"id": "email", "name": "이메일", "pattern": r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", "replacement": "***@***.***", "enabled": True, "description": "이메일 주소"},
    {"id": "card", "name": "카드번호", "pattern": r"\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}", "replacement": "****-****-****-****", "enabled": True, "description": "신용/체크카드 번호 (16자리)"},
    {"id": "ip", "name": "IP 주소", "pattern": r"\b(?:\d{1,3}\.){3}\d{1,3}\b", "replacement": "***.***.***.***", "enabled": False, "description": "IPv4 주소"},
    {"id": "passport", "name": "여권번호", "pattern": r"[A-Z]{1,2}\d{7,8}", "replacement": "**********", "enabled": False, "description": "한국 여권번호"},
]


# ─── DB seed (insert defaults if empty) ──────────────

async def _seed_defaults():
    conn = await _get_conn()
    try:
        # Seed models
        cnt = await conn.fetchval("SELECT COUNT(*) FROM ai_model_config")
        if cnt == 0:
            for m in _DEFAULT_MODELS:
                await conn.execute("""
                    INSERT INTO ai_model_config (model_id, name, type, version, parameters, gpu_memory_mb, description, health_url, test_url, config)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (model_id) DO NOTHING
                """, m["id"], m["name"], m["type"], m["version"], m["parameters"],
                   m["gpu_memory_mb"], m["description"], m["health_url"], m["test_url"],
                   json.dumps(m.get("config", {})))
            logger.info("Seeded %d default AI models", len(_DEFAULT_MODELS))

        # Seed PII patterns
        cnt = await conn.fetchval("SELECT COUNT(*) FROM ai_pii_pattern")
        if cnt == 0:
            for i, p in enumerate(_DEFAULT_PII_PATTERNS):
                await conn.execute("""
                    INSERT INTO ai_pii_pattern (pattern_id, name, pattern, replacement, enabled, description, sort_order)
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    ON CONFLICT (pattern_id) DO NOTHING
                """, p["id"], p["name"], p["pattern"], p["replacement"], p["enabled"], p["description"], i)
            logger.info("Seeded %d default PII patterns", len(_DEFAULT_PII_PATTERNS))
    finally:
        await _rel(conn)


# ─── 모델 CRUD (async, DB-backed) ────────────────────

async def db_load_models() -> List[Dict[str, Any]]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        rows = await conn.fetch("SELECT * FROM ai_model_config ORDER BY created_at")
        if not rows:
            await _seed_defaults()
            rows = await conn.fetch("SELECT * FROM ai_model_config ORDER BY created_at")
        return [_row_to_model(r) for r in rows]
    finally:
        await _rel(conn)


def _row_to_model(r) -> Dict[str, Any]:
    cfg = r["config"]
    if isinstance(cfg, str):
        cfg = json.loads(cfg)
    return {
        "id": r["model_id"],
        "name": r["name"],
        "type": r["type"],
        "version": r["version"],
        "parameters": r["parameters"],
        "gpu_memory_mb": r["gpu_memory_mb"],
        "description": r["description"],
        "health_url": r["health_url"],
        "test_url": r["test_url"],
        "config": cfg or {},
    }


async def db_get_model(model_id: str) -> Optional[Dict[str, Any]]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        r = await conn.fetchrow("SELECT * FROM ai_model_config WHERE model_id=$1", model_id)
        return _row_to_model(r) if r else None
    finally:
        await _rel(conn)


async def db_update_model(model_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        existing = await conn.fetchrow("SELECT * FROM ai_model_config WHERE model_id=$1", model_id)
        if not existing:
            return None
        config_update = updates.pop("config", None)
        sets = []
        vals = []
        idx = 1
        for k, v in updates.items():
            if v is not None and k in ("health_url", "test_url", "description"):
                sets.append(f"{k}=${idx}")
                vals.append(v)
                idx += 1
        if config_update:
            old_cfg = existing["config"]
            if isinstance(old_cfg, str):
                old_cfg = json.loads(old_cfg)
            old_cfg = old_cfg or {}
            old_cfg.update(config_update)
            sets.append(f"config=${idx}")
            vals.append(json.dumps(old_cfg))
            idx += 1
        sets.append(f"updated_at=${idx}")
        vals.append(datetime.now())
        idx += 1
        vals.append(model_id)
        await conn.execute(
            f"UPDATE ai_model_config SET {', '.join(sets)} WHERE model_id=${idx}",
            *vals
        )
        return await db_get_model(model_id)
    finally:
        await _rel(conn)


# ─── PII 패턴 CRUD (async, DB-backed) ────────────────

# In-memory cache for sync detect_pii / mask_pii
_pii_cache: List[Dict[str, Any]] = []
_pii_cache_ts: float = 0

async def db_load_pii_patterns() -> List[Dict[str, Any]]:
    global _pii_cache, _pii_cache_ts
    await _ensure_tables()
    conn = await _get_conn()
    try:
        rows = await conn.fetch("SELECT * FROM ai_pii_pattern ORDER BY sort_order, created_at")
        if not rows:
            await _seed_defaults()
            rows = await conn.fetch("SELECT * FROM ai_pii_pattern ORDER BY sort_order, created_at")
        patterns = [_row_to_pii(r) for r in rows]
        _pii_cache = patterns
        import time
        _pii_cache_ts = time.time()
        return patterns
    finally:
        await _rel(conn)


def _row_to_pii(r) -> Dict[str, Any]:
    return {
        "id": r["pattern_id"],
        "name": r["name"],
        "pattern": r["pattern"],
        "replacement": r["replacement"],
        "enabled": r["enabled"],
        "description": r["description"],
    }


async def db_create_pii_pattern(data: Dict[str, Any]) -> Dict[str, Any]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        next_id = await conn.fetchval("SELECT COALESCE(MAX(sort_order),0)+1 FROM ai_pii_pattern")
        pid = data.get("id", f"custom_{next_id}")
        await conn.execute("""
            INSERT INTO ai_pii_pattern (pattern_id, name, pattern, replacement, enabled, description, sort_order)
            VALUES ($1,$2,$3,$4,$5,$6,$7)
        """, pid, data["name"], data["pattern"], data.get("replacement", "***"),
           data.get("enabled", True), data.get("description", ""), next_id)
        await db_load_pii_patterns()  # refresh cache
        return {"id": pid, **data}
    finally:
        await _rel(conn)


async def db_update_pii_pattern(pattern_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        existing = await conn.fetchrow("SELECT * FROM ai_pii_pattern WHERE pattern_id=$1", pattern_id)
        if not existing:
            return None
        sets, vals, idx = [], [], 1
        for k, v in updates.items():
            if v is not None and k in ("name", "pattern", "replacement", "enabled", "description"):
                db_k = k
                sets.append(f"{db_k}=${idx}")
                vals.append(v)
                idx += 1
        if not sets:
            return _row_to_pii(existing)
        vals.append(pattern_id)
        await conn.execute(f"UPDATE ai_pii_pattern SET {', '.join(sets)} WHERE pattern_id=${idx}", *vals)
        await db_load_pii_patterns()  # refresh cache
        r = await conn.fetchrow("SELECT * FROM ai_pii_pattern WHERE pattern_id=$1", pattern_id)
        return _row_to_pii(r) if r else None
    finally:
        await _rel(conn)


async def db_delete_pii_pattern(pattern_id: str) -> bool:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        result = await conn.execute("DELETE FROM ai_pii_pattern WHERE pattern_id=$1", pattern_id)
        deleted = result.split()[-1] != "0"
        if deleted:
            await db_load_pii_patterns()  # refresh cache
        return deleted
    finally:
        await _rel(conn)


# ─── PII 유틸리티 (sync, cache-backed) ───────────────

def _get_pii_patterns_sync() -> List[Dict[str, Any]]:
    """동기적으로 캐시된 PII 패턴 반환 (캐시 없으면 기본값 사용)"""
    if _pii_cache:
        return _pii_cache
    return _DEFAULT_PII_PATTERNS


def detect_pii(text: str) -> List[Dict[str, Any]]:
    patterns = _get_pii_patterns_sync()
    findings = []
    for pat in patterns:
        if not pat.get("enabled", True):
            continue
        try:
            for match in re.finditer(pat["pattern"], text):
                findings.append({
                    "pattern_id": pat["id"],
                    "pattern_name": pat["name"],
                    "matched_text": match.group(),
                    "start": match.start(),
                    "end": match.end(),
                })
        except re.error:
            pass
    return findings


def mask_pii(text: str) -> tuple:
    patterns = _get_pii_patterns_sync()
    count = 0
    masked = text
    for pat in patterns:
        if not pat.get("enabled", True):
            continue
        try:
            masked, n = re.subn(pat["pattern"], pat["replacement"], masked)
            count += n
        except re.error:
            pass
    return masked, count


# ─── 환각 검증 유틸리티 ──────────────────────────────

def verify_hallucination(llm_response: str, sql_results: Optional[List] = None) -> Dict[str, Any]:
    if not sql_results:
        return {"status": "skipped", "reason": "SQL 결과 없음", "checks": []}

    llm_numbers = set()
    for m in re.finditer(r"[\d,]+\.?\d*", llm_response):
        num_str = m.group().replace(",", "")
        try:
            llm_numbers.add(float(num_str))
        except ValueError:
            pass

    sql_numbers = set()
    for row in sql_results:
        if isinstance(row, (list, tuple)):
            for val in row:
                if isinstance(val, (int, float)):
                    sql_numbers.add(float(val))
        elif isinstance(row, dict):
            for val in row.values():
                if isinstance(val, (int, float)):
                    sql_numbers.add(float(val))

    if not sql_numbers:
        return {"status": "skipped", "reason": "SQL 결과에 수치 없음", "checks": []}

    checks = []
    matched = 0
    for sql_num in sql_numbers:
        found = any(abs(sql_num - llm_num) < 0.01 for llm_num in llm_numbers) if sql_num != 0 else True
        checks.append({"sql_value": sql_num, "found_in_response": found})
        if found:
            matched += 1

    total = len(checks)
    ratio = matched / total if total > 0 else 1.0
    status = "pass" if ratio >= 0.8 else ("warning" if ratio >= 0.5 else "fail")

    return {"status": status, "match_ratio": round(ratio, 2), "checks": checks}


# ─── 감사 로그 (async, DB-backed) ────────────────────

async def db_append_audit_log(entry: Dict[str, Any]):
    await _ensure_tables()
    conn = await _get_conn()
    try:
        await conn.execute("""
            INSERT INTO ai_audit_log (log_entry_id, ts, user_id, model, query_type, query, response, latency_ms, tokens, pii_count, hallucination_status, injection_blocked, error, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        """,
            entry.get("id", f"LOG-{datetime.now().strftime('%Y%m%d%H%M%S')}"),
            datetime.fromisoformat(entry["timestamp"]) if entry.get("timestamp") else datetime.now(),
            entry.get("user", ""),
            entry.get("model", ""),
            entry.get("query_type", ""),
            entry.get("query", ""),
            entry.get("response", ""),
            entry.get("latency_ms"),
            entry.get("tokens"),
            entry.get("pii_count", 0),
            entry.get("hallucination_status", ""),
            entry.get("injection_blocked", False),
            entry.get("error", ""),
            json.dumps(entry.get("metadata", {})),
        )
    except Exception as e:
        logger.warning(f"Failed to write audit log to DB: {e}")
    finally:
        await _rel(conn)


async def db_load_audit_logs(
    page: int = 1,
    page_size: int = 20,
    model: Optional[str] = None,
    user: Optional[str] = None,
    query_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> Dict[str, Any]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        conditions = []
        params = []
        idx = 1
        if model:
            conditions.append(f"model=${idx}")
            params.append(model)
            idx += 1
        if user:
            conditions.append(f"LOWER(user_id) LIKE ${idx}")
            params.append(f"%{user.lower()}%")
            idx += 1
        if query_type:
            conditions.append(f"query_type=${idx}")
            params.append(query_type)
            idx += 1
        if date_from:
            conditions.append(f"ts >= ${idx}::timestamptz")
            params.append(date_from)
            idx += 1
        if date_to:
            conditions.append(f"ts <= (${idx}::date + interval '1 day')")
            params.append(date_to)
            idx += 1

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        total = await conn.fetchval(f"SELECT COUNT(*) FROM ai_audit_log {where}", *params)
        offset = (page - 1) * page_size
        rows = await conn.fetch(
            f"SELECT * FROM ai_audit_log {where} ORDER BY ts DESC LIMIT {page_size} OFFSET {offset}",
            *params
        )
        logs = [_row_to_audit(r) for r in rows]
        return {
            "logs": logs,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if total else 0,
        }
    finally:
        await _rel(conn)


def _row_to_audit(r) -> Dict[str, Any]:
    return {
        "id": r["log_entry_id"] or f"LOG-{r['log_id']}",
        "timestamp": r["ts"].isoformat() if r["ts"] else "",
        "user": r["user_id"] or "",
        "model": r["model"] or "",
        "query_type": r["query_type"] or "",
        "query": r["query"] or "",
        "response": r["response"] or "",
        "latency_ms": r["latency_ms"],
        "tokens": r["tokens"],
        "pii_count": r["pii_count"] or 0,
        "hallucination_status": r["hallucination_status"] or "",
        "injection_blocked": r["injection_blocked"] or False,
        "error": r["error"] or "",
    }


async def db_audit_stats() -> Dict[str, Any]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        total = await conn.fetchval("SELECT COUNT(*) FROM ai_audit_log")
        if total == 0:
            return {
                "total_queries": 0,
                "avg_latency_ms": 0,
                "model_distribution": {},
                "query_type_distribution": {},
                "daily_counts": [],
                "note": "감사 로그가 없습니다. AI 대화를 사용하면 자동으로 기록됩니다.",
            }

        avg_lat = await conn.fetchval("SELECT ROUND(AVG(latency_ms)::numeric, 1) FROM ai_audit_log WHERE latency_ms IS NOT NULL") or 0

        model_rows = await conn.fetch("SELECT model, COUNT(*) as cnt FROM ai_audit_log GROUP BY model ORDER BY cnt DESC")
        model_dist = {r["model"] or "unknown": r["cnt"] for r in model_rows}

        qt_rows = await conn.fetch("SELECT query_type, COUNT(*) as cnt FROM ai_audit_log GROUP BY query_type ORDER BY cnt DESC")
        qt_dist = {r["query_type"] or "unknown": r["cnt"] for r in qt_rows}

        daily_rows = await conn.fetch("SELECT ts::date as day, COUNT(*) as cnt FROM ai_audit_log GROUP BY day ORDER BY day")
        daily_counts = [{"date": str(r["day"]), "count": r["cnt"]} for r in daily_rows]

        return {
            "total_queries": total,
            "avg_latency_ms": float(avg_lat),
            "model_distribution": model_dist,
            "query_type_distribution": qt_dist,
            "daily_counts": daily_counts,
        }
    finally:
        await _rel(conn)


async def db_hallucination_stats() -> Dict[str, Any]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        total = await conn.fetchval("SELECT COUNT(*) FROM ai_audit_log WHERE hallucination_status != '' AND hallucination_status IS NOT NULL")
        if total == 0:
            return {
                "total_verified": 0, "pass_count": 0, "warning_count": 0, "fail_count": 0,
                "pass_rate": 0, "note": "환각 검증 데이터 없음. AI 대화 시 자동 기록됩니다.",
            }
        pass_count = await conn.fetchval("SELECT COUNT(*) FROM ai_audit_log WHERE hallucination_status='pass'")
        warning_count = await conn.fetchval("SELECT COUNT(*) FROM ai_audit_log WHERE hallucination_status='warning'")
        fail_count = await conn.fetchval("SELECT COUNT(*) FROM ai_audit_log WHERE hallucination_status='fail'")
        return {
            "total_verified": total,
            "pass_count": pass_count,
            "warning_count": warning_count,
            "fail_count": fail_count,
            "pass_rate": round(pass_count / total * 100, 1) if total else 0,
        }
    finally:
        await _rel(conn)


async def db_injection_stats() -> Dict[str, Any]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        total = await conn.fetchval("SELECT COUNT(*) FROM ai_audit_log")
        blocked = await conn.fetchval("SELECT COUNT(*) FROM ai_audit_log WHERE injection_blocked=TRUE")
        return {
            "total_queries": total,
            "injection_blocked": blocked,
            "block_rate": round(blocked / total * 100, 2) if total else 0,
            "safe_queries": total - blocked,
        }
    finally:
        await _rel(conn)


async def db_model_metrics(model_id: str) -> Dict[str, Any]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        total = await conn.fetchval("SELECT COUNT(*) FROM ai_audit_log WHERE model=$1", model_id)
        if total == 0:
            return {
                "model_id": model_id, "total_requests": 0, "avg_latency_ms": 0,
                "error_rate": 0, "p95_latency_ms": 0, "daily_trend": [],
                "note": "감사 로그에 데이터가 없습니다. AI 대화를 사용하면 자동으로 기록됩니다.",
            }

        avg_lat = await conn.fetchval(
            "SELECT ROUND(AVG(latency_ms)::numeric, 1) FROM ai_audit_log WHERE model=$1 AND latency_ms IS NOT NULL", model_id) or 0
        errors = await conn.fetchval("SELECT COUNT(*) FROM ai_audit_log WHERE model=$1 AND error != '' AND error IS NOT NULL", model_id)
        p95 = await conn.fetchval("""
            SELECT ROUND(percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)::numeric, 1)
            FROM ai_audit_log WHERE model=$1 AND latency_ms IS NOT NULL
        """, model_id) or 0
        daily = await conn.fetch(
            "SELECT ts::date as day, COUNT(*) as cnt FROM ai_audit_log WHERE model=$1 GROUP BY day ORDER BY day", model_id)

        return {
            "model_id": model_id,
            "total_requests": total,
            "avg_latency_ms": float(avg_lat),
            "error_rate": round(errors / total * 100, 1) if total else 0,
            "p95_latency_ms": float(p95),
            "daily_trend": [{"date": str(r["day"]), "count": r["cnt"]} for r in daily],
        }
    finally:
        await _rel(conn)


async def db_export_audit_logs(
    model: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    await _ensure_tables()
    conn = await _get_conn()
    try:
        conditions, params, idx = [], [], 1
        if model:
            conditions.append(f"model=${idx}")
            params.append(model)
            idx += 1
        if date_from:
            conditions.append(f"ts >= ${idx}::timestamptz")
            params.append(date_from)
            idx += 1
        if date_to:
            conditions.append(f"ts <= (${idx}::date + interval '1 day')")
            params.append(date_to)
            idx += 1
        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        rows = await conn.fetch(f"SELECT * FROM ai_audit_log {where} ORDER BY ts DESC LIMIT 10000", *params)
        return [_row_to_audit(r) for r in rows]
    finally:
        await _rel(conn)


# ─── Pydantic Models ────────────────────────────────

class ModelConfigUpdate(BaseModel):
    health_url: Optional[str] = None
    test_url: Optional[str] = None
    description: Optional[str] = None
    config: Optional[Dict[str, Any]] = None


class TestQueryRequest(BaseModel):
    prompt: str
    max_tokens: int = 256


class PIITestRequest(BaseModel):
    text: str


class PIIPatternCreate(BaseModel):
    name: str
    pattern: str
    replacement: str = "***"
    enabled: bool = True
    description: str = ""


class PIIPatternUpdate(BaseModel):
    name: Optional[str] = None
    pattern: Optional[str] = None
    replacement: Optional[str] = None
    enabled: Optional[bool] = None
    description: Optional[str] = None


class PromptInjectionRequest(BaseModel):
    text: str


# ─── Legacy compatibility (sync functions for callers that still use them) ──

def append_audit_log(entry: Dict[str, Any]):
    """동기 호환 — 비동기 DB 쓰기를 fire-and-forget으로 실행"""
    import asyncio
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(db_append_audit_log(entry))
    except RuntimeError:
        logger.warning("No event loop for audit log write; entry dropped")
