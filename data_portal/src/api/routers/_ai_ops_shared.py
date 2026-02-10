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

from ._ai_ops_models import (
    ModelConfigUpdate, TestQueryRequest, PIITestRequest,
    PIIPatternCreate, PIIPatternUpdate, PromptInjectionRequest,
    _DEFAULT_MODELS, _DEFAULT_PII_PATTERNS,
)

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


# ─── Legacy compatibility (sync functions for callers that still use them) ──

def append_audit_log(entry: Dict[str, Any]):
    """동기 호환 — 비동기 DB 쓰기를 fire-and-forget으로 실행"""
    import asyncio
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(db_append_audit_log(entry))
    except RuntimeError:
        logger.warning("No event loop for audit log write; entry dropped")
