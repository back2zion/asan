"""
DGR-009: 지능형 거버넌스 및 자동화 확장 — 사용 패턴 분석 기반 최적화/메타데이터 관리
"""
import sqlite3
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List

from fastapi import APIRouter

from .gov_shared import get_connection, _parse_usage_data, _get_sqlite_db_path

router = APIRouter()


# ── helpers ──

def _sqlite_query_stats() -> Dict[str, Any]:
    """SQLite query_history에서 실행시간 통계 + 시간대별 분포 조회"""
    import os, json

    db_path = _get_sqlite_db_path()
    if not os.path.exists(db_path):
        return {"avg_ms": 0, "total": 0, "peak_hours": [0] * 24}

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(
        "SELECT execution_time_ms, created_at "
        "FROM query_history "
        "WHERE execution_time_ms IS NOT NULL"
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {"avg_ms": 0, "total": 0, "peak_hours": [0] * 24}

    total = len(rows)
    avg_ms = round(sum(r[0] for r in rows) / total, 1)

    hour_dist = [0] * 24
    for _, ts in rows:
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                hour_dist[dt.hour] += 1
            except Exception:
                pass

    return {"avg_ms": avg_ms, "total": total, "peak_hours": hour_dist}


async def _pg_table_stats(conn) -> List[Dict[str, Any]]:
    """pg_stat_user_tables에서 seq/idx scan, rows, vacuum/analyze 정보 조회"""
    rows = await conn.fetch(
        "SELECT relname, seq_scan, idx_scan, n_live_tup, "
        "       n_tup_ins, n_tup_upd, n_tup_del, "
        "       last_vacuum, last_autovacuum, last_analyze, last_autoanalyze "
        "FROM pg_stat_user_tables "
        "WHERE schemaname = 'public' "
        "ORDER BY n_live_tup DESC"
    )
    return [dict(r) for r in rows]


async def _pg_index_stats(conn) -> List[Dict[str, Any]]:
    """pg_stat_user_indexes에서 미사용 인덱스 탐지"""
    rows = await conn.fetch(
        "SELECT relname, indexrelname, idx_scan, idx_tup_read, idx_tup_fetch "
        "FROM pg_stat_user_indexes "
        "WHERE schemaname = 'public' "
        "ORDER BY idx_scan ASC"
    )
    return [dict(r) for r in rows]


# ── endpoints ──

@router.get("/smart-optimization")
async def smart_optimization():
    """사용 패턴 기반 성능 최적화 분석"""
    usage = _parse_usage_data()
    qstats = _sqlite_query_stats()

    conn = await get_connection()
    try:
        pg_tables = await _pg_table_stats(conn)
    finally:
        await conn.close()

    # query_patterns
    table_freq = usage.get("table_frequency", {})
    co_edges = usage.get("co_occurrence_edges", [])
    top_tables = [{"table": t, "count": c} for t, c in list(table_freq.items())[:10]]
    top_joins = co_edges[:10]

    # index_recommendations
    index_recs: List[Dict[str, Any]] = []
    for t in pg_tables:
        seq = t.get("seq_scan") or 0
        idx = t.get("idx_scan") or 0
        rows = t.get("n_live_tup") or 0
        if seq == 0:
            continue
        ratio = round(seq / max(seq + idx, 1) * 100, 1)
        if ratio > 70 and rows > 10000:
            priority = "high" if (ratio > 90 and rows > 1_000_000) else ("medium" if ratio > 80 else "low")
            index_recs.append({
                "table": t["relname"],
                "seq_scan": seq,
                "idx_scan": idx,
                "ratio": ratio,
                "rows": rows,
                "recommendation": f"시퀀셜 스캔 비율 {ratio}% — WHERE/JOIN 조건 컬럼에 인덱스 추가 권장",
                "priority": priority,
            })
    index_recs.sort(key=lambda x: ({"high": 0, "medium": 1, "low": 2}[x["priority"]], -x["rows"]))

    # cache_suggestions — co-occurrence 빈도 상위 join 조합
    cache_suggestions = []
    for edge in co_edges[:8]:
        cache_suggestions.append({
            "tables": [edge["source"], edge["target"]],
            "frequency": edge["weight"],
            "recommendation": f"'{edge['source']}' ↔ '{edge['target']}' 조인이 {edge['weight']}회 사용됨 — 결과 캐싱 권장",
        })

    # peak_hours
    peak_hours = [{"hour": h, "count": c} for h, c in enumerate(qstats["peak_hours"])]

    return {
        "query_patterns": {
            "total_queries": qstats["total"],
            "avg_execution_time_ms": qstats["avg_ms"],
            "top_tables": top_tables,
            "top_join_patterns": top_joins,
        },
        "index_recommendations": index_recs,
        "cache_suggestions": cache_suggestions,
        "peak_hours": peak_hours,
    }


@router.get("/smart-metadata")
async def smart_metadata():
    """지능형 메타데이터 관리 — 품질 이상, 영향도, 보관 추천"""
    usage = _parse_usage_data()
    table_freq = usage.get("table_frequency", {})
    co_edges = usage.get("co_occurrence_edges", [])

    conn = await get_connection()
    try:
        pg_tables = await _pg_table_stats(conn)
    finally:
        await conn.close()

    now = datetime.utcnow()

    # quality_anomalies
    anomalies: List[Dict[str, Any]] = []
    for t in pg_tables:
        name = t["relname"]
        rows = t.get("n_live_tup") or 0

        # vacuum 오래된 테이블
        last_vac = t.get("last_autovacuum") or t.get("last_vacuum")
        if last_vac:
            days_since = (now - last_vac.replace(tzinfo=None)).days
            if days_since > 30:
                anomalies.append({
                    "table": name, "type": "vacuum_needed",
                    "severity": "high" if days_since > 90 else "medium",
                    "detail": f"마지막 VACUUM이 {days_since}일 전 — 자동 정리 필요",
                })

        # analyze 오래된 테이블
        last_ana = t.get("last_autoanalyze") or t.get("last_analyze")
        if last_ana:
            days_since = (now - last_ana.replace(tzinfo=None)).days
            if days_since > 30:
                anomalies.append({
                    "table": name, "type": "stale_stats",
                    "severity": "high" if days_since > 90 else "medium",
                    "detail": f"마지막 ANALYZE가 {days_since}일 전 — 통계 갱신 필요",
                })
        elif rows > 0:
            anomalies.append({
                "table": name, "type": "stale_stats",
                "severity": "low",
                "detail": "ANALYZE 이력 없음 — 초기 통계 수집 필요",
            })

        # 쓰기 없는 대형 테이블
        writes = (t.get("n_tup_ins") or 0) + (t.get("n_tup_upd") or 0) + (t.get("n_tup_del") or 0)
        if rows > 100000 and writes == 0:
            anomalies.append({
                "table": name, "type": "stale_data",
                "severity": "low",
                "detail": f"행 {rows:,}개이나 INSERT/UPDATE/DELETE 0건 — 변경 없는 정적 데이터",
            })

    anomalies.sort(key=lambda x: {"high": 0, "medium": 1, "low": 2}[x["severity"]])

    # impact_graph — co-occurrence로 연관 테이블 매핑
    dep_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for edge in co_edges:
        dep_map[edge["source"]].append({"table": edge["target"], "strength": edge["weight"]})
        dep_map[edge["target"]].append({"table": edge["source"], "strength": edge["weight"]})

    impact_graph = []
    for t in pg_tables[:30]:
        name = t["relname"]
        deps = dep_map.get(name, [])
        deps_sorted = sorted(deps, key=lambda d: -d["strength"])[:5]
        total_strength = sum(d["strength"] for d in deps_sorted)
        impact_level = "high" if total_strength > 20 else ("medium" if total_strength > 5 else "low")
        impact_graph.append({
            "table": name,
            "row_count": t.get("n_live_tup") or 0,
            "dependent_tables": [d["table"] for d in deps_sorted],
            "dependency_strength": total_strength,
            "impact_level": impact_level,
        })
    impact_graph.sort(key=lambda x: -x["dependency_strength"])

    # retention_recommendations
    retention_recs = []
    for t in pg_tables:
        name = t["relname"]
        rows = t.get("n_live_tup") or 0
        freq = table_freq.get(name, 0)

        if rows < 100:
            continue

        if freq == 0 and rows > 100000:
            retention_recs.append({
                "table": name, "row_count": rows, "query_frequency": freq,
                "recommended_retention": "아카이브",
                "reason": f"쿼리 사용 0회 & 행 {rows:,}개 — 콜드 스토리지 이전 권장",
            })
        elif freq > 0 and freq <= 2 and rows > 500000:
            retention_recs.append({
                "table": name, "row_count": rows, "query_frequency": freq,
                "recommended_retention": "6개월",
                "reason": f"쿼리 {freq}회 & 행 {rows:,}개 — 6개월 보관 후 아카이브 권장",
            })
        elif freq > 2 and rows > 1000000:
            retention_recs.append({
                "table": name, "row_count": rows, "query_frequency": freq,
                "recommended_retention": "12개월",
                "reason": f"쿼리 {freq}회 & 행 {rows:,}개 — 12개월 활성 보관 권장",
            })
    retention_recs.sort(key=lambda x: x["query_frequency"])

    return {
        "quality_anomalies": anomalies,
        "impact_graph": impact_graph,
        "retention_recommendations": retention_recs,
    }


@router.get("/smart-summary")
async def smart_summary():
    """대시보드용 지능형 거버넌스 요약"""
    usage = _parse_usage_data()
    qstats = _sqlite_query_stats()
    table_freq = usage.get("table_frequency", {})
    co_edges = usage.get("co_occurrence_edges", [])

    conn = await get_connection()
    try:
        pg_tables = await _pg_table_stats(conn)
    finally:
        await conn.close()

    # index suggestions count
    index_count = 0
    for t in pg_tables:
        seq = t.get("seq_scan") or 0
        idx = t.get("idx_scan") or 0
        rows = t.get("n_live_tup") or 0
        if seq > 0:
            ratio = seq / max(seq + idx, 1) * 100
            if ratio > 70 and rows > 10000:
                index_count += 1

    # critical anomalies count
    now = datetime.utcnow()
    critical = 0
    for t in pg_tables:
        last_vac = t.get("last_autovacuum") or t.get("last_vacuum")
        if last_vac and (now - last_vac.replace(tzinfo=None)).days > 90:
            critical += 1
        last_ana = t.get("last_autoanalyze") or t.get("last_analyze")
        if last_ana and (now - last_ana.replace(tzinfo=None)).days > 90:
            critical += 1

    # archive candidates
    archive = sum(
        1 for t in pg_tables
        if table_freq.get(t["relname"], 0) == 0 and (t.get("n_live_tup") or 0) > 100000
    )

    # peak hour
    peak_idx = max(range(24), key=lambda h: qstats["peak_hours"][h])
    peak_val = qstats["peak_hours"][peak_idx]

    # optimization score (heuristic 0~100)
    total_recs = index_count + len(co_edges[:8]) + archive + critical
    score = max(0, min(100, 100 - total_recs * 5))

    return {
        "optimization_score": score,
        "total_recommendations": total_recs,
        "critical_anomalies": critical,
        "index_suggestions": index_count,
        "cache_candidates": min(len(co_edges), 8),
        "peak_hour": peak_idx,
        "peak_hour_count": peak_val,
        "archive_candidates": archive,
    }
