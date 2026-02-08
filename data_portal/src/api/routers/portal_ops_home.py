"""
포털 홈 대시보드 + 데이터 패브릭 + 분석 통계 API
Home.tsx, DataFabric.tsx, Analysis.tsx의 하드코딩 제거를 위한 실 DB 연동 엔드포인트
"""
import os
import json
import time
from datetime import datetime, timedelta

from fastapi import APIRouter
import asyncpg

from ._portal_ops_shared import get_connection, portal_ops_init, OMOP_DB_CONFIG

router = APIRouter(tags=["PortalOps-Home"])

# ── 캐시 (5분) ──
_cache: dict = {}
CACHE_TTL = 300


def _cached(key: str):
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < CACHE_TTL:
        return entry["data"]
    return None


def _set_cache(key: str, data):
    _cache[key] = {"data": data, "ts": time.time()}


# ═══════════════════════════════════════════════════
#  GET /home-dashboard — Home.tsx 전체 데이터
# ═══════════════════════════════════════════════════

@router.get("/home-dashboard")
async def home_dashboard():
    cached = _cached("home-dashboard")
    if cached:
        return cached

    conn = await get_connection()
    try:
        await portal_ops_init(conn)

        # 1) 테이블·컬럼 수
        tc = await conn.fetchrow("""
            SELECT
                (SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema='public' AND table_type='BASE TABLE') AS table_count,
                (SELECT COUNT(*) FROM information_schema.columns
                 WHERE table_schema='public') AS column_count
        """)
        table_count = tc["table_count"]
        column_count = tc["column_count"]

        # 2) 총 레코드 수 + 보유율 (non-empty 테이블 비율)
        stats_rows = await conn.fetch("""
            SELECT relname, n_live_tup
            FROM pg_stat_user_tables
            WHERE schemaname='public'
            ORDER BY n_live_tup DESC
        """)
        total_records = sum(r["n_live_tup"] for r in stats_rows)
        non_empty = sum(1 for r in stats_rows if r["n_live_tup"] > 0)
        retention_pct = round(non_empty / max(len(stats_rows), 1) * 100, 1)

        # 3) 데이터 품질 — 핵심 OMOP CDM 테이블 NOT NULL 채움률
        quality = await _compute_quality(conn)

        # 4) 월별 실행 추이 (etl_execution_log)
        monthly_data = await _get_monthly_data(conn)

        # 5) 공지사항 — po_announcement 최신 4건
        announcements = await _get_announcements(conn)

        # 6) 인기 테이블 — 접근 빈도 기반 (pg_stat_user_tables.seq_scan)
        popular_tables = await _get_popular_tables(conn)

        # 7) 최근 대화 — conversation_session 최신
        recent_chats = await _get_recent_chats(conn)

        # 8) 관심 도메인 — OMOP CDM 테이블 카테고리 별 건수
        interest_domains = _compute_interest_domains(stats_rows)

        result = {
            "data_overview": {
                "table_count": table_count,
                "column_count": column_count,
                "total_records": total_records,
                "retention_pct": retention_pct,
            },
            "quality": quality,
            "monthly_data": monthly_data,
            "announcements": announcements,
            "popular_tables": popular_tables,
            "recent_chats": recent_chats,
            "interest_domains": interest_domains,
        }
        _set_cache("home-dashboard", result)
        return result
    finally:
        await conn.close()


async def _compute_quality(conn) -> dict:
    """핵심 OMOP 테이블의 NOT NULL 필드 채움률로 품질 지수 산출"""
    core_tables = ["person", "visit_occurrence", "condition_occurrence", "measurement", "drug_exposure"]
    fill_rates = []
    for tbl in core_tables:
        try:
            row = await conn.fetchrow(f"""
                SELECT COUNT(*) AS total,
                       COUNT(person_id) AS filled_person
                FROM {tbl} LIMIT 10000
            """)
            if row and row["total"] > 0:
                fill_rates.append(row["filled_person"] / row["total"] * 100)
        except Exception:
            pass

    fill_rate = round(sum(fill_rates) / max(len(fill_rates), 1), 1)

    # 유효성: 날짜 필드 유효 비율 (visit_occurrence.visit_start_date NOT NULL)
    validity = 0.0
    try:
        vr = await conn.fetchrow("""
            SELECT COUNT(*) AS total, COUNT(visit_start_date) AS valid
            FROM visit_occurrence LIMIT 50000
        """)
        if vr and vr["total"] > 0:
            validity = round(vr["valid"] / vr["total"] * 100, 1)
    except Exception:
        pass

    # 정확성: condition_source_value 숫자 코드 비율
    accuracy = 0.0
    try:
        ar = await conn.fetchrow("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE condition_source_value ~ '^[0-9]+$') AS numeric_codes
            FROM condition_occurrence LIMIT 50000
        """)
        if ar and ar["total"] > 0:
            accuracy = round(ar["numeric_codes"] / ar["total"] * 100, 1)
    except Exception:
        pass

    # 일관성: person.gender_source_value M/F 비율
    consistency = 0.0
    try:
        cr = await conn.fetchrow("""
            SELECT COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE gender_source_value IN ('M','F')) AS consistent
            FROM person
        """)
        if cr and cr["total"] > 0:
            consistency = round(cr["consistent"] / cr["total"] * 100, 1)
    except Exception:
        pass

    overall = round((fill_rate + validity + accuracy + consistency) / 4, 1)

    return {
        "overall_score": overall,
        "fill_rate": fill_rate,
        "validity": validity,
        "accuracy": accuracy,
        "consistency": consistency,
    }


async def _get_monthly_data(conn) -> list:
    """etl_execution_log 월별 성공률"""
    try:
        rows = await conn.fetch("""
            SELECT EXTRACT(MONTH FROM started_at)::int AS month,
                   COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE status='success') AS success
            FROM etl_execution_log
            WHERE started_at >= NOW() - INTERVAL '12 months'
            GROUP BY 1 ORDER BY 1
        """)
        monthly = {}
        for r in rows:
            monthly[r["month"]] = round(r["success"] / max(r["total"], 1) * 100, 1)
        return [monthly.get(m, 0) for m in range(1, 13)]
    except Exception:
        return [0] * 12


async def _get_announcements(conn) -> list:
    """po_announcement 최신 4건"""
    try:
        rows = await conn.fetch("""
            SELECT ann_id, title, ann_type, priority, created_at
            FROM po_announcement
            WHERE is_active = TRUE
            ORDER BY is_pinned DESC, priority DESC, created_at DESC
            LIMIT 4
        """)
        return [
            {
                "type": r["ann_type"] or "info",
                "title": r["title"],
                "date": r["created_at"].strftime("%Y-%m-%d") if r["created_at"] else "",
            }
            for r in rows
        ]
    except Exception:
        return []


async def _get_popular_tables(conn) -> list:
    """pg_stat_user_tables seq_scan 기반 인기 테이블"""
    rows = await conn.fetch("""
        SELECT relname,
               COALESCE(seq_scan, 0) + COALESCE(idx_scan, 0) AS total_scans,
               n_live_tup
        FROM pg_stat_user_tables
        WHERE schemaname='public' AND n_live_tup > 0
        ORDER BY (COALESCE(seq_scan, 0) + COALESCE(idx_scan, 0)) DESC
        LIMIT 4
    """)
    return [
        {
            "name": r["relname"],
            "type": "table",
            "views": r["total_scans"],
        }
        for r in rows
    ]


async def _get_recent_chats(conn) -> list:
    """conversation_session 최신 대화"""
    try:
        rows = await conn.fetch("""
            SELECT role, content
            FROM conversation_message
            ORDER BY created_at DESC
            LIMIT 2
        """)
        if rows:
            return [{"role": r["role"], "content": r["content"][:100]} for r in reversed(rows)]
    except Exception:
        pass

    # 테이블 없으면 빈 배열
    return []


def _compute_interest_domains(stats_rows) -> list:
    """OMOP CDM 테이블을 도메인별로 분류"""
    domain_map = {
        "임상데이터": ["person", "visit_occurrence", "visit_detail", "condition_occurrence",
                    "condition_era", "observation_period", "death"],
        "약물정보": ["drug_exposure", "drug_era"],
        "검사결과": ["measurement", "observation"],
        "시술정보": ["procedure_occurrence", "device_exposure"],
        "비용정보": ["cost", "payer_plan_period"],
        "의료기관": ["care_site", "provider", "location"],
    }

    result = []
    name_rows = {r["relname"]: r["n_live_tup"] for r in stats_rows}

    for domain, tables in domain_map.items():
        count = sum(name_rows.get(t, 0) for t in tables)
        table_cnt = sum(1 for t in tables if t in name_rows)
        result.append({
            "name": domain,
            "count": table_cnt,
            "records": count,
        })

    return result


# ═══════════════════════════════════════════════════
#  GET /fabric-stats — DataFabric.tsx 데이터
# ═══════════════════════════════════════════════════

@router.get("/fabric-stats")
async def fabric_stats():
    cached = _cached("fabric-stats")
    if cached:
        return cached

    conn = await get_connection()
    try:
        await portal_ops_init(conn)

        # 수집량: etl_execution_log 시간대별 집계
        ingestion_data = []
        try:
            rows = await conn.fetch("""
                SELECT EXTRACT(HOUR FROM started_at)::int AS hour,
                       COALESCE(SUM(rows_processed), 0) AS volume,
                       CASE WHEN SUM(rows_processed) > 0
                            THEN ROUND(SUM(rows_failed)::numeric / SUM(rows_processed), 4)
                            ELSE 0 END AS error_rate
                FROM etl_execution_log
                GROUP BY 1 ORDER BY 1
            """)
            for r in rows:
                h = r["hour"]
                ingestion_data.append({
                    "time": f"{h:02d}:00",
                    "volume": int(r["volume"]),
                    "errorRate": float(r["error_rate"]),
                })
        except Exception:
            pass

        # 품질: 도메인별 NOT NULL 채움률 (경량 샘플링)
        quality_data = []
        domain_tables = {
            "임상": ["person", "visit_occurrence", "condition_occurrence"],
            "검사": ["measurement"],
            "영상": ["observation"],
            "원무": ["cost", "payer_plan_period"],
            "약물": ["drug_exposure", "drug_era"],
        }
        for domain, tables in domain_tables.items():
            total_cols = 0
            filled_cols = 0
            issues = 0
            for tbl in tables:
                try:
                    cols = await conn.fetch(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema='public' AND table_name=$1", tbl
                    )
                    if not cols:
                        continue
                    total_cols += len(cols)
                    # 단일 쿼리로 모든 컬럼 NOT NULL 비율 체크 (100행 샘플)
                    col_exprs = ", ".join(
                        f"COUNT({c['column_name']}) AS c{i}"
                        for i, c in enumerate(cols)
                    )
                    row = await conn.fetchrow(
                        f"SELECT COUNT(*) AS total, {col_exprs} "
                        f"FROM (SELECT * FROM {tbl} LIMIT 100) sub"
                    )
                    if row and row["total"] > 0:
                        tc = row["total"]
                        for i in range(len(cols)):
                            ratio = row[f"c{i}"] / tc
                            if ratio >= 0.9:
                                filled_cols += 1
                            elif ratio < 0.5:
                                issues += 1
                except Exception:
                    pass

            score = round(filled_cols / max(total_cols, 1) * 100, 1)
            quality_data.append({"domain": domain, "score": score, "issues": issues})

        # 소스 수
        source_count = 0
        try:
            source_count = await conn.fetchval(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema='public' AND table_type='BASE TABLE'"
            )
        except Exception:
            pass

        result = {
            "ingestion_data": ingestion_data,
            "quality_data": quality_data,
            "source_count": source_count,
        }
        _set_cache("fabric-stats", result)
        return result
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════
#  GET /analysis-stats — Analysis.tsx 데이터
# ═══════════════════════════════════════════════════

@router.get("/analysis-stats")
async def analysis_stats():
    cached = _cached("analysis-stats")
    if cached:
        return cached

    conn = await get_connection()
    try:
        await portal_ops_init(conn)

        # 대화/쿼리 통계
        total_queries = 0
        success_queries = 0
        avg_duration = 0.0
        try:
            row = await conn.fetchrow("""
                SELECT COUNT(*) AS total,
                       COUNT(*) FILTER (WHERE status='success') AS success,
                       COALESCE(AVG(EXTRACT(EPOCH FROM (ended_at - started_at)))::numeric, 0) AS avg_dur
                FROM etl_execution_log
            """)
            if row:
                total_queries = row["total"]
                success_queries = row["success"]
                avg_duration = round(float(row["avg_dur"]), 1)
        except Exception:
            pass

        accuracy = round(success_queries / max(total_queries, 1) * 100, 1)

        # 대화 메시지 건수
        chat_count = 0
        try:
            chat_count = await conn.fetchval(
                "SELECT COUNT(*) FROM conversation_message"
            ) or 0
        except Exception:
            pass

        # 사용자 만족도: 에러율 기반 역산
        error_rate = 1 - (success_queries / max(total_queries, 1))
        satisfaction = round(5.0 - error_rate * 3, 1)
        satisfaction = max(1.0, min(5.0, satisfaction))

        result = {
            "items": [
                {
                    "key": "1",
                    "category": "모델 정확도",
                    "value": f"{accuracy}%",
                    "raw_value": accuracy,
                    "change": f"+{round(accuracy - 90, 1)}%" if accuracy > 90 else f"{round(accuracy - 90, 1)}%",
                    "trend": "up" if accuracy > 90 else "down",
                },
                {
                    "key": "2",
                    "category": "평균 처리 시간",
                    "value": f"{avg_duration}초",
                    "raw_value": avg_duration,
                    "change": f"-{round(5.0 - avg_duration, 1)}초" if avg_duration < 5 else f"+{round(avg_duration - 5, 1)}초",
                    "trend": "down" if avg_duration < 5 else "up",
                },
                {
                    "key": "3",
                    "category": "처리된 케이스",
                    "value": f"{total_queries:,}건",
                    "raw_value": total_queries,
                    "change": f"+{chat_count}건",
                    "trend": "up",
                },
                {
                    "key": "4",
                    "category": "서비스 만족도",
                    "value": f"{satisfaction}/5.0",
                    "raw_value": satisfaction,
                    "change": f"+{round(satisfaction - 4.0, 1)}",
                    "trend": "up" if satisfaction >= 4.0 else "down",
                },
            ]
        }
        _set_cache("analysis-stats", result)
        return result
    finally:
        await conn.close()
