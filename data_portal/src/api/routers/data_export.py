"""
바-1: 데이터 반출 API — CSV/JSON 다운로드 + IRB 승인 워크플로우
RFP 요구: 표준 인터페이스 기반 데이터 서비스 연동
"""
import os
import io
import csv
import json
import time
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, HTTPException, Query, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/export", tags=["DataExport"])

# ── DB 연결 ──
async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)

# ── Pydantic 모델 ──
class ExportRequest(BaseModel):
    sql: str = Field(..., min_length=5, max_length=5000)
    format: str = Field(default="csv", pattern=r"^(csv|json)$")
    filename: Optional[str] = Field(None, max_length=200)
    purpose: str = Field(..., min_length=2, max_length=500)
    irb_number: Optional[str] = Field(None, max_length=50)

class ExportApprovalRequest(BaseModel):
    status: str = Field(..., pattern=r"^(approved|rejected)$")
    reviewer: str = Field(default="admin", max_length=50)
    comment: Optional[str] = Field(None, max_length=1000)

# ── 테이블 초기화 ──
_tbl_ok = False
async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS export_request (
            request_id SERIAL PRIMARY KEY,
            requester VARCHAR(50) NOT NULL DEFAULT 'demo',
            sql_query TEXT NOT NULL,
            format VARCHAR(10) DEFAULT 'csv',
            filename VARCHAR(200),
            purpose TEXT NOT NULL,
            irb_number VARCHAR(50),
            status VARCHAR(20) DEFAULT 'pending',
            row_count INTEGER,
            reviewer VARCHAR(50),
            review_comment TEXT,
            reviewed_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_export_req_status ON export_request(status);
    """)
    _tbl_ok = True

MAX_EXPORT_ROWS = int(os.getenv("EXPORT_MAX_ROWS", "100000"))

# ── SQL 검증 (읽기 전용) ──
import re
_FORBIDDEN = ["INSERT","UPDATE","DELETE","DROP","CREATE","ALTER","TRUNCATE",
              "GRANT","REVOKE","EXECUTE","COPY","LOAD"]

def _validate_export_sql(sql: str) -> tuple:
    s = sql.upper().strip()
    if not (s.startswith("SELECT") or s.startswith("WITH")):
        return False, "SELECT/WITH 쿼리만 허용됩니다"
    for kw in _FORBIDDEN:
        if re.search(rf'\b{kw}\b', s):
            return False, f"금지 키워드: {kw}"
    if "--" in sql or "/*" in sql:
        return False, "SQL 주석 금지"
    return True, ""


# ══════════════════════════════════════════
# 엔드포인트
# ══════════════════════════════════════════

@router.post("/request")
async def create_export_request(body: ExportRequest):
    """반출 요청 생성 (IRB 번호 필수 시 검증)"""
    ok, msg = _validate_export_sql(body.sql)
    if not ok:
        raise HTTPException(400, msg)
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        # IRB 필수 여부 확인
        irb_required = await conn.fetchval(
            "SELECT setting_value::text FROM po_system_setting WHERE setting_key = 'export.require_approval'"
        )
        if irb_required and irb_required.strip('"') == "true" and not body.irb_number:
            raise HTTPException(400, "데이터 반출에 IRB 승인 번호가 필요합니다")

        # 행 수 사전 추정
        count_sql = f"SELECT COUNT(*) FROM ({body.sql}) t"
        try:
            row_est = await conn.fetchval(count_sql)
        except Exception:
            row_est = None
        if row_est and row_est > MAX_EXPORT_ROWS:
            raise HTTPException(400, f"반출 최대 행 수 초과: {row_est:,} > {MAX_EXPORT_ROWS:,}")

        auto_approve = (irb_required and irb_required.strip('"') != "true")
        status = "approved" if auto_approve else "pending"

        rid = await conn.fetchval("""
            INSERT INTO export_request (requester, sql_query, format, filename, purpose, irb_number, status, row_count)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING request_id
        """, "demo", body.sql, body.format, body.filename or f"export_{int(time.time())}",
            body.purpose, body.irb_number, status, row_est)
        return {"request_id": rid, "status": status, "estimated_rows": row_est}
    finally:
        await _rel(conn)


@router.get("/requests")
async def list_export_requests(status: Optional[str] = None, limit: int = Query(50, le=200)):
    """반출 요청 목록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT * FROM export_request"
        params = []
        if status:
            q += " WHERE status = $1"
            params.append(status)
        q += " ORDER BY created_at DESC LIMIT " + str(limit)
        rows = await conn.fetch(q, *params)
        return [dict(r) for r in rows]
    finally:
        await _rel(conn)


@router.put("/requests/{request_id}/review")
async def review_export_request(request_id: int, body: ExportApprovalRequest):
    """반출 요청 승인/거절"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        existing = await conn.fetchrow("SELECT * FROM export_request WHERE request_id = $1", request_id)
        if not existing:
            raise HTTPException(404, "요청을 찾을 수 없습니다")
        if existing["status"] != "pending":
            raise HTTPException(400, f"이미 처리된 요청: {existing['status']}")
        await conn.execute("""
            UPDATE export_request SET status=$1, reviewer=$2, review_comment=$3, reviewed_at=NOW()
            WHERE request_id=$4
        """, body.status, body.reviewer, body.comment, request_id)
        return {"request_id": request_id, "status": body.status}
    finally:
        await _rel(conn)


@router.get("/download/{request_id}")
async def download_export(request_id: int):
    """승인된 반출 요청 데이터 다운로드 (CSV/JSON)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        req = await conn.fetchrow("SELECT * FROM export_request WHERE request_id = $1", request_id)
        if not req:
            raise HTTPException(404, "요청을 찾을 수 없습니다")
        if req["status"] != "approved":
            raise HTTPException(403, f"승인되지 않은 요청: {req['status']}")

        # SQL 실행
        sql = req["sql_query"]
        if "LIMIT" not in sql.upper():
            sql += f" LIMIT {MAX_EXPORT_ROWS}"

        rows = await conn.fetch(sql)
        if not rows:
            raise HTTPException(404, "결과가 없습니다")

        columns = list(rows[0].keys())
        fname = req["filename"] or f"export_{request_id}"

        if req["format"] == "json":
            data = json.dumps([dict(r) for r in rows], ensure_ascii=False, default=str, indent=2)
            return StreamingResponse(
                io.BytesIO(data.encode("utf-8")),
                media_type="application/json",
                headers={"Content-Disposition": f'attachment; filename="{fname}.json"'}
            )
        else:
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            for r in rows:
                writer.writerow([r[c] for c in columns])
            return StreamingResponse(
                io.BytesIO(buf.getvalue().encode("utf-8-sig")),
                media_type="text/csv",
                headers={"Content-Disposition": f'attachment; filename="{fname}.csv"'}
            )
    finally:
        await _rel(conn)


@router.get("/stats")
async def export_stats():
    """반출 통계"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        stats = await conn.fetchrow("""
            SELECT
                COUNT(*) as total_requests,
                COUNT(*) FILTER (WHERE status='approved') as approved,
                COUNT(*) FILTER (WHERE status='pending') as pending,
                COUNT(*) FILTER (WHERE status='rejected') as rejected,
                COALESCE(SUM(row_count) FILTER (WHERE status='approved'), 0) as total_rows_exported
            FROM export_request
        """)
        return dict(stats)
    finally:
        await _rel(conn)
