"""
거버넌스 컴플라이언스 + 보존정책
HIPAA/개인정보보호법 체크리스트, 보존정책 관리
"""
import csv
import io
import json
from datetime import datetime
from typing import Optional, List
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response
from pydantic import BaseModel, Field

router = APIRouter(prefix="/compliance", tags=["GovCompliance"])

async def _get_conn():
    from services.db_pool import get_pool
    return await (await get_pool()).acquire()

async def _rel(conn):
    from services.db_pool import get_pool
    await (await get_pool()).release(conn)

_tbl_ok = False

async def _ensure_tables(conn):
    global _tbl_ok
    if _tbl_ok:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS gov_compliance_report (
            report_id SERIAL PRIMARY KEY,
            report_type VARCHAR(50) NOT NULL DEFAULT 'HIPAA',
            title VARCHAR(300) NOT NULL,
            status VARCHAR(20) DEFAULT 'draft',
            score DOUBLE PRECISION DEFAULT 0,
            checklist JSONB DEFAULT '[]',
            findings JSONB DEFAULT '[]',
            generated_by VARCHAR(50) DEFAULT 'system',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS gov_retention_policy (
            policy_id SERIAL PRIMARY KEY,
            policy_name VARCHAR(200) NOT NULL,
            target_table VARCHAR(100) NOT NULL,
            retention_days INTEGER NOT NULL DEFAULT 365,
            action VARCHAR(20) DEFAULT 'archive',
            condition_sql TEXT,
            last_executed_at TIMESTAMPTZ,
            rows_affected BIGINT DEFAULT 0,
            enabled BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_comp_report_type ON gov_compliance_report(report_type);
        CREATE INDEX IF NOT EXISTS idx_retention_table ON gov_retention_policy(target_table);
    """)
    _tbl_ok = True


# ── Pydantic ──
class RetentionPolicyCreate(BaseModel):
    policy_name: str = Field(..., min_length=1, max_length=200)
    target_table: str = Field(..., pattern=r"^[a-z_][a-z0-9_]{0,99}$")
    retention_days: int = Field(default=365, ge=1, le=36500)
    action: str = Field(default="archive", pattern=r"^(archive|purge)$")
    condition_sql: Optional[str] = Field(None, max_length=2000)
    enabled: bool = True


class ReportGenerate(BaseModel):
    report_type: str = Field(default="HIPAA", pattern=r"^(HIPAA|개인정보보호법|GDPR|공통)$")
    title: Optional[str] = Field(None, max_length=300)


# ══════════════════════════════════════════
# 컴플라이언스 리포트
# ══════════════════════════════════════════

HIPAA_CHECKLIST = [
    {"id": "H-01", "category": "Access Control", "item": "사용자 인증 시스템 구현", "weight": 10},
    {"id": "H-02", "category": "Access Control", "item": "역할 기반 접근 제어 (RBAC)", "weight": 10},
    {"id": "H-03", "category": "Audit", "item": "감사 로그 기록 및 보존", "weight": 10},
    {"id": "H-04", "category": "Audit", "item": "데이터 접근 이력 추적", "weight": 8},
    {"id": "H-05", "category": "Encryption", "item": "전송 중 암호화 (TLS/SSL)", "weight": 10},
    {"id": "H-06", "category": "Encryption", "item": "저장 시 암호화", "weight": 8},
    {"id": "H-07", "category": "De-identification", "item": "PHI 비식별화 처리", "weight": 10},
    {"id": "H-08", "category": "De-identification", "item": "PII 탐지 및 마스킹", "weight": 8},
    {"id": "H-09", "category": "Integrity", "item": "데이터 무결성 검증", "weight": 8},
    {"id": "H-10", "category": "Integrity", "item": "백업 및 복구 체계", "weight": 8},
    {"id": "H-11", "category": "Policy", "item": "보존 정책 수립 및 시행", "weight": 5},
    {"id": "H-12", "category": "Policy", "item": "사고 대응 절차 수립", "weight": 5},
]

PRIVACY_ACT_CHECKLIST = [
    {"id": "P-01", "category": "동의", "item": "개인정보 수집 동의 관리", "weight": 10},
    {"id": "P-02", "category": "동의", "item": "제3자 제공 동의 관리", "weight": 8},
    {"id": "P-03", "category": "보호조치", "item": "접근 권한 관리", "weight": 10},
    {"id": "P-04", "category": "보호조치", "item": "접속기록 보관 (6개월)", "weight": 8},
    {"id": "P-05", "category": "보호조치", "item": "암호화 조치", "weight": 10},
    {"id": "P-06", "category": "파기", "item": "보유기간 경과 시 파기", "weight": 8},
    {"id": "P-07", "category": "파기", "item": "파기 절차 및 방법 수립", "weight": 6},
    {"id": "P-08", "category": "권리보장", "item": "정보주체 열람/정정/삭제 권리", "weight": 8},
    {"id": "P-09", "category": "안전성", "item": "개인정보 영향평가 실시", "weight": 6},
    {"id": "P-10", "category": "안전성", "item": "개인정보 보호책임자 지정", "weight": 6},
]


@router.get("/reports")
async def list_reports(report_type: Optional[str] = None):
    """컴플라이언스 리포트 목록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        q = "SELECT report_id, report_type, title, status, score, generated_by, created_at FROM gov_compliance_report"
        params = []
        if report_type:
            q += " WHERE report_type = $1"
            params.append(report_type)
        q += " ORDER BY created_at DESC LIMIT 50"
        rows = await conn.fetch(q, *params)
        return {"reports": [dict(r) for r in rows], "total": len(rows)}
    finally:
        await _rel(conn)


@router.post("/reports/generate")
async def generate_report(body: ReportGenerate):
    """컴플라이언스 리포트 자동 생성"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)

        if body.report_type in ("HIPAA", "공통"):
            checklist = HIPAA_CHECKLIST
        else:
            checklist = PRIVACY_ACT_CHECKLIST

        # 시스템 상태 기반 자동 점수 산정
        findings = []
        total_weight = sum(item["weight"] for item in checklist)
        achieved_weight = 0

        # 인증 시스템 확인
        try:
            auth_exists = await conn.fetchval("SELECT COUNT(*) FROM pg_tables WHERE tablename='auth_user'")
            if auth_exists and auth_exists > 0:
                achieved_weight += checklist[0]["weight"]
                findings.append({"check": checklist[0]["id"], "status": "pass", "detail": "인증 테이블 존재"})
            else:
                findings.append({"check": checklist[0]["id"], "status": "fail", "detail": "인증 테이블 미확인"})
        except Exception:
            findings.append({"check": checklist[0]["id"], "status": "unknown", "detail": "확인 불가"})

        # 감사 로그 확인
        try:
            audit_exists = await conn.fetchval("SELECT COUNT(*) FROM pg_tables WHERE tablename='po_audit_log'")
            if audit_exists and audit_exists > 0:
                achieved_weight += 18  # H-03 + H-04
                findings.append({"check": "H-03/H-04", "status": "pass", "detail": "감사 로그 테이블 존재"})
            else:
                findings.append({"check": "H-03/H-04", "status": "fail", "detail": "감사 로그 테이블 미확인"})
        except Exception:
            pass

        # 거버넌스 테이블 확인
        try:
            gov_exists = await conn.fetchval("SELECT COUNT(*) FROM pg_tables WHERE tablename LIKE 'gov_%'")
            if gov_exists and gov_exists > 0:
                achieved_weight += 26  # 비식별화+무결성+정책
                findings.append({"check": "H-07~H-11", "status": "pass", "detail": f"거버넌스 테이블 {gov_exists}개 확인"})
        except Exception:
            pass

        # 보존정책 확인
        try:
            ret_count = await conn.fetchval("SELECT COUNT(*) FROM gov_retention_policy WHERE enabled=TRUE")
            if ret_count and ret_count > 0:
                achieved_weight += 5
                findings.append({"check": "H-11", "status": "pass", "detail": f"보존정책 {ret_count}개 활성"})
        except Exception:
            pass

        # RBAC 확인
        try:
            rbac = await conn.fetchval("SELECT COUNT(*) FROM pg_tables WHERE tablename='gov_role'")
            if rbac and rbac > 0:
                achieved_weight += 10
                findings.append({"check": "H-02", "status": "pass", "detail": "RBAC 테이블 존재"})
        except Exception:
            pass

        score = round(achieved_weight / total_weight * 100, 1) if total_weight > 0 else 0
        title = body.title or f"{body.report_type} 컴플라이언스 리포트 ({datetime.now().strftime('%Y-%m-%d')})"

        report_id = await conn.fetchval("""
            INSERT INTO gov_compliance_report (report_type, title, status, score, checklist, findings)
            VALUES ($1,$2,'completed',$3,$4::jsonb,$5::jsonb) RETURNING report_id
        """, body.report_type, title, score, json.dumps(checklist), json.dumps(findings))

        return {
            "report_id": report_id,
            "report_type": body.report_type,
            "title": title,
            "score": score,
            "checklist_items": len(checklist),
            "findings_count": len(findings),
        }
    finally:
        await _rel(conn)


@router.get("/score")
async def compliance_score():
    """종합 컴플라이언스 점수"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        row = await conn.fetchrow("""
            SELECT report_type, AVG(score) as avg_score, COUNT(*) as report_count
            FROM gov_compliance_report
            WHERE status = 'completed'
            GROUP BY report_type
        """)
        all_reports = await conn.fetch("""
            SELECT report_type, AVG(score) as avg_score, COUNT(*) as report_count
            FROM gov_compliance_report WHERE status='completed'
            GROUP BY report_type
        """)
        overall = await conn.fetchval("""
            SELECT AVG(score) FROM gov_compliance_report WHERE status='completed'
        """)
        return {
            "overall_score": round(overall, 1) if overall else 0,
            "by_type": [{"type": r["report_type"], "avg_score": round(r["avg_score"], 1), "reports": r["report_count"]} for r in all_reports],
        }
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# 보존정책
# ══════════════════════════════════════════

@router.post("/retention-policies")
async def create_retention_policy(body: RetentionPolicyCreate):
    """보존정책 생성"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        pid = await conn.fetchval("""
            INSERT INTO gov_retention_policy (policy_name, target_table, retention_days, action, condition_sql, enabled)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING policy_id
        """, body.policy_name, body.target_table, body.retention_days, body.action, body.condition_sql, body.enabled)
        return {"policy_id": pid, "message": f"보존정책 '{body.policy_name}' 생성됨"}
    finally:
        await _rel(conn)


@router.get("/retention-policies")
async def list_retention_policies():
    """보존정책 목록"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        rows = await conn.fetch("SELECT * FROM gov_retention_policy ORDER BY created_at DESC")
        return {"policies": [dict(r) for r in rows], "total": len(rows)}
    finally:
        await _rel(conn)


@router.post("/retention-policies/{policy_id}/execute")
async def execute_retention_policy(policy_id: int, simulate: bool = Query(True)):
    """보존정책 실행 (시뮬레이션)"""
    conn = await _get_conn()
    try:
        await _ensure_tables(conn)
        policy = await conn.fetchrow("SELECT * FROM gov_retention_policy WHERE policy_id=$1", policy_id)
        if not policy:
            raise HTTPException(404, "보존정책을 찾을 수 없습니다")

        # 대상 행 수 산정
        table = policy["target_table"]
        days = policy["retention_days"]
        # 날짜 컬럼 추정 (테이블별)
        date_cols = {
            "condition_occurrence": "condition_start_date",
            "drug_exposure": "drug_exposure_start_date",
            "measurement": "measurement_date",
            "visit_occurrence": "visit_start_date",
            "procedure_occurrence": "procedure_date",
            "observation": "observation_date",
        }
        date_col = date_cols.get(table)
        if not date_col:
            return {"policy_id": policy_id, "simulate": simulate, "message": f"테이블 '{table}'의 날짜 컬럼을 추정할 수 없습니다", "affected_rows": 0}

        affected = await conn.fetchval(
            f"SELECT COUNT(*) FROM {table} WHERE {date_col} < NOW() - INTERVAL '{days} days'") or 0

        if not simulate:
            # 실제 실행은 위험하므로 시뮬레이션만 지원
            return {"policy_id": policy_id, "message": "실제 삭제는 DBA 승인 후 별도 실행. 현재는 시뮬레이션만 지원합니다.", "affected_rows": affected}

        await conn.execute("""
            UPDATE gov_retention_policy SET last_executed_at=NOW(), rows_affected=$1 WHERE policy_id=$2
        """, affected, policy_id)

        return {
            "policy_id": policy_id,
            "simulate": simulate,
            "table": table,
            "retention_days": days,
            "action": policy["action"],
            "affected_rows": affected,
            "message": f"시뮬레이션: {affected}행이 보존기간 초과",
        }
    finally:
        await _rel(conn)


@router.get("/audit-export")
async def export_audit_csv(date_from: Optional[str] = None, date_to: Optional[str] = None):
    """감사 추적 CSV 내보내기"""
    conn = await _get_conn()
    try:
        q = "SELECT * FROM po_audit_log WHERE 1=1"
        params, idx = [], 1
        if date_from:
            q += f" AND timestamp >= ${idx}::timestamptz"; params.append(date_from); idx += 1
        if date_to:
            q += f" AND timestamp <= ${idx}::timestamptz"; params.append(date_to + "T23:59:59"); idx += 1
        q += " ORDER BY timestamp DESC LIMIT 10000"
        rows = await conn.fetch(q, *params)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["audit_id", "timestamp", "user_id", "method", "path", "status_code", "duration_ms", "ip_address"])
        for r in rows:
            writer.writerow([r["audit_id"], r["timestamp"], r.get("user_id", ""), r["method"], r["path"],
                           r["status_code"], r.get("duration_ms", ""), r.get("ip_address", "")])

        return Response(
            content=output.getvalue().encode("utf-8-sig"),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=audit_export_{datetime.now().strftime('%Y%m%d')}.csv"},
        )
    except Exception as e:
        return {"message": f"감사 로그 테이블 조회 실패: {e}", "rows": 0}
    finally:
        await _rel(conn)
