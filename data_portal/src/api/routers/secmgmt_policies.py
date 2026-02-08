"""
DGR-005: 보안 정책 관리, 표준 용어 기반 보안, Biz 메타데이터 보안
- Security Policies CRUD (섹션 1)
- Term-based Security Rules (섹션 2)
- Biz Metadata Security (섹션 3)
"""
from typing import Optional
from fastapi import APIRouter, HTTPException

from .secmgmt_shared import (
    get_conn, ensure_tables, ensure_seed,
    SecurityPolicyCreate, TermSecurityRuleCreate, BizSecurityMetaCreate,
)

router = APIRouter()


# ═══════════════════════════════════════════════════════════
# 1. Security Policies (보안 정책 CRUD)
# ═══════════════════════════════════════════════════════════

@router.get("/policies")
async def get_security_policies(
    target_type: Optional[str] = None,
    security_level: Optional[str] = None,
    enabled: Optional[bool] = None,
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM sec_policy WHERE 1=1"
        args = []
        if target_type:
            args.append(target_type)
            q += f" AND target_type=${len(args)}"
        if security_level:
            args.append(security_level)
            q += f" AND security_level=${len(args)}"
        if enabled is not None:
            args.append(enabled)
            q += f" AND enabled=${len(args)}"
        q += " ORDER BY security_level DESC, target_table, target_column"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/policies")
async def create_security_policy(body: SecurityPolicyCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO sec_policy (target_type, target_table, target_column, security_type, security_level, legal_basis, deident_method, deident_timing, description, enabled)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *
        """, body.target_type, body.target_table, body.target_column, body.security_type, body.security_level, body.legal_basis, body.deident_method, body.deident_timing, body.description, body.enabled)
        return dict(row)
    finally:
        await conn.close()


@router.put("/policies/{policy_id}")
async def update_security_policy(policy_id: int, body: SecurityPolicyCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE sec_policy SET target_type=$2, target_table=$3, target_column=$4, security_type=$5,
            security_level=$6, legal_basis=$7, deident_method=$8, deident_timing=$9, description=$10,
            enabled=$11, updated_at=NOW() WHERE policy_id=$1 RETURNING *
        """, policy_id, body.target_type, body.target_table, body.target_column, body.security_type, body.security_level, body.legal_basis, body.deident_method, body.deident_timing, body.description, body.enabled)
        if not row:
            raise HTTPException(404, "Policy not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/policies/{policy_id}")
async def delete_security_policy(policy_id: int):
    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM sec_policy WHERE policy_id=$1", policy_id)
        return {"deleted": True}
    finally:
        await conn.close()


@router.get("/policies/overview")
async def get_policy_overview():
    """보안 정책 현황 대시보드 통계"""
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        stats = {}
        stats["total"] = await conn.fetchval("SELECT COUNT(*) FROM sec_policy")
        stats["by_level"] = [dict(r) for r in await conn.fetch(
            "SELECT security_level, COUNT(*) as cnt FROM sec_policy GROUP BY security_level ORDER BY cnt DESC")]
        stats["by_type"] = [dict(r) for r in await conn.fetch(
            "SELECT security_type, COUNT(*) as cnt FROM sec_policy GROUP BY security_type ORDER BY cnt DESC")]
        stats["by_target"] = [dict(r) for r in await conn.fetch(
            "SELECT target_type, COUNT(*) as cnt FROM sec_policy GROUP BY target_type")]
        stats["enabled_count"] = await conn.fetchval("SELECT COUNT(*) FROM sec_policy WHERE enabled=TRUE")
        return stats
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 2. Term-based Security Rules (표준 용어 기반 보안)
# ═══════════════════════════════════════════════════════════

@router.get("/term-rules")
async def get_term_security_rules():
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT * FROM sec_term_rule ORDER BY security_level DESC, term_name")
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/term-rules")
async def create_term_security_rule(body: TermSecurityRuleCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO sec_term_rule (term_name, security_type, security_level, deident_method, auto_propagate, description)
            VALUES ($1,$2,$3,$4,$5,$6) RETURNING *
        """, body.term_name, body.security_type, body.security_level, body.deident_method, body.auto_propagate, body.description)
        return dict(row)
    finally:
        await conn.close()


@router.put("/term-rules/{rule_id}")
async def update_term_security_rule(rule_id: int, body: TermSecurityRuleCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE sec_term_rule SET term_name=$2, security_type=$3, security_level=$4,
            deident_method=$5, auto_propagate=$6, description=$7, updated_at=NOW()
            WHERE rule_id=$1 RETURNING *
        """, rule_id, body.term_name, body.security_type, body.security_level, body.deident_method, body.auto_propagate, body.description)
        if not row:
            raise HTTPException(404, "Rule not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/term-rules/{rule_id}")
async def delete_term_security_rule(rule_id: int):
    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM sec_term_rule WHERE rule_id=$1", rule_id)
        return {"deleted": True}
    finally:
        await conn.close()


@router.post("/term-rules/propagate")
async def propagate_term_rules():
    """표준 용어 기반 보안 규칙을 전체 컬럼에 전파"""
    conn = await get_conn()
    try:
        rules = await conn.fetch("SELECT * FROM sec_term_rule WHERE auto_propagate=TRUE")
        columns = await conn.fetch("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name NOT LIKE 'sec_%' AND table_name NOT LIKE 'etl_%'
              AND table_name NOT LIKE 'meta_%' AND table_name NOT LIKE 'catalog_%'
              AND table_name NOT LIKE 'dm_%' AND table_name NOT LIKE 'gov_%'
        """)
        # Korean term -> column name pattern mapping
        TERM_PATTERNS = {
            "환자": ["person_id", "person_source", "patient"],
            "진단": ["condition", "diagnosis", "icd"],
            "처방": ["drug", "prescription", "medication"],
            "검사": ["measurement", "lab", "test", "value_as"],
            "수술": ["procedure", "surgery", "operation"],
            "진료": ["visit", "encounter", "admission"],
            "투약": ["drug_exposure", "dose", "dosage"],
            "활력징후": ["vital", "blood_pressure", "heart_rate", "temperature"],
        }
        propagated = []
        for rule in rules:
            patterns = TERM_PATTERNS.get(rule["term_name"], [])
            if not patterns:
                continue
            matched = 0
            for col in columns:
                col_full = f"{col['table_name']}.{col['column_name']}".lower()
                if any(p in col_full for p in patterns):
                    existing = await conn.fetchval(
                        "SELECT COUNT(*) FROM sec_policy WHERE target_table=$1 AND target_column=$2",
                        col["table_name"], col["column_name"])
                    if existing == 0:
                        await conn.execute("""
                            INSERT INTO sec_policy (target_type, target_table, target_column, security_type, security_level, deident_method, description, enabled)
                            VALUES ('column', $1, $2, $3, $4, $5, $6, TRUE)
                        """, col["table_name"], col["column_name"], rule["security_type"],
                            rule["security_level"], rule["deident_method"],
                            f"[자동전파] 표준 용어 '{rule['term_name']}' 기반")
                        matched += 1
            if matched > 0:
                await conn.execute(
                    "UPDATE sec_term_rule SET applied_count=applied_count+$2, updated_at=NOW() WHERE rule_id=$1",
                    rule["rule_id"], matched)
            propagated.append({"term": rule["term_name"], "new_policies": matched})
        return {"propagated": propagated, "total_new": sum(p["new_policies"] for p in propagated)}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 3. Biz Metadata Security (보안 유형/비식별 관리속성)
# ═══════════════════════════════════════════════════════════

@router.get("/biz-security")
async def get_biz_security_meta(
    table_name: Optional[str] = None,
    deident_target: Optional[bool] = None,
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM sec_biz_meta WHERE 1=1"
        args = []
        if table_name:
            args.append(table_name)
            q += f" AND table_name=${len(args)}"
        if deident_target is not None:
            args.append(deident_target)
            q += f" AND deident_target=${len(args)}"
        q += " ORDER BY table_name, column_name"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/biz-security")
async def create_biz_security_meta(body: BizSecurityMetaCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO sec_biz_meta (table_name, column_name, security_type, deident_target, deident_timing, deident_level, deident_method, retention_period, access_scope, description)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING *
        """, body.table_name, body.column_name, body.security_type, body.deident_target, body.deident_timing, body.deident_level, body.deident_method, body.retention_period, body.access_scope, body.description)
        return dict(row)
    finally:
        await conn.close()


@router.put("/biz-security/{meta_id}")
async def update_biz_security_meta(meta_id: int, body: BizSecurityMetaCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE sec_biz_meta SET table_name=$2, column_name=$3, security_type=$4, deident_target=$5,
            deident_timing=$6, deident_level=$7, deident_method=$8, retention_period=$9, access_scope=$10,
            description=$11, updated_at=NOW() WHERE meta_id=$1 RETURNING *
        """, meta_id, body.table_name, body.column_name, body.security_type, body.deident_target, body.deident_timing, body.deident_level, body.deident_method, body.retention_period, body.access_scope, body.description)
        if not row:
            raise HTTPException(404, "Biz security meta not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/biz-security/{meta_id}")
async def delete_biz_security_meta(meta_id: int):
    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM sec_biz_meta WHERE meta_id=$1", meta_id)
        return {"deleted": True}
    finally:
        await conn.close()
