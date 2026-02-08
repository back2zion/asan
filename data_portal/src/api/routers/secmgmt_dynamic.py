"""
DGR-005: 동적 보안 정책, 마스킹 규칙
- Dynamic Security Policies (섹션 6)
- Masking Rules - Row/Column/Cell 3단계 (섹션 7)
"""
import json as _json
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, HTTPException

from .secmgmt_shared import (
    get_conn, ensure_tables, ensure_seed,
    DynamicPolicyCreate, MaskingRuleCreate,
)

router = APIRouter()


# ═══════════════════════════════════════════════════════════
# 6. Dynamic Security Policies (동적 보안 정책)
# ═══════════════════════════════════════════════════════════

@router.get("/dynamic-policies")
async def get_dynamic_policies(
    policy_type: Optional[str] = None,
    enabled: Optional[bool] = None,
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM sec_dynamic_policy WHERE 1=1"
        args = []
        if policy_type:
            args.append(policy_type)
            q += f" AND policy_type=${len(args)}"
        if enabled is not None:
            args.append(enabled)
            q += f" AND enabled=${len(args)}"
        q += " ORDER BY priority ASC"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/dynamic-policies")
async def create_dynamic_policy(body: DynamicPolicyCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO sec_dynamic_policy (name, policy_type, condition, action, priority, target_tables, enabled, description)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *
        """, body.name, body.policy_type, _json.dumps(body.condition), _json.dumps(body.action),
            body.priority, body.target_tables, body.enabled, body.description)
        return dict(row)
    finally:
        await conn.close()


@router.put("/dynamic-policies/{dp_id}")
async def update_dynamic_policy(dp_id: int, body: DynamicPolicyCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE sec_dynamic_policy SET name=$2, policy_type=$3, condition=$4, action=$5,
            priority=$6, target_tables=$7, enabled=$8, description=$9, updated_at=NOW()
            WHERE dp_id=$1 RETURNING *
        """, dp_id, body.name, body.policy_type, _json.dumps(body.condition), _json.dumps(body.action),
            body.priority, body.target_tables, body.enabled, body.description)
        if not row:
            raise HTTPException(404, "Dynamic policy not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/dynamic-policies/{dp_id}")
async def delete_dynamic_policy(dp_id: int):
    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM sec_dynamic_policy WHERE dp_id=$1", dp_id)
        return {"deleted": True}
    finally:
        await conn.close()


@router.post("/dynamic-policies/{dp_id}/evaluate")
async def evaluate_dynamic_policy(dp_id: int, user_id: str = "test_user"):
    """동적 정책을 특정 사용자 컨텍스트로 평가 (시뮬레이션)"""
    conn = await get_conn()
    try:
        policy = await conn.fetchrow("SELECT * FROM sec_dynamic_policy WHERE dp_id=$1", dp_id)
        if not policy:
            raise HTTPException(404, "Policy not found")
        user = await conn.fetchrow("SELECT * FROM sec_user_attribute WHERE user_id=$1", user_id)
        condition = _json.loads(policy["condition"]) if isinstance(policy["condition"], str) else policy["condition"]
        action = _json.loads(policy["action"]) if isinstance(policy["action"], str) else policy["action"]
        # Evaluate conditions
        matched = True
        eval_details = []
        if "user_role" in condition:
            role_map = {1: "관리자", 2: "연구자", 3: "임상의", 4: "분석가"}
            user_role = role_map.get(user["role_id"], "unknown") if user else "unknown"
            matches = user_role == condition["user_role"]
            eval_details.append({"condition": "user_role", "expected": condition["user_role"], "actual": user_role, "match": matches})
            if not matches:
                matched = False
        if "has_irb" in condition:
            user_attrs = _json.loads(user["attributes"]) if user and isinstance(user["attributes"], str) else (user["attributes"] if user else {})
            has_irb = user_attrs.get("irb_certified", False) if user_attrs else False
            matches = has_irb == condition["has_irb"]
            eval_details.append({"condition": "has_irb", "expected": condition["has_irb"], "actual": has_irb, "match": matches})
            if not matches:
                matched = False
        if "time_range" in condition:
            now = datetime.now()
            bh = condition.get("business_hours", {"start": "08:00", "end": "18:00"})
            in_hours = int(bh["start"].split(":")[0]) <= now.hour < int(bh["end"].split(":")[0])
            is_outside = condition["time_range"] == "outside_business_hours"
            matches = (not in_hours) if is_outside else in_hours
            eval_details.append({"condition": "time_range", "expected": condition["time_range"], "actual": f"{now.hour}:00", "in_business_hours": in_hours, "match": matches})
            if not matches:
                matched = False
        await conn.execute(
            "UPDATE sec_dynamic_policy SET evaluated_count=evaluated_count+1, last_evaluated_at=NOW() WHERE dp_id=$1", dp_id)
        return {
            "policy_id": dp_id,
            "policy_name": policy["name"],
            "policy_type": policy["policy_type"],
            "user_id": user_id,
            "user_name": user["user_name"] if user else None,
            "condition_matched": matched,
            "evaluation_details": eval_details,
            "action_to_apply": action if matched else None,
            "target_tables": policy["target_tables"],
        }
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 7. Masking Rules (Row/Column/Cell 3단계 마스킹)
# ═══════════════════════════════════════════════════════════

@router.get("/masking-rules")
async def get_masking_rules(
    masking_level: Optional[str] = None,
    target_table: Optional[str] = None,
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM sec_masking_rule WHERE 1=1"
        args = []
        if masking_level:
            args.append(masking_level)
            q += f" AND masking_level=${len(args)}"
        if target_table:
            args.append(target_table)
            q += f" AND target_table=${len(args)}"
        q += " ORDER BY masking_level, target_table"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/masking-rules")
async def create_masking_rule(body: MaskingRuleCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO sec_masking_rule (name, masking_level, target_table, target_column, masking_method, condition, parameters, enabled)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *
        """, body.name, body.masking_level, body.target_table, body.target_column, body.masking_method,
            _json.dumps(body.condition) if body.condition else None,
            _json.dumps(body.parameters) if body.parameters else None,
            body.enabled)
        return dict(row)
    finally:
        await conn.close()


@router.put("/masking-rules/{rule_id}")
async def update_masking_rule(rule_id: int, body: MaskingRuleCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE sec_masking_rule SET name=$2, masking_level=$3, target_table=$4, target_column=$5,
            masking_method=$6, condition=$7, parameters=$8, enabled=$9, updated_at=NOW()
            WHERE rule_id=$1 RETURNING *
        """, rule_id, body.name, body.masking_level, body.target_table, body.target_column, body.masking_method,
            _json.dumps(body.condition) if body.condition else None,
            _json.dumps(body.parameters) if body.parameters else None,
            body.enabled)
        if not row:
            raise HTTPException(404, "Masking rule not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/masking-rules/{rule_id}")
async def delete_masking_rule(rule_id: int):
    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM sec_masking_rule WHERE rule_id=$1", rule_id)
        return {"deleted": True}
    finally:
        await conn.close()
