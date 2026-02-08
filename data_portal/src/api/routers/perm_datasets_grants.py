"""
DGR-007 권한 관리 — 데이터세트, 권한 부여, 역할 할당, 역할 파라미터
"""
import json as _json
from datetime import datetime
from typing import Optional, Dict, Any

from fastapi import APIRouter, HTTPException

from .permission_mgmt_shared import (
    DatasetCreate, PermGrantCreate, RoleAssignmentCreate, RoleParamDefCreate,
    get_conn, ensure_tables, ensure_seed,
)

router = APIRouter()


# ═══════════════════════════════════════════════════════════
# 1. Dataset Management (데이터세트 추상화)
# ═══════════════════════════════════════════════════════════

@router.get("/datasets")
async def get_datasets(
    dataset_type: Optional[str] = None,
    classification: Optional[str] = None,
    active: Optional[bool] = None,
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM perm_dataset WHERE 1=1"
        args = []
        if dataset_type:
            args.append(dataset_type); q += f" AND dataset_type=${len(args)}"
        if classification:
            args.append(classification); q += f" AND classification=${len(args)}"
        if active is not None:
            args.append(active); q += f" AND active=${len(args)}"
        q += " ORDER BY name"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.get("/datasets/{dataset_id}")
async def get_dataset_detail(dataset_id: int):
    conn = await get_conn()
    try:
        ds = await conn.fetchrow("SELECT * FROM perm_dataset WHERE dataset_id=$1", dataset_id)
        if not ds:
            raise HTTPException(404, "Dataset not found")
        result = dict(ds)
        # Include grants for this dataset
        grants = await conn.fetch("SELECT * FROM perm_grant WHERE dataset_id=$1 ORDER BY grant_type", dataset_id)
        result["grants"] = [dict(g) for g in grants]
        return result
    finally:
        await conn.close()


@router.post("/datasets")
async def create_dataset(body: DatasetCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO perm_dataset (name, description, dataset_type, tables, columns, row_filter, owner, classification)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING *
        """, body.name, body.description, body.dataset_type, body.tables,
            _json.dumps(body.columns) if body.columns else None,
            body.row_filter, body.owner, body.classification)
        return dict(row)
    finally:
        await conn.close()


@router.put("/datasets/{dataset_id}")
async def update_dataset(dataset_id: int, body: DatasetCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE perm_dataset SET name=$2, description=$3, dataset_type=$4, tables=$5,
            columns=$6, row_filter=$7, owner=$8, classification=$9, updated_at=NOW()
            WHERE dataset_id=$1 RETURNING *
        """, dataset_id, body.name, body.description, body.dataset_type, body.tables,
            _json.dumps(body.columns) if body.columns else None,
            body.row_filter, body.owner, body.classification)
        if not row:
            raise HTTPException(404, "Dataset not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/datasets/{dataset_id}")
async def delete_dataset(dataset_id: int):
    conn = await get_conn()
    try:
        await conn.execute("UPDATE perm_dataset SET active=FALSE, updated_at=NOW() WHERE dataset_id=$1", dataset_id)
        return {"deleted": True}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 2. Permission Grants (권한 부여 — 테이블/컬럼/로우)
# ═══════════════════════════════════════════════════════════

@router.get("/grants")
async def get_grants(
    dataset_id: Optional[int] = None,
    grantee_id: Optional[str] = None,
    grant_type: Optional[str] = None,
    active: Optional[bool] = None,
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT g.*, d.name as dataset_name FROM perm_grant g LEFT JOIN perm_dataset d ON g.dataset_id=d.dataset_id WHERE 1=1"
        args = []
        if dataset_id:
            args.append(dataset_id); q += f" AND g.dataset_id=${len(args)}"
        if grantee_id:
            args.append(grantee_id); q += f" AND g.grantee_id=${len(args)}"
        if grant_type:
            args.append(grant_type); q += f" AND g.grant_type=${len(args)}"
        if active is not None:
            args.append(active); q += f" AND g.active=${len(args)}"
        q += " ORDER BY g.created_at DESC"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/grants")
async def create_grant(body: PermGrantCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO perm_grant (dataset_id, target_table, target_columns, row_filter, grant_type, grantee_type, grantee_id, source_role_id, granted_by, expires_at, condition)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING *
        """, body.dataset_id, body.target_table, body.target_columns, body.row_filter,
            body.grant_type, body.grantee_type, body.grantee_id, body.source_role_id, body.granted_by,
            datetime.fromisoformat(body.expires_at) if body.expires_at else None,
            _json.dumps(body.condition) if body.condition else None)
        # Audit
        await conn.execute("""
            INSERT INTO perm_audit (action, actor, target_type, target_id, details, source_roles)
            VALUES ('GRANT', $1, 'grant', $2, $3, $4)
        """, body.granted_by or 'system', str(row["grant_id"]),
            _json.dumps({"grant_type": body.grant_type, "grantee": f"{body.grantee_type}:{body.grantee_id}"}),
            [body.source_role_id] if body.source_role_id else None)
        return dict(row)
    finally:
        await conn.close()


@router.put("/grants/{grant_id}/revoke")
async def revoke_grant(grant_id: int, reason: str = "권한 회수"):
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "UPDATE perm_grant SET active=FALSE, updated_at=NOW() WHERE grant_id=$1 AND active=TRUE RETURNING *",
            grant_id)
        if not row:
            raise HTTPException(404, "Active grant not found")
        await conn.execute("""
            INSERT INTO perm_audit (action, actor, target_type, target_id, details)
            VALUES ('REVOKE', 'admin', 'grant', $1, $2)
        """, str(grant_id), _json.dumps({"reason": reason, "grant_type": row["grant_type"]}))
        return dict(row)
    finally:
        await conn.close()


@router.delete("/grants/{grant_id}")
async def delete_grant(grant_id: int):
    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM perm_grant WHERE grant_id=$1", grant_id)
        return {"deleted": True}
    finally:
        await conn.close()


@router.get("/grants/effective/{user_id}")
async def get_effective_grants(user_id: str):
    """사용자의 실효 권한 (모든 역할에서 취득된 권한 종합 + 출처 역할 구분)"""
    conn = await get_conn()
    try:
        # 1. Get all role assignments for user
        assignments = await conn.fetch(
            "SELECT * FROM perm_role_assignment WHERE user_id=$1 AND active=TRUE ORDER BY priority", user_id)
        # 2. Get grants from each role
        results = []
        for asgn in assignments:
            role_id = asgn["role_id"]
            role_name_row = await conn.fetchrow(
                "SELECT role_name FROM governance_role WHERE role_id=$1", role_id)
            role_name = role_name_row["role_name"] if role_name_row else f"Role#{role_id}"
            # Grants by role
            grants = await conn.fetch("""
                SELECT g.*, d.name as dataset_name
                FROM perm_grant g LEFT JOIN perm_dataset d ON g.dataset_id=d.dataset_id
                WHERE g.active=TRUE AND (
                    (g.grantee_type='role' AND g.grantee_id=$1)
                    OR (g.grantee_type='user' AND g.grantee_id=$2 AND g.source_role_id=$3)
                    OR (g.grantee_type='role_assignment' AND g.grantee_id=$2)
                )
            """, str(role_id), user_id, role_id)
            for g in grants:
                gd = dict(g)
                gd["source_role_id"] = role_id
                gd["source_role_name"] = role_name
                gd["assignment_type"] = asgn["assignment_type"]
                gd["assignment_priority"] = asgn["priority"]
                gd["role_parameters"] = asgn["parameters"]
                results.append(gd)
        # 3. Direct user grants (no role)
        direct = await conn.fetch("""
            SELECT g.*, d.name as dataset_name
            FROM perm_grant g LEFT JOIN perm_dataset d ON g.dataset_id=d.dataset_id
            WHERE g.active=TRUE AND g.grantee_type='user' AND g.grantee_id=$1 AND g.source_role_id IS NULL
        """, user_id)
        for g in direct:
            gd = dict(g)
            gd["source_role_id"] = None
            gd["source_role_name"] = "직접 부여"
            gd["assignment_type"] = "direct"
            gd["assignment_priority"] = 0
            gd["role_parameters"] = None
            results.append(gd)
        return {
            "user_id": user_id,
            "role_count": len(assignments),
            "roles": [{"role_id": a["role_id"], "type": a["assignment_type"], "priority": a["priority"]} for a in assignments],
            "total_grants": len(results),
            "grants": results,
        }
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 3. Role Assignments (복수 역할 할당)
# ═══════════════════════════════════════════════════════════

@router.get("/role-assignments")
async def get_role_assignments(
    user_id: Optional[str] = None,
    role_id: Optional[int] = None,
    assignment_type: Optional[str] = None,
):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = """SELECT ra.*, u.user_name, u.department
               FROM perm_role_assignment ra
               LEFT JOIN sec_user_attribute u ON ra.user_id=u.user_id
               WHERE 1=1"""
        args = []
        if user_id:
            args.append(user_id); q += f" AND ra.user_id=${len(args)}"
        if role_id:
            args.append(role_id); q += f" AND ra.role_id=${len(args)}"
        if assignment_type:
            args.append(assignment_type); q += f" AND ra.assignment_type=${len(args)}"
        q += " ORDER BY ra.user_id, ra.priority"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/role-assignments")
async def create_role_assignment(body: RoleAssignmentCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO perm_role_assignment (user_id, role_id, assignment_type, priority, parameters, expires_at, granted_by)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *
        """, body.user_id, body.role_id, body.assignment_type, body.priority,
            _json.dumps(body.parameters or {}),
            datetime.fromisoformat(body.expires_at) if body.expires_at else None,
            body.granted_by)
        await conn.execute("""
            INSERT INTO perm_audit (action, actor, target_type, target_id, details, source_roles)
            VALUES ('ASSIGN_ROLE', $1, 'user', $2, $3, $4)
        """, body.granted_by or 'system', body.user_id,
            _json.dumps({"role_id": body.role_id, "type": body.assignment_type, "params": body.parameters}),
            [body.role_id])
        return dict(row)
    finally:
        await conn.close()


@router.put("/role-assignments/{assignment_id}")
async def update_role_assignment(assignment_id: int, body: RoleAssignmentCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE perm_role_assignment SET role_id=$2, assignment_type=$3, priority=$4, parameters=$5,
            expires_at=$6, granted_by=$7, updated_at=NOW()
            WHERE assignment_id=$1 RETURNING *
        """, assignment_id, body.role_id, body.assignment_type, body.priority,
            _json.dumps(body.parameters or {}),
            datetime.fromisoformat(body.expires_at) if body.expires_at else None,
            body.granted_by)
        if not row:
            raise HTTPException(404, "Assignment not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/role-assignments/{assignment_id}")
async def delete_role_assignment(assignment_id: int):
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "DELETE FROM perm_role_assignment WHERE assignment_id=$1 RETURNING user_id, role_id", assignment_id)
        if row:
            await conn.execute("""
                INSERT INTO perm_audit (action, actor, target_type, target_id, details)
                VALUES ('UNASSIGN_ROLE', 'admin', 'user', $1, $2)
            """, row["user_id"], _json.dumps({"role_id": row["role_id"]}))
        return {"deleted": True}
    finally:
        await conn.close()


# ═══════════════════════════════════════════════════════════
# 4. Role Parameter Definitions (역할 파라미터)
# ═══════════════════════════════════════════════════════════

@router.get("/role-params")
async def get_role_param_defs(role_id: Optional[int] = None):
    await ensure_tables()
    await ensure_seed()
    conn = await get_conn()
    try:
        q = "SELECT * FROM perm_role_param_def WHERE 1=1"
        args = []
        if role_id:
            args.append(role_id); q += f" AND role_id=${len(args)}"
        q += " ORDER BY role_id, param_name"
        rows = await conn.fetch(q, *args)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


@router.post("/role-params")
async def create_role_param_def(body: RoleParamDefCreate):
    await ensure_tables()
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            INSERT INTO perm_role_param_def (role_id, param_name, param_type, description, default_value, validation_rule, required)
            VALUES ($1,$2,$3,$4,$5,$6,$7) RETURNING *
        """, body.role_id, body.param_name, body.param_type, body.description,
            body.default_value, body.validation_rule, body.required)
        return dict(row)
    finally:
        await conn.close()


@router.put("/role-params/{param_def_id}")
async def update_role_param_def(param_def_id: int, body: RoleParamDefCreate):
    conn = await get_conn()
    try:
        row = await conn.fetchrow("""
            UPDATE perm_role_param_def SET role_id=$2, param_name=$3, param_type=$4, description=$5,
            default_value=$6, validation_rule=$7, required=$8
            WHERE param_def_id=$1 RETURNING *
        """, param_def_id, body.role_id, body.param_name, body.param_type, body.description,
            body.default_value, body.validation_rule, body.required)
        if not row:
            raise HTTPException(404, "Param definition not found")
        return dict(row)
    finally:
        await conn.close()


@router.delete("/role-params/{param_def_id}")
async def delete_role_param_def(param_def_id: int):
    conn = await get_conn()
    try:
        await conn.execute("DELETE FROM perm_role_param_def WHERE param_def_id=$1", param_def_id)
        return {"deleted": True}
    finally:
        await conn.close()


@router.post("/role-params/validate")
async def validate_role_params(role_id: int, parameters: Dict[str, Any]):
    """역할 파라미터 유효성 검증"""
    conn = await get_conn()
    try:
        defs = await conn.fetch("SELECT * FROM perm_role_param_def WHERE role_id=$1", role_id)
        errors = []
        for d in defs:
            val = parameters.get(d["param_name"])
            if d["required"] and val is None:
                errors.append({"param": d["param_name"], "error": "필수 파라미터 누락"})
                continue
            if val is not None and d["validation_rule"]:
                rule = d["validation_rule"]
                if rule.startswith("RANGE("):
                    bounds = rule[6:-1].split(",")
                    try:
                        num_val = int(val) if isinstance(val, (int, str)) else val
                        if num_val < int(bounds[0]) or num_val > int(bounds[1]):
                            errors.append({"param": d["param_name"], "error": f"범위 초과: {bounds[0]}~{bounds[1]}"})
                    except (ValueError, TypeError):
                        errors.append({"param": d["param_name"], "error": "숫자 형식이 아님"})
                elif rule.startswith("IN("):
                    allowed = rule[3:-1].split(",")
                    if str(val) not in allowed:
                        errors.append({"param": d["param_name"], "error": f"허용값: {','.join(allowed)}"})
        return {"valid": len(errors) == 0, "errors": errors, "checked_params": len(defs)}
    finally:
        await conn.close()
