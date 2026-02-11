"""
거버넌스 - RBAC 역할 CRUD
"""
from fastapi import APIRouter, HTTPException

from routers.gov_shared import get_connection, RoleCreate
from services.redis_cache import cached

router = APIRouter()


@router.get("/roles")
@cached("gov-roles", ttl=300)
async def get_roles():
    """RBAC 역할 현황 - governance_role 테이블 기반"""
    conn = await get_connection()
    try:
        # 테이블 없으면 생성 + 시드
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS governance_role (
                role_id SERIAL PRIMARY KEY,
                role_name VARCHAR(50) NOT NULL,
                description VARCHAR(200),
                access_scope VARCHAR(100),
                allowed_tables TEXT[],
                security_level VARCHAR(50)
            )
        """)
        count = await conn.fetchval("SELECT COUNT(*) FROM governance_role")
        if count == 0:
            # 실제 테이블 목록 조회
            table_rows = await conn.fetch("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            all_tables = [r["table_name"] for r in table_rows]

            clinical_tables = [t for t in all_tables if t in {
                "person", "visit_occurrence", "visit_detail",
                "condition_occurrence", "drug_exposure", "procedure_occurrence",
                "measurement", "observation", "device_exposure", "imaging_study",
                "note", "note_nlp", "death",
            }]
            research_tables = [t for t in all_tables if t not in {
                "note", "note_nlp", "governance_role", "deident_scan_history",
            }]
            analyst_tables = [t for t in all_tables if t in {
                "condition_era", "drug_era", "observation_period", "cost",
                "payer_plan_period", "care_site", "provider", "location",
            }]

            seeds = [
                ("관리자", "시스템 전체 관리 및 모니터링", "전체 접근", all_tables, "Row/Column/Cell"),
                ("연구자", "비식별화된 CDM 데이터 기반 연구", "비식별 데이터", research_tables, "Row/Column"),
                ("임상의", "담당 환자의 진료 데이터 조회", "담당 환자", clinical_tables, "Row"),
                ("분석가", "집계/통계 데이터 분석", "집계 데이터", analyst_tables, "Table"),
            ]
            for name, desc, scope, tables, sec_level in seeds:
                await conn.execute("""
                    INSERT INTO governance_role (role_name, description, access_scope, allowed_tables, security_level)
                    VALUES ($1, $2, $3, $4, $5)
                """, name, desc, scope, tables, sec_level)

        roles = await conn.fetch("SELECT * FROM governance_role ORDER BY role_id")
        return [
            {
                "role_id": r["role_id"],
                "role": r["role_name"],
                "description": r["description"],
                "access_scope": r["access_scope"],
                "table_count": len(r["allowed_tables"]) if r["allowed_tables"] else 0,
                "tables": r["allowed_tables"] or [],
                "security_level": r["security_level"],
            }
            for r in roles
        ]
    finally:
        await conn.close()


@router.post("/roles")
async def create_role(body: RoleCreate):
    """RBAC 역할 생성"""
    conn = await get_connection()
    try:
        role_id = await conn.fetchval("""
            INSERT INTO governance_role (role_name, description, access_scope, allowed_tables, security_level)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING role_id
        """, body.role_name, body.description, body.access_scope, body.allowed_tables, body.security_level)
        return {"success": True, "role_id": role_id}
    finally:
        await conn.close()


@router.put("/roles/{role_id}")
async def update_role(role_id: int, body: RoleCreate):
    """RBAC 역할 수정"""
    conn = await get_connection()
    try:
        result = await conn.execute("""
            UPDATE governance_role
            SET role_name = $1, description = $2, access_scope = $3, allowed_tables = $4, security_level = $5
            WHERE role_id = $6
        """, body.role_name, body.description, body.access_scope, body.allowed_tables, body.security_level, role_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="역할을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


@router.delete("/roles/{role_id}")
async def delete_role(role_id: int):
    """RBAC 역할 삭제"""
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM governance_role WHERE role_id = $1", role_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="역할을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()
