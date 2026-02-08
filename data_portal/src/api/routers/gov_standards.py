"""
거버넌스 - 표준 용어 사전 및 표준 지표 관리
"""
from typing import Optional
from fastapi import APIRouter, HTTPException, Query
import asyncpg

from routers.gov_shared import (
    get_connection,
    StandardTermCreate, StandardIndicatorCreate,
)

router = APIRouter()


# ── 표준 용어 사전 ──

async def _ensure_standard_term_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS standard_term (
            term_id SERIAL PRIMARY KEY,
            standard_name VARCHAR(100) NOT NULL UNIQUE,
            definition VARCHAR(500) NOT NULL,
            domain VARCHAR(50) NOT NULL,
            synonyms TEXT[] DEFAULT '{}',
            abbreviation VARCHAR(50),
            english_name VARCHAR(100),
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    count = await conn.fetchval("SELECT COUNT(*) FROM standard_term")
    if count == 0:
        seeds = [
            ("환자", "의료 서비스를 받는 개인", "환자", ["피검자", "수진자", "대상자"], "Pt", "Patient"),
            ("진단", "질병 또는 상태를 식별하는 의학적 판단", "진료", ["진단명", "병명"], "Dx", "Diagnosis"),
            ("처방", "의사가 환자에게 약물/시술을 지시하는 행위", "처방", ["오더", "약처방"], "Rx", "Prescription"),
            ("진료", "환자와 의료진 간 의료 서비스 제공 행위", "진료", ["방문", "외래", "입원"], None, "Visit"),
            ("검사", "환자의 상태를 평가하기 위한 임상 시험", "검사", ["랩검사", "혈액검사", "임상검사"], "Lab", "Laboratory Test"),
            ("수술", "치료 목적으로 인체에 시행하는 의학적 시술", "시술", ["시술", "수술명", "OP"], "OP", "Surgery"),
            ("투약", "약물을 환자에게 투여하는 행위", "처방", ["약물투여", "약물 복용"], "Med", "Medication"),
            ("활력징후", "환자의 기본 생체 신호 측정값", "검사", ["바이탈", "V/S", "Vital Sign"], "VS", "Vital Signs"),
            ("퇴원", "입원 치료 종료 후 병원을 떠나는 행위", "진료", ["퇴원일", "DC"], "DC", "Discharge"),
            ("재원일수", "입원일부터 퇴원일까지의 기간", "진료", ["재원기간", "LOS"], "LOS", "Length of Stay"),
            ("주상병", "입원/방문의 주된 진단명", "진료", ["주진단", "Primary Dx"], "PDx", "Primary Diagnosis"),
            ("부상병", "주상병 외 추가 진단명", "진료", ["부진단", "Secondary Dx"], "SDx", "Secondary Diagnosis"),
            ("의무기록", "환자의 진료 내용을 기록한 문서", "기록", ["차트", "EMR", "전자의무기록"], "MR", "Medical Record"),
            ("동의서", "의료 행위에 대한 환자의 동의 문서", "기록", ["수술동의서", "검사동의서"], None, "Consent Form"),
            ("OMOP CDM", "국제 의료 데이터 공통 데이터 모델", "표준", ["CDM", "공통데이터모델"], "CDM", "OMOP Common Data Model"),
            ("SNOMED CT", "국제 의료 용어 체계 표준", "표준", ["스노메드", "SNOMED"], None, "SNOMED CT"),
            ("ICD-10", "국제 질병 분류 코드 10차 개정판", "표준", ["질병코드", "ICD"], None, "ICD-10"),
            ("비식별화", "개인정보를 식별 불가능하게 처리하는 기법", "보안", ["가명처리", "익명화", "마스킹"], "De-ID", "De-identification"),
            ("K-익명성", "최소 K명의 동일 속성 그룹을 보장하는 기법", "보안", ["K-Anonymity"], None, "K-Anonymity"),
            ("데이터마트", "특정 분석 목적으로 구축된 소규모 데이터셋", "설계", ["DM", "마트"], "DM", "Data Mart"),
        ]
        for name, defn, domain, syns, abbr, eng in seeds:
            await conn.execute("""
                INSERT INTO standard_term (standard_name, definition, domain, synonyms, abbreviation, english_name)
                VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING
            """, name, defn, domain, syns, abbr, eng)


@router.get("/standard-terms")
async def get_standard_terms(domain: Optional[str] = Query(None), search: Optional[str] = Query(None)):
    """표준 용어 사전 조회 (도메인/검색 필터)"""
    conn = await get_connection()
    try:
        await _ensure_standard_term_table(conn)
        query = "SELECT * FROM standard_term WHERE 1=1"
        params = []
        idx = 1
        if domain:
            query += f" AND domain = ${idx}"
            params.append(domain)
            idx += 1
        if search:
            query += f" AND (standard_name ILIKE ${idx} OR definition ILIKE ${idx} OR english_name ILIKE ${idx} OR abbreviation ILIKE ${idx} OR ${idx}::text = ANY(synonyms))"
            params.append(f"%{search}%")
            idx += 1
        query += " ORDER BY domain, standard_name"
        rows = await conn.fetch(query, *params)

        domains_row = await conn.fetch("SELECT DISTINCT domain FROM standard_term ORDER BY domain")
        domains = [r["domain"] for r in domains_row]

        return {
            "terms": [
                {
                    "term_id": r["term_id"],
                    "standard_name": r["standard_name"],
                    "definition": r["definition"],
                    "domain": r["domain"],
                    "synonyms": r["synonyms"] or [],
                    "abbreviation": r["abbreviation"],
                    "english_name": r["english_name"],
                    "status": r["status"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ],
            "total": len(rows),
            "domains": domains,
        }
    finally:
        await conn.close()


@router.post("/standard-terms")
async def create_standard_term(body: StandardTermCreate):
    """표준 용어 등록"""
    conn = await get_connection()
    try:
        await _ensure_standard_term_table(conn)
        term_id = await conn.fetchval("""
            INSERT INTO standard_term (standard_name, definition, domain, synonyms, abbreviation, english_name)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING term_id
        """, body.standard_name, body.definition, body.domain, body.synonyms, body.abbreviation, body.english_name)
        return {"success": True, "term_id": term_id}
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="동일한 표준 용어가 이미 존재합니다")
    finally:
        await conn.close()


@router.put("/standard-terms/{term_id}")
async def update_standard_term(term_id: int, body: StandardTermCreate):
    """표준 용어 수정"""
    conn = await get_connection()
    try:
        result = await conn.execute("""
            UPDATE standard_term
            SET standard_name=$1, definition=$2, domain=$3, synonyms=$4, abbreviation=$5, english_name=$6, updated_at=NOW()
            WHERE term_id=$7
        """, body.standard_name, body.definition, body.domain, body.synonyms, body.abbreviation, body.english_name, term_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="용어를 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


@router.delete("/standard-terms/{term_id}")
async def delete_standard_term(term_id: int):
    """표준 용어 삭제"""
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM standard_term WHERE term_id = $1", term_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="용어를 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


# ── 표준 지표 관리 ──

async def _ensure_standard_indicator_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS standard_indicator (
            indicator_id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL UNIQUE,
            definition VARCHAR(500) NOT NULL,
            formula VARCHAR(300) NOT NULL,
            unit VARCHAR(50) NOT NULL,
            frequency VARCHAR(50) NOT NULL,
            owner_dept VARCHAR(100) NOT NULL,
            data_source VARCHAR(200) NOT NULL,
            category VARCHAR(50) DEFAULT '임상',
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    count = await conn.fetchval("SELECT COUNT(*) FROM standard_indicator")
    if count == 0:
        seeds = [
            ("평균 재원일수", "입원 환자의 평균 입원 기간", "SUM(재원일수) / COUNT(퇴원건수)", "일", "월간", "의료정보실", "visit_occurrence", "진료"),
            ("재입원율", "퇴원 후 30일 이내 동일 진단 재입원 비율", "30일내 재입원 건수 / 전체 퇴원 건수 × 100", "%", "월간", "QI실", "visit_occurrence, condition_occurrence", "품질"),
            ("병원 사망률", "입원 중 사망한 환자 비율", "입원 사망 건수 / 전체 입원 건수 × 100", "%", "월간", "QI실", "death, visit_occurrence", "품질"),
            ("외래 초진율", "전체 외래 중 초진 비율", "초진 건수 / 전체 외래 건수 × 100", "%", "월간", "원무팀", "visit_occurrence", "운영"),
            ("병상 가동률", "가용 병상 대비 실제 사용 병상 비율", "사용 병상일 / 가용 병상일 × 100", "%", "일간", "간호부", "visit_occurrence", "운영"),
            ("수술 건수", "기간 내 시행된 수술 총 건수", "COUNT(procedure_occurrence WHERE procedure_type='수술')", "건", "월간", "수술실", "procedure_occurrence", "진료"),
            ("검사 완료율", "처방된 검사 중 결과가 보고된 비율", "결과보고 건수 / 처방 건수 × 100", "%", "일간", "진단검사의학과", "measurement", "품질"),
            ("약물 부작용 보고율", "투약 건 대비 부작용 보고 비율", "ADR 보고 건수 / 총 투약 건수 × 100", "%", "월간", "약제부", "drug_exposure, observation", "안전"),
            ("비식별화 처리율", "전체 PII 대비 비식별 처리 완료 비율", "비식별 처리 건수 / 전체 PII 건수 × 100", "%", "주간", "빅데이터센터", "person, sensitivity_override", "보안"),
            ("CDM 변환 완료율", "원천 데이터 대비 CDM 변환 완료 비율", "CDM 적재 건수 / 원천 추출 건수 × 100", "%", "일간", "융합연구센터", "etl_log", "데이터"),
        ]
        for name, defn, formula, unit, freq, dept, src, cat in seeds:
            await conn.execute("""
                INSERT INTO standard_indicator (name, definition, formula, unit, frequency, owner_dept, data_source, category)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT DO NOTHING
            """, name, defn, formula, unit, freq, dept, src, cat)


@router.get("/standard-indicators")
async def get_standard_indicators(category: Optional[str] = Query(None)):
    """표준 지표 조회"""
    conn = await get_connection()
    try:
        await _ensure_standard_indicator_table(conn)
        if category:
            rows = await conn.fetch(
                "SELECT * FROM standard_indicator WHERE category = $1 ORDER BY category, name", category
            )
        else:
            rows = await conn.fetch("SELECT * FROM standard_indicator ORDER BY category, name")

        cats_row = await conn.fetch("SELECT DISTINCT category FROM standard_indicator ORDER BY category")
        categories = [r["category"] for r in cats_row]

        return {
            "indicators": [
                {
                    "indicator_id": r["indicator_id"],
                    "name": r["name"],
                    "definition": r["definition"],
                    "formula": r["formula"],
                    "unit": r["unit"],
                    "frequency": r["frequency"],
                    "owner_dept": r["owner_dept"],
                    "data_source": r["data_source"],
                    "category": r["category"],
                    "status": r["status"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ],
            "total": len(rows),
            "categories": categories,
        }
    finally:
        await conn.close()


@router.post("/standard-indicators")
async def create_standard_indicator(body: StandardIndicatorCreate):
    """표준 지표 등록"""
    conn = await get_connection()
    try:
        await _ensure_standard_indicator_table(conn)
        ind_id = await conn.fetchval("""
            INSERT INTO standard_indicator (name, definition, formula, unit, frequency, owner_dept, data_source, category)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING indicator_id
        """, body.name, body.definition, body.formula, body.unit, body.frequency, body.owner_dept, body.data_source, body.category)
        return {"success": True, "indicator_id": ind_id}
    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="동일한 지표명이 이미 존재합니다")
    finally:
        await conn.close()


@router.put("/standard-indicators/{indicator_id}")
async def update_standard_indicator(indicator_id: int, body: StandardIndicatorCreate):
    """표준 지표 수정"""
    conn = await get_connection()
    try:
        result = await conn.execute("""
            UPDATE standard_indicator
            SET name=$1, definition=$2, formula=$3, unit=$4, frequency=$5, owner_dept=$6, data_source=$7, category=$8, updated_at=NOW()
            WHERE indicator_id=$9
        """, body.name, body.definition, body.formula, body.unit, body.frequency, body.owner_dept, body.data_source, body.category, indicator_id)
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="지표를 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()


@router.delete("/standard-indicators/{indicator_id}")
async def delete_standard_indicator(indicator_id: int):
    """표준 지표 삭제"""
    conn = await get_connection()
    try:
        result = await conn.execute("DELETE FROM standard_indicator WHERE indicator_id = $1", indicator_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="지표를 찾을 수 없습니다")
        return {"success": True}
    finally:
        await conn.close()
