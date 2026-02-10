"""
바-2: FHIR R4 리소스 서버 — OMOP CDM → FHIR R4 매핑 (읽기 전용)
RFP 요구: 의료 데이터 표준을 준수하는 시스템 간 데이터 서비스 연동
지원 리소스: Patient, Condition, Observation, MedicationStatement, Procedure, Encounter
"""
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ._fhir_helpers import (
    _get_conn, _rel,
    _gender_map, _visit_class, _person_to_patient,
    BundleEntry, BundleRequest,
)

router = APIRouter(prefix="/fhir", tags=["FHIR"])


# ══════════════════════════════════════════
# metadata (CapabilityStatement)
# ══════════════════════════════════════════

@router.get("/metadata")
async def capability_statement():
    """FHIR CapabilityStatement — 서버 기능 선언"""
    return {
        "resourceType": "CapabilityStatement",
        "status": "active",
        "date": "2026-02-08",
        "kind": "instance",
        "software": {"name": "Asan IDP FHIR Server", "version": "1.0.0"},
        "implementation": {"description": "Seoul Asan Medical Center IDP — OMOP CDM to FHIR R4"},
        "fhirVersion": "4.0.1",
        "format": ["json"],
        "rest": [{
            "mode": "server",
            "resource": [
                {"type": t, "interaction": [{"code": "read"}, {"code": "search-type"}],
                 "searchParam": [{"name": "_id", "type": "number"}]}
                for t in ["Patient", "Condition", "Observation", "MedicationStatement", "Procedure", "Encounter"]
            ]
        }]
    }


# ══════════════════════════════════════════
# Patient (person 테이블)
# ══════════════════════════════════════════

@router.get("/Patient/{person_id}")
async def get_patient(person_id: int):
    conn = await _get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT person_id, gender_source_value, year_of_birth, month_of_birth, day_of_birth, "
            "race_source_value, ethnicity_source_value FROM person WHERE person_id = $1", person_id)
        if not row:
            raise HTTPException(404, "Patient not found")
        return _person_to_patient(row)
    finally:
        await _rel(conn)

@router.get("/Patient")
async def search_patients(
    gender: Optional[str] = None,
    birthdate: Optional[str] = None,
    _count: int = Query(20, le=100, alias="_count"),
    _offset: int = Query(0, ge=0, alias="_offset"),
):
    """Patient 검색"""
    conn = await _get_conn()
    try:
        q = "SELECT person_id, gender_source_value, year_of_birth, month_of_birth, day_of_birth, race_source_value, ethnicity_source_value FROM person WHERE 1=1"
        params = []
        idx = 1
        if gender:
            fhir_to_omop = {"male": "M", "female": "F"}
            q += f" AND gender_source_value = ${idx}"
            params.append(fhir_to_omop.get(gender, gender))
            idx += 1
        if birthdate:
            q += f" AND year_of_birth = ${idx}"
            params.append(int(birthdate[:4]))
            idx += 1
        q += f" ORDER BY person_id LIMIT ${idx} OFFSET ${idx+1}"
        params.extend([_count, _offset])
        rows = await conn.fetch(q, *params)
        total = await conn.fetchval("SELECT COUNT(*) FROM person" + (f" WHERE gender_source_value='{fhir_to_omop.get(gender,gender)}'" if gender else ""))
        return {
            "resourceType": "Bundle", "type": "searchset", "total": total,
            "entry": [{"resource": _person_to_patient(r)} for r in rows]
        }
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# Condition (condition_occurrence 테이블)
# ══════════════════════════════════════════

@router.get("/Condition")
async def search_conditions(
    patient: Optional[int] = None, code: Optional[str] = None,
    _count: int = Query(20, le=100, alias="_count"),
):
    conn = await _get_conn()
    try:
        q = ("SELECT condition_occurrence_id, person_id, condition_source_value, "
             "condition_start_date, condition_end_date, condition_type_concept_id "
             "FROM condition_occurrence WHERE 1=1")
        params, idx = [], 1
        if patient:
            q += f" AND person_id = ${idx}"; params.append(patient); idx += 1
        if code:
            q += f" AND condition_source_value = ${idx}"; params.append(code); idx += 1
        q += f" ORDER BY condition_start_date DESC LIMIT ${idx}"
        params.append(_count)
        rows = await conn.fetch(q, *params)
        return {
            "resourceType": "Bundle", "type": "searchset", "total": len(rows),
            "entry": [{"resource": {
                "resourceType": "Condition",
                "id": str(r["condition_occurrence_id"]),
                "subject": {"reference": f"Patient/{r['person_id']}"},
                "code": {"coding": [{"system": "http://snomed.info/sct", "code": r["condition_source_value"] or ""}]},
                "onsetDateTime": r["condition_start_date"].isoformat() if r["condition_start_date"] else None,
                "abatementDateTime": r["condition_end_date"].isoformat() if r["condition_end_date"] else None,
            }} for r in rows]
        }
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# Observation (measurement 테이블)
# ══════════════════════════════════════════

@router.get("/Observation")
async def search_observations(
    patient: Optional[int] = None, code: Optional[str] = None,
    _count: int = Query(20, le=50, alias="_count"),
):
    conn = await _get_conn()
    try:
        q = ("SELECT measurement_id, person_id, measurement_source_value, measurement_date, "
             "value_as_number, unit_source_value, range_low, range_high "
             "FROM measurement WHERE 1=1")
        params, idx = [], 1
        if patient:
            q += f" AND person_id = ${idx}"; params.append(patient); idx += 1
        if code:
            q += f" AND measurement_source_value = ${idx}"; params.append(code); idx += 1
        q += f" ORDER BY measurement_date DESC LIMIT ${idx}"
        params.append(_count)
        rows = await conn.fetch(q, *params)
        return {
            "resourceType": "Bundle", "type": "searchset", "total": len(rows),
            "entry": [{"resource": {
                "resourceType": "Observation",
                "id": str(r["measurement_id"]),
                "status": "final",
                "subject": {"reference": f"Patient/{r['person_id']}"},
                "code": {"coding": [{"system": "http://loinc.org", "code": r["measurement_source_value"] or ""}]},
                "effectiveDateTime": r["measurement_date"].isoformat() if r["measurement_date"] else None,
                "valueQuantity": {
                    "value": float(r["value_as_number"]) if r["value_as_number"] is not None else None,
                    "unit": r["unit_source_value"] or "",
                } if r["value_as_number"] is not None else None,
                "referenceRange": [{"low": {"value": float(r["range_low"])} if r["range_low"] else None,
                                    "high": {"value": float(r["range_high"])} if r["range_high"] else None}]
                if r["range_low"] or r["range_high"] else None,
            }} for r in rows]
        }
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# MedicationStatement (drug_exposure 테이블)
# ══════════════════════════════════════════

@router.get("/MedicationStatement")
async def search_medications(
    patient: Optional[int] = None, _count: int = Query(20, le=100, alias="_count"),
):
    conn = await _get_conn()
    try:
        q = ("SELECT drug_exposure_id, person_id, drug_source_value, drug_exposure_start_date, "
             "drug_exposure_end_date, quantity, days_supply, route_source_value "
             "FROM drug_exposure WHERE 1=1")
        params, idx = [], 1
        if patient:
            q += f" AND person_id = ${idx}"; params.append(patient); idx += 1
        q += f" ORDER BY drug_exposure_start_date DESC LIMIT ${idx}"
        params.append(_count)
        rows = await conn.fetch(q, *params)
        return {
            "resourceType": "Bundle", "type": "searchset", "total": len(rows),
            "entry": [{"resource": {
                "resourceType": "MedicationStatement",
                "id": str(r["drug_exposure_id"]),
                "status": "active",
                "subject": {"reference": f"Patient/{r['person_id']}"},
                "medicationCodeableConcept": {"coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": r["drug_source_value"] or ""}]},
                "effectivePeriod": {
                    "start": r["drug_exposure_start_date"].isoformat() if r["drug_exposure_start_date"] else None,
                    "end": r["drug_exposure_end_date"].isoformat() if r["drug_exposure_end_date"] else None,
                },
                "dosage": [{"route": {"text": r["route_source_value"] or ""}, "doseAndRate": [{"doseQuantity": {"value": float(r["quantity"]) if r["quantity"] else None}}]}]
            }} for r in rows]
        }
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# Procedure (procedure_occurrence)
# ══════════════════════════════════════════

@router.get("/Procedure")
async def search_procedures(
    patient: Optional[int] = None, _count: int = Query(20, le=100, alias="_count"),
):
    conn = await _get_conn()
    try:
        q = ("SELECT procedure_occurrence_id, person_id, procedure_source_value, procedure_date "
             "FROM procedure_occurrence WHERE 1=1")
        params, idx = [], 1
        if patient:
            q += f" AND person_id = ${idx}"; params.append(patient); idx += 1
        q += f" ORDER BY procedure_date DESC LIMIT ${idx}"
        params.append(_count)
        rows = await conn.fetch(q, *params)
        return {
            "resourceType": "Bundle", "type": "searchset", "total": len(rows),
            "entry": [{"resource": {
                "resourceType": "Procedure",
                "id": str(r["procedure_occurrence_id"]),
                "status": "completed",
                "subject": {"reference": f"Patient/{r['person_id']}"},
                "code": {"coding": [{"system": "http://snomed.info/sct", "code": r["procedure_source_value"] or ""}]},
                "performedDateTime": r["procedure_date"].isoformat() if r["procedure_date"] else None,
            }} for r in rows]
        }
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# Encounter (visit_occurrence)
# ══════════════════════════════════════════

@router.get("/Encounter")
async def search_encounters(
    patient: Optional[int] = None, _count: int = Query(20, le=100, alias="_count"),
):
    conn = await _get_conn()
    try:
        q = ("SELECT visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_end_date "
             "FROM visit_occurrence WHERE 1=1")
        params, idx = [], 1
        if patient:
            q += f" AND person_id = ${idx}"; params.append(patient); idx += 1
        q += f" ORDER BY visit_start_date DESC LIMIT ${idx}"
        params.append(_count)
        rows = await conn.fetch(q, *params)
        return {
            "resourceType": "Bundle", "type": "searchset", "total": len(rows),
            "entry": [{"resource": {
                "resourceType": "Encounter",
                "id": str(r["visit_occurrence_id"]),
                "status": "finished",
                "class": _visit_class(r["visit_concept_id"]),
                "subject": {"reference": f"Patient/{r['person_id']}"},
                "period": {
                    "start": r["visit_start_date"].isoformat() if r["visit_start_date"] else None,
                    "end": r["visit_end_date"].isoformat() if r["visit_end_date"] else None,
                }
            }} for r in rows]
        }
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# Patient/$everything (전체 데이터 번들)
# ══════════════════════════════════════════

@router.get("/Patient/{person_id}/$everything")
async def patient_everything(person_id: int, _count: int = Query(50, le=200, alias="_count")):
    """Patient/$everything — 환자의 전체 임상 데이터 번들"""
    conn = await _get_conn()
    try:
        # 환자 기본
        patient = await conn.fetchrow(
            "SELECT person_id, gender_source_value, year_of_birth, month_of_birth, day_of_birth, "
            "race_source_value, ethnicity_source_value FROM person WHERE person_id = $1", person_id)
        if not patient:
            raise HTTPException(404, "Patient not found")

        entries = [{"resource": _person_to_patient(patient), "search": {"mode": "match"}}]

        # Conditions
        conditions = await conn.fetch(
            "SELECT condition_occurrence_id, person_id, condition_source_value, "
            "condition_start_date, condition_end_date FROM condition_occurrence "
            "WHERE person_id = $1 ORDER BY condition_start_date DESC LIMIT $2", person_id, _count)
        for r in conditions:
            entries.append({"resource": {
                "resourceType": "Condition",
                "id": str(r["condition_occurrence_id"]),
                "subject": {"reference": f"Patient/{person_id}"},
                "code": {"coding": [{"system": "http://snomed.info/sct", "code": r["condition_source_value"] or ""}]},
                "onsetDateTime": r["condition_start_date"].isoformat() if r["condition_start_date"] else None,
                "abatementDateTime": r["condition_end_date"].isoformat() if r["condition_end_date"] else None,
            }, "search": {"mode": "include"}})

        # Observations (measurement)
        measurements = await conn.fetch(
            "SELECT measurement_id, measurement_source_value, measurement_date, "
            "value_as_number, unit_source_value FROM measurement "
            "WHERE person_id = $1 ORDER BY measurement_date DESC LIMIT $2", person_id, _count)
        for r in measurements:
            entries.append({"resource": {
                "resourceType": "Observation",
                "id": str(r["measurement_id"]),
                "status": "final",
                "subject": {"reference": f"Patient/{person_id}"},
                "code": {"coding": [{"system": "http://loinc.org", "code": r["measurement_source_value"] or ""}]},
                "effectiveDateTime": r["measurement_date"].isoformat() if r["measurement_date"] else None,
                "valueQuantity": {"value": float(r["value_as_number"]), "unit": r["unit_source_value"] or ""}
                if r["value_as_number"] is not None else None,
            }, "search": {"mode": "include"}})

        # MedicationStatements (drug_exposure)
        drugs = await conn.fetch(
            "SELECT drug_exposure_id, drug_source_value, drug_exposure_start_date, "
            "drug_exposure_end_date, quantity FROM drug_exposure "
            "WHERE person_id = $1 ORDER BY drug_exposure_start_date DESC LIMIT $2", person_id, _count)
        for r in drugs:
            entries.append({"resource": {
                "resourceType": "MedicationStatement",
                "id": str(r["drug_exposure_id"]),
                "status": "active",
                "subject": {"reference": f"Patient/{person_id}"},
                "medicationCodeableConcept": {"coding": [{"system": "http://www.nlm.nih.gov/research/umls/rxnorm", "code": r["drug_source_value"] or ""}]},
                "effectivePeriod": {
                    "start": r["drug_exposure_start_date"].isoformat() if r["drug_exposure_start_date"] else None,
                    "end": r["drug_exposure_end_date"].isoformat() if r["drug_exposure_end_date"] else None,
                },
            }, "search": {"mode": "include"}})

        # Procedures
        procedures = await conn.fetch(
            "SELECT procedure_occurrence_id, procedure_source_value, procedure_date "
            "FROM procedure_occurrence WHERE person_id = $1 ORDER BY procedure_date DESC LIMIT $2", person_id, _count)
        for r in procedures:
            entries.append({"resource": {
                "resourceType": "Procedure",
                "id": str(r["procedure_occurrence_id"]),
                "status": "completed",
                "subject": {"reference": f"Patient/{person_id}"},
                "code": {"coding": [{"system": "http://snomed.info/sct", "code": r["procedure_source_value"] or ""}]},
                "performedDateTime": r["procedure_date"].isoformat() if r["procedure_date"] else None,
            }, "search": {"mode": "include"}})

        # Encounters (visit_occurrence)
        visits = await conn.fetch(
            "SELECT visit_occurrence_id, visit_concept_id, visit_start_date, visit_end_date "
            "FROM visit_occurrence WHERE person_id = $1 ORDER BY visit_start_date DESC LIMIT $2", person_id, _count)
        for r in visits:
            entries.append({"resource": {
                "resourceType": "Encounter",
                "id": str(r["visit_occurrence_id"]),
                "status": "finished",
                "class": _visit_class(r["visit_concept_id"]),
                "subject": {"reference": f"Patient/{person_id}"},
                "period": {
                    "start": r["visit_start_date"].isoformat() if r["visit_start_date"] else None,
                    "end": r["visit_end_date"].isoformat() if r["visit_end_date"] else None,
                },
            }, "search": {"mode": "include"}})

        return {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": len(entries),
            "entry": entries,
        }
    finally:
        await _rel(conn)


# ══════════════════════════════════════════
# Bundle (배치 처리)
# ══════════════════════════════════════════

@router.post("/Bundle")
async def process_bundle(body: BundleRequest):
    """FHIR Bundle 배치 처리"""
    if body.type != "batch":
        raise HTTPException(400, "batch 타입만 지원됩니다")

    results = []
    for entry in body.entry[:50]:  # 최대 50개
        try:
            # URL 파싱: "Patient/123" 등
            parts = entry.url.strip("/").split("/")
            if len(parts) < 1:
                results.append({"status": "400", "error": "잘못된 URL"})
                continue

            resource_type = parts[0]
            resource_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None

            conn = await _get_conn()
            try:
                if resource_type == "Patient" and resource_id:
                    row = await conn.fetchrow(
                        "SELECT person_id, gender_source_value, year_of_birth, month_of_birth, "
                        "day_of_birth, race_source_value, ethnicity_source_value "
                        "FROM person WHERE person_id = $1", resource_id)
                    if row:
                        results.append({"status": "200", "resource": _person_to_patient(row)})
                    else:
                        results.append({"status": "404", "error": "Not found"})
                elif resource_type == "Condition" and resource_id:
                    row = await conn.fetchrow(
                        "SELECT condition_occurrence_id, person_id, condition_source_value, "
                        "condition_start_date, condition_end_date FROM condition_occurrence "
                        "WHERE condition_occurrence_id = $1", resource_id)
                    if row:
                        results.append({"status": "200", "resource": {
                            "resourceType": "Condition",
                            "id": str(row["condition_occurrence_id"]),
                            "subject": {"reference": f"Patient/{row['person_id']}"},
                            "code": {"coding": [{"system": "http://snomed.info/sct",
                                                  "code": row["condition_source_value"] or ""}]},
                            "onsetDateTime": row["condition_start_date"].isoformat() if row["condition_start_date"] else None,
                        }})
                    else:
                        results.append({"status": "404", "error": "Not found"})
                elif resource_type == "Encounter" and resource_id:
                    row = await conn.fetchrow(
                        "SELECT visit_occurrence_id, person_id, visit_concept_id, "
                        "visit_start_date, visit_end_date FROM visit_occurrence "
                        "WHERE visit_occurrence_id = $1", resource_id)
                    if row:
                        results.append({"status": "200", "resource": {
                            "resourceType": "Encounter",
                            "id": str(row["visit_occurrence_id"]),
                            "status": "finished",
                            "class": _visit_class(row["visit_concept_id"]),
                            "subject": {"reference": f"Patient/{row['person_id']}"},
                        }})
                    else:
                        results.append({"status": "404", "error": "Not found"})
                else:
                    results.append({"status": "400", "error": f"지원하지 않는 리소스: {resource_type}"})
            finally:
                await _rel(conn)
        except Exception as e:
            results.append({"status": "500", "error": str(e)[:200]})

    return {
        "resourceType": "Bundle",
        "type": "batch-response",
        "entry": [{"response": r} for r in results],
    }


# ══════════════════════════════════════════
# 통계
# ══════════════════════════════════════════

@router.get("/stats")
async def fhir_stats():
    """FHIR 리소스 통계"""
    conn = await _get_conn()
    try:
        stats = {}
        for tbl, res in [("person","Patient"), ("condition_occurrence","Condition"),
                         ("measurement","Observation"), ("drug_exposure","MedicationStatement"),
                         ("procedure_occurrence","Procedure"), ("visit_occurrence","Encounter")]:
            cnt = await conn.fetchval(f"SELECT n_live_tup FROM pg_stat_user_tables WHERE relname='{tbl}'")
            stats[res] = cnt or 0
        return {"resourceType": "Parameters", "parameter": [{"name": k, "valueInteger": v} for k, v in stats.items()]}
    finally:
        await _rel(conn)
