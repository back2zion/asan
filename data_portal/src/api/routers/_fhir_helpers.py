"""
FHIR helpers — DB connection, mapping functions, Pydantic models.
Extracted from fhir.py to reduce file size.
"""
from typing import List
from datetime import date

from pydantic import BaseModel


# ── DB helpers ──

async def _get_conn():
    from services.db_pool import get_pool
    pool = await get_pool()
    return await pool.acquire()


async def _rel(conn):
    from services.db_pool import get_pool
    pool = await get_pool()
    await pool.release(conn)


# ── FHIR R4 매핑 함수들 ──

def _gender_map(src: str) -> str:
    return {"M": "male", "F": "female"}.get(src, "unknown")


def _visit_class(concept_id: int) -> dict:
    m = {9201: ("IMP", "inpatient"), 9202: ("AMB", "ambulatory"), 9203: ("EMER", "emergency")}
    code, display = m.get(concept_id, ("UNK", "unknown"))
    return {"system": "http://terminology.hl7.org/CodeSystem/v3-ActCode", "code": code, "display": display}


def _person_to_patient(r) -> dict:
    bd = None
    if r["year_of_birth"]:
        m = r["month_of_birth"] or 1
        d = r["day_of_birth"] or 1
        try:
            bd = date(r["year_of_birth"], m, d).isoformat()
        except Exception:
            bd = f"{r['year_of_birth']}-01-01"
    return {
        "resourceType": "Patient",
        "id": str(r["person_id"]),
        "gender": _gender_map(r["gender_source_value"] or ""),
        "birthDate": bd,
        "extension": [
            {"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
             "valueString": r["race_source_value"] or "unknown"},
        ],
    }


# ── Pydantic 모델 ──

class BundleEntry(BaseModel):
    method: str = "GET"
    url: str


class BundleRequest(BaseModel):
    resourceType: str = "Bundle"
    type: str = "batch"
    entry: List[BundleEntry]
