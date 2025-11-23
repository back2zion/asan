from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, List

router = APIRouter()


class OLAPQuery(BaseModel):
    dimensions: List[str]
    metrics: List[str]
    filters: Dict[str, Any] = {}


@router.post("/olap/query")
async def olap_query(query: OLAPQuery):
    # Return sample dataset compatible with frontend sampleData
    sample_data = [
        {"name": "20대", "patient_count": 150, "avg_duration": 3.2},
        {"name": "30대", "patient_count": 230, "avg_duration": 4.5},
        {"name": "40대", "patient_count": 280, "avg_duration": 5.1},
        {"name": "50대", "patient_count": 320, "avg_duration": 6.8},
        {"name": "60대", "patient_count": 250, "avg_duration": 7.2},
    ]
    return {"data": sample_data}


class Neo4jRequest(BaseModel):
    cypher: str


@router.post("/neo4j/query")
async def neo4j_query(req: Neo4jRequest):
    # Very small mock graph response
    nodes = [
        {"id": 1, "labels": ["Patient"], "properties": {"name": "김환자", "age": 68}},
        {
            "id": 2,
            "labels": ["Doctor"],
            "properties": {"name": "Dr. Lee", "dept": "Cardiology"},
        },
        {
            "id": 3,
            "labels": ["Diagnosis"],
            "properties": {"code": "I21", "desc": "AMI"},
        },
    ]
    relationships = [
        {"from": 1, "to": 2, "type": "CARED_BY"},
        {"from": 1, "to": 3, "type": "HAS_DIAGNOSIS"},
    ]
    return {"nodes": nodes, "relationships": relationships}


@router.post("/postgres/{query_name}")
async def postgres_query(query_name: str, payload: Dict[str, Any] = {}):
    # Simple switch to return different mock datasets by name
    if query_name == "patients_sample":
        rows = [{"id": 1, "name": "김환자"}, {"id": 2, "name": "이환자"}]
    else:
        rows = [{"ok": True, "query": query_name}]
    return {"rows": rows}


@router.get("/graph/nodes")
async def graph_nodes(type: str = None):
    data = []
    if not type or type.lower() == "patient":
        data = [
            {"id": "p1", "label": "Patient", "name": "김환자"},
            {"id": "p2", "label": "Patient", "name": "이환자"},
        ]
    elif type.lower() == "doctor":
        data = [{"id": "d1", "label": "Doctor", "name": "Dr. Lee"}]
    else:
        data = [{"id": "n1", "label": "Node", "name": "Sample"}]
    return {"nodes": data}
