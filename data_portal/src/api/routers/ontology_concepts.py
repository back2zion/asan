"""
Medical Ontology Knowledge Graph — Concept endpoints

- /ontology/graph — Knowledge graph with filtering
- /ontology/search — Search ontology nodes
- /ontology/node/{node_id} — Node detail + neighbors
- /ontology/triples — RDF triple listing
- /ontology/stats — Ontology statistics
"""

from typing import Optional
from collections import defaultdict

from fastapi import APIRouter, HTTPException, Query

from .ontology_data import get_or_build_graph, _generate_rdf_triples
from .ontology_constants import VOCABULARY_NODES, BODY_SYSTEMS, DRUG_CLASSES

router = APIRouter()


@router.get("/graph")
async def get_ontology_graph(
    graph_type: str = Query("full", pattern="^(full|schema|medical|causality|vocabulary)$"),
    force_refresh: bool = Query(False),
):
    """
    OMOP CDM 의료 온톨로지 Knowledge Graph 조회

    - full: 전체 온톨로지 (스키마 + 의료 개념 + 인과관계 + 표준용어)
    - schema: OMOP CDM 스키마 관계만
    - medical: 의료 개념 노드 + 치료/진단 관계
    - causality: 인과 관계 체인
    - vocabulary: 표준 용어 체계
    """
    graph = await get_or_build_graph(force_refresh=force_refresh)

    if graph_type == "full":
        return graph

    # Filter based on type
    type_filters = {
        "schema": {"node_types": {"domain"}, "link_types": {"schema_fk"}},
        "medical": {"node_types": {"condition", "drug", "measurement", "procedure", "body_system",
                                    "drug_class", "visit"},
                    "link_types": {"treatment", "diagnostic", "comorbidity",
                                   "taxonomy", "drug_classification",
                                   "data_instance", "pharmacology", "visit_classification"}},
        "causality": {"node_types": {"causal", "condition"},
                      "link_types": {"causality", "causal_chain", "comorbidity", "includes_step"}},
        "vocabulary": {"node_types": {"domain", "vocabulary"},
                       "link_types": {"vocabulary_mapping", "schema_fk"}},
    }

    filt = type_filters.get(graph_type, {})
    allowed_node_types = filt.get("node_types", set())
    allowed_link_types = filt.get("link_types", set())

    filtered_nodes = [n for n in graph["nodes"] if n["type"] in allowed_node_types]
    filtered_ids = {n["id"] for n in filtered_nodes}
    filtered_links = [
        l for l in graph["links"]
        if l["type"] in allowed_link_types and l["source"] in filtered_ids and l["target"] in filtered_ids
    ]
    filtered_triples = [
        t for t in graph["triples"]
        if t["subject"] in filtered_ids and t["object"] in filtered_ids
    ]

    return {
        "nodes": filtered_nodes,
        "links": filtered_links,
        "triples": filtered_triples,
        "stats": {
            "total_nodes": len(filtered_nodes),
            "total_links": len(filtered_links),
            "total_triples": len(filtered_triples),
            "filter": graph_type,
        },
        "causal_chains": graph.get("causal_chains", []) if graph_type == "causality" else [],
        "built_at": graph["built_at"],
    }


@router.get("/triples")
async def get_triples(
    triple_type: Optional[str] = Query(None),
    subject_type: Optional[str] = Query(None),
    limit: int = Query(200, le=1000),
):
    """
    RDF Triple 목록 조회 — Subject -> Predicate -> Object 형식

    필터: triple_type (treatment, diagnostic, comorbidity, ...), subject_type (condition, drug, ...)
    """
    graph = await get_or_build_graph()

    rdf_triples = _generate_rdf_triples(graph)

    if triple_type:
        rdf_triples = [t for t in rdf_triples if t["triple_type"] == triple_type]
    if subject_type:
        rdf_triples = [t for t in rdf_triples if t["subject"]["type"] == subject_type]

    triple_types = defaultdict(int)
    for t in rdf_triples:
        triple_types[t["triple_type"]] += 1

    return {
        "triples": rdf_triples[:limit],
        "total": len(rdf_triples),
        "returned": min(len(rdf_triples), limit),
        "type_distribution": dict(triple_types),
    }


@router.get("/stats")
async def get_ontology_stats():
    """온톨로지 Knowledge Graph 통계"""
    graph = await get_or_build_graph()

    node_type_counts = defaultdict(int)
    for n in graph["nodes"]:
        node_type_counts[n["type"]] += 1

    link_type_counts = defaultdict(int)
    for l in graph["links"]:
        link_type_counts[l["type"]] += 1

    return {
        "total_nodes": len(graph["nodes"]),
        "total_links": len(graph["links"]),
        "total_triples": len(graph["triples"]),
        "node_types": dict(sorted(node_type_counts.items(), key=lambda x: -x[1])),
        "link_types": dict(sorted(link_type_counts.items(), key=lambda x: -x[1])),
        "causal_chains": len(graph.get("causal_chains", [])),
        "vocabulary_standards": len(VOCABULARY_NODES),
        "body_systems": len(BODY_SYSTEMS),
        "drug_classes": len(DRUG_CLASSES),
        "demographics": graph["stats"].get("demographics", {}),
        "data_volume": graph["stats"].get("table_stats", {}),
        "built_at": graph["built_at"],
    }


@router.get("/node/{node_id}")
async def get_node_detail(node_id: str):
    """특정 노드 상세 + 이웃 노드"""
    graph = await get_or_build_graph()

    node = next((n for n in graph["nodes"] if n["id"] == node_id), None)
    if not node:
        raise HTTPException(status_code=404, detail=f"노드 '{node_id}'를 찾을 수 없습니다")

    # Find neighbors
    outgoing = [l for l in graph["links"] if l["source"] == node_id]
    incoming = [l for l in graph["links"] if l["target"] == node_id]

    neighbor_ids = set()
    for l in outgoing:
        neighbor_ids.add(l["target"])
    for l in incoming:
        neighbor_ids.add(l["source"])

    neighbors = [n for n in graph["nodes"] if n["id"] in neighbor_ids]

    # Related triples
    node_triples = [
        t for t in graph["triples"]
        if t["subject"] == node_id or t["object"] == node_id
    ]

    return {
        "node": node,
        "outgoing_links": outgoing,
        "incoming_links": incoming,
        "neighbors": neighbors,
        "triples": node_triples,
        "degree": len(outgoing) + len(incoming),
    }


@router.get("/search")
async def search_ontology(q: str = Query(..., min_length=1)):
    """온톨로지 검색 — 노드 label/id 기반"""
    graph = await get_or_build_graph()

    q_lower = q.lower()
    results = []
    for n in graph["nodes"]:
        score = 0
        if q_lower == n["label"].lower():
            score = 100
        elif q_lower in n["label"].lower():
            score = 80
        elif q_lower in n["id"].lower():
            score = 60
        elif q_lower in n.get("full_label", "").lower():
            score = 70
        elif q_lower in n.get("description", "").lower():
            score = 40

        if score > 0:
            results.append({**n, "relevance_score": score})

    results.sort(key=lambda x: -x["relevance_score"])
    return {"query": q, "results": results[:20], "total": len(results)}
