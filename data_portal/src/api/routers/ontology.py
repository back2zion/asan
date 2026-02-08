"""
Medical Ontology Knowledge Graph — OMOP CDM 기반 의료 온톨로지

Sub-modules:
  - ontology_constants: Static reference data (SNOMED/RxNorm/LOINC lookups, CDM schema, relationships)
  - ontology_data:      DB queries, graph construction, cache, Neo4j/RDF export
  - ontology_concepts:  API endpoints (graph, search, node detail, stats, triples)
"""
from fastapi import APIRouter

from .ontology_concepts import router as concepts_router
from .ontology_data import router as data_router
from .ontology_data import warm_ontology_cache  # re-export for main.py lifespan

router = APIRouter(prefix="/ontology", tags=["Ontology"])
router.include_router(concepts_router)
router.include_router(data_router)

__all__ = ["router", "warm_ontology_cache"]
