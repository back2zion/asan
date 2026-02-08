"""
Medical Ontology Knowledge Graph — Data layer

DB queries, graph construction, cache management, Neo4j/RDF export.
Constants and reference data are in ontology_constants.py.
"""

import os
import json
import math
import time
from datetime import datetime
from typing import Optional, Dict, Any, List
from collections import defaultdict

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import asyncpg

from services.db_pool import get_pool

from .ontology_constants import (
    NODE_COLORS, CDM_DOMAIN_COLORS, EXCLUDED_SNOMED_CODES,
    _resolve_concept_name, OMOP_TABLE_META, OMOP_FK_RELATIONSHIPS,
    VOCABULARY_NODES, BODY_SYSTEMS, DRUG_CLASSES,
    TREATMENT_RELATIONSHIPS, DIAGNOSTIC_RELATIONSHIPS,
    COMORBIDITY_RELATIONSHIPS, CAUSAL_CHAINS,
)

router = APIRouter()

OMOP_DB_CONFIG = {
    "host": os.getenv("OMOP_DB_HOST", "localhost"),
    "port": int(os.getenv("OMOP_DB_PORT", "5436")),
    "user": os.getenv("OMOP_DB_USER", "omopuser"),
    "password": os.getenv("OMOP_DB_PASSWORD", "omop"),
    "database": os.getenv("OMOP_DB_NAME", "omop_cdm"),
}


async def _get_conn():
    try:
        pool = await get_pool()
        return await pool.acquire()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB 연결 실패: {e}")


async def _release_conn(conn):
    try:
        pool = await get_pool()
        await pool.release(conn)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════
#  GRAPH CACHE
# ══════════════════════════════════════════════════════════════════════

_GRAPH_CACHE: Dict[str, Any] = {"data": None, "built_at": 0}
_CACHE_TTL = 600  # 10 minutes

# Persistent disk cache — survives server restarts
_CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "etl_data")
_DISK_CACHE_FILE = os.path.join(_CACHE_DIR, "ontology_cache.json")


def _save_disk_cache(graph: Dict[str, Any]) -> None:
    """Save built graph to disk for persistence across restarts"""
    try:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(_DISK_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(graph, f, ensure_ascii=False)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Disk cache save failed: {e}")


def _load_disk_cache() -> Optional[Dict[str, Any]]:
    """Load graph from disk cache if available and not too old (24h)"""
    try:
        if not os.path.exists(_DISK_CACHE_FILE):
            return None
        age = time.time() - os.path.getmtime(_DISK_CACHE_FILE)
        if age > 86400:
            return None
        with open(_DISK_CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


async def warm_ontology_cache() -> None:
    """Pre-build ontology cache (call on startup). Safe to call multiple times."""
    global _GRAPH_CACHE
    import logging
    log = logging.getLogger(__name__)

    # 1) Try disk cache first (instant)
    if not _GRAPH_CACHE["data"]:
        disk = _load_disk_cache()
        if disk:
            _GRAPH_CACHE["data"] = disk
            _GRAPH_CACHE["built_at"] = time.time()
            log.info("Ontology cache loaded from disk (instant)")

    # 2) Build fresh in background regardless (updates disk + memory)
    try:
        log.info("Ontology cache warming: building full graph...")
        t0 = time.time()
        graph = await _build_full_ontology()
        elapsed = time.time() - t0
        _GRAPH_CACHE["data"] = graph
        _GRAPH_CACHE["built_at"] = time.time()
        _save_disk_cache(graph)
        log.info(f"Ontology cache warmed in {elapsed:.1f}s ({len(graph['nodes'])} nodes, {len(graph['links'])} links)")
    except Exception as e:
        log.warning(f"Ontology cache warming failed: {e}")


# ══════════════════════════════════════════════════════════════════════
#  DATABASE QUERIES
# ══════════════════════════════════════════════════════════════════════

async def _query_table_stats(conn) -> Dict[str, int]:
    """Row counts from pg_stat_user_tables"""
    rows = await conn.fetch("""
        SELECT relname, n_live_tup
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
    """)
    return {r["relname"]: int(r["n_live_tup"]) for r in rows}


async def _query_top_concepts(conn, table: str, concept_col: str, source_col: str,
                               limit: int = 25, exclude_codes: set = None) -> List[Dict]:
    """Top N concepts by frequency from a clinical table"""
    try:
        exclude = exclude_codes or set()
        fetch_limit = limit + len(exclude) + 10
        rows = await conn.fetch(f"""
            SELECT {concept_col} as concept_id,
                   {source_col} as source_value,
                   COUNT(*) as record_count,
                   COUNT(DISTINCT person_id) as patient_count
            FROM {table}
            WHERE {source_col} IS NOT NULL AND {source_col} != ''
            GROUP BY {concept_col}, {source_col}
            ORDER BY record_count DESC
            LIMIT $1
        """, fetch_limit)
        results = []
        for r in rows:
            sv = str(r["source_value"]).strip()
            if sv in exclude:
                continue
            results.append({
                "concept_id": r["concept_id"], "source_value": sv,
                "record_count": r["record_count"], "patient_count": r["patient_count"],
            })
            if len(results) >= limit:
                break
        return results
    except Exception:
        return []


async def _query_visit_types(conn) -> List[Dict]:
    """Visit type distribution"""
    try:
        rows = await conn.fetch("""
            SELECT visit_concept_id, COUNT(*) as cnt,
                   COUNT(DISTINCT person_id) as patients
            FROM visit_occurrence
            GROUP BY visit_concept_id
            ORDER BY cnt DESC
        """)
        type_map = {9201: "입원 (Inpatient)", 9202: "외래 (Outpatient)", 9203: "응급 (Emergency)"}
        return [
            {"concept_id": r["visit_concept_id"],
             "label": type_map.get(r["visit_concept_id"], f"Visit #{r['visit_concept_id']}"),
             "count": r["cnt"], "patients": r["patients"]}
            for r in rows
        ]
    except Exception:
        return []


async def _query_demographics(conn) -> Dict:
    """Basic demographics from person table"""
    try:
        gender = await conn.fetch("""
            SELECT gender_source_value, COUNT(*) as cnt
            FROM person GROUP BY gender_source_value
        """)
        total = await conn.fetchval("SELECT COUNT(*) FROM person")
        return {
            "total_patients": total,
            "gender": {r["gender_source_value"]: r["cnt"] for r in gender},
        }
    except Exception:
        return {"total_patients": 0, "gender": {}}


# ══════════════════════════════════════════════════════════════════════
#  GRAPH CONSTRUCTION
# ══════════════════════════════════════════════════════════════════════

def _node_id(prefix: str, name: str) -> str:
    """Generate stable node ID"""
    clean = name.replace(" ", "_").replace("/", "_").lower()
    return f"{prefix}_{clean}"


async def _build_full_ontology() -> Dict[str, Any]:
    """Build the complete ontology graph from OMOP CDM"""
    conn = await _get_conn()
    try:
        nodes = []
        links = []
        triples = []
        node_ids = set()

        def add_node(nid, label, ntype, **kwargs):
            if nid not in node_ids:
                node_ids.add(nid)
                color = kwargs.pop("color_override", None) or NODE_COLORS.get(ntype, "#718096")
                node = {
                    "id": nid, "label": label, "type": ntype,
                    "color": color,
                    "size": kwargs.get("size", 8),
                    **{k: v for k, v in kwargs.items() if k != "size"},
                }
                nodes.append(node)

        link_keys = set()

        def add_link(source, target, label, ltype, **kwargs):
            if source in node_ids and target in node_ids:
                key = (source, target, ltype)
                if key in link_keys:
                    return
                link_keys.add(key)
                links.append({
                    "source": source, "target": target,
                    "label": label, "type": ltype,
                    **kwargs,
                })
                triples.append({
                    "subject": source, "predicate": label, "object": target,
                    "type": ltype,
                    "description": kwargs.get("description", ""),
                })

        # ── Layer 1: OMOP CDM Schema Tables ──
        table_stats = await _query_table_stats(conn)

        for table_name, meta in OMOP_TABLE_META.items():
            db_name = meta.get("db_table", table_name)
            row_count = table_stats.get(db_name, 0)
            size = 8 + (math.log10(max(row_count, 1)) - 3) * 2.5 if row_count > 0 else 8
            size = max(8, min(18, size))
            domain_color = CDM_DOMAIN_COLORS.get(meta["domain"], NODE_COLORS["domain"])
            add_node(
                f"table_{table_name}", meta["label"], "domain",
                size=size, row_count=row_count, domain=meta["domain"],
                icon=meta["icon"], description=meta["description"],
                table_name=table_name, color_override=domain_color,
            )

        for src, tgt, rel, desc in OMOP_FK_RELATIONSHIPS:
            add_link(f"table_{src}", f"table_{tgt}", rel, "schema_fk", description=desc)

        # ── Layer 2: Vocabulary Standards ──
        for v in VOCABULARY_NODES:
            add_node(v["id"], v["label"], "vocabulary", size=10,
                     full_name=v["full_name"], vocab_domain=v["domain"],
                     concept_count=v["concepts"])

        vocab_table_links = [
            ("vocab_snomed", "table_condition_occurrence", "encodes", "SNOMED CT -> 진단 코딩"),
            ("vocab_snomed", "table_procedure_occurrence", "encodes", "SNOMED CT -> 시술 코딩"),
            ("vocab_icd10", "table_condition_occurrence", "maps_to", "ICD-10 -> 진단 매핑"),
            ("vocab_kcd7", "table_condition_occurrence", "maps_to", "KCD-7 -> 한국 진단 매핑"),
            ("vocab_rxnorm", "table_drug_exposure", "encodes", "RxNorm -> 약물 코딩"),
            ("vocab_atc", "table_drug_exposure", "classifies", "ATC -> 약물 분류"),
            ("vocab_edi", "table_drug_exposure", "maps_to", "EDI -> 건강보험 약물 코드"),
            ("vocab_loinc", "table_measurement", "encodes", "LOINC -> 검사 코딩"),
            ("vocab_cpt4", "table_procedure_occurrence", "encodes", "CPT-4 -> 시술 코딩"),
            ("vocab_hcpcs", "table_procedure_occurrence", "encodes", "HCPCS -> 시술/기기 코딩"),
            ("vocab_edi", "table_procedure_occurrence", "maps_to", "EDI -> 건강보험 시술 코드"),
            ("vocab_mesh", "table_observation", "references", "MeSH -> 관찰 참조"),
            ("vocab_omop", "table_person", "defines", "OMOP CDM 표준 스키마"),
        ]
        for vs, vt, vl, vd in vocab_table_links:
            add_link(vs, vt, vl, "vocabulary_mapping", description=vd)

        # ── Layer 3: Body Systems ──
        for bs in BODY_SYSTEMS:
            add_node(bs["id"], bs["label"], "body_system", size=14)

        # ── Layer 4: Drug Classes ──
        for dc in DRUG_CLASSES:
            add_node(dc["id"], dc["label"], "drug_class", size=11, atc=dc["atc"])
            add_link(dc["id"], dc["target_system"], "targets", "pharmacology",
                     description=f"{dc['label']} -> {dc['target_system']}")

        # ── Layer 5: Top Conditions from Data ──
        top_conditions = await _query_top_concepts(
            conn, "condition_occurrence",
            "condition_concept_id", "condition_source_value", 30,
            exclude_codes=EXCLUDED_SNOMED_CODES
        )
        for c in top_conditions:
            resolved = _resolve_concept_name(c["source_value"], c["concept_id"], "condition")
            nid = _node_id("cond", c["source_value"])
            label_short = resolved[:40] + ("..." if len(resolved) > 40 else "")
            add_node(nid, label_short, "condition", size=max(10, min(24, 10 + c["patient_count"] / 5000)),
                     concept_id=c["concept_id"], record_count=c["record_count"],
                     patient_count=c["patient_count"], full_label=resolved,
                     source_code=c["source_value"])
            add_link(f"table_condition_occurrence", nid, "contains", "data_instance",
                     description=f"진단 기록 {c['record_count']:,}건")
            for bs in BODY_SYSTEMS:
                for bc in bs["conditions"]:
                    if bc.lower() in resolved.lower() or resolved.lower() in bc.lower():
                        add_link(nid, bs["id"], "belongs_to", "taxonomy",
                                 description=f"{label_short} -> {bs['label']}")
                        break

        # ── Layer 6: Top Drugs from Data ──
        top_drugs = await _query_top_concepts(
            conn, "drug_exposure",
            "drug_concept_id", "drug_source_value", 30
        )
        for d in top_drugs:
            resolved = _resolve_concept_name(d["source_value"], d["concept_id"], "drug")
            nid = _node_id("drug", d["source_value"])
            label_short = resolved[:40] + ("..." if len(resolved) > 40 else "")
            add_node(nid, label_short, "drug", size=max(10, min(22, 10 + d["patient_count"] / 5000)),
                     concept_id=d["concept_id"], record_count=d["record_count"],
                     patient_count=d["patient_count"], full_label=resolved,
                     source_code=d["source_value"])
            add_link(f"table_drug_exposure", nid, "contains", "data_instance",
                     description=f"투약 기록 {d['record_count']:,}건")
            for dc in DRUG_CLASSES:
                for dd in dc["drugs"]:
                    if dd.lower() in resolved.lower():
                        add_link(nid, dc["id"], "member_of", "drug_classification",
                                 description=f"{label_short} ∈ {dc['label']}")
                        break

        # ── Layer 7: Top Measurements from Data ──
        top_measurements = await _query_top_concepts(
            conn, "measurement",
            "measurement_concept_id", "measurement_source_value", 25
        )
        for m in top_measurements:
            resolved = _resolve_concept_name(m["source_value"], m["concept_id"], "measurement")
            nid = _node_id("meas", m["source_value"])
            label_short = resolved[:35] + ("..." if len(resolved) > 35 else "")
            add_node(nid, label_short, "measurement", size=max(9, min(20, 9 + m["patient_count"] / 6000)),
                     concept_id=m["concept_id"], record_count=m["record_count"],
                     patient_count=m["patient_count"], full_label=resolved,
                     source_code=m["source_value"])
            add_link(f"table_measurement", nid, "contains", "data_instance",
                     description=f"검사 기록 {m['record_count']:,}건")
            for bs in BODY_SYSTEMS:
                for bm in bs.get("measurements", []):
                    if bm.lower() in resolved.lower() or resolved.lower() in bm.lower():
                        add_link(nid, bs["id"], "measures", "diagnostic",
                                 description=f"{label_short} -> {bs['label']}")
                        break

        # ── Layer 8: Top Procedures from Data ──
        top_procedures = await _query_top_concepts(
            conn, "procedure_occurrence",
            "procedure_concept_id", "procedure_source_value", 20
        )
        for p in top_procedures:
            resolved = _resolve_concept_name(p["source_value"], p["concept_id"], "procedure")
            nid = _node_id("proc", p["source_value"])
            label_short = resolved[:35] + ("..." if len(resolved) > 35 else "")
            add_node(nid, label_short, "procedure", size=max(9, min(20, 9 + p["patient_count"] / 6000)),
                     concept_id=p["concept_id"], record_count=p["record_count"],
                     patient_count=p["patient_count"], full_label=resolved,
                     source_code=p["source_value"])
            add_link(f"table_procedure_occurrence", nid, "contains", "data_instance",
                     description=f"시술 기록 {p['record_count']:,}건")

        # ── Layer 9: Visit Types ──
        visit_types = await _query_visit_types(conn)
        for vt in visit_types:
            nid = f"visit_type_{vt['concept_id']}"
            add_node(nid, vt["label"], "visit", size=max(6, min(18, 6 + vt["count"] / 500000)),
                     concept_id=vt["concept_id"], visit_count=vt["count"], patients=vt["patients"])
            add_link("table_visit_occurrence", nid, "includes", "visit_classification",
                     description=f"{vt['label']} {vt['count']:,}건")

        # ── Layer 10: Treatment Relationships (knowledge-based) ──
        for cond_name, drug_name, rel_type, confidence, desc in TREATMENT_RELATIONSHIPS:
            cond_node = None
            drug_node = None
            for n in nodes:
                if n["type"] == "condition" and (
                    cond_name.lower() in n.get("full_label", n["label"]).lower()
                    or cond_name.lower() in n["label"].lower()
                ):
                    cond_node = n["id"]
                if n["type"] == "drug" and drug_name.lower() in n.get("full_label", n["label"]).lower():
                    drug_node = n["id"]
            if cond_node and drug_node:
                add_link(cond_node, drug_node, f"treated_with ({rel_type})", "treatment",
                         confidence=confidence, description=desc)

        # ── Layer 11: Diagnostic Relationships ──
        for cond_name, meas_name, rel_type, desc in DIAGNOSTIC_RELATIONSHIPS:
            cond_node = None
            meas_node = None
            for n in nodes:
                if n["type"] == "condition" and cond_name.lower() in n.get("full_label", n["label"]).lower():
                    cond_node = n["id"]
                if n["type"] == "measurement" and meas_name.lower() in n.get("full_label", n["label"]).lower():
                    meas_node = n["id"]
            if cond_node and meas_node:
                add_link(cond_node, meas_node, f"diagnosed_by ({rel_type})", "diagnostic",
                         description=desc)

        # ── Layer 12: Comorbidity Relationships ──
        for cond_a, cond_b, strength, desc in COMORBIDITY_RELATIONSHIPS:
            node_a = node_b = None
            for n in nodes:
                if n["type"] == "condition":
                    lbl = n.get("full_label", n["label"]).lower()
                    if cond_a.lower() in lbl:
                        node_a = n["id"]
                    if cond_b.lower() in lbl:
                        node_b = n["id"]
            if node_a and node_b:
                add_link(node_a, node_b, "comorbid_with", "comorbidity",
                         strength=strength, description=desc)

        # ── Layer 13: Causal Chains ──
        for chain in CAUSAL_CHAINS:
            add_node(chain["id"], chain["label"], "causal", size=12,
                     description=chain["description"])
            for i, step in enumerate(chain["path"]):
                step_id = f"{chain['id']}_step_{i}"
                add_node(step_id, step, "causal", size=6, chain=chain["id"], order=i)
                add_link(chain["id"], step_id, "includes_step", "causal_chain")
                if i > 0:
                    prev_id = f"{chain['id']}_step_{i-1}"
                    add_link(prev_id, step_id, "leads_to", "causality",
                             description=f"{chain['path'][i-1]} -> {step}")

        # Demographics
        demographics = await _query_demographics(conn)

        return {
            "nodes": nodes,
            "links": links,
            "triples": triples,
            "stats": {
                "total_nodes": len(nodes),
                "total_links": len(links),
                "total_triples": len(triples),
                "total_patients": demographics.get("total_patients", 0),
                "total_records": sum(table_stats.values()),
                "node_types": dict(sorted(
                    defaultdict(int, {n["type"]: 0 for n in nodes}).items()
                )),
                "link_types": dict(sorted(
                    defaultdict(int, {l["type"]: 0 for l in links}).items()
                )),
                "demographics": demographics,
                "table_stats": table_stats,
            },
            "causal_chains": CAUSAL_CHAINS,
            "built_at": datetime.now().isoformat(),
        }
    finally:
        await _release_conn(conn)


# ══════════════════════════════════════════════════════════════════════
#  NEO4J CYPHER EXPORT
# ══════════════════════════════════════════════════════════════════════

def _generate_cypher(graph: Dict) -> str:
    """Generate Neo4j Cypher import script"""
    lines = [
        "// ═══════════════════════════════════════════════",
        "// OMOP CDM 의료 온톨로지 Knowledge Graph",
        "// 서울아산병원 통합 데이터 플랫폼",
        f"// Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"// Nodes: {len(graph['nodes'])}, Relationships: {len(graph['links'])}",
        "// ═══════════════════════════════════════════════",
        "",
        "// Step 1: Clear existing data",
        "MATCH (n) DETACH DELETE n;",
        "",
        "// Step 2: Create constraints",
    ]

    node_types = set(n["type"] for n in graph["nodes"])
    for nt in sorted(node_types):
        label = nt.replace("_", " ").title().replace(" ", "")
        lines.append(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE;")

    lines.append("")
    lines.append("// Step 3: Create nodes")
    lines.append("")

    for n in graph["nodes"]:
        label = n["type"].replace("_", " ").title().replace(" ", "")
        props = {
            "id": n["id"],
            "label": n["label"],
            "type": n["type"],
        }
        if "row_count" in n:
            props["row_count"] = n["row_count"]
        if "record_count" in n:
            props["record_count"] = n["record_count"]
        if "patient_count" in n:
            props["patient_count"] = n["patient_count"]
        if "concept_id" in n:
            props["concept_id"] = n["concept_id"]
        if "description" in n:
            props["description"] = n["description"]

        prop_str = ", ".join(
            f"{k}: {json.dumps(v, ensure_ascii=False)}" if isinstance(v, str)
            else f"{k}: {v}"
            for k, v in props.items()
        )
        lines.append(f"CREATE (:{label} {{{prop_str}}});")

    lines.append("")
    lines.append("// Step 4: Create relationships")
    lines.append("")

    for l in graph["links"]:
        rel_type = l["label"].upper().replace(" ", "_").replace("(", "").replace(")", "")
        rel_type = "".join(c for c in rel_type if c.isalnum() or c == "_")
        if not rel_type:
            rel_type = "RELATED_TO"

        desc = l.get("description", "")
        desc_prop = f', description: "{desc}"' if desc else ""

        lines.append(
            f'MATCH (a {{id: "{l["source"]}"}}), (b {{id: "{l["target"]}"}}) '
            f'CREATE (a)-[:{rel_type} {{label: "{l["label"]}", type: "{l["type"]}"{desc_prop}}}]->(b);'
        )

    lines.extend([
        "",
        "// Step 5: Verify",
        "MATCH (n) RETURN labels(n) AS type, COUNT(n) AS count ORDER BY count DESC;",
        "MATCH ()-[r]->() RETURN type(r) AS type, COUNT(r) AS count ORDER BY count DESC;",
    ])

    return "\n".join(lines)


def _generate_rdf_triples(graph: Dict) -> List[Dict]:
    """Generate RDF-style triples for the knowledge graph"""
    rdf_triples = []
    for t in graph["triples"]:
        subj_node = next((n for n in graph["nodes"] if n["id"] == t["subject"]), None)
        obj_node = next((n for n in graph["nodes"] if n["id"] == t["object"]), None)

        subj_label = subj_node["label"] if subj_node else t["subject"]
        subj_type = subj_node["type"] if subj_node else "unknown"
        obj_label = obj_node["label"] if obj_node else t["object"]
        obj_type = obj_node["type"] if obj_node else "unknown"

        rdf_triples.append({
            "subject": {"id": t["subject"], "label": subj_label, "type": subj_type},
            "predicate": t["predicate"],
            "object": {"id": t["object"], "label": obj_label, "type": obj_type},
            "triple_type": t["type"],
            "description": t.get("description", ""),
            "readable": f"{subj_label} -> [{t['predicate']}] -> {obj_label}",
        })

    return rdf_triples


async def get_or_build_graph(force_refresh: bool = False) -> Dict[str, Any]:
    """Get cached graph or build fresh one. Shared by all endpoints."""
    global _GRAPH_CACHE

    now = time.time()
    if not force_refresh and _GRAPH_CACHE["data"] and (now - _GRAPH_CACHE["built_at"]) < _CACHE_TTL:
        return _GRAPH_CACHE["data"]

    # Try disk cache before expensive rebuild
    if not force_refresh:
        disk = _load_disk_cache()
        if disk:
            _GRAPH_CACHE["data"] = disk
            _GRAPH_CACHE["built_at"] = now
            return disk

    graph = await _build_full_ontology()
    _GRAPH_CACHE["data"] = graph
    _GRAPH_CACHE["built_at"] = now
    _save_disk_cache(graph)
    return graph


# ══════════════════════════════════════════════════════════════════════
#  ENDPOINT: Neo4j export
# ══════════════════════════════════════════════════════════════════════

@router.get("/neo4j-export")
async def export_neo4j_cypher():
    """Neo4j Cypher 내보내기 — 온톨로지를 Neo4j로 가져오기 위한 Cypher 스크립트"""
    graph = await get_or_build_graph()
    cypher = _generate_cypher(graph)

    return {
        "cypher": cypher,
        "nodes_count": len(graph["nodes"]),
        "relationships_count": len(graph["links"]),
        "instructions": {
            "step1": "Neo4j Desktop 또는 Aura에서 새 데이터베이스 생성",
            "step2": "Neo4j Browser에서 위 Cypher 스크립트 실행",
            "step3": "CALL db.schema.visualization() 으로 그래프 스키마 확인",
            "step4": "MATCH (n)-[r]->(m) RETURN n,r,m LIMIT 200 으로 시각화",
        },
        "size_estimate": f"{len(cypher) / 1024:.1f} KB",
    }


# ══════════════════════════════════════════════════════════════════════
#  MANAGEMENT ENDPOINTS
# ══════════════════════════════════════════════════════════════════════

@router.post("/cache-refresh")
async def refresh_ontology_cache():
    """온톨로지 캐시 강제 갱신 (메모리 + 디스크)"""
    global _GRAPH_CACHE
    import time as _t
    t0 = _t.time()
    graph = await _build_full_ontology()
    elapsed = _t.time() - t0
    _GRAPH_CACHE["data"] = graph
    _GRAPH_CACHE["built_at"] = _t.time()
    _save_disk_cache(graph)
    return {
        "success": True,
        "elapsed_seconds": round(elapsed, 1),
        "nodes": len(graph["nodes"]),
        "links": len(graph["links"]),
    }


@router.post("/cache-clear")
async def clear_ontology_cache():
    """온톨로지 메모리 캐시 삭제 (다음 요청 시 재구축)"""
    global _GRAPH_CACHE
    _GRAPH_CACHE = {"data": None, "built_at": 0}
    return {"success": True, "message": "캐시가 초기화되었습니다"}


# Custom annotation table
async def _ensure_annotation_table(conn):
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS ontology_annotation (
            annotation_id SERIAL PRIMARY KEY,
            node_id VARCHAR(200) NOT NULL,
            note TEXT NOT NULL,
            author VARCHAR(100) DEFAULT 'admin',
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)


class AnnotationCreate(BaseModel):
    node_id: str = Field(..., max_length=200)
    note: str = Field(..., max_length=2000)
    author: str = Field(default="admin", max_length=100)


class AnnotationUpdate(BaseModel):
    note: str = Field(..., max_length=2000)


@router.get("/annotations")
async def list_annotations(node_id: Optional[str] = Query(None)):
    """노드 주석 목록 조회"""
    conn = await _get_conn()
    try:
        await _ensure_annotation_table(conn)
        if node_id:
            rows = await conn.fetch(
                "SELECT * FROM ontology_annotation WHERE node_id=$1 ORDER BY created_at DESC", node_id
            )
        else:
            rows = await conn.fetch("SELECT * FROM ontology_annotation ORDER BY created_at DESC LIMIT 100")
        return {
            "annotations": [
                {
                    "annotation_id": r["annotation_id"],
                    "node_id": r["node_id"],
                    "note": r["note"],
                    "author": r["author"],
                    "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                }
                for r in rows
            ]
        }
    finally:
        await _release_conn(conn)


@router.post("/annotations")
async def create_annotation(body: AnnotationCreate):
    """노드 주석 추가"""
    conn = await _get_conn()
    try:
        await _ensure_annotation_table(conn)
        aid = await conn.fetchval("""
            INSERT INTO ontology_annotation (node_id, note, author)
            VALUES ($1, $2, $3) RETURNING annotation_id
        """, body.node_id, body.note, body.author)
        return {"success": True, "annotation_id": aid}
    finally:
        await _release_conn(conn)


@router.put("/annotations/{annotation_id}")
async def update_annotation(annotation_id: int, body: AnnotationUpdate):
    """노드 주석 수정"""
    conn = await _get_conn()
    try:
        result = await conn.execute(
            "UPDATE ontology_annotation SET note=$1 WHERE annotation_id=$2",
            body.note, annotation_id,
        )
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="주석을 찾을 수 없습니다")
        return {"success": True, "annotation_id": annotation_id}
    finally:
        await _release_conn(conn)


@router.delete("/annotations/{annotation_id}")
async def delete_annotation(annotation_id: int):
    """노드 주석 삭제"""
    conn = await _get_conn()
    try:
        result = await conn.execute(
            "DELETE FROM ontology_annotation WHERE annotation_id=$1", annotation_id
        )
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="주석을 찾을 수 없습니다")
        return {"success": True}
    finally:
        await _release_conn(conn)
