"""
DIT-001: 데이터 설계 시드 데이터
- Zone / Entity / Relation / Naming / Unstructured 초기 데이터
Extracted from data_design.py to keep module under 800 lines.
"""
import json


async def _ensure_seed_data(conn):
    count = await conn.fetchval("SELECT COUNT(*) FROM data_zone")
    if count > 0:
        return

    # Zones
    zones = [
        ('원천 데이터 (Source)', 'source', 'hybrid', 's3://asan-datalake/source/', 'json', '운영 DB 원본 데이터 수집 영역 (EMR, OCS, LIS 등)', 90, 'YYYY/MM/DD'),
        ('Bronze (Raw)', 'bronze', 'object_storage', 's3://asan-datalake/bronze/', 'parquet', '원본 그대로 적재된 데이터 레이크 영역 (스키마 보존)', 365, 'source_system/YYYY/MM'),
        ('Silver (Cleansed)', 'silver', 'object_storage', 's3://asan-datalake/silver/', 'iceberg', 'OMOP CDM 표준 변환 데이터 (품질 검증 완료)', 1095, 'domain/table_name'),
        ('Gold (Curated)', 'gold', 'object_storage', 's3://asan-datalake/gold/', 'iceberg', '분석·연구용 큐레이션 데이터 (비식별화 적용)', 1825, 'use_case/YYYY'),
        ('데이터 마트 (Mart)', 'mart', 'rdbms', 'postgresql://mart-db:5432/mart', 'delta', '서비스별 집계/요약 데이터 마트', 730, 'service_name'),
    ]
    zone_ids = {}
    for name, ztype, stype, path, fmt, desc, ret, part in zones:
        zid = await conn.fetchval("""
            INSERT INTO data_zone (zone_name, zone_type, storage_type, storage_path, file_format, description, retention_days, partition_strategy)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING zone_id
        """, name, ztype, stype, path, fmt, desc, ret, part)
        zone_ids[ztype] = zid

    # Entities from actual OMOP CDM tables
    rc_rows = await conn.fetch("""
        SELECT relname, n_live_tup FROM pg_stat_user_tables WHERE schemaname='public'
    """)
    rc_map = {r["relname"]: r["n_live_tup"] for r in rc_rows}

    omop_entities = [
        ('person', '환자', 'silver', '환자', 'table', 'OMOP CDM 환자 마스터'),
        ('visit_occurrence', '방문', 'silver', '진료', 'table', 'OMOP CDM 진료 방문'),
        ('condition_occurrence', '진단', 'silver', '진료', 'table', 'OMOP CDM 진단 기록'),
        ('drug_exposure', '약물 처방', 'silver', '처방', 'table', 'OMOP CDM 약물 노출'),
        ('procedure_occurrence', '시술', 'silver', '시술', 'table', 'OMOP CDM 시술 기록'),
        ('measurement', '검사 결과', 'silver', '검사', 'table', 'OMOP CDM 검사 측정값'),
        ('observation', '관찰', 'silver', '관찰', 'table', 'OMOP CDM 관찰 기록'),
        ('device_exposure', '의료기기', 'silver', '시술', 'table', 'OMOP CDM 의료기기 사용'),
        ('death', '사망', 'silver', '환자', 'table', 'OMOP CDM 사망 기록'),
        ('observation_period', '관찰 기간', 'silver', '환자', 'table', 'OMOP CDM 관찰 기간'),
        ('condition_era', '진단 에라', 'gold', '분석', 'table', 'OMOP CDM 진단 에라 (집계)'),
        ('drug_era', '약물 에라', 'gold', '분석', 'table', 'OMOP CDM 약물 에라 (집계)'),
        ('cost', '비용', 'silver', '비용', 'table', 'OMOP CDM 비용 정보'),
        ('payer_plan_period', '보험', 'silver', '비용', 'table', 'OMOP CDM 보험 기간'),
        ('care_site', '진료 부서', 'silver', '기관', 'table', 'OMOP CDM 진료 부서'),
        ('provider', '의료진', 'silver', '기관', 'table', 'OMOP CDM 의료진'),
        ('location', '위치', 'silver', '기관', 'table', 'OMOP CDM 위치 정보'),
    ]

    # Get column info for each
    for ename, lname, zone, domain, etype, desc in omop_entities:
        cols = await conn.fetch("""
            SELECT column_name, data_type, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=$1
            ORDER BY ordinal_position
        """, ename)
        columns_json = [
            {"name": c["column_name"], "type": c["data_type"], "nullable": c["is_nullable"] == "YES"}
            for c in cols
        ]
        await conn.execute("""
            INSERT INTO data_model_entity (entity_name, logical_name, zone_id, domain, entity_type, description, columns_json, row_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
        """, ename, lname, zone_ids.get(zone), domain, etype, desc,
             json.dumps(columns_json), rc_map.get(ename, 0))

    # Source entities
    source_entities = [
        ('emr_patients', 'EMR 환자 원본', 'source', '환자', 'external', 'Oracle EMR 환자 테이블'),
        ('emr_encounters', 'EMR 방문 원본', 'source', '진료', 'external', 'Oracle EMR 방문 테이블'),
        ('lab_results', 'LIS 검사 결과', 'source', '검사', 'external', 'MySQL LIS 검사 결과'),
        ('surgery_records', '수술 기록 원본', 'source', '시술', 'external', 'SQL Server 수술 기록'),
        ('pacs_metadata', 'PACS 영상 메타', 'source', '영상', 'external', 'MongoDB PACS 메타데이터'),
    ]
    for ename, lname, zone, domain, etype, desc in source_entities:
        await conn.execute("""
            INSERT INTO data_model_entity (entity_name, logical_name, zone_id, domain, entity_type, description, columns_json, row_count)
            VALUES ($1, $2, $3, $4, $5, $6, '[]'::jsonb, 0)
        """, ename, lname, zone_ids.get(zone), domain, etype, desc)

    # Relations
    relations = [
        ('person', 'visit_occurrence', '1:N', 'person_id', 'person → visit_occurrence'),
        ('person', 'condition_occurrence', '1:N', 'person_id', 'person → condition_occurrence'),
        ('person', 'drug_exposure', '1:N', 'person_id', 'person → drug_exposure'),
        ('person', 'procedure_occurrence', '1:N', 'person_id', 'person → procedure_occurrence'),
        ('person', 'measurement', '1:N', 'person_id', 'person → measurement'),
        ('person', 'observation', '1:N', 'person_id', 'person → observation'),
        ('person', 'death', '1:1', 'person_id', 'person → death'),
        ('person', 'observation_period', '1:N', 'person_id', 'person → observation_period'),
        ('visit_occurrence', 'condition_occurrence', '1:N', 'visit_occurrence_id', '방문 중 진단'),
        ('visit_occurrence', 'drug_exposure', '1:N', 'visit_occurrence_id', '방문 중 처방'),
        ('visit_occurrence', 'measurement', '1:N', 'visit_occurrence_id', '방문 중 검사'),
        ('visit_occurrence', 'procedure_occurrence', '1:N', 'visit_occurrence_id', '방문 중 시술'),
        ('condition_occurrence', 'condition_era', '1:N', 'condition_concept_id', '진단 → 에라 집계'),
        ('drug_exposure', 'drug_era', '1:N', 'drug_concept_id', '약물 → 에라 집계'),
        ('care_site', 'visit_occurrence', '1:N', 'care_site_id', '부서 → 방문'),
        ('provider', 'visit_occurrence', '1:N', 'provider_id', '의료진 → 방문'),
        ('location', 'care_site', '1:N', 'location_id', '위치 → 부서'),
        ('emr_patients', 'person', '1:1', 'patient_id → person_id', 'EMR → OMOP ETL'),
        ('emr_encounters', 'visit_occurrence', '1:1', 'encounter_id → visit_occurrence_id', 'EMR → OMOP ETL'),
        ('lab_results', 'measurement', '1:N', 'test_code → measurement_concept_id', 'LIS → OMOP ETL'),
    ]
    for src, tgt, rtype, fk, desc in relations:
        await conn.execute("""
            INSERT INTO data_model_relation (source_entity, target_entity, relation_type, fk_columns, description)
            VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING
        """, src, tgt, rtype, fk, desc)

    # Naming standards
    naming_rules = [
        ('테이블 명명 규칙', 'table', r'^[a-z][a-z0-9_]*$', 'person, visit_occurrence', '소문자 + 언더스코어, 영문으로 시작'),
        ('컬럼 명명 규칙', 'column', r'^[a-z][a-z0-9_]*$', 'person_id, measurement_date', '소문자 + 언더스코어, 영문으로 시작'),
        ('PK 컬럼 규칙', 'column', r'^[a-z_]+_id$', 'person_id, visit_occurrence_id', 'PK 컬럼은 _id 접미사'),
        ('FK 컬럼 규칙', 'column', r'^[a-z_]+_id$', 'person_id (FK to person)', 'FK 컬럼은 참조 테이블_id'),
        ('스키마 명명 규칙', 'schema', r'^[a-z][a-z0-9_]*$', 'public, staging, analytics', '소문자 + 언더스코어'),
        ('인덱스 명명 규칙', 'index', r'^idx_[a-z_]+$', 'idx_person_id, idx_visit_date', 'idx_ 접두사 + 테이블_컬럼'),
        ('제약조건 명명 규칙', 'constraint', r'^(pk|fk|uq|ck)_[a-z_]+$', 'pk_person, fk_visit_person', 'pk_/fk_/uq_/ck_ 접두사'),
    ]
    for name, target, pattern, example, desc in naming_rules:
        await conn.execute("""
            INSERT INTO naming_standard (rule_name, target, pattern, example, description)
            VALUES ($1, $2, $3, $4, $5)
        """, name, target, pattern, example, desc)

    # Unstructured data mappings
    unstructured = [
        ('병리검사 보고서', '암병원 병리 검사 결과 보고서 (PDF/텍스트)', 'observation', 'NLP 구조화 추출', 'BioClinicalBERT + 규칙 기반', ['observation_concept_id', 'value_as_string', 'observation_date'], 'active'),
        ('영상검사 소견', '방사선과 판독 소견 보고서', 'observation', 'NLP 엔티티 추출', 'Medical NER (d4data)', ['observation_concept_id', 'value_as_string'], 'active'),
        ('수술기록', '수술실 수술 기록지 (자유텍스트)', 'procedure_occurrence', 'NLP 시술코드 매핑', 'Qwen3 + SNOMED 매핑', ['procedure_concept_id', 'procedure_date', 'modifier_concept_id'], 'testing'),
        ('경과기록', '담당의 경과기록 노트', 'note', '원문 저장 + NLP 태깅', 'BioClinicalBERT', ['note_text', 'note_type_concept_id', 'note_date'], 'active'),
        ('DICOM 메타데이터', 'DICOM 영상 헤더 메타데이터', 'imaging_study', '구조화 파싱', 'pydicom 파서', ['study_date', 'modality', 'body_site'], 'active'),
    ]
    for stype, sdesc, target, method, model, cols, status in unstructured:
        await conn.execute("""
            INSERT INTO unstructured_mapping (source_type, source_description, target_table, extraction_method, nlp_model, output_columns, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, stype, sdesc, target, method, model, cols, status)

    # Update zone table counts
    for zone_type, zid in zone_ids.items():
        cnt = await conn.fetchval("SELECT COUNT(*) FROM data_model_entity WHERE zone_id=$1", zid)
        await conn.execute("UPDATE data_zone SET table_count=$1 WHERE zone_id=$2", cnt, zid)
