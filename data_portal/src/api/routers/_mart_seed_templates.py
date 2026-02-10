"""
마트 추천 시드 템플릿 데이터.
Imported by mart_recommend.py _seed_templates().
"""

SEED_TEMPLATES = [
    # ── Disease ──
    (
        "당뇨 코호트 마트",
        "disease",
        "제2형 당뇨병(SNOMED 44054006) 환자 코호트 마트. 인구통계+진단+방문 통합.",
        (
            "SELECT p.person_id, p.gender_source_value, p.year_of_birth, "
            "co.condition_start_date, co.condition_end_date, "
            "COUNT(vo.visit_occurrence_id) AS visit_count "
            "FROM person p "
            "JOIN condition_occurrence co ON p.person_id = co.person_id "
            "LEFT JOIN visit_occurrence vo ON p.person_id = vo.person_id "
            "WHERE co.condition_source_value = '44054006' "
            "GROUP BY p.person_id, p.gender_source_value, p.year_of_birth, "
            "co.condition_start_date, co.condition_end_date"
        ),
        '["person","condition_occurrence","visit_occurrence"]',
    ),
    (
        "고혈압 환자 마트",
        "disease",
        "고혈압성 장애(SNOMED 38341003) 환자 코호트 마트. 인구통계+진단+약물 통합.",
        (
            "SELECT p.person_id, p.gender_source_value, p.year_of_birth, "
            "co.condition_start_date, co.condition_end_date, "
            "COUNT(de.drug_exposure_id) AS drug_count "
            "FROM person p "
            "JOIN condition_occurrence co ON p.person_id = co.person_id "
            "LEFT JOIN drug_exposure de ON p.person_id = de.person_id "
            "WHERE co.condition_source_value = '38341003' "
            "GROUP BY p.person_id, p.gender_source_value, p.year_of_birth, "
            "co.condition_start_date, co.condition_end_date"
        ),
        '["person","condition_occurrence","drug_exposure"]',
    ),
    (
        "다제약물 사용 마트",
        "disease",
        "5개 이상 약물을 처방받은 다약제(polypharmacy) 환자 마트.",
        (
            "SELECT person_id, COUNT(DISTINCT drug_source_value) AS drug_count "
            "FROM drug_exposure "
            "GROUP BY person_id "
            "HAVING COUNT(DISTINCT drug_source_value) >= 5"
        ),
        '["drug_exposure"]',
    ),
    # ── Research ──
    (
        "입원 재원일수 분석 마트",
        "research",
        "입원(visit_concept_id=9201) 환자의 재원일수 분석 마트.",
        (
            "SELECT person_id, visit_occurrence_id, visit_start_date, visit_end_date, "
            "EXTRACT(DAY FROM visit_end_date - visit_start_date) AS los_days "
            "FROM visit_occurrence "
            "WHERE visit_concept_id = 9201 "
            "AND visit_end_date IS NOT NULL"
        ),
        '["visit_occurrence"]',
    ),
    (
        "검사치 이상 환자 마트",
        "research",
        "검사 결과가 정상 범위를 벗어난 환자 마트.",
        (
            "SELECT person_id, measurement_source_value, value_as_number, "
            "range_low, range_high, measurement_date "
            "FROM measurement "
            "WHERE value_as_number IS NOT NULL "
            "AND (value_as_number < range_low OR value_as_number > range_high)"
        ),
        '["measurement"]',
    ),
    (
        "응급실 방문 패턴 마트",
        "research",
        "응급실(visit_concept_id=9203) 방문 패턴 분석 마트.",
        (
            "SELECT person_id, visit_occurrence_id, visit_start_date, "
            "EXTRACT(DOW FROM visit_start_date) AS day_of_week, "
            "EXTRACT(HOUR FROM visit_start_date) AS hour_of_day "
            "FROM visit_occurrence "
            "WHERE visit_concept_id = 9203"
        ),
        '["visit_occurrence"]',
    ),
    # ── Management ──
    (
        "환자 방문 현황 마트",
        "management",
        "방문 유형별 방문 건수 및 환자 수 현황 마트.",
        (
            "SELECT visit_concept_id, "
            "COUNT(*) AS visit_count, "
            "COUNT(DISTINCT person_id) AS patient_count "
            "FROM visit_occurrence "
            "GROUP BY visit_concept_id"
        ),
        '["visit_occurrence"]',
    ),
    (
        "데이터 품질 현황 마트",
        "management",
        "주요 테이블 핵심 컬럼의 NULL 비율 분석 마트.",
        (
            "SELECT 'condition_occurrence' AS table_name, "
            "COUNT(*) AS total_rows, "
            "ROUND(COUNT(condition_source_value)::numeric / NULLIF(COUNT(*),0) * 100, 2) AS source_value_fill_pct, "
            "ROUND(COUNT(condition_start_date)::numeric / NULLIF(COUNT(*),0) * 100, 2) AS start_date_fill_pct "
            "FROM condition_occurrence "
            "UNION ALL "
            "SELECT 'drug_exposure', COUNT(*), "
            "ROUND(COUNT(drug_source_value)::numeric / NULLIF(COUNT(*),0) * 100, 2), "
            "ROUND(COUNT(drug_exposure_start_date)::numeric / NULLIF(COUNT(*),0) * 100, 2) "
            "FROM drug_exposure "
            "UNION ALL "
            "SELECT 'measurement', COUNT(*), "
            "ROUND(COUNT(measurement_source_value)::numeric / NULLIF(COUNT(*),0) * 100, 2), "
            "ROUND(COUNT(value_as_number)::numeric / NULLIF(COUNT(*),0) * 100, 2) "
            "FROM measurement"
        ),
        '["condition_occurrence","drug_exposure","measurement"]',
    ),
    (
        "테이블 사용량 현황 마트",
        "management",
        "PostgreSQL pg_stat_user_tables 기반 테이블 사용량/크기 분석 마트.",
        (
            "SELECT relname AS table_name, "
            "n_live_tup AS live_rows, "
            "n_dead_tup AS dead_rows, "
            "seq_scan, idx_scan, "
            "pg_size_pretty(pg_total_relation_size(relid)) AS total_size "
            "FROM pg_stat_user_tables "
            "WHERE schemaname = 'public' "
            "ORDER BY n_live_tup DESC"
        ),
        '["pg_stat_user_tables"]',
    ),
]
