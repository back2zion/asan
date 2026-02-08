"""
Medical Ontology Knowledge Graph — Static constants & reference data

OMOP CDM schema metadata, vocabulary standards, medical knowledge base,
SNOMED/RxNorm/LOINC concept name lookups, relationship definitions.
"""

# ══════════════════════════════════════════════════════════════════════
#  Node Types & Colors
# ══════════════════════════════════════════════════════════════════════

NODE_COLORS = {
    "domain": "#1A365D",
    "condition": "#E53E3E",
    "drug": "#805AD5",
    "measurement": "#319795",
    "procedure": "#DD6B20",
    "observation": "#D69E2E",
    "device": "#D53F8C",
    "visit": "#38A169",
    "person": "#005BAC",
    "death": "#718096",
    "cost": "#00B5D8",
    "vocabulary": "#2D3748",
    "body_system": "#4A5568",
    "drug_class": "#6B46C1",
    "comorbidity_cluster": "#C05621",
    "causal": "#B83280",
}

# CDM 5.4 ERD official domain colors (OHDSI standard)
CDM_DOMAIN_COLORS = {
    "Clinical Data": "#E76F51",
    "Health System Data": "#2A9D8F",
    "Health Economics": "#6C5CE7",
    "Derived Elements": "#0077B6",
    "Extension": "#E9C46A",
}

# ── Non-clinical SNOMED codes to EXCLUDE ──

EXCLUDED_SNOMED_CODES = {
    "160903007", "160904001", "706893006",
    "423315002", "422650009", "424393004",
    "224299000", "278860009",
    "82423001", "473461003", "161744009", "274531002", "384709000",
    "741062008", "18718003", "160968000",
    "714628002", "307426000",
    "162864005",
    "278588009", "278598003", "278558000", "278602001",
    "266948004",
    "105531004",
    "1149222004",
    "713458007",
    "224295006",
    "266934004",
}

# ══════════════════════════════════════════════════════════════════════
#  Concept Name Lookups
# ══════════════════════════════════════════════════════════════════════

SNOMED_NAMES = {
    "314529007": "우울장애 (Major Depression)", "73595000": "스트레스 장애 (Stress disorder)",
    "66383009": "치은염 (Gingivitis)", "444814009": "바이러스성 부비동염",
    "44054006": "당뇨병 제2형 (Diabetes mellitus type 2)",
    "38341003": "고혈압 (Hypertensive disorder)", "15777000": "전당뇨 (Prediabetes)",
    "53741008": "관상동맥질환 (Coronary heart disease)",
    "59621000": "본태성 고혈압 (Essential hypertension)",
    "40055000": "만성 부비동염 (Chronic sinusitis)",
    "87433001": "폐기종 (Pulmonary emphysema)",
    "185086009": "만성폐쇄성기관지염 (Chronic obstructive bronchitis)",
    "195662009": "급성 바이러스성 인두염", "109570002": "자궁경부암 검진 양성",
    "64859006": "골다공증 (Osteoporosis)", "35489007": "우울증 (Depression)",
    "36971009": "부비동염 (Sinusitis)", "233604007": "폐렴 (Pneumonia)",
    "49727002": "기침 (Cough)", "55822004": "과혈당 (Hyperglycemia)",
    "43878008": "위식도역류 (GERD)", "431855005": "만성신장질환 1기",
    "431856006": "만성신장질환 2기", "433144002": "만성신장질환 3기",
    "431857002": "만성신장질환 4기", "46177005": "말기신부전 (ESRD)",
    "56018004": "류마티스관절염", "69896004": "골관절염 (Osteoarthritis)",
    "230690007": "뇌졸중 (Stroke)", "22298006": "심근경색 (MI)",
    "84114007": "심부전 (Heart failure)", "13645005": "COPD",
    "399211009": "천식 (Asthma)", "10509002": "알레르기성 비염",
    "254637007": "비소세포폐암 (NSCLC)", "126906006": "유방암 (Breast cancer)",
    "363406005": "대장암 (Colon cancer)", "91302008": "대동맥류",
    "368581000119106": "신경병증 (Neuropathy)", "72892002": "골관절염",
    "271737000": "빈혈 (Anemia)", "90781000119102": "골다공증",
    "370143000": "주요우울장애", "698754002": "만성 통증",
    "47693006": "파열 (Rupture)", "30832001": "대상포진",
    "840539006": "COVID-19", "62106007": "골절",
    "68496003": "폴립 (Polyp)", "25064002": "두통 (Headache)",
    "132281000119108": "급성 호흡기 감염", "75498004": "급성 기관지염",
    "302870006": "고지혈증 (Hyperlipidemia)",
    "73438004": "관절통 (Joint pain)", "427898007": "흉통 (Chest pain)",
    "414545008": "허혈성 심질환 (Ischemic heart disease)",
    "65363002": "중이염 (Otitis media)",
    "237602007": "대사 증후군 (Metabolic syndrome)",
    "125605004": "골절 (Fracture)", "312608009": "급성 인두염",
    "80583007": "출혈 (Hemorrhage)", "10939881000119105": "아토피성 피부염",
    "37320007": "급성 편도인두염 (Pharyngotonsillitis)",
    "1121000119107": "당뇨 망막병증 (Diabetic retinopathy)",
    "267020005": "다갈증 (Excessive thirst)",
    "44465007": "발목 염좌 (Ankle sprain)",
    "127013003": "당뇨성 신장질환 (Diabetic renal disease)",
    "840544004": "COVID-19 의심 (Suspected COVID-19)",
}

RXNORM_NAMES = {
    "205923": "아목시실린/클라불란산 (Amoxicillin/Clavulanate)",
    "106892": "황산제일철 325mg (Ferrous Sulfate)", "314076": "리시노프릴 10mg (Lisinopril)",
    "310798": "하이드로클로로티아지드 25mg (HCTZ)", "308136": "아목시실린 500mg",
    "1535362": "니트로글리세린 (Nitroglycerin)", "1736854": "날록손 (Naloxone)",
    "583214": "나프록센 500mg (Naproxen)", "860975": "메트포르민 500mg (Metformin)",
    "314231": "심바스타틴 20mg (Simvastatin)", "197361": "암로디핀 5mg (Amlodipine)",
    "904420": "아토르바스타틴 10mg (Atorvastatin)", "312961": "아스피린 81mg (Aspirin)",
    "855332": "와파린 5mg (Warfarin)", "311671": "나프록센 220mg (Naproxen)",
    "1049221": "아세트아미노펜 (Acetaminophen)", "197696": "클로피도그렐 (Clopidogrel)",
    "309362": "알부테롤 (Albuterol)", "313820": "이부프로펜 (Ibuprofen)",
    "316672": "오메프라졸 (Omeprazole)", "1049630": "메트포르민 1000mg",
    "198405": "프레드니손 (Prednisone)", "313416": "시프로플록사신 (Ciprofloxacin)",
    "259543": "플루옥세틴 (Fluoxetine)", "312938": "아지스로마이신 (Azithromycin)",
    "351761": "로사르탄 50mg (Losartan)", "876514": "서트랄린 (Sertraline)",
    "617310": "아테놀롤 25mg (Atenolol)", "310429": "독시사이클린 (Doxycycline)",
    "314077": "리시노프릴 20mg (Lisinopril 20mg)",
    "904419": "글리피지드 10mg (Glipizide)",
    "1664463": "세마글루타이드 (Semaglutide)",
    "209387": "페니실린 V (Penicillin V)",
    "206905": "세팔렉신 500mg (Cephalexin)",
    "856987": "인슐린 글라진 100IU (Insulin Glargine)",
    "313521": "아테놀롤 50mg (Atenolol)",
    "1049625": "아세트아미노펜 325mg (Acetaminophen)",
    "313782": "오메프라졸 20mg (Omeprazole)",
    "1719286": "티오트로피움 (Tiotropium)",
    "630208": "콜레칼시페롤 (Cholecalciferol/VitD)",
    "245314": "프레드니손 5mg (Prednisone)",
    "896209": "아목시실린 250mg (Amoxicillin)",
    "226719": "독시사이클린 100mg (Doxycycline)",
    "245134": "프레드니솔론 (Prednisolone)",
    "895996": "세팔렉신 250mg (Cephalexin)",
    "835603": "세트리진 (Cetirizine)",
    "1049504": "이부프로펜 200mg (Ibuprofen)",
    "1860491": "에독사반 (Edoxaban)",
    "351109": "포모테롤 (Formoterol)",
}

LOINC_NAMES = {
    "72514-3": "통증 강도 (Pain severity)", "8462-4": "이완기혈압 (Diastolic BP)",
    "8480-6": "수축기혈압 (Systolic BP)", "29463-7": "체중 (Body Weight)",
    "8867-4": "심박수 (Heart rate)", "9279-1": "호흡수 (Respiratory rate)",
    "8302-2": "신장 (Body Height)", "39156-5": "BMI (체질량지수)",
    "33914-3": "eGFR (사구체여과율)", "74006-8": "동적 통증 점수",
    "2093-3": "총콜레스테롤 (Total Cholesterol)", "2571-8": "중성지방 (Triglycerides)",
    "2085-9": "HDL 콜레스테롤", "18262-6": "LDL 콜레스테롤",
    "6299-2": "BUN (혈중요소질소)", "2160-0": "크레아티닌 (Creatinine)",
    "2345-7": "혈당 (Glucose)", "4548-4": "HbA1c (당화혈색소)",
    "17861-6": "칼슘 (Calcium)", "718-7": "헤모글로빈 (Hemoglobin)",
    "26515-7": "혈소판 (Platelet)", "6690-2": "WBC (백혈구)",
    "789-8": "적혈구 (Erythrocyte)", "1742-6": "ALT (간기능)",
    "1920-8": "AST (간기능)", "2823-3": "칼륨 (Potassium)",
    "2951-2": "나트륨 (Sodium)", "2075-0": "염소 (Chloride)",
    "1975-2": "빌리루빈 (Bilirubin)", "1751-7": "알부민 (Albumin)",
    "49765-1": "칼슘", "2947-0": "나트륨",
}


def _resolve_concept_name(source_value: str, concept_id: int, domain: str) -> str:
    """Resolve concept ID to human-readable name"""
    sv = str(source_value).strip()
    if sv.isdigit():
        if domain == "condition":
            return SNOMED_NAMES.get(sv, f"SNOMED:{sv}")
        elif domain == "drug":
            return RXNORM_NAMES.get(sv, f"RxNorm:{sv}")
    if "-" in sv and domain == "measurement":
        return LOINC_NAMES.get(sv, f"LOINC:{sv}")
    if any(c.isalpha() for c in sv):
        return sv
    return f"#{sv}"


# ══════════════════════════════════════════════════════════════════════
#  Domain Groups
# ══════════════════════════════════════════════════════════════════════

DOMAIN_GROUPS = {
    "Clinical Data": [
        "person", "observation_period", "visit_occurrence", "visit_detail",
        "condition_occurrence", "drug_exposure", "procedure_occurrence",
        "device_exposure", "measurement", "observation", "death",
        "note", "note_nlp", "specimen", "survey_conduct",
    ],
    "Health System Data": ["care_site", "provider", "location", "location_history"],
    "Health Economics": ["cost", "payer_plan_period"],
    "Derived Elements": ["condition_era", "drug_era"],
    "Extension": ["imaging_study"],
}

# ══════════════════════════════════════════════════════════════════════
#  OMOP CDM SCHEMA — Table definitions & FK relationships
# ══════════════════════════════════════════════════════════════════════

OMOP_TABLE_META = {
    "person": {"label": "환자 (Person)", "domain": "Clinical Data", "icon": "👤",
               "description": "환자 인구통계 정보 — 성별, 생년, 인종, 민족"},
    "observation_period": {"label": "관찰기간 (Observation Period)", "domain": "Clinical Data", "icon": "📅",
                           "description": "환자의 의료 데이터 관찰 기간"},
    "visit_occurrence": {"label": "방문 (Visit Occurrence)", "domain": "Clinical Data", "icon": "🏥",
                         "description": "의료기관 방문 — 입원, 외래, 응급"},
    "visit_detail": {"label": "방문상세 (Visit Detail)", "domain": "Clinical Data", "icon": "📋",
                     "description": "방문 내 세부 이동/전과 기록"},
    "condition_occurrence": {"label": "진단 (Condition)", "domain": "Clinical Data", "icon": "🩺",
                             "description": "진단/상병 기록 — SNOMED CT 표준"},
    "drug_exposure": {"label": "투약 (Drug Exposure)", "domain": "Clinical Data", "icon": "💊",
                      "description": "약물 처방/투여 기록 — RxNorm 표준"},
    "procedure_occurrence": {"label": "시술 (Procedure)", "domain": "Clinical Data", "icon": "🔬",
                             "description": "수술/시술/처치 기록 — CPT/SNOMED"},
    "device_exposure": {"label": "의료기기 (Device)", "domain": "Clinical Data", "icon": "🩻",
                        "description": "의료기기 사용 기록"},
    "measurement": {"label": "검사결과 (Measurement)", "domain": "Clinical Data", "icon": "🧪",
                    "description": "검사/측정 결과 — LOINC 표준"},
    "observation": {"label": "관찰 (Observation)", "domain": "Clinical Data", "icon": "👁️",
                    "description": "임상 관찰 — 사회력, 가족력 포함"},
    "death": {"label": "사망 (Death)", "domain": "Clinical Data", "icon": "✝️",
              "description": "사망 기록 — 원인, 일시"},
    "note": {"label": "노트 (Note)", "domain": "Clinical Data", "icon": "📝",
             "description": "비정형 임상 노트 — 의사 소견, 퇴원 요약",
             "db_table": "note"},
    "note_nlp": {"label": "NLP 분석 (Note NLP)", "domain": "Clinical Data", "icon": "🤖",
                 "description": "임상 노트 자연어 처리 결과",
                 "db_table": "note_nlp"},
    "specimen": {"label": "검체 (Specimen)", "domain": "Clinical Data", "icon": "🧫",
                 "description": "생체 검체 수집 기록 — 혈액, 조직 등",
                 "db_table": "specimen_id"},
    "survey_conduct": {"label": "설문조사 (Survey)", "domain": "Clinical Data", "icon": "📊",
                       "description": "환자 설문/PRO 수행 기록",
                       "db_table": "survey_conduct"},
    "care_site": {"label": "의료기관 (Care Site)", "domain": "Health System Data", "icon": "🏨",
                  "description": "의료기관/진료과 정보"},
    "provider": {"label": "의료인 (Provider)", "domain": "Health System Data", "icon": "👨‍⚕️",
                 "description": "의료인 정보 — 전문의, 간호사 등"},
    "location": {"label": "지역 (Location)", "domain": "Health System Data", "icon": "📍",
                 "description": "지리적 위치/주소 정보"},
    "location_history": {"label": "주소이력 (Location History)", "domain": "Health System Data", "icon": "🗺️",
                         "description": "환자/기관 주소 변경 이력",
                         "db_table": "location_history"},
    "cost": {"label": "비용 (Cost)", "domain": "Health Economics", "icon": "💰",
             "description": "의료비용 — 청구, 지불, 급여 기록"},
    "payer_plan_period": {"label": "보험기간 (Payer Plan Period)", "domain": "Health Economics", "icon": "🏦",
                          "description": "보험/급여 가입 기간"},
    "drug_era": {"label": "투약기간 (Drug Era)", "domain": "Derived Elements", "icon": "💊",
                 "description": "연속 투약 기간 집계 — 활성 성분 기준"},
    "condition_era": {"label": "질환기간 (Condition Era)", "domain": "Derived Elements", "icon": "📈",
                      "description": "질환 지속 기간 집계 — SNOMED 기준"},
    "imaging_study": {"label": "영상검사 (Imaging Study)", "domain": "Extension", "icon": "🫁",
                      "description": "의료 영상 — X-ray, CT, MRI 판독 결과",
                      "db_table": "imaging_study"},
}

OMOP_FK_RELATIONSHIPS = [
    ("person", "observation_period", "has_observation_period", "CDM 5.4 FK: observation_period.person_id"),
    ("person", "visit_occurrence", "has_visit", "CDM 5.4 FK: visit_occurrence.person_id"),
    ("person", "condition_occurrence", "has_condition", "CDM 5.4 FK: condition_occurrence.person_id"),
    ("person", "drug_exposure", "has_drug_exposure", "CDM 5.4 FK: drug_exposure.person_id"),
    ("person", "procedure_occurrence", "has_procedure", "CDM 5.4 FK: procedure_occurrence.person_id"),
    ("person", "device_exposure", "has_device_exposure", "CDM 5.4 FK: device_exposure.person_id"),
    ("person", "measurement", "has_measurement", "CDM 5.4 FK: measurement.person_id"),
    ("person", "observation", "has_observation", "CDM 5.4 FK: observation.person_id"),
    ("person", "death", "has_death", "CDM 5.4 FK: death.person_id"),
    ("person", "note", "has_note", "CDM 5.4 FK: note.person_id"),
    ("person", "specimen", "has_specimen", "CDM 5.4 FK: specimen.person_id"),
    ("person", "payer_plan_period", "has_payer_plan", "CDM 5.4 FK: payer_plan_period.person_id"),
    ("person", "drug_era", "has_drug_era", "CDM 5.4 FK: drug_era.person_id"),
    ("person", "condition_era", "has_condition_era", "CDM 5.4 FK: condition_era.person_id"),
    ("person", "survey_conduct", "has_survey", "CDM 5.4 FK: survey_conduct.person_id"),
    ("person", "imaging_study", "has_imaging", "Extension FK: imaging_study.person_id"),
    ("person", "location", "resides_at", "CDM 5.4 FK: person.location_id -> location"),
    ("person", "provider", "primary_provider", "CDM 5.4 FK: person.provider_id -> provider"),
    ("person", "care_site", "registered_at", "CDM 5.4 FK: person.care_site_id -> care_site"),
    ("visit_occurrence", "condition_occurrence", "visit_condition", "CDM 5.4 FK: visit_occurrence_id"),
    ("visit_occurrence", "drug_exposure", "visit_drug", "CDM 5.4 FK: visit_occurrence_id"),
    ("visit_occurrence", "procedure_occurrence", "visit_procedure", "CDM 5.4 FK: visit_occurrence_id"),
    ("visit_occurrence", "device_exposure", "visit_device", "CDM 5.4 FK: visit_occurrence_id"),
    ("visit_occurrence", "measurement", "visit_measurement", "CDM 5.4 FK: visit_occurrence_id"),
    ("visit_occurrence", "observation", "visit_observation", "CDM 5.4 FK: visit_occurrence_id"),
    ("visit_occurrence", "note", "visit_note", "CDM 5.4 FK: visit_occurrence_id"),
    ("visit_occurrence", "visit_detail", "has_detail", "CDM 5.4 FK: visit_detail.visit_occurrence_id"),
    ("visit_occurrence", "cost", "has_cost", "CDM 5.4 FK: cost.cost_event_id"),
    ("care_site", "location", "located_at", "CDM 5.4 FK: care_site.location_id -> location"),
    ("provider", "care_site", "affiliated_with", "CDM 5.4 FK: provider.care_site_id -> care_site"),
    ("visit_occurrence", "care_site", "occurred_at", "CDM 5.4 FK: visit_occurrence.care_site_id"),
    ("visit_occurrence", "provider", "attended_by", "CDM 5.4 FK: visit_occurrence.provider_id"),
    ("condition_occurrence", "condition_era", "aggregated_to", "CDM 5.4 Derived: 질환 기간 집계"),
    ("drug_exposure", "drug_era", "aggregated_to", "CDM 5.4 Derived: 투약 기간 집계"),
    ("note", "note_nlp", "nlp_processed", "CDM 5.4 FK: note_nlp.note_id -> note"),
    ("visit_occurrence", "visit_occurrence", "preceding_visit", "CDM 5.4 FK: preceding_visit_occurrence_id"),
    ("location", "location_history", "has_history", "주소 변경 이력"),
]

# ══════════════════════════════════════════════════════════════════════
#  VOCABULARY STANDARDS
# ══════════════════════════════════════════════════════════════════════

VOCABULARY_NODES = [
    {"id": "vocab_snomed", "label": "SNOMED CT", "full_name": "Systematized Nomenclature of Medicine",
     "domain": "Condition, Procedure", "concepts": "~350,000", "color": "#E53E3E"},
    {"id": "vocab_icd10", "label": "ICD-10-CM", "full_name": "International Classification of Diseases 10th",
     "domain": "Condition", "concepts": "~72,000", "color": "#C53030"},
    {"id": "vocab_rxnorm", "label": "RxNorm", "full_name": "RxNorm Normalized Drug Names",
     "domain": "Drug", "concepts": "~250,000", "color": "#805AD5"},
    {"id": "vocab_atc", "label": "ATC", "full_name": "Anatomical Therapeutic Chemical Classification",
     "domain": "Drug Class", "concepts": "~6,000", "color": "#6B46C1"},
    {"id": "vocab_loinc", "label": "LOINC", "full_name": "Logical Observation Identifiers Names and Codes",
     "domain": "Measurement", "concepts": "~98,000", "color": "#319795"},
    {"id": "vocab_cpt4", "label": "CPT-4", "full_name": "Current Procedural Terminology",
     "domain": "Procedure", "concepts": "~10,000", "color": "#DD6B20"},
    {"id": "vocab_hcpcs", "label": "HCPCS", "full_name": "Healthcare Common Procedure Coding System",
     "domain": "Procedure, Device", "concepts": "~7,500", "color": "#C05621"},
    {"id": "vocab_mesh", "label": "MeSH", "full_name": "Medical Subject Headings",
     "domain": "Literature", "concepts": "~30,000", "color": "#2D3748"},
    {"id": "vocab_omop", "label": "OMOP CDM", "full_name": "OHDSI OMOP Common Data Model v5.4",
     "domain": "Standard", "concepts": "CDM Schema", "color": "#1A365D"},
    {"id": "vocab_kcd7", "label": "KCD-7", "full_name": "한국표준질병사인분류 제7차",
     "domain": "Condition (Korea)", "concepts": "~23,000", "color": "#9B2C2C"},
    {"id": "vocab_edi", "label": "EDI (건강보험)", "full_name": "건강보험심사평가원 EDI 코드",
     "domain": "Drug, Procedure (Korea)", "concepts": "~180,000", "color": "#553C9A"},
]

# ══════════════════════════════════════════════════════════════════════
#  MEDICAL KNOWLEDGE BASE — Body Systems & Drug Classes
# ══════════════════════════════════════════════════════════════════════

BODY_SYSTEMS = [
    {"id": "sys_cardio", "label": "순환기계 (Cardiovascular)", "conditions": ["고혈압", "관상동맥질환", "심부전", "부정맥", "심근경색"],
     "measurements": ["수축기혈압", "이완기혈압", "총콜레스테롤", "LDL", "HDL", "중성지방"]},
    {"id": "sys_endo", "label": "내분비계 (Endocrine)", "conditions": ["당뇨병 제2형", "당뇨병 제1형", "갑상선기능저하", "비만", "대사증후군"],
     "measurements": ["혈당", "HbA1c", "BMI", "TSH", "인슐린"]},
    {"id": "sys_resp", "label": "호흡기계 (Respiratory)", "conditions": ["천식", "COPD", "폐렴", "급성기관지염", "부비동염"],
     "measurements": ["산소포화도", "FEV1", "호흡수"]},
    {"id": "sys_renal", "label": "비뇨기계 (Renal)", "conditions": ["만성신장질환", "요로감염", "신결석", "신부전"],
     "measurements": ["크레아티닌", "BUN", "eGFR", "요산"]},
    {"id": "sys_gi", "label": "소화기계 (Gastrointestinal)", "conditions": ["GERD", "소화성궤양", "간경변", "담석증", "과민성장증후군"],
     "measurements": ["AST/GOT", "ALT/GPT", "빌리루빈", "알부민"]},
    {"id": "sys_neuro", "label": "신경계 (Neurological)", "conditions": ["우울증", "불안장애", "치매", "편두통", "뇌졸중", "간질"],
     "measurements": ["PHQ-9", "GAD-7", "MMSE"]},
    {"id": "sys_musculo", "label": "근골격계 (Musculoskeletal)", "conditions": ["골관절염", "골다공증", "요통", "류마티스관절염", "골절"],
     "measurements": ["골밀도", "CRP", "ESR", "류마티스인자"]},
    {"id": "sys_immune", "label": "면역계 (Immune)", "conditions": ["알레르기", "아나필락시스", "자가면역질환", "면역결핍"],
     "measurements": ["WBC", "IgE", "보체", "ANA"]},
    {"id": "sys_hemato", "label": "혈액/종양 (Hematology/Oncology)", "conditions": ["빈혈", "유방암", "폐암", "대장암", "백혈병", "림프종"],
     "measurements": ["헤모글로빈", "혈소판", "WBC", "RBC", "CEA", "AFP", "PSA"]},
    {"id": "sys_derma", "label": "피부계 (Dermatology)", "conditions": ["아토피피부염", "건선", "두드러기", "피부감염"],
     "measurements": ["피부조직검사"]},
]

DRUG_CLASSES = [
    {"id": "dc_antidiabetic", "label": "항당뇨제 (Antidiabetics)", "drugs": ["Metformin", "Insulin", "Glipizide", "Sitagliptin", "Empagliflozin"],
     "atc": "A10", "target_system": "sys_endo"},
    {"id": "dc_antihypertensive", "label": "항고혈압제 (Antihypertensives)", "drugs": ["Lisinopril", "Amlodipine", "Hydrochlorothiazide", "Losartan", "Atenolol"],
     "atc": "C02/C03/C07/C08/C09", "target_system": "sys_cardio"},
    {"id": "dc_statin", "label": "스타틴 (Statins)", "drugs": ["Atorvastatin", "Simvastatin", "Rosuvastatin", "Pravastatin"],
     "atc": "C10AA", "target_system": "sys_cardio"},
    {"id": "dc_anticoagulant", "label": "항응고제 (Anticoagulants)", "drugs": ["Warfarin", "Heparin", "Enoxaparin", "Rivaroxaban"],
     "atc": "B01A", "target_system": "sys_hemato"},
    {"id": "dc_antibiotic", "label": "항생제 (Antibiotics)", "drugs": ["Amoxicillin", "Azithromycin", "Ciprofloxacin", "Cephalexin", "Doxycycline"],
     "atc": "J01", "target_system": "sys_immune"},
    {"id": "dc_nsaid", "label": "소염진통제 (NSAIDs)", "drugs": ["Ibuprofen", "Naproxen", "Aspirin", "Diclofenac", "Celecoxib"],
     "atc": "M01A/N02BA", "target_system": "sys_musculo"},
    {"id": "dc_antidepressant", "label": "항우울제 (Antidepressants)", "drugs": ["Fluoxetine", "Sertraline", "Escitalopram", "Bupropion", "Venlafaxine"],
     "atc": "N06A", "target_system": "sys_neuro"},
    {"id": "dc_bronchodilator", "label": "기관지확장제 (Bronchodilators)", "drugs": ["Albuterol", "Ipratropium", "Tiotropium", "Formoterol"],
     "atc": "R03", "target_system": "sys_resp"},
    {"id": "dc_ppi", "label": "양성자펌프억제제 (PPIs)", "drugs": ["Omeprazole", "Esomeprazole", "Pantoprazole", "Lansoprazole"],
     "atc": "A02BC", "target_system": "sys_gi"},
    {"id": "dc_antiplatelet", "label": "항혈소판제 (Antiplatelets)", "drugs": ["Aspirin", "Clopidogrel", "Ticagrelor"],
     "atc": "B01AC", "target_system": "sys_cardio"},
    {"id": "dc_opioid", "label": "오피오이드 (Opioids)", "drugs": ["Morphine", "Oxycodone", "Tramadol", "Fentanyl"],
     "atc": "N02A", "target_system": "sys_neuro"},
    {"id": "dc_bisphosphonate", "label": "비스포스포네이트 (Bisphosphonates)", "drugs": ["Alendronate", "Risedronate", "Zoledronic acid"],
     "atc": "M05BA", "target_system": "sys_musculo"},
]

# ══════════════════════════════════════════════════════════════════════
#  CAUSAL & COMORBIDITY RELATIONSHIPS
# ══════════════════════════════════════════════════════════════════════

TREATMENT_RELATIONSHIPS = [
    ("본태성 고혈압", "Lisinopril", "first_line_treatment", 0.88, "ACE 억제제 — 혈압 조절"),
    ("본태성 고혈압", "HCTZ", "treatment", 0.58, "이뇨제"),
    ("본태성 고혈압", "아테놀롤 50", "treatment", 0.65, "베타차단제"),
    ("허혈성 심질환", "Nitroglycerin", "treatment", 0.85, "협심증 급성 치료"),
    ("허혈성 심질환", "Simvastatin", "treatment", 0.80, "스타틴 — 콜레스테롤 조절"),
    ("허혈성 심질환", "Warfarin", "treatment", 0.70, "항응고 요법"),
    ("우울장애", "Fluoxetine", "first_line_treatment", 0.75, "SSRI 1차 치료"),
    ("우울장애", "Sertraline", "treatment", 0.72, "SSRI 대안"),
    ("골관절염", "나프록센 500", "treatment", 0.65, "소염진통제"),
    ("골관절염", "이부프로펜 200", "treatment", 0.82, "소염진통제"),
    ("골관절염", "Acetaminophen", "treatment", 0.60, "진통제"),
    ("관절통", "이부프로펜 200", "treatment", 0.78, "소염진통제"),
    ("관절통", "나프록센 500", "treatment", 0.70, "소염진통제"),
    ("빈혈", "Ferrous Sulfate", "treatment", 0.75, "철분 보충"),
    ("위식도역류", "Omeprazole", "first_line_treatment", 0.88, "PPI — 위산 억제"),
    ("알레르기성 비염", "Cetirizine", "treatment", 0.80, "항히스타민제"),
    ("아토피성 피부염", "Cetirizine", "treatment", 0.72, "항히스타민제"),
    ("아토피성 피부염", "Prednisolone", "treatment", 0.55, "스테로이드 — 급성기"),
    ("만성 부비동염", "아목시실린 250", "first_line_treatment", 0.82, "1차 항생제"),
    ("급성 바이러스성 인두염", "Penicillin", "treatment", 0.75, "항생제"),
    ("편도인두염", "Cephalexin", "treatment", 0.72, "세팔로스포린 항생제"),
    ("편도인두염", "페니실린 V", "treatment", 0.80, "1차 항생제"),
    ("중이염", "아목시실린 250", "first_line_treatment", 0.85, "1차 항생제"),
    ("흉통", "Nitroglycerin", "symptomatic", 0.70, "흉통 증상 완화"),
    ("당뇨 망막병증", "Semaglutide", "treatment", 0.55, "혈당 조절 -> 진행 억제"),
    ("당뇨성 신장", "Lisinopril", "renoprotective", 0.82, "ACE 억제제 — 신장 보호"),
    ("대사 증후군", "Metformin", "treatment", 0.72, "인슐린 감수성 개선"),
    ("대사 증후군", "Glipizide", "treatment", 0.50, "혈당 조절"),
]

DIAGNOSTIC_RELATIONSHIPS = [
    ("본태성 고혈압", "수축기혈압", "primary_diagnostic", "혈압 측정 — 수축기"),
    ("본태성 고혈압", "이완기혈압", "primary_diagnostic", "혈압 측정 — 이완기"),
    ("허혈성 심질환", "총콜레스테롤", "risk_assessment", "심혈관 위험도 평가"),
    ("허혈성 심질환", "HDL", "protective_factor", "HDL — 보호 인자"),
    ("대사 증후군", "BMI", "primary_diagnostic", "체질량지수"),
    ("대사 증후군", "Triglycerides", "risk_assessment", "중성지방"),
    ("당뇨 망막병증", "HbA1c", "monitoring", "당화혈색소 — 혈당 관리"),
    ("당뇨성 신장", "eGFR", "staging", "CKD 병기 결정"),
    ("당뇨성 신장", "크레아티닌", "diagnostic", "신장 기능 평가"),
    ("빈혈", "헤모글로빈", "primary_diagnostic", "빈혈 진단 핵심"),
    ("우울장애", "통증", "associated", "만성 통증 동반 평가"),
    ("흉통", "심박수", "monitoring", "심박수 모니터링"),
]

COMORBIDITY_RELATIONSHIPS = [
    ("고혈압", "허혈성 심질환", 0.42, "혈관 손상 -> 죽상경화"),
    ("고혈압", "대사 증후군", 0.55, "대사증후군 구성 요소"),
    ("우울장애", "관절통", 0.22, "만성 통증 -> 우울"),
    ("우울장애", "스트레스", 0.60, "스트레스 관련 정신건강"),
    ("골관절염", "관절통", 0.72, "퇴행성 관절 질환 -> 통증"),
    ("부비동염", "알레르기", 0.48, "알레르기 -> 부비동 염증"),
    ("부비동염", "인두염", 0.35, "상기도 감염 동반"),
    ("허혈성 심질환", "흉통", 0.65, "허혈성 심질환의 주요 증상"),
    ("당뇨 망막", "대사 증후군", 0.38, "대사 이상 -> 미세혈관 합병증"),
    ("아토피", "알레르기", 0.55, "아토피 행진 — IgE 매개"),
    ("빈혈", "출혈", 0.45, "출혈 -> 철결핍 빈혈"),
    ("골절", "골관절염", 0.25, "골 취약성 증가"),
]

CAUSAL_CHAINS = [
    {"id": "chain_metabolic", "label": "대사증후군 -> 심혈관 위험",
     "path": ["비만", "인슐린저항성", "당뇨병 제2형", "고혈압", "이상지질혈증", "관상동맥질환", "심근경색"],
     "description": "대사증후군에서 심혈관 질환으로의 인과적 진행 경로"},
    {"id": "chain_ckd", "label": "당뇨 -> 만성 신장질환",
     "path": ["당뇨병 제2형", "미세알부민뇨", "단백뇨", "만성신장질환 1기", "만성신장질환 3기", "말기신부전", "투석"],
     "description": "당뇨성 신증의 단계적 진행"},
    {"id": "chain_copd", "label": "COPD 악화 경로",
     "path": ["흡연", "만성기관지염", "COPD", "급성악화", "폐렴", "호흡부전"],
     "description": "COPD의 진행 및 급성 악화 경로"},
    {"id": "chain_osteo", "label": "골다공증 -> 골절",
     "path": ["폐경/노화", "골밀도감소", "골다공증", "낙상", "골절", "기동성저하", "이차골절"],
     "description": "골다공증에서 연쇄 골절로의 경로"},
]
