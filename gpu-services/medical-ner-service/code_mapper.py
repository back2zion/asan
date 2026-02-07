"""
OMOP CodeMapper
- d4data 라벨을 프론트엔드 엔티티 타입으로 매핑
- rapidfuzz Levenshtein fuzzy matching으로 표준 코드 매핑
- 임계값: 85%
"""
from typing import Dict, List, Optional, Tuple
from rapidfuzz import fuzz, process

# d4data/biomedical-ner-all 라벨 → 프론트엔드 타입 매핑
LABEL_TYPE_MAP: Dict[str, str] = {
    "Disease_disorder": "condition",
    "Sign_symptom": "condition",
    "Medication": "drug",
    "Lab_value": "measurement",
    "Diagnostic_procedure": "procedure",
    "Therapeutic_procedure": "procedure",
    "Age": "person",
    "Sex": "person",
    "Personal_background": "person",
    "Clinical_event": "condition",
    "Nonbiological_location": "procedure",
    "Biological_structure": "procedure",
}

# 코드 시스템 매핑 (타입별)
TYPE_CODE_SYSTEM: Dict[str, str] = {
    "condition": "ICD-10",
    "drug": "RxNorm",
    "measurement": "LOINC",
    "procedure": "SNOMED CT",
    "person": "PII",
}

# 표준 코드 사전 (fuzzy match 대상)
CODE_DICTIONARY: Dict[str, List[Dict[str, str]]] = {
    "ICD-10": [
        {"concept": "Type 2 diabetes mellitus", "code": "E11.9"},
        {"concept": "Type 1 diabetes mellitus", "code": "E10.9"},
        {"concept": "Essential hypertension", "code": "I10"},
        {"concept": "Heart failure", "code": "I50.9"},
        {"concept": "Pneumonia", "code": "J18.9"},
        {"concept": "Acute myocardial infarction", "code": "I21.9"},
        {"concept": "Chronic kidney disease", "code": "N18.9"},
        {"concept": "Coronary artery disease", "code": "I25.10"},
        {"concept": "Angina pectoris", "code": "I20.9"},
        {"concept": "Cardiomegaly", "code": "I51.7"},
        {"concept": "Atrial fibrillation", "code": "I48.91"},
        {"concept": "Hyperlipidemia", "code": "E78.5"},
        {"concept": "Chronic obstructive pulmonary disease", "code": "J44.1"},
        {"concept": "Asthma", "code": "J45.909"},
        {"concept": "Cerebrovascular accident", "code": "I63.9"},
        {"concept": "Deep vein thrombosis", "code": "I82.409"},
        {"concept": "Pulmonary embolism", "code": "I26.99"},
        {"concept": "Sepsis", "code": "A41.9"},
        {"concept": "Anemia", "code": "D64.9"},
        {"concept": "Hypothyroidism", "code": "E03.9"},
        {"concept": "Obesity", "code": "E66.9"},
        {"concept": "Depression", "code": "F32.9"},
        {"concept": "Anxiety", "code": "F41.9"},
        {"concept": "Gastroesophageal reflux disease", "code": "K21.0"},
        {"concept": "Osteoarthritis", "code": "M19.90"},
        {"concept": "Rheumatoid arthritis", "code": "M06.9"},
        {"concept": "Cirrhosis", "code": "K74.60"},
        {"concept": "Hepatitis", "code": "K75.9"},
        {"concept": "Pancreatitis", "code": "K85.9"},
        {"concept": "Epilepsy", "code": "G40.909"},
        {"concept": "Migraine", "code": "G43.909"},
        {"concept": "Parkinson disease", "code": "G20"},
        {"concept": "Alzheimer disease", "code": "G30.9"},
        {"concept": "Multiple sclerosis", "code": "G35"},
        {"concept": "Systemic lupus erythematosus", "code": "M32.9"},
        {"concept": "Fever", "code": "R50.9"},
        {"concept": "Chest pain", "code": "R07.9"},
        {"concept": "Dyspnea", "code": "R06.00"},
        {"concept": "Cough", "code": "R05.9"},
        {"concept": "Headache", "code": "R51.9"},
        {"concept": "Nausea", "code": "R11.0"},
        {"concept": "Edema", "code": "R60.0"},
        {"concept": "Fatigue", "code": "R53.83"},
        {"concept": "Dizziness", "code": "R42"},
        {"concept": "Abdominal pain", "code": "R10.9"},
    ],
    "RxNorm": [
        {"concept": "Metformin", "code": "6809"},
        {"concept": "Aspirin", "code": "1191"},
        {"concept": "Glimepiride", "code": "25789"},
        {"concept": "Atorvastatin", "code": "83367"},
        {"concept": "Amlodipine", "code": "17767"},
        {"concept": "Losartan", "code": "52175"},
        {"concept": "Clopidogrel", "code": "32968"},
        {"concept": "Nitroglycerin", "code": "7832"},
        {"concept": "Lisinopril", "code": "29046"},
        {"concept": "Warfarin", "code": "11289"},
        {"concept": "Insulin", "code": "5856"},
        {"concept": "Omeprazole", "code": "7646"},
        {"concept": "Furosemide", "code": "4603"},
        {"concept": "Prednisone", "code": "8640"},
        {"concept": "Amoxicillin", "code": "723"},
        {"concept": "Ciprofloxacin", "code": "2551"},
        {"concept": "Ibuprofen", "code": "5640"},
        {"concept": "Acetaminophen", "code": "161"},
        {"concept": "Heparin", "code": "5224"},
        {"concept": "Carvedilol", "code": "20352"},
        {"concept": "Simvastatin", "code": "36567"},
        {"concept": "Levothyroxine", "code": "10582"},
        {"concept": "Gabapentin", "code": "25480"},
        {"concept": "Hydrochlorothiazide", "code": "5487"},
        {"concept": "Spironolactone", "code": "9997"},
        {"concept": "Digoxin", "code": "3407"},
        {"concept": "Morphine", "code": "7052"},
        {"concept": "Vancomycin", "code": "11124"},
        {"concept": "Enoxaparin", "code": "67108"},
        {"concept": "Pantoprazole", "code": "40790"},
    ],
    "LOINC": [
        {"concept": "Hemoglobin A1c", "code": "4548-4"},
        {"concept": "LDL Cholesterol", "code": "2089-1"},
        {"concept": "Glomerular filtration rate", "code": "48642-3"},
        {"concept": "Brain natriuretic peptide", "code": "30934-4"},
        {"concept": "Troponin I", "code": "10839-9"},
        {"concept": "C-reactive protein", "code": "1988-5"},
        {"concept": "Creatinine", "code": "2160-0"},
        {"concept": "White blood cell count", "code": "6690-2"},
        {"concept": "Hemoglobin", "code": "718-7"},
        {"concept": "Platelet count", "code": "777-3"},
        {"concept": "Glucose", "code": "2345-7"},
        {"concept": "Potassium", "code": "2823-3"},
        {"concept": "Sodium", "code": "2951-2"},
        {"concept": "Albumin", "code": "1751-7"},
        {"concept": "Bilirubin", "code": "1975-2"},
        {"concept": "ALT", "code": "1742-6"},
        {"concept": "AST", "code": "1920-8"},
        {"concept": "Blood urea nitrogen", "code": "3094-0"},
        {"concept": "TSH", "code": "3016-3"},
        {"concept": "HDL Cholesterol", "code": "2085-9"},
        {"concept": "Total Cholesterol", "code": "2093-3"},
        {"concept": "Triglycerides", "code": "2571-8"},
        {"concept": "Prothrombin time", "code": "5902-2"},
        {"concept": "INR", "code": "6301-6"},
        {"concept": "D-dimer", "code": "48066-5"},
    ],
    "SNOMED CT": [
        {"concept": "Echocardiography", "code": "40701008"},
        {"concept": "Chest X-ray", "code": "399208008"},
        {"concept": "Coronary angiography", "code": "33367005"},
        {"concept": "Coronary stent insertion", "code": "36969009"},
        {"concept": "CT scan", "code": "77477000"},
        {"concept": "MRI", "code": "113091000"},
        {"concept": "Electrocardiogram", "code": "29303009"},
        {"concept": "Blood transfusion", "code": "116859006"},
        {"concept": "Mechanical ventilation", "code": "40617009"},
        {"concept": "Dialysis", "code": "108241001"},
        {"concept": "Biopsy", "code": "86273004"},
        {"concept": "Endoscopy", "code": "423827005"},
        {"concept": "Ultrasound", "code": "16310003"},
        {"concept": "PET scan", "code": "82918005"},
        {"concept": "Cardiac catheterization", "code": "41976001"},
    ],
}

FUZZY_THRESHOLD = 85  # PRD: 85% 임계값


def map_label_to_type(label: str) -> str:
    return LABEL_TYPE_MAP.get(label, "condition")


def map_to_standard_code(entity_text: str, entity_type: str) -> Tuple[str, str, str, float]:
    """
    엔티티 텍스트를 표준 코드에 매핑.
    Returns: (omopConcept, standardCode, codeSystem, matchScore)
    """
    if entity_type == "person":
        return "Person (PII)", "-", "PII", 1.0

    code_system = TYPE_CODE_SYSTEM.get(entity_type, "ICD-10")
    entries = CODE_DICTIONARY.get(code_system, [])

    if not entries:
        return entity_text, "-", code_system, 0.0

    concepts = [e["concept"] for e in entries]
    result = process.extractOne(
        entity_text,
        concepts,
        scorer=fuzz.token_sort_ratio,
        score_cutoff=FUZZY_THRESHOLD,
    )

    if result:
        matched_concept, score, idx = result
        entry = entries[idx]
        return entry["concept"], entry["code"], code_system, score / 100.0
    else:
        return entity_text, "-", code_system, 0.0
