"""
한국어 의료 텍스트 처리
- 한국어 감지
- 의료용어 사전 기반 NER
- 한영 매핑
"""
import json
import re
from pathlib import Path
from typing import List, Dict, Any


def load_korean_dict() -> List[Dict[str, Any]]:
    dict_path = Path(__file__).parent / "dictionaries" / "korean_medical.json"
    with open(dict_path, "r", encoding="utf-8") as f:
        return json.load(f)


KOREAN_DICT: List[Dict[str, Any]] = []


def get_korean_dict() -> List[Dict[str, Any]]:
    global KOREAN_DICT
    if not KOREAN_DICT:
        KOREAN_DICT = load_korean_dict()
    return KOREAN_DICT


def is_korean(text: str) -> bool:
    korean_chars = sum(1 for c in text if '\uac00' <= c <= '\ud7a3')
    return korean_chars / max(len(text), 1) > 0.15


def extract_korean_entities(text: str) -> List[Dict[str, Any]]:
    """한국어 의료용어 사전 기반 엔티티 추출"""
    entries = get_korean_dict()
    entities = []
    used_ranges = []

    for entry in entries:
        pattern = re.compile(re.escape(entry["term"]))
        for match in pattern.finditer(text):
            start, end = match.start(), match.end()
            # Skip overlapping
            if any(start < ur[1] and end > ur[0] for ur in used_ranges):
                continue
            used_ranges.append((start, end))
            entities.append({
                "text": match.group(),
                "type": entry["type"],
                "start": start,
                "end": end,
                "omopConcept": entry["omopConcept"],
                "standardCode": entry["standardCode"],
                "codeSystem": entry["codeSystem"],
                "confidence": entry.get("confidence", 0.90),
                "source": "korean_dict",
            })

    # 한국어 인물명 패턴 (PII)
    pii_exclude = {'남성', '여성', '환자', '소견', '진단', '처방', '검사', '결과', '치료', '시행', '확인', '필요', '동반', '상승', '가능', '추가', '경과', '관찰', '병력'}
    pii_pattern = re.compile(r'[가-힣]{2,4}(?=\s*(?:환자|씨)|\(|,\s*만?\s*\d)')
    for match in pii_pattern.finditer(text):
        if match.group() in pii_exclude:
            continue
        start, end = match.start(), match.end()
        if any(start < ur[1] and end > ur[0] for ur in used_ranges):
            continue
        used_ranges.append((start, end))
        entities.append({
            "text": match.group(),
            "type": "person",
            "start": start,
            "end": end,
            "omopConcept": "Person name (PII)",
            "standardCode": "-",
            "codeSystem": "PII",
            "confidence": 0.88,
            "source": "korean_regex",
        })

    # 한국어 텍스트 내 영어 의료 용어 추출 (약물명, 검사치 등)
    # 예: "HbA1c 7.8%", "Metformin 500mg"
    eng_patterns = [
        (re.compile(r'HbA1c\s*[\d.]+%?'), 'measurement', 'Hemoglobin A1c', '4548-4', 'LOINC', 0.97),
        (re.compile(r'LDL\s*\d+\s*mg/dL'), 'measurement', 'LDL Cholesterol', '2089-1', 'LOINC', 0.95),
        (re.compile(r'eGFR\s*\d+'), 'measurement', 'Glomerular filtration rate', '48642-3', 'LOINC', 0.93),
        (re.compile(r'BNP\s*\d+\s*pg/mL'), 'measurement', 'Brain natriuretic peptide', '30934-4', 'LOINC', 0.94),
        (re.compile(r'Troponin[- ]?I\s*[\d.]+\s*ng/mL', re.I), 'measurement', 'Troponin I', '10839-9', 'LOINC', 0.96),
        (re.compile(r'CRP\s*[\d.]+\s*mg/L'), 'measurement', 'C-reactive protein', '1988-5', 'LOINC', 0.94),
        (re.compile(r'Creatinine\s*[\d.]+\s*mg/dL', re.I), 'measurement', 'Creatinine', '2160-0', 'LOINC', 0.95),
        (re.compile(r'WBC\s*[\d.]+'), 'measurement', 'White blood cell count', '6690-2', 'LOINC', 0.93),
        (re.compile(r'Metformin\s*\d*\s*mg', re.I), 'drug', 'Metformin', '6809', 'RxNorm', 0.98),
        (re.compile(r'Aspirin\s*\d*\s*mg', re.I), 'drug', 'Aspirin', '1191', 'RxNorm', 0.97),
        (re.compile(r'Glimepiride\s*\d*\s*mg', re.I), 'drug', 'Glimepiride', '25789', 'RxNorm', 0.95),
        (re.compile(r'Atorvastatin\s*\d*\s*mg', re.I), 'drug', 'Atorvastatin', '83367', 'RxNorm', 0.96),
        (re.compile(r'Amlodipine\s*\d*\s*mg', re.I), 'drug', 'Amlodipine', '17767', 'RxNorm', 0.95),
        (re.compile(r'Losartan\s*\d*\s*mg', re.I), 'drug', 'Losartan', '52175', 'RxNorm', 0.94),
        (re.compile(r'Clopidogrel\s*\d*\s*mg', re.I), 'drug', 'Clopidogrel', '32968', 'RxNorm', 0.96),
        (re.compile(r'Nitroglycerin', re.I), 'drug', 'Nitroglycerin', '7832', 'RxNorm', 0.93),
        (re.compile(r'Cardiomegaly', re.I), 'condition', 'Cardiomegaly', 'I51.7', 'ICD-10', 0.91),
    ]
    for pat, etype, concept, code, system, conf in eng_patterns:
        for match in pat.finditer(text):
            start, end = match.start(), match.end()
            if any(start < ur[1] and end > ur[0] for ur in used_ranges):
                continue
            used_ranges.append((start, end))
            entities.append({
                "text": match.group(),
                "type": etype,
                "start": start,
                "end": end,
                "omopConcept": concept,
                "standardCode": code,
                "codeSystem": system,
                "confidence": conf,
                "source": "korean_eng_pattern",
            })

    entities.sort(key=lambda e: e["start"])
    return entities
