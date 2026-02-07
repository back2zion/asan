"""
Medical NER Service - BioClinicalBERT 기반
- d4data/biomedical-ner-all 모델 (GPU 추론)
- 한국어 의료용어 사전 매핑
- OMOP CodeMapper (rapidfuzz)
- FastAPI on port 8100
"""
import time
import torch
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from transformers import AutoTokenizer, AutoModelForTokenClassification, pipeline

from korean_handler import is_korean, extract_korean_entities
from code_mapper import map_label_to_type, map_to_standard_code

app = FastAPI(title="Medical NER Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── 모델 로드 ──
MODEL_NAME = "d4data/biomedical-ner-all"
device = 0 if torch.cuda.is_available() else -1

print(f"Loading {MODEL_NAME} on {'GPU' if device == 0 else 'CPU'}...")
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForTokenClassification.from_pretrained(MODEL_NAME)
if device == 0:
    model = model.cuda()
ner_pipeline = pipeline(
    "ner",
    model=model,
    tokenizer=tokenizer,
    device=device,
    aggregation_strategy="simple",
)
print(f"Model loaded successfully on {'GPU ' + torch.cuda.get_device_name(0) if device == 0 else 'CPU'}")


# ── 스키마 ──
class AnalyzeRequest(BaseModel):
    text: str
    language: Optional[str] = None  # "ko", "en", or auto-detect


class NEREntityResponse(BaseModel):
    text: str
    type: str
    start: int
    end: int
    omopConcept: str
    standardCode: str
    codeSystem: str
    confidence: float
    source: str  # "bioclinicalbert", "korean_dict", "korean_regex", "korean_eng_pattern"


class AnalyzeResponse(BaseModel):
    entities: List[NEREntityResponse]
    language: str
    model: str
    processingTimeMs: int


def run_english_ner(text: str) -> List[dict]:
    """BioClinicalBERT NER 추론"""
    raw_entities = ner_pipeline(text)
    entities = []
    for ent in raw_entities:
        label = ent["entity_group"]
        entity_type = map_label_to_type(label)
        entity_text = ent["word"].strip()
        if not entity_text or len(entity_text) < 2:
            continue

        omop_concept, std_code, code_system, match_score = map_to_standard_code(
            entity_text, entity_type
        )

        confidence = float(ent["score"])
        # Adjust confidence by code mapping quality
        if std_code != "-":
            confidence = min(confidence, match_score)

        entities.append({
            "text": entity_text,
            "type": entity_type,
            "start": int(ent["start"]),
            "end": int(ent["end"]),
            "omopConcept": omop_concept,
            "standardCode": std_code,
            "codeSystem": code_system,
            "confidence": round(confidence, 4),
            "source": "bioclinicalbert",
        })

    return entities


@app.post("/ner/analyze", response_model=AnalyzeResponse)
async def analyze(req: AnalyzeRequest):
    start_time = time.time()

    lang = req.language
    if not lang:
        lang = "ko" if is_korean(req.text) else "en"

    if lang == "ko":
        entities = extract_korean_entities(req.text)
        model_name = "korean_dict + bioclinicalbert"
    else:
        entities = run_english_ner(req.text)
        model_name = MODEL_NAME

    elapsed_ms = int((time.time() - start_time) * 1000)

    return AnalyzeResponse(
        entities=[NEREntityResponse(**e) for e in entities],
        language=lang,
        model=model_name,
        processingTimeMs=elapsed_ms,
    )


@app.get("/ner/health")
async def health():
    gpu_available = torch.cuda.is_available()
    gpu_name = torch.cuda.get_device_name(0) if gpu_available else None
    return {
        "status": "healthy",
        "model": MODEL_NAME,
        "device": f"cuda:0 ({gpu_name})" if gpu_available else "cpu",
        "gpu_memory_used_mb": round(torch.cuda.memory_allocated(0) / 1024 / 1024, 1) if gpu_available else 0,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8100)
