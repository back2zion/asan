"""
Prompt Enhancement 설정

LLM 엔드포인트 및 확장 관련 설정을 관리합니다.
"""

import os

# LLM API 설정
PROMPT_ENHANCEMENT_API_URL = os.getenv(
    "PROMPT_ENHANCEMENT_API_URL", "http://localhost:8888/v1"
)
PROMPT_ENHANCEMENT_MODEL = os.getenv(
    "PROMPT_ENHANCEMENT_MODEL", "Qwen3-32B-AWQ"
)

# 생성 파라미터
ENHANCEMENT_TEMPERATURE = float(os.getenv("ENHANCEMENT_TEMPERATURE", "0.3"))
ENHANCEMENT_MAX_TOKENS = int(os.getenv("ENHANCEMENT_MAX_TOKENS", "512"))
ENHANCEMENT_TIMEOUT = int(os.getenv("ENHANCEMENT_TIMEOUT", "30"))

# 기능 활성화
ENABLE_PROMPT_ENHANCEMENT = os.getenv(
    "ENABLE_PROMPT_ENHANCEMENT", "true"
).lower() == "true"

# 신뢰도 임계값 (이 값 미만이면 확장 미적용)
ENHANCEMENT_MIN_CONFIDENCE = float(os.getenv("ENHANCEMENT_MIN_CONFIDENCE", "0.7"))
