"""
XiYanSQL-QwenCoder-7B 설정

vLLM 서빙 연결 및 모델 파라미터 설정.
"""

import os

# vLLM API 연결
XIYAN_SQL_API_URL = os.getenv("XIYAN_SQL_API_URL", "http://localhost:8001/v1")
XIYAN_SQL_MODEL = os.getenv("XIYAN_SQL_MODEL", "XiYanSQL-QwenCoder-7B-2504")
XIYAN_SQL_TIMEOUT = int(os.getenv("XIYAN_SQL_TIMEOUT", "60"))

# SQL 생성 파라미터
XIYAN_SQL_DIALECT = os.getenv("XIYAN_SQL_DIALECT", "PostgreSQL")
XIYAN_SQL_TEMPERATURE = float(os.getenv("XIYAN_SQL_TEMPERATURE", "0.1"))
XIYAN_SQL_MAX_TOKENS = int(os.getenv("XIYAN_SQL_MAX_TOKENS", "1024"))
