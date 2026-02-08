"""
IDP API 설정
"""
from pydantic_settings import BaseSettings
from typing import List
import os


class Settings(BaseSettings):
    # 기본 설정
    APP_NAME: str = "IDP API"
    DEBUG: bool = os.getenv("DEBUG", "false").lower() == "true"

    # CORS
    CORS_ORIGINS: List[str] = [
        "http://localhost:5173",
        "http://localhost:18000",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "http://127.0.0.1:18000",
    ]

    # Database
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "asan")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "asan2025!")
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "asan_db")

    # Redis
    REDIS_HOST: str = os.getenv("REDIS_HOST", "redis")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_PASSWORD: str = os.getenv("REDIS_PASSWORD", "asan2025!")

    # Milvus Vector DB
    MILVUS_HOST: str = os.getenv("MILVUS_HOST", "milvus")
    MILVUS_PORT: int = int(os.getenv("MILVUS_PORT", "19530"))

    # MinIO S3 Object Storage
    MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() == "true"

    # LLM (XiYanSQL-QwenCoder-7B via vLLM on port 8001)
    LLM_API_URL: str = os.getenv("LLM_API_URL", "http://localhost:8001/v1")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "XiYanSQL-QwenCoder-7B-2504")
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")

    # Embedding
    EMBEDDING_API_URL: str = os.getenv("EMBEDDING_API_URL", "http://localhost:8082")

    # XiYan SQL (vLLM)
    XIYAN_SQL_API_URL: str = os.getenv("XIYAN_SQL_API_URL", "http://localhost:8001/v1")
    XIYAN_SQL_MODEL: str = os.getenv("XIYAN_SQL_MODEL", "XiYanSQL-QwenCoder-7B-2504")

    # Conversation Memory
    CONVERSATION_MEMORY_DB: str = os.getenv("CONVERSATION_MEMORY_DB", "conversation_memory.db")
    CONVERSATION_SESSION_TTL_MINUTES: int = int(os.getenv("CONVERSATION_SESSION_TTL_MINUTES", "30"))
    CONVERSATION_MAX_TURNS: int = int(os.getenv("CONVERSATION_MAX_TURNS", "50"))
    CONVERSATION_USE_POSTGRES: bool = os.getenv("CONVERSATION_USE_POSTGRES", "false").lower() == "true"

    @property
    def DATABASE_URL(self) -> str:
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @property
    def REDIS_URL(self) -> str:
        return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/0"

    class Config:
        env_file = ".env"


settings = Settings()
