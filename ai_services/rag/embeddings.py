"""
임베딩 서비스 — sentence-transformers 싱글톤

paraphrase-multilingual-MiniLM-L12-v2 (420MB, 한국어 지원, 384차원)
CUDA GPU 사용 가능 시 자동으로 GPU에서 실행합니다.
"""

import logging
import os
import threading
from typing import List, Optional

logger = logging.getLogger(__name__)

# 모델 이름 상수
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
EMBEDDING_DIM = 384


class EmbeddingService:
    """sentence-transformers 임베딩 싱글톤."""

    _instance: Optional["EmbeddingService"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "EmbeddingService":
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._model = None
        self._initialized = True

    def _load_model(self):
        """모델을 지연 로드합니다. CUDA GPU 사용 가능 시 자동 활용."""
        if self._model is not None:
            return
        try:
            import torch
            from sentence_transformers import SentenceTransformer

            # GPU 디바이스 결정: EMBEDDING_DEVICE 환경변수 > CUDA 자동 감지 > CPU
            device = os.getenv("EMBEDDING_DEVICE", "")
            if not device:
                if torch.cuda.is_available():
                    # VRAM 여유가 큰 GPU 선택
                    gpu_count = torch.cuda.device_count()
                    if gpu_count > 1:
                        free_mem = []
                        for i in range(gpu_count):
                            free, _ = torch.cuda.mem_get_info(i)
                            free_mem.append(free)
                        best = free_mem.index(max(free_mem))
                        device = f"cuda:{best}"
                    else:
                        device = "cuda:0"
                else:
                    device = "cpu"

            logger.info(f"Loading embedding model: {MODEL_NAME} on {device}")
            self._model = SentenceTransformer(MODEL_NAME, device=device)
            logger.info(f"Embedding model loaded (dim={EMBEDDING_DIM}, device={device})")
        except Exception as e:
            logger.error(f"Failed to load embedding model: {e}")
            raise

    @property
    def is_loaded(self) -> bool:
        return self._model is not None

    def embed_query(self, text: str) -> List[float]:
        """단일 텍스트를 임베딩합니다.

        Args:
            text: 임베딩할 텍스트

        Returns:
            384차원 float 벡터
        """
        self._load_model()
        embedding = self._model.encode(text, normalize_embeddings=True)
        return embedding.tolist()

    def embed_batch(self, texts: List[str], batch_size: int = 32) -> List[List[float]]:
        """텍스트 배치를 임베딩합니다.

        Args:
            texts: 임베딩할 텍스트 리스트
            batch_size: 배치 크기

        Returns:
            384차원 float 벡터 리스트
        """
        self._load_model()
        embeddings = self._model.encode(
            texts, normalize_embeddings=True, batch_size=batch_size
        )
        return embeddings.tolist()
