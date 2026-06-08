from functools import lru_cache

from fastembed import TextEmbedding

from app.core.config import settings


@lru_cache(maxsize=1)
def _model() -> TextEmbedding:
    return TextEmbedding(model_name=settings.embedding_model)


def embed(content: str) -> list[float]:
    return [float(v) for v in next(_model().embed([content]))]
