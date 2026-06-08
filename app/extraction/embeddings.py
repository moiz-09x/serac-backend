import uuid
from datetime import datetime, timezone
from functools import lru_cache

from fastembed import TextEmbedding
from sqlalchemy import text

from app.core.config import settings
from app.db import get_engine


@lru_cache(maxsize=1)
def _model() -> TextEmbedding:
    return TextEmbedding(model_name=settings.embedding_model)


def embed(content: str) -> list[float]:
    return [float(v) for v in next(_model().embed([content]))]


async def find_similar_thread(tenant_id: str, vector: list[float]) -> uuid.UUID | None:
    query = text("""
        SELECT thread_id, 1 - (embedding <=> CAST(:vec AS vector)) AS similarity
        FROM thread_embeddings
        WHERE tenant_id = :tid
        ORDER BY embedding <=> CAST(:vec AS vector)
        LIMIT 1
    """)
    async with get_engine().connect() as conn:
        row = (await conn.execute(query, {
            "vec": str(vector),
            "tid": tenant_id,
        })).fetchone()
    if row and row.similarity >= settings.thread_attach_threshold:
        return uuid.UUID(row.thread_id)
    return None


async def store_thread_embedding(thread_id: uuid.UUID, tenant_id: str, vector: list[float]) -> None:
    query = text("""
        INSERT INTO thread_embeddings (thread_id, tenant_id, embedding, created_at)
        VALUES (:tid, :tenant, CAST(:vec AS vector), :now)
        ON CONFLICT (thread_id) DO NOTHING
    """)
    async with get_engine().begin() as conn:
        await conn.execute(query, {
            "tid": str(thread_id),
            "tenant": tenant_id,
            "vec": str(vector),
            "now": datetime.now(timezone.utc),
        })
