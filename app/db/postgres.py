from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.core.config import settings

_engine: AsyncEngine | None = None


async def init_engine() -> None:
    global _engine
    _engine = create_async_engine(settings.postgres_dsn, echo=False)
    async with _engine.begin() as conn:
        await conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS thread_embeddings (
                thread_id  TEXT PRIMARY KEY,
                tenant_id  TEXT NOT NULL,
                embedding  vector({settings.embedding_dim}) NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """))


async def close_engine() -> None:
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Postgres engine not initialised — did lifespan run?")
    return _engine
