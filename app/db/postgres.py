from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from app.core.config import settings

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


async def init_engine() -> None:
    global _engine, _session_factory
    _engine = create_async_engine(settings.postgres_dsn, echo=False)
    _session_factory = async_sessionmaker(_engine, expire_on_commit=False)
    from app.db.models import Base  # late import avoids circular at module load
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_engine() -> None:
    global _engine, _session_factory
    if _engine:
        await _engine.dispose()
        _engine = None
    _session_factory = None


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Postgres engine not initialised — did lifespan run?")
    return _engine


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if _session_factory is None:
        raise RuntimeError("Postgres session factory not initialised — did lifespan run?")
    async with _session_factory() as session:
        yield session
