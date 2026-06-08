from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.core.config import settings

_engine: AsyncEngine | None = None


async def init_engine() -> None:
    global _engine
    _engine = create_async_engine(settings.postgres_dsn, echo=False)


async def close_engine() -> None:
    global _engine
    if _engine:
        await _engine.dispose()
        _engine = None


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Postgres engine not initialised — did lifespan run?")
    return _engine
