import redis.asyncio as aioredis

from app.core.config import settings

_pool: aioredis.ConnectionPool | None = None


async def init_pool() -> None:
    global _pool
    _pool = aioredis.ConnectionPool.from_url(settings.redis_url, decode_responses=True)
    client = aioredis.Redis(connection_pool=_pool)
    await client.ping()
    await client.aclose()


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.aclose()
        _pool = None


def get_client() -> aioredis.Redis:
    if _pool is None:
        raise RuntimeError("Redis pool not initialised — did lifespan run?")
    return aioredis.Redis(connection_pool=_pool)
