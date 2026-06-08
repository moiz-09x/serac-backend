import redis.asyncio as aioredis
from arq.connections import ArqRedis, RedisSettings, create_pool

from app.core.config import settings

_pool: aioredis.ConnectionPool | None = None
_arq_pool: ArqRedis | None = None


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


async def init_arq_pool() -> None:
    global _arq_pool
    _arq_pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))


async def close_arq_pool() -> None:
    global _arq_pool
    if _arq_pool:
        await _arq_pool.aclose()
        _arq_pool = None


def get_arq_pool() -> ArqRedis:
    if _arq_pool is None:
        raise RuntimeError("Arq pool not initialised — did lifespan run?")
    return _arq_pool
