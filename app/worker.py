from arq.connections import RedisSettings

from app.core.config import settings
from app.db import close_driver, close_engine, close_pool, init_driver, init_engine, init_pool
from app.extraction import pipeline
from app.schemas import CanonicalEvent


async def process_event(ctx: dict, event_data: dict) -> None:
    event = CanonicalEvent.model_validate(event_data)
    await pipeline.run(event)


async def startup(ctx: dict) -> None:
    await init_driver()
    await init_engine()
    await init_pool()


async def shutdown(ctx: dict) -> None:
    await close_pool()
    await close_engine()
    await close_driver()


class WorkerSettings:
    functions = [process_event]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    max_jobs = 10
    job_timeout = 300
