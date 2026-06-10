from arq.connections import RedisSettings

from app.core.config import settings
from app.db import (
    close_arq_pool,
    close_driver,
    close_engine,
    close_pool,
    init_arq_pool,
    init_driver,
    init_engine,
    init_pool,
)
from app.extraction import pipeline
from app.extraction.thread_linker import link_related_threads
from app.schemas import CanonicalEvent


async def process_event(ctx: dict, event_data: dict) -> None:
    event = CanonicalEvent.model_validate(event_data)
    await pipeline.run(event)


async def startup(ctx: dict) -> None:
    await init_driver()
    await init_engine()
    await init_pool()
    await init_arq_pool()


async def shutdown(ctx: dict) -> None:
    await close_arq_pool()
    await close_pool()
    await close_engine()
    await close_driver()


class WorkerSettings:
    functions = [process_event, link_related_threads]
    cron_jobs = []
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    max_jobs = 10
    job_timeout = 300
