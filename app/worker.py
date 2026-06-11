import logging

from arq import cron
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


log = logging.getLogger(__name__)


async def refresh_expiring_tokens(ctx: dict) -> None:
    """Refresh OAuth tokens expiring within the next hour."""
    from app.connectors.credentials import credentials

    expiring = await credentials.get_expiring(within_seconds=3600)
    for cred in expiring:
        try:
            if cred.integration == "notion":
                # Notion tokens don't expire in practice (1-year TTL, no refresh flow)
                continue
            # Linear and Slack tokens are long-lived — placeholder for future refresh logic
            log.info("Token refresh check: %s / %s", cred.tenant_id, cred.integration)
        except Exception:
            log.exception("Failed to refresh token for %s / %s", cred.tenant_id, cred.integration)


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
    functions = [process_event, link_related_threads, refresh_expiring_tokens]
    cron_jobs = [
        cron(refresh_expiring_tokens, minute={0, 30}),  # every 30 mins
    ]
    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    max_jobs = 10
    job_timeout = 300
