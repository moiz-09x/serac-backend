import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import health, webhooks
from app.connectors.linear import backfill as linear_backfill
from app.core.config import settings
from app.db import (
    close_driver,
    close_engine,
    close_pool,
    init_driver,
    init_engine,
    init_pool,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_driver()
    await init_engine()
    await init_pool()
    yield
    await close_pool()
    await close_engine()
    await close_driver()


app = FastAPI(
    title=settings.app_name,
    description="Serac backend — ingestion, knowledge graph retrieval, and governance.",
    lifespan=lifespan,
)

app.include_router(health.router)
app.include_router(webhooks.router)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": settings.app_name, "environment": settings.environment}


@app.post("/admin/backfill/linear")
async def trigger_backfill():
    """Kick off a full Linear backfill for the configured tenant. Runs in background."""
    import asyncio
    tenant_id = uuid.UUID(settings.tenant_id)
    asyncio.create_task(linear_backfill.run(tenant_id, settings.linear_api_key))
    return {"status": "backfill started"}
