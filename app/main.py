import logging
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)

from app.api.routes import health, webhooks
from app.api.routes.graph import router as graph_router
from app.api.routes.query import router as query_router
from app.connectors.linear import backfill as linear_backfill
from app.connectors.notion import backfill as notion_backfill
from app.connectors.slack import backfill as slack_backfill
from app.connectors.slack import socket as slack_socket
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_driver()
    await init_engine()
    await init_pool()
    await init_arq_pool()
    await slack_socket.start()
    yield
    await slack_socket.stop()
    await close_arq_pool()
    await close_pool()
    await close_engine()
    await close_driver()


app = FastAPI(
    title=settings.app_name,
    description="Serac backend — ingestion, knowledge graph retrieval, and governance.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(webhooks.router)
app.include_router(query_router)
app.include_router(graph_router)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": settings.app_name, "environment": settings.environment}


@app.post("/admin/backfill/linear")
async def trigger_linear_backfill():
    import asyncio
    asyncio.create_task(linear_backfill.run(uuid.UUID(settings.tenant_id), settings.linear_api_key))
    return {"status": "linear backfill started"}


@app.post("/admin/backfill/slack")
async def trigger_slack_backfill():
    import asyncio
    asyncio.create_task(slack_backfill.run(uuid.UUID(settings.tenant_id)))
    return {"status": "slack backfill started"}


@app.post("/admin/backfill/notion")
async def trigger_notion_backfill():
    import asyncio
    asyncio.create_task(notion_backfill.run(uuid.UUID(settings.tenant_id), settings.notion_api_key))
    return {"status": "notion backfill started"}


