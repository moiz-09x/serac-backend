from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import health
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
    description="Brain backend — ingestion, knowledge graph retrieval, and governance services.",
    lifespan=lifespan,
)

app.include_router(health.router)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": settings.app_name, "environment": settings.environment}
