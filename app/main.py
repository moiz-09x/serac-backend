"""FastAPI application entrypoint.

Single backend serving all logical services described in
Docs/00-Overview/01-system-architecture.md §3 (extraction pipeline,
retrieval & query layer, rules induction, permission resolution, admin API).
They share one process/deployable for v1; split out later only if a specific
piece needs independent scaling.

Run locally:
    uv run uvicorn app.main:app --reload
"""

from fastapi import FastAPI

from app.api.routes import health
from app.core.config import settings

app = FastAPI(
    title=settings.app_name,
    description="Brain backend — ingestion, knowledge graph retrieval, and governance services.",
)

app.include_router(health.router)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": settings.app_name, "environment": settings.environment}
