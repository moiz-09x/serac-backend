import uuid

from fastapi import APIRouter, BackgroundTasks, Request

from app.connectors.linear.normalizer import (
    comment_to_event,
    issue_to_event,
    state_change_to_event,
)
from app.core.config import settings
from app.extraction import pipeline

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


@router.post("/linear")
async def linear_webhook(request: Request, background_tasks: BackgroundTasks):
    payload = await request.json()
    background_tasks.add_task(_handle_linear, payload)
    return {"ok": True}


async def _handle_linear(payload: dict) -> None:
    tenant_id = uuid.UUID(settings.tenant_id)
    action = payload.get("action")
    type_ = payload.get("type")
    data = payload.get("data", {})

    if type_ == "Issue" and action == "create":
        await pipeline.run(issue_to_event(data, tenant_id))

    elif type_ == "Comment" and action == "create":
        await pipeline.run(comment_to_event(data, data.get("issue", {}), tenant_id))
