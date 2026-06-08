import hashlib
import hmac
import uuid

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

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
    body = await request.body()
    _verify_signature(request, body)
    payload = await request.json()
    background_tasks.add_task(_handle_linear, payload)
    return {"ok": True}


def _verify_signature(request: Request, body: bytes) -> None:
    if not settings.linear_webhook_secret:
        return
    sig = request.headers.get("linear-signature", "")
    expected = hmac.new(
        settings.linear_webhook_secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")


async def _handle_linear(payload: dict) -> None:
    tenant_id = uuid.UUID(settings.tenant_id)
    action = payload.get("action")
    type_ = payload.get("type")
    data = payload.get("data", {})

    if type_ == "Issue" and action == "create":
        await pipeline.run(issue_to_event(data, tenant_id))

    elif type_ == "Comment" and action == "create":
        await pipeline.run(comment_to_event(data, data.get("issue", {}), tenant_id))

    elif type_ == "IssueHistory" and action == "create":
        event = state_change_to_event(data, data.get("issue", {}), tenant_id)
        if event:
            await pipeline.run(event)
