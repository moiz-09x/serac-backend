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
from app.db import get_arq_pool

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

    event = None
    if type_ == "Issue" and action == "create":
        event = issue_to_event(data, tenant_id)
    elif type_ == "Comment" and action == "create":
        event = comment_to_event(data, data.get("issue", {}), tenant_id)
    elif type_ == "IssueHistory" and action == "create":
        event = state_change_to_event(data, data.get("issue", {}), tenant_id)

    if event:
        await get_arq_pool().enqueue_job("process_event", event.model_dump(mode="json"))


@router.post("/notion")
async def notion_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.body()
    _verify_notion_signature(request, body)
    payload = await request.json()
    background_tasks.add_task(_handle_notion, payload)
    return {"ok": True}


def _verify_notion_signature(request: Request, body: bytes) -> None:
    if not settings.notion_webhook_secret:
        return
    sig = request.headers.get("x-notion-signature", "")
    expected = "v0=" + hmac.new(
        settings.notion_webhook_secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")


async def _handle_notion(payload: dict) -> None:
    from sqlalchemy import select
    from app.connectors.notion.backfill import extract_page_text
    from app.connectors.notion.client import NotionClient
    from app.connectors.notion.normalizer import page_to_event, page_updated_to_event
    from app.db import get_session
    from app.db.models import ConnectorToken

    tenant_id = uuid.UUID(settings.tenant_id)
    event_type = payload.get("type", "")
    entity = payload.get("entity", {})
    page_id = entity.get("id", "")

    if not page_id or event_type not in ("page.created", "page.updated"):
        return

    # prefer OAuth token from DB; fall back to static config key for dev
    api_key = settings.notion_api_key
    async with get_session() as session:
        result = await session.execute(
            select(ConnectorToken).where(
                ConnectorToken.tenant_id == tenant_id,
                ConnectorToken.platform == "notion",
            )
        )
        token = result.scalar_one_or_none()
        if token:
            api_key = token.access_token

    if not api_key:
        return

    async with NotionClient(api_key) as client:
        page = await client.get_page(page_id)
        text = await extract_page_text(client, page_id)
        if event_type == "page.created":
            event = page_to_event(page, text, tenant_id)
        else:
            event = page_updated_to_event(page, text, tenant_id)

    await get_arq_pool().enqueue_job("process_event", event.model_dump(mode="json"))
