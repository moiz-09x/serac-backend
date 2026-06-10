import hashlib
import hmac
import time
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


@router.post("/slack")
async def slack_webhook(request: Request, background_tasks: BackgroundTasks):
    """Handles Slack Event Subscriptions (webhook mode only)."""
    body = await request.body()
    _verify_slack_signature(request, body)
    payload = await request.json()

    # Slack sends a one-time URL verification challenge when you first save the URL
    if payload.get("type") == "url_verification":
        return {"challenge": payload.get("challenge")}

    background_tasks.add_task(_handle_slack, payload)
    return {"ok": True}


def _verify_slack_signature(request: Request, body: bytes) -> None:
    if not settings.slack_signing_secret:
        return
    timestamp = request.headers.get("x-slack-request-timestamp", "")
    sig = request.headers.get("x-slack-signature", "")

    if abs(time.time() - int(timestamp)) > 300:
        raise HTTPException(status_code=401, detail="Request too old")

    basestring = f"v0:{timestamp}:{body.decode()}"
    expected = (
        "v0="
        + hmac.new(
            settings.slack_signing_secret.encode(),
            basestring.encode(),
            hashlib.sha256,
        ).hexdigest()
    )
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(status_code=401, detail="Invalid Slack signature")


async def _handle_slack(payload: dict) -> None:
    from app.connectors.slack.client import get_user_email, get_web_client
    from app.connectors.slack.normalizer import message_to_event

    event = payload.get("event", {})
    if event.get("type") not in ("message", "message.channels", "message.groups"):
        return
    if event.get("subtype"):
        return

    tenant_id = uuid.UUID(settings.tenant_id)
    channel_id = event.get("channel", "")
    is_private = event.get("channel_type") == "group"
    user_id = event.get("user", "unknown")

    token = None
    try:
        from app.connectors.credentials import credentials as cred_store

        token = await cred_store.get_token(str(tenant_id), "slack")
    except LookupError:
        pass

    client = get_web_client(token)
    email = await get_user_email(client, user_id)
    canonical = message_to_event(event, channel_id, is_private, tenant_id, email_hint=email)
    await get_arq_pool().enqueue_job("process_event", canonical.model_dump(mode="json"))


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
    expected = (
        "v0="
        + hmac.new(
            settings.notion_webhook_secret.encode(),
            body,
            hashlib.sha256,
        ).hexdigest()
    )
    if not hmac.compare_digest(sig, expected):
        raise HTTPException(status_code=401, detail="Invalid webhook signature")


async def _handle_notion(payload: dict) -> None:
    from app.connectors.notion.backfill import extract_page_text
    from app.connectors.notion.client import NotionClient
    from app.connectors.notion.normalizer import page_to_event, page_updated_to_event

    tenant_id = uuid.UUID(settings.tenant_id)
    event_type = payload.get("type", "")
    entity = payload.get("entity", {})
    page_id = entity.get("id", "")

    if not page_id or event_type not in ("page.created", "page.updated"):
        return

    from app.connectors.credentials import credentials as cred_store

    try:
        notion_token = await cred_store.get_token(str(tenant_id), "notion")
    except LookupError:
        notion_token = settings.notion_api_key

    async with NotionClient(notion_token) as client:
        page = await client.get_page(page_id)
        text = await extract_page_text(client, page_id)
        if event_type == "page.created":
            event = page_to_event(page, text, tenant_id)
        else:
            event = page_updated_to_event(page, text, tenant_id)

    await get_arq_pool().enqueue_job("process_event", event.model_dump(mode="json"))
