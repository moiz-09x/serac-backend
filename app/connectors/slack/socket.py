import asyncio
import uuid

from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler
from slack_bolt.async_app import AsyncApp

from app.connectors.slack.client import get_user_email, get_web_client
from app.connectors.slack.normalizer import message_to_event
from app.core.config import settings
from app.db import get_arq_pool

_handler: AsyncSocketModeHandler | None = None


def _make_app() -> AsyncApp:
    app = AsyncApp(token=settings.slack_bot_token, ignoring_self_events_enabled=True)

    @app.event("message")
    async def handle_message(event: dict, **_):
        if event.get("subtype"):
            return
        tenant_id = uuid.UUID(settings.tenant_id)
        channel_id = event.get("channel", "")
        # conversations_info would tell us if it's private, but that's an extra API call
        # per message. We store is_private=False as default; backfill sets it correctly.
        # For real-time events the access scope channel ID is what matters for VISIBLE_TO edges.
        is_private = event.get("channel_type") == "group"
        user_id = event.get("user", "unknown")
        email = await get_user_email(get_web_client(), user_id)
        canonical = message_to_event(event, channel_id, is_private, tenant_id, email_hint=email)
        await get_arq_pool().enqueue_job("process_event", canonical.model_dump(mode="json"))

    return app


async def start() -> None:
    global _handler
    if not settings.slack_app_token or not settings.slack_bot_token:
        return
    _handler = AsyncSocketModeHandler(_make_app(), settings.slack_app_token)
    asyncio.create_task(_handler.start_async())


async def stop() -> None:
    global _handler
    if _handler:
        await _handler.close_async()
        _handler = None
