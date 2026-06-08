import uuid

from slack_bolt.async_app import AsyncApp
from slack_bolt.adapter.socket_mode.async_handler import AsyncSocketModeHandler

from app.connectors.slack.normalizer import message_to_event
from app.core.config import settings
from app.extraction import pipeline

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
        canonical = message_to_event(event, channel_id, is_private, tenant_id)
        await pipeline.run(canonical)

    return app


async def start() -> None:
    global _handler
    if not settings.slack_app_token or not settings.slack_bot_token:
        return
    _handler = AsyncSocketModeHandler(_make_app(), settings.slack_app_token)
    await _handler.start_async()


async def stop() -> None:
    global _handler
    if _handler:
        await _handler.close_async()
        _handler = None
