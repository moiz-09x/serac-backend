import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

from app.connectors.slack.client import get_user_email, get_web_client
from app.connectors.slack.normalizer import message_to_event
from app.extraction import pipeline

log = logging.getLogger(__name__)


async def run(tenant_id: uuid.UUID) -> None:
    client = get_web_client()
    cutoff = str((datetime.now(UTC) - timedelta(days=365)).timestamp())

    resp = await client.conversations_list(types="public_channel,private_channel", limit=200)
    channels = resp["channels"]
    log.info("Slack backfill: %d channels found", len(channels))

    for channel in channels:
        member = channel.get("is_member", False)
        log.info("  #%s (id=%s) is_member=%s", channel.get("name"), channel.get("id"), member)
        if not member:
            continue
        await _backfill_channel(client, channel, tenant_id, cutoff)
        await asyncio.sleep(0.5)  # Slack Tier 2 rate limit


async def _backfill_channel(client, channel: dict, tenant_id: uuid.UUID, cutoff: str) -> None:
    channel_id = channel["id"]
    is_private = channel.get("is_private", False)
    cursor = None

    while True:
        resp = await client.conversations_history(
            channel=channel_id,
            oldest=cutoff,
            limit=200,
            cursor=cursor,
        )
        for msg in resp["messages"]:
            if msg.get("subtype"):  # skip system messages (joins, topic changes, etc.)
                continue
            user_id = msg.get("user", "unknown")
            email = await get_user_email(client, user_id)
            event = message_to_event(msg, channel_id, is_private, tenant_id, email_hint=email)
            await pipeline.run(event)

            # Pull thread replies if this is a parent message
            if msg.get("reply_count", 0) > 0:
                await _backfill_thread(client, channel_id, is_private, msg["ts"], tenant_id)

            await asyncio.sleep(0.05)

        if not resp.get("has_more"):
            break
        cursor = resp["response_metadata"]["next_cursor"]


async def _backfill_thread(
    client, channel_id: str, is_private: bool, thread_ts: str, tenant_id: uuid.UUID
) -> None:
    resp = await client.conversations_replies(channel=channel_id, ts=thread_ts, limit=200)
    for msg in resp["messages"][1:]:  # first message is the parent, already processed
        user_id = msg.get("user", "unknown")
        email = await get_user_email(client, user_id)
        event = message_to_event(msg, channel_id, is_private, tenant_id, email_hint=email)
        await pipeline.run(event)
