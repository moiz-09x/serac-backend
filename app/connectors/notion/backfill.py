import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta

from app.connectors.notion.client import NotionClient
from app.connectors.notion.normalizer import comment_to_event, page_to_event
from app.extraction import pipeline

log = logging.getLogger(__name__)

_TEXT_BLOCK_TYPES = frozenset(
    {
        "paragraph",
        "heading_1",
        "heading_2",
        "heading_3",
        "bulleted_list_item",
        "numbered_list_item",
        "toggle",
        "quote",
        "callout",
        "code",
        "to_do",
    }
)

_SLEEP = 0.35  # ~3 req/s — Notion's documented rate limit


async def extract_page_text(client: NotionClient, block_id: str, depth: int = 0) -> str:
    """Recursively extract plain text from all blocks. Shared with the webhook handler."""
    if depth > 3:
        return ""
    parts: list[str] = []
    cursor = None
    while True:
        data = await client.get_block_children(block_id, start_cursor=cursor)
        await asyncio.sleep(_SLEEP)
        for block in data.get("results", []):
            btype = block.get("type", "")
            if btype in _TEXT_BLOCK_TYPES:
                rich_text = block.get(btype, {}).get("rich_text", [])
                text = "".join(rt.get("plain_text", "") for rt in rich_text)
                if text:
                    parts.append(text)
            # recurse into nested blocks, but not child pages or databases
            if block.get("has_children") and btype not in ("child_page", "child_database"):
                child_text = await extract_page_text(client, block["id"], depth + 1)
                if child_text:
                    parts.append(child_text)
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return "\n".join(parts)


async def _resolve_token(tenant_id: uuid.UUID, api_key: str | None) -> str:
    if api_key:
        return api_key
    from app.connectors.credentials import credentials
    return await credentials.get_token(str(tenant_id), "notion")


async def run(tenant_id: uuid.UUID, api_key: str | None = None) -> None:
    token = await _resolve_token(tenant_id, api_key)
    cutoff = datetime.now(UTC) - timedelta(days=365)
    async with NotionClient(token) as client:
        await _backfill_pages(client, tenant_id, cutoff)


async def _backfill_pages(client: NotionClient, tenant_id: uuid.UUID, cutoff: datetime) -> None:
    cursor = None
    page_count = 0
    while True:
        data = await client.search(filter_type="page", start_cursor=cursor)
        await asyncio.sleep(_SLEEP)
        for page in data.get("results", []):
            last_edited = datetime.fromisoformat(page["last_edited_time"].replace("Z", "+00:00"))
            if last_edited < cutoff:
                continue
            await _process_page(client, page, tenant_id)
            page_count += 1
            await asyncio.sleep(_SLEEP)
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    log.info("Notion backfill complete: %d pages processed", page_count)


async def _process_page(client: NotionClient, page: dict, tenant_id: uuid.UUID) -> None:
    page_id = page["id"]
    log.info("Notion backfill: page %s", page_id)
    text = await extract_page_text(client, page_id)
    await pipeline.run(page_to_event(page, text, tenant_id))

    try:
        comments_data = await client.get_comments(page_id)
        await asyncio.sleep(_SLEEP)
        for comment in comments_data.get("results", []):
            await pipeline.run(comment_to_event(comment, page, tenant_id))
    except Exception:
        log.warning("Notion backfill: could not fetch comments for page %s", page_id)
