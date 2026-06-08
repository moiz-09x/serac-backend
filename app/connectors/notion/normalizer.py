import uuid
from datetime import datetime, timezone

from app.schemas import (
    AccessScopeItem,
    ActorSignature,
    CanonicalEvent,
    DeltaPayload,
    EventMetadata,
    PrincipalType,
    SourcePlatform,
    Visibility,
)


def _ts(iso: str) -> datetime:
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _extract_rich_text(rich_text: list[dict]) -> str:
    return "".join(rt.get("plain_text", "") for rt in rich_text)


def _page_title(page: dict) -> str:
    for prop in page.get("properties", {}).values():
        if prop.get("type") == "title":
            return _extract_rich_text(prop.get("title", []))
    return ""


def _scope(page: dict) -> AccessScopeItem:
    is_public = page.get("parent", {}).get("type") == "workspace"
    return AccessScopeItem(
        principal_type=PrincipalType.NOTION_PAGE,
        principal_id=page["id"],
        visibility=Visibility.PUBLIC if is_public else Visibility.RESTRICTED,
    )


def _actor(user: dict) -> ActorSignature:
    return ActorSignature(
        native_user_id=user.get("id", "unknown"),
        email_hint=user.get("person", {}).get("email"),
    )


def page_to_event(page: dict, text_content: str, tenant_id: uuid.UUID) -> CanonicalEvent:
    title = _page_title(page)
    full_text = f"{title}\n\n{text_content}".strip() if text_content else title
    return CanonicalEvent(
        metadata=EventMetadata(
            tenant_id=tenant_id,
            source_platform=SourcePlatform.NOTION,
            native_event_id=page["id"],
            timestamp=_ts(page["created_time"]),
            event_type="PageCreated",
        ),
        actor_signature=_actor(page.get("created_by", {})),
        delta_payload=DeltaPayload(text_content=full_text),
        access_scope=[_scope(page)],
    )


def page_updated_to_event(page: dict, text_content: str, tenant_id: uuid.UUID) -> CanonicalEvent:
    title = _page_title(page)
    full_text = f"{title}\n\n{text_content}".strip() if text_content else title
    return CanonicalEvent(
        metadata=EventMetadata(
            tenant_id=tenant_id,
            source_platform=SourcePlatform.NOTION,
            # composite ID ensures each edit is a distinct, deduplicated event
            native_event_id=f"{page['id']}_{page['last_edited_time']}",
            parent_native_id=page["id"],
            timestamp=_ts(page["last_edited_time"]),
            event_type="PageUpdated",
        ),
        actor_signature=_actor(page.get("last_edited_by", {})),
        delta_payload=DeltaPayload(text_content=full_text),
        access_scope=[_scope(page)],
    )


def comment_to_event(comment: dict, page: dict, tenant_id: uuid.UUID) -> CanonicalEvent:
    text = _extract_rich_text(comment.get("rich_text", []))
    return CanonicalEvent(
        metadata=EventMetadata(
            tenant_id=tenant_id,
            source_platform=SourcePlatform.NOTION,
            native_event_id=comment["id"],
            parent_native_id=page["id"],
            timestamp=_ts(comment["created_time"]),
            event_type="CommentAdded",
        ),
        actor_signature=_actor(comment.get("created_by", {})),
        delta_payload=DeltaPayload(text_content=text),
        access_scope=[_scope(page)],
    )
