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


def _ts(slack_ts: str) -> datetime:
    return datetime.fromtimestamp(float(slack_ts), tz=timezone.utc)


def _scope(channel_id: str, is_private: bool) -> AccessScopeItem:
    return AccessScopeItem(
        principal_type=PrincipalType.SLACK_CHANNEL,
        principal_id=channel_id,
        visibility=Visibility.RESTRICTED if is_private else Visibility.PUBLIC,
    )


def message_to_event(
    msg: dict,
    channel_id: str,
    is_private: bool,
    tenant_id: uuid.UUID,
) -> CanonicalEvent:
    thread_ts = msg.get("thread_ts")
    is_reply = thread_ts and thread_ts != msg["ts"]

    return CanonicalEvent(
        metadata=EventMetadata(
            tenant_id=tenant_id,
            source_platform=SourcePlatform.SLACK,
            native_event_id=msg["ts"],
            # thread_ts is the parent message — gives us Scenario A for replies
            parent_native_id=thread_ts if is_reply else None,
            timestamp=_ts(msg["ts"]),
            event_type="ThreadReply" if is_reply else "Message",
        ),
        actor_signature=ActorSignature(
            native_user_id=msg.get("user", "unknown"),
        ),
        delta_payload=DeltaPayload(text_content=msg.get("text", "")),
        access_scope=[_scope(channel_id, is_private)],
    )
