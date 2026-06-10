import uuid
from datetime import UTC, datetime

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
    return datetime.fromtimestamp(float(slack_ts), tz=UTC)


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
    email_hint: str | None = None,
) -> CanonicalEvent:
    msg_ts = msg["ts"]
    thread_ts = msg.get("thread_ts")
    is_reply = thread_ts and thread_ts != msg_ts

    return CanonicalEvent(
        metadata=EventMetadata(
            tenant_id=tenant_id,
            source_platform=SourcePlatform.SLACK,
            native_event_id=msg_ts,
            platform_thread_id=thread_ts if is_reply else msg_ts,
            timestamp=_ts(msg_ts),
            event_type="ThreadReply" if is_reply else "Message",
        ),
        actor_signature=ActorSignature(
            native_user_id=msg.get("user", "unknown"),
            email_hint=email_hint,
        ),
        delta_payload=DeltaPayload(text_content=msg.get("text", "")),
        access_scope=[_scope(channel_id, is_private)],
    )
