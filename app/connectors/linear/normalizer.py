import uuid
from datetime import datetime, timezone

from app.schemas import (
    AccessScopeItem,
    ActorSignature,
    CanonicalEvent,
    DeltaPayload,
    EventMetadata,
    FieldMutation,
    OutcomeType,
    PrincipalType,
    SourcePlatform,
    Visibility,
)

_COMPLETED_STATES = {"done", "completed", "fixed", "resolved"}
_CANCELLED_STATES = {"cancelled", "canceled", "duplicate", "won't fix", "wont fix", "wontfix"}


def _scope(team: dict) -> AccessScopeItem:
    return AccessScopeItem(
        principal_type=PrincipalType.LINEAR_TEAM,
        principal_id=team["id"],
        visibility=Visibility.RESTRICTED if team.get("private") else Visibility.PUBLIC,
    )


def _ts(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def issue_to_event(issue: dict, tenant_id: uuid.UUID) -> CanonicalEvent:
    creator = issue.get("creator") or {}
    text = f"{issue['title']}\n\n{issue.get('description') or ''}".strip()
    return CanonicalEvent(
        metadata=EventMetadata(
            tenant_id=tenant_id,
            source_platform=SourcePlatform.LINEAR,
            native_event_id=issue["id"],
            timestamp=_ts(issue["createdAt"]),
            event_type="IssueCreated",
        ),
        actor_signature=ActorSignature(
            native_user_id=creator.get("id", "unknown"),
            email_hint=creator.get("email"),
        ),
        delta_payload=DeltaPayload(text_content=text),
        access_scope=[_scope(issue["team"])],
    )


def comment_to_event(comment: dict, issue: dict, tenant_id: uuid.UUID) -> CanonicalEvent:
    user = comment.get("user") or {}
    return CanonicalEvent(
        metadata=EventMetadata(
            tenant_id=tenant_id,
            source_platform=SourcePlatform.LINEAR,
            native_event_id=comment["id"],
            parent_native_id=issue["id"],
            timestamp=_ts(comment["createdAt"]),
            event_type="Comment",
        ),
        actor_signature=ActorSignature(
            native_user_id=user.get("id", "unknown"),
            email_hint=user.get("email"),
        ),
        delta_payload=DeltaPayload(text_content=comment["body"]),
        access_scope=[_scope(issue["team"])],
    )


def state_change_to_event(history: dict, issue: dict, tenant_id: uuid.UUID) -> CanonicalEvent | None:
    if not history.get("fromState") or not history.get("toState"):
        return None
    actor = history.get("actor") or {}
    to_state = history["toState"]["name"].lower()

    if to_state in _COMPLETED_STATES:
        outcome_signal = OutcomeType.COMPLETED
    elif to_state in _CANCELLED_STATES:
        outcome_signal = OutcomeType.CANCELLED
    else:
        outcome_signal = None

    return CanonicalEvent(
        metadata=EventMetadata(
            tenant_id=tenant_id,
            source_platform=SourcePlatform.LINEAR,
            native_event_id=history["id"],
            parent_native_id=issue["id"],
            timestamp=_ts(history["createdAt"]),
            event_type="StateChange",
        ),
        actor_signature=ActorSignature(
            native_user_id=actor.get("id", "unknown"),
            email_hint=actor.get("email"),
        ),
        delta_payload=DeltaPayload(
            field_mutations=[FieldMutation(
                field="status",
                old=history["fromState"]["name"],
                new=history["toState"]["name"],
            )]
        ),
        access_scope=[_scope(issue["team"])],
        outcome_signal=outcome_signal,
    )
