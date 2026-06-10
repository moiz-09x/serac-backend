import uuid
from datetime import datetime

from pydantic import BaseModel, Field

from .enums import OutcomeType, PrincipalType, SourcePlatform, Visibility


class AccessScopeItem(BaseModel):
    principal_type: PrincipalType
    principal_id: str
    visibility: Visibility


class ActorSignature(BaseModel):
    native_user_id: str
    email_hint: str | None = None


class FieldMutation(BaseModel):
    field: str
    old: str | None = None
    new: str | None = None


class DeltaPayload(BaseModel):
    text_content: str | None = None
    field_mutations: list[FieldMutation] = Field(default_factory=list)


class EventMetadata(BaseModel):
    tenant_id: uuid.UUID
    source_platform: SourcePlatform
    native_event_id: str
    timestamp: datetime
    event_type: str
    platform_thread_id: str  # native ID of the thread this event belongs to


class CanonicalEvent(BaseModel):
    transaction_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    metadata: EventMetadata
    actor_signature: ActorSignature
    delta_payload: DeltaPayload
    access_scope: list[AccessScopeItem] = Field(default_factory=list)
    outcome_signal: OutcomeType | None = None  # set by normaliser when event is a terminal state
