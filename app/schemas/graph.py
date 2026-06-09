import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from .enums import (
    ActorStatus,
    GoverningDocStatus,
    GoverningDocType,
    GroupKind,
    OutcomeType,
    SemanticEnrichment,
    SourcePlatform,
    ThreadStatus,
)



def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


class NodeBase(BaseModel):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    tenant_id: uuid.UUID
    created_at: datetime = Field(default_factory=_now_utc)


class Actor(NodeBase):
    name: str
    role_title: str | None = None
    department: str | None = None
    status: ActorStatus = ActorStatus.UNVERIFIED
    resolved_identities: dict[str, str] = Field(default_factory=dict)


class Group(NodeBase):
    source_platform: SourcePlatform
    native_id: str
    kind: GroupKind
    name: str | None = None


class Event(NodeBase):
    source_platform: SourcePlatform
    native_id: str
    timestamp: datetime
    event_type: str
    delta_fields: list[str] = Field(default_factory=list)
    vector_ref_id: uuid.UUID | None = None
    log_ref_id: uuid.UUID | None = None  # nullable in v1; mandatory in production
    semantic_enrichment: SemanticEnrichment = SemanticEnrichment.PENDING


class Thread(NodeBase):
    status: ThreadStatus = ThreadStatus.ACTIVE
    resolved_at: datetime | None = None
    source_platform: SourcePlatform | None = None
    platform_native_id: str | None = None
    title: str | None = None
    anchor_event_type: str | None = None


class GoverningDoc(NodeBase):
    title: str
    type: GoverningDocType
    status: GoverningDocStatus | None = None
    version: str | None = None
    content_hash: str | None = None  # SHA-256; used for idempotent re-ingestion
    raw_text_uri: str | None = None


class Outcome(NodeBase):
    type: OutcomeType
    duration_ms: int | None = None
    summary: str | None = None
    contributing_thread_count: int = 0
    trigger_platform: SourcePlatform | None = None
    trigger_native_id: str | None = None


class Client(NodeBase):
    name: str
    industry: str | None = None
    native_system_ref: str | None = None


class Deal(NodeBase):
    name: str
    native_system_ref: str | None = None


class Repo(NodeBase):
    name: str
    native_system_ref: str | None = None
