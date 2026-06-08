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


class DecisionThread(NodeBase):
    topic_summary: str
    status: ThreadStatus = ThreadStatus.ACTIVE
    confidence_score: float = 0.0
    resolved_at: datetime | None = None


class GoverningDoc(NodeBase):
    title: str
    type: GoverningDocType
    status: GoverningDocStatus | None = None
    version: str | None = None
    content_hash: str | None = None  # SHA-256; used for idempotent re-ingestion
    raw_text_uri: str | None = None


class Outcome(NodeBase):
    type: OutcomeType
    financial_impact: float | None = None
    duration_ms: int | None = None
    sentiment_score: float | None = None


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
