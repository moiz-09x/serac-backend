from datetime import datetime

from sqlalchemy import ARRAY, DateTime, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class IntegrationCredential(Base):
    __tablename__ = "integration_credentials"

    tenant_id: Mapped[str] = mapped_column(UUID(as_uuid=False), primary_key=True)
    integration: Mapped[str] = mapped_column(String(64), primary_key=True)

    # encrypted blobs — use crypto.encrypt/decrypt
    access_token: Mapped[str] = mapped_column(Text, nullable=False)
    refresh_token: Mapped[str | None] = mapped_column(Text, nullable=True)

    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    scopes: Mapped[list[str] | None] = mapped_column(ARRAY(Text), nullable=True)
    raw: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )
