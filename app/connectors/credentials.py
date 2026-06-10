"""CredentialProvider — single access point for integration tokens."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import delete, select, text
from sqlalchemy.dialects.postgresql import insert

from app.connectors.crypto import decrypt, encrypt
from app.db.models import IntegrationCredential


class CredentialProvider:
    """Fetch, store, and delete OAuth credentials for a tenant+integration pair."""

    async def get_token(self, tenant_id: str, integration: str) -> str:
        from app.db.postgres import get_engine

        async with get_engine().connect() as conn:
            row = await conn.execute(
                select(IntegrationCredential).where(
                    IntegrationCredential.tenant_id == tenant_id,
                    IntegrationCredential.integration == integration,
                )
            )
            cred = row.fetchone()

        if cred is None:
            raise LookupError(f"No credentials for {integration} / tenant {tenant_id}")
        return decrypt(cred.access_token)

    async def store(
        self,
        tenant_id: str,
        integration: str,
        access_token: str,
        refresh_token: str | None = None,
        expires_at: datetime | None = None,
        scopes: list[str] | None = None,
        raw: dict | None = None,
    ) -> None:
        from app.db.postgres import get_engine

        now = datetime.now(timezone.utc)
        values = {
            "tenant_id": tenant_id,
            "integration": integration,
            "access_token": encrypt(access_token),
            "refresh_token": encrypt(refresh_token) if refresh_token else None,
            "expires_at": expires_at,
            "scopes": scopes,
            "raw": raw,
            "created_at": now,
            "updated_at": now,
        }
        stmt = (
            insert(IntegrationCredential)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["tenant_id", "integration"],
                set_={
                    "access_token": values["access_token"],
                    "refresh_token": values["refresh_token"],
                    "expires_at": values["expires_at"],
                    "scopes": values["scopes"],
                    "raw": values["raw"],
                    "updated_at": now,
                },
            )
        )
        async with get_engine().begin() as conn:
            await conn.execute(stmt)

    async def delete(self, tenant_id: str, integration: str) -> None:
        from app.db.postgres import get_engine

        async with get_engine().begin() as conn:
            await conn.execute(
                delete(IntegrationCredential).where(
                    IntegrationCredential.tenant_id == tenant_id,
                    IntegrationCredential.integration == integration,
                )
            )

    async def is_connected(self, tenant_id: str, integration: str) -> bool:
        from app.db.postgres import get_engine

        async with get_engine().connect() as conn:
            row = await conn.execute(
                select(text("1")).where(
                    IntegrationCredential.tenant_id == tenant_id,
                    IntegrationCredential.integration == integration,
                )
            )
            return row.fetchone() is not None

    async def get_expiring(self, within_seconds: int = 3600) -> list[IntegrationCredential]:
        """Return credentials expiring within the given window (for token refresh job)."""
        from app.db.postgres import get_engine

        cutoff = datetime.now(timezone.utc).timestamp() + within_seconds
        async with get_engine().connect() as conn:
            rows = await conn.execute(
                select(IntegrationCredential).where(
                    IntegrationCredential.expires_at.isnot(None),
                    IntegrationCredential.refresh_token.isnot(None),
                    text(f"EXTRACT(EPOCH FROM expires_at) < {cutoff}"),
                )
            )
            return list(rows.fetchall())


credentials = CredentialProvider()
