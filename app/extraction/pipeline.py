import asyncio
import hashlib
import logging
import uuid
from datetime import UTC, datetime

from app.core.config import settings
from app.db import get_driver, get_redis
from app.schemas import (
    ActorStatus,
    CanonicalEvent,
    GroupKind,
    PrincipalType,
    SemanticEnrichment,
    SourcePlatform,
)
from app.schemas.enums import ThreadStatus

log = logging.getLogger(__name__)


async def run(event: CanonicalEvent) -> None:
    if not await _dedup(event):
        log.debug(
            "dedup skip: %s %s", event.metadata.source_platform, event.metadata.native_event_id
        )
        return
    actor_id = await _resolve_actor(event)
    await _write_to_graph(event, actor_id)


# ── Stage 1: deduplication ────────────────────────────────────────────────────


async def _dedup(event: CanonicalEvent) -> bool:
    key = hashlib.sha256(
        f"{event.metadata.source_platform}{event.metadata.native_event_id}{event.metadata.timestamp.isoformat()}".encode()
    ).hexdigest()
    result = await get_redis().set(f"dedup:{key}", "1", nx=True, ex=86400)
    return result is not None


# ── Stage 2: identity resolution ─────────────────────────────────────────────


async def _resolve_actor(event: CanonicalEvent) -> uuid.UUID:
    native_uid = event.actor_signature.native_user_id
    email = event.actor_signature.email_hint
    tenant_id = str(event.metadata.tenant_id)
    platform = event.metadata.source_platform.value.lower()
    id_prop = f"{platform}_uid"

    async with get_driver().session(database=settings.neo4j_database) as s:
        result = await s.run(
            f"MATCH (a:Actor {{tenant_id: $tid, {id_prop}: $uid}}) RETURN a.id AS id",
            tid=tenant_id,
            uid=native_uid,
        )
        record = await result.single()
        if record:
            return uuid.UUID(record["id"])

        if email:
            result = await s.run(
                "MATCH (a:Actor {tenant_id: $tid, name: $email}) RETURN a.id AS id",
                tid=tenant_id,
                email=email,
            )
            record = await result.single()
            if record:
                actor_id = uuid.UUID(record["id"])
                await s.run(
                    f"MATCH (a:Actor {{tenant_id: $tid, id: $aid}}) SET a.{id_prop} = $uid",
                    tid=tenant_id,
                    aid=str(actor_id),
                    uid=native_uid,
                )
                log.info(
                    "actor merge: %s platform uid added to existing actor %s", platform, actor_id
                )
                return actor_id

        actor_id = uuid.uuid4()
        await s.run(
            f"""
            CREATE (a:Actor {{
                id: $id, tenant_id: $tid, name: $name,
                status: $status, {id_prop}: $uid, created_at: $ts
            }})
            """,
            id=str(actor_id),
            tid=tenant_id,
            name=email or native_uid,
            status=ActorStatus.UNVERIFIED.value,
            uid=native_uid,
            ts=datetime.now(UTC).isoformat(),
        )
        return actor_id


# ── Stage 3: graph write ──────────────────────────────────────────────────────


async def _write_to_graph(event: CanonicalEvent, actor_id: uuid.UUID) -> None:
    tenant_id = str(event.metadata.tenant_id)
    async with get_driver().session(database=settings.neo4j_database) as s:
        thread_id = await _ensure_thread(s, event, tenant_id)
        event_id = await _create_event(s, event, tenant_id)
        await _create_edges(s, event, tenant_id, actor_id, event_id, thread_id)

    if event.delta_payload.text_content:
        event_vector = await _store_event_embedding(
            event_id, tenant_id, event.delta_payload.text_content
        )
        if event_vector:
            await _update_thread_embedding(thread_id, tenant_id, event_vector)
            await _enqueue_thread_linking(thread_id, tenant_id)


async def _ensure_thread(session, event: CanonicalEvent, tenant_id: str) -> uuid.UUID:
    """Deterministic MERGE on (tenant_id, source_platform, platform_native_id).
    One external artifact = one Thread node. No similarity involved.
    """
    platform = event.metadata.source_platform.value
    native_thread_id = event.metadata.platform_thread_id
    title = _thread_title(event)

    result = await session.run(
        """
        MERGE (t:Thread {
            tenant_id: $tid,
            source_platform: $platform,
            platform_native_id: $native_id
        })
        ON CREATE SET
            t.id = $new_id,
            t.status = $status,
            t.created_at = $now,
            t.title = $title,
            t.anchor_event_type = $anchor_type,
            t.embedding_event_count = 0
        RETURN t.id AS thread_id
        """,
        tid=tenant_id,
        platform=platform,
        native_id=native_thread_id,
        new_id=str(uuid.uuid4()),
        status=ThreadStatus.ACTIVE.value,
        now=event.metadata.timestamp.isoformat(),
        title=title,
        anchor_type=event.metadata.event_type,
    )
    record = await result.single()
    return uuid.UUID(record["thread_id"])


def _thread_title(event: CanonicalEvent) -> str | None:
    """Best-effort human-readable title for a thread derived from the event."""
    platform = event.metadata.source_platform
    text = event.delta_payload.text_content or ""
    if platform == SourcePlatform.LINEAR and event.metadata.event_type == "IssueCreated":
        return text.split("\n")[0][:120] or None
    if platform == SourcePlatform.NOTION and event.metadata.event_type == "PageCreated":
        return text.split("\n")[0][:120] or None
    if platform == SourcePlatform.SLACK and event.metadata.event_type == "Message":
        return text[:80] or None
    return None


async def _create_event(session, event: CanonicalEvent, tenant_id: str) -> uuid.UUID:
    delta_fields = [f"{m.field}: {m.old} -> {m.new}" for m in event.delta_payload.field_mutations]
    await session.run(
        """
        CREATE (e:Event {
            id: $id, tenant_id: $tid,
            source_platform: $platform, native_id: $native_id,
            timestamp: $ts, event_type: $etype,
            text_content: $text,
            delta_fields: $dfields,
            semantic_enrichment: $enrichment,
            created_at: $now
        })
        """,
        id=str(event.transaction_id),
        tid=tenant_id,
        platform=event.metadata.source_platform.value,
        native_id=event.metadata.native_event_id,
        ts=event.metadata.timestamp.isoformat(),
        etype=event.metadata.event_type,
        text=event.delta_payload.text_content or "",
        dfields=delta_fields,
        enrichment=SemanticEnrichment.PENDING.value,
        now=datetime.now(UTC).isoformat(),
    )
    return event.transaction_id


async def _create_edges(
    session,
    event: CanonicalEvent,
    tenant_id: str,
    actor_id: uuid.UUID,
    event_id: uuid.UUID,
    thread_id: uuid.UUID,
) -> None:
    ts = event.metadata.timestamp.isoformat()

    await session.run(
        """
        MATCH (a:Actor {tenant_id: $tid, id: $aid})
        MATCH (e:Event {tenant_id: $tid, id: $eid})
        CREATE (a)-[:EXECUTED {timestamp: $ts}]->(e)
        """,
        tid=tenant_id,
        aid=str(actor_id),
        eid=str(event_id),
        ts=ts,
    )
    await session.run(
        """
        MATCH (e:Event {tenant_id: $tid, id: $eid})
        MATCH (t:Thread {tenant_id: $tid, id: $thid})
        CREATE (e)-[:PART_OF {timestamp: $ts}]->(t)
        """,
        tid=tenant_id,
        eid=str(event_id),
        thid=str(thread_id),
        ts=ts,
    )

    for scope in event.access_scope:
        await session.run(
            """
            MERGE (g:Group {tenant_id: $tid, native_id: $nid})
            ON CREATE SET g.id = $gid, g.source_platform = $platform,
                          g.kind = $kind, g.created_at = $now
            WITH g
            MATCH (t:Thread {tenant_id: $tid, id: $thid})
            MERGE (t)-[:VISIBLE_TO]->(g)
            """,
            tid=tenant_id,
            nid=scope.principal_id,
            gid=str(uuid.uuid4()),
            platform=event.metadata.source_platform.value,
            kind=_group_kind(scope.principal_type),
            now=datetime.now(UTC).isoformat(),
            thid=str(thread_id),
        )


# ── Embeddings ────────────────────────────────────────────────────────────────


async def _store_event_embedding(
    event_id: uuid.UUID, tenant_id: str, text: str
) -> list[float] | None:
    from app.extraction.embeddings import embed

    try:
        vector = await asyncio.get_event_loop().run_in_executor(None, embed, text)
        async with get_driver().session(database=settings.neo4j_database) as session:
            await session.run(
                "MATCH (e:Event {tenant_id: $tid, id: $eid}) SET e.embedding = $vec",
                tid=tenant_id,
                eid=str(event_id),
                vec=vector,
            )
        return vector
    except Exception as e:
        log.warning("event embedding failed for %s: %s", event_id, e)
        return None


async def _update_thread_embedding(
    thread_id: uuid.UUID, tenant_id: str, new_vector: list[float]
) -> None:
    """Incrementally update thread embedding as running mean of all event embeddings."""
    async with get_driver().session(database=settings.neo4j_database) as session:
        result = await session.run(
            """
            MATCH (t:Thread {tenant_id: $tid, id: $thid})
            RETURN t.embedding AS emb, t.embedding_event_count AS n
            """,
            tid=tenant_id,
            thid=str(thread_id),
        )
        record = await result.single()
        if not record:
            return

        old_emb = record["emb"]
        n = record["n"] or 0

        if old_emb and len(old_emb) == len(new_vector):
            updated = [(old_emb[i] * n + new_vector[i]) / (n + 1) for i in range(len(new_vector))]
        else:
            updated = new_vector

        await session.run(
            """
            MATCH (t:Thread {tenant_id: $tid, id: $thid})
            SET t.embedding = $emb, t.embedding_event_count = $n
            """,
            tid=tenant_id,
            thid=str(thread_id),
            emb=updated,
            n=n + 1,
        )


async def _enqueue_thread_linking(thread_id: uuid.UUID, tenant_id: str) -> None:
    try:
        from app.db import get_arq_pool

        pool = get_arq_pool()
        await pool.enqueue_job("link_related_threads", str(thread_id), tenant_id)
    except Exception as e:
        log.warning("failed to enqueue thread linking for %s: %s", thread_id, e)


def _group_kind(principal_type: PrincipalType) -> str:
    return {
        PrincipalType.LINEAR_TEAM: GroupKind.TEAM.value,
        PrincipalType.SLACK_CHANNEL: GroupKind.CHANNEL.value,
        PrincipalType.NOTION_PAGE: GroupKind.CHANNEL.value,
    }.get(principal_type, GroupKind.ROLE.value)
