import asyncio
import hashlib
import uuid
from datetime import datetime, timezone

from app.core.config import settings
from app.db import get_driver, get_redis
from app.schemas import (
    ActorStatus,
    CanonicalEvent,
    GroupKind,
    PrincipalType,
    SemanticEnrichment,
)


async def run(event: CanonicalEvent) -> None:
    if not await _dedup(event):
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


# ── Stage 3: identity resolution ──────────────────────────────────────────────

async def _resolve_actor(event: CanonicalEvent) -> uuid.UUID:
    native_uid = event.actor_signature.native_user_id
    tenant_id = str(event.metadata.tenant_id)
    platform = event.metadata.source_platform.value.lower()
    id_prop = f"{platform}_uid"

    async with get_driver().session(database=settings.neo4j_database) as s:
        result = await s.run(
            f"MATCH (a:Actor {{tenant_id: $tid, {id_prop}: $uid}}) RETURN a.id AS id",
            tid=tenant_id, uid=native_uid,
        )
        record = await result.single()
        if record:
            return uuid.UUID(record["id"])

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
            name=event.actor_signature.email_hint or native_uid,
            status=ActorStatus.UNVERIFIED.value,
            uid=native_uid,
            ts=datetime.now(timezone.utc).isoformat(),
        )
        return actor_id


# ── Stage 4: graph write + embedding ─────────────────────────────────────────

async def _write_to_graph(event: CanonicalEvent, actor_id: uuid.UUID) -> None:
    tenant_id = str(event.metadata.tenant_id)
    async with get_driver().session(database=settings.neo4j_database) as s:
        thread_id, is_new_thread = await _ensure_thread(s, event, tenant_id)
        event_id = await _create_event(s, event, tenant_id)
        await _create_edges(s, event, tenant_id, actor_id, event_id, thread_id)

    if is_new_thread and event.delta_payload.text_content:
        await _store_embedding(thread_id, tenant_id, event.delta_payload.text_content)


async def _store_embedding(thread_id: uuid.UUID, tenant_id: str, text: str) -> None:
    from app.extraction.embeddings import embed, store_thread_embedding
    # Run the CPU-bound embed call in a thread pool so it doesn't block the event loop
    vector = await asyncio.get_event_loop().run_in_executor(None, embed, text)
    await store_thread_embedding(thread_id, tenant_id, vector)


async def _ensure_thread(
    session, event: CanonicalEvent, tenant_id: str
) -> tuple[uuid.UUID, bool]:
    """Returns (thread_id, is_new_thread).

    Scenario A — explicit parent pointer (Linear comments, Slack thread replies):
        look up by native_issue_id / parent message native_id.

    Scenario B — no parent pointer (standalone Slack messages):
        embed text → similarity search → attach or create new thread.
    """
    text = event.delta_payload.text_content

    # Scenario A: has an explicit parent
    if event.metadata.parent_native_id:
        result = await session.run(
            "MATCH (t:DecisionThread {tenant_id: $tid, native_issue_id: $iid}) RETURN t.id AS id",
            tid=tenant_id, iid=event.metadata.parent_native_id,
        )
        record = await result.single()
        if record:
            return uuid.UUID(record["id"]), False
        # Parent not in graph yet — fall through and create a new thread
        native_anchor = event.metadata.parent_native_id

    # IssueCreated: the issue itself is always the thread anchor
    elif event.metadata.event_type == "IssueCreated":
        native_anchor = event.metadata.native_event_id
        result = await session.run(
            "MATCH (t:DecisionThread {tenant_id: $tid, native_issue_id: $iid}) RETURN t.id AS id",
            tid=tenant_id, iid=native_anchor,
        )
        record = await result.single()
        if record:
            return uuid.UUID(record["id"]), False

    # Scenario B: standalone message — use semantic similarity
    else:
        native_anchor = event.metadata.native_event_id
        if text:
            from app.extraction.embeddings import embed, find_similar_thread
            vector = await asyncio.get_event_loop().run_in_executor(None, embed, text)
            existing = await find_similar_thread(tenant_id, vector)
            if existing:
                return existing, False

    # Create a new thread
    thread_id = uuid.uuid4()
    summary = (text or "")[:200] or event.metadata.event_type
    await session.run(
        """
        CREATE (t:DecisionThread {
            id: $id, tenant_id: $tid, native_issue_id: $anchor,
            topic_summary: $summary, status: 'Active',
            confidence_score: 1.0, created_at: $ts
        })
        """,
        id=str(thread_id), tid=tenant_id, anchor=native_anchor,
        summary=summary, ts=event.metadata.timestamp.isoformat(),
    )
    return thread_id, True


async def _create_event(session, event: CanonicalEvent, tenant_id: str) -> uuid.UUID:
    delta_fields = [
        f"{m.field}: {m.old} -> {m.new}" for m in event.delta_payload.field_mutations
    ]
    await session.run(
        """
        CREATE (e:Event {
            id: $id, tenant_id: $tid,
            source_platform: $platform, native_id: $native_id,
            timestamp: $ts, event_type: $etype,
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
        dfields=delta_fields,
        enrichment=SemanticEnrichment.PENDING.value,
        now=datetime.now(timezone.utc).isoformat(),
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
        tid=tenant_id, aid=str(actor_id), eid=str(event_id), ts=ts,
    )
    await session.run(
        """
        MATCH (e:Event {tenant_id: $tid, id: $eid})
        MATCH (t:DecisionThread {tenant_id: $tid, id: $thid})
        CREATE (e)-[:PART_OF {timestamp: $ts}]->(t)
        """,
        tid=tenant_id, eid=str(event_id), thid=str(thread_id), ts=ts,
    )

    for scope in event.access_scope:
        await session.run(
            """
            MERGE (g:Group {tenant_id: $tid, native_id: $nid})
            ON CREATE SET g.id = $gid, g.source_platform = $platform,
                          g.kind = $kind, g.created_at = $now
            WITH g
            MATCH (e:Event {tenant_id: $tid, id: $eid})
            MERGE (e)-[:VISIBLE_TO]->(g)
            """,
            tid=tenant_id,
            nid=scope.principal_id,
            gid=str(uuid.uuid4()),
            platform=event.metadata.source_platform.value,
            kind=_group_kind(scope.principal_type),
            now=datetime.now(timezone.utc).isoformat(),
            eid=str(event_id),
        )


def _group_kind(principal_type: PrincipalType) -> str:
    return {
        PrincipalType.LINEAR_TEAM: GroupKind.TEAM.value,
        PrincipalType.SLACK_CHANNEL: GroupKind.CHANNEL.value,
    }.get(principal_type, GroupKind.ROLE.value)
