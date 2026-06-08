import asyncio
import hashlib
import logging
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
from app.schemas.enums import OutcomeType, ThreadStatus

log = logging.getLogger(__name__)


async def run(event: CanonicalEvent) -> None:
    if not await _dedup(event):
        log.debug("dedup skip: %s %s", event.metadata.source_platform, event.metadata.native_event_id)
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
    email = event.actor_signature.email_hint
    tenant_id = str(event.metadata.tenant_id)
    platform = event.metadata.source_platform.value.lower()
    id_prop = f"{platform}_uid"

    async with get_driver().session(database=settings.neo4j_database) as s:
        # Fast path: actor already has this platform's UID
        result = await s.run(
            f"MATCH (a:Actor {{tenant_id: $tid, {id_prop}: $uid}}) RETURN a.id AS id",
            tid=tenant_id, uid=native_uid,
        )
        record = await result.single()
        if record:
            return uuid.UUID(record["id"])

        # Cross-platform merge: find existing actor by email, add this platform's UID
        if email:
            result = await s.run(
                "MATCH (a:Actor {tenant_id: $tid, name: $email}) RETURN a.id AS id",
                tid=tenant_id, email=email,
            )
            record = await result.single()
            if record:
                actor_id = uuid.UUID(record["id"])
                await s.run(
                    f"MATCH (a:Actor {{tenant_id: $tid, id: $aid}}) SET a.{id_prop} = $uid",
                    tid=tenant_id, aid=str(actor_id), uid=native_uid,
                )
                log.info("actor merge: %s platform uid added to existing actor %s", platform, actor_id)
                return actor_id

        # No match — create a new actor
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
            ts=datetime.now(timezone.utc).isoformat(),
        )
        return actor_id


# ── Stage 4: graph write + embedding ─────────────────────────────────────────

async def _write_to_graph(event: CanonicalEvent, actor_id: uuid.UUID) -> None:
    tenant_id = str(event.metadata.tenant_id)
    async with get_driver().session(database=settings.neo4j_database) as s:
        thread_id, confidence = await _ensure_thread(s, event, tenant_id)
        event_id = await _create_event(s, event, tenant_id)
        await _create_edges(s, event, tenant_id, actor_id, event_id, thread_id, confidence)

    if event.delta_payload.text_content:
        await _store_event_embedding(event_id, tenant_id, event.delta_payload.text_content)

    if event.outcome_signal:
        await _create_outcome(thread_id, tenant_id, event.outcome_signal)


async def _store_event_embedding(event_id: uuid.UUID, tenant_id: str, text: str) -> None:
    from app.extraction.embeddings import embed
    vector = await asyncio.get_event_loop().run_in_executor(None, embed, text)
    async with get_driver().session(database=settings.neo4j_database) as session:
        await session.run(
            "MATCH (e:Event {tenant_id: $tid, id: $eid}) SET e.embedding = $vec",
            tid=tenant_id, eid=str(event_id), vec=vector,
        )


async def _ensure_thread(
    session, event: CanonicalEvent, tenant_id: str
) -> tuple[uuid.UUID, float]:
    """Returns (thread_id, confidence).

    Scenario A — explicit parent pointer (Linear comments, Slack thread replies):
        find the Event that carries the parent nativeId and follow PART_OF to its thread.
        confidence = 1.0 (deterministic).

    Scenario B — standalone message (no parent pointer):
        vector similarity search across existing event embeddings.
        confidence = cosine score if matched, 1.0 if new thread created.
    """
    text = event.delta_payload.text_content

    platform = event.metadata.source_platform.value
    etype = event.metadata.event_type

    # IssueCreated always anchors its own new thread — never attach to an existing one
    if event.metadata.event_type == "IssueCreated":
        log.info("[thread-stitch] %s %s → new thread (IssueCreated anchor)", platform, etype)

    # Scenario A: has an explicit parent — look up via the parent Event node
    elif event.metadata.parent_native_id:
        result = await session.run(
            """
            MATCH (e:Event {tenant_id: $tid, native_id: $nid})-[:PART_OF]->(t:Thread)
            RETURN t.id AS id
            """,
            tid=tenant_id, nid=event.metadata.parent_native_id,
        )
        record = await result.single()
        if record:
            log.info("[thread-stitch] %s %s → Scenario A hit (parent %s)",
                     platform, etype, event.metadata.parent_native_id)
            return uuid.UUID(record["id"]), 1.0
        log.warning("[thread-stitch] %s %s → Scenario A MISS (parent %s not in graph) → new thread",
                    platform, etype, event.metadata.parent_native_id)

    # Scenario B: standalone message — find related thread via event embedding similarity.
    # Query top-5 nearest events, group by thread, accumulate scores.
    # Then apply three-zone logic + tie-break gate to decide whether to trust the
    # vector result directly or escalate to the LLM stitch judge.
    elif text:
        from app.extraction.embeddings import embed
        vector = await asyncio.get_event_loop().run_in_executor(None, embed, text)
        result = await session.run(
            """
            CALL db.index.vector.queryNodes('event_embeddings', 5, $vec)
            YIELD node AS evt, score
            WHERE evt.tenant_id = $tid AND score >= $threshold
            MATCH (evt)-[:PART_OF]->(thread:Thread {tenant_id: $tid})
            WITH thread.id AS thread_id, sum(score) AS total_score, max(score) AS best_score, count(*) AS hits
            ORDER BY total_score DESC
            RETURN thread_id, total_score, best_score, hits
            """,
            vec=vector, tid=tenant_id, threshold=settings.thread_attach_threshold,
        )
        candidates = [dict(r) async for r in result]

        if candidates:
            top = candidates[0]
            second_best = candidates[1]["best_score"] if len(candidates) > 1 else 0.0
            gap = top["best_score"] - second_best

            # Clear hit AND no close competitor → trust vector directly
            if top["best_score"] >= settings.stitch_ambiguity_hi and gap >= settings.stitch_tie_gap:
                log.info(
                    "[thread-stitch] %s %s → Scenario B clear hit "
                    "(best=%.3f gap=%.3f total=%.3f hits=%d thread=%s)",
                    platform, etype, top["best_score"], gap, top["total_score"],
                    top["hits"], top["thread_id"],
                )
                return uuid.UUID(top["thread_id"]), top["best_score"]

            # Ambiguous zone OR close contest → escalate to LLM judge
            trigger = "ambiguous" if top["best_score"] < settings.stitch_ambiguity_hi else "tie"
            log.info(
                "[thread-stitch] %s %s → Scenario B %s (best=%.3f gap=%.3f) → LLM judge",
                platform, etype, trigger, top["best_score"], gap,
            )
            from app.extraction.stitch_judge import llm_stitch_verdict
            verdict_id, reason = await llm_stitch_verdict(
                event_text=text,
                candidates=candidates[:3],
                session=session,
                tenant_id=tenant_id,
                event_platform=platform,
                event_type=etype,
            )
            if verdict_id:
                log.info("[thread-stitch] %s %s → LLM attached to %s (%s)", platform, etype, verdict_id, reason)
                return uuid.UUID(verdict_id), top["best_score"]
            log.info("[thread-stitch] %s %s → LLM rejected all candidates (%s) → new thread", platform, etype, reason)
        else:
            log.info("[thread-stitch] %s %s → Scenario B miss (no match above %.2f) → new thread",
                     platform, etype, settings.thread_attach_threshold)

    # Create a new thread
    thread_id = uuid.uuid4()
    await session.run(
        """
        CREATE (t:Thread {
            id: $id, tenant_id: $tid,
            status: $status, created_at: $ts
        })
        """,
        id=str(thread_id),
        tid=tenant_id,
        status=ThreadStatus.ACTIVE.value,
        ts=event.metadata.timestamp.isoformat(),
    )
    return thread_id, 1.0


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
    confidence: float,
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
        MATCH (t:Thread {tenant_id: $tid, id: $thid})
        CREATE (e)-[:PART_OF {timestamp: $ts, confidence: $confidence}]->(t)
        """,
        tid=tenant_id, eid=str(event_id), thid=str(thread_id), ts=ts, confidence=confidence,
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


async def _create_outcome(
    thread_id: uuid.UUID, tenant_id: str, outcome_type: OutcomeType
) -> None:
    now = datetime.now(timezone.utc)
    outcome_id = uuid.uuid4()

    async with get_driver().session(database=settings.neo4j_database) as session:
        result = await session.run(
            "MATCH (t:Thread {tenant_id: $tid, id: $thid}) RETURN t.created_at AS created_at",
            tid=tenant_id, thid=str(thread_id),
        )
        record = await result.single()
        if not record:
            return

        thread_created = datetime.fromisoformat(record["created_at"])
        if thread_created.tzinfo is None:
            thread_created = thread_created.replace(tzinfo=timezone.utc)
        duration_ms = int((now - thread_created).total_seconds() * 1000)

        await session.run(
            """
            MATCH (t:Thread {tenant_id: $tid, id: $thid})
            CREATE (o:Outcome {
                id: $oid, tenant_id: $tid,
                type: $type, duration_ms: $duration,
                created_at: $now
            })
            CREATE (t)-[:RESULTED_IN {timestamp: $now}]->(o)
            SET t.status = $concluded, t.resolved_at = $now
            """,
            oid=str(outcome_id),
            tid=tenant_id,
            thid=str(thread_id),
            type=outcome_type.value,
            duration=duration_ms,
            now=now.isoformat(),
            concluded=ThreadStatus.CONCLUDED.value,
        )



def _group_kind(principal_type: PrincipalType) -> str:
    return {
        PrincipalType.LINEAR_TEAM: GroupKind.TEAM.value,
        PrincipalType.SLACK_CHANNEL: GroupKind.CHANNEL.value,
        PrincipalType.NOTION_PAGE: GroupKind.CHANNEL.value,
    }.get(principal_type, GroupKind.ROLE.value)
