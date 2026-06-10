import json
import logging
import time
from datetime import UTC, datetime

from app.core.config import settings
from app.db import get_driver

log = logging.getLogger(__name__)

_JUDGE_PROMPT = """\
You are a thread-relationship judge. Decide whether two threads from different \
platforms are about the same subject, the same work, or the same decision.

THREAD A:
Platform: {platform_a}
Title: {title_a}
Recent events:
{events_a}

THREAD B:
Platform: {platform_b}
Title: {title_b}
Recent events:
{events_b}

Rules:
- Return true only if these threads are clearly about the same subject, deal, \
feature, or decision — not just sharing common vocabulary.
- Consider actors, timeline, and content together.
- Respond with valid JSON only.

Response format:
{{"related": true/false, "reason": "<one sentence>"}}
"""


async def link_related_threads(ctx: dict, thread_id: str, tenant_id: str) -> None:
    """Background job: find threads semantically related to this one and create RELATES_TO edges."""
    async with get_driver().session(database=settings.neo4j_database) as session:
        # Read thread embedding
        result = await session.run(
            """
            MATCH (t:Thread {id: $tid, tenant_id: $tenant})
            RETURN t.embedding AS emb, t.source_platform AS platform, t.title AS title
            """,
            tid=thread_id,
            tenant=tenant_id,
        )
        record = await result.single()
        if not record or not record["emb"]:
            log.debug("thread_linker: no embedding for thread %s — skipping", thread_id)
            return

        thread_emb = record["emb"]
        thread_platform = record["platform"]
        thread_title = record["title"]

        # Vector search against all other active threads for this tenant
        candidates_result = await session.run(
            """
            CALL db.index.vector.queryNodes('thread_embeddings', 10, $vec)
            YIELD node AS t, score
            WHERE t.tenant_id = $tenant
              AND t.id <> $tid
              AND t.status <> $concluded
              AND score >= $lo_threshold
            RETURN t.id AS candidate_id,
                   t.source_platform AS platform,
                   t.title AS title,
                   score
            ORDER BY score DESC
            """,
            vec=thread_emb,
            tenant=tenant_id,
            tid=thread_id,
            concluded=settings.thread_status_concluded,
            lo_threshold=settings.thread_link_lo_threshold,
        )
        candidates = [dict(r) async for r in candidates_result]

    if not candidates:
        return

    for candidate in candidates:
        score = candidate["score"]
        candidate_id = candidate["candidate_id"]

        if score >= settings.thread_link_hi_threshold:
            # Clear hit — trust cosine directly
            await _write_relates_to(
                thread_id, candidate_id, tenant_id, confidence=score, source="semantic"
            )
            log.info(
                "thread_linker: direct link %s → %s (score=%.3f)",
                thread_id,
                candidate_id,
                score,
            )

        elif score >= settings.thread_link_lo_threshold:
            # Ambiguous zone — escalate to LLM judge
            log.info(
                "thread_linker: ambiguous score=%.3f for %s → %s, calling LLM judge",
                score,
                thread_id,
                candidate_id,
            )
            related, reason = await _thread_relation_judge(
                thread_id=thread_id,
                thread_platform=thread_platform,
                thread_title=thread_title,
                candidate_id=candidate_id,
                candidate_platform=candidate["platform"],
                candidate_title=candidate["title"],
                tenant_id=tenant_id,
            )
            if related:
                await _write_relates_to(
                    thread_id, candidate_id, tenant_id, confidence=score, source="semantic"
                )
                log.info(
                    "thread_linker: LLM confirmed link %s → %s (%s)",
                    thread_id,
                    candidate_id,
                    reason,
                )
            else:
                log.info(
                    "thread_linker: LLM rejected link %s → %s (%s)",
                    thread_id,
                    candidate_id,
                    reason,
                )


async def _write_relates_to(
    thread_id: str,
    candidate_id: str,
    tenant_id: str,
    confidence: float,
    source: str,
) -> None:
    now = datetime.now(UTC).isoformat()
    async with get_driver().session(database=settings.neo4j_database) as session:
        # MERGE so we don't create duplicates; only update if new confidence is higher
        # Always MERGE with IDs in sorted order so concurrent jobs produce the same
        # directed edge and don't create anti-parallel duplicates.
        a, b = sorted([thread_id, candidate_id])
        await session.run(
            """
            MATCH (a:Thread {id: $aid, tenant_id: $tid})
            MATCH (b:Thread {id: $bid, tenant_id: $tid})
            MERGE (a)-[r:RELATES_TO]->(b)
            ON CREATE SET r.confidence = $conf, r.source = $src, r.linked_at = $now
            ON MATCH SET
                r.confidence = CASE WHEN $conf > r.confidence THEN $conf ELSE r.confidence END,
                r.linked_at = CASE WHEN $conf > r.confidence THEN $now ELSE r.linked_at END
            """,
            aid=a,
            bid=b,
            tid=tenant_id,
            conf=confidence,
            src=source,
            now=now,
        )


async def _fetch_thread_events(thread_id: str, tenant_id: str, limit: int = 8) -> list[dict]:
    async with get_driver().session(database=settings.neo4j_database) as session:
        result = await session.run(
            """
            MATCH (e:Event)-[:PART_OF]->(t:Thread {id: $tid, tenant_id: $tenant})
            OPTIONAL MATCH (a:Actor)-[:EXECUTED]->(e)
            RETURN e.event_type AS etype, e.timestamp AS ts,
                   e.text_content AS text, a.name AS actor
            ORDER BY e.timestamp DESC
            LIMIT $lim
            """,
            tid=thread_id,
            tenant=tenant_id,
            lim=limit,
        )
        rows = []
        async for row in result:
            rows.append(
                {
                    "actor": row["actor"] or "Unknown",
                    "etype": row["etype"] or "",
                    "ts": row["ts"] or "",
                    "text": (row["text"] or "")[:200],
                }
            )
        return list(reversed(rows))


def _format_events(events: list[dict]) -> str:
    if not events:
        return "  (no events)"
    return "\n".join(f"  [{e['actor']}] {e['etype']} at {e['ts']}: {e['text']}" for e in events)


async def _thread_relation_judge(
    thread_id: str,
    thread_platform: str,
    thread_title: str | None,
    candidate_id: str,
    candidate_platform: str,
    candidate_title: str | None,
    tenant_id: str,
) -> tuple[bool, str]:
    from app.core.llm import thread_relation_llm

    events_a = await _fetch_thread_events(thread_id, tenant_id)
    events_b = await _fetch_thread_events(candidate_id, tenant_id)

    prompt = _JUDGE_PROMPT.format(
        platform_a=thread_platform or "Unknown",
        title_a=thread_title or "(no title)",
        events_a=_format_events(events_a),
        platform_b=candidate_platform or "Unknown",
        title_b=candidate_title or "(no title)",
        events_b=_format_events(events_b),
    )

    llm = thread_relation_llm()
    t0 = time.monotonic()
    raw = ""
    related = False
    reason = "parse_error"
    try:
        response = await llm.ainvoke(prompt)
        raw = response.content.strip()
        parsed = json.loads(raw)
        related = bool(parsed.get("related", False))
        reason = parsed.get("reason", "")
    except Exception as e:
        log.warning("thread_relation_judge: parse error (%s) raw: %s", e, raw[:200])

    latency_ms = int((time.monotonic() - t0) * 1000)
    log.info(
        "thread_relation_judge: latency=%dms related=%s reason=%r",
        latency_ms,
        related,
        reason,
    )
    _trace_relation_judge(
        thread_id=thread_id,
        thread_platform=thread_platform,
        candidate_id=candidate_id,
        candidate_platform=candidate_platform,
        prompt=prompt,
        raw_response=raw,
        related=related,
        reason=reason,
        latency_ms=latency_ms,
    )
    return related, reason


def _trace_relation_judge(
    thread_id: str,
    thread_platform: str,
    candidate_id: str,
    candidate_platform: str,
    prompt: str,
    raw_response: str,
    related: bool,
    reason: str,
    latency_ms: int,
) -> None:
    try:
        from app.core.observability import get_langfuse

        lf = get_langfuse()
        if not lf:
            return
        trace = lf.trace(
            name="thread-relation-judge",
            metadata={
                "thread_id": thread_id,
                "thread_platform": thread_platform,
                "candidate_id": candidate_id,
                "candidate_platform": candidate_platform,
                "related": related,
                "reason": reason,
                "latency_ms": latency_ms,
            },
        )
        trace.generation(
            name="thread-relation-llm",
            model=settings.thread_relation_model,
            input=prompt,
            output=raw_response,
            metadata={"latency_ms": latency_ms},
        )
        lf.flush()
    except Exception as e:
        log.debug("thread_relation_judge: Langfuse trace failed: %s", e)
