import json
import logging
import time
import uuid

from app.core.config import settings
from app.core.llm import stitch_llm

log = logging.getLogger(__name__)

_PROMPT_TEMPLATE = """\
You are a thread-stitching judge. Decide whether the new event belongs to one of \
the candidate threads below, or should start a brand-new thread.

NEW EVENT:
{event_text}

CANDIDATE THREADS (ordered by vector similarity, highest first):
{candidates_block}

Rules:
- Attach to a thread only if the new event is clearly part of the same ongoing \
discussion, issue, or decision.
- Consider actors, timeline, and content — not just topic similarity.
- Return null for thread_id if the event starts a new topic, even if scores are \
above the threshold.
- Respond with valid JSON only — no explanation outside the JSON.

Response format:
{{"thread_id": "<thread_id or null>", "reason": "<one sentence>"}}
"""


async def _expand_candidate_thread(session, thread_id: str, tenant_id: str) -> list[dict]:
    """Fetch the most recent N events for a candidate thread."""
    result = await session.run(
        """
        MATCH (e:Event)-[:PART_OF]->(t:Thread {id: $tid, tenant_id: $tenant})
        OPTIONAL MATCH (a:Actor)-[:EXECUTED]->(e)
        RETURN e.event_type  AS etype,
               e.timestamp   AS ts,
               e.text_content AS text,
               a.name         AS actor
        ORDER BY e.timestamp DESC
        LIMIT $lim
        """,
        tid=thread_id,
        tenant=tenant_id,
        lim=settings.stitch_expand_limit,
    )
    rows = []
    async for row in result:
        rows.append({
            "actor": row["actor"] or "Unknown",
            "etype": row["etype"] or "",
            "ts": row["ts"] or "",
            "text": row["text"] or "",
        })
    return list(reversed(rows))  # chronological order for the prompt


def _format_candidates_block(candidates: list[dict]) -> str:
    sections = []
    for i, c in enumerate(candidates, 1):
        header = (
            f"{i}. Thread {c['thread_id']} "
            f"(total_score={c['total_score']:.3f}, best={c['best_score']:.3f}, hits={c['hits']})"
        )
        if c.get("status"):
            header += f" | status={c['status']}"
        if c.get("created_at"):
            header += f" | started={c['created_at'][:10]}"
        if c.get("outcome_type"):
            header += f" | outcome={c['outcome_type']}"

        event_lines = []
        for ev in c.get("events", []):
            line = f"   [{ev['actor']}] {ev['etype']} at {ev['ts']}"
            if ev["text"]:
                line += f": {ev['text'][:200]}"
            event_lines.append(line)

        sections.append(header + "\n" + ("\n".join(event_lines) if event_lines else "   (no events)"))
    return "\n\n".join(sections)


async def llm_stitch_verdict(
    event_text: str,
    candidates: list[dict],
    session,
    tenant_id: str,
    event_platform: str = "",
    event_type: str = "",
) -> tuple[str | None, str]:
    """Call the LLM to decide which candidate thread (if any) to attach to.

    Returns (thread_id | None, reason).
    Traces the decision to Langfuse if configured.
    """
    # Expand each candidate thread with its recent events
    for c in candidates:
        thread_meta = await _fetch_thread_meta(session, c["thread_id"], tenant_id)
        c.update(thread_meta)
        c["events"] = await _expand_candidate_thread(session, c["thread_id"], tenant_id)

    candidates_block = _format_candidates_block(candidates)
    prompt = _PROMPT_TEMPLATE.format(
        event_text=event_text,
        candidates_block=candidates_block,
    )

    t0 = time.monotonic()
    raw_response = ""
    try:
        response = await stitch_llm().ainvoke(prompt)
        raw_response = response.content.strip()
        parsed = json.loads(raw_response)
        thread_id = parsed.get("thread_id") or None
        reason = parsed.get("reason", "")
    except Exception as e:
        log.warning("[stitch-judge] LLM parse error (%s) — raw: %s", e, raw_response[:200])
        thread_id = None
        reason = "parse_error"
    latency_ms = int((time.monotonic() - t0) * 1000)

    _trace(
        event_text=event_text,
        candidates=candidates,
        prompt=prompt,
        raw_response=raw_response,
        thread_id=thread_id,
        reason=reason,
        latency_ms=latency_ms,
        event_platform=event_platform,
        event_type=event_type,
    )

    log.info(
        "[stitch-judge] verdict=%s reason=%r latency=%dms platform=%s etype=%s",
        thread_id, reason, latency_ms, event_platform, event_type,
    )
    return thread_id, reason


async def _fetch_thread_meta(session, thread_id: str, tenant_id: str) -> dict:
    result = await session.run(
        """
        MATCH (t:Thread {id: $tid, tenant_id: $tenant})
        OPTIONAL MATCH (t)-[:RESULTED_IN]->(o:Outcome)
        RETURN t.status AS status, t.created_at AS created_at,
               t.resolved_at AS resolved_at, o.type AS outcome_type
        """,
        tid=thread_id,
        tenant=tenant_id,
    )
    record = await result.single()
    if not record:
        return {}
    return {
        "status": record["status"],
        "created_at": record["created_at"],
        "resolved_at": record["resolved_at"],
        "outcome_type": record["outcome_type"],
    }


def _trace(
    event_text: str,
    candidates: list[dict],
    prompt: str,
    raw_response: str,
    thread_id: str | None,
    reason: str,
    latency_ms: int,
    event_platform: str,
    event_type: str,
) -> None:
    try:
        from app.core.observability import get_langfuse
        lf = get_langfuse()
        if not lf:
            return

        trace = lf.trace(
            name="thread-stitch-judge",
            metadata={
                "event_platform": event_platform,
                "event_type": event_type,
                "candidate_count": len(candidates),
                "candidates": [
                    {
                        "thread_id": c["thread_id"],
                        "total_score": round(c.get("total_score", 0), 4),
                        "best_score": round(c.get("best_score", 0), 4),
                        "hits": c.get("hits", 0),
                    }
                    for c in candidates
                ],
                "verdict": thread_id,
                "reason": reason,
                "latency_ms": latency_ms,
            },
        )
        trace.generation(
            name="stitch-judge-llm",
            model=settings.stitch_llm_model,
            input=prompt,
            output=raw_response,
            metadata={"latency_ms": latency_ms},
        )
        lf.flush()
    except Exception as e:
        log.debug("[stitch-judge] Langfuse trace failed: %s", e)
