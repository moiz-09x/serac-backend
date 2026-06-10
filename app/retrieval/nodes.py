import asyncio
import json
import logging
import re

from app.core.config import settings
from app.core.llm import decomposition_llm, synthesis_llm
from app.db import get_driver
from app.extraction.embeddings import embed
from app.retrieval.state import ContextItem, RetrievalState, ThreadContext, TokenUsage

log = logging.getLogger(__name__)


def _extract_usage(response) -> TokenUsage:
    meta = getattr(response, "usage_metadata", None) or {}
    inp = meta.get("input_tokens", 0)
    out = meta.get("output_tokens", 0)
    return TokenUsage(input_tokens=inp, output_tokens=out, total_tokens=inp + out)


def _add_usage(a: TokenUsage, b: TokenUsage) -> TokenUsage:
    return TokenUsage(
        input_tokens=a["input_tokens"] + b["input_tokens"],
        output_tokens=a["output_tokens"] + b["output_tokens"],
        total_tokens=a["total_tokens"] + b["total_tokens"],
    )


async def decompose(state: RetrievalState) -> dict:
    prompt = (
        "Break the following question into 1-3 short, focused search queries.\n"
        "Rules:\n"
        "- If the question is already specific and focused, return exactly 1 query.\n"
        "- Only generate multiple queries if they are meaningfully different — attacking different aspects, entities, or time periods.\n"
        "- Never generate near-duplicate or rephrased versions of the same query.\n"
        "Return only a JSON array of strings, no explanation.\n\n"
        f"Question: {state['question']}"
    )
    response = await decomposition_llm().ainvoke(prompt)
    usage = _extract_usage(response)
    log.info("decompose tokens: in=%d out=%d", usage["input_tokens"], usage["output_tokens"])
    try:
        sub_queries = json.loads(response.content)
    except Exception:
        sub_queries = [state["question"]]
    return {"sub_queries": sub_queries, "token_usage": usage}


async def search(state: RetrievalState) -> dict:
    """Thread-level vector search — finds the most relevant Thread nodes directly."""
    matched: dict[str, float] = {}  # thread_id → best score

    async with get_driver().session(database=settings.neo4j_database) as session:
        for query_text in state["sub_queries"]:
            vector = await asyncio.get_event_loop().run_in_executor(None, embed, query_text)
            result = await session.run(
                """
                CALL db.index.vector.queryNodes('thread_embeddings', 10, $vec)
                YIELD node AS t, score
                WHERE t.tenant_id = $tid AND score >= $min_score
                RETURN t.id AS thread_id, score
                ORDER BY score DESC
                LIMIT 5
                """,
                vec=vector,
                tid=state["tenant_id"],
                min_score=settings.retrieval_min_score,
            )
            async for row in result:
                tid = row["thread_id"]
                if tid and (tid not in matched or row["score"] > matched[tid]):
                    matched[tid] = row["score"]

    top = sorted(matched.items(), key=lambda x: x[1], reverse=True)[:5]
    return {
        "matched_thread_ids": [tid for tid, _ in top],
        "thread_scores": {tid: round(score, 4) for tid, score in top},
    }


async def expand(state: RetrievalState) -> dict:
    """Graph traversal — for each matched thread, expand via RELATES_TO edges to get
    the full cross-platform cluster, then pull all events from all threads in the cluster.
    """
    thread_contexts: list[ThreadContext] = []
    context_items: list[ContextItem] = []
    seen_event_ids: set[str] = set()

    if not state.get("matched_thread_ids"):
        return {"thread_contexts": [], "context_items": []}

    async with get_driver().session(database=settings.neo4j_database) as session:
        # Expand each seed thread via RELATES_TO up to 3 hops, collect the full cluster
        expand_result = await session.run(
            """
            UNWIND $seed_ids AS seed_id
            MATCH (seed:Thread {id: seed_id, tenant_id: $tenant})
            OPTIONAL MATCH (seed)-[rels:RELATES_TO*1..3]-(related:Thread {tenant_id: $tenant})
            WHERE ALL(r IN rels WHERE r.confidence >= $conf_threshold)
            WITH collect(distinct seed.id) + collect(distinct related.id) AS cluster_ids
            UNWIND cluster_ids AS cid
            RETURN DISTINCT cid
            """,
            seed_ids=state["matched_thread_ids"],
            tenant=state["tenant_id"],
            conf_threshold=0.65,
        )
        all_thread_ids = [row["cid"] async for row in expand_result if row["cid"]]

        if not all_thread_ids:
            all_thread_ids = state["matched_thread_ids"]

        # Pull full context for every thread in the cluster
        result = await session.run(
            """
            UNWIND $thread_ids AS tid
            MATCH (t:Thread {id: tid, tenant_id: $tenant})
            OPTIONAL MATCH (e:Event)-[:PART_OF]->(t)
            OPTIONAL MATCH (a:Actor)-[:EXECUTED]->(e)
            WITH t,
                 collect(distinct {
                     id: e.id,
                     type: e.event_type,
                     ts: e.timestamp,
                     text: e.text_content,
                     fields: e.delta_fields,
                     actor: a.name
                 }) AS events
            ORDER BY t.created_at ASC
            RETURN t.id             AS thread_id,
                   t.status         AS status,
                   t.source_platform AS platform,
                   t.title          AS title,
                   t.created_at     AS created_at,
                   t.resolved_at    AS resolved_at,
                   events
            """,
            thread_ids=all_thread_ids,
            tenant=state["tenant_id"],
        )

        async for row in result:
            thread_id = row["thread_id"]
            events = [e for e in (row["events"] or []) if e.get("id")]

            thread_contexts.append(
                ThreadContext(
                    thread_id=thread_id,
                    platform=row["platform"] or "",
                    status=row["status"] or "Unknown",
                    created_at=row["created_at"] or "",
                    resolved_at=row["resolved_at"],
                    events=events,
                )
            )

            for ev in events:
                if ev["id"] in seen_event_ids:
                    continue
                seen_event_ids.add(ev["id"])

                parts = [
                    f"{ev.get('actor', 'Unknown')} — {ev.get('type', '')} at {ev.get('ts', '')}"
                ]
                if ev.get("text"):
                    parts.append(ev["text"])
                if ev.get("fields"):
                    parts.append(", ".join(ev["fields"]))

                context_items.append(
                    ContextItem(
                        source_id=f"event:{ev['id']}",
                        type="event",
                        content=" | ".join(parts),
                        thread_id=thread_id,
                    )
                )

    return {"thread_contexts": thread_contexts, "context_items": context_items}


async def synthesise(state: RetrievalState) -> dict:
    sections = []

    for tc in state.get("thread_contexts", []):
        header_parts = [f"=== Thread [{tc['thread_id']}]"]
        header_parts.append(f"Status: {tc['status']}")
        if tc["created_at"]:
            header_parts.append(f"Started: {tc['created_at'][:10]}")
        if tc["resolved_at"]:
            header_parts.append(f"Resolved: {tc['resolved_at'][:10]}")
        header_parts.append("===")

        lines = [" | ".join(header_parts)]
        for ev in tc["events"]:
            source_id = f"event:{ev['id']}"
            parts = [f"{ev.get('actor', 'Unknown')} — {ev.get('type', '')} at {ev.get('ts', '')}"]
            if ev.get("text"):
                parts.append(ev["text"])
            if ev.get("fields"):
                parts.append(", ".join(ev["fields"]))
            lines.append(f"  [{source_id}] {' | '.join(parts)}")

        sections.append("\n".join(lines))

    context_block = "\n\n".join(sections)

    prompt = (
        "You are a decision intelligence assistant. Answer the question using ONLY the context below.\n\n"
        "RULES:\n"
        "1. When you make a factual claim, cite the specific event whose text directly supports it. "
        "Copy the EXACT full event ID from the context as shown, e.g. [event:7a085ae7-442d-4a33-a47a-698f2e9ffae9] — never truncate or abbreviate. "
        "Before writing a citation, confirm the supporting text appears in that event. "
        "An uncited claim is always better than a wrong citation — do not guess.\n"
        "2. Events within the same Thread block belong to the same conversation or issue.\n"
        "3. Threads from different platforms may be about the same subject — treat them as one body of work.\n"
        "4. The thread header tells you its status, when it started, and when it resolved.\n"
        "5. Do not use any knowledge outside the context below.\n"
        "6. If the context does not contain enough information, say so explicitly.\n\n"
        f"Context:\n{context_block}\n\n"
        f"Question: {state['question']}"
    )
    response = await synthesis_llm().ainvoke(prompt)
    usage = _extract_usage(response)
    log.info("synthesise tokens: in=%d out=%d", usage["input_tokens"], usage["output_tokens"])
    accumulated = _add_usage(
        state.get("token_usage") or TokenUsage(input_tokens=0, output_tokens=0, total_tokens=0),
        usage,
    )
    log.info(
        "retrieval total tokens: in=%d out=%d total=%d",
        accumulated["input_tokens"],
        accumulated["output_tokens"],
        accumulated["total_tokens"],
    )
    return {"draft_answer": response.content, "token_usage": accumulated}


_VERIFY_STOP_WORDS = {
    "that",
    "this",
    "with",
    "from",
    "have",
    "been",
    "they",
    "their",
    "them",
    "will",
    "would",
    "could",
    "should",
    "also",
    "both",
    "into",
    "than",
    "then",
    "when",
    "which",
    "what",
    "about",
    "being",
    "used",
    "using",
    "were",
    "some",
    "each",
    "only",
    "more",
    "said",
    "were",
    "just",
    "very",
    "there",
}


def _resolve_cite_id(cite_id: str, valid_ids: set[str]) -> str | None:
    """Resolve a citation ID to its canonical form in valid_ids.
    LLMs sometimes truncate UUIDs — try prefix match as fallback.
    """
    if cite_id in valid_ids:
        return cite_id
    matches = [vid for vid in valid_ids if vid.startswith(cite_id)]
    return matches[0] if len(matches) == 1 else None


async def verify(state: RetrievalState) -> dict:
    valid_ids = {item["source_id"] for item in state["context_items"]}
    content_map = {item["source_id"]: item["content"].lower() for item in state["context_items"]}
    cited = set(re.findall(r"\[([^\]]+:[^\]]+)\]", state["draft_answer"]))

    answer = state["draft_answer"]

    # Phase 1: remove citations for IDs that don't resolve to any context event.
    # Expand truncated IDs in place before deciding — LLMs sometimes abbreviate UUIDs.
    resolved: dict[str, str] = {}  # cited_id → canonical valid_id
    for cite_id in cited:
        canonical = _resolve_cite_id(cite_id, valid_ids)
        if canonical:
            resolved[cite_id] = canonical
            if cite_id != canonical:
                answer = answer.replace(f"[{cite_id}]", f"[{canonical}]")
        else:
            answer = re.sub(rf"\s*\[{re.escape(cite_id)}\]", "", answer)

    # Phase 2: remove citations where the surrounding claim shares no keywords
    # with the cited event's content — catches hallucinated IDs that happen to resolve
    for canonical_id in set(resolved.values()):
        event_words = set(re.findall(r"\b[a-z]{4,}\b", content_map[canonical_id]))
        for m in re.finditer(rf"\[{re.escape(canonical_id)}\]", answer):
            window_start = max(0, m.start() - 150)
            window_end = min(len(answer), m.end() + 50)
            window = answer[window_start:window_end]
            claim_text = re.sub(r"\[[^\]]+\]", "", window).lower()
            claim_words = {
                w for w in re.findall(r"\b[a-z]{4,}\b", claim_text) if w not in _VERIFY_STOP_WORDS
            }
            if claim_words and not (claim_words & event_words):
                answer = answer.replace(f"[{canonical_id}]", "")

    return {"answer": answer.strip()}
