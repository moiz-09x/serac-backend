import asyncio
import json
import re

from app.core.config import settings
from app.core.llm import decomposition_llm, synthesis_llm
from app.db import get_driver
from app.extraction.embeddings import embed
from app.retrieval.state import ContextItem, RetrievalState, ThreadContext


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
    try:
        sub_queries = json.loads(response.content)
    except Exception:
        sub_queries = [state["question"]]
    return {"sub_queries": sub_queries}


async def search(state: RetrievalState) -> dict:
    """Vector search — returns the IDs of the most relevant threads."""
    matched: dict[str, float] = {}  # thread_id → best score

    async with get_driver().session(database=settings.neo4j_database) as session:
        for query_text in state["sub_queries"]:
            vector = await asyncio.get_event_loop().run_in_executor(None, embed, query_text)
            result = await session.run(
                """
                CALL db.index.vector.queryNodes('event_embeddings', 10, $vec)
                YIELD node AS evt, score
                WHERE evt.tenant_id = $tid AND score >= $min_score
                MATCH (evt)-[:PART_OF]->(t:DecisionThread {tenant_id: $tid})
                WITH t.id AS thread_id, max(score) AS best_score
                ORDER BY best_score DESC
                LIMIT 5
                RETURN thread_id, best_score
                """,
                vec=vector,
                tid=state["tenant_id"],
                min_score=settings.retrieval_min_score,
            )
            async for row in result:
                tid = row["thread_id"]
                if tid and (tid not in matched or row["best_score"] > matched[tid]):
                    matched[tid] = row["best_score"]

    # Return top 5 threads by score across all sub-queries
    top = sorted(matched.items(), key=lambda x: x[1], reverse=True)[:5]
    return {
        "matched_thread_ids": [tid for tid, _ in top],
        "thread_scores": {tid: round(score, 4) for tid, score in top},
    }


async def expand(state: RetrievalState) -> dict:
    """Graph traversal — for each matched thread, pull the full context:
    all events, all actors, thread metadata, and outcome if it exists.
    """
    thread_contexts: list[ThreadContext] = []
    context_items: list[ContextItem] = []
    seen_event_ids: set[str] = set()

    if not state.get("matched_thread_ids"):
        return {"thread_contexts": [], "context_items": []}

    async with get_driver().session(database=settings.neo4j_database) as session:
        result = await session.run(
            """
            UNWIND $thread_ids AS tid
            MATCH (t:DecisionThread {id: tid, tenant_id: $tenant})
            OPTIONAL MATCH (e:Event)-[:PART_OF]->(t)
            OPTIONAL MATCH (a:Actor)-[:EXECUTED]->(e)
            OPTIONAL MATCH (t)-[:RESULTED_IN]->(o:Outcome)
            WITH t, o,
                 collect(distinct {
                     id: e.id,
                     type: e.event_type,
                     ts: e.timestamp,
                     text: e.text_content,
                     fields: e.delta_fields,
                     actor: a.name
                 }) AS events
            ORDER BY t.created_at ASC
            RETURN t.id          AS thread_id,
                   t.status      AS status,
                   t.created_at  AS created_at,
                   t.resolved_at AS resolved_at,
                   o.type        AS outcome_type,
                   events
            """,
            thread_ids=state["matched_thread_ids"],
            tenant=state["tenant_id"],
        )

        async for row in result:
            thread_id = row["thread_id"]
            events = [e for e in (row["events"] or []) if e.get("id")]

            thread_contexts.append(ThreadContext(
                thread_id=thread_id,
                status=row["status"] or "Unknown",
                created_at=row["created_at"] or "",
                resolved_at=row["resolved_at"],
                outcome_type=row["outcome_type"],
                events=events,
            ))

            for ev in events:
                if ev["id"] in seen_event_ids:
                    continue
                seen_event_ids.add(ev["id"])

                parts = [f"{ev.get('actor', 'Unknown')} — {ev.get('type', '')} at {ev.get('ts', '')}"]
                if ev.get("text"):
                    parts.append(ev["text"])
                if ev.get("fields"):
                    parts.append(", ".join(ev["fields"]))

                context_items.append(ContextItem(
                    source_id=f"event:{ev['id']}",
                    type="event",
                    content=" | ".join(parts),
                    thread_id=thread_id,
                ))

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
        if tc["outcome_type"]:
            header_parts.append(f"Outcome: {tc['outcome_type']}")
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
        "1. Every factual claim MUST be immediately followed by its citation in brackets, "
        "e.g. [event:abc123]. No exceptions.\n"
        "2. Events within the same Thread block belong to the same decision or issue.\n"
        "3. The thread header tells you its status, when it started, when it resolved, and its outcome.\n"
        "4. Do not use any knowledge outside the context below.\n"
        "5. If the context does not contain enough information, say so explicitly.\n\n"
        f"Context:\n{context_block}\n\n"
        f"Question: {state['question']}"
    )
    response = await synthesis_llm().ainvoke(prompt)
    return {"draft_answer": response.content}


async def verify(state: RetrievalState) -> dict:
    valid_ids = {item["source_id"] for item in state["context_items"]}
    cited = set(re.findall(r"\[([^\]]+:[^\]]+)\]", state["draft_answer"]))
    invalid = cited - valid_ids

    answer = state["draft_answer"]
    for bad_id in invalid:
        answer = re.sub(rf"\s*\[{re.escape(bad_id)}\]", "", answer)

    return {"answer": answer.strip()}
