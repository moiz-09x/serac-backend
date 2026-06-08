import asyncio
import json
import re

from app.core.config import settings
from app.core.llm import decomposition_llm, synthesis_llm
from app.db import get_driver
from app.extraction.embeddings import embed
from app.retrieval.state import ContextItem, RetrievalState


async def decompose(state: RetrievalState) -> dict:
    prompt = (
        "Break the following question into 2-3 short, focused search queries. "
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
    items: list[ContextItem] = []
    seen_threads: set[str] = set()

    async with get_driver().session(database=settings.neo4j_database) as session:
        for query_text in state["sub_queries"]:
            vector = await asyncio.get_event_loop().run_in_executor(None, embed, query_text)
            result = await session.run(
                """
                CALL db.index.vector.queryNodes('event_embeddings', 10, $vec)
                YIELD node AS matched_event, score
                WHERE matched_event.tenant_id = $tid
                MATCH (matched_event)-[:PART_OF]->(thread:DecisionThread {tenant_id: $tid})
                WITH thread, max(score) AS best_score
                ORDER BY best_score DESC
                LIMIT 5
                OPTIONAL MATCH (e:Event {tenant_id: $tid})-[:PART_OF]->(thread)
                OPTIONAL MATCH (a:Actor {tenant_id: $tid})-[:EXECUTED]->(e)
                RETURN thread.id AS thread_id,
                       collect(distinct {
                           id: e.id, type: e.event_type, ts: e.timestamp,
                           text: e.text_content, fields: e.delta_fields, actor: a.name
                       }) AS events,
                       best_score AS score
                ORDER BY score DESC
                """,
                vec=vector,
                tid=state["tenant_id"],
            )
            async for row in result:
                thread_id = row["thread_id"]
                if not thread_id or thread_id in seen_threads:
                    continue
                seen_threads.add(thread_id)

                for ev in row["events"]:
                    if not ev.get("id"):
                        continue
                    parts = [f"{ev.get('actor', 'Unknown')} — {ev.get('type', '')} at {ev.get('ts', '')}"]
                    if ev.get("text"):
                        parts.append(ev["text"])
                    if ev.get("fields"):
                        parts.append(", ".join(ev["fields"]))
                    items.append(ContextItem(
                        source_id=f"event:{ev['id']}",
                        type="event",
                        content=" | ".join(parts),
                        thread_id=thread_id,
                    ))

    return {"context_items": items}


async def assemble(state: RetrievalState) -> dict:
    seen: set[str] = set()
    unique: list[ContextItem] = []
    for item in state["context_items"]:
        if item["source_id"] not in seen:
            seen.add(item["source_id"])
            unique.append(item)
    return {"context_items": unique}


async def synthesise(state: RetrievalState) -> dict:
    # Group events by thread so the LLM understands which events belong together
    threads: dict[str, list[ContextItem]] = {}
    for item in state["context_items"]:
        threads.setdefault(item["thread_id"], []).append(item)

    sections = []
    for thread_id, items in threads.items():
        lines = [f"=== Thread [{thread_id}] ==="]
        for item in items:
            lines.append(f"  [{item['source_id']}] {item['content']}")
        sections.append("\n".join(lines))
    context_block = "\n\n".join(sections)

    prompt = (
        "You are a decision intelligence assistant. Answer the question using ONLY the context below.\n\n"
        "RULES:\n"
        "1. Every factual claim MUST be immediately followed by its citation in brackets, "
        "e.g. [event:abc123]. No exceptions.\n"
        "2. Events within the same Thread block belong to the same decision or issue.\n"
        "3. Do not use any knowledge outside the context below.\n"
        "4. If the context does not contain enough information, say so explicitly.\n\n"
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
