import json
import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core.config import settings
from app.core.observability import get_langfuse
from app.retrieval import build_graph
from app.retrieval.nodes import decompose, expand, search, synthesise, verify
from app.retrieval.state import RetrievalState, TokenUsage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/query", tags=["query"])

_graph = None


def get_graph():
    global _graph
    if _graph is None:
        _graph = build_graph()
    return _graph


class QueryRequest(BaseModel):
    question: str


class QueryResponse(BaseModel):
    answer: str
    question: str


@router.post("", response_model=QueryResponse)
async def query(request: QueryRequest):
    try:
        lf = get_langfuse()
        config = {}
        if lf:
            try:
                from langfuse.langchain import CallbackHandler
                config = {"callbacks": [CallbackHandler()]}
            except Exception as e:
                logger.warning("Langfuse callback handler failed: %s", e)

        result = await get_graph().ainvoke(
            {
                "question": request.question,
                "tenant_id": settings.tenant_id,
                "sub_queries": [],
                "matched_thread_ids": [],
                "thread_scores": {},
                "thread_contexts": [],
                "context_items": [],
                "draft_answer": "",
                "answer": "",
                "token_usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
            },
            config=config,
        )

        if lf:
            lf.flush()

        return QueryResponse(question=request.question, answer=result["answer"])
    except Exception as e:
        logger.exception("Retrieval pipeline failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stream")
async def query_stream(request: QueryRequest):
    async def event_gen():
        state: RetrievalState = {
            "question": request.question,
            "tenant_id": settings.tenant_id,
            "sub_queries": [],
            "matched_thread_ids": [],
            "thread_scores": {},
            "thread_contexts": [],
            "context_items": [],
            "draft_answer": "",
            "answer": "",
            "token_usage": TokenUsage(input_tokens=0, output_tokens=0, total_tokens=0),
        }

        def emit(data: dict) -> str:
            return f"data: {json.dumps(data)}\n\n"

        try:
            yield emit({"stage": "decompose", "status": "running"})
            state.update(await decompose(state))
            yield emit({"stage": "decompose", "status": "done", "sub_queries": state["sub_queries"]})

            yield emit({"stage": "search", "status": "running"})
            state.update(await search(state))
            yield emit({
                "stage": "search",
                "status": "done",
                "matched_thread_ids": state["matched_thread_ids"],
                "thread_scores": state["thread_scores"],
            })

            yield emit({"stage": "expand", "status": "running"})
            state.update(await expand(state))
            yield emit({
                "stage": "expand",
                "status": "done",
                "expanded_thread_ids": [tc["thread_id"] for tc in state["thread_contexts"]],
                "event_ids": [
                    item["source_id"].split(":", 1)[1]
                    for item in state["context_items"]
                    if ":" in item["source_id"]
                ],
                "event_count": len(state["context_items"]),
            })

            yield emit({"stage": "synthesize", "status": "running"})
            state.update(await synthesise(state))
            yield emit({"stage": "synthesize", "status": "done"})

            yield emit({"stage": "verify", "status": "running"})
            state.update(await verify(state))
            yield emit({"stage": "verify", "status": "done"})

            citations: dict[str, dict] = {}
            for tc in state["thread_contexts"]:
                tc_platform = tc.get("platform", "")
                for ev in tc.get("events", []):
                    if not ev.get("id"):
                        continue
                    text = ev.get("text") or ""
                    citations[f"event:{ev['id']}"] = {
                        "actor":      ev.get("actor") or "Unknown",
                        "platform":   tc_platform,
                        "event_type": ev.get("type") or "",
                        "timestamp":  str(ev.get("ts") or "")[:10],
                        "text":       text[:300].strip(),
                    }

            yield emit({
                "stage": "result",
                "status": "done",
                "answer": state["answer"],
                "citations": citations,
            })

        except Exception as exc:
            logger.exception("stream query failed")
            yield emit({"stage": "error", "status": "error", "error": str(exc)})

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
