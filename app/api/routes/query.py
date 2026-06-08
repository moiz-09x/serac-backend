import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.config import settings
from app.core.observability import get_langfuse
from app.retrieval import build_graph

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
            },
            config=config,
        )

        if lf:
            lf.flush()

        return QueryResponse(question=request.question, answer=result["answer"])
    except Exception as e:
        logger.exception("Retrieval pipeline failed")
        raise HTTPException(status_code=500, detail=str(e))
