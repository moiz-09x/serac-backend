from typing import TypedDict


class ContextItem(TypedDict):
    source_id: str
    type: str
    content: str
    thread_id: str


class ThreadContext(TypedDict):
    thread_id: str
    status: str
    created_at: str
    resolved_at: str | None
    outcome_type: str | None
    events: list[dict]


class RetrievalState(TypedDict):
    question: str
    tenant_id: str
    sub_queries: list[str]
    matched_thread_ids: list[str]
    thread_contexts: list[ThreadContext]
    context_items: list[ContextItem]
    draft_answer: str
    answer: str
