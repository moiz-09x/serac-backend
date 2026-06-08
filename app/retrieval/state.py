from typing import TypedDict


class ContextItem(TypedDict):
    source_id: str
    type: str
    content: str
    thread_id: str


class RetrievalState(TypedDict):
    question: str
    tenant_id: str
    sub_queries: list[str]
    context_items: list[ContextItem]
    draft_answer: str
    answer: str
