from typing import TypedDict


class ContextItem(TypedDict):
    source_id: str
    type: str
    content: str
    thread_id: str


class ThreadContext(TypedDict):
    thread_id: str
    platform: str
    status: str
    created_at: str
    resolved_at: str | None
    events: list[dict]


class TokenUsage(TypedDict):
    input_tokens: int
    output_tokens: int
    total_tokens: int


class RetrievalState(TypedDict):
    question: str
    tenant_id: str
    sub_queries: list[str]
    matched_thread_ids: list[str]
    thread_scores: dict[str, float]  # thread_id → best similarity score
    thread_contexts: list[ThreadContext]
    context_items: list[ContextItem]
    draft_answer: str
    answer: str
    token_usage: TokenUsage
