from langchain_openai import ChatOpenAI

from app.core.config import settings


def _api_key(provider: str) -> str:
    return {
        "deepseek": settings.deepseek_api_key,
        "openai": settings.openai_api_key,
        "anthropic": settings.anthropic_api_key,
    }.get(provider.lower(), "")


def _base_url(provider: str) -> str | None:
    return {
        "deepseek": settings.deepseek_base_url,
        "openai": settings.openai_base_url,
    }.get(provider.lower())


def build_llm(provider: str, model: str, temperature: float = 0) -> ChatOpenAI:
    return ChatOpenAI(
        model=model,
        api_key=_api_key(provider),
        base_url=_base_url(provider),
        temperature=temperature,
    )


def decomposition_llm() -> ChatOpenAI:
    return build_llm(settings.decomposition_provider, settings.decomposition_model)


def synthesis_llm() -> ChatOpenAI:
    return build_llm(settings.synthesis_provider, settings.synthesis_model)


def thread_relation_llm() -> ChatOpenAI:
    return build_llm(settings.thread_relation_provider, settings.thread_relation_model)


