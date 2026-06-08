import logging

from app.core.config import settings

log = logging.getLogger(__name__)

_langfuse = None


def get_langfuse():
    global _langfuse
    if _langfuse is not None:
        return _langfuse
    if not settings.langfuse_public_key or not settings.langfuse_secret_key:
        return None
    try:
        from langfuse import Langfuse
        _langfuse = Langfuse(
            public_key=settings.langfuse_public_key,
            secret_key=settings.langfuse_secret_key,
            base_url=settings.langfuse_host,
        )
        return _langfuse
    except Exception as e:
        log.warning("Langfuse init failed: %s", e)
        return None
