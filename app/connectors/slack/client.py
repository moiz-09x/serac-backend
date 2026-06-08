from slack_sdk.web.async_client import AsyncWebClient

from app.core.config import settings

_email_cache: dict[str, str | None] = {}


def get_web_client() -> AsyncWebClient:
    return AsyncWebClient(token=settings.slack_bot_token)


async def get_user_email(client: AsyncWebClient, user_id: str) -> str | None:
    """Fetch a Slack user's email, caching per process to avoid per-message API calls."""
    if user_id in _email_cache:
        return _email_cache[user_id]
    try:
        resp = await client.users_info(user=user_id)
        email = resp["user"].get("profile", {}).get("email")
    except Exception:
        email = None
    _email_cache[user_id] = email
    return email
