from slack_sdk.web.async_client import AsyncWebClient

from app.core.config import settings


def get_web_client() -> AsyncWebClient:
    return AsyncWebClient(token=settings.slack_bot_token)
