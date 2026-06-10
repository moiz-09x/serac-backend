import httpx

from app.connectors.credentials import credentials
from app.connectors.state import sign_state, verify_state
from app.core.config import settings

_AUTH_URL = "https://slack.com/oauth/v2/authorize"
_TOKEN_URL = "https://slack.com/api/oauth.v2.access"

# Bot scopes required for Serac's Slack connector
_BOT_SCOPES = ",".join(
    [
        "channels:history",
        "channels:read",
        "groups:history",
        "groups:read",
        "users:read",
        "users:read.email",
        "team:read",
    ]
)


def authorize_url(tenant_id: str) -> str:
    from urllib.parse import urlencode

    params = {
        "client_id": settings.slack_client_id,
        "redirect_uri": f"{settings.base_url}/integrations/slack/callback",
        "scope": _BOT_SCOPES,
        "state": sign_state(tenant_id),
    }
    return f"{_AUTH_URL}?{urlencode(params)}"


async def handle_callback(code: str, state: str) -> str:
    """Exchange code for bot token, persist it, return tenant_id."""
    tenant_id = verify_state(state)

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _TOKEN_URL,
            data={
                "client_id": settings.slack_client_id,
                "client_secret": settings.slack_client_secret,
                "redirect_uri": f"{settings.base_url}/integrations/slack/callback",
                "code": code,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        data = resp.json()

    if not data.get("ok"):
        raise RuntimeError(f"Slack OAuth error: {data.get('error')}")

    # Slack returns the bot token under access_token at the top level for v2
    bot_token = data["access_token"]
    # app_token (xapp-) for Socket Mode is obtained separately via app manifest,
    # not through the user OAuth flow — store bot token for now.
    await credentials.store(
        tenant_id=tenant_id,
        integration="slack",
        access_token=bot_token,
        raw=data,
    )
    return tenant_id
