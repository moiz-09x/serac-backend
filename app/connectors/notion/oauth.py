import base64

import httpx

from app.connectors.credentials import credentials
from app.connectors.state import sign_state, verify_state
from app.core.config import settings

_AUTH_URL = "https://api.notion.com/v1/oauth/authorize"
_TOKEN_URL = "https://api.notion.com/v1/oauth/token"


def authorize_url(tenant_id: str) -> str:
    from urllib.parse import urlencode

    params = {
        "client_id": settings.notion_client_id,
        "redirect_uri": f"{settings.base_url}/integrations/notion/callback",
        "response_type": "code",
        "owner": "user",
        "state": sign_state(tenant_id),
    }
    return f"{_AUTH_URL}?{urlencode(params)}"


async def handle_callback(code: str, state: str) -> str:
    """Exchange code for token, persist it, return tenant_id."""
    tenant_id = verify_state(state)

    credentials_str = base64.b64encode(
        f"{settings.notion_client_id}:{settings.notion_client_secret}".encode()
    ).decode()

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _TOKEN_URL,
            json={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": f"{settings.base_url}/integrations/notion/callback",
            },
            headers={
                "Authorization": f"Basic {credentials_str}",
                "Content-Type": "application/json",
            },
        )
        resp.raise_for_status()
        data = resp.json()

    await credentials.store(
        tenant_id=tenant_id,
        integration="notion",
        access_token=data["access_token"],
        raw=data,
    )
    return tenant_id
