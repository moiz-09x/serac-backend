import httpx

from app.connectors.credentials import credentials
from app.connectors.state import sign_state, verify_state
from app.core.config import settings

_AUTH_URL = "https://linear.app/oauth/authorize"
_TOKEN_URL = "https://api.linear.app/oauth/token"

# Read-only scope for v1 — add "issues:create" when write-back is needed
_SCOPES = "read"


def authorize_url(tenant_id: str) -> str:
    from urllib.parse import urlencode

    params = {
        "client_id": settings.linear_client_id,
        "redirect_uri": f"{settings.base_url}/integrations/linear/callback",
        "response_type": "code",
        "scope": _SCOPES,
        "state": sign_state(tenant_id),
        "prompt": "consent",
    }
    return f"{_AUTH_URL}?{urlencode(params)}"


async def handle_callback(code: str, state: str) -> str:
    """Exchange code for token, persist it, return tenant_id."""
    tenant_id = verify_state(state)

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            _TOKEN_URL,
            data={
                "client_id": settings.linear_client_id,
                "client_secret": settings.linear_client_secret,
                "redirect_uri": f"{settings.base_url}/integrations/linear/callback",
                "code": code,
                "grant_type": "authorization_code",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp.raise_for_status()
        data = resp.json()

    await credentials.store(
        tenant_id=tenant_id,
        integration="linear",
        access_token=data["access_token"],
        raw=data,
    )
    return tenant_id
