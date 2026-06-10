from fastapi import APIRouter, HTTPException
from fastapi.responses import RedirectResponse

from app.connectors.credentials import credentials

router = APIRouter(prefix="/integrations", tags=["integrations"])

_SUPPORTED = {"notion", "linear", "slack"}


@router.get("/{integration}/connect")
async def connect(integration: str, tenant_id: str):
    """Redirect user to the provider's OAuth consent screen."""
    _check(integration)
    url = _authorize_url(integration, tenant_id)
    return RedirectResponse(url)


@router.get("/{integration}/callback")
async def callback(integration: str, code: str, state: str):
    """OAuth callback — exchange code for token then redirect to dashboard."""
    _check(integration)
    try:
        tenant_id = await _handle_callback(integration, code, state)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OAuth exchange failed: {e}")
    return RedirectResponse(f"/dashboard?connected={integration}&tenant_id={tenant_id}")


@router.get("/{integration}/status")
async def status(integration: str, tenant_id: str):
    """Check whether a tenant has connected an integration."""
    _check(integration)
    connected = await credentials.is_connected(tenant_id, integration)
    return {"integration": integration, "tenant_id": tenant_id, "connected": connected}


@router.delete("/{integration}/disconnect")
async def disconnect(integration: str, tenant_id: str):
    """Revoke and delete stored credentials for an integration."""
    _check(integration)
    await credentials.delete(tenant_id, integration)
    return {"integration": integration, "tenant_id": tenant_id, "disconnected": True}


# ── helpers ───────────────────────────────────────────────────────────────────


def _check(integration: str) -> None:
    if integration not in _SUPPORTED:
        raise HTTPException(status_code=404, detail=f"Unknown integration: {integration}")


def _authorize_url(integration: str, tenant_id: str) -> str:
    if integration == "notion":
        from app.connectors.notion.oauth import authorize_url

        return authorize_url(tenant_id)
    if integration == "linear":
        from app.connectors.linear.oauth import authorize_url

        return authorize_url(tenant_id)
    from app.connectors.slack.oauth import authorize_url

    return authorize_url(tenant_id)


async def _handle_callback(integration: str, code: str, state: str) -> str:
    if integration == "notion":
        from app.connectors.notion.oauth import handle_callback

        return await handle_callback(code, state)
    if integration == "linear":
        from app.connectors.linear.oauth import handle_callback

        return await handle_callback(code, state)
    from app.connectors.slack.oauth import handle_callback

    return await handle_callback(code, state)
