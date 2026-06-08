import base64
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse, RedirectResponse
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.core.config import settings
from app.db import get_session
from app.db.models import ConnectorToken

router = APIRouter(prefix="/auth", tags=["auth"])

_NOTION_AUTHORIZE_URL = "https://api.notion.com/v1/oauth/authorize"
_NOTION_TOKEN_URL = "https://api.notion.com/v1/oauth/token"


@router.get("/notion")
async def notion_oauth_start(tenant_id: str = Query(default=settings.tenant_id)):
    if not settings.notion_oauth_client_id or not settings.notion_oauth_redirect_uri:
        raise HTTPException(status_code=500, detail="Notion OAuth not configured — set NOTION_OAUTH_CLIENT_ID and NOTION_OAUTH_REDIRECT_URI")
    url = _NOTION_AUTHORIZE_URL + "?" + urlencode({
        "client_id": settings.notion_oauth_client_id,
        "response_type": "code",
        "owner": "user",
        "redirect_uri": settings.notion_oauth_redirect_uri,
        "state": tenant_id,
    })
    return RedirectResponse(url=url)


@router.get("/notion/callback")
async def notion_oauth_callback(
    code: str,
    state: str = Query(default=settings.tenant_id),
):
    token_data = await _exchange_code(code)
    tenant_id = uuid.UUID(state)
    await _upsert_token(tenant_id, token_data)
    return JSONResponse({
        "status": "connected",
        "workspace": token_data.get("workspace_name"),
        "workspace_id": token_data.get("workspace_id"),
    })


async def _exchange_code(code: str) -> dict:
    credentials = base64.b64encode(
        f"{settings.notion_oauth_client_id}:{settings.notion_oauth_client_secret}".encode()
    ).decode()
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            _NOTION_TOKEN_URL,
            headers={
                "Authorization": f"Basic {credentials}",
                "Content-Type": "application/json",
            },
            json={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": settings.notion_oauth_redirect_uri,
            },
        )
    if not resp.is_success:
        raise HTTPException(status_code=400, detail=f"Notion token exchange failed: {resp.text}")
    return resp.json()


async def _upsert_token(tenant_id: uuid.UUID, data: dict) -> None:
    now = datetime.now(timezone.utc)
    async with get_session() as session:
        stmt = insert(ConnectorToken).values(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            platform="notion",
            access_token=data["access_token"],
            workspace_id=data.get("workspace_id"),
            workspace_name=data.get("workspace_name"),
            bot_id=data.get("bot_id"),
            created_at=now,
            updated_at=now,
        ).on_conflict_do_update(
            constraint="uq_connector_token_tenant_platform",
            set_={
                "access_token": data["access_token"],
                "workspace_id": data.get("workspace_id"),
                "workspace_name": data.get("workspace_name"),
                "bot_id": data.get("bot_id"),
                "updated_at": now,
            },
        )
        await session.execute(stmt)
        await session.commit()
