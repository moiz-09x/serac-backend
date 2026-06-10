import httpx

_BASE = "https://api.notion.com/v1"
_VERSION = "2022-06-28"


class NotionClient:
    def __init__(self, access_token: str) -> None:
        self._http = httpx.AsyncClient(
            base_url=_BASE,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Notion-Version": _VERSION,
                "Content-Type": "application/json",
            },
            timeout=30.0,
        )

    async def search(self, filter_type: str = "page", start_cursor: str | None = None) -> dict:
        body: dict = {
            "filter": {"value": filter_type, "property": "object"},
            "sort": {"direction": "descending", "timestamp": "last_edited_time"},
            "page_size": 100,
        }
        if start_cursor:
            body["start_cursor"] = start_cursor
        resp = await self._http.post("/search", json=body)
        if not resp.is_success:
            raise RuntimeError(f"Notion API {resp.status_code}: {resp.text}")
        return resp.json()

    async def get_page(self, page_id: str) -> dict:
        resp = await self._http.get(f"/pages/{page_id}")
        if not resp.is_success:
            raise RuntimeError(f"Notion API {resp.status_code}: {resp.text}")
        return resp.json()

    async def get_block_children(self, block_id: str, start_cursor: str | None = None) -> dict:
        params: dict = {"page_size": 100}
        if start_cursor:
            params["start_cursor"] = start_cursor
        resp = await self._http.get(f"/blocks/{block_id}/children", params=params)
        if not resp.is_success:
            raise RuntimeError(f"Notion API {resp.status_code}: {resp.text}")
        return resp.json()

    async def get_comments(self, block_id: str) -> dict:
        resp = await self._http.get("/comments", params={"block_id": block_id, "page_size": 100})
        if not resp.is_success:
            raise RuntimeError(f"Notion API {resp.status_code}: {resp.text}")
        return resp.json()

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()
