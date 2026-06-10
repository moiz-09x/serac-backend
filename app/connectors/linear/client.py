import httpx

_URL = "https://api.linear.app/graphql"


class LinearClient:
    def __init__(self, access_token: str) -> None:
        self._http = httpx.AsyncClient(
            headers={"Authorization": access_token, "Content-Type": "application/json"},
            timeout=30.0,
        )

    async def query(self, q: str, variables: dict | None = None) -> dict:
        resp = await self._http.post(_URL, json={"query": q, "variables": variables or {}})
        if not resp.is_success:
            raise RuntimeError(f"Linear API {resp.status_code}: {resp.text}")
        data = resp.json()
        if "errors" in data:
            raise RuntimeError(f"Linear API: {data['errors']}")
        return data["data"]

    async def close(self) -> None:
        await self._http.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()
