from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
def health() -> dict[str, str]:
    """Liveness check. Extend with real dependency pings (Neo4j, Postgres,
    Redis) once those connections exist — for now this just confirms the
    process is up and routing requests."""
    return {"status": "ok"}
