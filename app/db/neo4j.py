from neo4j import AsyncDriver, AsyncGraphDatabase

from app.core.config import settings

_driver: AsyncDriver | None = None


async def init_driver() -> None:
    global _driver
    _driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    await _driver.verify_connectivity()
    async with _driver.session(database=settings.neo4j_database) as session:
        await session.run(
            """
            CREATE VECTOR INDEX event_embeddings IF NOT EXISTS
            FOR (e:Event) ON (e.embedding)
            OPTIONS {indexConfig: {
                `vector.dimensions`: $dims,
                `vector.similarity_function`: 'cosine'
            }}
            """,
            dims=settings.embedding_dim,
        )
        await session.run(
            """
            CREATE VECTOR INDEX thread_embeddings IF NOT EXISTS
            FOR (t:Thread) ON (t.embedding)
            OPTIONS {indexConfig: {
                `vector.dimensions`: $dims,
                `vector.similarity_function`: 'cosine'
            }}
            """,
            dims=settings.embedding_dim,
        )
        await session.run(
            """
            CREATE INDEX thread_platform_native IF NOT EXISTS
            FOR (t:Thread) ON (t.tenant_id, t.source_platform, t.platform_native_id)
            """
        )
        await session.run(
            """
            CREATE INDEX relates_to_confidence IF NOT EXISTS
            FOR ()-[r:RELATES_TO]-() ON (r.confidence)
            """
        )


async def close_driver() -> None:
    global _driver
    if _driver:
        await _driver.close()
        _driver = None


def get_driver() -> AsyncDriver:
    if _driver is None:
        raise RuntimeError("Neo4j driver not initialised — did lifespan run?")
    return _driver


def tenant_db(tenant_id: str) -> str:
    return f"tenant_{tenant_id}"
