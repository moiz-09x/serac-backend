"""Application settings, loaded from environment variables / .env.

This is the single place runtime configuration is defined. Add new settings
here rather than reading os.environ directly elsewhere — it keeps every
config value typed, validated, and discoverable in one spot.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- App ---
    app_name: str = "brain-backend"
    environment: str = "development"

    # --- Knowledge graph (Neo4j, per-tenant databases — see Docs/03-Data-Model/data-model.md) ---
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "neo4j"

    # --- Application DB / vector store (Postgres + pgvector) ---
    postgres_dsn: str = "postgresql+psycopg://postgres:postgres@localhost:5432/brain"

    # --- Cache / dedup / rate limiting / job queue backend ---
    redis_url: str = "redis://localhost:6379/0"

    # --- LLM provider ---
    anthropic_api_key: str = ""
    # Two-tier model split: small/fast for bounded micro-agent calls
    # (Docs/02-Pipeline/micro-agent-injection-layer.md), larger reasoning
    # model for synthesis / citation verification / intent routing
    # (Docs/04-Intelligence/retrieval-layer.md).
    micro_agent_model: str = "claude-haiku-4-5-20251001"
    synthesis_model: str = "claude-sonnet-4-6"


settings = Settings()
