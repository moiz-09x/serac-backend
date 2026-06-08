from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "serac-backend"
    environment: str = "development"

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "neo4j"
    neo4j_database: str = "neo4j"

    postgres_dsn: str = "postgresql+psycopg://postgres:postgres@localhost:5432/brain"

    redis_url: str = "redis://localhost:6379/0"

    anthropic_api_key: str = ""
    micro_agent_model: str = "claude-haiku-4-5-20251001"
    synthesis_model: str = "claude-sonnet-4-6"

    tenant_id: str = ""
    linear_api_key: str = ""

    # Slack — Socket Mode tokens (both required to enable Slack connector)
    slack_app_token: str = ""   # xapp-... (Socket Mode token)
    slack_bot_token: str = ""   # xoxb-... (Bot token)

    # Embedding model (local, via fastembed — no API key needed)
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_dim: int = 384
    thread_attach_threshold: float = 0.85  # THREAD_ATTACH_THRESHOLD from spec


settings = Settings()
