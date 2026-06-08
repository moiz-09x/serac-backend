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

    # ── Provider API keys (one per provider) ─────────────────────────────────
    deepseek_api_key: str = ""
    openai_api_key: str = ""
    anthropic_api_key: str = ""

    # ── Provider base URLs ────────────────────────────────────────────────────
    deepseek_base_url: str = "https://api.deepseek.com"
    openai_base_url: str = "https://api.openai.com/v1"

    # ── Agent model configs (use case → provider + model) ─────────────────────
    # Query decomposition: breaks a natural-language question into sub-queries
    decomposition_provider: str = "deepseek"
    decomposition_model: str = "deepseek-chat"

    # Synthesis: reads retrieved context and writes the cited answer
    synthesis_provider: str = "deepseek"
    synthesis_model: str = "deepseek-chat"

    # ── Connectors ────────────────────────────────────────────────────────────
    tenant_id: str = ""
    linear_api_key: str = ""
    linear_webhook_secret: str = ""

    slack_app_token: str = ""   # xapp-... (Socket Mode token)
    slack_bot_token: str = ""   # xoxb-... (Bot token)

    # ── Observability ─────────────────────────────────────────────────────────
    langfuse_host: str = "http://localhost:3000"
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""

    # ── Embedding (local, via fastembed — no API key needed) ──────────────────
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_dim: int = 384
    thread_attach_threshold: float = 0.65
    retrieval_min_score: float = 0.72


settings = Settings()
