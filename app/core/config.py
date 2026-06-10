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

    # ── Server ────────────────────────────────────────────────────────────────
    base_url: str = "http://localhost:8000"

    # ── OAuth credentials ─────────────────────────────────────────────────────
    # 256-bit AES key for encrypting tokens at rest: openssl rand -base64 32
    token_encryption_key: str = ""
    # HMAC secret for signing OAuth state params: openssl rand -hex 32
    oauth_state_secret: str = "change-me-in-production"

    notion_client_id: str = ""
    notion_client_secret: str = ""

    linear_client_id: str = ""
    linear_client_secret: str = ""

    slack_client_id: str = ""
    slack_client_secret: str = ""

    # ── Connectors (legacy / self-hosted fallback) ────────────────────────────
    tenant_id: str = ""
    linear_api_key: str = ""
    linear_webhook_secret: str = ""

    slack_app_token: str = ""  # xapp-... (Socket Mode token)
    slack_bot_token: str = ""  # xoxb-... (Bot token)

    notion_api_key: str = ""
    notion_webhook_secret: str = ""

    # ── Observability ─────────────────────────────────────────────────────────
    langfuse_host: str = "http://localhost:3000"
    langfuse_public_key: str = ""
    langfuse_secret_key: str = ""

    # ── Embedding (local, via fastembed — no API key needed) ──────────────────
    embedding_model: str = "BAAI/bge-small-en-v1.5"
    embedding_dim: int = 384
    retrieval_min_score: float = 0.72

    # ── Thread-relation LLM judge (used by background thread linker) ─────────
    thread_relation_provider: str = "deepseek"
    thread_relation_model: str = "deepseek-chat"

    # ── Thread linking thresholds ─────────────────────────────────────────────
    # Scores at or above hi → create RELATES_TO directly (no LLM needed)
    thread_link_hi_threshold: float = 0.85
    # Scores between lo and hi → escalate to LLM judge
    thread_link_lo_threshold: float = 0.72

    # ── Thread status helpers ─────────────────────────────────────────────────
    thread_status_concluded: str = "Concluded"


settings = Settings()
