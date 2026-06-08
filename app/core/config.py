from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "serac-backend"
    environment: str = "development"

    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "neo4j"
    neo4j_database: str = "neo4j"  # single DB in v1; swap to tenant_{id} for multi-tenant

    postgres_dsn: str = "postgresql+psycopg://postgres:postgres@localhost:5432/brain"

    redis_url: str = "redis://localhost:6379/0"

    anthropic_api_key: str = ""
    micro_agent_model: str = "claude-haiku-4-5-20251001"
    synthesis_model: str = "claude-sonnet-4-6"

    tenant_id: str = ""           # UUID for this deployment's tenant
    linear_api_key: str = ""      # lin_api_xxxx from Linear settings


settings = Settings()
