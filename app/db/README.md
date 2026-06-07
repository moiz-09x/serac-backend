# DB Layer

Connection management for the three datastores this backend talks to:
  - Neo4j (knowledge graph, per-tenant databases)
  - Postgres + pgvector (application DB: identity review queue, credential
    metadata, tenant config, proposed-rule review state, AND vector embeddings
    — folded into one instance for v1 to minimize moving parts)
  - Redis (dedup/rate-limiting/idempotency + Arq job queue backend)

Spec: Docs/00-Overview/01-system-architecture.md §4 (Tech Stack)
Status: not yet implemented.
