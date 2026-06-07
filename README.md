# Brain Backend

Single FastAPI backend for all logical services described in
`Docs/00-Overview/01-system-architecture.md §3`: extraction pipeline,
retrieval & query layer, rules induction, permission resolution, and the
admin API. They share one deployable for v1 — split out only if a specific
piece needs independent scaling.

## Stack

- **Framework:** FastAPI + Uvicorn
- **Agent orchestration:** LangGraph (the staged, branching,
  verification-gated retrieval pipeline maps directly onto its
  node/edge/conditional-routing model) + Anthropic Claude
- **Knowledge graph:** Neo4j (per-tenant databases)
- **App DB + vector store:** Postgres with pgvector (one instance — keeps
  v1 infra minimal; identity review queue, credential metadata, tenant
  config, proposed-rule review state, and embeddings all live here)
- **Cache / dedup / queue backend:** Redis
- **Background jobs:** Arq (asyncio-native, pairs naturally with FastAPI)
- **Schema validation:** Pydantic v2 (doubles as the canonical schema
  definition — one artifact, not two to keep in sync)

## Layout

```
app/
  main.py             FastAPI entrypoint
  core/               settings, shared config
  api/routes/         HTTP route modules
  extraction/         Stages 1-4 ingestion pipeline (DAG of jobs)
  retrieval/          6-stage agentic GraphRAG pipeline (LangGraph)
  permissions/        :VISIBLE_TO edge materialization, Permission Gate
  rules_induction/    nightly motif mining (post-v1, lowest priority here)
  db/                 Neo4j / Postgres / Redis connection management
  schemas/            Pydantic canonical schema + graph ontology models
```

Each subpackage has its own README pointing to the Docs/ spec that governs
it — read that first before writing code there.

## Running locally

```bash
cp .env.example .env   # fill in real values
uv sync
uv run uvicorn app.main:app --reload
```

`GET /health` and `GET /` should both return 200.

## Status

Scaffold only — routing, settings, and a health check exist; no business
logic yet. Build order follows `Docs/00-Overview/03-v1-mvp-scope.md §5`.
