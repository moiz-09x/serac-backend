# Schemas

Pydantic models for the canonical event schema (including access_scope) and
the graph ontology types (:Actor, :Group, :DecisionThread, :Event, etc).
Pydantic doubles here as both the validation layer AND the schema definition
— FastAPI uses it natively, so there's exactly one source of truth, not two
artifacts to keep in sync.

Spec: Docs/02-Pipeline/extraction-pipeline.md (canonical schema, Stage 2)
      Docs/03-Data-Model/data-model.md (graph ontology)
Status: not yet implemented.
