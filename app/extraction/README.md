# Extraction Pipeline

Stages 1-4 of event ingestion: dedupe, normalize to canonical schema, resolve
identity, position events in the knowledge graph. Runs as an ordered DAG of
independently-retried jobs (not a linear synchronous chain).

Spec: Docs/02-Pipeline/extraction-pipeline.md
Status: not yet implemented.
