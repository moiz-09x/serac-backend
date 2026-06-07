# Retrieval & Query Layer

Six-stage agentic GraphRAG pipeline: Permission Gate -> Intent Router ->
Parallel Context Gathering -> Cognitive Synthesis -> Citation Verification ->
Guardrail/Policy Engine. Implement as a LangGraph state machine — the staged,
branching, verification-gated shape maps directly onto LangGraph's
node/edge/conditional-routing model.

Spec: Docs/04-Intelligence/retrieval-layer.md
Status: not yet implemented.
