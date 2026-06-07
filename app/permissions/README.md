# Permission Resolution

Resolves source-platform principals to :Actor/:Group nodes and materializes
:VISIBLE_TO edges at ingestion time. Also runs the periodic reconciliation
sweep that catches permission drift after ingestion.

Fail-closed: an event positioned in the graph but not yet access-scoped must
be invisible by default. This module backs Stage 0 (Permission Gate) of the
retrieval pipeline — there is no bypass, including for MCP-connected agents.

Spec: Docs/04-Intelligence/permissions-and-access.md
Status: not yet implemented.
