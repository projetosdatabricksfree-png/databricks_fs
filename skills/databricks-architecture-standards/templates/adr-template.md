# ADR-NNN — <Short decision title>
**Status:** Proposed | Accepted | Superseded by ADR-MMM | Deprecated
**Date:** DD/MM/YYYY

## Context
What forces are at play? The problem, constraints (e.g. serverless-only Free Edition),
options considered, and what "good" looks like. State the business/technical driver.

## Decision
The choice, stated in one or two sentences. Be specific about the standard the team
should now follow (e.g. "Auto Loader `STREAM read_files` is the Bronze default").

## Justification
Why this option beats the alternatives. Score against the framework:
1. Scalability — ...
2. Cost (DBU + storage) — ...
3. Operational simplicity — ...
4. Alignment (recommended path / exam blueprint) — ...
5. Reversibility — ...

## Consequences
What this decision imposes on future work — the constraints code must respect. Examples:
- Tables targeted by AUTO CDC are pipeline-managed → no manual DML.
- The landing zone is the source of truth for full refresh → do not delete files.
- The tie-breaker column must be monotonic per key.

State both positive and negative consequences honestly. Supersede (don't edit) once accepted.
