# Decision log

Track durable decisions that affect architecture, operations, or workflow.

## Template

```markdown
### YYYY-MM-DD - Decision title

- Context:
- Decision:
- Consequences:
- Follow-up:
```

## Entries

### 2026-03-08 - Compartmentalize optional workflows into skills

- Context:
  core runtime instructions were mixed with optional observability and benchmark details, creating drift.
- Decision:
  keep core runtime path in top-level docs and move optional operator workflows into `skills/`.
- Consequences:
  lower cognitive load for default development flow; optional operations remain documented in dedicated skill guides.
- Follow-up:
  keep future optional operational procedures in `skills/` first, then link from primary docs.
