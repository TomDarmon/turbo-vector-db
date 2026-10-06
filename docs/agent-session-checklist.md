# Agent session checklist

## Start

1. Read in order: `README.md` -> `docs/project-standards.md` -> `docs/agent-session-checklist.md` -> `docs/agent-handoff.md`.
2. Define one objective for this session.
3. Pick validation commands before editing.

## Build

1. Keep changes small and scoped.
2. Preserve project invariants from `docs/project-standards.md`.
3. Add or update focused tests for touched behavior.

## Validate and record

1. Run required checks for the change type.
2. For runtime changes, run `make runtime-up`, `make smoke`, `make runtime-down`.
3. Update `CHANGELOG.md` and `docs/agent-handoff.md` for substantial changes.
