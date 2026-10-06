# Project standards

This is the single source of truth for day-to-day contribution rules.

## Core invariants

1. Object storage is the only durable source of truth.
2. Compute is disposable.
3. Data files are immutable once written.
4. Manifest generations are atomic and monotonic.
5. Retry behavior remains idempotent with `Idempotency-Key`.
6. Recovery from object-store state is deterministic.

## Work loop

1. Keep scope to one objective.
2. Implement small, reviewable changes.
3. Run checks proportional to risk.
4. Update required records in the same change set.

## Required checks by change type

- Rust behavior changes (required):
  - `make rust-build`
  - `make rust-test`
- Rust quality/style (required):
  - `make rust-fmt-check`
  - `make rust-clippy-strict`
- Python benchmark/tooling changes:
  - `make python-test`
- Runtime/compose wiring changes:
  - `make runtime-up`
  - `make smoke`
  - `make runtime-down`

If a check cannot run, document why and provide best-effort alternative validation.

## Code quality rules

- Prefer simple, inspectable logic over speculative optimization.
- Keep failure handling explicit.
- Add focused regression tests for bug fixes.
- Keep and improve meaningful coverage for touched behavior.
- Keep comments short and only for non-obvious logic.
- Avoid mixing unrelated cleanup with behavior changes.

## Definition of done

A substantial change is done when:

- relevant checks passed
- docs and run commands match reality
- changelog is updated (`Unreleased`)
- handoff is updated with what changed, validation, risks, and next step
- decision log is updated when durable tradeoffs are made
- limits registry is updated when a limit changes or a new ceiling is discovered

## Handoff entry format

Use this structure in `docs/agent-handoff.md`:

- Session date + short title
- Objective
- What changed
- Validation (commands + results)
- Limits/perf notes
- Risks and next step
