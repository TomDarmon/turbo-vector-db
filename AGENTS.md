# Agent operating plan

## Canonical read order

1. `README.md`
2. `docs/project-standards.md`
3. `docs/agent-session-checklist.md`
4. `docs/agent-handoff.md`

## Scope discipline

- Keep the main path simple:
  - start with `make runtime-up`,
  - validate with `make smoke`,
  - stop with `make runtime-down`.
- Treat observability and benchmark operations as optional add-ons.
- Move optional workflows into `skills/` and keep core docs short.

## Runtime stack

- Main runtime stack is `broker + api + upsert-worker`.
- Supported providers:
  - `rustfs` for local quick testing,
  - `s3` for S3-compatible backends,
  - `gcs` for Google Cloud Storage via ADC.
- GCS local setup expectation:
  - `TV_STORAGE_PROVIDER=gcs TV_STORAGE_BUCKET=<your-gcs-bucket> make runtime-up STORAGE=gcs`

## Hygiene requirements

- Always run checks proportional to change risk.
- Minimum quality gates for Rust behavior changes:
  - `make rust-build`
  - `make rust-test`
  - `make rust-fmt-check`
  - `make rust-clippy-strict` (fails on warnings, including dead code)
- For runtime wiring changes, also run:
  - `make runtime-up`
  - `make smoke`
  - `make runtime-down`
- Prefer maintainable code over clever code; add focused tests that protect behavior.

## Required records for substantial changes

- `CHANGELOG.md`
- `docs/agent-handoff.md`
- `docs/decision-log.md` (only when a durable decision is made)
- `docs/limits-registry.md` (only when limits change or new ceilings are discovered)

## Skills folder

- `skills/observability/SKILL.md`:
  dashboard generate/sync flow and post-run ClickHouse query workflow.
- `skills/benchmarks/SKILL.md`:
  canonical benchmark runs and strict benchmark gates.
