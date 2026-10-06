# Remote GCP Benchmark Automation Plan for turbo-vector

## Purpose

Create a dedicated, implementation-ready benchmark automation package that:

1. provisions a GCP VM,
2. uploads the current local workspace snapshot,
3. runs turbo-vector via Docker Compose with `STORAGE=gcs`,
4. executes benchmark workloads remotely against the VM-local API,
5. downloads results,
6. destroys all provisioned compute/network resources by default.

This plan is designed to be executable by another agent without prior chat context.

## Locked Decisions

- Benchmark runner style: **declarative TOML benchmark engine** (not Locust-only).
- Lifecycle model: **`create` + `run` + `destroy`** commands.
- GCS auth mode: **VM service account by default**.
- Code source for remote run: **upload local workspace snapshot**.
- Cleanup default: **full deletion** of VM + persistent disk + external IP.
- Default benchmark set: **safe profiles first** (lighter runs).
- Heavy profiles: **explicit opt-in** via flag (for example `--include-heavy`).

## Canonical Local Context (must stay aligned)

- Runtime and smoke orchestration:
  - `runtime-up`, `smoke`, `runtime-down`, `bench` in `Makefile`
  - File: `Makefile`
- Compose storage + credential wiring:
  - GCS env and ADC mount behavior in `docker-compose.yml`
  - File: `docker-compose.yml`
- Existing benchmark behavior baseline:
  - `python/benchmarks/locust_stress_single_node.py`
- Namespace API endpoints:
  - `rust/crates/api/src/routes.rs`
- GCS object-store behavior (ADC required, bucket must exist):
  - `rust/crates/storage/src/lib.rs`

## Deliverables

Create a new dedicated folder:

- `bench/remote-gcp/`

Expected contents:

- Standalone Go module + CLI binary source for `tvbench`.
- Benchmark definitions (TOML).
- Remote script/templates for bootstrap + run + teardown.
- Result output conventions and docs.

CLI command surface:

- `tvbench run <definition.toml>`
- `tvbench gcp create`
- `tvbench gcp run`
- `tvbench gcp destroy`

Benchmark profile sets:

- Vector + FTS profile variants for turbo-vector.
- Safe default profile set (lighter data volume) for default runs.
- Heavy 10M-like profile set behind explicit opt-in.

Results:

- Persist under `results/<profile>/...` with machine-readable + human-readable outputs.

## Implementation Blueprint

### Phase 1 - Scaffold and interfaces

Tasks:

- Create `bench/remote-gcp/` as an isolated Go module.
- Define CLI root + subcommands (`run`, `gcp create`, `gcp run`, `gcp destroy`).
- Add shared config model with precedence: flags > env > defaults.
- Add `--dry-run` mode to all `gcp` subcommands.

Acceptance:

- `tvbench --help` and subcommand help work.
- Config resolution behavior is deterministic and unit-tested.
- `--dry-run` prints full action plan without side effects.

### Phase 2 - TOML benchmark engine

Tasks:

- Implement definition parsing and validation for benchmark TOML files.
- Build workload runner for vector and FTS paths against turbo-vector namespace endpoints.
- Implement setup/seed, query/upsert workload execution, duration controls, and report collection.
- Ensure result artifacts are emitted under `results/<profile>/`.

Acceptance:

- `tvbench run <definition.toml>` executes end-to-end locally against a running API.
- Reports include enough detail to compare runs and spot regressions.
- Invalid/missing TOML fields fail fast with actionable errors.

### Phase 3 - GCP lifecycle orchestration

Tasks:

- `gcp create`:
  - Provision VM, disk, and networking metadata needed for SSH and benchmark runs.
  - Attach service account suitable for GCS access.
- `gcp run`:
  - Verify instance running (start if stopped).
  - Upload local workspace snapshot and benchmark assets.
  - Run remote bootstrap script:
    - prerequisite checks,
    - runtime bring-up with `STORAGE=gcs`,
    - smoke check,
    - benchmark execution,
    - result packaging.
  - Download results locally.
- `gcp destroy`:
  - Best-effort remote runtime shutdown.
  - Delete VM + disk + external IP by default.
  - Be idempotent for partially missing resources.

Acceptance:

- Command generation for `gcloud`, `ssh`, `scp` is deterministic and test-covered.
- Remote result retrieval is reliable.
- Destroy works cleanly on reruns after partial cleanup.

### Phase 4 - Remote runtime correctness hardening

Tasks:

- Provide remote Compose override to avoid mandatory ADC file mount when using VM service-account auth.
- Add preflight checks:
  - bucket existence,
  - IAM access,
  - disk free space,
  - Docker/Compose availability.
- Add clear warnings for high-stress profiles run on undersized VMs.

Acceptance:

- Service-account mode works without copying local JSON creds.
- Missing bucket/permissions fail early with clear remediation text.
- Heavy-profile warnings are displayed before execution.

### Phase 5 - Documentation and operational polish

Tasks:

- Add `bench/remote-gcp/README.md` with:
  - setup prerequisites,
  - example commands,
  - profile selection guidance,
  - cleanup guarantees,
  - cost/sizing notes.
- Update docs/skills references if command surface changes.
- Record implementation summary and validations in project records.

Acceptance:

- A new agent can run the full flow by following docs only.
- Operational gotchas and limits are explicit.

## Known Pitfalls and Mitigations

1. VM service-account auth vs compose ADC mount mismatch
   - Risk: `docker-compose.yml` currently expects a mounted ADC file path.
   - Mitigation: generate and use a remote compose override that removes strict local file dependency for VM runs.

2. GCS bucket preexistence requirement
   - Risk: storage layer does not auto-create GCS buckets.
   - Mitigation: add explicit preflight bucket existence + access checks before runtime startup.

3. Same VM hosts both stack and load generator
   - Risk: noisy-neighbor self-contention can skew benchmarks and crash heavy runs.
   - Mitigation: safe profiles as default, heavy profiles explicit opt-in, and publish minimum machine recommendations.

4. Remote prerequisites drift
   - Risk: Docker/Compose/SSH/gcloud path or permissions fail mid-run.
   - Mitigation: strict bootstrap validation stage with fail-fast diagnostics.

5. Partial resource cleanup
   - Risk: orphaned disks/IPs increase cost.
   - Mitigation: idempotent destroy flow that attempts all cleanup steps independently.

## Test Strategy

Unit tests:

- TOML parse/validation paths.
- Workload scheduler and duration handling.
- Response parsing + report serialization.
- CLI flag/env/default precedence.

Remote orchestration tests (no real GCP required):

- Generated `gcloud`/`ssh`/`scp` command correctness.
- Workspace packaging exclusions and payload construction.
- Compose override generation for service-account mode.

Behavioral checks:

- `--dry-run` for `gcp create`, `gcp run`, and `gcp destroy`.
- Failure-mode tests for missing bucket and auth failures.

Manual acceptance checklist:

1. `gcp create` provisions required resources.
2. `gcp run` uploads, boots runtime, passes smoke, runs selected profiles, downloads results.
3. `gcp destroy` removes VM/disk/IP cleanly.
4. Re-running destroy succeeds even if resources are already gone.

## Suggested Defaults and Guardrails

- Default region/zone should be configurable but set to practical GCP defaults.
- Default run should target safe profiles only.
- Heavy profiles require explicit flag and show a caution banner.
- Remote run should always emit a summary table with:
  - profiles attempted,
  - success/failure status,
  - remote runtime duration,
  - results path.

## Required Records in the Same Implementation PR

- `CHANGELOG.md`
- `docs/agent-handoff.md`
- `skills/benchmarks/SKILL.md` (if command flow changes)

## Definition of Done

This plan is complete when:

1. `bench/remote-gcp/` exists with the `tvbench` CLI and documented flow.
2. Full lifecycle works (`create` -> `run` -> `destroy`) with service-account GCS auth.
3. Safe profiles run by default; heavy profiles are explicit opt-in.
4. Results are reproducible and stored predictably.
5. Tests cover parsing, orchestration command generation, and dry-run semantics.
6. Docs and records are updated and aligned with actual commands.
