# Agent handoff

Use this template for substantial changes.

## Entry template

- Session date + short title:
- Objective:
- What changed:
- Validation (commands + results):
- Limits/perf notes:
- Risks and next step:

## Entries

### 2026-10-06 - Disable release CI/CD by default and neutralize namespace API naming

- Objective:
  make the repository self-contained: no hosted project/bucket identifiers,
  opt-in release CI/CD.
- What changed:
  release workflows gated on `vars.CICD_ENABLED`, GCP values moved to
  repository variables, added `docs/ci-cd-setup.md`; namespace API adapter
  identifiers renamed and multi-query param renamed to `overload`; GCS bucket
  examples replaced with `<your-gcs-bucket>`.
- Validation (commands + results): see `make rust-*` gates for this change.
- Limits/perf notes:
  no runtime limits changed.
- Risks and next step:
  clients using the old multi-query query param must switch to
  `overload=multiQuery`. To publish releases, follow `docs/ci-cd-setup.md`.

### 2026-03-22 - Optional viz tutorial layer with tRPC frontend routing

- Objective:
  implement a removable, optional tutorial visualization layer that executes
  real runtime scenarios, while keeping the core runtime path unchanged and
  avoiding custom Next.js API routes for viz.
- What changed:
  backend:
  - added `rust/crates/api/src/observability.rs` and mounted read-only
    observability endpoints under `/v1/observability/*`,
  - added explain payload shaping for query path pedagogy
    (path/temperature/steps/optimizations/fallback/object reads/cache/timings),
  - added storage-key classification and queue snapshot summarization helpers,
  - added endpoint/unit coverage for observability flows,
  - added `TV_VIZ_ENABLED` runtime flag plumbing and queue clippy fix in
    `rust/crates/queue/src/lib.rs` (`is_none_or` simplification).
  frontend:
  - removed custom `app/api/viz/*` route layer and moved viz access to
    tRPC router procedures (`frontend/src/server/api/routers/viz.ts`),
  - follow-up auth simplification:
    - removed `frontend/src/server/viz/guard.ts`,
    - removed dedicated viz procedure wrapper in favor of plain
      `protectedProcedure`,
    - added `frontend/src/proxy.ts` to redirect unauthenticated `/viz*`
      traffic to `/login`,
    - moved session user into `protectedProcedure` context (`ctx.user`) to
      avoid per-router glue code,
    - bootstrap script no longer creates auth tables; it only seeds admin
      credentials and password-rotation policy.
  - added auth bootstrap/policy support (`frontend/src/server/viz/*`) for admin
    bootstrap and required first-login password rotation,
  - added tutorial UI pages and components:
    - `/login`,
    - `/viz`,
    - `/viz/scenarios`,
    - `/viz/query-lab`,
    - `/viz/storage-map`,
    - `/viz/change-password`,
  - all browser interactions now call tRPC (`api.viz.*`) and frontend server
    helpers; no browser direct calls to broker.
  packaging:
  - Docker Compose:
    - added optional `frontend` service under profile `viz`,
    - added env wiring for `TV_VIZ_*`, `BETTER_AUTH_*`,
  - Makefile:
    - added `viz-up`, `viz-down`, `viz-logs`,
  - Helm:
    - added `viz` values block (`enabled=false` by default),
    - added conditional `deployment-viz.yaml` + `service-viz.yaml`,
    - runtime ConfigMap now exposes `TV_VIZ_ENABLED`,
    - notes/helper/schema updated for viz service wiring.
- Validation (commands + results):
  - `cd frontend && bun run typecheck` -> passed.
  - `cd frontend && bun run build` -> passed.
  - `cd frontend && bunx biome check src/proxy.ts src/server/viz/bootstrap.ts src/server/api/trpc.ts src/server/api/routers/viz.ts src/app/login/page.tsx src/app/viz/layout.tsx src/app/viz/page.tsx src/app/viz/scenarios/page.tsx src/app/viz/query-lab/page.tsx src/app/viz/storage-map/page.tsx src/app/viz/change-password/page.tsx` -> passed.
  - `helm lint deploy/helm/turbo-vector` -> passed.
  - `helm template tv deploy/helm/turbo-vector --namespace turbo-vector` -> passed.
  - `helm template tv deploy/helm/turbo-vector --namespace turbo-vector --set viz.enabled=true` -> passed.
  - `make rust-build` -> passed.
  - `cargo clippy --manifest-path rust/Cargo.toml -p turbo-vector-api --all-targets -- -W dead_code` -> passed (no dead-code warnings introduced in touched queue broker layer).
  - `cargo test --manifest-path rust/Cargo.toml -p turbo-vector-api observability::tests:: -- --nocapture` -> passed.
  - `cargo test --manifest-path rust/Cargo.toml -p turbo-vector-api observability_query_explain -- --nocapture` -> passed.
  - `cargo test --manifest-path rust/Cargo.toml -p turbo-vector-api observability_queue_and_storage_inventory_return_read_only_summaries -- --nocapture` -> passed.
  - `make rust-test` -> failed on existing integration test outside this
    refactor:
    `tests::endpoint_tests::fts_lexical_reindex_serves_old_or_new_generation_during_publish_recovery`.
  - `make rust-clippy-strict` -> failed due many existing pre-existing clippy
    findings outside this change set (across ANN/FTS/tests files).
  - `make runtime-up STORAGE=rustfs` / `make runtime-down` -> failed in this
    environment because docker daemon is unavailable.
  - `cd frontend && bun run check` -> failed due existing repo-wide Biome
    formatting/style issues outside viz change scope.
- Limits/perf notes:
  no runtime durability limits changed; observability/viz layer remains
  read-only and optional. Query explain path reuses existing query logic.
- Risks and next step:
  primary residual risk is operational enablement sequencing: API must also run
  with `TV_VIZ_ENABLED=true` for frontend viz endpoints to work. Next step:
  validate full end-to-end with docker daemon available using:
  `make runtime-up STORAGE=rustfs TV_VIZ_ENABLED=true`, `make viz-up`,
  scenario runs in `/viz/scenarios`, then `make viz-down` and `make runtime-down`.

### 2026-03-18 - Move release workflows to app repo and split manual triggers

- Objective:
  implement a release model with tag-separated RC publication and manual,
  per-component promotion for images and Helm charts.
- What changed:
  updated `.github/workflows` to a 4-workflow release matrix:
  - `publish-rc-image.yml`: trigger `push tags: image/v*.*.*-rc*`; build and
    push RC images,
  - `publish-rc-helm.yml`: trigger `push tags: helm/v*.*.*-rc*`; package and
    push RC Helm chart OCI artifact,
  - `publish-image.yml`: manual image promotion (`source_rc_tag` ->
    `target_tag`) without rebuild,
  - `promote-helm.yml`: manual chart promotion (`source_rc_tag` ->
    `target_tag`) without repackaging.
  Added `docs/release-process.md` and updated `README.md` docs links to reflect
  image/chart RC publish and manual promotion flows.
- Validation (commands + results):
  - `ruby -e 'require "yaml"; Dir[".github/workflows/*.yml"].each{|f| YAML.load_file(f)}; puts "ok"'` -> passed (`ok`).
  - `pre-commit run check-yaml --files ...` -> not runnable in this repo because `.pre-commit-config.yaml` is absent.
- Limits/perf notes:
  no runtime limits changed.
- Risks and next step:
  workflows require valid repository secrets
  (`GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_SERVICE_ACCOUNT`) and correct manual
  promotion inputs. Next step: run one `image/...-rc` tag publish, one
  `helm/...-rc` tag publish, and one manual promotion per component in GitHub
  Actions to confirm end-to-end permissions.

### 2026-03-17 - Helm v1 chart for api/broker/worker with fail-fast validation

- Objective:
  add a first Kubernetes Helm chart for `turbo-vector` that supports
  `api`/`broker`/`worker`, optional local RustFS mode, and early-fail validation
  for invalid configuration combinations.
- What changed:
  added `deploy/helm/turbo-vector` with:
  `Chart.yaml`, `values.yaml`, `values.schema.json`, and templates for
  role Deployments, API/Broker Services, ServiceAccount, runtime ConfigMap,
  conditional S3 Secret creation, optional RustFS Deployment/Service/PVC and
  `rustfs-init` bucket Job, optional API cache PVC, and release NOTES.
  Added strict template validation guards in `templates/_helpers.tpl` +
  `templates/validate.yaml` for:
  mode/rustfs alignment, S3 credential requirements, GCS auth exclusivity,
  API PVC requirements, and broker replica safety gate.
  Added startup init-container guards (wait-for-broker and RustFS bucket
  readiness) plus optional post-install/upgrade preflight hook Jobs for
  broker and storage connectivity checks.
  Fixed chart runtime issues discovered during OrbStack install validation:
  RustFS container now uses `args: [\"/data\"]` (not `command`), and helper
  init/preflight containers use `IfNotPresent` pull policy so app-level
  `image.pullPolicy=Never` does not break helper images.
- Validation (commands + results):
  - `helm lint deploy/helm/turbo-vector` -> passed.
  - `helm template tv deploy/helm/turbo-vector --namespace turbo-vector` -> passed.
  - `helm template` positive profiles:
    - `storage.mode=s3` with inline creds -> passed.
    - `storage.mode=gcs` with workload identity -> passed.
  - `helm template` negative/fail-fast profiles -> expected failures:
    - gcs dual-auth (`useWorkloadIdentity=true` + `credentialsSecretName`),
    - s3 missing credentials,
    - rustfs mode with external s3 endpoint configured,
    - api cache `pvc` mode with missing pvc sizing,
    - broker replicas `>1` without `allowUnsafeBrokerHA=true`.
  - OrbStack live install:
    - `helm upgrade --install ... --kube-context orbstack --wait --wait-for-jobs --set image.pullPolicy=Never`
      -> passed after runtime fixes above.
    - API checks via port-forward:
      `GET /health` returned `{\"status\":\"ok\"}`,
      `GET /v1/system/runtime` showed expected broker URL, RustFS endpoint, and
      SSD cache dir wiring.
  - Cleanup:
    `helm uninstall tv-local -n turbo-vector-helm-test2` and namespace deletion.
- Limits/perf notes:
  no runtime limits changed; chart defaults preserve disposable compute +
  object-store durability model.
- Risks and next step:
  primary residual risk is cloud-identity specifics for GCS/S3 production
  environments (service-account IAM and secret management conventions). Next
  step: add environment-specific values files (`values-local-rustfs.yaml`,
  `values-s3.yaml`, `values-gcs.yaml`) and optional CI `helm template` matrix.

### 2026-03-17 - Local host-process runtime for faster Rust iteration

- Objective:
  add a rustfs-first local stack that keeps storage in docker but runs `broker`,
  `api`, and `upsert-worker` as host processes for faster Rust edit/restart loops.
- What changed:
  updated `Makefile` with local runtime process orchestration targets:
  `local-up`, `local-down`, `local-ps`, `local-logs`,
  `local-restart-api|broker|worker`, plus internal start/stop/health helpers.
  Host-process defaults force `TV_STORAGE_PROVIDER=s3`,
  `TV_STORAGE_ENDPOINT=http://127.0.0.1:9000`, and
  `TV_STORAGE_REGION=us-east-1`; local pid/log artifacts are managed in `local/`.
  Updated `README.md` with a fast local Rust iteration section and added an
  `Unreleased` changelog entry.
- Validation (commands + results):
  - `make help` -> passed; new local targets are listed.
  - `make -n local-up` -> passed; target graph/scripts render as expected.
  - `make local-check-ports` -> passed.
  - `make local-ps` -> passed; reports stopped state with no pid files.
  - `make local-down` -> passed; no-op shutdown path works without pid files.
  - `make -n local-restart-api`
    `make -n local-restart-broker`
    `make -n local-restart-worker` -> passed; restart flows resolve correctly.
  - `make runtime-up STORAGE=rustfs` -> failed in this session because docker
    daemon was unavailable (`Cannot connect to the Docker daemon ...`).
  - `make local-up` -> failed in this session for the same docker-daemon reason.
- Limits/perf notes:
  no runtime limits changed; expected dev-loop speedup is from avoiding image
  rebuild/container restart for Rust role changes.
- Risks and next step:
  main risk is unvalidated end-to-end startup in this environment due missing
  docker daemon. Next step: rerun
  `make runtime-up STORAGE=rustfs && make smoke && make runtime-down` and
  `make local-up && make smoke && make local-ps && make local-down` once docker
  is available.

### 2026-03-08 - Docs simplification and skills compartmentalization

- Objective:
  simplify README and agent instructions, isolate observability/benchmark workflows into skills.
- What changed:
  updated core docs (`README.md`, `AGENTS.md`, `docs/project-standards.md`, `docs/agent-session-checklist.md`, `docs/README.md`), added `skills/observability/SKILL.md`, `skills/benchmarks/SKILL.md`, and created missing record docs.
- Validation (commands + results):
  docs and command references reviewed against `Makefile`; no code-path runtime behavior changed.
- Limits/perf notes:
  no runtime limits changed.
- Risks and next step:
  low risk (docs-only + one additive Make target); next step is to optionally run `make rust-clippy-strict` in CI to enforce strict warning hygiene.
