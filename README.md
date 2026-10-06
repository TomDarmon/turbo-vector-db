    # turbo-vector

    Object-storage-native vector store with disposable compute and deterministic recovery.

    ## Core runtime flow

    ```bash
    make bootstrap
    make doctor
    make runtime-up STORAGE=rustfs
    make smoke
    make runtime-down
    ```

`runtime-up` starts the main stack: `broker`, `api`, `upsert-worker`.

## Kubernetes (Helm v1)

First chart location:

```bash
deploy/helm/turbo-vector
```

Validate and render:

```bash
helm lint deploy/helm/turbo-vector
helm template tv deploy/helm/turbo-vector --namespace turbo-vector
```

Local OrbStack install example (uses local image name):

```bash
helm upgrade --install tv-local deploy/helm/turbo-vector \
  --kube-context orbstack \
  -n turbo-vector \
  --create-namespace \
  --wait --wait-for-jobs \
  --set image.pullPolicy=Never
```

For default ClusterIP API access:

```bash
kubectl --context orbstack -n turbo-vector port-forward svc/tv-local-turbo-vector-api 8080:8080
curl -fsS http://127.0.0.1:8080/health
curl -fsS http://127.0.0.1:8080/v1/system/runtime
```

## Fast local Rust iteration stack

For faster Rust edit/test loops, run storage in Docker but run runtime roles
as host processes:

    ```bash
    make local-up
    make smoke
    make local-down
    ```

    This starts `rustfs` + `rustfs-init` in Docker and runs `broker`, `api`,
    and `upsert-worker` via `cargo run` in separate background processes.
    Logs and pid files are written under `local/`.

    Useful helpers:

    - `make local-ps`
    - `make local-logs`
    - `make local-restart-api`
    - `make local-restart-broker`
    - `make local-restart-worker`

    Use `make runtime-up STORAGE=<provider>` with:

    - `rustfs` (default, fully local)
    - `s3` (external S3-compatible backend)
    - `gcs` (Google Cloud Storage)

    ## Storage providers

    ### Local quick testing (`rustfs`)

    ```bash
    make runtime-up STORAGE=rustfs
    ```

    ### External S3-compatible backend (`s3`)

    ```bash
    TV_STORAGE_PROVIDER=s3 \
    TV_STORAGE_BUCKET=<bucket> \
    TV_STORAGE_ENDPOINT=<endpoint> \
    TV_STORAGE_REGION=<region> \
    TV_STORAGE_ACCESS_KEY=<access-key> \
    TV_STORAGE_SECRET_KEY=<secret-key> \
    make runtime-up STORAGE=s3
    ```

    ### Google Cloud Storage (`gcs`, ADC)

    GCS mode expects Application Default Credentials from the local user machine.
    Set provider + bucket and run:

    ```bash
    TV_STORAGE_PROVIDER=gcs TV_STORAGE_BUCKET=<your-gcs-bucket> make runtime-up STORAGE=gcs
    ```

    Nothing else is required when local ADC is already configured.

    ## Optional observability (separate from core runtime)

    Runtime only:

    ```bash
    make runtime-up STORAGE=rustfs
    ```

    Runtime + SigNoz stack:

    ```bash
    make dev-up
    make dev-down
    ```

    Observability scripts and ClickHouse query tips are in
    `skills/observability/SKILL.md`.

    ## Optional runtime tutorial UI (viz)

    The visualization layer is optional and removable. It is disabled by default.

    Start runtime with viz endpoints enabled:

    ```bash
    TV_VIZ_ENABLED=true make runtime-up STORAGE=rustfs
    ```

    Start the frontend tutorial service:

    ```bash
    make viz-up
    ```

    Open:

    ```text
    http://127.0.0.1:3000/viz
    ```

    Stop:

    ```bash
    make viz-down
    make runtime-down
    ```

    ## Quality and hygiene checks

    ```bash
    make rust-build
    make rust-test
    make rust-fmt-check
    make rust-clippy-strict
    make python-test
    ```

    Benchmark commands are intentionally separated in `skills/benchmarks/SKILL.md`.

    ## Docs

    - Contribution flow: `CONTRIBUTING.md`
    - Agent operating rules: `AGENTS.md`
    - Release process (images/chart RC + promotion): `docs/release-process.md`
    - CI/CD setup (disabled by default, bring your own GCP project): `docs/ci-cd-setup.md`
    - Project standards (tests + quality + done criteria): `docs/project-standards.md`
    - Session checklist: `docs/agent-session-checklist.md`
    - Session handoff: `docs/agent-handoff.md`
    - Decision log: `docs/decision-log.md`
    - Limits registry: `docs/limits-registry.md`
    - Docs map: `docs/README.md`
    - Skills index: `skills/README.md`
    - Changelog: `CHANGELOG.md`
