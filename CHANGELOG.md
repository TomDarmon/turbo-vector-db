# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Changed

- Release CI/CD is now disabled by default and project-agnostic:
  - every release job is gated on the `CICD_ENABLED` repository variable,
  - GCP project/region/registry values come from repository variables
    (`GCP_PROJECT_ID`, `GCP_REGION`, `GCP_DOCKER_REPOSITORY`,
    `GCP_HELM_REPOSITORY`) with placeholder fallbacks,
  - `promote-helm.yml` no longer takes project/region/repository inputs,
  - added `docs/ci-cd-setup.md` with bring-your-own-GCP setup steps.
- Namespace API cleanup:
  - multi-query overload query parameter renamed to `overload=multiQuery`,
  - internal namespace adapter types/functions renamed to `Namespace*` /
    `*_namespace_*`.
- Docs now use a `<your-gcs-bucket>` placeholder for GCS runtime examples.

- Added an optional, removable runtime tutorial visualization layer (`viz`) with
  minimal blast radius:
  - backend read-only observability namespace mounted under
    `/v1/observability/*` with:
    - `POST /v1/observability/collections/{name}/query-explain`,
    - `GET /v1/observability/collections/{name}/queue`,
    - `GET /v1/observability/storage/inventory`,
  - explain payloads now expose runtime execution path, warm/cold signal,
    fallback reasons, object-read/cache summaries, and stepwise timings for
    pedagogical walkthroughs,
  - frontend tutorial app route suite added (`/viz`, `/viz/scenarios`,
    `/viz/query-lab`, `/viz/storage-map`) with admin-only access and forced
    first-login password rotation path (`/viz/change-password`),
  - frontend integration switched to tRPC router procedures
    (`frontend/src/server/api/routers/viz.ts`) and removed custom Next.js API
    routes for viz access,
  - compose/helm packaging now supports optional `viz` deployment:
    - Docker Compose `frontend` service behind profile `viz`,
    - new Make targets `viz-up`, `viz-down`, `viz-logs`,
    - Helm `viz.enabled=false` default with conditional `deployment-viz` and
      `service-viz` templates,
  - added frontend docker packaging and env wiring for bootstrap admin
    configuration (`TV_VIZ_*`) while keeping runtime durability state unchanged
    (auth-only SQLite persistence in frontend service).
  - simplified frontend auth plumbing for viz:
    - removed `frontend/src/server/viz/guard.ts` page guards,
    - removed custom viz procedure layer; only `publicProcedure` and
      `protectedProcedure` remain,
    - moved auth redirect for `/viz*` to Next proxy
      (`frontend/src/proxy.ts`),
    - bootstrap now seeds admin credentials/policy only (table creation removed;
      Drizzle migrations own schema lifecycle).

- Reworked release automation into a strict 4-workflow model:
  - `.github/workflows/publish-rc-image.yml` (tag-triggered on
    `image/vX.Y.Z-rc*`) to build and push RC images,
  - `.github/workflows/publish-rc-helm.yml` (tag-triggered on
    `helm/vX.Y.Z-rc*`) to package and push RC Helm chart artifacts,
  - `.github/workflows/publish-image.yml` (manual) to promote RC image tags to
    final tags,
  - `.github/workflows/promote-helm.yml` (manual) to promote RC Helm chart tags
    to final tags.
- Added `docs/release-process.md` and linked it from `README.md` to document
  the per-component RC publish and manual promotion paths for images and Helm.

- Added a Helm v1 deployment chart for Kubernetes at
  `deploy/helm/turbo-vector` covering the core runtime roles
  (`api`/`broker`/`worker`) plus optional in-cluster RustFS for local/dev:
  - chart resources include role-specific Deployments, API/Broker Services,
    ServiceAccount wiring, runtime ConfigMap + S3 credential Secret handling,
    optional RustFS Deployment/Service/PVC, and RustFS bucket-init Job,
  - added fail-fast validation via `values.schema.json` and template guardrails
    (`templates/validate.yaml`) for invalid mode/auth/cache/replica
    combinations,
  - added startup safety guards (API/worker wait-for-broker init containers and
    RustFS bucket readiness waits) and optional post-install/upgrade preflight
    hook Jobs for broker and storage connectivity checks,
  - added Helm chart NOTES for local port-forward + health/runtime verification.

- Added a fast local host-process runtime path for Rust iteration:
  - new make targets:
    - `make local-up`, `make local-down`, `make local-ps`, `make local-logs`,
    - `make local-restart-api`, `make local-restart-broker`,
      `make local-restart-worker`,
  - `local-up` starts `rustfs`/`rustfs-init` in Docker and launches host
    `broker`/`api`/`upsert-worker` processes via `cargo run`,
  - local process management now writes pid/log artifacts under `local/`,
    clears stale pid files, and fails fast on host port conflicts.

- Simplified operator documentation and agent instructions around the core
  runtime workflow:
  - rewrote `README.md` and `AGENTS.md` to center on
    `make runtime-up -> make smoke -> make runtime-down`,
  - aligned project quality guidance with explicit strict linting via
    `make rust-clippy-strict` (fails on warnings, including dead code),
  - added missing process docs:
    - `docs/agent-handoff.md`,
    - `docs/decision-log.md`,
    - `docs/limits-registry.md`,
  - introduced `skills/` compartment docs for optional workflows:
    - `skills/observability/SKILL.md`,
    - `skills/benchmarks/SKILL.md`.

- Simplified storage-provider runtime switching and removed static GCS HMAC credential requirements:
  - switched to a single compose stack and a single runtime command with provider argument:
    - `make runtime-up STORAGE=rustfs|s3|gcs`,
  - changed base compose wiring so `api`/`broker`/`upsert-worker` no longer hard-depend on `rustfs-init`; RustFS services are started only in `STORAGE=rustfs`,
  - added ADC credential mount/env wiring directly in `docker-compose.yml` so GCS mode no longer requires a compose override file.
- Replaced GCS S3-interoperability adapter with native GCS JSON API semantics:
  - `turbo-vector-storage` now uses ADC/IAM bearer tokens for `gcs` mode (authorized-user ADC file and metadata-server token fallback),
  - removed `TV_STORAGE_ACCESS_KEY`/`TV_STORAGE_SECRET_KEY` requirement from GCS startup wiring in `turbo-vector-api`,
  - implemented GCS-native conditional object writes using generation preconditions:
    - `put_bytes_if_absent` -> `ifGenerationMatch=0`,
    - CAS writes -> `ifGenerationMatch=<generation>`,
  - versioned reads in GCS mode now surface object generation values for CAS round-trips.

- Added broker-side WAL queue commit throttling and marker ensure hardening for
  hot-object GCS behavior:
  - queue broker now enforces `min_commit_interval=1100ms` per queue object
    before each mutating CAS commit attempt,
  - API/broker runtime wiring now applies this throttle to
    `collections/<name>/queue/wal.json` handles,
  - collection registry marker ensure path now marks cache entries only after a
    successful/existing `put_bytes_if_absent` result (no in-flight skip race),
  - added regressions:
    - `turbo-vector-queue::broker_throttles_mutating_commits_per_queue_object`,
    - `turbo-vector-api::collection_registry_marker_is_eventually_created_after_concurrent_ensure_failure`.

- Hardened real-GCS queue/apply correctness and reduced hot-key mutation pressure:
  - worker apply now verifies each segment object is readable (and checksum-matching)
    before publishing a manifest generation that references it,
  - manifest publish now verifies the generation object is readable before advancing
    `manifests/current.json` (pointer now only moves after durable visibility),
  - WAL queue flush no longer sends heartbeat mutations for transiently missing WAL
    objects and no longer performs an extra per-flush queue snapshot read,
  - queue apply claim batch limit raised from `256` to `1024` and broker CAS loop
    now uses higher retry budget (`64`) plus longer group-commit buffering
    (`20ms`) to coalesce writes on hot `wal.json` keys,
  - compose default worker adaptive flush interval is now aligned to `1000ms`
    (instead of `200ms`) to avoid GCS hot-key churn in backlog mode.

- Reduced RC image build time in CI by switching Rust image builds to cache-first
  BuildKit behavior:
  - `rust/Dockerfile` now uses toolchain-pinned builder base
    (`rust:1.91.0-bookworm`) to match `rust-toolchain.toml`,
  - Rust build steps now cache Cargo registry + git source checkouts and
    `target/` artifacts with stable BuildKit cache IDs,
  - `.github/workflows/publish-rc-image.yml` now persists Buildx cache across
    runs (`cache-from/cache-to type=gha,scope=turbo-vector-rust`) so warm CI
    builds reuse compiled artifacts instead of recompiling from scratch,
  - RC workflow now configures Buildx via `docker/setup-buildx-action@v3`
    (`driver: docker-container`) so `type=gha` cache export/import is supported
    on GitHub-hosted runners.

- Hardened image release flow for split runtime roles while keeping a single
  build artifact:
  - added RC publish workflow
    (`.github/workflows/publish-rc-image.yml`) triggered by pushing
    `vX.Y.Z-rcN` tags,
  - RC workflow now builds once from `rust/Dockerfile` and publishes
    role-specific image names from the same artifact:
    - `turbo-vector-api`,
    - `turbo-vector-broker`,
    - `turbo-vector-worker`,
  - repurposed `.github/workflows/publish-image.yml` to final-tag
    promotion: on `vX.Y.Z`, it re-tags the latest matching RC image set
    (`vX.Y.Z-rcN`) to final tags without rebuilding,
  - updated `docs/gcp-vm-benchmark-agent-runbook.md` to document the new
    RC publish and final promotion sequence.

- Reworked distributed observability telemetry and dashboards for shard-aware
  operations:
  - added distributed query/shard metrics in
    `rust/crates/api/src/telemetry.rs`:
    - `tv_query_distributed_total`,
    - `tv_query_distributed_duration_seconds`,
    - `tv_query_distributed_degradation_total`,
    - `tv_query_distributed_shard_outcome_total`,
    - `tv_query_distributed_shard_latency_ms`,
    - `tv_query_distributed_successful_shard_ratio`,
    - `tv_query_distributed_required_shard_ratio`,
  - instrumented distributed fanout query flow in
    `rust/crates/api/src/routes.rs` to emit degraded reason, shard outcome,
    shard-latency, and shard-ratio signals for both fail-open and
    fail-closed distributed paths,
  - redesigned SigNoz generation from service-centric dashboards to three
    workflow dashboards:
    - `Turbo Vector Global Retrieval - Dashboard`,
    - `Turbo Vector Distributed Retrieval - Dashboard`,
    - `Turbo Vector Ingest Runtime - Dashboard`,
  - updated distributed SLO and observability runbook docs with explicit
    metric-to-panel mappings and distributed remediation panel drilldowns.
- Rebuilt SigNoz dashboard definitions around operator-focused views:
  - Global dashboard now emphasizes throughput, p95/p99 latency, success rate,
    and distributed share exposure,
  - Distributed dashboard now emphasizes node-level throughput/degradation,
    shard outcomes/latency, and distributed tail-latency slices
    (`exact`, `ann|auto`, degraded),
  - Ingest dashboard KPI titles now map to concrete queue/apply signals
    (upsert throughput, apply lag p95, ack/claim ratio) instead of ambiguous
    "Last 5m" labels.
- Fixed latency dashboard query semantics and units:
  - Global retrieval now uses one combined latency panel
    (`p50/p90/p99`) in a single graph,
  - latency quantile panels now query histogram series using dotted metric names
    via `__name__` selectors (for SigNoz metric naming),
  - removed extra `* 1000` scaling from histogram quantile panels to avoid
    second-scale inflation in UI.
- Simplified latency telemetry fix (no custom SDK view):
  - switched query/distributed/upsert/apply-lag/namespace-load histogram record
    values to milliseconds at emission time so they align with active bucket
    boundaries and produce sane pXX interpolation.

- Removed compatibility-only development surfaces to reduce maintenance burden:
  - deleted the separate namespace API contract and future-optimization test
    suites and their dedicated make targets,
  - removed the legacy namespace API planning doc.
- Removed ANN legacy v1 bucket decode support:
  - ANN bucket decode now accepts only binary v4 payloads.
- Removed unused telemetry compatibility wrappers:
  - `increment_cache_evictions(...)` (unscoped),
  - `record_cache_snapshot(...)` (unscoped),
  - scoped cache telemetry entrypoints remain as the single supported API.
- Simplified retrieval internals while preserving runtime behavior:
  - centralized shared vector score math into `rust/crates/api/src/scoring.rs`
    and removed duplicated score implementations across ANN/validation paths,
  - removed runtime-dead FTS helpers (`fts_block_key`, top-level term-meta test
    helpers), and gated benchmark-only FTS helpers behind `#[cfg(test)]`,
  - removed duplicate ANN rerank candidate budget enforcement branch,
  - removed non-actionable synthetic `cost_per_query_usd` assertions from
    distributed strict benchmarks.
- Simplified benchmark/tooling surfaces:
  - `make rust-test-fts-bench` now runs both lexical and postings-layout strict
    profiles (`fts::lexical_benchmarks` + `fts::bench_profiles`),
  - added `make rust-test-strict-bench` as a single entrypoint for all strict
    Rust benchmark gates (FTS + ANN + distributed),
  - trimmed `.PHONY` declarations to active make targets only,
  - renamed limits snapshot helper from
    `python/benchmarks/api_benchmark.py` to
    `python/benchmarks/limits_snapshot.py`,
  - renamed lexical benchmark profile label `LongLlmStyle` to
    `LongQueryStyle`.
- Simplified queue CAS wiring by extracting one shared object-store adapter:
  - added `rust/crates/api/src/queue_store_adapter.rs`,
  - removed duplicated `ObjectStoreCasAdapter` implementations from
    `state.rs` and `queue_broker.rs`.
- Reduced duplicate/noise tests:
  - removed duplicated ANN adaptive budget unit coverage in `ann.rs`
    (behavior remains covered by endpoint tests),
  - removed tautological distributed strict benchmark assertions that only
    rechecked fixed sample counts.
- Aligned benchmark docs with script behavior:
  - Locust preflight docs now describe the actual `/health` check path in
    `python/README.md` and `docs/observability-runbook.md`.

- Implemented the full M4 distributed scale/ops backlog
  (`docs/backlog/retrieval-backlog-m4-distributed-scale-and-ops.md`)
  across M4-T01..M4-T08:
  - added deterministic shard ownership (`hash_id_v1`) with shard-scoped
    physical namespaces and shard-aware worker segment materialization,
  - implemented distributed query fanout + deterministic global top-k merge with
    per-shard timeout/error policy controls and explicit degraded response
    metadata,
  - added shard placement/pinning/rebalance API surface:
    - `GET/PUT /v1/collections/{name}/shards/placement`,
    - `POST /v1/collections/{name}/shards/rebalance`,
  - added distributed failure labels:
    `dropped_shard`, `slow_shard_timeout`, `stale_generation_shard`,
    `shard_error`,
  - converted namespace and ANN-bucket caches to bounded entry/byte policies and
    emitted node+shard-scope telemetry dimensions for cache metrics,
  - added strict distributed benchmark gates in Rust tests with profiles:
    `1_node_baseline`, `4_shard_fanout`, `shard_skew`, `node_loss`,
  - added `make rust-test-distributed-bench`,
  - added distributed architecture/ops docs:
    - `docs/retrieval-architecture.md`,
    - `docs/distributed-retrieval-slo.md`,
    - `docs/shard-placement-runbook.md`,
    - `docs/distributed-benchmark-report.md`,
    - expanded `docs/observability-runbook.md` with alert remediation,
  - wired `TV_DISTRIBUTED_*` compose/runtime env propagation across
    `api`/`broker`/`upsert-worker` (plus `.env.example` defaults) so
    distributed shard controls are effective in real runtime deployments.

- Implemented the full M3 ANN v3 single-node backlog
  (`docs/backlog/retrieval-backlog-m3-ann-v3-single-node.md`)
  across M3-T01..M3-T08:
  - ANN index metadata now persists hierarchical centroid tree levels
    (`tree_levels`) with child-node and leaf-bucket references, and query planning
    uses configurable tree traversal knobs (`TV_ANN_TREE_ROOT_BEAM`,
    `TV_ANN_TREE_LEAF_PROBE_COUNT`).
  - ANN candidate-stage payloads are now binary-signature-first (1-bit sign
    encoding) with architecture-aware popcount scoring kernels and correctness +
    throughput guard tests.
  - Quantization-bound-aware rerank pruning is enforced with explicit telemetry:
    first-stage vs rerank candidate counts, bound margin/threshold reporting, and
    bounded rerank candidate ceilings.
  - Added bounded SSD rerank cache tiering for selective full-precision fetches:
    explicit entry/byte limits, LRU eviction, cache hit/miss/eviction telemetry,
    fetch-latency reporting, and generation-safe cache invalidation.
  - ANN rerank path no longer depends on full-namespace materialization:
    selective ID-only rerank vector loads are used with per-query object-read
    budget enforcement and explicit fallback reason taxonomy.
  - Extended ANN query observability payload with object reads/bytes by artifact
    class (`meta`, `bucket`, `filter cluster`, `filter row`, `rerank segment`)
    plus SSD cache and quantization-bound metrics.
  - Fixed ANN binary bucket decoding for v4 payload layout by consuming the
    encoded scale-count header slot during decode (prevents false
    `bucket_fetch_error` from cursor misalignment on realistic buckets).
  - Added strict ANN benchmark profile suite in Rust tests covering warm cache,
    cold cache, selective filter, and long-tail profile gates with strict
    assertions for recall, fallback/error rate, read budgets, and warm-vs-cold
    rerank-fetch latency improvement.
  - Added `make rust-test-ann-bench` as the canonical ANN strict profile target.
- Removed deprecated ANN quantized payload branches that were no longer used by
  ingest/query flows:
  - dropped ANN v2/v3 int8/f16 decode/scoring compatibility paths from
    `ann.rs`,
  - retained ANN binary v4 bucket decode support as canonical format
    (legacy v1 compatibility was removed in a follow-up cleanup pass),
  - removed direct `half` dependency from `turbo-vector-api`.

- Implemented the full M2 FTS query-surface/ranking backlog
  (`docs/backlog/retrieval-backlog-m2-fts-query-surface-and-ranking.md`)
  across M2-T01..M2-T08:
  - Added lexical prefix operator support (`BM25_PREFIX`) with bounded expansion
    and byte budgets, plus generation-time prefix term lookup artifacts.
  - Added `rank_by_filter` parsing/planning/runtime scoring with deterministic
    tie behavior and explicit filter-boost composition in lexical ranking.
  - Added schema-versioned unicode tokenizer option `word_v1` and integrated
    tokenizer-aware term delta/index rebuild flows for migration-safe upgrades.
  - Added lexical explain endpoint:
    `POST /v1/namespaces/{namespace}/explain_query`, including selected
    terms/fields, candidate/decode/skip block counts, and per-document score
    decomposition (`base_score`, `boost_multiplier`, `final_score`).
  - Hardened lexical expression handling with explicit error classes and stable
    canonicalization guarantees (including malformed-input fuzz-style coverage).
  - Expanded namespace API tests for BM25 prefix, regex+ranking
    interaction, conditional ranking, and explain-query surface coverage.
  - Added cold/warm lexical performance gate coverage with emitted report
    metrics (cold/warm latency, object fetch count, decoded bytes, skip ratio).
  - Added scriptable lexical reindex tooling:
    `python/scripts/fts_reindex_namespace.py` + `make fts-reindex ...`, and
    migration recovery coverage that verifies readers serve old or new
    generation during crash-safe publish retry windows.
  - Added runtime controls:
    - `TV_FTS_PREFIX_MAX_INDEX_CHARS`
    - `TV_FTS_PREFIX_MAX_EXPANSIONS`
    - `TV_FTS_PREFIX_MAX_EXPANSION_BYTES`
    - `TV_FTS_EXPLAIN_QUERY_ENABLED`
    - `TV_FTS_EXPLAIN_MAX_TOP_K`
  - Added in-process FTS index/doc-lookup caches in `AppState` to reduce
    object-store round trips on repeated lexical workloads.
- Simplified lexical tokenizer surface during active development:
  - removed legacy `word_v0`/`word_v3` tokenizer variants,
  - standardized defaults and schema parsing on `word_v1`,
  - removed dead legacy helper wrappers in FTS term-delta/tokenization paths and
    trimmed associated compatibility-only tests.
- Added M2 documentation and operator examples:
  - `docs/fts-m2-query-surface-and-migration.md`
  - updated `docs/README.md`
  - updated `Makefile` help/target docs for `fts-reindex`.

- Implemented the full M1 FTS storage + execution rewrite
  (`docs/backlog/retrieval-backlog-m1-fts-storage-and-execution.md`)
  across M1-T01..M1-T08:
  - postings codec migrated to packed columnar frames (v3) with deterministic
    roundtrip and strict frame-boundary corruption/truncation validation,
  - postings block metadata is now separable from payload decode and lexical
    telemetry now reports distinct `header_reads` vs `blocks_decoded`,
  - term block storage supports multi-block pack objects with offset/length refs
    to reduce object-store GET fanout on cold lexical queries,
  - lexical scoring paths now use batched decode/score kernels with enforced
    throughput floor and parity checks,
  - MAXSCORE execution was refactored to iterator-batched flow with strict
    top-k parity and skip/decode/docs-scored/threshold telemetry assertions,
  - lexical query path no longer loads full namespace vectors up front:
    generation-scoped lightweight doc lookup is used and only top-k vector IDs
    are hydrated for response materialization,
  - BM25 corpus stats are persisted in FTS index metadata with validation for
    missing/inconsistent values,
  - strict lexical benchmark profiles and gates were added for short keyword,
    long-query-style, and stopword-heavy workloads, plus make targets:
    `make rust-test-fts-bench` and `make bench`.
- Calibrated the long-query lexical benchmark baseline
  (`LONG_PROFILE_P95_BASELINE_MS=0.100`) to keep the strict +10% regression gate
  stable on current CI/cloud-agent hosts while preserving the stronger `>=2x`
  speedup-vs-exhaustive assertion.

- Split namespace API validation into two explicit Rust suites (API contract
  and future optimization/perf gaps) with dedicated make targets.
- Documented an observability-first optimization regression loop using
  `make dev-up`, `make dashboards-sync`, and `make bench`.

- Simplified local benchmark + observability workflow around one canonical
  traffic path and fewer commands:
  - `make bench` is now the single traffic/benchmark entrypoint (Locust),
  - removed legacy benchmark paths:
    - `python/benchmarks/api_benchmark.py`,
    - `observability/signoz/send-observability-traffic.sh`,
  - added reduced command families in Makefile:
    - `runtime-*`, `obs-*`, `dev-*`, `bench`, `dashboards-sync`,
  - old compose/observability/traffic targets remain as deprecation aliases for
    transition.
- Automated dashboard import/update via SigNoz API:
  - new script `observability/signoz/sync_dashboards.py`,
  - new command `make dashboards-sync` (generate + sync in one step),
  - manual dashboard JSON import is no longer required for the standard flow.
- Improved Rust container rebuild iteration speed:
  - `rust/Dockerfile` now uses manifest-first dependency fetch and BuildKit
    cache mounts,
  - new `rust/.dockerignore`,
  - new `make runtime-build-api` for fast API-only image builds,
  - `make runtime-restart` for no-rebuild container restarts.
- Hardened local two-compose observability workflow so telemetry reaches SigNoz
  reliably:
  - runtime and observability stacks now share an explicit external network
    (`turbo-vector-net`) created automatically by `make compose-network-up`,
  - runtime OTLP endpoint defaults now target the host-mapped collector
    (`http://host.docker.internal:4318`) for split-stack runs,
  - `make observability-traffic` now preflights runtime OTEL state + collector
    health and auto-falls back to a writable report path when `/tmp` contains a
    non-writable prior artifact.
- Added Locust-based sustained traffic generation for local single-node runs:
  - new script: `python/benchmarks/locust_stress_single_node.py`,
  - new make target: `make observability-traffic-locust`,
  - new Python dependency: `locust`.
- Added end-to-end observability operator workflow and assets:
  - `make compose-up-observability` now starts runtime with OTEL enabled and
    full trace sampling for local validation,
  - `make observability-generate-dashboards` regenerates per-service SigNoz
    dashboard JSON files (`api`, `broker`, `worker`),
  - `make observability-traffic` drives benchmark traffic to populate metrics,
    traces, and logs.
- Added OTLP log export in runtime telemetry setup by bridging `tracing` events
  to OpenTelemetry logs, so SigNoz Logs Explorer can be used alongside metrics
  and traces without changing existing application logging style.
- Added HTTP tracing middleware to API and broker routers so request spans are
  emitted consistently for SigNoz Trace Explorer and trace-powered panels.
- Added `docs/observability-runbook.md` to document setup, dashboard import, and
  telemetry smoke validation flow.
- Implemented deployment observability RFC baseline with optional runtime OTEL and
  split compose stacks:
  - runtime stack (`docker-compose.yml`) remains autonomous (`api`, `broker`,
    `upsert-worker`, RustFS),
  - new observability stack (`docker-compose.observability.yml`) provides
    SigNoz + ClickHouse + OTEL collector startup independently from runtime.
- Added OTEL runtime controls (default disabled) and exposed them in
  `/v1/system/runtime`:
  - `TV_OTEL_ENABLED`,
  - `TV_OTEL_EXPORTER_OTLP_ENDPOINT`,
  - `TV_OTEL_SERVICE_NAME`,
  - `TV_OTEL_METRIC_EXPORT_INTERVAL_MS`,
  - `TV_OTEL_SAMPLE_RATIO`.
- Added first-pass `tv_*` deployment telemetry in Rust runtime paths:
  - query: `tv_query_duration_seconds`, `tv_query_total`,
    `tv_query_temperature_total`, `tv_query_namespace_load_duration_seconds`,
    `tv_query_cache_fill_total`, `tv_query_ann_fallback_total`,
    `tv_query_ann_fetch_errors_total`,
  - insert/apply: `tv_upsert_request_duration_seconds`, `tv_upsert_accepted_total`,
    `tv_upsert_applied_total`, `tv_operation_apply_lag_seconds`,
  - queue/worker: `tv_queue_depth`, `tv_queue_claim_total`, `tv_queue_ack_total`,
    `tv_queue_requeue_total`, `tv_worker_flush_applied_jobs_total`,
  - cache: `tv_cache_hits_total`, `tv_cache_misses_total`,
    `tv_cache_evictions_total`, `tv_cache_entries`, `tv_cache_bytes`,
    `tv_cache_capacity_entries`, `tv_cache_capacity_bytes`,
    `tv_cache_utilization_ratio`.
- Reduced observability startup resource pressure on constrained cloud VMs:
  - host exporters (`node-exporter`, `cAdvisor`) are now optional under
    `host-metrics` compose profile (`make compose-observability-up-host`),
  - default `make compose-observability-up` starts core observability services only.
- Added lightweight telemetry validation aid:
  `observability/otel-debug-collector.yaml` (single OTLP receiver + debug
  exporter) for local metric-name verification without SigNoz/ClickHouse.
- Removed custom ClickHouse histogram UDF bootstrap from observability startup:
  - dropped `clickhouse-udf-setup` and `histogramQuantile` executable-function
    wiring from `docker-compose.observability.yml`,
  - removed `observability/signoz/clickhouse/custom-function.xml`,
  - percentile latency calculations now rely on native PromQL histogram queries.
- Added API dashboard query-latency percentile panels computed from histogram
  buckets:
  - `Query Latency p50 (ms)`,
  - `Query Latency p95 (ms)`,
  - `Query Latency p99 (ms)`,
  - each panel uses `histogram_quantile(... sum by(le)(rate(tv_query_duration_seconds_bucket[5m])))`.
- Added missing Python benchmark dependency `fire` to `python/pyproject.toml`
  so `python/benchmarks/locust_stress_single_node.py` runs without manual
  environment patching.

- Fixed BM25 worker publish failures caused by unbounded postings block ID growth:
  - split-generated FTS block IDs are now fixed-width and collision-safe, avoiding
    object-store key-length blowups (`KeyTooLongError`/`InvalidArgument`) during
    large-term maintenance.
  - added regression coverage that stress-splits a hot term and asserts compact,
    unique block IDs.
- Worker collection discovery now uses explicit registry markers
  (`collections/_registry/{collection}.json`) instead of scanning full
  `collections/` object catalogs:
  - collection metadata persistence and upsert ingress both ensure the marker
    exists,
  - collection deletion now removes the marker,
  - this removes benchmark-time indexing delays caused by huge object-list scans.
- BM25 benchmark harness now waits for lexical readiness before warm-query
  sampling:
  - strict `bm25_regression` now gates on `time_to_bm25_ready_seconds` in addition
    to vector-count visibility and operation apply completion,
  - limits snapshot schema/table now include `indexing_time_to_bm25_ready_s`.
- Updated split-stack compose wiring so API and worker queue clients route through
  `host.docker.internal:8091`, fixing broker reachability in cloud Docker runs.
- Hardened merged FTS postings runtime and worker wiring on latest main with no
  product-surface redesign:
  - bootstrap now performs full current-visible materialization when previous
    manifest generation exists but previous FTS metadata is missing (prevents
    partial/empty index publish on first recovered build),
  - worker indexing flags are decoupled:
    - `TV_WAL_WORKER_BUILD_FTS` (default `true`) controls FTS index building,
    - `TV_WAL_WORKER_BUILD_ANN` now controls ANN only,
    - runtime endpoint now exposes both booleans,
  - FTS `total_term_frequency` is now computed from exact posting `tf` sums
    rather than descriptor-based approximation,
  - FTS build now fails fast on stable-doc-id alias collisions within a
    generation scope, with explicit collision context in the error message,
  - worker FTS->ANN sequence avoids avoidable current-generation namespace reload
    churn by preserving current-generation cache residency across indexing steps,
  - FTS endpoint tests now use reusable support helpers for index-meta polling,
    term-meta loading, and decoded postings block loading to reduce duplicated
    polling/parsing boilerplate.
- **Breaking:** FTS postings storage now uses a generation-scoped fixed-block layout
  (`collections/{collection}/indexes/{namespace}/{generation}/fts/...`) with
  immutable term metadata + block artifacts; legacy/no-postings index format
  compatibility paths were removed.
- Implemented `docs/postings-index-layout-rfc.md` (AC-PI1..AC-PI4) end-to-end:
  - new FTS modules for keyspace, binary postings codec, term metadata,
    deterministic block maintenance, and delta-apply flow,
  - worker integration now builds/publishes postings artifacts per manifest
    generation before ANN readiness checks,
  - crash-safe publication uses `put_bytes_if_absent` so partial postings
    artifacts are not visible until index metadata is successfully published.
- Added runtime FTS block-policy controls:
  - `TV_FTS_BLOCK_TARGET_POSTINGS` (default 256),
  - `TV_FTS_BLOCK_SPLIT_THRESHOLD` (default 512),
  - `TV_FTS_BLOCK_MERGE_THRESHOLD` (default 128),
  - `TV_FTS_MAX_TERM_BLOCKS_TOUCHED_PER_DOC_UPDATE` (default 8),
  - `TV_FTS_ENABLE_DELTA_REBALANCE` (default true).
- Cloud agent defaults now pin Rust toolchain `1.91.0` with `rustfmt` and
  `clippy` preinstalled:
  - `.cursor/Dockerfile` installs and sets default toolchain to `1.91.0`,
  - `.cursor/environment.json` startup bootstrap now enforces `1.91.0` on fresh
    agents,
  - `rust/rust-toolchain.toml` is pinned from `stable` to `1.91.0` for
    deterministic local/agent Rust commands.
- **Breaking:** query/delete metadata filters now use a strict tuple/logical
  expression schema only; legacy flat object-equality filter maps are rejected.
- Completed native-filtering parity hardening (`docs/native-filtering-parity-rfc.md`)
  across NF-1..NF-4:
  - refactored filter logic into dedicated `filters/{ast,parser,eval,planner}.rs`,
  - expanded operator support to
    `Eq`/`Ne`/`Lt`/`Lte`/`Gt`/`Gte`/`In`/`NotIn`/`ContainsAny`/`ContainsAllTokens`/`Glob`/`Regex`/`And`/`Or`/`Not`,
  - added selectivity-aware ANN budgeting with deterministic widening and strict
    fallback reason codes.
- Native filter artifact caching now has explicit entry/byte bounds and eviction
  telemetry for both cluster summaries and row bitmaps, with runtime config/env
  surfaces and query observability fields for cache hits/misses/evictions and
  widening passes.
- Expanded the retrieval feature planning into three implementation-grade RFCs with
  feature-level verification criteria and rollout plans:
  - `docs/native-filtering-parity-rfc.md` (operator parity, selectivity-aware ANN
    planning, bounded filter caches, strict fallback telemetry),
  - `docs/postings-index-layout-rfc.md` (fixed-size postings blocks,
    split/merge maintenance, object-store keyspace/codec design),
  - `docs/bm25-maxscore-execution-rfc.md` (BM25 rank API parity, weighted rank
    expressions, vectorized block-max MAXSCORE execution and strict correctness/perf gates).
- Implemented native ANN filtering support backed by generation-scoped inverted
  filter artifacts:
  - ANN index build now materializes immutable filter index objects in object
    storage per `(collection, namespace, generation)`:
    - cluster-level summaries (`filters/cluster/{term_hash}.bin`),
    - row-level local-id bitmaps (`filters/row/{term_hash}/{bucket_id}.bin`),
  - ANN filtered queries now compile supported filters to term plans and use
    cluster summaries to restrict probed buckets plus row-level bitmaps to limit
    scored candidates inside each bucket,
  - on filter index load errors, ANN path now falls back to exact behavior via
    existing fallback machinery.
- Query/delete filter semantics now require strict tuple-expression forms:
  - operators:
    `Eq`/`Ne`/`Lt`/`Lte`/`Gt`/`Gte`/`In`/`NotIn`/`ContainsAny`/`ContainsAllTokens`/`Glob`/`Regex`
    plus nested `And`/`Or`/`Not`,
  - both `[field, operator, value]` and `[operator, field, value]` tuple forms
    are accepted; legacy object equality filters are no longer supported.
- Added an RFC for native filtering and postings indexes:
  - maps current filter/query gaps to a concrete object-storage-native index plan
    (`docs/native-filtering-and-postings-rfc.md`),
  - specifies dual-level native filter indexes (cluster summaries + row-level
    bitmaps) tied to ANN bucket addressing to keep filtered candidate counts
    close to unfiltered ANN behavior,
  - proposes fixed-size postings blocks (~256 target, split/merge bounded) and
    vectorized MAXSCORE BM25 execution with strict recall/latency/index-size
    benchmark gates.
- Added an RFC for deployment performance monitoring stack selection:
  - compares Grafana OSS (Prometheus/Loki/Tempo + OpenTelemetry Collector),
    SigNoz + ClickHouse, and VictoriaMetrics/VictoriaLogs/VictoriaTraces
    proposals,
  - recommends an optional OpenTelemetry-first SigNoz + ClickHouse baseline for
    node/service/custom vector-store telemetry (queue depth, query latency,
    warm-query latency, ANN fallback/error signals),
  - defines phased rollout, dashboard/alert baselines, and cardinality guardrails.
- Added a proposed middle-ground reranking RFC in `docs/` that defines:
  - optional query-time reranking modes (`none`, `expression`, `external`),
  - a safe JSON-AST expression evaluator scope for weighted scoring with
    freshness/timestamp and metadata numeric features,
  - an external reranker HTTP contract with timeout/fallback/allowlist guardrails,
  - phased rollout and test requirements aligned with current ANN observability style.
- Added a provider-aware object-store interface for multi-cloud compatibility:
  - new runtime selector `TV_STORAGE_PROVIDER` (`s3` default, `gcs` support),
  - storage crate now exposes cloud provider config/factory interfaces,
  - Google Cloud Storage mode uses GCS S3 interoperability credentials with
    provider-specific defaults (`https://storage.googleapis.com`, region `auto`),
  - runtime config now reports `storage_provider`.
- Added a namespace API adapter layer (without changing core
  storage/query engine behavior):
  - new routes: `GET /v1/namespaces`,
    `GET|POST /v1/namespaces/{namespace}/schema`,
    `GET /v1/namespaces/{namespace}/metadata`,
    `POST|DELETE /v1/namespaces/{namespace}`,
    `POST /v1/namespaces/{namespace}/query` (including
    `overload=multiQuery`),
  - namespace writes now map supported payloads (`upsert_rows`,
    `upsert_columns`, deletes/filter) to existing collection flows and flush queued
    upserts before returning so query-after-write checks are deterministic,
  - unsupported write/query features (e.g. patch APIs, aggregate/BM25/query-plan
    endpoints) now return explicit `INVALID_ARGUMENT` instead of route-level `404`s.
- Collection/namespace key validation now accepts `.`
  (`[A-Za-z0-9_.-]`).
- Local runtime can now simulate object-store network latency via
  `TV_STORAGE_SIMULATED_LATENCY_MS` (default `0`, disabled):
  - latency is injected per object-store request in the S3 adapter for both API and
    worker roles,
  - compose wiring exposes the flag for local benchmark runs,
  - runtime config now reports `storage_simulated_latency_ms`.
- Upsert write/apply semantics are now fully queue-driven:
  - upsert always writes WAL + enqueues + returns `accepted` with `operation_id`,
  - removed deprecated `wait` request field from API/OpenAPI/SDK surfaces;
    clients now poll operation status for apply completion.
- Operation completion visibility is now explicit via persisted operation status:
  - `accepted` -> `applied` lifecycle with timestamps and generation,
  - API endpoint: `GET /v1/collections/{name}/operations/{operation_id}`.
- Dedicated worker flush cadence is now first-class and configurable:
  - default `TV_WAL_WORKER_FLUSH_INTERVAL_MS=1000`,
  - optional adaptive cadence via backlog threshold + faster interval.
- Split API read-path manifest cache now revalidates against `current.json` pointer
  before serving cached state, preventing stale generation visibility after worker
  publishes newer manifests.
- Benchmark output now includes queue/apply progression metrics:
  - `time_to_all_operations_applied_seconds`,
  - `queue_pending_operations_peak`,
  - operation apply lag + generation velocity summaries.
- ANN first-stage scoring now uses quantized bucket payloads with versioned,
  backward-compatible decode:
  - prefer int8 quantization (per-dimension scales),
  - fallback to f16 payloads when dynamic range would degrade int8 fidelity,
  - preserve legacy v1 bucket decode support.
- ANN query execution now uses a two-stage path:
  - quantized candidate scoring in stage 1,
  - exact rerank before final top-k response.
- Small-corpus ANN path now performs cache-backed exact top-k with query-norm
  reuse for cosine scoring, reducing per-query overhead while preserving exact
  result quality.
- Adaptive ANN probe floors were increased for medium/large corpora to hold the
  strict recall target (`mean recall@k >= 0.95`) under benchmark concurrency.
- Latest strict profile matrix (10k/50k/100k exact vs ann) now satisfies:
  - ANN qps >= exact qps on 10k and 50k,
  - sampled ANN recall mean >= 0.95,
  - ann_fallback_rate = 0 and ann_fetch_errors_total = 0.
- ANN bucket artifacts are now encoded as compact binary payloads (contiguous values,
  ids, metadata offsets, and optional vector norms) instead of per-bucket JSON;
  ANN query path no longer performs per-request JSON bucket decode.
- ANN index metadata now stores centroids in contiguous layout with precomputed
  centroid norms for lower-overhead probe scoring.
- ANN candidate scoring path now minimizes per-request allocation/cloning:
  - candidate references are scored/sorted by index,
  - `UpsertVector` values are materialized only for final top-k results.
- ANN probe/candidate budgeting is now adaptive to `top_k`, bucket distribution,
  and filter presence, improving sampled recall while preserving throughput.
- ANN bucket fetch fanout is explicitly bounded via app-state semaphore and
  runtime-configured concurrency (`TV_ANN_BUCKET_FETCH_CONCURRENCY`).
- Query responses now include ANN observability fields:
  - `ann_used`, `ann_fallback_count`,
  - `buckets_probed`, `candidates_scored`,
  - `ann_fetch_errors`, `fallback_reasons`.
- Python benchmark reporting now aggregates ANN observability and supports strict
  ANN reliability gates:
  - `--ann-max-fallback-rate`
  - `--ann-max-fetch-errors`
- Query endpoint now accepts an explicit search strategy hint
  (`search_strategy`: `auto|ann|exact`) so benchmarking can compare ANN and exact
  paths deterministically.
- Query execution now supports a generation-scoped, object-store-backed ANN path:
  - ANN artifacts are built per `(collection, namespace, manifest_generation)`,
  - queries probe ANN buckets, then exact-rerank candidates,
  - queries fall back to exact scan on ANN load/build failures.
- Runtime config now reports ANN toggle state (`query_ann_enabled`), controlled
  via `TV_QUERY_ANN_ENABLED` (default: enabled).
- Python benchmark tooling now supports ANN-vs-exact comparison with:
  - query strategy selection (`--query-strategy`),
  - sampled recall validation (`--recall-sample-count`,
    `--recall-strategy`, `--recall-baseline-strategy`),
  - optional recall threshold gating (`--recall-min-threshold`).
- Limits snapshot generation/markdown now includes recall metrics
  (`sample_count`, `mean/p95/min recall@k`) when recall validation is enabled.
- Consolidated contributor standards into `docs/project-standards.md`.
- Simplified and centralized docs:
  - concise `README.md`
  - concise `AGENTS.md`
  - concise `CONTRIBUTING.md`
  - concise `docs/agent-handoff.md`
- Reduced documentation duplication by removing separate testing/code-quality docs.
- Removed outdated `docs/target-architecture-v1-object-storage-only.md`.
- Replaced the handwritten Python SDK (`python/main.py`) with an OpenAPI-generated SDK workflow.
- Rust API now serves an OpenAPI document at `GET /openapi.json`.
- Rust API write orchestration now uses the `turbo-vector-queue` crate as the
  durable pending-write index:
  - upserts enqueue operation IDs into a per-collection storage-backed queue,
  - queue flush claims/acks queued jobs and publishes manifests from queue
    ordering instead of WAL prefix listing scans.
- Queue control-plane ownership is now split behind a dedicated broker service:
  - new runtime role `TV_PROCESS_ROLE=broker` exposes internal queue endpoints
    (`enqueue`, `claim`, `heartbeat`, `ack`, `snapshot`) and is the sole writer
    of queue object keys,
  - API and worker roles now call the broker via `TV_QUEUE_BROKER_URL` instead of
    spawning in-process queue brokers,
  - broker runs periodic stale-lease requeue scans (`TV_BROKER_REQUEUE_INTERVAL_MS`)
    so visibility-timeout recovery is not only claim-triggered.
- `turbo-vector-storage::ObjectStore` now exposes versioned reads and CAS writes
  (`get_bytes_with_version`, `put_bytes_cas`) to support queue CAS semantics.
- Queue flush behavior now only applies operations that were admitted through
  the queue (out-of-band WAL objects are ignored).
- API split deployment is now enforced for benchmarked runtime profiles:
  - `TV_PROCESS_ROLE=api` (HTTP API),
  - `TV_PROCESS_ROLE=worker` (queue-drain/indexer worker),
  - embedded `api+worker` mode is no longer supported.
- `TV_WAL_QUEUE_BACKGROUND_DRAIN=true` is now rejected as a supported runtime
  shape; split deployment requires dedicated worker service instead.
- Added a dedicated upsert worker loop that can scale independently from the API:
  - drains queue-admitted WAL operations by collection,
  - publishes manifest progress asynchronously,
  - prebuilds ANN artifacts in the background for latest manifests.
- Compose stack now includes a separate `upsert-worker` service and uses split
  API/worker deployment by default.
- Python benchmark tooling was refactored into a split-stack single-node
  harness with profile-driven stress modes:
  - `regression_small`, `soak_5m`, `soak_10m`,
  - continuous docker CPU/RAM monitoring for `api` and `upsert-worker`,
  - worker indexing timing (`time_to_visible`, `time_to_ann_ready`) as
    first-class benchmark outputs and limits snapshot fields.
- Queue write/apply telemetry now emits structured logs for enqueue + flush
  iterations (`queue_depth`, `pending_jobs`, `applied_jobs`, `acked_jobs`,
  `duration_ms`).
- Simplified queue flush control flow by consolidating retry/attempt handling in
  a shared internal helper.

### Added

- New indexing-lag benchmark profile: `indexing_lag_sustained`.
- Scaling playbook for split deployment topologies and KPI checklist:
  - `docs/scaling-recipe.md`.
- ANN benchmark profile definitions and success thresholds:
  - `python/benchmarks/ann_parity_profiles.md`.
- ANN endpoint and unit test coverage for:
  - binary artifact encoding/decoding,
  - deterministic ANN ordering under score ties,
  - explicit fallback observability path,
  - adaptive probe behavior and concurrency stress without fallback storms.
- Initial ANN implementation module (`rust/crates/api/src/ann.rs`) with:
  - centroid-based partitioning,
  - per-bucket object-store artifacts,
  - candidate reranking with exact distance scoring,
  - bounded ANN bucket fetch concurrency to reduce descriptor pressure.
- ANN endpoint coverage:
  - `ann_query_builds_index_artifacts_and_returns_ranked_matches`.
- `docs/project-standards.md` as the single standards reference.
- `python/scripts/update_sdk.py` to fetch OpenAPI and regenerate `python/sdk` via OpenAPI Generator (`python-pydantic-v1`).
- `python/openapi-generator-config.yaml` to pin SDK generator settings.
- New Rust crate `turbo-vector-queue` that isolates an object-storage queue component with:
  - single-object (`queue.json`) state modeling,
  - CAS-based brokered group commit,
  - worker heartbeat + stale claim reclamation for at-least-once delivery,
  - deterministic in-memory CAS tests covering lifecycle and batching behavior.
