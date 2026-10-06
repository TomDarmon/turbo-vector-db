# Observability runbook (SigNoz)

This runbook uses one benchmark path (`make bench`) and one dashboard sync path
(`make dashboards-sync`) while keeping runtime and observability stacks split.

## 1) Start stacks

Single command (recommended):

```bash
make dev-up
```

Manual split start (if needed):

```bash
make obs-up
make runtime-up-otel
```

Optional host/container metrics:

```bash
make obs-up-host
```

UI and health endpoints:

- SigNoz UI: `http://127.0.0.1:3301`
- Collector health: `http://127.0.0.1:13133`
- Runtime health: `http://127.0.0.1:8080/health`

## 2) Sync dashboards (no manual JSON upload)

```bash
make dashboards-sync
```

`dashboards-sync` runs:

1. `observability/signoz/generate_dashboards.py`
2. `observability/signoz/sync_dashboards.py`

Synced dashboard set (workflow-oriented):

1. `Turbo Vector Global Retrieval - Dashboard`
2. `Turbo Vector Distributed Retrieval - Dashboard`
3. `Turbo Vector Ingest Runtime - Dashboard`

Authentication options for dashboard sync:

- `TV_SIGNOZ_API_TOKEN=<bearer-token>` (preferred), or
- `TV_SIGNOZ_EMAIL=<email>` + `TV_SIGNOZ_PASSWORD=<password>`, optionally
- `TV_SIGNOZ_API_KEY=<api-key>`.

## 3) Generate telemetry traffic

```bash
make bench
```

`make bench` runs the Locust single-node split workload and preflights:

- runtime health endpoint.

Common tuning examples:

```bash
TV_LOCUST_RUN_TIME=10m make bench
TV_LOCUST_USERS=80 TV_LOCUST_SPAWN_RATE=12 make bench
TV_LOCUST_COLLECTION=locust-demo TV_LOCUST_NAMESPACE=benchmark make bench
```

## 4) Validate metrics, traces, logs

- **Metrics:** open synced dashboards and confirm these core panels populate:
  - Global Retrieval:
    - `Throughput`, `Query Latency p95 (ms)`, `Distributed Degraded Query Rate (%)`.
  - Distributed Retrieval:
    - `Distributed Degraded Query Rate (%)`,
    - `Degradation Reasons by Type`,
    - `Shard Outcomes by Status`,
    - `Exact Distributed Latency p99 (ms)`,
    - `ANN/Auto Distributed Latency p99 (ms)`,
    - `Degraded Distributed Latency p99 (ms)`.
  - Ingest Runtime:
    - `Queue Claim Rate`, `Queue Ack Rate`, `Queue Requeue Rate`,
    - `Apply Lag p95 (ms)`,
    - `Queue Depth`.
- **Traces:** use Traces Explorer, filter `serviceName` to runtime services.
- **Logs:** use Logs Explorer, filter `resource.service.name` by service.

If signals are empty:

1. `curl -fsS http://127.0.0.1:8080/v1/system/runtime` and verify OTEL fields.
2. `curl -fsS http://127.0.0.1:13133`.
3. `make obs-logs`.

## 5) Distributed retrieval alert remediation

### A) Tail latency breach (p95/p99)

1. Check shard degradation reasons in query responses (`distributed.degradation_reasons`).
2. Check `Turbo Vector Distributed Retrieval - Dashboard`:
   - `Exact Distributed Latency p95/p99 (ms)`,
   - `ANN/Auto Distributed Latency p95/p99 (ms)`,
   - `Degraded Distributed Latency p99 (ms)`.
3. Verify shard timeout config and placement states:
   - `GET /v1/collections/{name}/shards/placement`.
4. If only one shard is slow, mark it `draining` or `offline` and rebalance.
5. Re-run `make rust-test-distributed-bench` to confirm p99 returns below threshold.

### B) Recall regression

1. Confirm ANN path is active (`ann.ann_used=true`) and fallback/error rates.
2. Run ANN strict profile:
   - `make rust-test-ann-bench`.
3. If recall is below floor, tune ANN read/probe budgets before increasing shard timeout.

### C) Shard degradation surge

1. Inspect `distributed.shard_statuses` for dominant failure class.
2. Check `Degradation Reasons by Type` and `Shard Outcomes by Status` panels.
3. For `dropped_shard`, restore node/placement or apply rebalance.
4. For `stale_generation_shard`, block cutover until generation guard is satisfied.

### D) Timeout surge

1. Identify affected shard IDs in degraded responses.
2. Check `Timeout Reason Rate (%)` and `Shard Latency p95 by Status (ms)` panels.
3. Increase timeout only after confirming shard IO or cache pressure root cause.
4. Validate with distributed benchmark profile and compare timeout reason rate.

### E) Dropped shard

1. Mark shard as `active` once node is healthy, or rebalance to healthy node set.
2. Check `Dropped Shard Reason Rate (%)` and `Shard Outcomes by Status`.
3. Keep fail-open enabled only as temporary mitigation.
4. Confirm degraded-rate drops below SLO threshold.

## 6) Shutdown

```bash
make dev-down
```

Manual split shutdown:

```bash
make runtime-down
make obs-down
```
