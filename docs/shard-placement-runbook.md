# Shard placement and rebalance runbook

## Purpose

Operate shard pinning/placement safely while preserving generation visibility guarantees.

## APIs

- Read placement:
  - `GET /v1/collections/{name}/shards/placement`
- Update placement directly:
  - `PUT /v1/collections/{name}/shards/placement`
- Generate/apply rebalance plan:
  - `POST /v1/collections/{name}/shards/rebalance`

## Safe rebalance flow

1. **Read current state**
   - fetch current placement and current manifest generation.
2. **Dry-run plan**
   - call rebalance endpoint with `dry_run=true`.
   - review returned `safety_checks`.
3. **Apply planned placement**
   - call rebalance with `dry_run=false`.
4. **Warm target shard paths**
   - run canonical traffic/bench workload.
   - verify cache and query metrics remain healthy.
5. **Cutover guard**
   - only set migration phase to `cutover` when target `min_visible_generation` is <= current manifest generation.
6. **Post-cutover validation**
   - verify no unlabeled partial responses,
   - verify degradation reason rates are below SLO thresholds.

## Safety checks (minimum)

- shard_count in placement must match node runtime shard_count.
- assignments must cover each shard exactly once.
- no empty `node_id`.
- `cutover` requires `min_visible_generation <= current_generation`.
- do not proceed if `dropped_shard` or timeout alerts are already firing.

## Rollback

1. Reapply previous placement version.
2. Mark affected shard assignments `active`.
3. Verify degraded query rate returns to baseline.
