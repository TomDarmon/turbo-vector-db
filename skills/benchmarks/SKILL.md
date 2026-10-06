# Benchmarking skill

Use this only when the task explicitly needs performance validation.

## Runtime preflight

```bash
make runtime-up STORAGE=rustfs
make smoke
```

## Canonical traffic benchmark

```bash
make bench
```

Useful overrides:

```bash
TV_LOCUST_RUN_TIME=10m make bench
TV_LOCUST_USERS=80 TV_LOCUST_SPAWN_RATE=12 make bench
```

## Strict benchmark gates

```bash
make rust-test-fts-bench
make rust-test-ann-bench
make rust-test-distributed-bench
make rust-test-strict-bench
```

## Record keeping

For meaningful benchmark changes:

- update `docs/limits-registry.md` with ceiling changes
- update `docs/agent-handoff.md` with commands and observed results
- update `CHANGELOG.md` in `Unreleased`

## Teardown

```bash
make runtime-down
```
