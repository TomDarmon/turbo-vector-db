# Observability skill

Use this only when the task needs telemetry validation, dashboard maintenance, or data inspection.

## 1) Start the optional stack

```bash
make dev-up
```

For runtime only (no SigNoz):

```bash
make runtime-up STORAGE=rustfs
```

## 2) Generate and sync dashboards

Generate dashboard JSON:

```bash
make dashboards-generate
```

Generate + sync into SigNoz:

```bash
make dashboards-sync
```

If SigNoz auth is enabled, export one of:

- `TV_SIGNOZ_API_TOKEN`
- `TV_SIGNOZ_EMAIL` + `TV_SIGNOZ_PASSWORD`

## 3) Produce telemetry traffic

```bash
make bench
```

## 4) Query ClickHouse after a run

Use the observability compose stack container:

```bash
docker compose -f docker-compose.observability.yml exec -T clickhouse clickhouse-client --query "SHOW DATABASES"
```

Discover tables first:

```bash
docker compose -f docker-compose.observability.yml exec -T clickhouse clickhouse-client --query "SHOW TABLES FROM signoz_traces"
docker compose -f docker-compose.observability.yml exec -T clickhouse clickhouse-client --query "SHOW TABLES FROM signoz_metrics"
docker compose -f docker-compose.observability.yml exec -T clickhouse clickhouse-client --query "SHOW TABLES FROM signoz_logs"
```

Example quick volume checks:

```bash
docker compose -f docker-compose.observability.yml exec -T clickhouse clickhouse-client --query "SELECT database, table, total_rows FROM system.tables WHERE database LIKE 'signoz_%' ORDER BY total_rows DESC LIMIT 20"
```

## 5) Stop optional stack

```bash
make dev-down
```
