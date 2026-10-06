# Python tools

This folder contains lightweight Python tooling for turbo-vector.

## Setup (uv)

```bash
cd /workspace/python
uv sync
```

## Python SDK generation from OpenAPI

The Python SDK is generated from the Rust API OpenAPI spec (`/openapi.json`), not maintained by hand.
Generation uses OpenAPI Generator (`python-pydantic-v1`) via Docker.
Make sure Docker is available before running the generator script.

Regenerate it with:

```bash
cd /workspace/python
uv run python scripts/update_sdk.py \
  --openapi-url http://127.0.0.1:8080/openapi.json
```

This writes:

- normalized OpenAPI JSON to `python/openapi/turbo-vector.openapi.json`
- generated SDK package to `python/sdk/turbo_vector_sdk/`

## Locust benchmark / traffic simulation (canonical)

Turbo Vector now uses one canonical benchmark traffic path:
`python/benchmarks/locust_stress_single_node.py`.

Default behavior:

- validates runtime health endpoint,
- creates/seeds a collection once,
- runs mixed query/upsert traffic for `~5m` in headless mode.

Recommended run:

```bash
make dev-up
cd /workspace/python
uv run python benchmarks/locust_stress_single_node.py
make dev-down
```

Common overrides:

```bash
uv run python benchmarks/locust_stress_single_node.py --run-time 10m --users 80 --spawn-rate 12
TV_LOCUST_COLLECTION=locust-demo TV_LOCUST_NAMESPACE=benchmark uv run python benchmarks/locust_stress_single_node.py
```

Preflight check:

```bash
curl -fsS http://127.0.0.1:8080/health
```
