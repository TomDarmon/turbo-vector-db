# /// script
# dependencies = ["httpx"]
# ///
"""
Profiling benchmark for turbo-vector.

Measures cold-namespace and warm-namespace query latencies with p50/p90/p99
reporting. Deterministic, sequential — focused on latency percentiles rather
than throughput under load.

Cold measurement: each namespace is queried exactly ONCE before moving to the
next namespace, so the namespace cache is guaranteed empty on first contact.
Multiple rounds cycle through all namespaces, giving enough samples while
keeping each round's first-per-namespace query truly cold.

Warm measurement: after all cold rounds, namespace data is cached. Warm queries
hit the in-memory namespace cache directly.

Usage:
    uv run python benchmarks/bench_profile.py --config ../python/benchmarks/profiles/default.toml
"""
from __future__ import annotations

import argparse
import json
import random
import statistics
import sys
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

DEFAULTS = {
    "server": {"base_url": "http://127.0.0.1:8080", "health_timeout_s": 5.0},
    "collection": {
        "name": "bench-profile",
        "dimension": 128,
        "metric": "cosine",
        "cleanup": True,
    },
    "ingest": {
        "num_namespaces": 3,
        "vectors_per_namespace": 10000,
        "batch_size": 200,
        "wait_for_applied": True,
        "poll_interval_s": 0.25,
        "poll_timeout_s": 120.0,
        "post_ingest_delay_ms": 200,
    },
    "query": {
        "top_k": 10,
        "search_strategy": "ann",
        "include_metadata": False,
        "include_values": False,
        "cold": {"enabled": True, "queries_per_namespace": 50},
        "warm": {"enabled": True, "queries_per_namespace": 200, "warmup_queries": 10},
    },
}


def _deep_merge(base: dict, override: dict) -> dict:
    merged = dict(base)
    for k, v in override.items():
        if k in merged and isinstance(merged[k], dict) and isinstance(v, dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def load_config(path: str) -> dict:
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    return _deep_merge(DEFAULTS, raw)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


@dataclass
class LatencyBucket:
    label: str
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.latencies_ms)

    def percentile(self, p: float) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_l = sorted(self.latencies_ms)
        idx = int(len(sorted_l) * p / 100.0)
        idx = min(idx, len(sorted_l) - 1)
        return sorted_l[idx]

    @property
    def p50(self) -> float:
        return self.percentile(50)

    @property
    def p90(self) -> float:
        return self.percentile(90)

    @property
    def p99(self) -> float:
        return self.percentile(99)

    @property
    def mean(self) -> float:
        return statistics.mean(self.latencies_ms) if self.latencies_ms else 0.0


@dataclass
class IngestResult:
    total_vectors: int
    total_time_s: float

    @property
    def throughput(self) -> float:
        return self.total_vectors / self.total_time_s if self.total_time_s > 0 else 0.0


@dataclass
class BenchmarkResults:
    config: dict
    collection_name: str
    ingest: IngestResult | None = None
    cold: LatencyBucket | None = None
    warm: LatencyBucket | None = None


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def health_check(client: httpx.Client, base_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_err = None
    while time.monotonic() < deadline:
        try:
            r = client.get(f"{base_url}/health", timeout=2.0)
            r.raise_for_status()
            print("[ok] health check passed")
            return
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    raise RuntimeError(f"health check failed after {timeout}s: {last_err}")


def create_collection(
    client: httpx.Client, base_url: str, name: str, dimension: int, metric: str
) -> None:
    payload = {"name": name, "dimension": dimension, "metric": metric}
    r = client.post(f"{base_url}/v1/collections", json=payload, timeout=10.0)
    if r.status_code not in {200, 201, 409}:
        raise RuntimeError(
            f"create collection failed: status={r.status_code} body={r.text}"
        )
    if r.status_code == 409:
        print(f"  collection '{name}' already exists (409)")
    else:
        print(f"  collection '{name}' created")


def delete_collection(client: httpx.Client, base_url: str, name: str) -> None:
    r = client.delete(f"{base_url}/v1/collections/{name}", timeout=60.0)
    if r.status_code in {200, 204, 404}:
        print(f"  collection '{name}' deleted")
    else:
        print(f"  [warn] delete collection: status={r.status_code} body={r.text}")


def _poll_operation(
    client: httpx.Client,
    base_url: str,
    collection: str,
    operation_id: str,
    poll_interval: float,
    poll_timeout: float,
) -> None:
    url = f"{base_url}/v1/collections/{collection}/operations/{operation_id}"
    deadline = time.monotonic() + poll_timeout
    while time.monotonic() < deadline:
        try:
            r = client.get(url, timeout=5.0)
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == "applied":
                    return
            elif r.status_code == 404:
                pass  # not yet visible
        except Exception:
            pass
        time.sleep(poll_interval)
    raise RuntimeError(
        f"operation {operation_id} not applied within {poll_timeout}s"
    )


def ingest_namespace(
    client: httpx.Client,
    base_url: str,
    collection: str,
    namespace: str,
    dimension: int,
    count: int,
    batch_size: int,
    wait_for_applied: bool,
    poll_interval: float,
    poll_timeout: float,
    rng: random.Random,
) -> float:
    upsert_url = f"{base_url}/v1/collections/{collection}/vectors/upsert"
    t0 = time.monotonic()
    for i in range(0, count, batch_size):
        end = min(i + batch_size, count)
        vectors = [
            {
                "id": f"{namespace}-{j}",
                "values": [rng.uniform(-1, 1) for _ in range(dimension)],
                "metadata": {"source": "bench-profile", "ns": namespace},
            }
            for j in range(i, end)
        ]
        payload = {"namespace": namespace, "vectors": vectors}
        r = client.post(upsert_url, json=payload, timeout=30.0)
        if r.status_code not in {200, 202}:
            raise RuntimeError(
                f"upsert failed: status={r.status_code} body={r.text}"
            )
        if wait_for_applied and r.status_code == 202:
            op_id = r.json().get("operation_id", "")
            if op_id:
                _poll_operation(
                    client, base_url, collection, op_id, poll_interval, poll_timeout
                )
    return time.monotonic() - t0


def query_single(
    client: httpx.Client,
    base_url: str,
    collection: str,
    namespace: str,
    dimension: int,
    top_k: int,
    search_strategy: str,
    include_metadata: bool,
    include_values: bool,
    rng: random.Random,
) -> float:
    """Issue one query and return its latency in ms."""
    query_url = f"{base_url}/v1/collections/{collection}/vectors/query"
    vector = [rng.uniform(-1, 1) for _ in range(dimension)]
    payload: dict = {
        "namespace": namespace,
        "vector": vector,
        "top_k": top_k,
        "search_strategy": search_strategy,
    }
    if include_metadata:
        payload["include_metadata"] = True
    if include_values:
        payload["include_values"] = True
    t0 = time.monotonic()
    r = client.post(query_url, json=payload, timeout=10.0)
    elapsed_ms = (time.monotonic() - t0) * 1000.0
    if r.status_code != 200:
        raise RuntimeError(
            f"query failed: status={r.status_code} body={r.text}"
        )
    return elapsed_ms


def run_queries(
    client: httpx.Client,
    base_url: str,
    collection: str,
    namespace: str,
    dimension: int,
    count: int,
    top_k: int,
    search_strategy: str,
    include_metadata: bool,
    include_values: bool,
    rng: random.Random,
    discard: int = 0,
) -> list[float]:
    latencies: list[float] = []
    for i in range(count):
        lat = query_single(
            client, base_url, collection, namespace, dimension,
            top_k, search_strategy, include_metadata, include_values, rng,
        )
        if i >= discard:
            latencies.append(lat)
    return latencies


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_benchmark(cfg: dict) -> BenchmarkResults:
    server = cfg["server"]
    coll_cfg = cfg["collection"]
    ingest_cfg = cfg["ingest"]
    query_cfg = cfg["query"]

    base_url = server["base_url"].rstrip("/")
    ts = int(time.time())
    collection_name = f"{coll_cfg['name']}-{ts}"
    dimension = coll_cfg["dimension"]
    metric = coll_cfg["metric"]
    num_ns = ingest_cfg["num_namespaces"]
    vecs_per_ns = ingest_cfg["vectors_per_namespace"]
    total_vecs = num_ns * vecs_per_ns

    results = BenchmarkResults(
        config=cfg,
        collection_name=collection_name,
    )

    rng = random.Random(42)

    print("=" * 60)
    print(f"  turbo-vector benchmark: profile")
    print(f"  collection: {collection_name}, dim: {dimension}, metric: {metric}")
    print(f"  namespaces: {num_ns}, vectors/ns: {vecs_per_ns} ({total_vecs} total)")
    print("=" * 60)
    print()

    with httpx.Client() as client:
        # Health check
        print("HEALTH CHECK")
        health_check(client, base_url, server["health_timeout_s"])
        print()

        # Create collection
        print("CREATE COLLECTION")
        create_collection(client, base_url, collection_name, dimension, metric)
        print()

        # Ingest
        print("INGEST")
        namespaces = [f"ns-{i}" for i in range(num_ns)]
        ingest_t0 = time.monotonic()
        for ns in namespaces:
            ns_time = ingest_namespace(
                client,
                base_url,
                collection_name,
                ns,
                dimension,
                vecs_per_ns,
                ingest_cfg["batch_size"],
                ingest_cfg["wait_for_applied"],
                ingest_cfg["poll_interval_s"],
                ingest_cfg["poll_timeout_s"],
                rng,
            )
            print(f"  {ns}: {vecs_per_ns} vectors in {ns_time:.3f}s")
        ingest_total = time.monotonic() - ingest_t0
        results.ingest = IngestResult(total_vectors=total_vecs, total_time_s=ingest_total)
        print(f"  total: {total_vecs} vectors in {ingest_total:.3f}s ({results.ingest.throughput:.1f} vec/s)")
        print()

        # Post-ingest delay
        delay_ms = ingest_cfg["post_ingest_delay_ms"]
        if delay_ms > 0:
            print(f"POST-INGEST DELAY: {delay_ms}ms")
            time.sleep(delay_ms / 1000.0)
            print()

        # Cold queries — one query per namespace per round, cycling through
        # namespaces so each namespace's first query in every round is truly
        # cold (namespace cache was not populated by a prior query in this
        # round).  Because the server's namespace cache holds data after the
        # first-ever query, only the VERY FIRST round's queries are truly
        # cold.  Subsequent rounds measure "re-warm" latency (cache already
        # populated), but we keep them for sample size.  The report
        # separately shows the first-touch latency.
        if query_cfg["cold"]["enabled"]:
            print("COLD QUERIES (one per namespace per round)")
            cold_bucket = LatencyBucket(label="cold")
            cold_first_touch = LatencyBucket(label="cold_first_touch")
            cold_qpn = query_cfg["cold"]["queries_per_namespace"]
            for round_idx in range(cold_qpn):
                for ns_idx, ns in enumerate(namespaces):
                    lat = query_single(
                        client, base_url, collection_name, ns, dimension,
                        query_cfg["top_k"],
                        query_cfg["search_strategy"],
                        query_cfg["include_metadata"],
                        query_cfg["include_values"],
                        rng,
                    )
                    cold_bucket.latencies_ms.append(lat)
                    if round_idx == 0:
                        cold_first_touch.latencies_ms.append(lat)
                if round_idx == 0:
                    print(f"  round 0 (first touch): {[f'{l:.1f}ms' for l in cold_first_touch.latencies_ms]}")
                elif (round_idx + 1) % 10 == 0:
                    print(f"  round {round_idx + 1}/{cold_qpn} done")
            results.cold = cold_first_touch
            print(f"  total queries: {cold_bucket.count} ({cold_first_touch.count} first-touch)")
            print()

        # Warm queries — namespaces are now cached from the cold phase
        if query_cfg["warm"]["enabled"]:
            print("WARM QUERIES")
            warm_bucket = LatencyBucket(label="warm")
            warm_qpn = query_cfg["warm"]["queries_per_namespace"]
            warmup = query_cfg["warm"]["warmup_queries"]
            for ns in namespaces:
                lats = run_queries(
                    client,
                    base_url,
                    collection_name,
                    ns,
                    dimension,
                    warm_qpn,
                    query_cfg["top_k"],
                    query_cfg["search_strategy"],
                    query_cfg["include_metadata"],
                    query_cfg["include_values"],
                    rng,
                    discard=warmup,
                )
                warm_bucket.latencies_ms.extend(lats)
                print(f"  {ns}: {len(lats)} queries (discarded first {warmup})")
            results.warm = warm_bucket
            print()

        # Cleanup
        if coll_cfg["cleanup"]:
            print("CLEANUP")
            try:
                delete_collection(client, base_url, collection_name)
            except Exception as e:
                print(f"  [warn] cleanup failed: {e}")
            print()

    return results


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


def print_report(results: BenchmarkResults, fmt: str = "text") -> None:
    cfg = results.config
    coll_cfg = cfg["collection"]
    ingest_cfg = cfg["ingest"]
    num_ns = ingest_cfg["num_namespaces"]

    print("=" * 60)
    print(f"  turbo-vector benchmark: profile")
    print(f"  collection: {results.collection_name}, dim: {coll_cfg['dimension']}, metric: {coll_cfg['metric']}")
    print(f"  namespaces: {num_ns}, vectors/ns: {ingest_cfg['vectors_per_namespace']} ({results.ingest.total_vectors if results.ingest else 0} total)")
    print("=" * 60)
    print()

    if results.ingest:
        print("INGEST")
        print(f"  total vectors ........ {results.ingest.total_vectors}")
        print(f"  total time ........... {results.ingest.total_time_s:.3f}s")
        print(f"  throughput ........... {results.ingest.throughput:.1f} vec/s")
        print()

    if results.cold and results.cold.count > 0:
        print(f"COLD QUERIES — FIRST TOUCH ({results.cold.count} total, 1 per namespace)")
        print(f"  p50 .................. {results.cold.p50:.3f} ms")
        print(f"  p90 .................. {results.cold.p90:.3f} ms")
        print(f"  p99 .................. {results.cold.p99:.3f} ms")
        print(f"  mean ................. {results.cold.mean:.3f} ms")
        print()

    if results.warm and results.warm.count > 0:
        warm_qpn = cfg["query"]["warm"]["queries_per_namespace"]
        warmup = cfg["query"]["warm"]["warmup_queries"]
        effective = warm_qpn - warmup
        print(f"WARM QUERIES ({effective}/ns x {num_ns} ns = {results.warm.count} total)")
        print(f"  p50 .................. {results.warm.p50:.3f} ms")
        print(f"  p90 .................. {results.warm.p90:.3f} ms")
        print(f"  p99 .................. {results.warm.p99:.3f} ms")
        print(f"  mean ................. {results.warm.mean:.3f} ms")
        print()

    print("=" * 60)

    if fmt == "json":
        out = {
            "collection": results.collection_name,
            "dimension": coll_cfg["dimension"],
            "metric": coll_cfg["metric"],
            "namespaces": num_ns,
        }
        if results.ingest:
            out["ingest"] = {
                "total_vectors": results.ingest.total_vectors,
                "total_time_s": round(results.ingest.total_time_s, 3),
                "throughput_vps": round(results.ingest.throughput, 1),
            }
        for bucket_name, bucket in [("cold", results.cold), ("warm", results.warm)]:
            if bucket and bucket.count > 0:
                out[bucket_name] = {
                    "count": bucket.count,
                    "p50_ms": round(bucket.p50, 3),
                    "p90_ms": round(bucket.p90, 3),
                    "p99_ms": round(bucket.p99, 3),
                    "mean_ms": round(bucket.mean, 3),
                }
        print()
        print(json.dumps(out, indent=2))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="turbo-vector profiling benchmark")
    parser.add_argument(
        "--config",
        type=str,
        default="benchmarks/profiles/default.toml",
        help="path to TOML config file",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="output format",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    results = run_benchmark(cfg)
    print_report(results, fmt=args.format)


if __name__ == "__main__":
    main()
