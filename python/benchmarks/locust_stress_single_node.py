# /// script
# dependencies = ["locust", "fire", "httpx"]
# ///
import itertools
import os
import random
import sys
import threading
import time
from typing import Any

import fire
import httpx
from locust import HttpUser, between, task, events
from locust.main import main as locust_main


def env(key: str, default: Any) -> Any:
    val = os.getenv(f"TV_LOCUST_{key}")
    if val is None:
        return default
    if isinstance(default, bool):
        normalized = val.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
        return default
    if isinstance(default, int):
        return int(val)
    if isinstance(default, float):
        return float(val)
    return type(default)(val)


DEFAULT_QUERY_WEIGHT = 9
DEFAULT_UPSERT_WEIGHT = 1
DEFAULT_WAIT_FOR_SEED_APPLY = True
DEFAULT_WAIT_FOR_LIVE_APPLY = False
DEFAULT_OPERATION_WAIT_TIMEOUT_S = 60.0
DEFAULT_OPERATION_WAIT_POLL_INTERVAL_S = 0.25

DEFAULT_DIMENSION = 128
DEFAULT_COLLECTION = f"locust-single-node-{random.randint(1000, 9999)}"
DEFAULT_NUM_NAMESPACES = 100
DEFAULT_NAMESPACE_PREFIX = "user"
DEFAULT_METRIC = "cosine"

DEFAULT_SEED_VECTORS = 50
DEFAULT_SEED_BATCH_SIZE = 200

DEFAULT_TOP_K = 10
DEFAULT_SEARCH_STRATEGY = "ann"
DEFAULT_UPSERT_BATCH_SIZE = 1

DEFAULT_ACTIVE_WEIGHT = 20
DEFAULT_ACTIVE_WAIT_MIN = 1.0
DEFAULT_ACTIVE_WAIT_MAX = 2.0

DEFAULT_CASUAL_WEIGHT = 80
DEFAULT_CASUAL_WAIT_MIN = 5.0
DEFAULT_CASUAL_WAIT_MAX = 10.0

DEFAULT_HOST = "http://127.0.0.1:8080"
DEFAULT_USERS = 48
DEFAULT_SPAWN_RATE = 8.0
DEFAULT_RUN_TIME = "1m"
DEFAULT_ACTIVE_RATIO = 0.2

QUERY_TASK_WEIGHT = max(1, env("QUERY_WEIGHT", DEFAULT_QUERY_WEIGHT))
UPSERT_TASK_WEIGHT = max(0, env("UPSERT_WEIGHT", DEFAULT_UPSERT_WEIGHT))
WAIT_FOR_SEED_APPLY = env("WAIT_FOR_SEED_APPLY", DEFAULT_WAIT_FOR_SEED_APPLY)
WAIT_FOR_LIVE_APPLY = env("WAIT_FOR_LIVE_APPLY", DEFAULT_WAIT_FOR_LIVE_APPLY)
OPERATION_WAIT_TIMEOUT_S = max(1.0, env("OPERATION_WAIT_TIMEOUT_S", DEFAULT_OPERATION_WAIT_TIMEOUT_S))
OPERATION_WAIT_POLL_INTERVAL_S = max(0.05, env("OPERATION_WAIT_POLL_INTERVAL_S", DEFAULT_OPERATION_WAIT_POLL_INTERVAL_S))


@events.init.add_listener
def on_init(environment, **kwargs):
    environment.collection_created = False
    environment.seed_lock = threading.Lock()
    environment.id_cnt = itertools.count()
    environment.seeded_namespaces = set()


class TurboVectorUser(HttpUser):
    abstract = True

    def on_start(self):
        self.rng = random.Random(time.time_ns() ^ id(self))
        self.dim = env("DIMENSION", DEFAULT_DIMENSION)
        self.coll = env("COLLECTION", DEFAULT_COLLECTION)

        num_ns = max(1, env("NUM_NAMESPACES", DEFAULT_NUM_NAMESPACES))
        user_idx = next(self.environment.id_cnt)
        self.ns = f"{env('NAMESPACE_PREFIX', DEFAULT_NAMESPACE_PREFIX)}-{user_idx % num_ns}"

        self._ensure_seeded()

    def _collection_upsert_path(self) -> str:
        return f"/v1/collections/{self.coll}/vectors/upsert"

    def _collection_query_path(self) -> str:
        return f"/v1/collections/{self.coll}/vectors/query"

    def _new_vector(self) -> list[float]:
        return [self.rng.uniform(-1, 1) for _ in range(self.dim)]

    def _operation_status_path(self, operation_id: str) -> str:
        return f"/v1/collections/{self.coll}/operations/{operation_id}"

    def _wait_for_operation_applied(self, operation_id: str, op_name: str) -> None:
        deadline = time.monotonic() + OPERATION_WAIT_TIMEOUT_S
        while time.monotonic() < deadline:
            with self.client.get(
                self._operation_status_path(operation_id),
                name=op_name,
                catch_response=True,
            ) as response:
                if response.status_code == 200:
                    try:
                        payload = response.json()
                    except ValueError as exc:
                        response.failure(f"operation status parse failed: {exc}")
                        raise RuntimeError(
                            f"operation status parse failed for {operation_id}"
                        ) from exc
                    status = payload.get("status")
                    if status == "applied":
                        response.success()
                        return
                    response.success()
                elif response.status_code == 404:
                    # Accepted writes can take a moment to become visible in operation status.
                    response.success()
                else:
                    response.failure(
                        f"operation status failed: status={response.status_code} body={response.text}"
                    )
            time.sleep(OPERATION_WAIT_POLL_INTERVAL_S)
        raise RuntimeError(
            f"operation {operation_id} did not reach applied status in {OPERATION_WAIT_TIMEOUT_S}s"
        )

    def _ensure_seeded(self):
        with self.environment.seed_lock:
            if not self.environment.collection_created:
                payload = {"name": self.coll, "dimension": self.dim, "metric": DEFAULT_METRIC}
                with self.client.post(
                    "/v1/collections",
                    json=payload,
                    name="setup:create_collection",
                    catch_response=True,
                ) as response:
                    if response.status_code not in {200, 201, 409}:
                        response.failure(
                            f"create collection failed: status={response.status_code} body={response.text}"
                        )
                        raise RuntimeError(
                            f"create collection failed: status={response.status_code} body={response.text}"
                        )
                    response.success()
                self.environment.collection_created = True

            if self.ns in self.environment.seeded_namespaces:
                return

            count = env("SEED_VECTORS", DEFAULT_SEED_VECTORS)
            batch_size = max(1, env("SEED_BATCH_SIZE", DEFAULT_SEED_BATCH_SIZE))
            if count > 0:
                seed_rng = random.Random(self.ns)
                for i in range(0, count, batch_size):
                    vectors = [
                        {
                            "id": f"seed-{self.ns}-{j}",
                            "values": [seed_rng.uniform(-1, 1) for _ in range(self.dim)],
                            "metadata": {"source": "seed", "ns": self.ns},
                        }
                        for j in range(i, min(i + batch_size, count))
                    ]
                    payload = {"namespace": self.ns, "vectors": vectors}
                    with self.client.post(
                        self._collection_upsert_path(),
                        json=payload,
                        name="setup:seed_upsert",
                        catch_response=True,
                    ) as response:
                        if response.status_code not in {200, 202}:
                            response.failure(
                                f"seed upsert failed: status={response.status_code} body={response.text}"
                            )
                            raise RuntimeError(
                                f"seed upsert failed: status={response.status_code} body={response.text}"
                            )
                        operation_id = ""
                        if response.status_code == 202:
                            try:
                                operation_id = response.json().get("operation_id", "")
                            except ValueError:
                                operation_id = ""
                        response.success()
                    if WAIT_FOR_SEED_APPLY and operation_id:
                        self._wait_for_operation_applied(
                            operation_id, "setup:seed_wait_applied"
                        )

            self.environment.seeded_namespaces.add(self.ns)

    @task(QUERY_TASK_WEIGHT)
    def query(self):
        req = {
            "namespace": self.ns,
            "vector": self._new_vector(),
            "top_k": env("TOP_K", DEFAULT_TOP_K),
            "search_strategy": env("SEARCH_STRATEGY", DEFAULT_SEARCH_STRATEGY),
        }
        with self.client.post(
            self._collection_query_path(), json=req, name="query", catch_response=True
        ) as response:
            if response.status_code != 200:
                response.failure(
                    f"query failed: status={response.status_code} body={response.text}"
                )
            else:
                response.success()

    @task(max(1, UPSERT_TASK_WEIGHT))
    def upsert(self):
        if UPSERT_TASK_WEIGHT == 0:
            return
        batch = max(1, env("UPSERT_BATCH_SIZE", DEFAULT_UPSERT_BATCH_SIZE))
        vectors = [
            {
                "id": f"live-{int(time.time() * 1000)}-{self.rng.randint(0, 1000000)}",
                "values": self._new_vector(),
                "metadata": {"source": "live", "ns": self.ns},
            }
            for _ in range(batch)
        ]
        req = {"namespace": self.ns, "vectors": vectors}
        with self.client.post(
            self._collection_upsert_path(), json=req, name="upsert", catch_response=True
        ) as response:
            if response.status_code not in {200, 202}:
                response.failure(
                    f"upsert failed: status={response.status_code} body={response.text}"
                )
            else:
                operation_id = ""
                if response.status_code == 202:
                    try:
                        operation_id = response.json().get("operation_id", "")
                    except ValueError:
                        operation_id = ""
                response.success()
                if WAIT_FOR_LIVE_APPLY and operation_id:
                    self._wait_for_operation_applied(
                        operation_id, "upsert:wait_applied"
                    )


class ActiveUser(TurboVectorUser):
    """Simulates a highly active user with low wait times."""

    weight = env("ACTIVE_WEIGHT", DEFAULT_ACTIVE_WEIGHT)
    wait_time = between(env("ACTIVE_WAIT_MIN", DEFAULT_ACTIVE_WAIT_MIN), env("ACTIVE_WAIT_MAX", DEFAULT_ACTIVE_WAIT_MAX))


class CasualUser(TurboVectorUser):
    """Simulates a normal user with longer gaps between actions."""

    weight = env("CASUAL_WEIGHT", DEFAULT_CASUAL_WEIGHT)
    wait_time = between(env("CASUAL_WAIT_MIN", DEFAULT_CASUAL_WAIT_MIN), env("CASUAL_WAIT_MAX", DEFAULT_CASUAL_WAIT_MAX))


def run(
    host: str = env("HOST", DEFAULT_HOST),
    users: int = env("USERS", DEFAULT_USERS),
    spawn_rate: float = env("SPAWN_RATE", DEFAULT_SPAWN_RATE),
    run_time: str = env("RUN_TIME", DEFAULT_RUN_TIME),
    num_namespaces: int = env("NUM_NAMESPACES", DEFAULT_NUM_NAMESPACES),
    active_ratio: float = env("ACTIVE_RATIO", DEFAULT_ACTIVE_RATIO),
    web: bool = False,
    **kwargs,
):
    """
    Run Locust stress workload against Turbo Vector with multiple user profiles and namespaces.
    """
    os.environ["TV_LOCUST_HOST"] = host
    os.environ["TV_LOCUST_NUM_NAMESPACES"] = str(num_namespaces)
    os.environ["TV_LOCUST_ACTIVE_WEIGHT"] = str(int(active_ratio * 100))
    os.environ["TV_LOCUST_CASUAL_WEIGHT"] = str(int((1 - active_ratio) * 100))

    for k, v in kwargs.items():
        os.environ[f"TV_LOCUST_{k.upper()}"] = str(v)

    try:
        httpx.get(f"{host}/health", timeout=2.0).raise_for_status()
    except Exception as e:
        print(f"Preflight failed: {e}")
        return

    sys.argv = ["locust", "-f", __file__, "--host", host]
    if not web:
        sys.argv.extend(
            ["--headless", "-u", str(users), "-r", str(spawn_rate), "-t", run_time]
        )
    locust_main()


if __name__ == "__main__":
    fire.Fire(run)
