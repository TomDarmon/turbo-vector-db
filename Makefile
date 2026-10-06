SHELL := /bin/sh
COMPOSE_NETWORK_NAME ?= turbo-vector-net
RUNTIME_OTLP_ENDPOINT ?= http://host.docker.internal:4318
RUNTIME_OTEL_SAMPLE_RATIO ?= 1.0
API_HEALTH_URL ?= http://127.0.0.1:8080/health
API_BASE_URL ?= http://127.0.0.1:8080
COLLECTOR_HEALTH_URL ?= http://127.0.0.1:13133
SIGNOZ_HEALTH_URL ?= http://127.0.0.1:3301/api/v1/health
NAMESPACE ?= default
STORAGE ?= rustfs
LOCAL_RUNTIME_DIR ?= local
LOCAL_API_PID_FILE ?= $(LOCAL_RUNTIME_DIR)/api.pid
LOCAL_BROKER_PID_FILE ?= $(LOCAL_RUNTIME_DIR)/broker.pid
LOCAL_WORKER_PID_FILE ?= $(LOCAL_RUNTIME_DIR)/worker.pid
LOCAL_API_LOG_FILE ?= $(LOCAL_RUNTIME_DIR)/api.log
LOCAL_BROKER_LOG_FILE ?= $(LOCAL_RUNTIME_DIR)/broker.log
LOCAL_WORKER_LOG_FILE ?= $(LOCAL_RUNTIME_DIR)/worker.log
LOCAL_BROKER_HEALTH_URL ?= http://127.0.0.1:8091/health
LOCAL_RUSTFS_ENDPOINT ?= http://127.0.0.1:9000

.PHONY: help doctor bootstrap rust-build rust-test rust-test-fts-bench rust-test-ann-bench rust-test-distributed-bench rust-test-strict-bench rust-test-queue rust-fmt rust-fmt-check rust-clippy rust-clippy-strict python-test fts-reindex compose-network-up runtime-build runtime-build-api runtime-up runtime-up-otel runtime-down runtime-restart runtime-logs runtime-ps viz-up viz-down viz-logs local-up local-down local-ps local-logs local-restart-api local-restart-broker local-restart-worker local-start-api local-start-broker local-start-worker local-stop-api local-stop-broker local-stop-worker local-clean-stale-pids local-check-ports local-ensure-runtime-dir wait-broker obs-up obs-up-host obs-down obs-logs obs-ps wait-api wait-collector wait-signoz dashboards-generate dashboards-sync bench bench-profile dev-up dev-down dev-logs dev-ps smoke reset-local

help:
	@echo "turbo-vector commands"
	@echo "  make doctor         # check local prerequisites"
	@echo "  make bootstrap      # create .env if missing"
	@echo
	@echo "  make runtime-build  # build runtime image with cache"
	@echo "  make runtime-build-api # build only API image fast"
	@echo "  make runtime-up [STORAGE=rustfs|s3|gcs] # start runtime stack (default rustfs)"
	@echo "  make runtime-up-otel # start runtime stack with OTEL export enabled"
	@echo "  make runtime-restart # restart runtime containers without rebuild"
	@echo "  make runtime-down   # stop runtime stack"
	@echo "  make runtime-logs   # follow runtime logs"
	@echo "  make runtime-ps     # list runtime containers"
	@echo "  make viz-up         # start optional frontend tutorial service (profile: viz)"
	@echo "  make viz-down       # stop optional frontend tutorial service"
	@echo "  make viz-logs       # follow frontend tutorial logs"
	@echo "  make local-up       # start rustfs in docker + host broker/api/worker"
	@echo "  make local-down     # stop local host processes + rustfs services"
	@echo "  make local-ps       # show local host process status"
	@echo "  make local-logs     # tail local host process logs"
	@echo "  make local-restart-api # restart host api process"
	@echo "  make local-restart-broker # restart host broker process"
	@echo "  make local-restart-worker # restart host worker process"
	@echo
	@echo "  make obs-up         # start SigNoz observability stack"
	@echo "  make obs-up-host    # start observability + host exporters"
	@echo "  make obs-down       # stop observability stack"
	@echo "  make obs-logs       # follow observability logs"
	@echo "  make obs-ps         # list observability containers"
	@echo
	@echo "  make dev-up         # start observability, then runtime with OTEL"
	@echo "  make dev-down       # stop runtime, then observability"
	@echo "  make dev-logs       # show recent runtime + observability logs"
	@echo "  make dev-ps         # list both runtime and observability containers"
	@echo
	@echo "  make dashboards-sync # generate + sync SigNoz dashboards"
	@echo "  make bench          # run canonical locust traffic profile"
	@echo "  make bench-profile  # run profiling benchmark (cold/warm latencies)"
	@echo
	@echo "  make rust-build     # build rust api locally"
	@echo "  make rust-test      # run rust tests locally"
	@echo "  make rust-test-fts-bench # run strict FTS benchmark profile gates"
	@echo "  make rust-test-ann-bench # run strict ANN v3 benchmark profile gates"
	@echo "  make rust-test-distributed-bench # run strict distributed retrieval benchmark gates"
	@echo "  make rust-test-strict-bench # run all strict benchmark profile gates"
	@echo "  make rust-test-queue # run queue crate tests locally"
	@echo "  make rust-fmt       # format rust workspace"
	@echo "  make rust-fmt-check # check rust formatting"
	@echo "  make rust-clippy    # run clippy for rust workspace"
	@echo "  make rust-clippy-strict # fail on rust warnings (includes dead code)"
	@echo "  make python-test    # run python tooling tests"
	@echo "  make fts-reindex COLLECTION=<name> [NAMESPACE=default] [SCHEMA_FILE=path] # run lexical reindex workflow"
	@echo "  make smoke          # health + runtime checks"
	@echo "  make reset-local    # reset local rustfs data"

doctor:
	@for cmd in git python3 uv pre-commit rustc cargo docker; do \
		if command -v $$cmd >/dev/null 2>&1; then \
			echo "[ok] $$cmd"; \
		else \
			echo "[missing] $$cmd"; \
		fi; \
	done
	@if command -v docker >/dev/null 2>&1; then \
		if docker info >/dev/null 2>&1; then \
			echo "[ok] docker daemon access"; \
		else \
			echo "[warn] docker daemon access failed"; \
			user_groups=" $$(id -nG) "; \
			if [ "$$(id -u)" -ne 0 ] && [ "$${user_groups#* docker }" = "$$user_groups" ]; then \
				echo "       run: sudo usermod -aG docker $$USER"; \
				echo "       then open a new login shell"; \
			else \
				echo "       check that Docker daemon is running"; \
			fi; \
		fi; \
	fi

bootstrap:
	@if [ ! -f .env ]; then cp .env.example .env; echo "created .env from .env.example"; else echo ".env already exists"; fi

rust-build:
	cargo build --manifest-path rust/Cargo.toml -p turbo-vector-api

rust-test:
	cargo test --manifest-path rust/Cargo.toml

rust-test-fts-bench:
	cargo test --manifest-path rust/Cargo.toml -p turbo-vector-api "fts::lexical_benchmarks::" -- --nocapture
	cargo test --manifest-path rust/Cargo.toml -p turbo-vector-api "fts::bench_profiles::" -- --nocapture

rust-test-ann-bench:
	cargo test --manifest-path rust/Cargo.toml -p turbo-vector-api "tests::ann_strict_benchmarks::" -- --nocapture

rust-test-distributed-bench:
	cargo test --manifest-path rust/Cargo.toml -p turbo-vector-api "tests::distributed_strict_benchmarks::" -- --nocapture

rust-test-strict-bench:
	@$(MAKE) rust-test-fts-bench
	@$(MAKE) rust-test-ann-bench
	@$(MAKE) rust-test-distributed-bench

rust-test-queue:
	cargo test --manifest-path rust/Cargo.toml -p turbo-vector-queue

rust-fmt:
	cargo fmt --manifest-path rust/Cargo.toml --all

rust-fmt-check:
	cargo fmt --manifest-path rust/Cargo.toml --all -- --check

rust-clippy:
	cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets

rust-clippy-strict:
	cargo clippy --manifest-path rust/Cargo.toml --workspace --all-targets -- -D warnings

python-test:
	cd python && uv run python -m unittest discover -s tests -p "test_*.py"

fts-reindex:
	@if [ -z "$(COLLECTION)" ]; then \
		echo "[error] COLLECTION is required (example: make fts-reindex COLLECTION=my_collection)"; \
		exit 1; \
	fi
	@if [ -n "$(SCHEMA_FILE)" ]; then \
		python3 python/scripts/fts_reindex_namespace.py --base-url "$(API_BASE_URL)" --collection "$(COLLECTION)" --namespace "$(NAMESPACE)" --schema-file "$(SCHEMA_FILE)"; \
	else \
		python3 python/scripts/fts_reindex_namespace.py --base-url "$(API_BASE_URL)" --collection "$(COLLECTION)" --namespace "$(NAMESPACE)"; \
	fi

compose-network-up:
	@docker network inspect $(COMPOSE_NETWORK_NAME) >/dev/null 2>&1 || docker network create $(COMPOSE_NETWORK_NAME)

runtime-build:
	DOCKER_BUILDKIT=1 docker compose build api broker upsert-worker

runtime-build-api:
	DOCKER_BUILDKIT=1 docker compose build api

runtime-up: compose-network-up
	@case "$(STORAGE)" in \
		rustfs) \
			TV_STORAGE_PROVIDER=s3 TV_STORAGE_ENDPOINT=http://rustfs:9000 TV_STORAGE_REGION=us-east-1 docker compose up -d rustfs rustfs-init broker api upsert-worker ;; \
		s3) \
			TV_STORAGE_PROVIDER=s3 docker compose up -d broker api upsert-worker ;; \
		gcs) \
			TV_STORAGE_PROVIDER=gcs docker compose up -d broker api upsert-worker ;; \
		*) \
			echo "[error] STORAGE must be one of: rustfs, s3, gcs"; exit 1 ;; \
	esac

runtime-up-otel: compose-network-up
	@case "$(STORAGE)" in \
		rustfs) \
			TV_STORAGE_PROVIDER=s3 TV_STORAGE_ENDPOINT=http://rustfs:9000 TV_STORAGE_REGION=us-east-1 TV_OTEL_ENABLED=true TV_OTEL_EXPORTER_OTLP_ENDPOINT=$${TV_OTEL_EXPORTER_OTLP_ENDPOINT:-$(RUNTIME_OTLP_ENDPOINT)} TV_OTEL_SAMPLE_RATIO=$${TV_OTEL_SAMPLE_RATIO:-$(RUNTIME_OTEL_SAMPLE_RATIO)} docker compose up -d rustfs rustfs-init broker api upsert-worker ;; \
		s3) \
			TV_STORAGE_PROVIDER=s3 TV_OTEL_ENABLED=true TV_OTEL_EXPORTER_OTLP_ENDPOINT=$${TV_OTEL_EXPORTER_OTLP_ENDPOINT:-$(RUNTIME_OTLP_ENDPOINT)} TV_OTEL_SAMPLE_RATIO=$${TV_OTEL_SAMPLE_RATIO:-$(RUNTIME_OTEL_SAMPLE_RATIO)} docker compose up -d broker api upsert-worker ;; \
		gcs) \
			TV_STORAGE_PROVIDER=gcs TV_STORAGE_ENDPOINT=https://storage.googleapis.com TV_STORAGE_REGION=auto TV_OTEL_ENABLED=true TV_OTEL_EXPORTER_OTLP_ENDPOINT=$${TV_OTEL_EXPORTER_OTLP_ENDPOINT:-$(RUNTIME_OTLP_ENDPOINT)} TV_OTEL_SAMPLE_RATIO=$${TV_OTEL_SAMPLE_RATIO:-$(RUNTIME_OTEL_SAMPLE_RATIO)} docker compose up -d broker api upsert-worker ;; \
		*) \
			echo "[error] STORAGE must be one of: rustfs, s3, gcs"; exit 1 ;; \
	esac

runtime-down:
	docker compose down

runtime-restart:
	docker compose restart api broker upsert-worker

runtime-logs:
	docker compose logs -f --tail=200

runtime-ps:
	docker compose ps

viz-up: compose-network-up
	TV_VIZ_ENABLED=true docker compose --profile viz up -d --build frontend

viz-down:
	docker compose --profile viz stop frontend
	docker compose --profile viz rm -f frontend

viz-logs:
	docker compose --profile viz logs -f --tail=200 frontend

local-ensure-runtime-dir:
	@mkdir -p $(LOCAL_RUNTIME_DIR)

local-clean-stale-pids: local-ensure-runtime-dir
	@for pid_file in "$(LOCAL_BROKER_PID_FILE)" "$(LOCAL_API_PID_FILE)" "$(LOCAL_WORKER_PID_FILE)"; do \
		if [ ! -f "$$pid_file" ]; then \
			continue; \
		fi; \
		pid=$$(cat "$$pid_file" 2>/dev/null || true); \
		if [ -n "$$pid" ] && kill -0 "$$pid" >/dev/null 2>&1; then \
			echo "[error] process already running for $$pid_file (pid $$pid); use make local-down first"; \
			exit 1; \
		fi; \
		rm -f "$$pid_file"; \
		echo "[ok] removed stale pid file $$pid_file"; \
	done

local-check-ports:
	@for port in 8080 8091; do \
		if ! python3 -c "import socket,sys; s=socket.socket(); s.settimeout(0.2); code=s.connect_ex(('127.0.0.1', int(sys.argv[1]))); s.close(); raise SystemExit(0 if code != 0 else 1)" "$$port"; then \
			echo "[error] local runtime port $$port is already in use"; \
			exit 1; \
		fi; \
	done

local-start-broker: local-ensure-runtime-dir
	@pid_file="$(LOCAL_BROKER_PID_FILE)"; \
	log_file="$(LOCAL_BROKER_LOG_FILE)"; \
	if [ -f "$$pid_file" ]; then \
		pid=$$(cat "$$pid_file" 2>/dev/null || true); \
		if [ -n "$$pid" ] && kill -0 "$$pid" >/dev/null 2>&1; then \
			echo "[error] broker already running (pid $$pid)"; \
			exit 1; \
		fi; \
		rm -f "$$pid_file"; \
	fi; \
	nohup env \
		TV_PROCESS_ROLE=broker \
		TV_BIND_ADDR=127.0.0.1:8091 \
		TV_STORAGE_PROVIDER=s3 \
		TV_STORAGE_ENDPOINT=$(LOCAL_RUSTFS_ENDPOINT) \
		TV_STORAGE_REGION=us-east-1 \
		TV_STORAGE_BUCKET="$${TV_STORAGE_BUCKET:-turbo-vector-dev}" \
		TV_STORAGE_ACCESS_KEY="$${TV_STORAGE_ACCESS_KEY:-turboadmin}" \
		TV_STORAGE_SECRET_KEY="$${TV_STORAGE_SECRET_KEY:-turbosecret}" \
		cargo run --manifest-path rust/Cargo.toml -p turbo-vector-api >"$$log_file" 2>&1 & \
	pid=$$!; \
	echo "$$pid" >"$$pid_file"; \
	sleep 1; \
	if ! kill -0 "$$pid" >/dev/null 2>&1; then \
		echo "[error] broker failed to start; see $$log_file"; \
		rm -f "$$pid_file"; \
		exit 1; \
	fi; \
	echo "[ok] broker started (pid $$pid)"

local-start-api: local-ensure-runtime-dir
	@pid_file="$(LOCAL_API_PID_FILE)"; \
	log_file="$(LOCAL_API_LOG_FILE)"; \
	if [ -f "$$pid_file" ]; then \
		pid=$$(cat "$$pid_file" 2>/dev/null || true); \
		if [ -n "$$pid" ] && kill -0 "$$pid" >/dev/null 2>&1; then \
			echo "[error] api already running (pid $$pid)"; \
			exit 1; \
		fi; \
		rm -f "$$pid_file"; \
	fi; \
	nohup env \
		TV_PROCESS_ROLE=api \
		TV_BIND_ADDR=127.0.0.1:8080 \
		TV_QUEUE_BROKER_URL=http://127.0.0.1:8091 \
		TV_STORAGE_PROVIDER=s3 \
		TV_STORAGE_ENDPOINT=$(LOCAL_RUSTFS_ENDPOINT) \
		TV_STORAGE_REGION=us-east-1 \
		TV_STORAGE_BUCKET="$${TV_STORAGE_BUCKET:-turbo-vector-dev}" \
		TV_STORAGE_ACCESS_KEY="$${TV_STORAGE_ACCESS_KEY:-turboadmin}" \
		TV_STORAGE_SECRET_KEY="$${TV_STORAGE_SECRET_KEY:-turbosecret}" \
		cargo run --manifest-path rust/Cargo.toml -p turbo-vector-api >"$$log_file" 2>&1 & \
	pid=$$!; \
	echo "$$pid" >"$$pid_file"; \
	sleep 1; \
	if ! kill -0 "$$pid" >/dev/null 2>&1; then \
		echo "[error] api failed to start; see $$log_file"; \
		rm -f "$$pid_file"; \
		exit 1; \
	fi; \
	echo "[ok] api started (pid $$pid)"

local-start-worker: local-ensure-runtime-dir
	@pid_file="$(LOCAL_WORKER_PID_FILE)"; \
	log_file="$(LOCAL_WORKER_LOG_FILE)"; \
	if [ -f "$$pid_file" ]; then \
		pid=$$(cat "$$pid_file" 2>/dev/null || true); \
		if [ -n "$$pid" ] && kill -0 "$$pid" >/dev/null 2>&1; then \
			echo "[error] worker already running (pid $$pid)"; \
			exit 1; \
		fi; \
		rm -f "$$pid_file"; \
	fi; \
	nohup env \
		TV_PROCESS_ROLE=worker \
		TV_QUEUE_BROKER_URL=http://127.0.0.1:8091 \
		TV_STORAGE_PROVIDER=s3 \
		TV_STORAGE_ENDPOINT=$(LOCAL_RUSTFS_ENDPOINT) \
		TV_STORAGE_REGION=us-east-1 \
		TV_STORAGE_BUCKET="$${TV_STORAGE_BUCKET:-turbo-vector-dev}" \
		TV_STORAGE_ACCESS_KEY="$${TV_STORAGE_ACCESS_KEY:-turboadmin}" \
		TV_STORAGE_SECRET_KEY="$${TV_STORAGE_SECRET_KEY:-turbosecret}" \
		cargo run --manifest-path rust/Cargo.toml -p turbo-vector-api >"$$log_file" 2>&1 & \
	pid=$$!; \
	echo "$$pid" >"$$pid_file"; \
	sleep 1; \
	if ! kill -0 "$$pid" >/dev/null 2>&1; then \
		echo "[error] worker failed to start; see $$log_file"; \
		rm -f "$$pid_file"; \
		exit 1; \
	fi; \
	echo "[ok] worker started (pid $$pid)"

local-stop-broker:
	@pid_file="$(LOCAL_BROKER_PID_FILE)"; \
	if [ ! -f "$$pid_file" ]; then \
		echo "[ok] broker not running"; \
		exit 0; \
	fi; \
	pid=$$(cat "$$pid_file" 2>/dev/null || true); \
	if [ -z "$$pid" ]; then \
		rm -f "$$pid_file"; \
		echo "[ok] broker pid file cleared"; \
		exit 0; \
	fi; \
	if kill -0 "$$pid" >/dev/null 2>&1; then \
		kill "$$pid" >/dev/null 2>&1 || true; \
		for _ in 1 2 3 4 5; do \
			if kill -0 "$$pid" >/dev/null 2>&1; then \
				sleep 1; \
			else \
				break; \
			fi; \
		done; \
		if kill -0 "$$pid" >/dev/null 2>&1; then \
			kill -9 "$$pid" >/dev/null 2>&1 || true; \
		fi; \
		echo "[ok] broker stopped"; \
	else \
		echo "[ok] broker pid file was stale"; \
	fi; \
	rm -f "$$pid_file"

local-stop-api:
	@pid_file="$(LOCAL_API_PID_FILE)"; \
	if [ ! -f "$$pid_file" ]; then \
		echo "[ok] api not running"; \
		exit 0; \
	fi; \
	pid=$$(cat "$$pid_file" 2>/dev/null || true); \
	if [ -z "$$pid" ]; then \
		rm -f "$$pid_file"; \
		echo "[ok] api pid file cleared"; \
		exit 0; \
	fi; \
	if kill -0 "$$pid" >/dev/null 2>&1; then \
		kill "$$pid" >/dev/null 2>&1 || true; \
		for _ in 1 2 3 4 5; do \
			if kill -0 "$$pid" >/dev/null 2>&1; then \
				sleep 1; \
			else \
				break; \
			fi; \
		done; \
		if kill -0 "$$pid" >/dev/null 2>&1; then \
			kill -9 "$$pid" >/dev/null 2>&1 || true; \
		fi; \
		echo "[ok] api stopped"; \
	else \
		echo "[ok] api pid file was stale"; \
	fi; \
	rm -f "$$pid_file"

local-stop-worker:
	@pid_file="$(LOCAL_WORKER_PID_FILE)"; \
	if [ ! -f "$$pid_file" ]; then \
		echo "[ok] worker not running"; \
		exit 0; \
	fi; \
	pid=$$(cat "$$pid_file" 2>/dev/null || true); \
	if [ -z "$$pid" ]; then \
		rm -f "$$pid_file"; \
		echo "[ok] worker pid file cleared"; \
		exit 0; \
	fi; \
	if kill -0 "$$pid" >/dev/null 2>&1; then \
		kill "$$pid" >/dev/null 2>&1 || true; \
		for _ in 1 2 3 4 5; do \
			if kill -0 "$$pid" >/dev/null 2>&1; then \
				sleep 1; \
			else \
				break; \
			fi; \
		done; \
		if kill -0 "$$pid" >/dev/null 2>&1; then \
			kill -9 "$$pid" >/dev/null 2>&1 || true; \
		fi; \
		echo "[ok] worker stopped"; \
	else \
		echo "[ok] worker pid file was stale"; \
	fi; \
	rm -f "$$pid_file"

wait-broker:
	@echo "waiting for broker health at $(LOCAL_BROKER_HEALTH_URL)"
	@attempt=0; \
	while [ $$attempt -lt 60 ]; do \
		if curl -fsS "$(LOCAL_BROKER_HEALTH_URL)" >/dev/null 2>&1; then \
			echo "[ok] broker healthy"; \
			exit 0; \
		fi; \
		attempt=$$((attempt + 1)); \
		sleep 1; \
	done; \
	echo "[error] broker did not become healthy at $(LOCAL_BROKER_HEALTH_URL)" >&2; \
	exit 1

local-up: compose-network-up local-clean-stale-pids local-check-ports
	@docker compose up -d rustfs rustfs-init
	@echo "waiting for rustfs on 127.0.0.1:9000"
	@attempt=0; \
	while [ $$attempt -lt 60 ]; do \
		if python3 -c "import socket; s=socket.socket(); s.settimeout(0.5); code=s.connect_ex(('127.0.0.1', 9000)); s.close(); raise SystemExit(0 if code == 0 else 1)"; then \
			echo "[ok] rustfs reachable"; \
			break; \
		fi; \
		attempt=$$((attempt + 1)); \
		sleep 1; \
	done; \
	if [ $$attempt -ge 60 ]; then \
		echo "[error] rustfs is not reachable on 127.0.0.1:9000" >&2; \
		exit 1; \
	fi
	@$(MAKE) local-start-broker
	@$(MAKE) wait-broker
	@$(MAKE) local-start-api
	@$(MAKE) local-start-worker
	@$(MAKE) wait-api

local-down:
	@$(MAKE) local-stop-worker
	@$(MAKE) local-stop-api
	@$(MAKE) local-stop-broker
	@docker compose stop rustfs rustfs-init >/dev/null 2>&1 || true
	@echo "[ok] local runtime stopped"

local-ps:
	@for service in broker api worker; do \
		pid_file="$(LOCAL_RUNTIME_DIR)/$$service.pid"; \
		if [ ! -f "$$pid_file" ]; then \
			echo "$$service: stopped (no pid file)"; \
			continue; \
		fi; \
		pid=$$(cat "$$pid_file" 2>/dev/null || true); \
		if [ -z "$$pid" ]; then \
			echo "$$service: stopped (empty pid file)"; \
			continue; \
		fi; \
		if kill -0 "$$pid" >/dev/null 2>&1; then \
			echo "$$service: running (pid $$pid)"; \
		else \
			echo "$$service: stopped (stale pid $$pid)"; \
		fi; \
	done

local-logs: local-ensure-runtime-dir
	@touch "$(LOCAL_BROKER_LOG_FILE)" "$(LOCAL_API_LOG_FILE)" "$(LOCAL_WORKER_LOG_FILE)"
	@tail -n 200 -f "$(LOCAL_BROKER_LOG_FILE)" "$(LOCAL_API_LOG_FILE)" "$(LOCAL_WORKER_LOG_FILE)"

local-restart-api:
	@$(MAKE) local-stop-api
	@$(MAKE) local-start-api
	@$(MAKE) wait-api

local-restart-broker:
	@$(MAKE) local-stop-broker
	@$(MAKE) local-start-broker
	@$(MAKE) wait-broker

local-restart-worker:
	@$(MAKE) local-stop-worker
	@$(MAKE) local-start-worker

obs-up: compose-network-up
	docker compose -f docker-compose.observability.yml up -d
	@$(MAKE) wait-collector
	@$(MAKE) wait-signoz

obs-up-host: compose-network-up
	docker compose -f docker-compose.observability.yml --profile host-metrics up -d
	@$(MAKE) wait-collector
	@$(MAKE) wait-signoz

obs-down:
	docker compose -f docker-compose.observability.yml down

obs-logs:
	docker compose -f docker-compose.observability.yml logs -f --tail=200

obs-ps:
	docker compose -f docker-compose.observability.yml ps

wait-api:
	@echo "waiting for API health at $(API_HEALTH_URL)"
	@attempt=0; \
	while [ $$attempt -lt 60 ]; do \
		if curl -fsS "$(API_HEALTH_URL)" >/dev/null 2>&1; then \
			echo "[ok] API healthy"; \
			exit 0; \
		fi; \
		attempt=$$((attempt + 1)); \
		sleep 2; \
	done; \
	echo "[error] API did not become healthy at $(API_HEALTH_URL)" >&2; \
	exit 1

wait-collector:
	@echo "waiting for collector health at $(COLLECTOR_HEALTH_URL)"
	@attempt=0; \
	while [ $$attempt -lt 90 ]; do \
		if curl -fsS "$(COLLECTOR_HEALTH_URL)" >/dev/null 2>&1; then \
			echo "[ok] collector healthy"; \
			exit 0; \
		fi; \
		attempt=$$((attempt + 1)); \
		sleep 2; \
	done; \
	echo "[error] collector did not become healthy at $(COLLECTOR_HEALTH_URL)" >&2; \
	exit 1

wait-signoz:
	@echo "waiting for SigNoz health at $(SIGNOZ_HEALTH_URL)"
	@attempt=0; \
	while [ $$attempt -lt 90 ]; do \
		if curl -fsS "$(SIGNOZ_HEALTH_URL)" >/dev/null 2>&1; then \
			echo "[ok] SigNoz healthy"; \
			exit 0; \
		fi; \
		attempt=$$((attempt + 1)); \
		sleep 2; \
	done; \
	echo "[error] SigNoz did not become healthy at $(SIGNOZ_HEALTH_URL)" >&2; \
	exit 1

dashboards-generate:
	uv run observability/signoz/generate_dashboards.py

dashboards-sync: dashboards-generate
	. .env && uv run observability/signoz/sync_dashboards.py

bench:
	cd python && uv run python benchmarks/locust_stress_single_node.py

BENCH_PROFILE ?= python/benchmarks/profiles/default.toml

bench-profile:
	cd python && uv run python benchmarks/bench_profile.py --config ../$(BENCH_PROFILE)

dev-up:
	@$(MAKE) obs-up
	@$(MAKE) runtime-up-otel
	@$(MAKE) wait-api

dev-down:
	@$(MAKE) runtime-down
	@$(MAKE) obs-down

dev-logs:
	docker compose logs --tail=200
	docker compose -f docker-compose.observability.yml logs --tail=200

dev-ps:
	docker compose ps
	docker compose -f docker-compose.observability.yml ps

smoke:
	curl -fsS http://localhost:8080/health
	@echo
	curl -fsS http://localhost:8080/v1/system/runtime
	@echo

reset-local:
	docker compose down -v --remove-orphans
