mod ann;
mod broker_routes;
mod distributed;
mod error;
mod filters;
mod fts;
mod keys;
mod models;
mod observability;
mod queue_broker;
mod queue_store_adapter;
mod routes;
mod scoring;
mod state;
mod storage_logic;
mod telemetry;
mod validation;
mod worker;

#[cfg(test)]
mod tests;

use anyhow::Context;
use std::{env, net::SocketAddr, sync::Arc, time::Duration};
use tracing::{debug, info, warn};
use turbo_vector_storage::{
    build_object_store, CloudObjectStoreConfig, CloudObjectStoreProvider, GcsConfig, S3Config,
};

use crate::{
    broker_routes::broker_router,
    filters::parser::FilterParserLimits,
    queue_broker::{HttpQueueBrokerClient, QueueBrokerClient, UnavailableQueueBrokerClient},
    routes::app_router,
    state::{AppState, FilterCacheConfig, RuntimeConfigResponse},
    telemetry::TelemetryConfig,
    worker::run_upsert_worker,
};

const DEFAULT_WAL_WORKER_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_WAL_WORKER_COLLECTION_REFRESH_INTERVAL_MS: u64 = 5_000;
const DEFAULT_WAL_WORKER_ADAPTIVE_BACKLOG_THRESHOLD: usize = 256;
const DEFAULT_WAL_WORKER_ADAPTIVE_FLUSH_INTERVAL_MS: u64 = 1_000;
const DEFAULT_BROKER_REQUEUE_INTERVAL_MS: u64 = 1_000;
const DEFAULT_FILTER_CLUSTER_CACHE_MAX_ENTRIES: usize = 8_192;
const DEFAULT_FILTER_CLUSTER_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_FILTER_ROW_CACHE_MAX_ENTRIES: usize = 65_536;
const DEFAULT_FILTER_ROW_CACHE_MAX_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_ANN_TREE_ROOT_BEAM: usize = 4;
const DEFAULT_ANN_TREE_LEAF_PROBE_COUNT: usize = 16;
const DEFAULT_ANN_OBJECT_READ_BUDGET: usize = 512;
const DEFAULT_ANN_QUANTIZATION_BOUND_MARGIN: f32 = 16.0;
const DEFAULT_ANN_RERANK_PRUNE_RATIO: f32 = 0.05;
const DEFAULT_ANN_RERANK_MAX_CANDIDATES: usize = 2_048;
const DEFAULT_ANN_RERANK_SSD_CACHE_DIR: &str = "/tmp/turbo-vector-ann-rerank-cache";
const DEFAULT_ANN_RERANK_SSD_CACHE_MAX_ENTRIES: usize = 100_000;
const DEFAULT_ANN_RERANK_SSD_CACHE_MAX_BYTES: usize = 512 * 1024 * 1024;
const DEFAULT_NAMESPACE_CACHE_MAX_ENTRIES: usize = 4_096;
const DEFAULT_NAMESPACE_CACHE_MAX_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_ANN_BUCKET_CACHE_MAX_ENTRIES: usize = 32_768;
const DEFAULT_ANN_BUCKET_CACHE_MAX_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_FILTER_MAX_REGEX_BYTES: usize = 512;
const DEFAULT_FILTER_MAX_GLOB_BYTES: usize = 512;
const DEFAULT_FILTER_MAX_WIDEN_PASSES: usize = 4;
const DEFAULT_FTS_BLOCK_TARGET_POSTINGS: usize = 256;
const DEFAULT_FTS_BLOCK_SPLIT_THRESHOLD: usize = 512;
const DEFAULT_FTS_BLOCK_MERGE_THRESHOLD: usize = 128;
const DEFAULT_FTS_MAX_TERM_BLOCKS_TOUCHED_PER_DOC_UPDATE: usize = 8;
const DEFAULT_FTS_PREFIX_MAX_INDEX_CHARS: usize = 8;
const DEFAULT_FTS_PREFIX_MAX_EXPANSIONS: usize = 64;
const DEFAULT_FTS_PREFIX_MAX_EXPANSION_BYTES: usize = 4 * 1024;
const DEFAULT_FTS_EXPLAIN_MAX_TOP_K: usize = 100;
const DEFAULT_DISTRIBUTED_SHARD_COUNT: usize = 1;
const DEFAULT_DISTRIBUTED_SHARD_TIMEOUT_MS: u64 = 250;
const DEFAULT_DISTRIBUTED_FAIL_OPEN: bool = true;
const DEFAULT_API_BODY_LIMIT_BYTES: usize = 32 * 1024 * 1024; // 32MB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessRole {
    Api,
    Worker,
    Broker,
}

impl ProcessRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            ProcessRole::Api => "api",
            ProcessRole::Worker => "worker",
            ProcessRole::Broker => "broker",
        }
    }
}

fn parse_collection_from_metadata_key(key: &str) -> Option<&str> {
    let prefix = "collections/";
    let suffix = "/metadata.json";
    if !key.starts_with(prefix) || !key.ends_with(suffix) {
        return None;
    }
    let collection = &key[prefix.len()..key.len().saturating_sub(suffix.len())];
    if collection.is_empty() || collection.contains('/') {
        return None;
    }
    Some(collection)
}

async fn run_broker_requeue_scan(state: AppState, requeue_interval: Duration) {
    let requeue_interval = requeue_interval.max(Duration::from_millis(1));
    loop {
        tokio::time::sleep(requeue_interval).await;
        let collections = match state.storage.list_prefix("collections/").await {
            Ok(keys) => keys
                .iter()
                .filter_map(|key| parse_collection_from_metadata_key(key).map(ToString::to_string))
                .collect::<Vec<_>>(),
            Err(error) => {
                warn!(
                    error = ?error,
                    "broker requeue scan failed to list collections"
                );
                continue;
            }
        };
        if collections.is_empty() {
            continue;
        }
        for collection in collections {
            let queue = state.wal_queue_handle(&collection).await;
            match queue
                .requeue_expired(state.wal_queue_lease_timeout_ms())
                .await
            {
                Ok(released) if released > 0 => {
                    crate::telemetry::increment_queue_requeue(&state.service_name, released as u64);
                    debug!(collection, released, "broker requeued stale queue leases");
                }
                Ok(_) => {}
                Err(error) => {
                    warn!(
                        collection,
                        error = ?error,
                        "broker requeue scan failed for collection"
                    );
                }
            }
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let process_role = env_process_role("TV_PROCESS_ROLE", ProcessRole::Api)?;
    let default_otel_service_name = format!("turbo-vector-{}", process_role.as_str());
    let otel_enabled = env_bool_or("TV_OTEL_ENABLED", false);
    let otel_exporter_otlp_endpoint = env_or(
        "TV_OTEL_EXPORTER_OTLP_ENDPOINT",
        "http://host.docker.internal:4318",
    );
    let otel_service_name = env_or("TV_OTEL_SERVICE_NAME", &default_otel_service_name);
    let otel_metric_export_interval_ms = env_u64_or("TV_OTEL_METRIC_EXPORT_INTERVAL_MS", 5_000);
    let otel_sample_ratio = env_f64_or("TV_OTEL_SAMPLE_RATIO", 0.05);
    let telemetry_runtime = crate::telemetry::init_telemetry(TelemetryConfig::with_defaults(
        otel_enabled,
        otel_exporter_otlp_endpoint,
        otel_service_name,
        otel_metric_export_interval_ms,
        otel_sample_ratio,
    ))?;

    let bind_addr = env_or("TV_BIND_ADDR", "0.0.0.0:8080");
    let api_body_limit_bytes =
        env_usize_or("TV_API_BODY_LIMIT_BYTES", DEFAULT_API_BODY_LIMIT_BYTES).max(1);
    let storage_provider_raw = env_or("TV_STORAGE_PROVIDER", CloudObjectStoreProvider::S3.as_str());
    let storage_provider = storage_provider_raw
        .parse::<CloudObjectStoreProvider>()
        .map_err(|error| {
            anyhow::anyhow!("invalid TV_STORAGE_PROVIDER='{storage_provider_raw}': {error}")
        })?;
    let storage_endpoint = env_or("TV_STORAGE_ENDPOINT", storage_provider.default_endpoint());
    let storage_region = env_or("TV_STORAGE_REGION", storage_provider.default_region());
    let storage_bucket = env_or("TV_STORAGE_BUCKET", "turbo-vector-dev");
    let storage_simulated_latency_ms = env_u64_or("TV_STORAGE_SIMULATED_LATENCY_MS", 0);
    let wal_queue_background_drain_requested = env_bool_or("TV_WAL_QUEUE_BACKGROUND_DRAIN", false);
    if wal_queue_background_drain_requested {
        warn!(
            "TV_WAL_QUEUE_BACKGROUND_DRAIN is no longer supported; run a dedicated TV_PROCESS_ROLE=worker process"
        );
    }
    let wal_queue_background_drain_enabled = false;
    let wal_queue_background_drain_interval_ms = env_u64_or("TV_WAL_QUEUE_DRAIN_INTERVAL_MS", 50);
    let query_ann_enabled = env_bool_or("TV_QUERY_ANN_ENABLED", true);
    let wal_worker_flush_interval_ms = env_u64("TV_WAL_WORKER_FLUSH_INTERVAL_MS")
        .or_else(|| env_u64("TV_WAL_WORKER_POLL_INTERVAL_MS"))
        .unwrap_or(DEFAULT_WAL_WORKER_FLUSH_INTERVAL_MS);
    let wal_worker_collection_refresh_interval_ms = env_u64_or(
        "TV_WAL_WORKER_COLLECTION_REFRESH_INTERVAL_MS",
        DEFAULT_WAL_WORKER_COLLECTION_REFRESH_INTERVAL_MS,
    );
    let wal_worker_adaptive_backlog_threshold = env_usize_or(
        "TV_WAL_WORKER_ADAPTIVE_BACKLOG_THRESHOLD",
        DEFAULT_WAL_WORKER_ADAPTIVE_BACKLOG_THRESHOLD,
    );
    let wal_worker_adaptive_flush_interval_ms = env_u64_or(
        "TV_WAL_WORKER_ADAPTIVE_FLUSH_INTERVAL_MS",
        DEFAULT_WAL_WORKER_ADAPTIVE_FLUSH_INTERVAL_MS,
    );
    let wal_worker_build_fts = env_bool_or("TV_WAL_WORKER_BUILD_FTS", true);
    let wal_worker_build_ann = env_bool_or("TV_WAL_WORKER_BUILD_ANN", true);
    let queue_broker_url = env_or("TV_QUEUE_BROKER_URL", "http://127.0.0.1:8091");
    let broker_requeue_interval_ms = env_u64_or(
        "TV_BROKER_REQUEUE_INTERVAL_MS",
        DEFAULT_BROKER_REQUEUE_INTERVAL_MS,
    );
    let ann_bucket_fetch_concurrency = env_usize_or("TV_ANN_BUCKET_FETCH_CONCURRENCY", 64).max(1);
    let ann_tree_root_beam =
        env_usize_or("TV_ANN_TREE_ROOT_BEAM", DEFAULT_ANN_TREE_ROOT_BEAM).max(1);
    let ann_tree_leaf_probe_count = env_usize_or(
        "TV_ANN_TREE_LEAF_PROBE_COUNT",
        DEFAULT_ANN_TREE_LEAF_PROBE_COUNT,
    )
    .max(1);
    let ann_object_read_budget =
        env_usize_or("TV_ANN_OBJECT_READ_BUDGET", DEFAULT_ANN_OBJECT_READ_BUDGET).max(1);
    let ann_quantization_bound_margin = env_f64_or(
        "TV_ANN_QUANTIZATION_BOUND_MARGIN",
        DEFAULT_ANN_QUANTIZATION_BOUND_MARGIN as f64,
    )
    .max(0.0) as f32;
    let ann_rerank_prune_ratio = env_f64_or(
        "TV_ANN_RERANK_PRUNE_RATIO",
        DEFAULT_ANN_RERANK_PRUNE_RATIO as f64,
    )
    .clamp(0.01, 1.0) as f32;
    let ann_rerank_max_candidates = env_usize_or(
        "TV_ANN_RERANK_MAX_CANDIDATES",
        DEFAULT_ANN_RERANK_MAX_CANDIDATES,
    )
    .max(1);
    let ann_rerank_ssd_cache_dir = env_or(
        "TV_ANN_RERANK_SSD_CACHE_DIR",
        DEFAULT_ANN_RERANK_SSD_CACHE_DIR,
    );
    let ann_rerank_ssd_cache_max_entries = env_usize_or(
        "TV_ANN_RERANK_SSD_CACHE_MAX_ENTRIES",
        DEFAULT_ANN_RERANK_SSD_CACHE_MAX_ENTRIES,
    )
    .max(1);
    let ann_rerank_ssd_cache_max_bytes = env_usize_or(
        "TV_ANN_RERANK_SSD_CACHE_MAX_BYTES",
        DEFAULT_ANN_RERANK_SSD_CACHE_MAX_BYTES,
    )
    .max(1);
    let namespace_cache_max_entries = env_usize_or(
        "TV_NAMESPACE_CACHE_MAX_ENTRIES",
        DEFAULT_NAMESPACE_CACHE_MAX_ENTRIES,
    )
    .max(1);
    let namespace_cache_max_bytes = env_usize_or(
        "TV_NAMESPACE_CACHE_MAX_BYTES",
        DEFAULT_NAMESPACE_CACHE_MAX_BYTES,
    )
    .max(1);
    let ann_bucket_cache_max_entries = env_usize_or(
        "TV_ANN_BUCKET_CACHE_MAX_ENTRIES",
        DEFAULT_ANN_BUCKET_CACHE_MAX_ENTRIES,
    )
    .max(1);
    let ann_bucket_cache_max_bytes = env_usize_or(
        "TV_ANN_BUCKET_CACHE_MAX_BYTES",
        DEFAULT_ANN_BUCKET_CACHE_MAX_BYTES,
    )
    .max(1);
    let filter_cluster_cache_max_entries = env_usize_or(
        "TV_FILTER_CLUSTER_CACHE_MAX_ENTRIES",
        DEFAULT_FILTER_CLUSTER_CACHE_MAX_ENTRIES,
    )
    .max(1);
    let filter_cluster_cache_max_bytes = env_usize_or(
        "TV_FILTER_CLUSTER_CACHE_MAX_BYTES",
        DEFAULT_FILTER_CLUSTER_CACHE_MAX_BYTES,
    )
    .max(1);
    let filter_row_cache_max_entries = env_usize_or(
        "TV_FILTER_ROW_CACHE_MAX_ENTRIES",
        DEFAULT_FILTER_ROW_CACHE_MAX_ENTRIES,
    )
    .max(1);
    let filter_row_cache_max_bytes = env_usize_or(
        "TV_FILTER_ROW_CACHE_MAX_BYTES",
        DEFAULT_FILTER_ROW_CACHE_MAX_BYTES,
    )
    .max(1);
    let filter_max_regex_bytes =
        env_usize_or("TV_FILTER_MAX_REGEX_BYTES", DEFAULT_FILTER_MAX_REGEX_BYTES).max(1);
    let filter_max_glob_bytes =
        env_usize_or("TV_FILTER_MAX_GLOB_BYTES", DEFAULT_FILTER_MAX_GLOB_BYTES).max(1);
    let filter_max_widen_passes = env_usize_or(
        "TV_FILTER_MAX_WIDEN_PASSES",
        DEFAULT_FILTER_MAX_WIDEN_PASSES,
    )
    .max(1);
    let fts_block_target_postings = env_usize_or(
        "TV_FTS_BLOCK_TARGET_POSTINGS",
        DEFAULT_FTS_BLOCK_TARGET_POSTINGS,
    )
    .max(1);
    let fts_block_split_threshold = env_usize_or(
        "TV_FTS_BLOCK_SPLIT_THRESHOLD",
        DEFAULT_FTS_BLOCK_SPLIT_THRESHOLD,
    )
    .max(2);
    let fts_block_merge_threshold = env_usize_or(
        "TV_FTS_BLOCK_MERGE_THRESHOLD",
        DEFAULT_FTS_BLOCK_MERGE_THRESHOLD,
    )
    .max(1)
    .min(fts_block_split_threshold.saturating_sub(1));
    let fts_max_term_blocks_touched_per_doc_update = env_usize_or(
        "TV_FTS_MAX_TERM_BLOCKS_TOUCHED_PER_DOC_UPDATE",
        DEFAULT_FTS_MAX_TERM_BLOCKS_TOUCHED_PER_DOC_UPDATE,
    )
    .max(1);
    let fts_enable_delta_rebalance = env_bool_or("TV_FTS_ENABLE_DELTA_REBALANCE", true);
    let fts_prefix_max_index_chars = env_usize_or(
        "TV_FTS_PREFIX_MAX_INDEX_CHARS",
        DEFAULT_FTS_PREFIX_MAX_INDEX_CHARS,
    )
    .max(1);
    let fts_prefix_max_expansions = env_usize_or(
        "TV_FTS_PREFIX_MAX_EXPANSIONS",
        DEFAULT_FTS_PREFIX_MAX_EXPANSIONS,
    )
    .max(1);
    let fts_prefix_max_expansion_bytes = env_usize_or(
        "TV_FTS_PREFIX_MAX_EXPANSION_BYTES",
        DEFAULT_FTS_PREFIX_MAX_EXPANSION_BYTES,
    )
    .max(64);
    let fts_explain_query_enabled = env_bool_or("TV_FTS_EXPLAIN_QUERY_ENABLED", true);
    let fts_explain_max_top_k =
        env_usize_or("TV_FTS_EXPLAIN_MAX_TOP_K", DEFAULT_FTS_EXPLAIN_MAX_TOP_K).max(1);
    let distributed_shard_count = env_usize_or(
        "TV_DISTRIBUTED_SHARD_COUNT",
        DEFAULT_DISTRIBUTED_SHARD_COUNT,
    )
    .max(1);
    let distributed_shard_timeout_ms = env_u64_or(
        "TV_DISTRIBUTED_SHARD_TIMEOUT_MS",
        DEFAULT_DISTRIBUTED_SHARD_TIMEOUT_MS,
    )
    .max(1);
    let distributed_fail_open =
        env_bool_or("TV_DISTRIBUTED_FAIL_OPEN", DEFAULT_DISTRIBUTED_FAIL_OPEN);
    let viz_enabled = env_bool_or("TV_VIZ_ENABLED", false);
    let distributed_required_successful_shards = env_usize_or(
        "TV_DISTRIBUTED_REQUIRED_SUCCESSFUL_SHARDS",
        distributed_shard_count.saturating_sub(1).max(1),
    )
    .clamp(1, distributed_shard_count);

    let queue_client: Arc<dyn QueueBrokerClient> = match process_role {
        ProcessRole::Api | ProcessRole::Worker => {
            Arc::new(HttpQueueBrokerClient::new(queue_broker_url.clone()))
        }
        ProcessRole::Broker => Arc::new(UnavailableQueueBrokerClient::new(
            "broker role does not issue outbound queue requests",
        )),
    };

    let storage = build_object_store(match storage_provider {
        CloudObjectStoreProvider::S3 => CloudObjectStoreConfig::S3(S3Config {
            endpoint: storage_endpoint.clone(),
            region: storage_region.clone(),
            bucket: storage_bucket.clone(),
            access_key: env_or("TV_STORAGE_ACCESS_KEY", "turboadmin"),
            secret_key: env_or("TV_STORAGE_SECRET_KEY", "turbosecret"),
            simulated_latency_ms: storage_simulated_latency_ms,
        }),
        CloudObjectStoreProvider::Gcs => CloudObjectStoreConfig::Gcs(GcsConfig {
            endpoint: storage_endpoint.clone(),
            region: storage_region.clone(),
            bucket: storage_bucket.clone(),
            simulated_latency_ms: storage_simulated_latency_ms,
        }),
    })
    .await
    .with_context(|| {
        format!(
            "failed to initialize {} object store",
            storage_provider.as_str()
        )
    })?;

    let state = AppState {
        node_id: env_or("HOSTNAME", "turbo-vector-api"),
        service_name: telemetry_runtime.service_name.clone(),
        runtime: RuntimeConfigResponse {
            bind_addr: bind_addr.clone(),
            api_body_limit_bytes,
            storage_provider: storage_provider.as_str().to_string(),
            storage_endpoint,
            storage_region,
            storage_bucket,
            storage_simulated_latency_ms,
            wal_queue_background_drain_enabled,
            wal_queue_background_drain_interval_ms,
            queue_broker_url: queue_broker_url.clone(),
            wal_worker_build_fts,
            wal_worker_build_ann,
            query_ann_enabled,
            ann_bucket_fetch_concurrency,
            ann_tree_root_beam,
            ann_tree_leaf_probe_count,
            ann_object_read_budget,
            ann_quantization_bound_margin,
            ann_rerank_prune_ratio,
            ann_rerank_max_candidates,
            ann_rerank_ssd_cache_dir: ann_rerank_ssd_cache_dir.clone(),
            ann_rerank_ssd_cache_max_entries,
            ann_rerank_ssd_cache_max_bytes,
            namespace_cache_max_entries,
            namespace_cache_max_bytes,
            ann_bucket_cache_max_entries,
            ann_bucket_cache_max_bytes,
            filter_cluster_cache_max_entries,
            filter_cluster_cache_max_bytes,
            filter_row_cache_max_entries,
            filter_row_cache_max_bytes,
            filter_max_regex_bytes,
            filter_max_glob_bytes,
            filter_max_widen_passes,
            fts_block_target_postings,
            fts_block_split_threshold,
            fts_block_merge_threshold,
            fts_max_term_blocks_touched_per_doc_update,
            fts_enable_delta_rebalance,
            fts_prefix_max_index_chars,
            fts_prefix_max_expansions,
            fts_prefix_max_expansion_bytes,
            fts_explain_query_enabled,
            fts_explain_max_top_k,
            distributed_shard_count,
            distributed_shard_timeout_ms,
            distributed_fail_open,
            distributed_required_successful_shards,
            viz_enabled,
            otel_enabled: telemetry_runtime.enabled,
            otel_exporter_otlp_endpoint: telemetry_runtime.exporter_otlp_endpoint.clone(),
            otel_service_name: telemetry_runtime.service_name.clone(),
            otel_metric_export_interval_ms: telemetry_runtime.metric_export_interval_ms,
            otel_sample_ratio: telemetry_runtime.sample_ratio,
            filter_cache_metrics: Default::default(),
        },
        storage,
        queue_client,
        manifest_write_locks: AppState::new_manifest_lock_registry(),
        ann_index_build_locks: AppState::new_ann_index_lock_registry(),
        fts_index_build_locks: AppState::new_fts_index_lock_registry(),
        wal_queue_handles: AppState::new_wal_queue_registry(),
        wal_queue_drain_workers: AppState::new_wal_queue_drain_worker_registry(),
        query_ann_enabled,
        ann_bucket_fetch_semaphore: AppState::new_ann_bucket_fetch_semaphore(
            ann_bucket_fetch_concurrency,
        ),
        ann_tree_root_beam,
        ann_tree_leaf_probe_count,
        ann_object_read_budget,
        ann_quantization_bound_margin,
        ann_rerank_prune_ratio,
        ann_rerank_max_candidates,
        distributed_shard_count,
        distributed_shard_timeout_ms,
        distributed_fail_open,
        distributed_required_successful_shards,
        filter_parser_limits: FilterParserLimits {
            max_regex_bytes: filter_max_regex_bytes,
            max_glob_bytes: filter_max_glob_bytes,
        },
        filter_max_widen_passes,
        namespace_vector_cache: AppState::new_namespace_cache(FilterCacheConfig::new(
            namespace_cache_max_entries,
            namespace_cache_max_bytes,
        )),
        ann_bucket_cache: AppState::new_ann_bucket_cache(FilterCacheConfig::new(
            ann_bucket_cache_max_entries,
            ann_bucket_cache_max_bytes,
        )),
        ann_rerank_ssd_cache: AppState::new_ann_rerank_ssd_cache(
            FilterCacheConfig::new(
                ann_rerank_ssd_cache_max_entries,
                ann_rerank_ssd_cache_max_bytes,
            ),
            ann_rerank_ssd_cache_dir.clone(),
        ),
        filter_cluster_cache: AppState::new_filter_cluster_cache(FilterCacheConfig::new(
            filter_cluster_cache_max_entries,
            filter_cluster_cache_max_bytes,
        )),
        filter_row_cache: AppState::new_filter_row_cache(FilterCacheConfig::new(
            filter_row_cache_max_entries,
            filter_row_cache_max_bytes,
        )),
        manifest_cache: AppState::new_manifest_cache(),
        collection_metadata_cache: AppState::new_collection_metadata_cache(),
        fts_index_meta_cache: AppState::new_fts_index_meta_cache(),
        fts_doc_lookup_cache: AppState::new_fts_doc_lookup_cache(),
        shard_placement_cache: AppState::new_shard_placement_cache(),
        ensured_collection_registry_markers: AppState::new_collection_registry_marker_cache(),
    };
    state.emit_cache_telemetry_snapshot().await;

    info!(
        process_role = process_role.as_str(),
        wal_worker_flush_interval_ms,
        wal_worker_collection_refresh_interval_ms,
        wal_worker_adaptive_backlog_threshold,
        wal_worker_adaptive_flush_interval_ms,
        wal_worker_build_fts,
        wal_worker_build_ann,
        queue_broker_url,
        broker_requeue_interval_ms,
        ann_tree_root_beam,
        ann_tree_leaf_probe_count,
        ann_object_read_budget,
        ann_quantization_bound_margin,
        ann_rerank_prune_ratio,
        ann_rerank_max_candidates,
        ann_rerank_ssd_cache_dir,
        ann_rerank_ssd_cache_max_entries,
        ann_rerank_ssd_cache_max_bytes,
        namespace_cache_max_entries,
        namespace_cache_max_bytes,
        ann_bucket_cache_max_entries,
        ann_bucket_cache_max_bytes,
        filter_cluster_cache_max_entries,
        filter_cluster_cache_max_bytes,
        filter_row_cache_max_entries,
        filter_row_cache_max_bytes,
        filter_max_regex_bytes,
        filter_max_glob_bytes,
        filter_max_widen_passes,
        fts_block_target_postings,
        fts_block_split_threshold,
        fts_block_merge_threshold,
        fts_max_term_blocks_touched_per_doc_update,
        fts_enable_delta_rebalance,
        fts_prefix_max_index_chars,
        fts_prefix_max_expansions,
        fts_prefix_max_expansion_bytes,
        fts_explain_query_enabled,
        fts_explain_max_top_k,
        distributed_shard_count,
        distributed_shard_timeout_ms,
        distributed_fail_open,
        distributed_required_successful_shards,
        viz_enabled,
        otel_enabled = telemetry_runtime.enabled,
        otel_exporter_otlp_endpoint = telemetry_runtime.exporter_otlp_endpoint,
        otel_service_name = telemetry_runtime.service_name,
        otel_metric_export_interval_ms = telemetry_runtime.metric_export_interval_ms,
        otel_sample_ratio = telemetry_runtime.sample_ratio,
        storage_provider = storage_provider.as_str(),
        storage_simulated_latency_ms,
        "starting turbo-vector process"
    );

    match process_role {
        ProcessRole::Api => {
            let app = app_router(state);
            let addr: SocketAddr = bind_addr
                .parse()
                .with_context(|| format!("invalid TV_BIND_ADDR: {bind_addr}"))?;

            info!("starting turbo-vector-api on {addr}");
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
        }
        ProcessRole::Worker => {
            run_upsert_worker(
                state,
                Duration::from_millis(wal_worker_flush_interval_ms),
                Duration::from_millis(wal_worker_collection_refresh_interval_ms),
                wal_worker_adaptive_backlog_threshold,
                Duration::from_millis(wal_worker_adaptive_flush_interval_ms),
                wal_worker_build_fts,
                wal_worker_build_ann,
            )
            .await;
        }
        ProcessRole::Broker => {
            let requeue_interval = Duration::from_millis(broker_requeue_interval_ms);
            tokio::spawn(run_broker_requeue_scan(state.clone(), requeue_interval));
            let app = broker_router(state);
            let addr: SocketAddr = bind_addr
                .parse()
                .with_context(|| format!("invalid TV_BIND_ADDR: {bind_addr}"))?;

            info!("starting turbo-vector-broker on {addr}");
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
        }
    }
    Ok(())
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_bool_or(key: &str, default: bool) -> bool {
    let Ok(value) = env::var(key) else {
        return default;
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

fn env_u64_or(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str) -> Option<u64> {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
}

fn env_usize_or(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_f64_or(key: &str, default: f64) -> f64 {
    env::var(key)
        .ok()
        .and_then(|value| value.trim().parse::<f64>().ok())
        .unwrap_or(default)
}

pub(crate) fn env_process_role(key: &str, default: ProcessRole) -> anyhow::Result<ProcessRole> {
    let Ok(raw) = env::var(key) else {
        return Ok(default);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "api" => Ok(ProcessRole::Api),
        "worker" | "upsert-worker" => Ok(ProcessRole::Worker),
        "broker" => Ok(ProcessRole::Broker),
        "all" | "both" | "api+worker" => {
            anyhow::bail!(
                "{key}='{raw}' is unsupported: embedded api+worker mode is no longer supported; run separate api and worker services"
            )
        }
        _ => anyhow::bail!("invalid {key}='{raw}'; expected one of: api, worker, broker"),
    }
}
