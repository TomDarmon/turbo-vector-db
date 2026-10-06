use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use turbo_vector_queue::QueueFile;

use crate::{
    error::ApiError,
    models::{QueryRequest, QueryResponse},
    routes::query_vectors_inner,
    state::AppState,
    storage_logic::load_current_manifest,
    validation::{normalize_namespace, validate_collection_name},
};

#[derive(Debug, Deserialize)]
struct QueryExplainRequest {
    #[serde(flatten)]
    query: QueryRequest,
    #[serde(default)]
    scenario_tag: Option<String>,
}

#[derive(Debug, Serialize)]
struct QueryExplainResponse {
    query_response: QueryResponse,
    explain: ExplainPayload,
}

#[derive(Debug, Serialize)]
struct ExplainPayload {
    path: &'static str,
    temperature: &'static str,
    scenario_tag: Option<String>,
    steps: Vec<ExplainStep>,
    optimizations: Vec<&'static str>,
    fallback_reasons: Vec<String>,
    cache_summary: serde_json::Value,
    object_reads_summary: serde_json::Value,
    timings_ms: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ExplainStep {
    id: &'static str,
    title: &'static str,
    service: &'static str,
    detail: String,
    proof: serde_json::Value,
    duration_ms: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct StorageInventoryQuery {
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_sizes: Option<bool>,
}

#[derive(Debug, Serialize)]
struct StorageInventoryResponse {
    prefix: String,
    cursor: Option<String>,
    limit: usize,
    include_sizes: bool,
    items: Vec<StorageInventoryItem>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
struct StorageInventoryItem {
    key: String,
    category: &'static str,
    size_bytes: Option<usize>,
}

#[derive(Debug, Serialize)]
struct QueueSummaryResponse {
    collection: String,
    queue_depth: usize,
    pending_jobs: usize,
    leased_jobs: usize,
    oldest_job_age_ms: Option<u64>,
    max_attempt: u32,
    jobs: Vec<QueueJobPreview>,
}

#[derive(Debug, Serialize)]
struct QueueJobPreview {
    id: String,
    enqueued_at_ms: u64,
    age_ms: u64,
    leased: bool,
    attempt: u32,
    worker_id: Option<String>,
}

pub(crate) fn observability_router() -> Router<AppState> {
    Router::new()
        .route(
            "/v1/observability/collections/:name/query-explain",
            post(query_explain),
        )
        .route(
            "/v1/observability/collections/:name/queue",
            get(collection_queue_summary),
        )
        .route(
            "/v1/observability/storage/inventory",
            get(storage_inventory),
        )
}

fn ensure_viz_enabled(state: &AppState) -> Result<(), ApiError> {
    if state.runtime.viz_enabled {
        Ok(())
    } else {
        Err(ApiError::not_found(
            "observability tutorial is disabled (set TV_VIZ_ENABLED=true)",
        ))
    }
}

async fn query_explain(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(payload): Json<QueryExplainRequest>,
) -> Result<Json<QueryExplainResponse>, ApiError> {
    ensure_viz_enabled(&state)?;
    validate_collection_name(&collection)?;

    let namespace = normalize_namespace(payload.query.namespace.as_deref())?;
    let manifest = load_current_manifest(&state, &collection).await?;
    let pre_query_cache_warm = match manifest.as_ref() {
        Some(manifest) => state
            .get_namespace_cache_entry(&collection, &namespace)
            .await
            .is_some_and(|entry| entry.generation == manifest.generation),
        None => true,
    };

    let started = Instant::now();
    let result = query_vectors_inner(
        State(state.clone()),
        Path(collection.clone()),
        Json(payload.query.clone()),
    )
    .await?;
    let total_ms = started.elapsed().as_secs_f64() * 1_000.0;

    let query_response = result.0;
    let path = if query_response.distributed.is_some() {
        "distributed"
    } else if query_response.ann.ann_used {
        "ann"
    } else if query_response.ann.ann_fallback_count > 0 {
        "ann_fallback"
    } else {
        "exact"
    };

    let inferred_temperature = if query_response.ann.rerank_ssd_cache_misses > 0
        || query_response.ann.ann_meta_object_reads > 0
        || query_response.ann.ann_bucket_object_reads > 0
        || query_response.ann.rerank_segment_object_reads > 0
    {
        "cold"
    } else if path == "exact" {
        if pre_query_cache_warm {
            "warm"
        } else {
            "cold"
        }
    } else {
        "warm"
    };

    let mut steps = vec![ExplainStep {
        id: "request_received",
        title: "Request Received",
        service: "api",
        detail: format!(
            "query top_k={} namespace={} strategy={:?}",
            payload.query.top_k, namespace, payload.query.search_strategy
        ),
        proof: json!({
            "collection": collection,
            "namespace": namespace,
            "path": path,
        }),
        duration_ms: Some(0.0),
    }];

    steps.push(ExplainStep {
        id: "plan_decision",
        title: "Execution Path Decision",
        service: "api",
        detail: match path {
            "distributed" => "Distributed path selected with shard fan-out".to_string(),
            "ann" => "ANN path selected with binary first-stage and rerank".to_string(),
            "ann_fallback" => "ANN attempted then fallback to exact".to_string(),
            _ => "Exact path selected".to_string(),
        },
        proof: json!({
            "ann_used": query_response.ann.ann_used,
            "ann_fallback_count": query_response.ann.ann_fallback_count,
            "distributed": query_response.distributed.as_ref(),
            "fallback_reasons": &query_response.ann.fallback_reasons,
        }),
        duration_ms: None,
    });

    steps.push(ExplainStep {
        id: "cache_and_io",
        title: "Cache & Object I/O",
        service: "api",
        detail: format!(
            "temperature={} with rerank hits={} misses={}, ANN bucket reads={}",
            inferred_temperature,
            query_response.ann.rerank_ssd_cache_hits,
            query_response.ann.rerank_ssd_cache_misses,
            query_response.ann.ann_bucket_object_reads,
        ),
        proof: json!({
            "filter_cluster_cache_hits": query_response.ann.filter_cluster_cache_hits,
            "filter_cluster_cache_misses": query_response.ann.filter_cluster_cache_misses,
            "filter_row_cache_hits": query_response.ann.filter_row_cache_hits,
            "filter_row_cache_misses": query_response.ann.filter_row_cache_misses,
            "rerank_ssd_cache_hits": query_response.ann.rerank_ssd_cache_hits,
            "rerank_ssd_cache_misses": query_response.ann.rerank_ssd_cache_misses,
            "ann_meta_object_reads": query_response.ann.ann_meta_object_reads,
            "ann_bucket_object_reads": query_response.ann.ann_bucket_object_reads,
            "rerank_segment_object_reads": query_response.ann.rerank_segment_object_reads,
        }),
        duration_ms: Some(query_response.ann.rerank_ssd_fetch_latency_ms),
    });

    if query_response.ann.first_stage_candidate_count > 0
        || query_response.ann.rerank_candidate_count > 0
    {
        steps.push(ExplainStep {
            id: "ann_first_stage",
            title: "ANN First Stage + Rerank",
            service: "api",
            detail: format!(
                "first-stage candidates={} rerank candidates={} buckets probed={}",
                query_response.ann.first_stage_candidate_count,
                query_response.ann.rerank_candidate_count,
                query_response.ann.buckets_probed
            ),
            proof: json!({
                "first_stage_candidate_count": query_response.ann.first_stage_candidate_count,
                "rerank_candidate_count": query_response.ann.rerank_candidate_count,
                "buckets_probed": query_response.ann.buckets_probed,
                "quantization_bound_margin": query_response.ann.quantization_bound_margin,
                "quantization_bound_threshold": query_response.ann.quantization_bound_threshold,
            }),
            duration_ms: None,
        });
    }

    if let Some(distributed) = query_response.distributed.as_ref() {
        steps.push(ExplainStep {
            id: "distributed_merge",
            title: "Shard Merge",
            service: "api",
            detail: format!(
                "planned={} successful={} degraded={}",
                distributed.planned_shards, distributed.successful_shards, distributed.degraded
            ),
            proof: json!({
                "degradation_reasons": distributed.degradation_reasons,
                "shard_statuses": distributed.shard_statuses,
            }),
            duration_ms: None,
        });
    }

    steps.push(ExplainStep {
        id: "response",
        title: "Response Built",
        service: "api",
        detail: format!("{} matches returned", query_response.matches.len()),
        proof: json!({"match_count": query_response.matches.len()}),
        duration_ms: Some(total_ms),
    });

    let mut optimizations = Vec::new();
    if query_response.ann.ann_used {
        optimizations.push("ann_binary_first_stage");
        optimizations.push("ann_rerank");
    }
    if query_response.ann.filter_cluster_cache_hits > 0
        || query_response.ann.filter_row_cache_hits > 0
    {
        optimizations.push("filter_cache");
    }
    if query_response.ann.rerank_ssd_cache_hits > 0 {
        optimizations.push("rerank_ssd_cache");
    }
    if query_response.distributed.is_some() {
        optimizations.push("distributed_fanout_merge");
    }
    if query_response.ann.object_read_budget_exceeded {
        optimizations.push("object_read_budget_guard");
    }

    let explain = ExplainPayload {
        path,
        temperature: inferred_temperature,
        scenario_tag: payload.scenario_tag,
        steps,
        optimizations,
        fallback_reasons: query_response.ann.fallback_reasons.clone(),
        cache_summary: json!({
            "filter_cluster": {
                "hits": query_response.ann.filter_cluster_cache_hits,
                "misses": query_response.ann.filter_cluster_cache_misses,
                "evictions": query_response.ann.filter_cluster_cache_evictions,
            },
            "filter_row": {
                "hits": query_response.ann.filter_row_cache_hits,
                "misses": query_response.ann.filter_row_cache_misses,
                "evictions": query_response.ann.filter_row_cache_evictions,
            },
            "rerank_ssd": {
                "hits": query_response.ann.rerank_ssd_cache_hits,
                "misses": query_response.ann.rerank_ssd_cache_misses,
                "evictions": query_response.ann.rerank_ssd_cache_evictions,
            }
        }),
        object_reads_summary: json!({
            "ann_meta": {
                "reads": query_response.ann.ann_meta_object_reads,
                "bytes": query_response.ann.ann_meta_object_bytes,
            },
            "ann_bucket": {
                "reads": query_response.ann.ann_bucket_object_reads,
                "bytes": query_response.ann.ann_bucket_object_bytes,
            },
            "ann_filter_cluster": {
                "reads": query_response.ann.ann_filter_cluster_object_reads,
                "bytes": query_response.ann.ann_filter_cluster_object_bytes,
            },
            "ann_filter_row": {
                "reads": query_response.ann.ann_filter_row_object_reads,
                "bytes": query_response.ann.ann_filter_row_object_bytes,
            },
            "rerank_segment": {
                "reads": query_response.ann.rerank_segment_object_reads,
                "bytes": query_response.ann.rerank_segment_object_bytes,
            }
        }),
        timings_ms: json!({
            "query_total": total_ms,
            "rerank_ssd_fetch_latency_ms": query_response.ann.rerank_ssd_fetch_latency_ms,
        }),
    };

    Ok(Json(QueryExplainResponse {
        query_response,
        explain,
    }))
}

async fn collection_queue_summary(
    State(state): State<AppState>,
    Path(collection): Path<String>,
) -> Result<Json<QueueSummaryResponse>, ApiError> {
    ensure_viz_enabled(&state)?;
    validate_collection_name(&collection)?;

    let snapshot = state
        .queue_client()
        .snapshot(&collection)
        .await
        .map_err(|error| ApiError::store_unavailable(format!("queue broker error: {error}")))?;

    let response = summarize_queue(&collection, &snapshot);
    Ok(Json(response))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn summarize_queue(collection: &str, snapshot: &QueueFile) -> QueueSummaryResponse {
    let now = now_ms();
    let queue_depth = snapshot.jobs.len();
    let pending_jobs = snapshot
        .jobs
        .iter()
        .filter(|job| job.lease.is_none())
        .count();
    let leased_jobs = queue_depth.saturating_sub(pending_jobs);
    let oldest_job_age_ms = snapshot
        .jobs
        .iter()
        .map(|job| now.saturating_sub(job.enqueued_at_ms))
        .max();
    let max_attempt = snapshot
        .jobs
        .iter()
        .filter_map(|job| job.lease.as_ref().map(|lease| lease.attempt))
        .max()
        .unwrap_or(0);

    let jobs = snapshot
        .jobs
        .iter()
        .take(50)
        .map(|job| QueueJobPreview {
            id: job.id.clone(),
            enqueued_at_ms: job.enqueued_at_ms,
            age_ms: now.saturating_sub(job.enqueued_at_ms),
            leased: job.lease.is_some(),
            attempt: job.lease.as_ref().map(|lease| lease.attempt).unwrap_or(0),
            worker_id: job.lease.as_ref().map(|lease| lease.worker_id.clone()),
        })
        .collect();

    QueueSummaryResponse {
        collection: collection.to_string(),
        queue_depth,
        pending_jobs,
        leased_jobs,
        oldest_job_age_ms,
        max_attempt,
        jobs,
    }
}

async fn storage_inventory(
    State(state): State<AppState>,
    Query(query): Query<StorageInventoryQuery>,
) -> Result<Json<StorageInventoryResponse>, ApiError> {
    ensure_viz_enabled(&state)?;

    let prefix = query.prefix.unwrap_or_else(|| "collections/".to_string());
    let include_sizes = query.include_sizes.unwrap_or(false);
    let limit = query.limit.unwrap_or(200).clamp(1, 500);

    let mut keys = state.storage.list_prefix(&prefix).await.map_err(|error| {
        ApiError::store_unavailable(format!(
            "failed to list storage keys for prefix '{prefix}': {error:?}"
        ))
    })?;
    keys.sort();

    let start_index = match query.cursor.as_deref() {
        Some(cursor) => keys.partition_point(|key| key.as_str() <= cursor),
        None => 0,
    };

    let selected = keys
        .iter()
        .skip(start_index)
        .take(limit)
        .cloned()
        .collect::<Vec<_>>();
    let has_more = keys.len() > start_index.saturating_add(selected.len());
    let next_cursor = if has_more {
        selected.last().cloned()
    } else {
        None
    };

    let mut items = Vec::with_capacity(selected.len());
    for key in selected {
        let size_bytes = if include_sizes {
            state
                .storage
                .get_bytes(&key)
                .await
                .ok()
                .map(|bytes| bytes.len())
        } else {
            None
        };
        items.push(StorageInventoryItem {
            category: classify_storage_key(&key),
            key,
            size_bytes,
        });
    }

    Ok(Json(StorageInventoryResponse {
        prefix,
        cursor: query.cursor,
        limit,
        include_sizes,
        items,
        next_cursor,
    }))
}

fn classify_storage_key(key: &str) -> &'static str {
    if key.ends_with("/metadata.json") {
        return "metadata";
    }
    if key.contains("/manifests/") {
        return "manifest";
    }
    if key.contains("/segments/") {
        return "segment";
    }
    if key.contains("/ann/") && key.ends_with("/meta.json") {
        return "ann_meta";
    }
    if key.contains("/ann/") && key.contains("/buckets/") {
        return "ann_bucket";
    }
    if key.contains("/ann/") && key.contains("/filters/") {
        return "filter";
    }
    if key.ends_with("/queue/wal.json") {
        return "queue";
    }
    if key.contains("/operations/") {
        return "status";
    }
    "other"
}

#[cfg(test)]
mod tests {
    use super::{classify_storage_key, summarize_queue};
    use serde_json::json;
    use turbo_vector_queue::{JobLease, QueueFile, QueueJob};

    #[test]
    fn storage_key_classification_works_for_known_shapes() {
        assert_eq!(
            classify_storage_key("collections/docs/manifests/1.json"),
            "manifest"
        );
        assert_eq!(
            classify_storage_key("collections/docs/segments/op123.json"),
            "segment"
        );
        assert_eq!(
            classify_storage_key("collections/docs/ann/default/2/meta.json"),
            "ann_meta"
        );
        assert_eq!(
            classify_storage_key("collections/docs/ann/default/2/buckets/7.bin"),
            "ann_bucket"
        );
        assert_eq!(
            classify_storage_key("collections/docs/ann/default/2/filters/cluster/x.bin"),
            "filter"
        );
        assert_eq!(
            classify_storage_key("collections/docs/queue/wal.json"),
            "queue"
        );
        assert_eq!(
            classify_storage_key("collections/docs/operations/op123.json"),
            "status"
        );
        assert_eq!(
            classify_storage_key("collections/docs/metadata.json"),
            "metadata"
        );
    }

    #[test]
    fn queue_summary_computes_pending_and_leased_jobs() {
        let snapshot = QueueFile {
            broker: Some("broker-a".to_string()),
            jobs: vec![
                QueueJob {
                    id: "j1".to_string(),
                    payload: json!({"operation_id":"op1"}),
                    enqueued_at_ms: 10,
                    lease: None,
                },
                QueueJob {
                    id: "j2".to_string(),
                    payload: json!({"operation_id":"op2"}),
                    enqueued_at_ms: 20,
                    lease: Some(JobLease {
                        worker_id: "worker-1".to_string(),
                        claimed_at_ms: 30,
                        heartbeat_at_ms: 40,
                        attempt: 2,
                    }),
                },
            ],
        };

        let summary = summarize_queue("docs", &snapshot);
        assert_eq!(summary.collection, "docs");
        assert_eq!(summary.queue_depth, 2);
        assert_eq!(summary.pending_jobs, 1);
        assert_eq!(summary.leased_jobs, 1);
        assert_eq!(summary.max_attempt, 2);
        assert_eq!(summary.jobs.len(), 2);
    }
}
