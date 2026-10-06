use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

use axum::http::StatusCode;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    task::JoinSet,
    time::{sleep, Duration},
};
use tracing::{debug, info};
use turbo_vector_core::TurboVectorError;
use turbo_vector_manifest::{CurrentPointer, Manifest, SegmentRef};

use crate::{
    error::{map_store_error, ApiError},
    filters::parse_metadata_filter_with_limits,
    keys::{
        collection_metadata_key, current_pointer_key, manifest_generation_key, now_rfc3339,
        operation_status_object_key, segment_object_key, sha256_hex, wal_object_key,
    },
    models::{
        CollectionMetadata, OperationApplyStatus, OperationStatusResponse, SegmentFile,
        SegmentKind, UpsertVector, WalRecord,
    },
    state::AppState,
    telemetry,
    validation::metadata_matches_filter,
};

const MANIFEST_READ_ATTEMPTS: usize = 5;
const MANIFEST_READ_BASE_BACKOFF_MS: u64 = 5;
const MANIFEST_PUBLISH_ATTEMPTS: usize = 8;
const WAL_READ_ATTEMPTS: usize = 4;
const WAL_READ_BASE_BACKOFF_MS: u64 = 4;
const QUEUE_WAIT_ATTEMPTS: usize = 8;
const QUEUE_WAIT_BASE_BACKOFF_MS: u64 = 4;
const WAL_QUEUE_BATCH_LIMIT: usize = 1_024;
const SEGMENT_VISIBILITY_READ_ATTEMPTS: usize = 6;
const SEGMENT_VISIBILITY_BASE_BACKOFF_MS: u64 = 8;

fn manifest_retry_delay(attempt: usize) -> Duration {
    let shift = (attempt as u32).min(6);
    Duration::from_millis(MANIFEST_READ_BASE_BACKOFF_MS.saturating_mul(1u64 << shift))
}

fn wal_retry_delay(attempt: usize) -> Duration {
    let shift = (attempt as u32).min(6);
    Duration::from_millis(WAL_READ_BASE_BACKOFF_MS.saturating_mul(1u64 << shift))
}

fn queue_retry_delay(attempt: usize) -> Duration {
    let shift = (attempt as u32).min(7);
    Duration::from_millis(QUEUE_WAIT_BASE_BACKOFF_MS.saturating_mul(1u64 << shift))
}

fn segment_visibility_retry_delay(attempt: usize) -> Duration {
    let shift = (attempt as u32).min(7);
    Duration::from_millis(SEGMENT_VISIBILITY_BASE_BACKOFF_MS.saturating_mul(1u64 << shift))
}

fn parse_manifest_generation_key(prefix: &str, key: &str) -> Option<u64> {
    if !key.starts_with(prefix) || key.ends_with("/current.json") || !key.ends_with(".json") {
        return None;
    }
    key.strip_prefix(prefix)?
        .strip_suffix(".json")?
        .parse::<u64>()
        .ok()
}

fn operation_id_from_segment_id(segment_id: &str) -> String {
    segment_id
        .split_once("--shard-")
        .map(|(operation_id, _)| operation_id.to_string())
        .unwrap_or_else(|| segment_id.to_string())
}

fn derive_last_applied_operation(manifest: &Manifest) -> Option<String> {
    manifest.last_applied_operation.clone().or_else(|| {
        manifest
            .segment_refs
            .iter()
            .map(|segment| operation_id_from_segment_id(&segment.segment_id))
            .max()
    })
}

fn derive_applied_operation_ids(manifest: &Manifest) -> BTreeSet<String> {
    let mut operation_ids: BTreeSet<String> = manifest
        .segment_refs
        .iter()
        .map(|segment| operation_id_from_segment_id(&segment.segment_id))
        .collect();
    if let Some(last_applied) = manifest.last_applied_operation.as_ref() {
        operation_ids.insert(last_applied.clone());
    }
    operation_ids
}

async fn load_manifest_generation_with_retry(
    state: &AppState,
    collection: &str,
    generation: u64,
) -> Result<Manifest, ApiError> {
    let manifest_key = manifest_generation_key(collection, generation);
    for attempt in 0..MANIFEST_READ_ATTEMPTS {
        match state.storage.get_bytes(&manifest_key).await {
            Ok(raw_manifest) => {
                let manifest: Manifest = serde_json::from_slice(&raw_manifest).map_err(|e| {
                    ApiError::internal(format!("failed to parse manifest '{manifest_key}': {e}"))
                })?;
                return Ok(manifest);
            }
            Err(TurboVectorError::NotFound(_)) if attempt + 1 < MANIFEST_READ_ATTEMPTS => {
                sleep(manifest_retry_delay(attempt)).await;
            }
            Err(TurboVectorError::NotFound(_)) => {
                return Err(ApiError::store_unavailable(format!(
                    "manifest generation {} for collection '{}' was not readable after {} attempts",
                    generation, collection, MANIFEST_READ_ATTEMPTS
                )));
            }
            Err(e) => return Err(map_store_error(e)),
        }
    }
    Err(ApiError::store_unavailable(format!(
        "manifest generation {} for collection '{}' was not readable after {} attempts",
        generation, collection, MANIFEST_READ_ATTEMPTS
    )))
}

async fn load_wal_record_with_retry(
    state: &AppState,
    collection: &str,
    operation_id: &str,
) -> Result<Option<WalRecord>, ApiError> {
    let wal_key = wal_object_key(collection, operation_id);
    for attempt in 0..WAL_READ_ATTEMPTS {
        match state.storage.get_bytes(&wal_key).await {
            Ok(raw) => {
                let wal: WalRecord = serde_json::from_slice(&raw).map_err(|e| {
                    ApiError::internal(format!("failed to parse WAL record '{wal_key}': {e}"))
                })?;
                return Ok(Some(wal));
            }
            Err(TurboVectorError::NotFound(_)) if attempt + 1 < WAL_READ_ATTEMPTS => {
                sleep(wal_retry_delay(attempt)).await;
            }
            Err(TurboVectorError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(map_store_error(e)),
        }
    }
    Ok(None)
}

async fn write_segment_and_confirm_visibility(
    state: &AppState,
    collection: &str,
    segment_id: &str,
    segment_bytes: &[u8],
    expected_checksum: &str,
) -> Result<String, ApiError> {
    let segment_key = segment_object_key(collection, segment_id);
    state
        .storage
        .put_bytes(&segment_key, segment_bytes)
        .await
        .map_err(map_store_error)?;

    for attempt in 0..SEGMENT_VISIBILITY_READ_ATTEMPTS {
        let visible = match state.storage.get_bytes_with_version(&segment_key).await {
            Ok(Some(versioned)) => Some(versioned),
            Ok(None) => None,
            Err(TurboVectorError::NotFound(_)) => None,
            Err(error) => return Err(map_store_error(error)),
        };

        if let Some(versioned) = visible {
            let actual_checksum = sha256_hex(&versioned.bytes);
            if actual_checksum != expected_checksum {
                return Err(ApiError::store_unavailable(format!(
                    "segment '{segment_id}' for collection '{collection}' failed visibility checksum verification"
                )));
            }
            debug!(
                collection,
                segment_id,
                segment_key,
                segment_version = %versioned.version,
                "segment artifact is visible before manifest publish"
            );
            return Ok(segment_key);
        }

        if attempt + 1 < SEGMENT_VISIBILITY_READ_ATTEMPTS {
            sleep(segment_visibility_retry_delay(attempt)).await;
            continue;
        }
    }

    Err(ApiError::store_unavailable(format!(
        "segment '{segment_id}' for collection '{collection}' was not readable after {} attempts",
        SEGMENT_VISIBILITY_READ_ATTEMPTS
    )))
}

async fn load_latest_manifest_from_listing(
    state: &AppState,
    collection: &str,
) -> Result<Option<Manifest>, ApiError> {
    let manifest_prefix = format!("collections/{collection}/manifests/");
    let manifest_keys = state
        .storage
        .list_prefix(&manifest_prefix)
        .await
        .map_err(map_store_error)?;
    let latest_generation = manifest_keys
        .iter()
        .filter_map(|key| parse_manifest_generation_key(&manifest_prefix, key))
        .max();

    let Some(generation) = latest_generation else {
        return Ok(None);
    };
    let manifest = load_manifest_generation_with_retry(state, collection, generation).await?;
    Ok(Some(manifest))
}

async fn load_latest_manifest_snapshot(
    state: &AppState,
    collection: &str,
) -> Result<Option<Manifest>, ApiError> {
    let cached_manifest = state.get_cached_manifest(collection).await;
    let cached_generation = cached_manifest.as_ref().map(|manifest| manifest.generation);
    let current_key = current_pointer_key(collection);
    let manifest = match state.storage.get_bytes(&current_key).await {
        Ok(raw_pointer) => {
            let pointer: CurrentPointer = serde_json::from_slice(&raw_pointer)
                .map_err(|e| ApiError::internal(format!("failed to parse current pointer: {e}")))?;
            if cached_generation.is_some_and(|generation| generation >= pointer.current_generation)
            {
                cached_manifest
            } else {
                Some(
                    load_manifest_generation_with_retry(
                        state,
                        collection,
                        pointer.current_generation,
                    )
                    .await?,
                )
            }
        }
        Err(TurboVectorError::NotFound(_)) => {
            let listed_manifest = load_latest_manifest_from_listing(state, collection).await?;
            match listed_manifest {
                Some(manifest) => Some(manifest),
                None => {
                    state.remove_cached_manifest(collection).await;
                    None
                }
            }
        }
        Err(e) => return Err(map_store_error(e)),
    };

    if let Some(manifest) = manifest.as_ref() {
        state
            .set_cached_manifest(collection, manifest.clone())
            .await;
    }
    Ok(manifest)
}

fn build_upsert_segments(state: &AppState, collection: &str, wal: &WalRecord) -> Vec<SegmentFile> {
    let shard_count = state.distributed_shard_count();
    if shard_count <= 1 {
        return vec![SegmentFile {
            segment_id: wal.operation_id.clone(),
            collection: collection.to_string(),
            namespace: wal.namespace.clone(),
            created_at: wal.accepted_at.clone(),
            kind: SegmentKind::Upsert,
            vectors: wal.request.vectors.clone(),
            deleted_ids: Vec::new(),
            delete_all: false,
            delete_filter: None,
        }];
    }

    let mut vectors_by_namespace: BTreeMap<String, Vec<UpsertVector>> = BTreeMap::new();
    for vector in &wal.request.vectors {
        let shard_id = state.shard_for_vector_id(collection, &wal.namespace, &vector.id);
        let shard_namespace = crate::distributed::shard_namespace(&wal.namespace, shard_id);
        vectors_by_namespace
            .entry(shard_namespace)
            .or_default()
            .push(vector.clone());
    }

    vectors_by_namespace
        .into_iter()
        .map(|(namespace, vectors)| {
            let shard_id = crate::distributed::extract_shard_id(&namespace).unwrap_or(0);
            SegmentFile {
                segment_id: format!("{}--shard-{shard_id:04}", wal.operation_id),
                collection: collection.to_string(),
                namespace,
                created_at: wal.accepted_at.clone(),
                kind: SegmentKind::Upsert,
                vectors,
                deleted_ids: Vec::new(),
                delete_all: false,
                delete_filter: None,
            }
        })
        .collect()
}

fn parse_operation_status(
    raw: &[u8],
    status_key: &str,
) -> Result<OperationStatusResponse, ApiError> {
    serde_json::from_slice(raw).map_err(|error| {
        ApiError::internal(format!(
            "failed to parse operation status '{status_key}': {error}"
        ))
    })
}

async fn write_operation_status(
    state: &AppState,
    collection: &str,
    status: &OperationStatusResponse,
) -> Result<(), ApiError> {
    let status_key = operation_status_object_key(collection, &status.operation_id);
    let bytes = serde_json::to_vec(status).map_err(|error| {
        ApiError::internal(format!(
            "failed to serialize operation status '{}': {error}",
            status.operation_id
        ))
    })?;
    state
        .storage
        .put_bytes(&status_key, &bytes)
        .await
        .map_err(map_store_error)
}

pub(crate) async fn record_operation_accepted(
    state: &AppState,
    collection: &str,
    operation_id: &str,
    accepted_at: &str,
) -> Result<OperationStatusResponse, ApiError> {
    let status = OperationStatusResponse {
        operation_id: operation_id.to_string(),
        status: OperationApplyStatus::Accepted,
        accepted_at: accepted_at.to_string(),
        applied_at: None,
        generation: None,
    };
    write_operation_status(state, collection, &status).await?;
    Ok(status)
}

pub(crate) async fn load_operation_status(
    state: &AppState,
    collection: &str,
    operation_id: &str,
) -> Result<OperationStatusResponse, ApiError> {
    let status_key = operation_status_object_key(collection, operation_id);
    let raw = state
        .storage
        .get_bytes(&status_key)
        .await
        .map_err(map_store_error)?;
    parse_operation_status(&raw, &status_key)
}

async fn mark_operation_applied(
    state: &AppState,
    collection: &str,
    operation_id: &str,
    accepted_at_hint: Option<&str>,
    generation: u64,
    applied_at: &str,
) -> Result<(), ApiError> {
    let existing = match load_operation_status(state, collection, operation_id).await {
        Ok(status) => status,
        Err(error) if error.status == StatusCode::NOT_FOUND => OperationStatusResponse {
            operation_id: operation_id.to_string(),
            status: OperationApplyStatus::Accepted,
            accepted_at: accepted_at_hint.unwrap_or(applied_at).to_string(),
            applied_at: None,
            generation: None,
        },
        Err(error) => return Err(error),
    };

    let already_applied = matches!(existing.status, OperationApplyStatus::Applied);
    let existing_generation = existing.generation.unwrap_or(0);
    let generation = generation.max(existing_generation);
    let applied_at = existing
        .applied_at
        .clone()
        .unwrap_or_else(|| applied_at.to_string());
    if already_applied && existing_generation >= generation {
        return Ok(());
    }
    let should_record_applied_metric = !already_applied;

    let status = OperationStatusResponse {
        operation_id: existing.operation_id,
        status: OperationApplyStatus::Applied,
        accepted_at: existing.accepted_at,
        applied_at: Some(applied_at),
        generation: Some(generation),
    };
    write_operation_status(state, collection, &status).await?;
    if should_record_applied_metric {
        telemetry::increment_upsert_applied(&state.service_name);
        if let Some(applied_at) = status.applied_at.as_deref() {
            if let Some(lag_seconds) = operation_apply_lag_seconds(&status.accepted_at, applied_at)
            {
                telemetry::record_operation_apply_lag(&state.service_name, lag_seconds);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalQueuePayload {
    operation_id: String,
}

#[derive(Debug, Clone)]
struct PendingQueueJob {
    queue_job_id: String,
    operation_id: String,
}

struct QueueFlushResult {
    generation: u64,
    target_applied: bool,
    queue_depth: usize,
    pending_jobs: usize,
    applied_jobs: usize,
    acked_jobs: usize,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct WalQueueFlushReport {
    pub(crate) generation: u64,
    pub(crate) queue_depth: usize,
    pub(crate) pending_jobs: usize,
    pub(crate) applied_jobs: usize,
    pub(crate) acked_jobs: usize,
}

fn map_queue_error(err: turbo_vector_queue::QueueError) -> ApiError {
    ApiError::store_unavailable(format!("write queue error: {err}"))
}

fn queue_target_applied(
    applied_operation_ids: &BTreeSet<String>,
    target_operation_id: Option<&str>,
) -> bool {
    target_operation_id
        .map(|target| applied_operation_ids.contains(target))
        .unwrap_or(true)
}

fn queue_flush_result(
    generation: u64,
    applied_operation_ids: &BTreeSet<String>,
    target_operation_id: Option<&str>,
    queue_depth: usize,
    pending_jobs: usize,
    applied_jobs: usize,
    acked_jobs: usize,
) -> QueueFlushResult {
    QueueFlushResult {
        generation,
        target_applied: queue_target_applied(applied_operation_ids, target_operation_id),
        queue_depth,
        pending_jobs,
        applied_jobs,
        acked_jobs,
    }
}

fn operation_apply_lag_seconds(accepted_at: &str, applied_at: &str) -> Option<f64> {
    let accepted = DateTime::parse_from_rfc3339(accepted_at).ok()?;
    let applied = DateTime::parse_from_rfc3339(applied_at).ok()?;
    let lag = applied - accepted;
    lag.to_std().ok().map(|duration| duration.as_secs_f64())
}

fn parse_wal_queue_payload(payload: &Value) -> Result<WalQueuePayload, ApiError> {
    serde_json::from_value(payload.clone())
        .map_err(|e| ApiError::internal(format!("failed to parse queue payload: {e}")))
}

fn parse_pending_jobs_from_claims(
    claimed_jobs: Vec<turbo_vector_queue::ClaimedJob>,
) -> Result<Vec<PendingQueueJob>, ApiError> {
    claimed_jobs
        .into_iter()
        .map(|claimed| {
            let payload = parse_wal_queue_payload(&claimed.payload)?;
            Ok(PendingQueueJob {
                queue_job_id: claimed.id,
                operation_id: payload.operation_id,
            })
        })
        .collect()
}

async fn ack_queue_jobs_batch(
    queue: &Arc<dyn crate::queue_broker::QueueBrokerClient>,
    collection: &str,
    worker_id: &str,
    queue_job_ids: &[String],
) -> Result<usize, ApiError> {
    if queue_job_ids.is_empty() {
        return Ok(0);
    }

    let acked = queue
        .ack_batch(collection, worker_id.to_string(), queue_job_ids.to_vec())
        .await
        .map_err(map_queue_error)?;
    if acked != queue_job_ids.len() {
        return Err(ApiError::store_unavailable(format!(
            "failed to ack all queue jobs: expected {}, acked {}",
            queue_job_ids.len(),
            acked
        )));
    }
    Ok(acked)
}

async fn flush_wal_queue_internal(
    state: &AppState,
    collection: &str,
    target_operation_id: Option<&str>,
    reason: &'static str,
) -> Result<QueueFlushResult, ApiError> {
    for attempt in 0..QUEUE_WAIT_ATTEMPTS {
        let started = Instant::now();
        let result = flush_wal_queue_once(state, collection, target_operation_id).await?;
        debug!(
            collection,
            reason,
            attempt,
            duration_ms = started.elapsed().as_millis() as u64,
            generation = result.generation,
            queue_depth = result.queue_depth,
            pending_jobs = result.pending_jobs,
            applied_jobs = result.applied_jobs,
            acked_jobs = result.acked_jobs,
            target_applied = result.target_applied,
            "write queue flush iteration"
        );
        if result.target_applied || target_operation_id.is_none() {
            return Ok(result);
        }
        if attempt + 1 < QUEUE_WAIT_ATTEMPTS {
            sleep(queue_retry_delay(attempt)).await;
        }
    }

    Err(ApiError::store_unavailable(format!(
        "operation '{}' was not observed in queue flush after {} attempts",
        target_operation_id.unwrap_or("unknown"),
        QUEUE_WAIT_ATTEMPTS
    )))
}

pub(crate) async fn enqueue_wal_operation(
    state: &AppState,
    collection: &str,
    operation_id: &str,
) -> Result<(), ApiError> {
    let queue = state.queue_client();
    let payload = serde_json::to_value(WalQueuePayload {
        operation_id: operation_id.to_string(),
    })
    .map_err(|e| ApiError::internal(format!("failed to serialize queue payload: {e}")))?;
    queue
        .enqueue(collection, payload)
        .await
        .map_err(map_queue_error)?;
    debug!(collection, operation_id, "enqueued WAL operation");
    Ok(())
}

async fn flush_wal_queue_once(
    state: &AppState,
    collection: &str,
    target_operation_id: Option<&str>,
) -> Result<QueueFlushResult, ApiError> {
    let _publish_guard = state.lock_collection_manifest(collection).await;
    let queue = state.queue_client();
    let worker_id = state.wal_queue_worker_id(collection);

    for publish_attempt in 0..MANIFEST_PUBLISH_ATTEMPTS {
        let previous_manifest = load_latest_manifest_snapshot(state, collection).await?;
        let previous_generation = previous_manifest
            .as_ref()
            .map(|manifest| manifest.generation);
        let mut last_applied_operation = previous_manifest
            .as_ref()
            .and_then(derive_last_applied_operation);
        let mut applied_operation_ids: BTreeSet<String> = previous_manifest
            .as_ref()
            .map(derive_applied_operation_ids)
            .unwrap_or_default();
        let current_generation = previous_generation.unwrap_or(0);
        let claimed_jobs = queue
            .claim_batch(collection, worker_id.clone(), WAL_QUEUE_BATCH_LIMIT)
            .await
            .map_err(map_queue_error)?;
        // Avoid an extra queue snapshot read per flush loop. This keeps broker traffic
        // lower on hot keys; claimed job count is sufficient for worker flush telemetry.
        let queue_depth = claimed_jobs.len();
        let pending_jobs = parse_pending_jobs_from_claims(claimed_jobs)?;
        let pending_jobs_count = pending_jobs.len();
        if pending_jobs.is_empty() {
            return Ok(queue_flush_result(
                current_generation,
                &applied_operation_ids,
                target_operation_id,
                queue_depth,
                pending_jobs_count,
                0,
                0,
            ));
        }

        let mut namespace_partitions: BTreeMap<String, Vec<String>> = previous_manifest
            .as_ref()
            .map(|manifest| manifest.namespace_partitions.clone())
            .unwrap_or_default();
        let mut segment_refs = previous_manifest
            .as_ref()
            .map(|manifest| manifest.segment_refs.clone())
            .unwrap_or_default();
        let mut queue_job_ids_already_applied: Vec<String> = Vec::new();
        let mut queue_job_ids_newly_applied: Vec<String> = Vec::new();
        let mut applied_jobs = 0usize;
        let mut applied_operation_status_updates: Vec<(String, String)> = Vec::new();

        let mut applied_any = false;
        for pending_job in pending_jobs {
            if applied_operation_ids.contains(&pending_job.operation_id) {
                mark_operation_applied(
                    state,
                    collection,
                    &pending_job.operation_id,
                    None,
                    current_generation,
                    &now_rfc3339(),
                )
                .await?;
                queue_job_ids_already_applied.push(pending_job.queue_job_id);
                continue;
            }

            let Some(wal) =
                load_wal_record_with_retry(state, collection, &pending_job.operation_id).await?
            else {
                debug!(
                    collection,
                    operation_id = pending_job.operation_id,
                    queue_job_id = pending_job.queue_job_id,
                    "WAL not visible yet for claimed queue job; deferring apply without heartbeat mutation"
                );
                let acked_jobs = ack_queue_jobs_batch(
                    &queue,
                    collection,
                    &worker_id,
                    &queue_job_ids_already_applied,
                )
                .await?;
                return Ok(queue_flush_result(
                    current_generation,
                    &applied_operation_ids,
                    target_operation_id,
                    queue_depth,
                    pending_jobs_count,
                    applied_jobs,
                    acked_jobs,
                ));
            };
            if wal.collection != collection {
                return Err(ApiError::internal(format!(
                    "WAL '{}' belongs to collection '{}', expected '{}'",
                    wal.operation_id, wal.collection, collection
                )));
            }
            if wal.operation_id != pending_job.operation_id {
                return Err(ApiError::internal(format!(
                    "WAL key '{}' does not match WAL payload operation_id '{}'",
                    pending_job.operation_id, wal.operation_id
                )));
            }
            let segments = build_upsert_segments(state, collection, &wal);
            for segment in segments {
                let row_count = segment.vectors.len() as u64;
                let segment_bytes = serde_json::to_vec(&segment)
                    .map_err(|e| ApiError::internal(format!("failed to serialize segment: {e}")))?;
                let segment_checksum = sha256_hex(&segment_bytes);
                let segment_key = write_segment_and_confirm_visibility(
                    state,
                    collection,
                    &segment.segment_id,
                    &segment_bytes,
                    &segment_checksum,
                )
                .await?;

                namespace_partitions
                    .entry(segment.namespace.clone())
                    .or_default()
                    .push(segment.segment_id.clone());
                segment_refs.push(SegmentRef {
                    segment_id: segment.segment_id.clone(),
                    uri: format!("s3://{}/{}", state.runtime.storage_bucket, segment_key),
                    row_count,
                    checksum: segment_checksum,
                });
            }
            if last_applied_operation
                .as_deref()
                .is_none_or(|watermark| wal.operation_id.as_str() > watermark)
            {
                last_applied_operation = Some(wal.operation_id.clone());
            }
            applied_operation_status_updates.push((wal.operation_id.clone(), wal.accepted_at));
            applied_operation_ids.insert(wal.operation_id);
            queue_job_ids_newly_applied.push(pending_job.queue_job_id);
            applied_jobs += 1;
            applied_any = true;
        }

        if !applied_any {
            let acked_jobs = ack_queue_jobs_batch(
                &queue,
                collection,
                &worker_id,
                &queue_job_ids_already_applied,
            )
            .await?;
            return Ok(queue_flush_result(
                current_generation,
                &applied_operation_ids,
                target_operation_id,
                queue_depth,
                pending_jobs_count,
                0,
                acked_jobs,
            ));
        }

        let generation = previous_generation.map_or(1, |value| value + 1);
        let manifest = Manifest {
            generation,
            collection: collection.to_string(),
            created_at: now_rfc3339(),
            created_by: state.node_id.clone(),
            previous_generation,
            last_applied_operation: last_applied_operation.clone(),
            namespace_partitions,
            segment_refs,
        };
        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|e| ApiError::internal(format!("failed to serialize manifest: {e}")))?;
        let manifest_key = manifest_generation_key(collection, generation);
        let published = state
            .storage
            .put_bytes_if_absent(&manifest_key, &manifest_bytes)
            .await
            .map_err(map_store_error)?;
        if !published {
            state.remove_cached_manifest(collection).await;
            if publish_attempt + 1 < MANIFEST_PUBLISH_ATTEMPTS {
                sleep(manifest_retry_delay(publish_attempt)).await;
                continue;
            }
            return Err(ApiError::store_unavailable(format!(
                "manifest publish contention for collection '{}' after {} attempts",
                collection, MANIFEST_PUBLISH_ATTEMPTS
            )));
        }

        // Ensure the new manifest generation is readable before advancing current pointer.
        load_manifest_generation_with_retry(state, collection, generation).await?;
        info!(
            collection,
            generation,
            segments = manifest.segment_refs.len(),
            namespaces = manifest.namespace_partitions.len(),
            "manifest published to storage"
        );

        let pointer = CurrentPointer {
            current_generation: generation,
            updated_at: now_rfc3339(),
        };
        let pointer_bytes = serde_json::to_vec(&pointer)
            .map_err(|e| ApiError::internal(format!("failed to serialize current pointer: {e}")))?;
        let current_key = current_pointer_key(collection);
        state
            .storage
            .put_bytes(&current_key, &pointer_bytes)
            .await
            .map_err(map_store_error)?;
        debug!(
            collection,
            generation,
            current_key,
            "updated current pointer after manifest visibility confirmation"
        );

        state.set_cached_manifest(collection, manifest).await;
        state.invalidate_collection_cache(collection).await;

        let applied_at = now_rfc3339();
        for (operation_id, accepted_at) in applied_operation_status_updates {
            mark_operation_applied(
                state,
                collection,
                &operation_id,
                Some(&accepted_at),
                generation,
                &applied_at,
            )
            .await?;
        }

        let mut queue_job_ids_to_ack = queue_job_ids_already_applied;
        queue_job_ids_to_ack.extend(queue_job_ids_newly_applied);
        let acked_after_publish =
            ack_queue_jobs_batch(&queue, collection, &worker_id, &queue_job_ids_to_ack).await?;

        return Ok(queue_flush_result(
            generation,
            &applied_operation_ids,
            target_operation_id,
            queue_depth,
            pending_jobs_count,
            applied_jobs,
            acked_after_publish,
        ));
    }

    Err(ApiError::store_unavailable(format!(
        "manifest publish contention for collection '{}' after {} attempts",
        collection, MANIFEST_PUBLISH_ATTEMPTS
    )))
}

pub(crate) async fn flush_wal_queue(
    state: &AppState,
    collection: &str,
    target_operation_id: Option<&str>,
) -> Result<u64, ApiError> {
    let report =
        flush_wal_queue_internal(state, collection, target_operation_id, "foreground").await?;
    Ok(report.generation)
}

pub(crate) async fn flush_wal_queue_with_report(
    state: &AppState,
    collection: &str,
    target_operation_id: Option<&str>,
) -> Result<WalQueueFlushReport, ApiError> {
    let result =
        flush_wal_queue_internal(state, collection, target_operation_id, "foreground").await?;
    Ok(WalQueueFlushReport {
        generation: result.generation,
        queue_depth: result.queue_depth,
        pending_jobs: result.pending_jobs,
        applied_jobs: result.applied_jobs,
        acked_jobs: result.acked_jobs,
    })
}

pub(crate) async fn publish_manifest(
    state: &AppState,
    collection: &str,
    namespace: &str,
    segment_id: &str,
    segment_key: &str,
    row_count: u64,
    checksum: String,
) -> Result<u64, ApiError> {
    let _publish_guard = state.lock_collection_manifest(collection).await;
    let current_key = current_pointer_key(collection);
    for publish_attempt in 0..MANIFEST_PUBLISH_ATTEMPTS {
        let previous_manifest = load_latest_manifest_snapshot(state, collection).await?;
        let previous_last_applied_operation = previous_manifest
            .as_ref()
            .and_then(derive_last_applied_operation);

        let previous_generation = previous_manifest.as_ref().map(|m| m.generation);
        let generation = previous_generation.map_or(1, |g| g + 1);
        let mut namespace_partitions: BTreeMap<String, Vec<String>> = previous_manifest
            .as_ref()
            .map(|m| m.namespace_partitions.clone())
            .unwrap_or_default();
        namespace_partitions
            .entry(namespace.to_string())
            .or_default()
            .push(segment_id.to_string());

        let mut segment_refs = previous_manifest
            .as_ref()
            .map(|m| m.segment_refs.clone())
            .unwrap_or_default();
        segment_refs.push(SegmentRef {
            segment_id: segment_id.to_string(),
            uri: format!("s3://{}/{}", state.runtime.storage_bucket, segment_key),
            row_count,
            checksum: checksum.clone(),
        });

        let manifest = Manifest {
            generation,
            collection: collection.to_string(),
            created_at: now_rfc3339(),
            created_by: state.node_id.clone(),
            previous_generation,
            last_applied_operation: previous_last_applied_operation,
            namespace_partitions,
            segment_refs,
        };
        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|e| ApiError::internal(format!("failed to serialize manifest: {e}")))?;
        let manifest_key = manifest_generation_key(collection, generation);
        let published = state
            .storage
            .put_bytes_if_absent(&manifest_key, &manifest_bytes)
            .await
            .map_err(map_store_error)?;
        if !published {
            state.remove_cached_manifest(collection).await;
            if publish_attempt + 1 < MANIFEST_PUBLISH_ATTEMPTS {
                sleep(manifest_retry_delay(publish_attempt)).await;
                continue;
            }
            return Err(ApiError::store_unavailable(format!(
                "manifest publish contention for collection '{}' after {} attempts",
                collection, MANIFEST_PUBLISH_ATTEMPTS
            )));
        }

        // Ensure the new manifest generation is readable before advancing current pointer.
        load_manifest_generation_with_retry(state, collection, generation).await?;
        info!(
            collection,
            generation,
            segments = manifest.segment_refs.len(),
            namespaces = manifest.namespace_partitions.len(),
            "manifest published to storage"
        );

        let pointer = CurrentPointer {
            current_generation: generation,
            updated_at: now_rfc3339(),
        };
        let pointer_bytes = serde_json::to_vec(&pointer)
            .map_err(|e| ApiError::internal(format!("failed to serialize current pointer: {e}")))?;
        state
            .storage
            .put_bytes(&current_key, &pointer_bytes)
            .await
            .map_err(map_store_error)?;
        debug!(
            collection,
            generation,
            current_key,
            "updated current pointer after manifest visibility confirmation"
        );
        state
            .set_cached_manifest(collection, manifest.clone())
            .await;
        state.invalidate_collection_cache(collection).await;

        return Ok(generation);
    }

    Err(ApiError::store_unavailable(format!(
        "manifest publish contention for collection '{}' after {} attempts",
        collection, MANIFEST_PUBLISH_ATTEMPTS
    )))
}

pub(crate) async fn load_collection_metadata(
    state: &AppState,
    name: &str,
) -> Result<CollectionMetadata, ApiError> {
    if let Some(metadata) = state.get_cached_collection_metadata(name).await {
        return Ok(metadata);
    }

    let metadata_key = collection_metadata_key(name);
    let raw = state
        .storage
        .get_bytes(&metadata_key)
        .await
        .map_err(map_store_error)?;
    let parsed = serde_json::from_slice::<CollectionMetadata>(&raw)
        .map_err(|e| ApiError::internal(format!("failed to parse collection metadata: {e}")))?;
    state
        .set_cached_collection_metadata(name, parsed.clone())
        .await;
    Ok(parsed)
}

pub(crate) async fn load_current_manifest(
    state: &AppState,
    collection: &str,
) -> Result<Option<Manifest>, ApiError> {
    load_latest_manifest_snapshot(state, collection).await
}

async fn load_namespace_segments(
    state: &AppState,
    collection: &str,
    namespace: &str,
    segment_ids: &[String],
    expected_checksums: &BTreeMap<String, String>,
) -> Result<Vec<SegmentFile>, ApiError> {
    let mut join_set = JoinSet::new();
    for (index, segment_id) in segment_ids.iter().enumerate() {
        let storage = state.storage.clone();
        let collection_name = collection.to_string();
        let namespace_name = namespace.to_string();
        let segment_id = segment_id.clone();
        let expected_checksum = expected_checksums.get(&segment_id).cloned();
        join_set.spawn(async move {
            let key = segment_object_key(&collection_name, &segment_id);
            let raw_segment = storage.get_bytes(&key).await.map_err(map_store_error)?;
            let Some(expected_checksum) = expected_checksum else {
                return Err(ApiError::store_unavailable(format!(
                    "missing checksum metadata for segment '{}' in collection '{}'",
                    segment_id, collection_name
                )));
            };
            let actual_checksum = sha256_hex(&raw_segment);
            if actual_checksum != expected_checksum {
                return Err(ApiError::store_unavailable(format!(
                    "checksum mismatch for segment '{}' in collection '{}'",
                    segment_id, collection_name
                )));
            }
            let segment: SegmentFile = serde_json::from_slice(&raw_segment).map_err(|e| {
                ApiError::internal(format!(
                    "failed to parse segment '{}' for collection '{}': {e}",
                    segment_id, collection_name
                ))
            })?;

            if segment.collection != collection_name {
                return Err(ApiError::internal(format!(
                    "segment '{}' belongs to collection '{}', expected '{}'",
                    segment.segment_id, segment.collection, collection_name
                )));
            }
            if segment.namespace != namespace_name {
                return Err(ApiError::internal(format!(
                    "segment '{}' belongs to namespace '{}', expected '{}'",
                    segment.segment_id, segment.namespace, namespace_name
                )));
            }

            Ok::<(usize, SegmentFile), ApiError>((index, segment))
        });
    }

    let mut ordered_segments: Vec<Option<SegmentFile>> =
        (0..segment_ids.len()).map(|_| None).collect();
    while let Some(join_result) = join_set.join_next().await {
        let (index, segment) = join_result
            .map_err(|e| ApiError::internal(format!("segment load task failed: {e}")))??;
        ordered_segments[index] = Some(segment);
    }

    ordered_segments
        .into_iter()
        .map(|segment| {
            segment.ok_or_else(|| ApiError::internal("segment load task missing output"))
        })
        .collect()
}

pub(crate) async fn load_namespace_vectors(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
) -> Result<Arc<BTreeMap<String, UpsertVector>>, ApiError> {
    if let Some(cache_entry) = state.get_namespace_cache_entry(collection, namespace).await {
        if cache_entry.generation == manifest.generation {
            let shard_scope = crate::distributed::shard_scope_label(namespace);
            telemetry::increment_cache_hits_scoped(
                &state.service_name,
                &state.node_id,
                "namespace_vectors",
                &shard_scope,
                1,
            );
            info!(
                collection,
                namespace,
                generation = manifest.generation,
                vectors = cache_entry.vectors.len(),
                "namespace cache HIT (warm)"
            );
            return Ok(cache_entry.vectors);
        }
    }
    let shard_scope = crate::distributed::shard_scope_label(namespace);
    telemetry::increment_cache_misses_scoped(
        &state.service_name,
        &state.node_id,
        "namespace_vectors",
        &shard_scope,
        1,
    );
    info!(
        collection,
        namespace,
        generation = manifest.generation,
        "namespace cache MISS (cold) — loading from storage"
    );

    let mut vectors = BTreeMap::new();
    let Some(segment_ids) = manifest.namespace_partitions.get(namespace) else {
        let vectors = Arc::new(vectors);
        state
            .put_namespace_cache_entry(collection, namespace, manifest.generation, vectors.clone())
            .await;
        return Ok(vectors);
    };

    let expected_checksums: BTreeMap<String, String> = manifest
        .segment_refs
        .iter()
        .map(|segment_ref| (segment_ref.segment_id.clone(), segment_ref.checksum.clone()))
        .collect();
    let segments = load_namespace_segments(
        state,
        collection,
        namespace,
        segment_ids,
        &expected_checksums,
    )
    .await?;
    for segment in segments {
        match segment.kind {
            SegmentKind::Upsert => {
                for vector in segment.vectors {
                    vectors.insert(vector.id.clone(), vector);
                }
            }
            SegmentKind::Delete => {
                if segment.delete_all {
                    vectors.clear();
                }
                if let Some(filter) = segment.delete_filter.as_ref() {
                    let parsed_filter = parse_metadata_filter_with_limits(
                        Some(filter.clone()),
                        state.filter_parser_limits(),
                    )
                    .map_err(|e| {
                        ApiError::internal(format!(
                            "delete filter for segment '{}' is invalid: {:?}",
                            segment.segment_id, e
                        ))
                    })?;
                    vectors.retain(|_, vector| {
                        !metadata_matches_filter(vector.metadata.as_ref(), parsed_filter.as_ref())
                    });
                }
                for id in segment.deleted_ids {
                    vectors.remove(&id);
                }
            }
        }
    }

    let vector_count = vectors.len();
    let vectors = Arc::new(vectors);
    state
        .put_namespace_cache_entry(collection, namespace, manifest.generation, vectors.clone())
        .await;
    info!(
        collection,
        namespace,
        generation = manifest.generation,
        vectors = vector_count,
        "namespace vectors loaded from storage and cached"
    );
    Ok(vectors)
}

pub(crate) async fn load_namespace_vectors_for_ids(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
    target_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, UpsertVector>, ApiError> {
    let result = load_namespace_vectors_for_ids_with_stats(
        state, collection, namespace, manifest, target_ids, None,
    )
    .await?;
    Ok(result.vectors)
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NamespaceVectorsForIdsLoadStats {
    pub(crate) segment_reads: usize,
    pub(crate) segment_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct NamespaceVectorsForIdsLoadResult {
    pub(crate) vectors: BTreeMap<String, UpsertVector>,
    pub(crate) stats: NamespaceVectorsForIdsLoadStats,
}

pub(crate) async fn load_namespace_vectors_for_ids_with_stats(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
    target_ids: &BTreeSet<String>,
    max_segment_reads: Option<usize>,
) -> Result<NamespaceVectorsForIdsLoadResult, ApiError> {
    if target_ids.is_empty() {
        return Ok(NamespaceVectorsForIdsLoadResult {
            vectors: BTreeMap::new(),
            stats: NamespaceVectorsForIdsLoadStats::default(),
        });
    }
    let Some(segment_ids) = manifest.namespace_partitions.get(namespace) else {
        return Ok(NamespaceVectorsForIdsLoadResult {
            vectors: BTreeMap::new(),
            stats: NamespaceVectorsForIdsLoadStats::default(),
        });
    };

    let expected_checksums: BTreeMap<String, String> = manifest
        .segment_refs
        .iter()
        .map(|segment_ref| (segment_ref.segment_id.clone(), segment_ref.checksum.clone()))
        .collect();

    let mut vectors = BTreeMap::new();
    let mut stats = NamespaceVectorsForIdsLoadStats::default();
    for segment_id in segment_ids {
        if max_segment_reads.is_some_and(|limit| stats.segment_reads >= limit) {
            return Err(ApiError::store_unavailable(format!(
                "segment read budget exceeded while loading rerank vectors for collection '{collection}' namespace '{namespace}'"
            )));
        }
        let key = segment_object_key(collection, segment_id);
        let raw_segment = state
            .storage
            .get_bytes(&key)
            .await
            .map_err(map_store_error)?;
        stats.segment_reads = stats.segment_reads.saturating_add(1);
        stats.segment_bytes = stats.segment_bytes.saturating_add(raw_segment.len());
        let Some(expected_checksum) = expected_checksums.get(segment_id) else {
            return Err(ApiError::store_unavailable(format!(
                "missing checksum metadata for segment '{segment_id}' in collection '{collection}'"
            )));
        };
        let actual_checksum = sha256_hex(&raw_segment);
        if actual_checksum != *expected_checksum {
            return Err(ApiError::store_unavailable(format!(
                "checksum mismatch for segment '{segment_id}' in collection '{collection}'"
            )));
        }
        let segment: SegmentFile = serde_json::from_slice(&raw_segment).map_err(|e| {
            ApiError::internal(format!(
                "failed to parse segment '{segment_id}' for collection '{collection}': {e}"
            ))
        })?;
        if segment.collection != collection {
            return Err(ApiError::internal(format!(
                "segment '{}' belongs to collection '{}', expected '{}'",
                segment.segment_id, segment.collection, collection
            )));
        }
        if segment.namespace != namespace {
            return Err(ApiError::internal(format!(
                "segment '{}' belongs to namespace '{}', expected '{}'",
                segment.segment_id, segment.namespace, namespace
            )));
        }

        match segment.kind {
            SegmentKind::Upsert => {
                for vector in segment.vectors {
                    if target_ids.contains(&vector.id) {
                        vectors.insert(vector.id.clone(), vector);
                    }
                }
            }
            SegmentKind::Delete => {
                if segment.delete_all {
                    vectors.clear();
                }
                if let Some(filter) = segment.delete_filter.as_ref() {
                    let parsed_filter = parse_metadata_filter_with_limits(
                        Some(filter.clone()),
                        state.filter_parser_limits(),
                    )
                    .map_err(|e| {
                        ApiError::internal(format!(
                            "delete filter for segment '{}' is invalid: {:?}",
                            segment.segment_id, e
                        ))
                    })?;
                    vectors.retain(|_, vector| {
                        !metadata_matches_filter(vector.metadata.as_ref(), parsed_filter.as_ref())
                    });
                }
                for id in segment.deleted_ids {
                    vectors.remove(&id);
                }
            }
        }
    }
    Ok(NamespaceVectorsForIdsLoadResult { vectors, stats })
}
