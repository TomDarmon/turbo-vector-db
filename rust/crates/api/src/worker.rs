use std::collections::BTreeSet;

use axum::http::StatusCode;
use tokio::time::{sleep, Duration, Instant};
use tracing::{debug, info, warn};

use crate::{
    ann::ensure_ann_indexes_for_manifest,
    error::{map_store_error, ApiError},
    fts::ensure_fts_indexes_for_manifest,
    keys::COLLECTION_REGISTRY_PREFIX,
    state::AppState,
    storage_logic::{
        flush_wal_queue_with_report, load_collection_metadata, load_current_manifest,
        WalQueueFlushReport,
    },
    telemetry,
};

fn parse_collection_from_registry_key(key: &str) -> Option<&str> {
    let suffix = ".json";
    if !key.starts_with(COLLECTION_REGISTRY_PREFIX) || !key.ends_with(suffix) {
        return None;
    }

    let collection = &key[COLLECTION_REGISTRY_PREFIX.len()..key.len().saturating_sub(suffix.len())];
    if collection.is_empty() || collection.contains('/') {
        return None;
    }
    Some(collection)
}

async fn refresh_known_collections(state: &AppState) -> Result<BTreeSet<String>, ApiError> {
    let keys = state
        .storage
        .list_prefix(COLLECTION_REGISTRY_PREFIX)
        .await
        .map_err(map_store_error)?;
    let collections = keys
        .into_iter()
        .filter_map(|key| parse_collection_from_registry_key(&key).map(ToString::to_string))
        .collect();
    Ok(collections)
}

async fn process_collection(
    state: &AppState,
    collection: &str,
    build_fts_indexes: bool,
    build_ann_indexes: bool,
) -> Result<WalQueueFlushReport, ApiError> {
    let flush_report = flush_wal_queue_with_report(state, collection, None).await?;
    if flush_report.applied_jobs > 0 {
        info!(
            collection,
            generation = flush_report.generation,
            queue_depth = flush_report.queue_depth,
            pending_jobs = flush_report.pending_jobs,
            applied_jobs = flush_report.applied_jobs,
            acked_jobs = flush_report.acked_jobs,
            "worker applied WAL operations"
        );
    } else {
        debug!(
            collection,
            generation = flush_report.generation,
            queue_depth = flush_report.queue_depth,
            pending_jobs = flush_report.pending_jobs,
            applied_jobs = flush_report.applied_jobs,
            acked_jobs = flush_report.acked_jobs,
            "worker flush cycle (no new operations)"
        );
    }
    telemetry::increment_worker_flush_applied_jobs(
        &state.service_name,
        flush_report.applied_jobs as u64,
    );

    if !build_fts_indexes && !build_ann_indexes {
        return Ok(flush_report);
    }

    let Some(manifest) = load_current_manifest(state, collection).await? else {
        return Ok(flush_report);
    };
    if build_fts_indexes {
        ensure_fts_indexes_for_manifest(state, collection, &manifest).await?;
    }
    if build_ann_indexes {
        let metadata = match load_collection_metadata(state, collection).await {
            Ok(metadata) => metadata,
            Err(error) if error.status == StatusCode::NOT_FOUND => return Ok(flush_report),
            Err(error) => return Err(error),
        };
        ensure_ann_indexes_for_manifest(
            state,
            collection,
            &manifest,
            &metadata.metric,
            metadata.dimension,
        )
        .await?;
    }
    Ok(flush_report)
}

pub(crate) async fn run_upsert_worker(
    state: AppState,
    flush_interval: Duration,
    collection_refresh_interval: Duration,
    adaptive_backlog_threshold: usize,
    adaptive_flush_interval: Duration,
    build_fts_indexes: bool,
    build_ann_indexes: bool,
) {
    let flush_interval = flush_interval.max(Duration::from_millis(1));
    let collection_refresh_interval = collection_refresh_interval.max(Duration::from_millis(1));
    let adaptive_flush_interval = adaptive_flush_interval
        .max(Duration::from_millis(1))
        .min(flush_interval);
    let adaptive_enabled =
        adaptive_backlog_threshold > 0 && adaptive_flush_interval < flush_interval;
    let mut next_sleep = flush_interval;

    let mut known_collections = BTreeSet::new();
    let mut next_collection_refresh = Instant::now();
    info!(
        flush_interval_ms = flush_interval.as_millis() as u64,
        collection_refresh_interval_ms = collection_refresh_interval.as_millis() as u64,
        adaptive_backlog_threshold,
        adaptive_flush_interval_ms = adaptive_flush_interval.as_millis() as u64,
        adaptive_enabled,
        build_fts_indexes,
        build_ann_indexes,
        "starting upsert queue worker"
    );

    loop {
        sleep(next_sleep).await;

        if known_collections.is_empty() || Instant::now() >= next_collection_refresh {
            match refresh_known_collections(&state).await {
                Ok(refreshed) => {
                    known_collections = refreshed;
                    next_collection_refresh = Instant::now() + collection_refresh_interval;
                }
                Err(error) => {
                    warn!(error = ?error, "upsert worker failed to refresh collection list");
                    continue;
                }
            }
        }

        let mut total_queue_depth = 0usize;
        for collection in &known_collections {
            match process_collection(&state, collection, build_fts_indexes, build_ann_indexes).await
            {
                Ok(report) => {
                    total_queue_depth = total_queue_depth.saturating_add(report.queue_depth);
                }
                Err(error) => {
                    warn!(
                        collection,
                        error = ?error,
                        "upsert worker failed to process collection"
                    );
                }
            }
        }
        telemetry::record_queue_depth(&state.service_name, total_queue_depth);

        if adaptive_enabled && total_queue_depth >= adaptive_backlog_threshold {
            next_sleep = adaptive_flush_interval;
            debug!(
                queue_depth = total_queue_depth,
                next_flush_interval_ms = next_sleep.as_millis() as u64,
                "upsert worker adaptive flush cadence engaged"
            );
        } else {
            next_sleep = flush_interval;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_collection_from_registry_key;

    #[test]
    fn parse_collection_registry_key_accepts_expected_shape() {
        assert_eq!(
            parse_collection_from_registry_key("collections/_registry/docs.json"),
            Some("docs")
        );
    }

    #[test]
    fn parse_collection_registry_key_rejects_invalid_shapes() {
        assert_eq!(parse_collection_from_registry_key("collections/docs"), None);
        assert_eq!(
            parse_collection_from_registry_key("collections/_registry/a/b.json"),
            None
        );
        assert_eq!(parse_collection_from_registry_key("other/docs.json"), None);
    }
}
