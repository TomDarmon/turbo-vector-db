use chrono::Utc;
use sha2::{Digest, Sha256};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    sync::OnceLock,
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) fn collection_metadata_key(collection: &str) -> String {
    format!("collections/{collection}/metadata.json")
}

pub(crate) const COLLECTION_REGISTRY_PREFIX: &str = "collections/_registry/";

pub(crate) fn collection_registry_key(collection: &str) -> String {
    format!("{COLLECTION_REGISTRY_PREFIX}{collection}.json")
}

pub(crate) fn current_pointer_key(collection: &str) -> String {
    format!("collections/{collection}/manifests/current.json")
}

pub(crate) fn manifest_generation_key(collection: &str, generation: u64) -> String {
    format!("collections/{collection}/manifests/{generation}.json")
}

pub(crate) fn segment_object_key(collection: &str, segment_id: &str) -> String {
    format!("collections/{collection}/segments/{segment_id}.json")
}

pub(crate) fn wal_object_key(collection: &str, operation_id: &str) -> String {
    format!("collections/{collection}/wal/{operation_id}.json")
}

pub(crate) fn operation_status_object_key(collection: &str, operation_id: &str) -> String {
    format!("collections/{collection}/operations/{operation_id}.json")
}

pub(crate) fn wal_queue_object_key(collection: &str) -> String {
    format!("collections/{collection}/queue/wal.json")
}

pub(crate) fn collection_shard_placement_key(collection: &str) -> String {
    format!("collections/{collection}/shards/placement.json")
}

pub(crate) fn ann_index_meta_key(collection: &str, namespace: &str, generation: u64) -> String {
    format!("collections/{collection}/ann/{namespace}/{generation}/meta.json")
}

pub(crate) fn ann_bucket_object_key(
    collection: &str,
    namespace: &str,
    generation: u64,
    bucket_id: usize,
) -> String {
    format!("collections/{collection}/ann/{namespace}/{generation}/buckets/{bucket_id}.bin")
}

pub(crate) fn ann_filter_cluster_object_key(
    collection: &str,
    namespace: &str,
    generation: u64,
    term_hash: &str,
) -> String {
    format!("collections/{collection}/ann/{namespace}/{generation}/filters/cluster/{term_hash}.bin")
}

pub(crate) fn ann_filter_row_object_key(
    collection: &str,
    namespace: &str,
    generation: u64,
    term_hash: &str,
    bucket_id: usize,
) -> String {
    format!(
        "collections/{collection}/ann/{namespace}/{generation}/filters/row/{term_hash}/{bucket_id}.bin"
    )
}

pub(crate) fn idempotency_object_key(collection: &str, idempotency_key: &str) -> String {
    let hashed_key = sha256_hex(idempotency_key.as_bytes());
    format!("collections/{collection}/idempotency/{hashed_key}.json")
}

pub(crate) fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hex::encode(hasher.finalize())
}

pub(crate) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

pub(crate) fn new_operation_id() -> String {
    static PROCESS_START_NANOS: OnceLock<u128> = OnceLock::new();
    static OPERATION_COUNTER: AtomicU64 = AtomicU64::new(0);

    let start_nanos = *PROCESS_START_NANOS.get_or_init(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0)
    });
    let counter = OPERATION_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{start_nanos:032x}-{counter:016x}")
}
