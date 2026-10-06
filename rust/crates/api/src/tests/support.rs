use async_trait::async_trait;
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Barrier, Mutex, RwLock},
    task::JoinHandle,
    time::sleep,
};
use tower::util::ServiceExt;
use turbo_vector_core::TurboVectorError;
use turbo_vector_storage::{ObjectStore, VersionedObject};

use crate::{
    filters::parser::FilterParserLimits,
    fts::{
        block_pack::{decode_block_pack, DecodedBlockPack},
        keyspace::{fts_block_pack_key, fts_index_meta_key, fts_term_meta_key},
        postings_codec::{decode_postings_block, DecodedPostingsBlock},
        term_meta::{FtsIndexMeta, TermMeta},
    },
    queue_broker::{InProcessQueueBrokerClient, QueueBrokerClient, UnavailableQueueBrokerClient},
    routes::app_router,
    state::{AppState, FilterCacheConfig, RuntimeConfigResponse},
    worker::run_upsert_worker,
};

#[derive(Debug, Default)]
pub(super) struct InMemoryObjectStore {
    objects: RwLock<BTreeMap<String, StoredObject>>,
}

#[derive(Debug, Clone)]
struct StoredObject {
    bytes: Vec<u8>,
    version: u64,
}

impl InMemoryObjectStore {
    pub(super) async fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        let objects = self.objects.read().await;
        let mut out: Vec<String> = objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect();
        out.sort();
        out
    }
}

#[async_trait]
impl ObjectStore for InMemoryObjectStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<()> {
        let mut objects = self.objects.write().await;
        let version = objects
            .get(key)
            .map_or(1, |stored| stored.version.saturating_add(1));
        objects.insert(
            key.to_string(),
            StoredObject {
                bytes: data.to_vec(),
                version,
            },
        );
        Ok(())
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<bool> {
        let mut objects = self.objects.write().await;
        if objects.contains_key(key) {
            return Ok(false);
        }
        objects.insert(
            key.to_string(),
            StoredObject {
                bytes: data.to_vec(),
                version: 1,
            },
        );
        Ok(true)
    }

    async fn get_bytes(&self, key: &str) -> turbo_vector_core::Result<Vec<u8>> {
        let objects = self.objects.read().await;
        objects
            .get(key)
            .map(|stored| stored.bytes.clone())
            .ok_or_else(|| TurboVectorError::NotFound(key.to_string()))
    }

    async fn get_bytes_with_version(
        &self,
        key: &str,
    ) -> turbo_vector_core::Result<Option<VersionedObject>> {
        let objects = self.objects.read().await;
        Ok(objects.get(key).map(|stored| VersionedObject {
            bytes: stored.bytes.clone(),
            version: stored.version.to_string(),
        }))
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_core::Result<bool> {
        let mut objects = self.objects.write().await;
        let current = objects.get(key).map(|stored| stored.version.to_string());
        let matches = match (current.as_deref(), expected_version) {
            (None, None) => true,
            (Some(current), Some(expected)) => current == expected,
            _ => false,
        };
        if !matches {
            return Ok(false);
        }
        let version = objects
            .get(key)
            .map_or(1, |stored| stored.version.saturating_add(1));
        objects.insert(
            key.to_string(),
            StoredObject {
                bytes: data.to_vec(),
                version,
            },
        );
        Ok(true)
    }

    async fn list_prefix(&self, prefix: &str) -> turbo_vector_core::Result<Vec<String>> {
        let objects = self.objects.read().await;
        let mut out: Vec<String> = objects
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect();
        out.sort();
        Ok(out)
    }

    async fn delete_bytes(&self, key: &str) -> turbo_vector_core::Result<()> {
        let mut objects = self.objects.write().await;
        objects.remove(key);
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct CountingGetStore {
    inner: Arc<InMemoryObjectStore>,
    get_counts: Mutex<BTreeMap<String, u64>>,
}

impl CountingGetStore {
    pub(super) fn new(inner: Arc<InMemoryObjectStore>) -> Self {
        Self {
            inner,
            get_counts: Mutex::new(BTreeMap::new()),
        }
    }

    pub(super) async fn get_count(&self, key: &str) -> u64 {
        let counts = self.get_counts.lock().await;
        counts.get(key).copied().unwrap_or(0)
    }
}

#[async_trait]
impl ObjectStore for CountingGetStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<()> {
        self.inner.put_bytes(key, data).await
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_if_absent(key, data).await
    }

    async fn get_bytes(&self, key: &str) -> turbo_vector_core::Result<Vec<u8>> {
        {
            let mut counts = self.get_counts.lock().await;
            let count = counts.entry(key.to_string()).or_insert(0);
            *count = count.saturating_add(1);
        }
        self.inner.get_bytes(key).await
    }

    async fn get_bytes_with_version(
        &self,
        key: &str,
    ) -> turbo_vector_core::Result<Option<VersionedObject>> {
        self.inner.get_bytes_with_version(key).await
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_cas(key, data, expected_version).await
    }

    async fn list_prefix(&self, prefix: &str) -> turbo_vector_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn delete_bytes(&self, key: &str) -> turbo_vector_core::Result<()> {
        self.inner.delete_bytes(key).await
    }
}

#[derive(Debug)]
pub(super) struct StaleCurrentPointerReadStore {
    inner: Arc<InMemoryObjectStore>,
    delay: Duration,
}

impl StaleCurrentPointerReadStore {
    pub(super) fn new(inner: Arc<InMemoryObjectStore>, delay: Duration) -> Self {
        Self { inner, delay }
    }
}

#[async_trait]
impl ObjectStore for StaleCurrentPointerReadStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<()> {
        self.inner.put_bytes(key, data).await
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_if_absent(key, data).await
    }

    async fn get_bytes(&self, key: &str) -> turbo_vector_core::Result<Vec<u8>> {
        if key.ends_with("/manifests/current.json") {
            // Return a snapshot captured before delay to simulate stale pointer reads.
            let snapshot = self.inner.get_bytes(key).await;
            tokio::time::sleep(self.delay).await;
            return snapshot;
        }
        self.inner.get_bytes(key).await
    }

    async fn get_bytes_with_version(
        &self,
        key: &str,
    ) -> turbo_vector_core::Result<Option<VersionedObject>> {
        if key.ends_with("/manifests/current.json") {
            let snapshot = self.inner.get_bytes_with_version(key).await;
            tokio::time::sleep(self.delay).await;
            return snapshot;
        }
        self.inner.get_bytes_with_version(key).await
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_cas(key, data, expected_version).await
    }

    async fn list_prefix(&self, prefix: &str) -> turbo_vector_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn delete_bytes(&self, key: &str) -> turbo_vector_core::Result<()> {
        self.inner.delete_bytes(key).await
    }
}

#[derive(Debug)]
pub(super) struct FailOnceGetStore {
    inner: Arc<InMemoryObjectStore>,
    pending_missing_keys: Mutex<BTreeSet<String>>,
}

impl FailOnceGetStore {
    pub(super) fn new(
        inner: Arc<InMemoryObjectStore>,
        keys: impl IntoIterator<Item = String>,
    ) -> Self {
        let pending_missing_keys = keys.into_iter().collect();
        Self {
            inner,
            pending_missing_keys: Mutex::new(pending_missing_keys),
        }
    }
}

#[async_trait]
impl ObjectStore for FailOnceGetStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<()> {
        self.inner.put_bytes(key, data).await
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_if_absent(key, data).await
    }

    async fn get_bytes(&self, key: &str) -> turbo_vector_core::Result<Vec<u8>> {
        let mut pending = self.pending_missing_keys.lock().await;
        if pending.remove(key) {
            return Err(TurboVectorError::NotFound(key.to_string()));
        }
        drop(pending);
        self.inner.get_bytes(key).await
    }

    async fn get_bytes_with_version(
        &self,
        key: &str,
    ) -> turbo_vector_core::Result<Option<VersionedObject>> {
        let mut pending = self.pending_missing_keys.lock().await;
        if pending.remove(key) {
            return Ok(None);
        }
        drop(pending);
        self.inner.get_bytes_with_version(key).await
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_cas(key, data, expected_version).await
    }

    async fn list_prefix(&self, prefix: &str) -> turbo_vector_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn delete_bytes(&self, key: &str) -> turbo_vector_core::Result<()> {
        self.inner.delete_bytes(key).await
    }
}

#[derive(Debug)]
pub(super) struct FailNTimesGetStore {
    inner: Arc<InMemoryObjectStore>,
    remaining_failures: Mutex<BTreeMap<String, usize>>,
}

impl FailNTimesGetStore {
    pub(super) fn new(inner: Arc<InMemoryObjectStore>) -> Self {
        Self {
            inner,
            remaining_failures: Mutex::new(BTreeMap::new()),
        }
    }

    pub(super) async fn set_failures(&self, key: String, failures: usize) {
        if failures == 0 {
            return;
        }
        let mut remaining = self.remaining_failures.lock().await;
        remaining.insert(key, failures);
    }
}

#[async_trait]
impl ObjectStore for FailNTimesGetStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<()> {
        self.inner.put_bytes(key, data).await
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_if_absent(key, data).await
    }

    async fn get_bytes(&self, key: &str) -> turbo_vector_core::Result<Vec<u8>> {
        let should_fail = {
            let mut remaining = self.remaining_failures.lock().await;
            let mut should_remove = false;
            let should_fail = if let Some(left) = remaining.get_mut(key) {
                *left = left.saturating_sub(1);
                should_remove = *left == 0;
                true
            } else {
                false
            };
            if should_remove {
                remaining.remove(key);
            }
            should_fail
        };
        if should_fail {
            return Err(TurboVectorError::NotFound(key.to_string()));
        }
        self.inner.get_bytes(key).await
    }

    async fn get_bytes_with_version(
        &self,
        key: &str,
    ) -> turbo_vector_core::Result<Option<VersionedObject>> {
        let should_fail = {
            let mut remaining = self.remaining_failures.lock().await;
            let mut should_remove = false;
            let should_fail = if let Some(left) = remaining.get_mut(key) {
                *left = left.saturating_sub(1);
                should_remove = *left == 0;
                true
            } else {
                false
            };
            if should_remove {
                remaining.remove(key);
            }
            should_fail
        };
        if should_fail {
            return Ok(None);
        }
        self.inner.get_bytes_with_version(key).await
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_cas(key, data, expected_version).await
    }

    async fn list_prefix(&self, prefix: &str) -> turbo_vector_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn delete_bytes(&self, key: &str) -> turbo_vector_core::Result<()> {
        self.inner.delete_bytes(key).await
    }
}

#[derive(Debug)]
pub(super) struct SegmentReadLagStore {
    inner: Arc<InMemoryObjectStore>,
    initial_segment_read_failures: usize,
    remaining_failures: Mutex<BTreeMap<String, usize>>,
}

impl SegmentReadLagStore {
    pub(super) fn new(
        inner: Arc<InMemoryObjectStore>,
        initial_segment_read_failures: usize,
    ) -> Self {
        Self {
            inner,
            initial_segment_read_failures,
            remaining_failures: Mutex::new(BTreeMap::new()),
        }
    }

    async fn should_fail_segment_read(&self, key: &str) -> bool {
        if self.initial_segment_read_failures == 0 || !key.contains("/segments/") {
            return false;
        }
        let mut remaining = self.remaining_failures.lock().await;
        let entry = remaining
            .entry(key.to_string())
            .or_insert(self.initial_segment_read_failures);
        if *entry == 0 {
            return false;
        }
        *entry = entry.saturating_sub(1);
        true
    }
}

#[async_trait]
impl ObjectStore for SegmentReadLagStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<()> {
        self.inner.put_bytes(key, data).await
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_if_absent(key, data).await
    }

    async fn get_bytes(&self, key: &str) -> turbo_vector_core::Result<Vec<u8>> {
        if self.should_fail_segment_read(key).await {
            return Err(TurboVectorError::NotFound(key.to_string()));
        }
        self.inner.get_bytes(key).await
    }

    async fn get_bytes_with_version(
        &self,
        key: &str,
    ) -> turbo_vector_core::Result<Option<VersionedObject>> {
        if self.should_fail_segment_read(key).await {
            return Ok(None);
        }
        self.inner.get_bytes_with_version(key).await
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_cas(key, data, expected_version).await
    }

    async fn list_prefix(&self, prefix: &str) -> turbo_vector_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn delete_bytes(&self, key: &str) -> turbo_vector_core::Result<()> {
        self.inner.delete_bytes(key).await
    }
}

#[derive(Debug)]
pub(super) struct FailOncePutIfAbsentStore {
    inner: Arc<InMemoryObjectStore>,
    target_key: String,
    failed: Mutex<bool>,
}

impl FailOncePutIfAbsentStore {
    pub(super) fn new(inner: Arc<InMemoryObjectStore>, target_key: String) -> Self {
        Self {
            inner,
            target_key,
            failed: Mutex::new(false),
        }
    }
}

#[async_trait]
impl ObjectStore for FailOncePutIfAbsentStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<()> {
        self.inner.put_bytes(key, data).await
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<bool> {
        if key == self.target_key {
            let mut failed = self.failed.lock().await;
            if !*failed {
                *failed = true;
                return Err(TurboVectorError::Storage(format!(
                    "injected put_bytes_if_absent failure for key '{key}'"
                )));
            }
        }
        self.inner.put_bytes_if_absent(key, data).await
    }

    async fn get_bytes(&self, key: &str) -> turbo_vector_core::Result<Vec<u8>> {
        self.inner.get_bytes(key).await
    }

    async fn get_bytes_with_version(
        &self,
        key: &str,
    ) -> turbo_vector_core::Result<Option<VersionedObject>> {
        self.inner.get_bytes_with_version(key).await
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_cas(key, data, expected_version).await
    }

    async fn list_prefix(&self, prefix: &str) -> turbo_vector_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn delete_bytes(&self, key: &str) -> turbo_vector_core::Result<()> {
        self.inner.delete_bytes(key).await
    }
}

#[derive(Debug)]
pub(super) struct DelayedFailOncePutIfAbsentStore {
    inner: Arc<InMemoryObjectStore>,
    target_key: String,
    delay: Duration,
    failed: Mutex<bool>,
}

impl DelayedFailOncePutIfAbsentStore {
    pub(super) fn new(
        inner: Arc<InMemoryObjectStore>,
        target_key: String,
        delay: Duration,
    ) -> Self {
        Self {
            inner,
            target_key,
            delay,
            failed: Mutex::new(false),
        }
    }
}

#[async_trait]
impl ObjectStore for DelayedFailOncePutIfAbsentStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<()> {
        self.inner.put_bytes(key, data).await
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<bool> {
        if key == self.target_key {
            let mut failed = self.failed.lock().await;
            if !*failed {
                *failed = true;
                drop(failed);
                sleep(self.delay).await;
                return Err(TurboVectorError::Storage(format!(
                    "injected delayed put_bytes_if_absent failure for key '{key}'"
                )));
            }
        }
        self.inner.put_bytes_if_absent(key, data).await
    }

    async fn get_bytes(&self, key: &str) -> turbo_vector_core::Result<Vec<u8>> {
        self.inner.get_bytes(key).await
    }

    async fn get_bytes_with_version(
        &self,
        key: &str,
    ) -> turbo_vector_core::Result<Option<VersionedObject>> {
        self.inner.get_bytes_with_version(key).await
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_core::Result<bool> {
        self.inner.put_bytes_cas(key, data, expected_version).await
    }

    async fn list_prefix(&self, prefix: &str) -> turbo_vector_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn delete_bytes(&self, key: &str) -> turbo_vector_core::Result<()> {
        self.inner.delete_bytes(key).await
    }
}

#[derive(Debug)]
pub(super) struct KeyWriteBarrierStore {
    inner: Arc<InMemoryObjectStore>,
    barrier: Arc<Barrier>,
    key_suffix: String,
    pending_waits: Mutex<usize>,
}

impl KeyWriteBarrierStore {
    pub(super) fn new(inner: Arc<InMemoryObjectStore>, key_suffix: String, waiters: usize) -> Self {
        Self {
            inner,
            barrier: Arc::new(Barrier::new(waiters)),
            key_suffix,
            pending_waits: Mutex::new(waiters),
        }
    }
}

#[async_trait]
impl ObjectStore for KeyWriteBarrierStore {
    async fn put_bytes(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<()> {
        let should_wait = if key.ends_with(&self.key_suffix) {
            let mut pending = self.pending_waits.lock().await;
            if *pending > 0 {
                *pending -= 1;
                true
            } else {
                false
            }
        } else {
            false
        };

        if should_wait {
            self.barrier.wait().await;
        }
        self.inner.put_bytes(key, data).await
    }

    async fn put_bytes_if_absent(&self, key: &str, data: &[u8]) -> turbo_vector_core::Result<bool> {
        let should_wait = if key.ends_with(&self.key_suffix) {
            let mut pending = self.pending_waits.lock().await;
            if *pending > 0 {
                *pending -= 1;
                true
            } else {
                false
            }
        } else {
            false
        };

        if should_wait {
            self.barrier.wait().await;
        }
        self.inner.put_bytes_if_absent(key, data).await
    }

    async fn get_bytes(&self, key: &str) -> turbo_vector_core::Result<Vec<u8>> {
        self.inner.get_bytes(key).await
    }

    async fn get_bytes_with_version(
        &self,
        key: &str,
    ) -> turbo_vector_core::Result<Option<VersionedObject>> {
        self.inner.get_bytes_with_version(key).await
    }

    async fn put_bytes_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> turbo_vector_core::Result<bool> {
        let should_wait = if key.ends_with(&self.key_suffix) {
            let mut pending = self.pending_waits.lock().await;
            if *pending > 0 {
                *pending -= 1;
                true
            } else {
                false
            }
        } else {
            false
        };

        if should_wait {
            self.barrier.wait().await;
        }
        self.inner.put_bytes_cas(key, data, expected_version).await
    }

    async fn list_prefix(&self, prefix: &str) -> turbo_vector_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix).await
    }

    async fn delete_bytes(&self, key: &str) -> turbo_vector_core::Result<()> {
        self.inner.delete_bytes(key).await
    }
}

#[derive(Debug, Clone, Copy)]
enum QueueClientMode {
    InProcess,
    Unavailable,
}

pub(super) struct TestApi {
    app: Router,
    pub(super) store: Arc<InMemoryObjectStore>,
    storage: Arc<dyn ObjectStore>,
    queue_client_mode: QueueClientMode,
    distributed_shard_count_override: Option<usize>,
    distributed_shard_timeout_ms_override: Option<u64>,
    distributed_fail_open_override: Option<bool>,
    distributed_required_successful_shards_override: Option<usize>,
}

fn build_test_state(
    storage: Arc<dyn ObjectStore>,
    node_id: &str,
    drain_interval: Option<Duration>,
    queue_client_mode: QueueClientMode,
    ann_object_read_budget_override: Option<usize>,
    ann_tree_root_beam_override: Option<usize>,
    ann_tree_leaf_probe_count_override: Option<usize>,
    distributed_shard_count_override: Option<usize>,
    distributed_shard_timeout_ms_override: Option<u64>,
    distributed_fail_open_override: Option<bool>,
    distributed_required_successful_shards_override: Option<usize>,
) -> AppState {
    let wal_queue_background_drain_enabled = drain_interval.is_some();
    let wal_queue_background_drain_interval = drain_interval.unwrap_or(Duration::from_secs(1));
    let wal_queue_background_drain_interval_ms =
        wal_queue_background_drain_interval.as_millis() as u64;
    let queue_client: Arc<dyn QueueBrokerClient> = match queue_client_mode {
        QueueClientMode::InProcess => Arc::new(InProcessQueueBrokerClient::new(
            storage.clone(),
            node_id.to_string(),
        )),
        QueueClientMode::Unavailable => Arc::new(UnavailableQueueBrokerClient::new(
            "test queue broker intentionally unavailable",
        )),
    };
    let queue_broker_url = match queue_client_mode {
        QueueClientMode::InProcess => "inprocess://queue-broker".to_string(),
        QueueClientMode::Unavailable => "http://unavailable-broker.test".to_string(),
    };
    let ann_object_read_budget = ann_object_read_budget_override.unwrap_or(512);
    let ann_tree_root_beam = ann_tree_root_beam_override.unwrap_or(4);
    let ann_tree_leaf_probe_count = ann_tree_leaf_probe_count_override.unwrap_or(16);
    let distributed_shard_count = distributed_shard_count_override.unwrap_or(1).max(1);
    let distributed_shard_timeout_ms = distributed_shard_timeout_ms_override.unwrap_or(250).max(1);
    let distributed_fail_open = distributed_fail_open_override.unwrap_or(true);
    let distributed_required_successful_shards = distributed_required_successful_shards_override
        .unwrap_or(distributed_shard_count.saturating_sub(1).max(1))
        .clamp(1, distributed_shard_count);
    let namespace_cache_max_entries = 1_024;
    let namespace_cache_max_bytes = 64 * 1024 * 1024;
    let ann_bucket_cache_max_entries = 2_048;
    let ann_bucket_cache_max_bytes = 128 * 1024 * 1024;

    AppState {
        node_id: node_id.to_string(),
        service_name: "turbo-vector-test".to_string(),
        runtime: RuntimeConfigResponse {
            bind_addr: "127.0.0.1:0".to_string(),
            api_body_limit_bytes: 32 * 1024 * 1024, // 32MB
            storage_provider: "memory".to_string(),
            storage_endpoint: "memory://store".to_string(),
            storage_region: "test-region".to_string(),
            storage_bucket: "test-bucket".to_string(),
            storage_simulated_latency_ms: 0,
            wal_queue_background_drain_enabled,
            wal_queue_background_drain_interval_ms,
            queue_broker_url,
            wal_worker_build_fts: true,
            wal_worker_build_ann: true,
            query_ann_enabled: true,
            ann_bucket_fetch_concurrency: 32,
            ann_tree_root_beam,
            ann_tree_leaf_probe_count,
            ann_object_read_budget,
            ann_quantization_bound_margin: 16.0,
            ann_rerank_prune_ratio: 0.05,
            ann_rerank_max_candidates: 2_048,
            ann_rerank_ssd_cache_dir: "/tmp/turbo-vector-test-ann-rerank-cache".to_string(),
            ann_rerank_ssd_cache_max_entries: 8_192,
            ann_rerank_ssd_cache_max_bytes: 64 * 1024 * 1024,
            namespace_cache_max_entries,
            namespace_cache_max_bytes,
            ann_bucket_cache_max_entries,
            ann_bucket_cache_max_bytes,
            filter_cluster_cache_max_entries: 1_024,
            filter_cluster_cache_max_bytes: 16 * 1024 * 1024,
            filter_row_cache_max_entries: 8_192,
            filter_row_cache_max_bytes: 64 * 1024 * 1024,
            filter_max_regex_bytes: 512,
            filter_max_glob_bytes: 512,
            filter_max_widen_passes: 4,
            fts_block_target_postings: 256,
            fts_block_split_threshold: 512,
            fts_block_merge_threshold: 128,
            fts_max_term_blocks_touched_per_doc_update: 8,
            fts_enable_delta_rebalance: true,
            fts_prefix_max_index_chars: 8,
            fts_prefix_max_expansions: 64,
            fts_prefix_max_expansion_bytes: 4 * 1024,
            fts_explain_query_enabled: true,
            fts_explain_max_top_k: 100,
            distributed_shard_count,
            distributed_shard_timeout_ms,
            distributed_fail_open,
            distributed_required_successful_shards,
            viz_enabled: true,
            otel_enabled: false,
            otel_exporter_otlp_endpoint: "http://127.0.0.1:4318".to_string(),
            otel_service_name: "turbo-vector-test".to_string(),
            otel_metric_export_interval_ms: 5_000,
            otel_sample_ratio: 0.05,
            filter_cache_metrics: Default::default(),
        },
        storage,
        queue_client,
        manifest_write_locks: AppState::new_manifest_lock_registry(),
        ann_index_build_locks: AppState::new_ann_index_lock_registry(),
        fts_index_build_locks: AppState::new_fts_index_lock_registry(),
        wal_queue_handles: AppState::new_wal_queue_registry(),
        wal_queue_drain_workers: AppState::new_wal_queue_drain_worker_registry(),
        query_ann_enabled: true,
        ann_bucket_fetch_semaphore: AppState::new_ann_bucket_fetch_semaphore(32),
        ann_tree_root_beam,
        ann_tree_leaf_probe_count,
        ann_object_read_budget,
        ann_quantization_bound_margin: 16.0,
        ann_rerank_prune_ratio: 0.05,
        ann_rerank_max_candidates: 2_048,
        distributed_shard_count,
        distributed_shard_timeout_ms,
        distributed_fail_open,
        distributed_required_successful_shards,
        filter_parser_limits: FilterParserLimits {
            max_regex_bytes: 512,
            max_glob_bytes: 512,
        },
        filter_max_widen_passes: 4,
        namespace_vector_cache: AppState::new_namespace_cache(FilterCacheConfig::new(
            namespace_cache_max_entries,
            namespace_cache_max_bytes,
        )),
        ann_bucket_cache: AppState::new_ann_bucket_cache(FilterCacheConfig::new(
            ann_bucket_cache_max_entries,
            ann_bucket_cache_max_bytes,
        )),
        ann_rerank_ssd_cache: AppState::new_ann_rerank_ssd_cache(
            FilterCacheConfig::new(8_192, 64 * 1024 * 1024),
            "/tmp/turbo-vector-test-ann-rerank-cache".to_string(),
        ),
        filter_cluster_cache: AppState::new_filter_cluster_cache(FilterCacheConfig::new(
            1_024,
            16 * 1024 * 1024,
        )),
        filter_row_cache: AppState::new_filter_row_cache(FilterCacheConfig::new(
            8_192,
            64 * 1024 * 1024,
        )),
        manifest_cache: AppState::new_manifest_cache(),
        collection_metadata_cache: AppState::new_collection_metadata_cache(),
        fts_index_meta_cache: AppState::new_fts_index_meta_cache(),
        fts_doc_lookup_cache: AppState::new_fts_doc_lookup_cache(),
        shard_placement_cache: AppState::new_shard_placement_cache(),
        ensured_collection_registry_markers: AppState::new_collection_registry_marker_cache(),
    }
}

impl TestApi {
    pub(super) fn new() -> Self {
        let store = Arc::new(InMemoryObjectStore::default());
        let storage = store.clone() as Arc<dyn ObjectStore>;
        Self::new_with_storage(storage, store)
    }

    pub(super) fn new_without_queue_broker() -> Self {
        let store = Arc::new(InMemoryObjectStore::default());
        let storage = store.clone() as Arc<dyn ObjectStore>;
        Self::new_with_storage_and_queue_mode_and_ann_budget(
            storage,
            store,
            None,
            QueueClientMode::Unavailable,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    pub(super) fn new_with_storage(
        storage: Arc<dyn ObjectStore>,
        store: Arc<InMemoryObjectStore>,
    ) -> Self {
        Self::new_with_storage_and_queue_mode_and_ann_budget(
            storage,
            store,
            None,
            QueueClientMode::InProcess,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    pub(super) fn new_with_storage_and_ann_object_read_budget(
        storage: Arc<dyn ObjectStore>,
        store: Arc<InMemoryObjectStore>,
        ann_object_read_budget: usize,
    ) -> Self {
        Self::new_with_storage_and_queue_mode_and_ann_budget(
            storage,
            store,
            None,
            QueueClientMode::InProcess,
            Some(ann_object_read_budget),
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    pub(super) fn new_with_storage_and_ann_tuning(
        storage: Arc<dyn ObjectStore>,
        store: Arc<InMemoryObjectStore>,
        ann_object_read_budget: usize,
        ann_tree_root_beam: usize,
        ann_tree_leaf_probe_count: usize,
    ) -> Self {
        Self::new_with_storage_and_queue_mode_and_ann_budget(
            storage,
            store,
            None,
            QueueClientMode::InProcess,
            Some(ann_object_read_budget),
            Some(ann_tree_root_beam),
            Some(ann_tree_leaf_probe_count),
            None,
            None,
            None,
            None,
        )
    }

    pub(super) fn new_with_distributed_sharding(
        shard_count: usize,
        shard_timeout_ms: u64,
        fail_open: bool,
        required_successful_shards: usize,
    ) -> Self {
        let store = Arc::new(InMemoryObjectStore::default());
        let storage = store.clone() as Arc<dyn ObjectStore>;
        Self::new_with_storage_and_queue_mode_and_ann_budget(
            storage,
            store,
            None,
            QueueClientMode::InProcess,
            None,
            None,
            None,
            Some(shard_count),
            Some(shard_timeout_ms),
            Some(fail_open),
            Some(required_successful_shards),
        )
    }

    fn new_with_storage_and_queue_mode_and_ann_budget(
        storage: Arc<dyn ObjectStore>,
        store: Arc<InMemoryObjectStore>,
        drain_interval: Option<Duration>,
        queue_client_mode: QueueClientMode,
        ann_object_read_budget_override: Option<usize>,
        ann_tree_root_beam_override: Option<usize>,
        ann_tree_leaf_probe_count_override: Option<usize>,
        distributed_shard_count_override: Option<usize>,
        distributed_shard_timeout_ms_override: Option<u64>,
        distributed_fail_open_override: Option<bool>,
        distributed_required_successful_shards_override: Option<usize>,
    ) -> Self {
        let state = build_test_state(
            storage.clone(),
            "test-api",
            drain_interval,
            queue_client_mode,
            ann_object_read_budget_override,
            ann_tree_root_beam_override,
            ann_tree_leaf_probe_count_override,
            distributed_shard_count_override,
            distributed_shard_timeout_ms_override,
            distributed_fail_open_override,
            distributed_required_successful_shards_override,
        );
        Self {
            app: app_router(state),
            store,
            storage,
            queue_client_mode,
            distributed_shard_count_override,
            distributed_shard_timeout_ms_override,
            distributed_fail_open_override,
            distributed_required_successful_shards_override,
        }
    }

    pub(super) async fn request(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
        headers: &[(&str, &str)],
    ) -> (StatusCode, Value) {
        let mut request_builder = Request::builder().method(method).uri(path);
        for (name, value) in headers {
            request_builder = request_builder.header(*name, *value);
        }

        let request = if let Some(payload) = body {
            request_builder = request_builder.header("content-type", "application/json");
            request_builder
                .body(Body::from(
                    serde_json::to_vec(&payload).expect("serialize request body"),
                ))
                .expect("build request with JSON body")
        } else {
            request_builder
                .body(Body::empty())
                .expect("build request without body")
        };

        let response = self
            .app
            .clone()
            .oneshot(request)
            .await
            .expect("router request should succeed");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        let payload = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("parse JSON response body")
        };
        (status, payload)
    }

    pub(super) async fn create_collection(&self, name: &str, dimension: u32, metric: &str) {
        let (status, body) = self
            .request(
                Method::POST,
                "/v1/collections",
                Some(json!({
                    "name": name,
                    "dimension": dimension,
                    "metric": metric
                })),
                &[],
            )
            .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["name"], name);
        assert_eq!(body["dimension"], dimension);
    }

    pub(super) async fn upsert(
        &self,
        collection: &str,
        payload: Value,
        headers: &[(&str, &str)],
    ) -> (StatusCode, Value) {
        self.request(
            Method::POST,
            &format!("/v1/collections/{collection}/vectors/upsert"),
            Some(payload),
            headers,
        )
        .await
    }

    pub(super) async fn upsert_and_wait_applied(
        &self,
        collection: &str,
        payload: Value,
        headers: &[(&str, &str)],
    ) -> (StatusCode, Value) {
        let (status, body) = self.upsert(collection, payload, headers).await;
        if status == StatusCode::OK {
            if let Some(operation_id) = body.get("operation_id").and_then(Value::as_str) {
                let helper_worker = self.spawn_worker(
                    "test-helper-worker",
                    Duration::from_millis(5),
                    Duration::from_millis(10),
                    0,
                    Duration::from_millis(5),
                    false,
                    false,
                );
                self.wait_for_operation_applied(
                    collection,
                    operation_id,
                    Duration::from_secs(5),
                    Duration::from_millis(25),
                )
                .await;
                helper_worker.abort();
            }
        }
        (status, body)
    }

    pub(super) async fn query(&self, collection: &str, payload: Value) -> (StatusCode, Value) {
        self.request(
            Method::POST,
            &format!("/v1/collections/{collection}/vectors/query"),
            Some(payload),
            &[],
        )
        .await
    }

    pub(super) async fn delete_vectors(
        &self,
        collection: &str,
        payload: Value,
    ) -> (StatusCode, Value) {
        self.request(
            Method::POST,
            &format!("/v1/collections/{collection}/vectors/delete"),
            Some(payload),
            &[],
        )
        .await
    }

    pub(super) async fn stats(
        &self,
        collection: &str,
        namespace: Option<&str>,
    ) -> (StatusCode, Value) {
        let mut path = format!("/v1/collections/{collection}/stats");
        if let Some(namespace) = namespace {
            path = format!("{path}?namespace={namespace}");
        }
        self.request(Method::GET, &path, None, &[]).await
    }

    pub(super) async fn operation_status(
        &self,
        collection: &str,
        operation_id: &str,
    ) -> (StatusCode, Value) {
        self.request(
            Method::GET,
            &format!("/v1/collections/{collection}/operations/{operation_id}"),
            None,
            &[],
        )
        .await
    }

    pub(super) async fn wait_for_operation_applied(
        &self,
        collection: &str,
        operation_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Value {
        let attempts = ((timeout.as_millis() / poll_interval.as_millis().max(1)) as usize).max(1);
        for _ in 0..attempts {
            let (status, body) = self.operation_status(collection, operation_id).await;
            if status == StatusCode::OK && body["status"] == "applied" {
                return body;
            }
            sleep(poll_interval).await;
        }
        panic!(
            "operation '{operation_id}' for collection '{collection}' did not become applied within {:?}",
            timeout
        );
    }

    pub(super) async fn wait_for_vector_count(
        &self,
        collection: &str,
        namespace: Option<&str>,
        expected_vector_count: u64,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Value {
        let attempts = ((timeout.as_millis() / poll_interval.as_millis().max(1)) as usize).max(1);
        let mut last_status = None;
        let mut last_body = Value::Null;
        for _ in 0..attempts {
            let (status, body) = self.stats(collection, namespace).await;
            last_status = Some(status);
            last_body = body.clone();
            if status == StatusCode::OK
                && body["vector_count"]
                    .as_u64()
                    .is_some_and(|count| count >= expected_vector_count)
            {
                return body;
            }
            sleep(poll_interval).await;
        }
        panic!(
            "collection '{collection}' namespace {:?} did not reach vector_count >= {} within {:?}; last_status={:?}, last_body={:?}",
            namespace, expected_vector_count, timeout, last_status, last_body
        );
    }

    pub(super) async fn wait_for_fts_index_meta(
        &self,
        collection: &str,
        namespace: &str,
        generation: u64,
        timeout: Duration,
        poll_interval: Duration,
    ) -> FtsIndexMeta {
        let key = fts_index_meta_key(collection, namespace, generation);
        let attempts = ((timeout.as_millis() / poll_interval.as_millis().max(1)) as usize).max(1);
        for _ in 0..attempts {
            if let Ok(raw) = self.store.get_bytes(&key).await {
                return serde_json::from_slice(&raw).expect("parse fts index meta");
            }
            sleep(poll_interval).await;
        }
        panic!(
            "fts index metadata '{}' was not readable within {:?}",
            key, timeout
        );
    }

    pub(super) async fn load_fts_term_meta(
        &self,
        collection: &str,
        namespace: &str,
        generation: u64,
        field_hash: &str,
        term_hash: &str,
    ) -> TermMeta {
        let key = fts_term_meta_key(collection, namespace, generation, field_hash, term_hash);
        let raw = self
            .store
            .get_bytes(&key)
            .await
            .expect("fts term metadata should exist");
        serde_json::from_slice(&raw).expect("parse fts term metadata")
    }

    pub(super) async fn load_decode_fts_term_blocks(
        &self,
        collection: &str,
        namespace: &str,
        field_hash: &str,
        term_hash: &str,
        term_meta: &TermMeta,
    ) -> Vec<DecodedPostingsBlock> {
        let mut out = Vec::with_capacity(term_meta.blocks.len());
        let mut pack_cache: BTreeMap<(u64, String), DecodedBlockPack> = BTreeMap::new();
        for descriptor in &term_meta.blocks {
            let pack_cache_key = (descriptor.generation, descriptor.pack_id.clone());
            if !pack_cache.contains_key(&pack_cache_key) {
                let pack_key = fts_block_pack_key(
                    collection,
                    namespace,
                    descriptor.generation,
                    field_hash,
                    term_hash,
                    &descriptor.pack_id,
                );
                let raw = self
                    .store
                    .get_bytes(&pack_key)
                    .await
                    .expect("fts postings pack should exist");
                let decoded_pack = decode_block_pack(&raw).expect("decode postings pack");
                pack_cache.insert(pack_cache_key.clone(), decoded_pack);
            }
            let pack = pack_cache
                .get(&pack_cache_key)
                .expect("pack cache entry should exist");
            let raw = pack
                .block_slice(
                    &descriptor.block_id,
                    descriptor.pack_offset,
                    descriptor.pack_len,
                )
                .expect("pack block slice should exist");
            let decoded = decode_postings_block(raw).expect("decode postings block");
            out.push(decoded);
        }
        out
    }

    pub(super) fn spawn_worker(
        &self,
        node_id: &str,
        flush_interval: Duration,
        collection_refresh_interval: Duration,
        adaptive_backlog_threshold: usize,
        adaptive_flush_interval: Duration,
        build_fts_indexes: bool,
        build_ann_indexes: bool,
    ) -> JoinHandle<()> {
        let state = build_test_state(
            self.storage.clone(),
            node_id,
            None,
            self.queue_client_mode,
            None,
            None,
            None,
            self.distributed_shard_count_override,
            self.distributed_shard_timeout_ms_override,
            self.distributed_fail_open_override,
            self.distributed_required_successful_shards_override,
        );
        tokio::spawn(async move {
            run_upsert_worker(
                state,
                flush_interval,
                collection_refresh_interval,
                adaptive_backlog_threshold,
                adaptive_flush_interval,
                build_fts_indexes,
                build_ann_indexes,
            )
            .await;
        })
    }
}
