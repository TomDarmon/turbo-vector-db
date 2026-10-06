use roaring::RoaringBitmap;
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, OwnedMutexGuard, RwLock, Semaphore},
    task::JoinHandle,
};
use turbo_vector_manifest::Manifest;
use turbo_vector_queue::{spawn_queue_broker, QueueBrokerConfig, QueueHandle};
use turbo_vector_storage::ObjectStore;
use utoipa::ToSchema;

use crate::models::{CollectionMetadata, UpsertVector};
use crate::queue_broker::QueueBrokerClient;
use crate::queue_store_adapter::ObjectStoreCasAdapter;
use crate::{
    ann::AnnBucketData,
    distributed::{self, CollectionShardPlacement},
    filters::{parser::FilterParserLimits, planner::NativeFilterClusterSummary},
    fts::term_meta::{FtsDocLookup, FtsIndexMeta},
    keys::{sha256_hex, wal_queue_object_key},
    telemetry,
};

const WAL_QUEUE_LEASE_TIMEOUT_MS: u64 = 30_000;
const WAL_QUEUE_CHANNEL_CAPACITY: usize = 4_096;
const WAL_QUEUE_MAX_BATCH_WAIT_MS: u64 = 20;
const WAL_QUEUE_MIN_COMMIT_INTERVAL_MS: u64 = 1_100;
const WAL_QUEUE_MAX_CAS_RETRIES: usize = 64;

#[derive(Clone)]
pub(crate) struct NamespaceVectorCacheEntry {
    pub(crate) generation: u64,
    pub(crate) vectors: Arc<BTreeMap<String, UpsertVector>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct FilterCacheConfig {
    pub(crate) max_entries: usize,
    pub(crate) max_bytes: usize,
}

impl FilterCacheConfig {
    pub(crate) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            max_entries: max_entries.max(1),
            max_bytes: max_bytes.max(1),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FilterCacheCounters {
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) evictions: u64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, utoipa::ToSchema)]
pub(crate) struct FilterCacheMetricsSnapshot {
    pub(crate) cluster_hits: u64,
    pub(crate) cluster_misses: u64,
    pub(crate) cluster_evictions: u64,
    pub(crate) row_hits: u64,
    pub(crate) row_misses: u64,
    pub(crate) row_evictions: u64,
}

#[derive(Clone)]
struct CacheEntry<V> {
    value: V,
    size_bytes: usize,
}

pub(crate) struct BoundedFilterCache<V> {
    entries: BTreeMap<String, CacheEntry<V>>,
    lru: VecDeque<String>,
    current_bytes: usize,
    config: FilterCacheConfig,
    counters: FilterCacheCounters,
}

impl<V: Clone> BoundedFilterCache<V> {
    fn new(config: FilterCacheConfig) -> Self {
        Self {
            entries: BTreeMap::new(),
            lru: VecDeque::new(),
            current_bytes: 0,
            config,
            counters: FilterCacheCounters::default(),
        }
    }

    fn get(&mut self, key: &str) -> Option<V> {
        if let Some(entry) = self.entries.get(key).cloned() {
            self.counters.hits = self.counters.hits.saturating_add(1);
            self.touch_key(key);
            return Some(entry.value);
        }
        self.counters.misses = self.counters.misses.saturating_add(1);
        None
    }

    fn insert(&mut self, key: String, value: V, size_bytes: usize) -> u64 {
        let entry_size = size_bytes
            .saturating_add(key.len())
            .saturating_add(std::mem::size_of::<CacheEntry<V>>());
        if entry_size > self.config.max_bytes {
            let evicted = self.entries.len() as u64;
            self.entries.clear();
            self.lru.clear();
            self.current_bytes = 0;
            self.counters.evictions = self.counters.evictions.saturating_add(evicted);
            return evicted;
        }

        if let Some(previous) = self.entries.remove(&key) {
            self.current_bytes = self.current_bytes.saturating_sub(previous.size_bytes);
            self.remove_lru_key(&key);
        }
        self.entries.insert(
            key.clone(),
            CacheEntry {
                value,
                size_bytes: entry_size,
            },
        );
        self.current_bytes = self.current_bytes.saturating_add(entry_size);
        self.lru.push_back(key);
        self.evict_if_needed()
    }

    fn counters(&self) -> FilterCacheCounters {
        self.counters
    }

    fn entry_count(&self) -> usize {
        self.entries.len()
    }

    fn current_bytes(&self) -> usize {
        self.current_bytes
    }

    fn capacity_entries(&self) -> usize {
        self.config.max_entries
    }

    fn capacity_bytes(&self) -> usize {
        self.config.max_bytes
    }

    fn touch_key(&mut self, key: &str) {
        self.remove_lru_key(key);
        self.lru.push_back(key.to_string());
    }

    fn remove_lru_key(&mut self, key: &str) {
        if let Some(position) = self.lru.iter().position(|entry| entry == key) {
            self.lru.remove(position);
        }
    }

    fn evict_if_needed(&mut self) -> u64 {
        let mut evicted = 0_u64;
        while self.entries.len() > self.config.max_entries
            || self.current_bytes > self.config.max_bytes
        {
            let Some(oldest_key) = self.lru.pop_front() else {
                break;
            };
            if let Some(oldest) = self.entries.remove(&oldest_key) {
                self.current_bytes = self.current_bytes.saturating_sub(oldest.size_bytes);
                evicted = evicted.saturating_add(1);
            }
        }
        self.counters.evictions = self.counters.evictions.saturating_add(evicted);
        evicted
    }

    fn retain_by_key_prefix(&mut self, prefix: &str) {
        let keys = self
            .entries
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return;
        }
        for key in keys {
            if let Some(entry) = self.entries.remove(&key) {
                self.current_bytes = self.current_bytes.saturating_sub(entry.size_bytes);
            }
            self.remove_lru_key(&key);
        }
    }
}

#[derive(Clone)]
struct SsdCacheEntry {
    file_path: PathBuf,
    size_bytes: usize,
}

pub(crate) struct BoundedSsdVectorCache {
    entries: BTreeMap<String, SsdCacheEntry>,
    lru: VecDeque<String>,
    current_bytes: usize,
    config: FilterCacheConfig,
    base_dir: PathBuf,
}

impl BoundedSsdVectorCache {
    fn new(config: FilterCacheConfig, directory: String) -> Self {
        let base_dir = PathBuf::from(directory);
        if let Err(error) = std::fs::create_dir_all(&base_dir) {
            tracing::warn!(
                directory = %base_dir.display(),
                error = %error,
                "failed to create ANN rerank SSD cache directory"
            );
        }
        Self {
            entries: BTreeMap::new(),
            lru: VecDeque::new(),
            current_bytes: 0,
            config,
            base_dir,
        }
    }

    fn entry_path_for_key(&self, key: &str) -> PathBuf {
        let digest = sha256_hex(key.as_bytes());
        self.base_dir.join(format!("{digest}.json"))
    }

    fn get(&mut self, key: &str) -> Option<UpsertVector> {
        let entry = self.entries.get(key).cloned()?;
        self.touch_key(key);
        match std::fs::read(&entry.file_path) {
            Ok(raw) => serde_json::from_slice::<UpsertVector>(&raw).ok(),
            Err(error) => {
                tracing::warn!(
                    key,
                    path = %entry.file_path.display(),
                    error = %error,
                    "failed to read ANN rerank SSD cache entry"
                );
                self.remove_entry(key);
                None
            }
        }
    }

    fn insert(&mut self, key: String, vector: &UpsertVector) -> u64 {
        let encoded = match serde_json::to_vec(vector) {
            Ok(encoded) => encoded,
            Err(error) => {
                tracing::warn!(
                    key,
                    error = %error,
                    "failed to encode ANN rerank SSD cache entry"
                );
                return 0;
            }
        };
        let entry_size = encoded
            .len()
            .saturating_add(key.len())
            .saturating_add(std::mem::size_of::<SsdCacheEntry>());
        if entry_size > self.config.max_bytes {
            let evicted = self.clear();
            return evicted;
        }

        let file_path = self.entry_path_for_key(&key);
        if let Some(parent) = file_path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                tracing::warn!(
                    key,
                    path = %parent.display(),
                    error = %error,
                    "failed to create ANN rerank SSD cache parent directory"
                );
                return 0;
            }
        }
        if let Err(error) = std::fs::write(&file_path, &encoded) {
            tracing::warn!(
                key,
                path = %file_path.display(),
                error = %error,
                "failed to write ANN rerank SSD cache entry"
            );
            return 0;
        }

        if self.entries.contains_key(&key) {
            self.remove_entry(&key);
        }
        self.entries.insert(
            key.clone(),
            SsdCacheEntry {
                file_path,
                size_bytes: entry_size,
            },
        );
        self.current_bytes = self.current_bytes.saturating_add(entry_size);
        self.lru.push_back(key);
        self.evict_if_needed()
    }

    fn entry_count(&self) -> usize {
        self.entries.len()
    }

    fn current_bytes(&self) -> usize {
        self.current_bytes
    }

    fn capacity_entries(&self) -> usize {
        self.config.max_entries
    }

    fn capacity_bytes(&self) -> usize {
        self.config.max_bytes
    }

    fn retain_by_key_prefix(&mut self, prefix: &str) {
        let keys = self
            .entries
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        for key in keys {
            self.remove_entry(&key);
        }
    }

    fn clear(&mut self) -> u64 {
        let evicted = self.entries.len() as u64;
        let keys = self.entries.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            self.remove_entry(&key);
        }
        evicted
    }

    fn touch_key(&mut self, key: &str) {
        self.remove_lru_key(key);
        self.lru.push_back(key.to_string());
    }

    fn remove_lru_key(&mut self, key: &str) {
        if let Some(position) = self.lru.iter().position(|entry| entry == key) {
            self.lru.remove(position);
        }
    }

    fn remove_entry(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.current_bytes = self.current_bytes.saturating_sub(entry.size_bytes);
            if let Err(error) = std::fs::remove_file(&entry.file_path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        key,
                        path = %entry.file_path.display(),
                        error = %error,
                        "failed to remove ANN rerank SSD cache file"
                    );
                }
            }
        }
        self.remove_lru_key(key);
    }

    fn evict_if_needed(&mut self) -> u64 {
        let mut evicted = 0_u64;
        while self.entries.len() > self.config.max_entries
            || self.current_bytes > self.config.max_bytes
        {
            let Some(oldest_key) = self.lru.pop_front() else {
                break;
            };
            if self.entries.contains_key(&oldest_key) {
                self.remove_entry(&oldest_key);
                evicted = evicted.saturating_add(1);
            }
        }
        evicted
    }
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) node_id: String,
    pub(crate) service_name: String,
    pub(crate) runtime: RuntimeConfigResponse,
    pub(crate) storage: Arc<dyn ObjectStore>,
    pub(crate) queue_client: Arc<dyn QueueBrokerClient>,
    pub(crate) manifest_write_locks: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    pub(crate) ann_index_build_locks: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    pub(crate) fts_index_build_locks: Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>>,
    pub(crate) wal_queue_handles: Arc<Mutex<BTreeMap<String, QueueHandle>>>,
    pub(crate) wal_queue_drain_workers: Arc<Mutex<BTreeMap<String, JoinHandle<()>>>>,
    pub(crate) query_ann_enabled: bool,
    pub(crate) ann_bucket_fetch_semaphore: Arc<Semaphore>,
    pub(crate) ann_tree_root_beam: usize,
    pub(crate) ann_tree_leaf_probe_count: usize,
    pub(crate) ann_object_read_budget: usize,
    pub(crate) ann_quantization_bound_margin: f32,
    pub(crate) ann_rerank_prune_ratio: f32,
    pub(crate) ann_rerank_max_candidates: usize,
    pub(crate) distributed_shard_count: usize,
    pub(crate) distributed_shard_timeout_ms: u64,
    pub(crate) distributed_fail_open: bool,
    pub(crate) distributed_required_successful_shards: usize,
    pub(crate) filter_parser_limits: FilterParserLimits,
    pub(crate) filter_max_widen_passes: usize,
    pub(crate) namespace_vector_cache: Arc<Mutex<BoundedFilterCache<NamespaceVectorCacheEntry>>>,
    pub(crate) ann_bucket_cache: Arc<Mutex<BoundedFilterCache<Arc<AnnBucketData>>>>,
    pub(crate) ann_rerank_ssd_cache: Arc<Mutex<BoundedSsdVectorCache>>,
    pub(crate) filter_cluster_cache: Arc<Mutex<BoundedFilterCache<NativeFilterClusterSummary>>>,
    pub(crate) filter_row_cache: Arc<Mutex<BoundedFilterCache<RoaringBitmap>>>,
    pub(crate) manifest_cache: Arc<RwLock<BTreeMap<String, Manifest>>>,
    pub(crate) collection_metadata_cache: Arc<RwLock<BTreeMap<String, CollectionMetadata>>>,
    pub(crate) fts_index_meta_cache: Arc<RwLock<BTreeMap<String, FtsIndexMeta>>>,
    pub(crate) fts_doc_lookup_cache: Arc<RwLock<BTreeMap<String, FtsDocLookup>>>,
    pub(crate) shard_placement_cache: Arc<RwLock<BTreeMap<String, CollectionShardPlacement>>>,
    pub(crate) ensured_collection_registry_markers: Arc<Mutex<BTreeSet<String>>>,
}

impl AppState {
    fn estimate_metadata_bytes(metadata: &Option<Value>) -> usize {
        metadata
            .as_ref()
            .and_then(|value| serde_json::to_vec(value).ok())
            .map_or(0, |bytes| bytes.len())
    }

    fn estimate_vector_bytes(vector: &UpsertVector) -> usize {
        vector
            .id
            .len()
            .saturating_add(
                vector
                    .values
                    .len()
                    .saturating_mul(std::mem::size_of::<f32>()),
            )
            .saturating_add(Self::estimate_metadata_bytes(&vector.metadata))
    }

    fn estimate_namespace_cache_entry_bytes(entry: &NamespaceVectorCacheEntry) -> usize {
        entry
            .vectors
            .values()
            .map(Self::estimate_vector_bytes)
            .sum()
    }

    fn estimate_ann_bucket_cache_entry_bytes(bucket: &AnnBucketData) -> usize {
        bucket.estimated_size_bytes()
    }

    fn record_namespace_cache_snapshot(
        &self,
        entries: usize,
        bytes: usize,
        capacity_entries: usize,
        capacity_bytes: usize,
    ) {
        telemetry::record_cache_snapshot_scoped(
            &self.service_name,
            &self.node_id,
            "namespace_vectors",
            "cluster",
            entries,
            bytes,
            capacity_entries,
            capacity_bytes,
        );
    }

    fn record_ann_bucket_cache_snapshot(
        &self,
        entries: usize,
        bytes: usize,
        capacity_entries: usize,
        capacity_bytes: usize,
    ) {
        telemetry::record_cache_snapshot_scoped(
            &self.service_name,
            &self.node_id,
            "ann_bucket",
            "cluster",
            entries,
            bytes,
            capacity_entries,
            capacity_bytes,
        );
    }

    fn record_ann_rerank_ssd_cache_snapshot(
        &self,
        entries: usize,
        bytes: usize,
        capacity_entries: usize,
        capacity_bytes: usize,
    ) {
        telemetry::record_cache_snapshot_scoped(
            &self.service_name,
            &self.node_id,
            "ann_rerank_ssd",
            "cluster",
            entries,
            bytes,
            capacity_entries,
            capacity_bytes,
        );
    }

    fn record_filter_cache_snapshot(
        &self,
        cache_name: &str,
        entries: usize,
        bytes: usize,
        capacity_entries: usize,
        capacity_bytes: usize,
    ) {
        telemetry::record_cache_snapshot_scoped(
            &self.service_name,
            &self.node_id,
            cache_name,
            "cluster",
            entries,
            bytes,
            capacity_entries,
            capacity_bytes,
        );
    }

    pub(crate) fn new_manifest_lock_registry() -> Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>> {
        Arc::new(Mutex::new(BTreeMap::new()))
    }

    pub(crate) fn new_wal_queue_registry() -> Arc<Mutex<BTreeMap<String, QueueHandle>>> {
        Arc::new(Mutex::new(BTreeMap::new()))
    }

    pub(crate) fn new_ann_index_lock_registry() -> Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>> {
        Arc::new(Mutex::new(BTreeMap::new()))
    }

    pub(crate) fn new_fts_index_lock_registry() -> Arc<Mutex<BTreeMap<String, Arc<Mutex<()>>>>> {
        Arc::new(Mutex::new(BTreeMap::new()))
    }

    pub(crate) fn new_wal_queue_drain_worker_registry(
    ) -> Arc<Mutex<BTreeMap<String, JoinHandle<()>>>> {
        Arc::new(Mutex::new(BTreeMap::new()))
    }

    pub(crate) async fn wal_queue_handle(&self, collection: &str) -> QueueHandle {
        let mut queues = self.wal_queue_handles.lock().await;
        if let Some(existing) = queues.get(collection) {
            return existing.clone();
        }

        let handle = spawn_queue_broker(
            Arc::new(ObjectStoreCasAdapter::new(self.storage.clone())),
            QueueBrokerConfig {
                queue_key: wal_queue_object_key(collection),
                broker_id: Some(self.node_id.clone()),
                lease_timeout_ms: WAL_QUEUE_LEASE_TIMEOUT_MS,
                channel_capacity: WAL_QUEUE_CHANNEL_CAPACITY,
                max_batch_wait: Duration::from_millis(WAL_QUEUE_MAX_BATCH_WAIT_MS),
                min_commit_interval: Duration::from_millis(WAL_QUEUE_MIN_COMMIT_INTERVAL_MS),
                max_cas_retries: WAL_QUEUE_MAX_CAS_RETRIES,
                ..QueueBrokerConfig::default()
            },
        );
        queues.insert(collection.to_string(), handle.clone());
        handle
    }

    pub(crate) fn wal_queue_worker_id(&self, collection: &str) -> String {
        format!("{}::{}::wal-flush", self.node_id, collection)
    }

    pub(crate) fn queue_client(&self) -> Arc<dyn QueueBrokerClient> {
        self.queue_client.clone()
    }

    pub(crate) fn wal_queue_lease_timeout_ms(&self) -> u64 {
        WAL_QUEUE_LEASE_TIMEOUT_MS
    }

    pub(crate) async fn remove_wal_queue_handle(&self, collection: &str) {
        let mut queues = self.wal_queue_handles.lock().await;
        queues.remove(collection);
        drop(queues);

        let mut workers = self.wal_queue_drain_workers.lock().await;
        if let Some(worker) = workers.remove(collection) {
            worker.abort();
        }
    }

    pub(crate) async fn lock_collection_manifest(&self, collection: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut registry = self.manifest_write_locks.lock().await;
            registry
                .entry(collection.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    pub(crate) async fn lock_ann_index_build(
        &self,
        collection: &str,
        namespace: &str,
        generation: u64,
    ) -> OwnedMutexGuard<()> {
        let key = format!("{collection}::{namespace}::{generation}");
        let lock = {
            let mut registry = self.ann_index_build_locks.lock().await;
            registry
                .entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    pub(crate) async fn lock_fts_index_build(
        &self,
        collection: &str,
        namespace: &str,
        generation: u64,
    ) -> OwnedMutexGuard<()> {
        let key = format!("{collection}::{namespace}::{generation}");
        let lock = {
            let mut registry = self.fts_index_build_locks.lock().await;
            registry
                .entry(key)
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        lock.lock_owned().await
    }

    pub(crate) fn query_ann_enabled(&self) -> bool {
        self.query_ann_enabled
    }

    pub(crate) fn ann_bucket_fetch_semaphore(&self) -> Arc<Semaphore> {
        self.ann_bucket_fetch_semaphore.clone()
    }

    pub(crate) fn ann_tree_root_beam(&self) -> usize {
        self.ann_tree_root_beam.max(1)
    }

    pub(crate) fn ann_tree_leaf_probe_count(&self) -> usize {
        self.ann_tree_leaf_probe_count.max(1)
    }

    pub(crate) fn ann_object_read_budget(&self) -> usize {
        self.ann_object_read_budget.max(1)
    }

    pub(crate) fn ann_quantization_bound_margin(&self) -> f32 {
        if self.ann_quantization_bound_margin.is_finite()
            && self.ann_quantization_bound_margin >= 0.0
        {
            self.ann_quantization_bound_margin
        } else {
            0.0
        }
    }

    pub(crate) fn ann_rerank_prune_ratio(&self) -> f32 {
        self.ann_rerank_prune_ratio.clamp(0.01, 1.0)
    }

    pub(crate) fn ann_rerank_max_candidates(&self) -> usize {
        self.ann_rerank_max_candidates.max(1)
    }

    pub(crate) fn distributed_shard_count(&self) -> usize {
        self.distributed_shard_count.max(1)
    }

    pub(crate) fn distributed_shard_timeout(&self) -> Duration {
        Duration::from_millis(self.distributed_shard_timeout_ms.max(1))
    }

    pub(crate) fn distributed_fail_open(&self) -> bool {
        self.distributed_fail_open
    }

    pub(crate) fn distributed_required_successful_shards(&self) -> usize {
        self.distributed_required_successful_shards
            .max(1)
            .min(self.distributed_shard_count())
    }

    pub(crate) fn shard_for_vector_id(
        &self,
        collection: &str,
        logical_namespace: &str,
        vector_id: &str,
    ) -> u32 {
        distributed::shard_for_vector_id(
            collection,
            logical_namespace,
            vector_id,
            self.distributed_shard_count(),
        )
    }

    pub(crate) fn shard_namespaces(&self, logical_namespace: &str) -> Vec<(u32, String)> {
        distributed::shard_namespaces(logical_namespace, self.distributed_shard_count())
    }

    pub(crate) fn filter_parser_limits(&self) -> FilterParserLimits {
        self.filter_parser_limits
    }

    pub(crate) fn filter_max_widen_passes(&self) -> usize {
        self.filter_max_widen_passes.max(1)
    }

    pub(crate) fn new_namespace_cache(
        config: FilterCacheConfig,
    ) -> Arc<Mutex<BoundedFilterCache<NamespaceVectorCacheEntry>>> {
        Arc::new(Mutex::new(BoundedFilterCache::new(config)))
    }

    pub(crate) fn new_manifest_cache() -> Arc<RwLock<BTreeMap<String, Manifest>>> {
        Arc::new(RwLock::new(BTreeMap::new()))
    }

    pub(crate) fn new_ann_bucket_fetch_semaphore(limit: usize) -> Arc<Semaphore> {
        Arc::new(Semaphore::new(limit.max(1)))
    }

    pub(crate) fn new_ann_bucket_cache(
        config: FilterCacheConfig,
    ) -> Arc<Mutex<BoundedFilterCache<Arc<AnnBucketData>>>> {
        Arc::new(Mutex::new(BoundedFilterCache::new(config)))
    }

    pub(crate) fn new_ann_rerank_ssd_cache(
        config: FilterCacheConfig,
        directory: String,
    ) -> Arc<Mutex<BoundedSsdVectorCache>> {
        Arc::new(Mutex::new(BoundedSsdVectorCache::new(config, directory)))
    }

    pub(crate) fn new_filter_cluster_cache(
        config: FilterCacheConfig,
    ) -> Arc<Mutex<BoundedFilterCache<NativeFilterClusterSummary>>> {
        Arc::new(Mutex::new(BoundedFilterCache::new(config)))
    }

    pub(crate) fn new_filter_row_cache(
        config: FilterCacheConfig,
    ) -> Arc<Mutex<BoundedFilterCache<RoaringBitmap>>> {
        Arc::new(Mutex::new(BoundedFilterCache::new(config)))
    }

    pub(crate) fn new_collection_metadata_cache(
    ) -> Arc<RwLock<BTreeMap<String, CollectionMetadata>>> {
        Arc::new(RwLock::new(BTreeMap::new()))
    }

    pub(crate) fn new_fts_index_meta_cache() -> Arc<RwLock<BTreeMap<String, FtsIndexMeta>>> {
        Arc::new(RwLock::new(BTreeMap::new()))
    }

    pub(crate) fn new_fts_doc_lookup_cache() -> Arc<RwLock<BTreeMap<String, FtsDocLookup>>> {
        Arc::new(RwLock::new(BTreeMap::new()))
    }

    pub(crate) fn new_shard_placement_cache(
    ) -> Arc<RwLock<BTreeMap<String, CollectionShardPlacement>>> {
        Arc::new(RwLock::new(BTreeMap::new()))
    }

    pub(crate) fn new_collection_registry_marker_cache() -> Arc<Mutex<BTreeSet<String>>> {
        Arc::new(Mutex::new(BTreeSet::new()))
    }

    fn namespace_cache_key(collection: &str, namespace: &str) -> String {
        format!("{collection}::{namespace}")
    }

    fn fts_cache_key(collection: &str, namespace: &str, generation: u64) -> String {
        format!("{collection}::{namespace}::{generation}")
    }

    pub(crate) async fn get_namespace_cache_entry(
        &self,
        collection: &str,
        namespace: &str,
    ) -> Option<NamespaceVectorCacheEntry> {
        let key = Self::namespace_cache_key(collection, namespace);
        let mut cache = self.namespace_vector_cache.lock().await;
        cache.get(&key)
    }

    pub(crate) async fn put_namespace_cache_entry(
        &self,
        collection: &str,
        namespace: &str,
        generation: u64,
        vectors: Arc<BTreeMap<String, UpsertVector>>,
    ) {
        let key = Self::namespace_cache_key(collection, namespace);
        let entry = NamespaceVectorCacheEntry {
            generation,
            vectors,
        };
        let size_bytes = Self::estimate_namespace_cache_entry_bytes(&entry);
        let mut cache = self.namespace_vector_cache.lock().await;
        let evictions = cache.insert(key, entry, size_bytes);
        let entries = cache.entry_count();
        let bytes = cache.current_bytes();
        let capacity_entries = cache.capacity_entries();
        let capacity_bytes = cache.capacity_bytes();
        drop(cache);
        telemetry::increment_cache_evictions_scoped(
            &self.service_name,
            &self.node_id,
            "namespace_vectors",
            "cluster",
            evictions,
        );
        self.record_namespace_cache_snapshot(entries, bytes, capacity_entries, capacity_bytes);
    }

    pub(crate) async fn invalidate_collection_cache(&self, collection: &str) {
        let prefix = format!("{collection}::");
        let mut cache = self.namespace_vector_cache.lock().await;
        cache.retain_by_key_prefix(&prefix);
        let namespace_entries = cache.entry_count();
        let namespace_bytes = cache.current_bytes();
        let namespace_capacity_entries = cache.capacity_entries();
        let namespace_capacity_bytes = cache.capacity_bytes();
        drop(cache);
        self.record_namespace_cache_snapshot(
            namespace_entries,
            namespace_bytes,
            namespace_capacity_entries,
            namespace_capacity_bytes,
        );

        let ann_prefix = format!("collections/{collection}/ann/");
        let mut ann_cache = self.ann_bucket_cache.lock().await;
        ann_cache.retain_by_key_prefix(&ann_prefix);
        let ann_entries = ann_cache.entry_count();
        let ann_bytes = ann_cache.current_bytes();
        let ann_capacity_entries = ann_cache.capacity_entries();
        let ann_capacity_bytes = ann_cache.capacity_bytes();
        drop(ann_cache);
        self.record_ann_bucket_cache_snapshot(
            ann_entries,
            ann_bytes,
            ann_capacity_entries,
            ann_capacity_bytes,
        );

        let rerank_prefix = format!("{collection}::");
        let mut rerank_cache = self.ann_rerank_ssd_cache.lock().await;
        rerank_cache.retain_by_key_prefix(&rerank_prefix);
        let rerank_entries = rerank_cache.entry_count();
        let rerank_bytes = rerank_cache.current_bytes();
        let rerank_capacity_entries = rerank_cache.capacity_entries();
        let rerank_capacity_bytes = rerank_cache.capacity_bytes();
        drop(rerank_cache);
        self.record_ann_rerank_ssd_cache_snapshot(
            rerank_entries,
            rerank_bytes,
            rerank_capacity_entries,
            rerank_capacity_bytes,
        );

        let mut cluster_cache = self.filter_cluster_cache.lock().await;
        cluster_cache.retain_by_key_prefix(&ann_prefix);
        let cluster_entries = cluster_cache.entry_count();
        let cluster_bytes = cluster_cache.current_bytes();
        let cluster_capacity_entries = cluster_cache.capacity_entries();
        let cluster_capacity_bytes = cluster_cache.capacity_bytes();
        drop(cluster_cache);
        self.record_filter_cache_snapshot(
            "filter_cluster",
            cluster_entries,
            cluster_bytes,
            cluster_capacity_entries,
            cluster_capacity_bytes,
        );

        let mut row_cache = self.filter_row_cache.lock().await;
        row_cache.retain_by_key_prefix(&ann_prefix);
        let row_entries = row_cache.entry_count();
        let row_bytes = row_cache.current_bytes();
        let row_capacity_entries = row_cache.capacity_entries();
        let row_capacity_bytes = row_cache.capacity_bytes();
        drop(row_cache);
        self.record_filter_cache_snapshot(
            "filter_row",
            row_entries,
            row_bytes,
            row_capacity_entries,
            row_capacity_bytes,
        );

        let fts_prefix = format!("{collection}::");
        {
            let mut cache = self.fts_index_meta_cache.write().await;
            cache.retain(|key, _| !key.starts_with(&fts_prefix));
        }
        {
            let mut cache = self.fts_doc_lookup_cache.write().await;
            cache.retain(|key, _| !key.starts_with(&fts_prefix));
        }
        self.remove_cached_shard_placement(collection).await;
    }

    pub(crate) async fn get_cached_manifest(&self, collection: &str) -> Option<Manifest> {
        let cache = self.manifest_cache.read().await;
        cache.get(collection).cloned()
    }

    pub(crate) async fn set_cached_manifest(&self, collection: &str, manifest: Manifest) {
        let mut cache = self.manifest_cache.write().await;
        cache.insert(collection.to_string(), manifest);
    }

    pub(crate) async fn remove_cached_manifest(&self, collection: &str) {
        let mut cache = self.manifest_cache.write().await;
        cache.remove(collection);
    }

    pub(crate) async fn get_cached_shard_placement(
        &self,
        collection: &str,
    ) -> Option<CollectionShardPlacement> {
        let cache = self.shard_placement_cache.read().await;
        cache.get(collection).cloned()
    }

    pub(crate) async fn set_cached_shard_placement(
        &self,
        collection: &str,
        placement: CollectionShardPlacement,
    ) {
        let mut cache = self.shard_placement_cache.write().await;
        cache.insert(collection.to_string(), placement);
    }

    pub(crate) async fn remove_cached_shard_placement(&self, collection: &str) {
        let mut cache = self.shard_placement_cache.write().await;
        cache.remove(collection);
    }

    pub(crate) async fn get_cached_collection_metadata(
        &self,
        collection: &str,
    ) -> Option<CollectionMetadata> {
        let cache = self.collection_metadata_cache.read().await;
        cache.get(collection).cloned()
    }

    pub(crate) async fn set_cached_collection_metadata(
        &self,
        collection: &str,
        metadata: CollectionMetadata,
    ) {
        let mut cache = self.collection_metadata_cache.write().await;
        cache.insert(collection.to_string(), metadata);
    }

    pub(crate) async fn remove_cached_collection_metadata(&self, collection: &str) {
        let mut cache = self.collection_metadata_cache.write().await;
        cache.remove(collection);
    }

    pub(crate) async fn collection_registry_marker_ensured(&self, collection: &str) -> bool {
        let cache = self.ensured_collection_registry_markers.lock().await;
        cache.contains(collection)
    }

    pub(crate) async fn mark_collection_registry_marker_ensured(&self, collection: &str) {
        let mut cache = self.ensured_collection_registry_markers.lock().await;
        cache.insert(collection.to_string());
    }

    pub(crate) async fn remove_collection_registry_marker_ensured(&self, collection: &str) {
        let mut cache = self.ensured_collection_registry_markers.lock().await;
        cache.remove(collection);
    }

    pub(crate) async fn get_cached_fts_index_meta(
        &self,
        collection: &str,
        namespace: &str,
        generation: u64,
    ) -> Option<FtsIndexMeta> {
        let key = Self::fts_cache_key(collection, namespace, generation);
        let cache = self.fts_index_meta_cache.read().await;
        cache.get(&key).cloned()
    }

    pub(crate) async fn set_cached_fts_index_meta(
        &self,
        collection: &str,
        namespace: &str,
        generation: u64,
        index_meta: FtsIndexMeta,
    ) {
        let key = Self::fts_cache_key(collection, namespace, generation);
        let mut cache = self.fts_index_meta_cache.write().await;
        cache.insert(key, index_meta);
    }

    pub(crate) async fn get_cached_fts_doc_lookup(
        &self,
        collection: &str,
        namespace: &str,
        generation: u64,
    ) -> Option<FtsDocLookup> {
        let key = Self::fts_cache_key(collection, namespace, generation);
        let cache = self.fts_doc_lookup_cache.read().await;
        cache.get(&key).cloned()
    }

    pub(crate) async fn set_cached_fts_doc_lookup(
        &self,
        collection: &str,
        namespace: &str,
        generation: u64,
        doc_lookup: FtsDocLookup,
    ) {
        let key = Self::fts_cache_key(collection, namespace, generation);
        let mut cache = self.fts_doc_lookup_cache.write().await;
        cache.insert(key, doc_lookup);
    }

    pub(crate) async fn get_ann_bucket_cache(
        &self,
        object_key: &str,
    ) -> Option<Arc<AnnBucketData>> {
        let mut cache = self.ann_bucket_cache.lock().await;
        cache.get(object_key)
    }

    pub(crate) async fn put_ann_bucket_cache(
        &self,
        object_key: String,
        bucket: Arc<AnnBucketData>,
    ) {
        let size_bytes = Self::estimate_ann_bucket_cache_entry_bytes(bucket.as_ref());
        let mut cache = self.ann_bucket_cache.lock().await;
        let evictions = cache.insert(object_key, bucket, size_bytes);
        let entries = cache.entry_count();
        let bytes = cache.current_bytes();
        let capacity_entries = cache.capacity_entries();
        let capacity_bytes = cache.capacity_bytes();
        drop(cache);
        telemetry::increment_cache_evictions_scoped(
            &self.service_name,
            &self.node_id,
            "ann_bucket",
            "cluster",
            evictions,
        );
        self.record_ann_bucket_cache_snapshot(entries, bytes, capacity_entries, capacity_bytes);
    }

    pub(crate) async fn get_ann_rerank_ssd_cache(&self, cache_key: &str) -> Option<UpsertVector> {
        let mut cache = self.ann_rerank_ssd_cache.lock().await;
        cache.get(cache_key)
    }

    pub(crate) async fn put_ann_rerank_ssd_cache(
        &self,
        cache_key: String,
        vector: &UpsertVector,
    ) -> u64 {
        let mut cache = self.ann_rerank_ssd_cache.lock().await;
        let evictions = cache.insert(cache_key, vector);
        let entries = cache.entry_count();
        let bytes = cache.current_bytes();
        let capacity_entries = cache.capacity_entries();
        let capacity_bytes = cache.capacity_bytes();
        drop(cache);
        telemetry::increment_cache_evictions_scoped(
            &self.service_name,
            &self.node_id,
            "ann_rerank_ssd",
            "cluster",
            evictions,
        );
        self.record_ann_rerank_ssd_cache_snapshot(entries, bytes, capacity_entries, capacity_bytes);
        evictions
    }

    pub(crate) async fn get_filter_cluster_cache(
        &self,
        object_key: &str,
    ) -> Option<NativeFilterClusterSummary> {
        let mut cache = self.filter_cluster_cache.lock().await;
        cache.get(object_key)
    }

    pub(crate) async fn put_filter_cluster_cache(
        &self,
        object_key: String,
        summary: NativeFilterClusterSummary,
    ) -> u64 {
        let size_bytes = summary.estimated_size_bytes();
        let mut cache = self.filter_cluster_cache.lock().await;
        let evictions = cache.insert(object_key, summary, size_bytes);
        let entries = cache.entry_count();
        let bytes = cache.current_bytes();
        let capacity_entries = cache.capacity_entries();
        let capacity_bytes = cache.capacity_bytes();
        drop(cache);
        telemetry::increment_cache_evictions_scoped(
            &self.service_name,
            &self.node_id,
            "filter_cluster",
            "cluster",
            evictions,
        );
        self.record_filter_cache_snapshot(
            "filter_cluster",
            entries,
            bytes,
            capacity_entries,
            capacity_bytes,
        );
        evictions
    }

    pub(crate) async fn get_filter_row_cache(&self, object_key: &str) -> Option<RoaringBitmap> {
        let mut cache = self.filter_row_cache.lock().await;
        cache.get(object_key)
    }

    pub(crate) async fn put_filter_row_cache(
        &self,
        object_key: String,
        bitmap: RoaringBitmap,
    ) -> u64 {
        let size_bytes = bitmap.serialized_size() as usize;
        let mut cache = self.filter_row_cache.lock().await;
        let evictions = cache.insert(object_key, bitmap, size_bytes);
        let entries = cache.entry_count();
        let bytes = cache.current_bytes();
        let capacity_entries = cache.capacity_entries();
        let capacity_bytes = cache.capacity_bytes();
        drop(cache);
        telemetry::increment_cache_evictions_scoped(
            &self.service_name,
            &self.node_id,
            "filter_row",
            "cluster",
            evictions,
        );
        self.record_filter_cache_snapshot(
            "filter_row",
            entries,
            bytes,
            capacity_entries,
            capacity_bytes,
        );
        evictions
    }

    pub(crate) async fn filter_cache_metrics_snapshot(&self) -> FilterCacheMetricsSnapshot {
        let cluster_counters = {
            let cache = self.filter_cluster_cache.lock().await;
            cache.counters()
        };
        let row_counters = {
            let cache = self.filter_row_cache.lock().await;
            cache.counters()
        };
        FilterCacheMetricsSnapshot {
            cluster_hits: cluster_counters.hits,
            cluster_misses: cluster_counters.misses,
            cluster_evictions: cluster_counters.evictions,
            row_hits: row_counters.hits,
            row_misses: row_counters.misses,
            row_evictions: row_counters.evictions,
        }
    }

    pub(crate) async fn emit_cache_telemetry_snapshot(&self) {
        {
            let cache = self.namespace_vector_cache.lock().await;
            self.record_namespace_cache_snapshot(
                cache.entry_count(),
                cache.current_bytes(),
                cache.capacity_entries(),
                cache.capacity_bytes(),
            );
        }
        {
            let cache = self.ann_bucket_cache.lock().await;
            self.record_ann_bucket_cache_snapshot(
                cache.entry_count(),
                cache.current_bytes(),
                cache.capacity_entries(),
                cache.capacity_bytes(),
            );
        }
        {
            let cache = self.ann_rerank_ssd_cache.lock().await;
            self.record_ann_rerank_ssd_cache_snapshot(
                cache.entry_count(),
                cache.current_bytes(),
                cache.capacity_entries(),
                cache.capacity_bytes(),
            );
        }
        {
            let cache = self.filter_cluster_cache.lock().await;
            self.record_filter_cache_snapshot(
                "filter_cluster",
                cache.entry_count(),
                cache.current_bytes(),
                cache.capacity_entries(),
                cache.capacity_bytes(),
            );
        }
        {
            let cache = self.filter_row_cache.lock().await;
            self.record_filter_cache_snapshot(
                "filter_row",
                cache.entry_count(),
                cache.current_bytes(),
                cache.capacity_entries(),
                cache.capacity_bytes(),
            );
        }
    }
}

#[derive(Clone, Serialize, ToSchema)]
pub(crate) struct RuntimeConfigResponse {
    pub(crate) bind_addr: String,
    pub(crate) api_body_limit_bytes: usize,
    pub(crate) storage_provider: String,
    pub(crate) storage_endpoint: String,
    pub(crate) storage_region: String,
    pub(crate) storage_bucket: String,
    pub(crate) storage_simulated_latency_ms: u64,
    pub(crate) wal_queue_background_drain_enabled: bool,
    pub(crate) wal_queue_background_drain_interval_ms: u64,
    pub(crate) queue_broker_url: String,
    pub(crate) wal_worker_build_fts: bool,
    pub(crate) wal_worker_build_ann: bool,
    pub(crate) query_ann_enabled: bool,
    pub(crate) ann_bucket_fetch_concurrency: usize,
    pub(crate) ann_tree_root_beam: usize,
    pub(crate) ann_tree_leaf_probe_count: usize,
    pub(crate) ann_object_read_budget: usize,
    pub(crate) ann_quantization_bound_margin: f32,
    pub(crate) ann_rerank_prune_ratio: f32,
    pub(crate) ann_rerank_max_candidates: usize,
    pub(crate) ann_rerank_ssd_cache_dir: String,
    pub(crate) ann_rerank_ssd_cache_max_entries: usize,
    pub(crate) ann_rerank_ssd_cache_max_bytes: usize,
    pub(crate) namespace_cache_max_entries: usize,
    pub(crate) namespace_cache_max_bytes: usize,
    pub(crate) ann_bucket_cache_max_entries: usize,
    pub(crate) ann_bucket_cache_max_bytes: usize,
    pub(crate) filter_cluster_cache_max_entries: usize,
    pub(crate) filter_cluster_cache_max_bytes: usize,
    pub(crate) filter_row_cache_max_entries: usize,
    pub(crate) filter_row_cache_max_bytes: usize,
    pub(crate) filter_max_regex_bytes: usize,
    pub(crate) filter_max_glob_bytes: usize,
    pub(crate) filter_max_widen_passes: usize,
    pub(crate) fts_block_target_postings: usize,
    pub(crate) fts_block_split_threshold: usize,
    pub(crate) fts_block_merge_threshold: usize,
    pub(crate) fts_max_term_blocks_touched_per_doc_update: usize,
    pub(crate) fts_enable_delta_rebalance: bool,
    pub(crate) fts_prefix_max_index_chars: usize,
    pub(crate) fts_prefix_max_expansions: usize,
    pub(crate) fts_prefix_max_expansion_bytes: usize,
    pub(crate) fts_explain_query_enabled: bool,
    pub(crate) fts_explain_max_top_k: usize,
    pub(crate) distributed_shard_count: usize,
    pub(crate) distributed_shard_timeout_ms: u64,
    pub(crate) distributed_fail_open: bool,
    pub(crate) distributed_required_successful_shards: usize,
    pub(crate) viz_enabled: bool,
    pub(crate) otel_enabled: bool,
    pub(crate) otel_exporter_otlp_endpoint: String,
    pub(crate) otel_service_name: String,
    pub(crate) otel_metric_export_interval_ms: u64,
    pub(crate) otel_sample_ratio: f64,
    #[serde(default)]
    pub(crate) filter_cache_metrics: FilterCacheMetricsSnapshot,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct HealthResponse {
    pub(crate) status: &'static str,
}
