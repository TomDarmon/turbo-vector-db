use serde::{Deserialize, Serialize};
use serde_json::Value;
use turbo_vector_core::Metric;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct CollectionMetadata {
    pub(crate) name: String,
    pub(crate) dimension: u32,
    pub(crate) metric: Metric,
    pub(crate) created_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata_schema: Option<Value>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct CreateCollectionRequest {
    pub(crate) name: String,
    pub(crate) dimension: u32,
    #[serde(default)]
    pub(crate) metric: Option<Metric>,
    #[serde(default)]
    pub(crate) metadata_schema: Option<Value>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ListCollectionsResponse {
    pub(crate) collections: Vec<CollectionMetadata>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct UpsertVector {
    pub(crate) id: String,
    pub(crate) values: Vec<f32>,
    #[serde(default)]
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct UpsertRequest {
    pub(crate) vectors: Vec<UpsertVector>,
    #[serde(default)]
    pub(crate) namespace: Option<String>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub(crate) struct QueryRequest {
    pub(crate) vector: Vec<f32>,
    #[serde(default = "default_top_k")]
    pub(crate) top_k: u32,
    #[serde(default)]
    pub(crate) namespace: Option<String>,
    #[serde(default = "default_include_metadata")]
    pub(crate) include_metadata: bool,
    #[serde(default)]
    pub(crate) include_values: bool,
    #[serde(default)]
    pub(crate) filter: Option<Value>,
    #[serde(default = "default_query_search_strategy")]
    pub(crate) search_strategy: QuerySearchStrategy,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct FetchRequest {
    pub(crate) ids: Vec<String>,
    #[serde(default)]
    pub(crate) namespace: Option<String>,
    #[serde(default = "default_include_metadata")]
    pub(crate) include_metadata: bool,
    #[serde(default = "default_include_values_true")]
    pub(crate) include_values: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct DeleteRequest {
    #[serde(default)]
    pub(crate) ids: Option<Vec<String>>,
    #[serde(default)]
    pub(crate) filter: Option<Value>,
    #[serde(default)]
    pub(crate) namespace: Option<String>,
    #[serde(default)]
    pub(crate) delete_all: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct DeleteResponse {
    pub(crate) deleted_count: u64,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct DeleteCollectionResponse {
    pub(crate) deleted: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct CollectionStatsResponse {
    pub(crate) dimension: u32,
    pub(crate) vector_count: u64,
    pub(crate) segments: u64,
    pub(crate) generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SegmentKind {
    Upsert,
    Delete,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SegmentFile {
    pub(crate) segment_id: String,
    pub(crate) collection: String,
    pub(crate) namespace: String,
    pub(crate) created_at: String,
    #[serde(default = "default_segment_kind")]
    pub(crate) kind: SegmentKind,
    #[serde(default)]
    pub(crate) vectors: Vec<UpsertVector>,
    #[serde(default)]
    pub(crate) deleted_ids: Vec<String>,
    #[serde(default)]
    pub(crate) delete_all: bool,
    #[serde(default)]
    pub(crate) delete_filter: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct WalRecord {
    pub(crate) operation_id: String,
    pub(crate) collection: String,
    pub(crate) namespace: String,
    pub(crate) accepted_at: String,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) request: UpsertRequest,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct IdempotencyRecord {
    pub(crate) request_hash: String,
    pub(crate) response: Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct QueryResponse {
    pub(crate) matches: Vec<QueryMatch>,
    pub(crate) namespace: String,
    pub(crate) ann: QueryAnnObservability,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) distributed: Option<QueryDistributedObservability>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct QueryMatch {
    pub(crate) id: String,
    pub(crate) score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) values: Option<Vec<f32>>,
}

#[derive(Debug, Clone, Serialize, Default, ToSchema)]
pub(crate) struct QueryAnnObservability {
    pub(crate) ann_used: bool,
    pub(crate) ann_fallback_count: u64,
    pub(crate) buckets_probed: u64,
    pub(crate) candidates_scored: u64,
    pub(crate) ann_fetch_errors: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) filter_cluster_cache_hits: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) filter_cluster_cache_misses: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) filter_cluster_cache_evictions: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) filter_row_cache_hits: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) filter_row_cache_misses: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) filter_row_cache_evictions: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) filter_widen_passes: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) rerank_ssd_cache_hits: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) rerank_ssd_cache_misses: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) rerank_ssd_cache_evictions: u64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub(crate) rerank_ssd_fetch_latency_ms: f64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) first_stage_candidate_count: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) rerank_candidate_count: u64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub(crate) quantization_bound_margin: f64,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub(crate) quantization_bound_threshold: f64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) object_read_budget: u64,
    #[serde(default, skip_serializing_if = "is_false_bool")]
    pub(crate) object_read_budget_exceeded: bool,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) ann_meta_object_reads: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) ann_meta_object_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) ann_bucket_object_reads: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) ann_bucket_object_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) ann_filter_cluster_object_reads: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) ann_filter_cluster_object_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) ann_filter_row_object_reads: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) ann_filter_row_object_bytes: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) rerank_segment_object_reads: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) rerank_segment_object_bytes: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) fallback_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Default, ToSchema)]
pub(crate) struct QueryDistributedObservability {
    pub(crate) planned_shards: u32,
    pub(crate) successful_shards: u32,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) timed_out_shards: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) dropped_shards: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) stale_shards: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) failed_shards: u64,
    pub(crate) required_successful_shards: u32,
    pub(crate) shard_timeout_ms: u64,
    #[serde(default, skip_serializing_if = "is_false_bool")]
    pub(crate) degraded: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) degradation_reasons: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) shard_statuses: Vec<QueryShardStatus>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub(crate) struct QueryShardStatus {
    pub(crate) shard_id: u32,
    pub(crate) node_id: String,
    pub(crate) status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) generation: Option<u64>,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub(crate) latency_ms: f64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub(crate) match_count: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub(crate) reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum QuerySearchStrategy {
    Auto,
    Exact,
    Ann,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct FetchResponse {
    pub(crate) vectors: Vec<FetchVector>,
    pub(crate) namespace: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct FetchVector {
    pub(crate) id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) values: Option<Vec<f32>>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(crate) struct UpsertAppliedResponse {
    pub(crate) upserted_count: usize,
    pub(crate) generation: u64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub(crate) struct UpsertAcceptedResponse {
    pub(crate) accepted: bool,
    pub(crate) operation_id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub(crate) enum UpsertResponse {
    Applied(UpsertAppliedResponse),
    Accepted(UpsertAcceptedResponse),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OperationApplyStatus {
    Accepted,
    Applied,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub(crate) struct OperationStatusResponse {
    pub(crate) operation_id: String,
    pub(crate) status: OperationApplyStatus,
    pub(crate) accepted_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) applied_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) generation: Option<u64>,
}

pub(crate) fn default_top_k() -> u32 {
    10
}

pub(crate) fn default_query_search_strategy() -> QuerySearchStrategy {
    QuerySearchStrategy::Auto
}

pub(crate) fn default_include_metadata() -> bool {
    true
}

pub(crate) fn default_include_values_true() -> bool {
    true
}

pub(crate) fn default_segment_kind() -> SegmentKind {
    SegmentKind::Upsert
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

fn is_zero_f64(value: &f64) -> bool {
    *value == 0.0
}

fn is_false_bool(value: &bool) -> bool {
    !*value
}
