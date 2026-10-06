use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    mem::size_of,
    sync::Arc,
    time::Instant,
};

use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinSet;
use tracing::{info, warn};
use turbo_vector_core::{Metric, TurboVectorError};
use turbo_vector_manifest::Manifest;

use crate::{
    error::{map_store_error, ApiError},
    filters::{
        metadata_matches_filter,
        planner::{
            compile_native_filter_plan, evaluate_native_filter_cluster_estimate,
            evaluate_native_filter_row_bitmap, filter_token_term_hash, filter_value_term_hash,
            scalar_value_type, FilterIndexMeta, FilterTokenLexiconEntry, FilterValueLexiconEntry,
            NativeFilterClusterSummary, NativeFilterExpression, NativeFilterPlan,
        },
        tokenize_text, MetadataFilterExpression,
    },
    keys::{
        ann_bucket_object_key, ann_filter_cluster_object_key, ann_filter_row_object_key,
        ann_index_meta_key,
    },
    models::UpsertVector,
    scoring::{
        l2_norm, metric_score_with_cached_query_norm, metric_score_with_norms_and_squared_euclidean,
    },
    state::AppState,
    storage_logic::{load_namespace_vectors, load_namespace_vectors_for_ids_with_stats},
    telemetry,
    validation::compute_score,
};

const ANN_MIN_VECTORS: usize = 2_000;
const ANN_MIN_CENTROIDS: usize = 8;
const ANN_MAX_CENTROIDS: usize = 256;
const ANN_KMEANS_ITERATIONS: usize = 2;
const ANN_MAX_PROBES: usize = 32;
const ANN_SMALL_CORPUS_VECTOR_THRESHOLD: usize = 20_000;

const ANN_BUCKET_MAGIC: &[u8; 4] = b"TVAB";
const ANN_BUCKET_VERSION_BINARY: u8 = 4;
const ANN_BUCKET_FLAG_HAS_NORMS: u8 = 0x1;
const ANN_BUCKET_FLAG_PAYLOAD_BINARY: u8 = 0x8;
const ANN_FILTER_CLUSTER_MAGIC: &[u8; 4] = b"TVFC";
const ANN_FILTER_ROW_MAGIC: &[u8; 4] = b"TVFR";
const ANN_FILTER_CODEC_VERSION: u8 = 1;
const ANN_BINARY_PRUNE_MIN_CANDIDATES: usize = 256;
const ANN_BINARY_MIN_TREE_ROOTS: usize = 2;
const ANN_BINARY_MAX_TREE_ROOTS: usize = 32;

const FALLBACK_REASON_OBJECT_READ_BUDGET_EXCEEDED: &str = "object_read_budget_exceeded";
const FALLBACK_REASON_RERANK_CANDIDATE_BUDGET_EXCEEDED: &str = "rerank_candidate_budget_exceeded";
const FALLBACK_REASON_RERANK_FETCH_BUDGET_EXCEEDED: &str = "rerank_fetch_budget_exceeded";

#[derive(Debug, Clone, Default)]
pub(crate) struct AnnQueryExecutionStats {
    pub(crate) buckets_probed: usize,
    pub(crate) candidates_scored: usize,
    pub(crate) ann_fetch_errors: usize,
    pub(crate) filter_cluster_cache_hits: usize,
    pub(crate) filter_cluster_cache_misses: usize,
    pub(crate) filter_cluster_cache_evictions: usize,
    pub(crate) filter_row_cache_hits: usize,
    pub(crate) filter_row_cache_misses: usize,
    pub(crate) filter_row_cache_evictions: usize,
    pub(crate) rerank_ssd_cache_hits: usize,
    pub(crate) rerank_ssd_cache_misses: usize,
    pub(crate) rerank_ssd_cache_evictions: usize,
    pub(crate) rerank_ssd_fetch_latency_ms: f64,
    pub(crate) widen_passes: usize,
    pub(crate) quantization_bound_margin: f32,
    pub(crate) quantization_bound_threshold: f32,
    pub(crate) first_stage_candidate_count: usize,
    pub(crate) rerank_candidate_count: usize,
    pub(crate) object_read_budget: usize,
    pub(crate) object_read_budget_exceeded: bool,
    pub(crate) ann_meta_object_reads: usize,
    pub(crate) ann_meta_object_bytes: usize,
    pub(crate) ann_bucket_object_reads: usize,
    pub(crate) ann_bucket_object_bytes: usize,
    pub(crate) ann_filter_cluster_object_reads: usize,
    pub(crate) ann_filter_cluster_object_bytes: usize,
    pub(crate) ann_filter_row_object_reads: usize,
    pub(crate) ann_filter_row_object_bytes: usize,
    pub(crate) rerank_segment_object_reads: usize,
    pub(crate) rerank_segment_object_bytes: usize,
    pub(crate) fallback_reasons: Vec<String>,
}

impl AnnQueryExecutionStats {
    fn push_fallback_reason(&mut self, reason: &str) {
        if self
            .fallback_reasons
            .iter()
            .any(|existing| existing == reason)
        {
            return;
        }
        self.fallback_reasons.push(reason.to_string());
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct AnnQueryExecution {
    pub(crate) scored: Option<Vec<(UpsertVector, f32)>>,
    pub(crate) stats: AnnQueryExecutionStats,
}

impl AnnQueryExecution {
    fn fallback(reason: &str) -> Self {
        let mut stats = AnnQueryExecutionStats::default();
        stats.push_fallback_reason(reason);
        Self {
            scored: None,
            stats,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnnIndexMeta {
    generation: u64,
    collection: String,
    namespace: String,
    dimension: u32,
    metric: Metric,
    vector_count: usize,
    #[serde(default)]
    centroids: Vec<f32>,
    #[serde(default)]
    centroid_norms: Vec<f32>,
    buckets: Vec<AnnBucketMeta>,
    #[serde(default)]
    tree_levels: Vec<AnnTreeLevelMeta>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    filter_index: Option<AnnFilterIndexMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnnTreeLevelMeta {
    level: usize,
    nodes: Vec<AnnTreeNodeMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnnTreeNodeMeta {
    node_id: usize,
    centroid: Vec<f32>,
    centroid_norm: f32,
    #[serde(default)]
    child_node_ids: Vec<usize>,
    #[serde(default)]
    bucket_id: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnnBucketMeta {
    bucket_id: usize,
    object_key: String,
    vector_count: usize,
}

type AnnFilterIndexMeta = FilterIndexMeta;
type AnnFilterClusterSummary = NativeFilterClusterSummary;

#[derive(Debug, Clone)]
pub(crate) struct AnnBucketData {
    bucket_id: usize,
    dimension: usize,
    ids: Vec<String>,
    metadata: Vec<Option<Value>>,
    values: AnnBucketValues,
    norms: Vec<f32>,
}

#[derive(Debug, Clone)]
enum AnnBucketValues {
    Binary {
        signatures: Vec<u8>,
        stride_bytes: usize,
    },
}

impl AnnBucketData {
    fn vector_count(&self) -> usize {
        self.ids.len()
    }

    fn vector_id(&self, index: usize) -> &str {
        &self.ids[index]
    }

    fn vector_metadata(&self, index: usize) -> Option<&Value> {
        self.metadata[index].as_ref()
    }

    pub(crate) fn estimated_size_bytes(&self) -> usize {
        let ids_bytes = self.ids.iter().map(|id| id.len()).sum::<usize>();
        let metadata_bytes = self
            .metadata
            .iter()
            .map(|value| {
                value
                    .as_ref()
                    .and_then(|entry| serde_json::to_vec(entry).ok())
                    .map_or(0, |bytes| bytes.len())
            })
            .sum::<usize>();
        let values_bytes = match &self.values {
            AnnBucketValues::Binary {
                signatures,
                stride_bytes: _,
            } => signatures.len(),
        };
        let norms_bytes = self.norms.len().saturating_mul(size_of::<f32>());
        ids_bytes
            .saturating_add(metadata_bytes)
            .saturating_add(values_bytes)
            .saturating_add(norms_bytes)
    }

    fn binary_query_signature(&self, query: &[f32]) -> Option<Vec<u8>> {
        let AnnBucketValues::Binary { stride_bytes, .. } = &self.values;
        if *stride_bytes == 0 {
            return None;
        }
        Some(pack_sign_bits(query, *stride_bytes))
    }

    fn score_vector(
        &self,
        metric: &Metric,
        query: &[f32],
        _query_norm: f32,
        index: usize,
        binary_query_signature: Option<&[u8]>,
    ) -> f32 {
        match &self.values {
            AnnBucketValues::Binary {
                signatures,
                stride_bytes,
            } => {
                let Some(query_signature) = binary_query_signature else {
                    return 0.0;
                };
                if *stride_bytes == 0 {
                    return 0.0;
                }
                let start = index.saturating_mul(*stride_bytes);
                let end = start.saturating_add(*stride_bytes);
                if end > signatures.len() {
                    return 0.0;
                }
                let candidate_signature = &signatures[start..end];
                let hamming_distance =
                    hamming_distance_bits(query_signature, candidate_signature) as usize;
                let active_dimensions = self.dimension.min(query.len()).max(1);
                let signed_similarity = active_dimensions as f32 - (2 * hamming_distance) as f32;
                match metric {
                    Metric::Dot => signed_similarity,
                    Metric::Cosine => signed_similarity / active_dimensions as f32,
                    Metric::Euclidean => -(hamming_distance as f32),
                }
            }
        }
    }
}

#[derive(Debug)]
struct ScoredCandidateRef {
    selected_bucket_index: usize,
    vector_index: usize,
    score: f32,
}

fn pack_sign_bits(values: &[f32], stride_bytes: usize) -> Vec<u8> {
    let mut packed = vec![0_u8; stride_bytes];
    for (index, value) in values.iter().enumerate() {
        if *value >= 0.0 {
            let byte_index = index / 8;
            if byte_index >= stride_bytes {
                break;
            }
            let bit_index = index % 8;
            packed[byte_index] |= 1_u8 << bit_index;
        }
    }
    packed
}

fn hamming_distance_bits_scalar(left: &[u8], right: &[u8]) -> u32 {
    left.iter()
        .zip(right.iter())
        .map(|(lhs, rhs)| (*lhs ^ *rhs).count_ones())
        .sum()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
fn hamming_distance_bits_popcnt(left: &[u8], right: &[u8]) -> u32 {
    let mut distance = 0_u32;
    let mut chunks_left = left.chunks_exact(8);
    let mut chunks_right = right.chunks_exact(8);
    for (lhs, rhs) in chunks_left.by_ref().zip(chunks_right.by_ref()) {
        let mut lhs_arr = [0_u8; 8];
        lhs_arr.copy_from_slice(lhs);
        let mut rhs_arr = [0_u8; 8];
        rhs_arr.copy_from_slice(rhs);
        let lhs_u64 = u64::from_le_bytes(lhs_arr);
        let rhs_u64 = u64::from_le_bytes(rhs_arr);
        distance = distance.saturating_add((lhs_u64 ^ rhs_u64).count_ones());
    }
    distance.saturating_add(hamming_distance_bits_scalar(
        chunks_left.remainder(),
        chunks_right.remainder(),
    ))
}

fn hamming_distance_bits(left: &[u8], right: &[u8]) -> u32 {
    if left.len() != right.len() {
        return 0;
    }
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::is_x86_feature_detected!("popcnt") {
            return hamming_distance_bits_popcnt(left, right);
        }
    }
    hamming_distance_bits_scalar(left, right)
}

fn choose_centroid_count(vector_count: usize) -> usize {
    if vector_count == 0 {
        return 0;
    }
    let estimate = (vector_count as f64).sqrt().round() as usize;
    estimate
        .clamp(ANN_MIN_CENTROIDS, ANN_MAX_CENTROIDS)
        .min(vector_count)
}

fn choose_tree_root_count(bucket_count: usize) -> usize {
    if bucket_count == 0 {
        return 0;
    }
    let estimate = (bucket_count as f64).sqrt().round() as usize;
    estimate
        .clamp(ANN_BINARY_MIN_TREE_ROOTS, ANN_BINARY_MAX_TREE_ROOTS)
        .min(bucket_count)
}

fn initial_centroids(vectors: &[UpsertVector], centroid_count: usize) -> Vec<Vec<f32>> {
    let mut centroids = Vec::with_capacity(centroid_count);
    for index in 0..centroid_count {
        let vector_index = index.saturating_mul(vectors.len()) / centroid_count;
        let selected = vectors
            .get(vector_index)
            .or_else(|| vectors.last())
            .expect("centroid selection requires non-empty vectors");
        centroids.push(selected.values.clone());
    }
    centroids
}

fn nearest_centroid(metric: &Metric, vector: &[f32], centroids: &[Vec<f32>]) -> usize {
    let mut best_index = 0usize;
    let mut best_score = f32::NEG_INFINITY;
    for (index, centroid) in centroids.iter().enumerate() {
        let score = compute_score(metric, vector, centroid);
        if score > best_score {
            best_score = score;
            best_index = index;
        }
    }
    best_index
}

fn average_centroid(vectors: &[&[f32]], dimension: usize) -> Vec<f32> {
    if vectors.is_empty() {
        return vec![0.0; dimension];
    }
    let mut centroid = vec![0.0_f32; dimension];
    for vector in vectors {
        for (out, value) in centroid.iter_mut().zip(vector.iter()) {
            *out += *value;
        }
    }
    let inv = 1.0_f32 / vectors.len() as f32;
    for value in &mut centroid {
        *value *= inv;
    }
    centroid
}

fn build_tree_levels(
    bucket_metas: &[AnnBucketMeta],
    flattened_centroids: &[f32],
    centroid_norms: &[f32],
    dimension: usize,
) -> Vec<AnnTreeLevelMeta> {
    if bucket_metas.is_empty() || dimension == 0 {
        return Vec::new();
    }
    let bucket_count = bucket_metas.len();
    let root_count = choose_tree_root_count(bucket_count).max(1);
    let chunk_size = bucket_count.div_ceil(root_count).max(1);
    let leaf_node_id_offset = root_count;

    let mut leaf_nodes = Vec::with_capacity(bucket_count);
    for (bucket_index, bucket_meta) in bucket_metas.iter().enumerate() {
        let centroid_start = bucket_index.saturating_mul(dimension);
        let centroid_end = centroid_start.saturating_add(dimension);
        let centroid = if centroid_end <= flattened_centroids.len() {
            flattened_centroids[centroid_start..centroid_end].to_vec()
        } else {
            vec![0.0_f32; dimension]
        };
        let centroid_norm = centroid_norms.get(bucket_index).copied().unwrap_or(0.0_f32);
        leaf_nodes.push(AnnTreeNodeMeta {
            node_id: leaf_node_id_offset + bucket_index,
            centroid,
            centroid_norm,
            child_node_ids: Vec::new(),
            bucket_id: Some(bucket_meta.bucket_id),
        });
    }

    let mut root_nodes = Vec::with_capacity(root_count);
    for root_index in 0..root_count {
        let start = root_index.saturating_mul(chunk_size);
        if start >= bucket_count {
            break;
        }
        let end = start.saturating_add(chunk_size).min(bucket_count);
        let child_ids = (start..end)
            .map(|leaf_index| leaf_node_id_offset + leaf_index)
            .collect::<Vec<_>>();
        let child_centroids = (start..end)
            .map(|leaf_index| leaf_nodes[leaf_index].centroid.as_slice())
            .collect::<Vec<_>>();
        let centroid = average_centroid(&child_centroids, dimension);
        root_nodes.push(AnnTreeNodeMeta {
            node_id: root_index,
            centroid_norm: l2_norm(&centroid),
            centroid,
            child_node_ids: child_ids,
            bucket_id: None,
        });
    }

    vec![
        AnnTreeLevelMeta {
            level: 0,
            nodes: root_nodes,
        },
        AnnTreeLevelMeta {
            level: 1,
            nodes: leaf_nodes,
        },
    ]
}

fn tree_bucket_candidates(
    index_meta: &AnnIndexMeta,
    metric: &Metric,
    query_vector: &[f32],
    query_norm: f32,
    root_beam: usize,
) -> Option<Vec<(usize, f32)>> {
    if index_meta.tree_levels.len() < 2 {
        return None;
    }
    let root_level = &index_meta.tree_levels[0];
    let leaf_level = &index_meta.tree_levels[1];
    if root_level.nodes.is_empty() || leaf_level.nodes.is_empty() {
        return None;
    }
    let mut root_scores = root_level
        .nodes
        .iter()
        .map(|node| {
            (
                node.node_id,
                metric_score_with_norms_and_squared_euclidean(
                    metric,
                    query_vector,
                    query_norm,
                    &node.centroid,
                    node.centroid_norm,
                ),
            )
        })
        .collect::<Vec<_>>();
    root_scores.sort_by(|(left_id, left_score), (right_id, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_id.cmp(right_id))
    });
    let selected_root_ids = root_scores
        .into_iter()
        .take(root_beam.max(1))
        .map(|(node_id, _)| node_id)
        .collect::<BTreeSet<_>>();
    let bucket_index_by_id = index_meta
        .buckets
        .iter()
        .enumerate()
        .map(|(index, bucket)| (bucket.bucket_id, index))
        .collect::<BTreeMap<_, _>>();
    let leaf_by_node_id = leaf_level
        .nodes
        .iter()
        .map(|node| (node.node_id, node))
        .collect::<BTreeMap<_, _>>();
    let mut candidates = Vec::new();
    let mut seen_bucket_ids = BTreeSet::new();
    for root_node in root_level
        .nodes
        .iter()
        .filter(|node| selected_root_ids.contains(&node.node_id))
    {
        for child_node_id in &root_node.child_node_ids {
            let Some(leaf) = leaf_by_node_id.get(child_node_id).copied() else {
                continue;
            };
            let Some(bucket_id) = leaf.bucket_id else {
                continue;
            };
            if !seen_bucket_ids.insert(bucket_id) {
                continue;
            }
            let Some(bucket_index) = bucket_index_by_id.get(&bucket_id).copied() else {
                continue;
            };
            let score = metric_score_with_norms_and_squared_euclidean(
                metric,
                query_vector,
                query_norm,
                &leaf.centroid,
                leaf.centroid_norm,
            );
            candidates.push((bucket_index, score));
        }
    }
    candidates.sort_by(|(left_index, left_score), (right_index, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_index.cmp(right_index))
    });
    Some(candidates)
}

fn encode_u32(out: &mut Vec<u8>, value: usize, label: &str) -> Result<(), ApiError> {
    let encoded = u32::try_from(value).map_err(|_| {
        ApiError::internal(format!(
            "{label} exceeds u32 range during ANN bucket encoding"
        ))
    })?;
    out.extend_from_slice(&encoded.to_le_bytes());
    Ok(())
}

fn encode_f32_slice(out: &mut Vec<u8>, values: &[f32]) {
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
}

fn decode_exact_slice<'a>(
    input: &'a [u8],
    cursor: &mut usize,
    count: usize,
    label: &str,
) -> Result<&'a [u8], ApiError> {
    let end = cursor
        .checked_add(count)
        .ok_or_else(|| ApiError::internal(format!("overflow while decoding {label}")))?;
    if end > input.len() {
        return Err(ApiError::internal(format!(
            "truncated ANN bucket while decoding {label}"
        )));
    }
    let slice = &input[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn decode_u32(input: &[u8], cursor: &mut usize, label: &str) -> Result<u32, ApiError> {
    let bytes = decode_exact_slice(input, cursor, size_of::<u32>(), label)?;
    let mut array = [0_u8; size_of::<u32>()];
    array.copy_from_slice(bytes);
    Ok(u32::from_le_bytes(array))
}

fn decode_u8(input: &[u8], cursor: &mut usize, label: &str) -> Result<u8, ApiError> {
    let bytes = decode_exact_slice(input, cursor, size_of::<u8>(), label)?;
    Ok(bytes[0])
}

fn decode_u32_vec(
    input: &[u8],
    cursor: &mut usize,
    count: usize,
    label: &str,
) -> Result<Vec<u32>, ApiError> {
    let byte_len = count
        .checked_mul(size_of::<u32>())
        .ok_or_else(|| ApiError::internal(format!("overflow while decoding {label}")))?;
    let bytes = decode_exact_slice(input, cursor, byte_len, label)?;
    let mut out = Vec::with_capacity(count);
    for chunk in bytes.chunks_exact(size_of::<u32>()) {
        let mut array = [0_u8; size_of::<u32>()];
        array.copy_from_slice(chunk);
        out.push(u32::from_le_bytes(array));
    }
    Ok(out)
}

fn decode_f32_vec(
    input: &[u8],
    cursor: &mut usize,
    count: usize,
    label: &str,
) -> Result<Vec<f32>, ApiError> {
    let byte_len = count
        .checked_mul(size_of::<f32>())
        .ok_or_else(|| ApiError::internal(format!("overflow while decoding {label}")))?;
    let bytes = decode_exact_slice(input, cursor, byte_len, label)?;
    let mut out = Vec::with_capacity(count);
    for chunk in bytes.chunks_exact(size_of::<f32>()) {
        let mut array = [0_u8; size_of::<f32>()];
        array.copy_from_slice(chunk);
        out.push(f32::from_le_bytes(array));
    }
    Ok(out)
}

enum EncodedAnnBucketPayload {
    Binary {
        signatures: Vec<u8>,
        stride_bytes: usize,
    },
}

fn choose_quantized_bucket_payload(
    values: &[f32],
    dimension: usize,
) -> Result<EncodedAnnBucketPayload, ApiError> {
    if dimension == 0 || !values.len().is_multiple_of(dimension) {
        return Err(ApiError::internal(
            "ANN quantization input has invalid dimension alignment",
        ));
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(ApiError::internal(
            "ANN bucket contains non-finite value during quantization",
        ));
    }
    let vector_count = values.len() / dimension;
    let stride_bytes = (dimension.saturating_add(7)) / 8;
    let mut signatures = Vec::with_capacity(vector_count.saturating_mul(stride_bytes));
    for vector in values.chunks_exact(dimension) {
        signatures.extend_from_slice(&pack_sign_bits(vector, stride_bytes));
    }
    Ok(EncodedAnnBucketPayload::Binary {
        signatures,
        stride_bytes,
    })
}

fn decode_ids_and_metadata(
    id_offsets: &[u32],
    metadata_offsets: &[u32],
    ids_blob: &[u8],
    metadata_blob: &[u8],
    vector_count: usize,
) -> Result<(Vec<String>, Vec<Option<Value>>), ApiError> {
    if id_offsets.first().copied().unwrap_or_default() != 0
        || id_offsets.last().copied().unwrap_or_default() as usize != ids_blob.len()
    {
        return Err(ApiError::internal("ANN id offsets are malformed"));
    }
    if metadata_offsets.first().copied().unwrap_or_default() != 0
        || metadata_offsets.last().copied().unwrap_or_default() as usize != metadata_blob.len()
    {
        return Err(ApiError::internal("ANN metadata offsets are malformed"));
    }

    let mut ids = Vec::with_capacity(vector_count);
    for index in 0..vector_count {
        let start = id_offsets[index] as usize;
        let end = id_offsets[index.saturating_add(1)] as usize;
        if start > end || end > ids_blob.len() {
            return Err(ApiError::internal("ANN id offset range is invalid"));
        }
        let id = std::str::from_utf8(&ids_blob[start..end])
            .map_err(|error| ApiError::internal(format!("invalid ANN id utf8: {error}")))?;
        if id.is_empty() {
            return Err(ApiError::internal("ANN bucket contains empty vector id"));
        }
        ids.push(id.to_string());
    }

    let mut metadata = Vec::with_capacity(vector_count);
    for index in 0..vector_count {
        let start = metadata_offsets[index] as usize;
        let end = metadata_offsets[index.saturating_add(1)] as usize;
        if start > end || end > metadata_blob.len() {
            return Err(ApiError::internal("ANN metadata offset range is invalid"));
        }
        if start == end {
            metadata.push(None);
            continue;
        }
        let value: Value = serde_json::from_slice(&metadata_blob[start..end]).map_err(|error| {
            ApiError::internal(format!("failed to parse ANN metadata payload: {error}"))
        })?;
        if !value.is_object() {
            return Err(ApiError::internal(
                "ANN metadata payload must deserialize to JSON object",
            ));
        }
        metadata.push(Some(value));
    }
    Ok((ids, metadata))
}

fn encode_ann_bucket(
    bucket_id: usize,
    dimension: usize,
    metric: &Metric,
    vectors: &[UpsertVector],
) -> Result<Vec<u8>, ApiError> {
    let vector_count = vectors.len();
    let has_norms = matches!(metric, Metric::Cosine);
    let mut ids_blob = Vec::new();
    let mut metadata_blob = Vec::new();
    let mut id_offsets = Vec::with_capacity(vector_count.saturating_add(1));
    let mut metadata_offsets = Vec::with_capacity(vector_count.saturating_add(1));
    let mut flattened_values = Vec::with_capacity(vector_count.saturating_mul(dimension));
    let mut norms = if has_norms {
        Vec::with_capacity(vector_count)
    } else {
        Vec::new()
    };

    id_offsets.push(0_u32);
    metadata_offsets.push(0_u32);
    for vector in vectors {
        if vector.values.len() != dimension {
            return Err(ApiError::internal(format!(
                "invalid ANN vector dimension: expected {dimension}, got {}",
                vector.values.len()
            )));
        }
        flattened_values.extend_from_slice(&vector.values);
        if has_norms {
            norms.push(l2_norm(&vector.values));
        }

        ids_blob.extend_from_slice(vector.id.as_bytes());
        id_offsets.push(
            u32::try_from(ids_blob.len()).map_err(|_| {
                ApiError::internal("ANN id blob exceeded u32 while encoding bucket")
            })?,
        );

        if let Some(metadata) = vector.metadata.as_ref() {
            let encoded = serde_json::to_vec(metadata).map_err(|error| {
                ApiError::internal(format!("failed to encode ANN metadata payload: {error}"))
            })?;
            metadata_blob.extend_from_slice(&encoded);
        }
        metadata_offsets.push(u32::try_from(metadata_blob.len()).map_err(|_| {
            ApiError::internal("ANN metadata blob exceeded u32 while encoding bucket")
        })?);
    }

    let payload = choose_quantized_bucket_payload(&flattened_values, dimension)?;
    let mut flags = if has_norms {
        ANN_BUCKET_FLAG_HAS_NORMS
    } else {
        0
    };
    flags |= ANN_BUCKET_FLAG_PAYLOAD_BINARY;
    let scale_count = 0;

    let mut out = Vec::new();
    out.extend_from_slice(ANN_BUCKET_MAGIC);
    out.push(ANN_BUCKET_VERSION_BINARY);
    out.push(flags);
    out.extend_from_slice(&0_u16.to_le_bytes());
    encode_u32(&mut out, bucket_id, "ANN bucket id")?;
    encode_u32(&mut out, dimension, "ANN bucket dimension")?;
    encode_u32(&mut out, vector_count, "ANN bucket vector count")?;
    encode_u32(&mut out, ids_blob.len(), "ANN id blob length")?;
    encode_u32(&mut out, metadata_blob.len(), "ANN metadata blob length")?;
    encode_u32(&mut out, scale_count, "ANN int8 scale count")?;
    for offset in id_offsets {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    for offset in metadata_offsets {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    let EncodedAnnBucketPayload::Binary {
        signatures,
        stride_bytes,
    } = payload;
    encode_u32(&mut out, stride_bytes, "ANN binary stride")?;
    encode_u32(
        &mut out,
        signatures.len(),
        "ANN binary signature byte length",
    )?;
    out.extend_from_slice(&signatures);
    if has_norms {
        encode_f32_slice(&mut out, &norms);
    }
    out.extend_from_slice(&ids_blob);
    out.extend_from_slice(&metadata_blob);
    Ok(out)
}

fn decode_ann_bucket(raw: &[u8]) -> Result<AnnBucketData, ApiError> {
    let mut cursor = 0usize;
    let magic = decode_exact_slice(raw, &mut cursor, ANN_BUCKET_MAGIC.len(), "ANN magic")?;
    if magic != ANN_BUCKET_MAGIC {
        return Err(ApiError::internal("invalid ANN bucket magic"));
    }
    let version = decode_u8(raw, &mut cursor, "ANN bucket version")?;
    if version != ANN_BUCKET_VERSION_BINARY {
        return Err(ApiError::internal(format!(
            "unsupported ANN bucket version: {version}"
        )));
    }
    let flags = decode_u8(raw, &mut cursor, "ANN bucket flags")?;
    let _reserved = decode_exact_slice(raw, &mut cursor, size_of::<u16>(), "ANN reserved bytes")?;

    let bucket_id = decode_u32(raw, &mut cursor, "ANN bucket id")? as usize;
    let dimension = decode_u32(raw, &mut cursor, "ANN bucket dimension")? as usize;
    let vector_count = decode_u32(raw, &mut cursor, "ANN bucket vector count")? as usize;
    let ids_blob_len = decode_u32(raw, &mut cursor, "ANN id blob len")? as usize;
    let metadata_blob_len = decode_u32(raw, &mut cursor, "ANN metadata blob len")? as usize;
    let scale_count = decode_u32(raw, &mut cursor, "ANN scale count")? as usize;

    if dimension == 0 {
        return Err(ApiError::internal("ANN bucket has zero dimension"));
    }

    let id_offsets = decode_u32_vec(
        raw,
        &mut cursor,
        vector_count.saturating_add(1),
        "ANN id offsets",
    )?;
    let metadata_offsets = decode_u32_vec(
        raw,
        &mut cursor,
        vector_count.saturating_add(1),
        "ANN metadata offsets",
    )?;
    if (flags & ANN_BUCKET_FLAG_PAYLOAD_BINARY) == 0 {
        return Err(ApiError::internal(
            "ANN binary bucket missing binary payload flag",
        ));
    }
    if scale_count != 0 {
        return Err(ApiError::internal(
            "ANN binary bucket should not provide non-zero scale count",
        ));
    }
    let stride_bytes = decode_u32(raw, &mut cursor, "ANN binary stride")? as usize;
    let signatures_len = decode_u32(raw, &mut cursor, "ANN binary signatures length")? as usize;
    if stride_bytes == 0 {
        return Err(ApiError::internal(
            "ANN binary bucket has zero signature stride",
        ));
    }
    let expected_len = vector_count
        .checked_mul(stride_bytes)
        .ok_or_else(|| ApiError::internal("ANN binary signature length overflow during decode"))?;
    if signatures_len != expected_len {
        return Err(ApiError::internal(format!(
            "ANN binary signature length mismatch: expected {expected_len}, got {signatures_len}"
        )));
    }
    let values = AnnBucketValues::Binary {
        signatures: decode_exact_slice(raw, &mut cursor, signatures_len, "ANN binary signatures")?
            .to_vec(),
        stride_bytes,
    };
    let has_norms = (flags & ANN_BUCKET_FLAG_HAS_NORMS) != 0;
    let norms = if has_norms {
        decode_f32_vec(raw, &mut cursor, vector_count, "ANN norms")?
    } else {
        Vec::new()
    };

    let ids_blob = decode_exact_slice(raw, &mut cursor, ids_blob_len, "ANN id blob")?;
    let metadata_blob =
        decode_exact_slice(raw, &mut cursor, metadata_blob_len, "ANN metadata blob")?;
    if cursor != raw.len() {
        return Err(ApiError::internal("ANN bucket payload has trailing bytes"));
    }

    let (ids, metadata) = decode_ids_and_metadata(
        &id_offsets,
        &metadata_offsets,
        ids_blob,
        metadata_blob,
        vector_count,
    )?;

    Ok(AnnBucketData {
        bucket_id,
        dimension,
        ids,
        metadata,
        values,
        norms,
    })
}

fn encode_ann_filter_row_bitmap(bitmap: &RoaringBitmap) -> Result<Vec<u8>, ApiError> {
    let mut encoded_bitmap = Vec::new();
    bitmap
        .serialize_into(&mut encoded_bitmap)
        .map_err(|error| ApiError::internal(format!("failed to encode row bitmap: {error}")))?;
    let encoded_len = u32::try_from(encoded_bitmap.len()).map_err(|_| {
        ApiError::internal("row bitmap payload exceeds u32 length during ANN filter encode")
    })?;
    let mut out = Vec::with_capacity(ANN_FILTER_ROW_MAGIC.len() + 1 + 4 + encoded_bitmap.len());
    out.extend_from_slice(ANN_FILTER_ROW_MAGIC);
    out.push(ANN_FILTER_CODEC_VERSION);
    out.extend_from_slice(&encoded_len.to_le_bytes());
    out.extend_from_slice(&encoded_bitmap);
    Ok(out)
}

fn decode_ann_filter_row_bitmap(raw: &[u8]) -> Result<RoaringBitmap, ApiError> {
    let mut cursor = 0usize;
    let magic = decode_exact_slice(
        raw,
        &mut cursor,
        ANN_FILTER_ROW_MAGIC.len(),
        "row bitmap magic",
    )?;
    if magic != ANN_FILTER_ROW_MAGIC {
        return Err(ApiError::internal("invalid ANN filter row bitmap magic"));
    }
    let version = decode_u8(raw, &mut cursor, "row bitmap version")?;
    if version != ANN_FILTER_CODEC_VERSION {
        return Err(ApiError::internal(format!(
            "unsupported ANN filter row bitmap version: {version}"
        )));
    }
    let bitmap_len = decode_u32(raw, &mut cursor, "row bitmap length")? as usize;
    let bitmap_bytes = decode_exact_slice(raw, &mut cursor, bitmap_len, "row bitmap bytes")?;
    if cursor != raw.len() {
        return Err(ApiError::internal(
            "ANN filter row bitmap payload has trailing bytes",
        ));
    }
    RoaringBitmap::deserialize_from(&mut Cursor::new(bitmap_bytes)).map_err(|error| {
        ApiError::internal(format!(
            "failed to decode ANN filter row bitmap payload: {error}"
        ))
    })
}

fn encode_ann_filter_cluster_summary(
    summary: &AnnFilterClusterSummary,
) -> Result<Vec<u8>, ApiError> {
    let mut encoded_bitmap = Vec::new();
    summary
        .bucket_bitmap
        .serialize_into(&mut encoded_bitmap)
        .map_err(|error| ApiError::internal(format!("failed to encode cluster bitmap: {error}")))?;
    let bitmap_len = u32::try_from(encoded_bitmap.len()).map_err(|_| {
        ApiError::internal("cluster bitmap payload exceeds u32 length during ANN filter encode")
    })?;
    let count_len = u32::try_from(summary.bucket_match_counts.len()).map_err(|_| {
        ApiError::internal("cluster count payload exceeds u32 length during ANN filter encode")
    })?;
    let mut out = Vec::new();
    out.extend_from_slice(ANN_FILTER_CLUSTER_MAGIC);
    out.push(ANN_FILTER_CODEC_VERSION);
    out.extend_from_slice(&bitmap_len.to_le_bytes());
    out.extend_from_slice(&count_len.to_le_bytes());
    out.extend_from_slice(&encoded_bitmap);
    for (bucket_id, count) in &summary.bucket_match_counts {
        out.extend_from_slice(&bucket_id.to_le_bytes());
        out.extend_from_slice(&count.to_le_bytes());
    }
    Ok(out)
}

fn decode_ann_filter_cluster_summary(raw: &[u8]) -> Result<AnnFilterClusterSummary, ApiError> {
    let mut cursor = 0usize;
    let magic = decode_exact_slice(
        raw,
        &mut cursor,
        ANN_FILTER_CLUSTER_MAGIC.len(),
        "cluster summary magic",
    )?;
    if magic != ANN_FILTER_CLUSTER_MAGIC {
        return Err(ApiError::internal(
            "invalid ANN filter cluster summary magic",
        ));
    }
    let version = decode_u8(raw, &mut cursor, "cluster summary version")?;
    if version != ANN_FILTER_CODEC_VERSION {
        return Err(ApiError::internal(format!(
            "unsupported ANN filter cluster summary version: {version}"
        )));
    }
    let bitmap_len = decode_u32(raw, &mut cursor, "cluster summary bitmap length")? as usize;
    let count_len = decode_u32(raw, &mut cursor, "cluster summary count length")? as usize;
    let bitmap_bytes = decode_exact_slice(raw, &mut cursor, bitmap_len, "cluster summary bitmap")?;
    let bucket_bitmap =
        RoaringBitmap::deserialize_from(&mut Cursor::new(bitmap_bytes)).map_err(|error| {
            ApiError::internal(format!(
                "failed to decode ANN filter cluster bitmap: {error}"
            ))
        })?;
    let mut bucket_match_counts = BTreeMap::new();
    for _ in 0..count_len {
        let bucket_id = decode_u32(raw, &mut cursor, "cluster summary bucket id")?;
        let count = decode_u32(raw, &mut cursor, "cluster summary bucket count")?;
        bucket_match_counts.insert(bucket_id, count);
    }
    if cursor != raw.len() {
        return Err(ApiError::internal(
            "ANN filter cluster summary payload has trailing bytes",
        ));
    }
    Ok(AnnFilterClusterSummary {
        bucket_bitmap,
        bucket_match_counts,
    })
}

async fn build_filter_index_artifacts(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
    buckets: &[Vec<UpsertVector>],
) -> Result<Option<AnnFilterIndexMeta>, ApiError> {
    let mut cluster_bitmaps: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
    let mut row_bitmaps: BTreeMap<(String, usize), RoaringBitmap> = BTreeMap::new();
    let mut value_lexicon: BTreeMap<String, BTreeMap<String, FilterValueLexiconEntry>> =
        BTreeMap::new();
    let mut token_lexicon: BTreeMap<String, BTreeMap<String, FilterTokenLexiconEntry>> =
        BTreeMap::new();

    for (bucket_id, bucket_vectors) in buckets.iter().enumerate() {
        if bucket_vectors.is_empty() {
            continue;
        }
        for (local_id, vector) in bucket_vectors.iter().enumerate() {
            let Some(Value::Object(metadata)) = vector.metadata.as_ref() else {
                continue;
            };
            for (field, value) in metadata {
                match value {
                    Value::Array(entries) => {
                        for entry in entries {
                            index_filter_scalar_value(
                                field,
                                entry,
                                bucket_id,
                                local_id,
                                &mut cluster_bitmaps,
                                &mut row_bitmaps,
                                &mut value_lexicon,
                            )?;
                            if let Value::String(string_value) = entry {
                                let mut seen_tokens = BTreeSet::new();
                                for token in tokenize_text(string_value) {
                                    if token.is_empty() || !seen_tokens.insert(token.clone()) {
                                        continue;
                                    }
                                    let term_hash = filter_token_term_hash(field, &token);
                                    insert_term_match(
                                        term_hash.clone(),
                                        bucket_id,
                                        local_id,
                                        &mut cluster_bitmaps,
                                        &mut row_bitmaps,
                                    );
                                    token_lexicon
                                        .entry(field.clone())
                                        .or_default()
                                        .entry(term_hash.clone())
                                        .or_insert_with(|| FilterTokenLexiconEntry {
                                            token: token.clone(),
                                            term_hash,
                                        });
                                }
                            }
                        }
                    }
                    _ => {
                        index_filter_scalar_value(
                            field,
                            value,
                            bucket_id,
                            local_id,
                            &mut cluster_bitmaps,
                            &mut row_bitmaps,
                            &mut value_lexicon,
                        )?;
                    }
                }

                if let Value::String(string_value) = value {
                    let mut seen_tokens = BTreeSet::new();
                    for token in tokenize_text(string_value) {
                        if token.is_empty() || !seen_tokens.insert(token.clone()) {
                            continue;
                        }
                        let term_hash = filter_token_term_hash(field, &token);
                        insert_term_match(
                            term_hash.clone(),
                            bucket_id,
                            local_id,
                            &mut cluster_bitmaps,
                            &mut row_bitmaps,
                        );
                        token_lexicon
                            .entry(field.clone())
                            .or_default()
                            .entry(term_hash.clone())
                            .or_insert_with(|| FilterTokenLexiconEntry {
                                token: token.clone(),
                                term_hash,
                            });
                    }
                }
            }
        }
    }

    if cluster_bitmaps.is_empty() {
        return Ok(None);
    }

    for (term_hash, bucket_bitmap) in &cluster_bitmaps {
        let mut bucket_match_counts = BTreeMap::new();
        for bucket_id in bucket_bitmap {
            let count = row_bitmaps
                .get(&(term_hash.clone(), bucket_id as usize))
                .map(|bitmap| bitmap.len())
                .unwrap_or(0);
            if count > 0 {
                let encoded_count = u32::try_from(count).map_err(|_| {
                    ApiError::internal("filter match count exceeded u32 during ANN filter encode")
                })?;
                bucket_match_counts.insert(bucket_id, encoded_count);
            }
        }
        let summary = AnnFilterClusterSummary {
            bucket_bitmap: bucket_bitmap.clone(),
            bucket_match_counts,
        };
        let summary_key =
            ann_filter_cluster_object_key(collection, namespace, generation, term_hash);
        let summary_bytes = encode_ann_filter_cluster_summary(&summary)?;
        state
            .storage
            .put_bytes_if_absent(&summary_key, &summary_bytes)
            .await
            .map_err(map_store_error)?;
    }

    for ((term_hash, bucket_id), row_bitmap) in row_bitmaps {
        let row_key =
            ann_filter_row_object_key(collection, namespace, generation, &term_hash, bucket_id);
        let row_bytes = encode_ann_filter_row_bitmap(&row_bitmap)?;
        state
            .storage
            .put_bytes_if_absent(&row_key, &row_bytes)
            .await
            .map_err(map_store_error)?;
    }

    let mut serialized_value_lexicon = BTreeMap::new();
    for (field, entries) in value_lexicon {
        let serialized_entries = entries.into_values().collect::<Vec<_>>();
        serialized_value_lexicon.insert(field, serialized_entries);
    }

    let mut serialized_token_lexicon = BTreeMap::new();
    for (field, entries) in token_lexicon {
        let serialized_entries = entries.into_values().collect::<Vec<_>>();
        serialized_token_lexicon.insert(field, serialized_entries);
    }

    Ok(Some(AnnFilterIndexMeta {
        value_lexicon: serialized_value_lexicon,
        token_lexicon: serialized_token_lexicon,
    }))
}

fn index_filter_scalar_value(
    field: &str,
    value: &Value,
    bucket_id: usize,
    local_id: usize,
    cluster_bitmaps: &mut BTreeMap<String, RoaringBitmap>,
    row_bitmaps: &mut BTreeMap<(String, usize), RoaringBitmap>,
    value_lexicon: &mut BTreeMap<String, BTreeMap<String, FilterValueLexiconEntry>>,
) -> Result<(), ApiError> {
    let Some(value_type) = scalar_value_type(value) else {
        return Ok(());
    };
    let term_hash = filter_value_term_hash(field, value)?;
    insert_term_match(
        term_hash.clone(),
        bucket_id,
        local_id,
        cluster_bitmaps,
        row_bitmaps,
    );
    value_lexicon
        .entry(field.to_string())
        .or_default()
        .entry(term_hash.clone())
        .or_insert_with(|| FilterValueLexiconEntry {
            value: value.clone(),
            value_type,
            term_hash,
        });
    Ok(())
}

fn insert_term_match(
    term_hash: String,
    bucket_id: usize,
    local_id: usize,
    cluster_bitmaps: &mut BTreeMap<String, RoaringBitmap>,
    row_bitmaps: &mut BTreeMap<(String, usize), RoaringBitmap>,
) {
    cluster_bitmaps
        .entry(term_hash.clone())
        .or_default()
        .insert(bucket_id as u32);
    row_bitmaps
        .entry((term_hash, bucket_id))
        .or_default()
        .insert(local_id as u32);
}

async fn load_ann_filter_cluster_summary(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
    term_hash: &str,
    stats: &mut AnnQueryExecutionStats,
    read_budget: &mut AnnObjectReadBudget,
) -> Result<AnnFilterClusterSummary, ApiError> {
    let key = ann_filter_cluster_object_key(collection, namespace, generation, term_hash);
    if let Some(cached) = state.get_filter_cluster_cache(&key).await {
        stats.filter_cluster_cache_hits = stats.filter_cluster_cache_hits.saturating_add(1);
        return Ok(cached);
    }
    stats.filter_cluster_cache_misses = stats.filter_cluster_cache_misses.saturating_add(1);
    if !read_budget.reserve(1) {
        stats.object_read_budget_exceeded = true;
        return Err(ApiError::store_unavailable(
            "ANN object read budget exceeded while loading cluster filter summary",
        ));
    }
    stats.ann_filter_cluster_object_reads = stats.ann_filter_cluster_object_reads.saturating_add(1);

    let raw = match state.storage.get_bytes(&key).await {
        Ok(raw) => raw,
        Err(TurboVectorError::NotFound(_)) => {
            let empty = AnnFilterClusterSummary::default();
            let evictions = state.put_filter_cluster_cache(key, empty.clone()).await;
            stats.filter_cluster_cache_evictions = stats
                .filter_cluster_cache_evictions
                .saturating_add(evictions as usize);
            return Ok(empty);
        }
        Err(error) => return Err(map_store_error(error)),
    };
    stats.ann_filter_cluster_object_bytes = stats
        .ann_filter_cluster_object_bytes
        .saturating_add(raw.len());
    let decoded = decode_ann_filter_cluster_summary(&raw)?;
    let evictions = state.put_filter_cluster_cache(key, decoded.clone()).await;
    stats.filter_cluster_cache_evictions = stats
        .filter_cluster_cache_evictions
        .saturating_add(evictions as usize);
    Ok(decoded)
}

async fn load_ann_filter_row_bitmap(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
    term_hash: &str,
    bucket_id: usize,
    stats: &mut AnnQueryExecutionStats,
    read_budget: &mut AnnObjectReadBudget,
) -> Result<RoaringBitmap, ApiError> {
    let key = ann_filter_row_object_key(collection, namespace, generation, term_hash, bucket_id);
    if let Some(cached) = state.get_filter_row_cache(&key).await {
        stats.filter_row_cache_hits = stats.filter_row_cache_hits.saturating_add(1);
        return Ok(cached);
    }
    stats.filter_row_cache_misses = stats.filter_row_cache_misses.saturating_add(1);
    if !read_budget.reserve(1) {
        stats.object_read_budget_exceeded = true;
        return Err(ApiError::store_unavailable(
            "ANN object read budget exceeded while loading row filter bitmap",
        ));
    }
    stats.ann_filter_row_object_reads = stats.ann_filter_row_object_reads.saturating_add(1);

    let raw = match state.storage.get_bytes(&key).await {
        Ok(raw) => raw,
        Err(TurboVectorError::NotFound(_)) => {
            let empty = RoaringBitmap::new();
            let evictions = state.put_filter_row_cache(key, empty.clone()).await;
            stats.filter_row_cache_evictions = stats
                .filter_row_cache_evictions
                .saturating_add(evictions as usize);
            return Ok(empty);
        }
        Err(error) => return Err(map_store_error(error)),
    };
    stats.ann_filter_row_object_bytes = stats.ann_filter_row_object_bytes.saturating_add(raw.len());
    let decoded = decode_ann_filter_row_bitmap(&raw)?;
    let evictions = state.put_filter_row_cache(key, decoded.clone()).await;
    stats.filter_row_cache_evictions = stats
        .filter_row_cache_evictions
        .saturating_add(evictions as usize);
    Ok(decoded)
}

async fn resolve_native_filter_allowed_buckets(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
    plan: &NativeFilterPlan,
    index_meta: &AnnIndexMeta,
    stats: &mut AnnQueryExecutionStats,
    read_budget: &mut AnnObjectReadBudget,
) -> Result<crate::filters::planner::NativeFilterClusterEstimate, ApiError> {
    let mut all_buckets = RoaringBitmap::new();
    let mut bucket_sizes = BTreeMap::new();
    for bucket in &index_meta.buckets {
        all_buckets.insert(bucket.bucket_id as u32);
        bucket_sizes.insert(bucket.bucket_id as u32, bucket.vector_count as u32);
    }
    if matches!(plan.expression, NativeFilterExpression::MatchAll) {
        return Ok(evaluate_native_filter_cluster_estimate(
            &plan.expression,
            &BTreeMap::new(),
            &bucket_sizes,
            &all_buckets,
        ));
    }
    if matches!(plan.expression, NativeFilterExpression::MatchNone) {
        return Ok(evaluate_native_filter_cluster_estimate(
            &plan.expression,
            &BTreeMap::new(),
            &bucket_sizes,
            &all_buckets,
        ));
    }

    let mut term_summaries = BTreeMap::new();
    for term_hash in &plan.term_hashes {
        let summary = load_ann_filter_cluster_summary(
            state,
            collection,
            namespace,
            generation,
            term_hash,
            stats,
            read_budget,
        )
        .await?;
        term_summaries.insert(term_hash.clone(), summary);
    }

    Ok(evaluate_native_filter_cluster_estimate(
        &plan.expression,
        &term_summaries,
        &bucket_sizes,
        &all_buckets,
    ))
}

async fn resolve_native_filter_allowed_locals(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
    plan: &NativeFilterPlan,
    bucket_id: usize,
    bucket_vector_count: usize,
    stats: &mut AnnQueryExecutionStats,
    read_budget: &mut AnnObjectReadBudget,
) -> Result<Option<RoaringBitmap>, ApiError> {
    if matches!(plan.expression, NativeFilterExpression::MatchAll) {
        return Ok(None);
    }
    if matches!(plan.expression, NativeFilterExpression::MatchNone) {
        return Ok(Some(RoaringBitmap::new()));
    }

    let mut term_bitmaps = BTreeMap::new();
    for term_hash in &plan.term_hashes {
        let bitmap = load_ann_filter_row_bitmap(
            state,
            collection,
            namespace,
            generation,
            term_hash,
            bucket_id,
            stats,
            read_budget,
        )
        .await?;
        term_bitmaps.insert(term_hash.clone(), bitmap);
    }
    Ok(evaluate_native_filter_row_bitmap(
        &plan.expression,
        &term_bitmaps,
        bucket_vector_count,
    ))
}

async fn load_ann_index_meta(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
    mut stats: Option<&mut AnnQueryExecutionStats>,
    mut read_budget: Option<&mut AnnObjectReadBudget>,
) -> Result<Option<AnnIndexMeta>, ApiError> {
    if let Some(budget) = read_budget.as_deref_mut() {
        if !budget.reserve(1) {
            if let Some(stats) = stats.as_deref_mut() {
                stats.object_read_budget_exceeded = true;
            }
            return Err(ApiError::store_unavailable(
                "ANN object read budget exceeded while loading index metadata",
            ));
        }
    }
    let key = ann_index_meta_key(collection, namespace, generation);
    let raw = match state.storage.get_bytes(&key).await {
        Ok(raw) => {
            if let Some(stats) = stats.as_deref_mut() {
                stats.ann_meta_object_reads = stats.ann_meta_object_reads.saturating_add(1);
                stats.ann_meta_object_bytes = stats.ann_meta_object_bytes.saturating_add(raw.len());
            }
            raw
        }
        Err(TurboVectorError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(map_store_error(error)),
    };
    let meta: AnnIndexMeta = serde_json::from_slice(&raw).map_err(|error| {
        ApiError::internal(format!("failed to parse ANN index metadata: {error}"))
    })?;
    Ok(Some(meta))
}

async fn build_ann_index(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
    metric: &Metric,
    dimension: u32,
    vectors: &BTreeMap<String, UpsertVector>,
) -> Result<Option<AnnIndexMeta>, ApiError> {
    if vectors.len() < ANN_MIN_VECTORS {
        return Ok(None);
    }

    let ordered_vectors: Vec<UpsertVector> = vectors.values().cloned().collect();
    let centroid_count = choose_centroid_count(ordered_vectors.len());
    if centroid_count == 0 {
        return Ok(None);
    }
    let mut centroids = initial_centroids(&ordered_vectors, centroid_count);

    for _ in 0..ANN_KMEANS_ITERATIONS {
        let mut sums = vec![vec![0.0_f32; dimension as usize]; centroid_count];
        let mut counts = vec![0usize; centroid_count];
        for vector in &ordered_vectors {
            let centroid_index = nearest_centroid(metric, &vector.values, &centroids);
            counts[centroid_index] = counts[centroid_index].saturating_add(1);
            for (sum, value) in sums[centroid_index].iter_mut().zip(vector.values.iter()) {
                *sum += *value;
            }
        }
        for (index, count) in counts.into_iter().enumerate() {
            if count == 0 {
                continue;
            }
            let inverse_count = 1.0_f32 / count as f32;
            for value in &mut sums[index] {
                *value *= inverse_count;
            }
            centroids[index] = sums[index].clone();
        }
    }

    let mut buckets: Vec<Vec<UpsertVector>> = (0..centroid_count).map(|_| Vec::new()).collect();
    for vector in ordered_vectors {
        let centroid_index = nearest_centroid(metric, &vector.values, &centroids);
        buckets[centroid_index].push(vector);
    }

    let mut bucket_metas = Vec::new();
    let mut flattened_centroids = Vec::new();
    let mut centroid_norms = Vec::new();
    for (bucket_id, bucket_vectors) in buckets.iter().enumerate() {
        if bucket_vectors.is_empty() {
            continue;
        }
        let bucket_key = ann_bucket_object_key(collection, namespace, generation, bucket_id);
        let bucket_bytes =
            encode_ann_bucket(bucket_id, dimension as usize, metric, &bucket_vectors)?;
        state
            .storage
            .put_bytes_if_absent(&bucket_key, &bucket_bytes)
            .await
            .map_err(map_store_error)?;
        flattened_centroids.extend_from_slice(&centroids[bucket_id]);
        centroid_norms.push(l2_norm(&centroids[bucket_id]));
        bucket_metas.push(AnnBucketMeta {
            bucket_id,
            object_key: bucket_key,
            vector_count: bucket_vectors.len(),
        });
    }

    if bucket_metas.is_empty() {
        return Ok(None);
    }

    let filter_index =
        build_filter_index_artifacts(state, collection, namespace, generation, &buckets).await?;
    let tree_levels = build_tree_levels(
        &bucket_metas,
        &flattened_centroids,
        &centroid_norms,
        dimension as usize,
    );

    let index_meta = AnnIndexMeta {
        generation,
        collection: collection.to_string(),
        namespace: namespace.to_string(),
        dimension,
        metric: metric.clone(),
        vector_count: vectors.len(),
        centroids: flattened_centroids,
        centroid_norms,
        buckets: bucket_metas,
        tree_levels,
        filter_index,
    };
    let index_bytes = serde_json::to_vec(&index_meta).map_err(|error| {
        ApiError::internal(format!("failed to serialize ANN index metadata: {error}"))
    })?;
    let index_key = ann_index_meta_key(collection, namespace, generation);
    let published = state
        .storage
        .put_bytes_if_absent(&index_key, &index_bytes)
        .await
        .map_err(map_store_error)?;
    if published {
        return Ok(Some(index_meta));
    }

    if let Some(existing) =
        load_ann_index_meta(state, collection, namespace, generation, None, None).await?
    {
        return Ok(Some(existing));
    }

    Err(ApiError::store_unavailable(format!(
        "ANN index metadata publish contention for collection '{collection}' namespace '{namespace}' generation {generation}"
    )))
}

async fn load_or_build_ann_index_meta(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
    metric: &Metric,
    dimension: u32,
) -> Result<Option<AnnIndexMeta>, ApiError> {
    if let Some(meta) = load_ann_index_meta(
        state,
        collection,
        namespace,
        manifest.generation,
        None,
        None,
    )
    .await?
    {
        return Ok(Some(meta));
    }

    let _build_guard = state
        .lock_ann_index_build(collection, namespace, manifest.generation)
        .await;
    if let Some(meta) = load_ann_index_meta(
        state,
        collection,
        namespace,
        manifest.generation,
        None,
        None,
    )
    .await?
    {
        return Ok(Some(meta));
    }

    let vectors = load_namespace_vectors(state, collection, namespace, manifest).await?;
    build_ann_index(
        state,
        collection,
        namespace,
        manifest.generation,
        metric,
        dimension,
        vectors.as_ref(),
    )
    .await
}

pub(crate) async fn ensure_ann_indexes_for_manifest(
    state: &AppState,
    collection: &str,
    manifest: &Manifest,
    metric: &Metric,
    dimension: u32,
) -> Result<(), ApiError> {
    for namespace in manifest.namespace_partitions.keys() {
        let _ =
            load_or_build_ann_index_meta(state, collection, namespace, manifest, metric, dimension)
                .await?;
    }
    Ok(())
}

fn min_probe_count(top_k: usize, bucket_count: usize, has_filter: bool) -> usize {
    if bucket_count == 0 {
        return 0;
    }
    let mut baseline: usize = match top_k {
        0..=5 => 12,
        6..=10 => 20,
        11..=25 => 22,
        26..=50 => 24,
        _ => 26,
    };
    if has_filter {
        baseline = baseline.saturating_add(2);
    }
    baseline.clamp(1, bucket_count)
}

fn target_candidate_count(
    top_k: usize,
    vector_count: usize,
    bucket_count: usize,
    has_filter: bool,
    estimated_filter_matches: Option<usize>,
) -> usize {
    if top_k == 0 || vector_count == 0 || bucket_count == 0 {
        return 0;
    }
    if vector_count <= ANN_SMALL_CORPUS_VECTOR_THRESHOLD {
        return vector_count;
    }
    let multiplier = match top_k {
        0..=5 => 56,
        6..=10 => 48,
        11..=25 => 40,
        26..=50 => 32,
        _ => 24,
    };
    let mut candidates = top_k.saturating_mul(multiplier).max(top_k);
    if has_filter {
        candidates = candidates.saturating_mul(2);
    }
    if has_filter {
        if let Some(estimated_matches) = estimated_filter_matches {
            let estimated_matches = estimated_matches.max(1).min(vector_count);
            let selectivity = (estimated_matches as f64 / vector_count as f64)
                .clamp(1.0 / vector_count as f64, 1.0);
            let inverse_selectivity = (1.0 / selectivity).clamp(1.0, 32.0);
            candidates = ((candidates as f64) * inverse_selectivity).ceil() as usize;
        }
    }
    let average_bucket_size = (vector_count.saturating_add(bucket_count).saturating_sub(1))
        .checked_div(bucket_count)
        .unwrap_or(1)
        .max(1);
    let floor =
        average_bucket_size.saturating_mul(min_probe_count(top_k, bucket_count, has_filter));
    candidates.max(floor).min(vector_count)
}

fn target_probe_count(
    top_k: usize,
    vector_count: usize,
    bucket_count: usize,
    target_candidates: usize,
    has_filter: bool,
    estimated_filter_matches: Option<usize>,
) -> usize {
    if bucket_count == 0 || vector_count == 0 || target_candidates == 0 {
        return 0;
    }
    let average_bucket_size = if has_filter {
        estimated_filter_matches
            .filter(|value| *value > 0)
            .map(|value| value as f64 / bucket_count as f64)
            .unwrap_or(vector_count as f64 / bucket_count as f64)
            .max(1.0)
    } else {
        (vector_count as f64 / bucket_count as f64).max(1.0)
    };
    let required = (target_candidates as f64 / average_bucket_size).ceil() as usize;
    let lower_bound = min_probe_count(top_k, bucket_count, has_filter);
    let upper_bound = if vector_count <= ANN_SMALL_CORPUS_VECTOR_THRESHOLD {
        bucket_count
    } else {
        ANN_MAX_PROBES.min(bucket_count)
    };
    required.clamp(lower_bound, upper_bound).max(1)
}

fn rerank_candidate_limit(
    top_k: usize,
    first_stage_candidates: usize,
    prune_ratio: f32,
    max_candidates: usize,
) -> usize {
    if top_k == 0 || first_stage_candidates == 0 {
        return 0;
    }
    let ratio_limit =
        ((first_stage_candidates as f32) * prune_ratio.clamp(0.01, 1.0)).ceil() as usize;
    ratio_limit.max(top_k).min(max_candidates.max(top_k))
}

fn apply_quantization_bound_pruning(
    scored_candidates: &mut Vec<ScoredCandidateRef>,
    top_k: usize,
    max_rerank_candidates: usize,
    prune_ratio: f32,
    bound_margin: f32,
    stats: &mut AnnQueryExecutionStats,
) {
    stats.first_stage_candidate_count = scored_candidates.len();
    stats.quantization_bound_margin = bound_margin.max(0.0);
    if scored_candidates.is_empty() || top_k == 0 {
        stats.rerank_candidate_count = 0;
        return;
    }
    let threshold_index = top_k
        .saturating_sub(1)
        .min(scored_candidates.len().saturating_sub(1));
    let kth_score = scored_candidates[threshold_index].score;
    let threshold = kth_score - stats.quantization_bound_margin;
    stats.quantization_bound_threshold = threshold;

    // Keep only candidates whose deterministic upper bound can still enter top-k.
    scored_candidates.retain(|candidate| candidate.score >= threshold);
    let effective_ratio = if scored_candidates.len() >= ANN_BINARY_PRUNE_MIN_CANDIDATES {
        prune_ratio.clamp(0.01, 1.0)
    } else {
        1.0
    };
    let rerank_limit = rerank_candidate_limit(
        top_k,
        scored_candidates.len(),
        effective_ratio,
        max_rerank_candidates,
    );
    if scored_candidates.len() > rerank_limit {
        scored_candidates.truncate(rerank_limit);
    }
    stats.rerank_candidate_count = scored_candidates.len();
}

fn compare_scored_candidates(
    left: &ScoredCandidateRef,
    right: &ScoredCandidateRef,
    loaded_buckets: &[Option<Arc<AnnBucketData>>],
) -> Ordering {
    right.score.total_cmp(&left.score).then_with(|| {
        let left_id = loaded_buckets[left.selected_bucket_index]
            .as_ref()
            .map(|bucket| bucket.vector_id(left.vector_index))
            .unwrap_or("");
        let right_id = loaded_buckets[right.selected_bucket_index]
            .as_ref()
            .map(|bucket| bucket.vector_id(right.vector_index))
            .unwrap_or("");
        left_id.cmp(right_id)
    })
}

fn score_bucket_candidates(
    metric: &Metric,
    query_vector: &[f32],
    query_norm: f32,
    metadata_filter: Option<&MetadataFilterExpression>,
    allowed_local_ids: Option<&RoaringBitmap>,
    selected_bucket_index: usize,
    bucket: &AnnBucketData,
    out: &mut Vec<ScoredCandidateRef>,
) {
    let binary_query_signature = bucket.binary_query_signature(query_vector);
    for vector_index in 0..bucket.vector_count() {
        if let Some(allowed_local_ids) = allowed_local_ids {
            if !allowed_local_ids.contains(vector_index as u32) {
                continue;
            }
        }
        if !metadata_matches_filter(bucket.vector_metadata(vector_index), metadata_filter) {
            continue;
        }
        let score = bucket.score_vector(
            metric,
            query_vector,
            query_norm,
            vector_index,
            binary_query_signature.as_deref(),
        );
        out.push(ScoredCandidateRef {
            selected_bucket_index,
            vector_index,
            score,
        });
    }
}

fn exact_top_k_from_visible_vectors(
    vectors: &BTreeMap<String, UpsertVector>,
    metric: &Metric,
    query_vector: &[f32],
    top_k: usize,
    metadata_filter: Option<&MetadataFilterExpression>,
) -> Vec<(UpsertVector, f32)> {
    if top_k == 0 {
        return Vec::new();
    }

    let query_norm = if matches!(metric, Metric::Cosine) {
        l2_norm(query_vector)
    } else {
        0.0
    };
    let mut ranked_ids: Vec<(&str, f32)> = Vec::with_capacity(top_k.saturating_add(1));
    for vector in vectors.values() {
        if !metadata_matches_filter(vector.metadata.as_ref(), metadata_filter) {
            continue;
        }
        let score =
            metric_score_with_cached_query_norm(metric, query_vector, query_norm, &vector.values);
        let vector_id = vector.id.as_str();
        if ranked_ids.len() < top_k {
            ranked_ids.push((vector_id, score));
            if ranked_ids.len() == top_k {
                ranked_ids.sort_by(|(left_id, left_score), (right_id, right_score)| {
                    right_score
                        .total_cmp(left_score)
                        .then_with(|| left_id.cmp(right_id))
                });
            }
            continue;
        }

        let Some((worst_id, worst_score)) = ranked_ids.last() else {
            continue;
        };
        let ordering = score.total_cmp(worst_score);
        if ordering.is_lt() || (ordering == Ordering::Equal && vector_id >= *worst_id) {
            continue;
        }
        ranked_ids.push((vector_id, score));
        ranked_ids.sort_by(|(left_id, left_score), (right_id, right_score)| {
            right_score
                .total_cmp(left_score)
                .then_with(|| left_id.cmp(right_id))
        });
        ranked_ids.pop();
    }

    ranked_ids
        .into_iter()
        .filter_map(|(id, score)| vectors.get(id).map(|vector| (vector.clone(), score)))
        .collect()
}

#[derive(Debug, Clone)]
struct AnnObjectReadBudget {
    configured: usize,
    remaining: usize,
    exceeded: bool,
}

impl AnnObjectReadBudget {
    fn new(configured: usize) -> Self {
        let configured = configured.max(1);
        Self {
            configured,
            remaining: configured,
            exceeded: false,
        }
    }

    fn configured(&self) -> usize {
        self.configured
    }

    fn remaining(&self) -> usize {
        self.remaining
    }

    fn reserve(&mut self, reads: usize) -> bool {
        if reads == 0 {
            return true;
        }
        if reads > self.remaining {
            self.exceeded = true;
            self.remaining = 0;
            return false;
        }
        self.remaining = self.remaining.saturating_sub(reads);
        true
    }
}

fn rerank_cache_key(collection: &str, namespace: &str, generation: u64, vector_id: &str) -> String {
    format!("{collection}::{namespace}::{generation}::{vector_id}")
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn query_namespace_with_ann(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
    metric: &Metric,
    dimension: u32,
    query_vector: &[f32],
    top_k: u32,
    metadata_filter: Option<&MetadataFilterExpression>,
) -> AnnQueryExecution {
    if !state.query_ann_enabled() {
        return AnnQueryExecution::fallback("ann_disabled");
    }

    if let Some(cache_entry) = state.get_namespace_cache_entry(collection, namespace).await {
        if cache_entry.generation == manifest.generation
            && cache_entry.vectors.len() <= ANN_SMALL_CORPUS_VECTOR_THRESHOLD
        {
            telemetry::increment_query_temperature(&state.service_name, "warm");
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
                vectors = cache_entry.vectors.len(),
                "ANN small-corpus warm path — serving from namespace cache"
            );
            let mut execution = AnnQueryExecution::default();
            execution.stats.candidates_scored = cache_entry.vectors.len();
            execution.scored = Some(exact_top_k_from_visible_vectors(
                cache_entry.vectors.as_ref(),
                metric,
                query_vector,
                top_k as usize,
                metadata_filter,
            ));
            return execution;
        }
    }

    let mut execution = AnnQueryExecution::default();
    let mut read_budget = AnnObjectReadBudget::new(state.ann_object_read_budget());
    execution.stats.object_read_budget = read_budget.configured();
    let index_meta = match load_ann_index_meta(
        state,
        collection,
        namespace,
        manifest.generation,
        Some(&mut execution.stats),
        Some(&mut read_budget),
    )
    .await
    {
        Ok(Some(meta)) => meta,
        Ok(None) => match load_or_build_ann_index_meta(
            state, collection, namespace, manifest, metric, dimension,
        )
        .await
        {
            Ok(Some(meta)) => meta,
            Ok(None) => {
                execution.stats.push_fallback_reason("index_unavailable");
                return execution;
            }
            Err(error) => {
                warn!(
                    collection,
                    namespace,
                    error = ?error,
                    "ANN index load/build failed; falling back to exact query"
                );
                execution.stats.push_fallback_reason("index_load_error");
                return execution;
            }
        },
        Err(error) => {
            warn!(
                collection,
                namespace,
                error = ?error,
                "ANN index metadata load failed; falling back to exact query"
            );
            if execution.stats.object_read_budget_exceeded {
                execution
                    .stats
                    .push_fallback_reason(FALLBACK_REASON_OBJECT_READ_BUDGET_EXCEEDED);
            } else {
                execution.stats.push_fallback_reason("index_load_error");
            }
            return execution;
        }
    };

    if index_meta.vector_count < ANN_MIN_VECTORS || index_meta.buckets.is_empty() {
        execution.stats.push_fallback_reason("insufficient_vectors");
        return execution;
    }
    if index_meta.dimension != dimension {
        warn!(
            collection,
            namespace,
            expected_dimension = dimension,
            ann_dimension = index_meta.dimension,
            "ANN index dimension mismatch; falling back to exact query"
        );
        execution.stats.push_fallback_reason("dimension_mismatch");
        return execution;
    }
    if index_meta.metric != *metric {
        warn!(
            collection,
            namespace,
            expected_metric = ?metric,
            ann_metric = ?index_meta.metric,
            "ANN index metric mismatch; falling back to exact query"
        );
        execution.stats.push_fallback_reason("metric_mismatch");
        return execution;
    }

    let top_k = top_k as usize;
    let mut native_filter_plan: Option<NativeFilterPlan> = None;
    let mut native_filter_cluster_estimate = None;
    if metadata_filter.is_some() {
        let Some(filter_index_meta) = index_meta.filter_index.as_ref() else {
            execution
                .stats
                .push_fallback_reason("filter_compile_failure");
            return execution;
        };
        let plan = match compile_native_filter_plan(metadata_filter, filter_index_meta) {
            Ok(Some(plan)) => plan,
            Ok(None) => {
                execution
                    .stats
                    .push_fallback_reason("filter_compile_failure");
                return execution;
            }
            Err(error) => {
                warn!(
                    collection,
                    namespace,
                    error = ?error,
                    "native filter compilation failed; falling back to exact query"
                );
                execution
                    .stats
                    .push_fallback_reason("filter_compile_failure");
                return execution;
            }
        };
        match resolve_native_filter_allowed_buckets(
            state,
            collection,
            namespace,
            manifest.generation,
            &plan,
            &index_meta,
            &mut execution.stats,
            &mut read_budget,
        )
        .await
        {
            Ok(allowed_estimate) => {
                if allowed_estimate.allowed_buckets.is_empty() {
                    execution.scored = Some(Vec::new());
                    return execution;
                }
                native_filter_cluster_estimate = Some(allowed_estimate);
                native_filter_plan = Some(plan);
            }
            Err(error) => {
                warn!(
                    collection,
                    namespace,
                    error = ?error,
                    "native filter cluster summary load failed; falling back to exact query"
                );
                if execution.stats.object_read_budget_exceeded {
                    execution
                        .stats
                        .push_fallback_reason(FALLBACK_REASON_OBJECT_READ_BUDGET_EXCEEDED);
                }
                execution
                    .stats
                    .push_fallback_reason("filter_cluster_summary_load_failure");
                return execution;
            }
        }
    }
    let estimated_filter_matches = native_filter_cluster_estimate
        .as_ref()
        .map(|estimate| estimate.expected_total as usize);
    let budget_has_filter = native_filter_plan.is_some();
    if index_meta.vector_count <= ANN_SMALL_CORPUS_VECTOR_THRESHOLD {
        let namespace_load_started = Instant::now();
        let visible_vectors = match load_namespace_vectors(state, collection, namespace, manifest)
            .await
        {
            Ok(vectors) => vectors,
            Err(error) => {
                warn!(
                    collection,
                    namespace,
                    error = ?error,
                    "small-corpus ANN exact path failed to load vectors; falling back to exact query"
                );
                execution
                    .stats
                    .push_fallback_reason("small_corpus_vector_load_error");
                return execution;
            }
        };
        let load_duration = namespace_load_started.elapsed();
        // Fix temperature label: load_namespace_vectors may have hit the
        // cache (warm) or loaded from storage (cold).  Use the load duration
        // as a heuristic — a sub-millisecond load is almost certainly a
        // cache hit.
        let temperature = if load_duration.as_millis() < 1 { "warm" } else { "cold" };
        telemetry::increment_query_temperature(&state.service_name, temperature);
        info!(
            collection,
            namespace,
            vectors = visible_vectors.len(),
            load_ms = load_duration.as_millis() as u64,
            temperature,
            "ANN small-corpus path — loaded namespace vectors"
        );
        telemetry::record_query_namespace_load_duration(
            &state.service_name,
            load_duration.as_secs_f64(),
        );
        telemetry::increment_query_cache_fill(&state.service_name, "namespace_vectors");
        execution.stats.buckets_probed = index_meta.buckets.len();
        execution.stats.candidates_scored = visible_vectors.len();
        execution.scored = Some(exact_top_k_from_visible_vectors(
            visible_vectors.as_ref(),
            metric,
            query_vector,
            top_k,
            metadata_filter,
        ));
        return execution;
    }

    let planning_bucket_count = native_filter_cluster_estimate
        .as_ref()
        .map(|estimate| estimate.allowed_buckets.len() as usize)
        .unwrap_or(index_meta.buckets.len())
        .max(1);
    let target_candidates = target_candidate_count(
        top_k,
        index_meta.vector_count,
        planning_bucket_count,
        budget_has_filter,
        estimated_filter_matches,
    );
    let target_probes = target_probe_count(
        top_k,
        index_meta.vector_count,
        planning_bucket_count,
        target_candidates,
        budget_has_filter,
        estimated_filter_matches,
    );

    let dimension = dimension as usize;
    let centroid_len_expected = index_meta.buckets.len().saturating_mul(dimension);
    if index_meta.centroids.len() != centroid_len_expected
        || index_meta.centroid_norms.len() != index_meta.buckets.len()
    {
        warn!(
            collection,
            namespace,
            centroid_len = index_meta.centroids.len(),
            centroid_len_expected,
            centroid_norms_len = index_meta.centroid_norms.len(),
            bucket_count = index_meta.buckets.len(),
            "ANN index centroid layout mismatch; falling back to exact query"
        );
        execution
            .stats
            .push_fallback_reason("centroid_layout_mismatch");
        return execution;
    }

    let query_norm = if matches!(metric, Metric::Cosine) {
        l2_norm(query_vector)
    } else {
        0.0
    };
    let rerank_max_candidates = state.ann_rerank_max_candidates();
    let mut tree_rank_by_bucket = BTreeMap::new();
    let mut tree_score_by_bucket = BTreeMap::new();
    if let Some(tree_candidates) = tree_bucket_candidates(
        &index_meta,
        metric,
        query_vector,
        query_norm,
        state.ann_tree_root_beam(),
    ) {
        for (rank, (bucket_index, score)) in tree_candidates.into_iter().enumerate() {
            tree_rank_by_bucket.insert(bucket_index, rank);
            tree_score_by_bucket.insert(bucket_index, score);
        }
    }

    let mut bucket_scores: Vec<(usize, usize, &AnnBucketMeta, f32, usize)> =
        Vec::with_capacity(index_meta.buckets.len());
    for (bucket_index, bucket) in index_meta.buckets.iter().enumerate() {
        let expected_matches = native_filter_cluster_estimate
            .as_ref()
            .and_then(|estimate| {
                estimate
                    .expected_matches
                    .get(&(bucket.bucket_id as u32))
                    .copied()
            })
            .unwrap_or(bucket.vector_count as u32)
            .min(bucket.vector_count as u32) as usize;
        if expected_matches == 0 {
            continue;
        }
        if let Some(allowed_estimate) = native_filter_cluster_estimate.as_ref() {
            if !allowed_estimate
                .allowed_buckets
                .contains(bucket.bucket_id as u32)
            {
                continue;
            }
        }
        let centroid_start = bucket_index.saturating_mul(dimension);
        let centroid_end = centroid_start.saturating_add(dimension);
        let centroid = &index_meta.centroids[centroid_start..centroid_end];
        let centroid_norm = index_meta.centroid_norms[bucket_index];
        let score = tree_score_by_bucket
            .get(&bucket_index)
            .copied()
            .unwrap_or_else(|| {
                metric_score_with_norms_and_squared_euclidean(
                    metric,
                    query_vector,
                    query_norm,
                    centroid,
                    centroid_norm,
                )
            });
        let tree_rank = tree_rank_by_bucket
            .get(&bucket_index)
            .copied()
            .unwrap_or(usize::MAX);
        bucket_scores.push((tree_rank, bucket_index, bucket, score, expected_matches));
    }
    bucket_scores.sort_by(
        |(left_rank, left_index, left_bucket, left_score, _),
         (right_rank, right_index, right_bucket, right_score, _)| {
            left_rank
                .cmp(right_rank)
                .then_with(|| right_score.total_cmp(left_score))
                .then_with(|| left_bucket.bucket_id.cmp(&right_bucket.bucket_id))
                .then_with(|| left_index.cmp(right_index))
        },
    );

    let mut selected = Vec::new();
    let mut estimated_survivors_budget = 0usize;
    let mut selected_count = 0usize;
    let initial_probe_target = target_probes
        .max(state.ann_tree_leaf_probe_count())
        .max(1)
        .min(bucket_scores.len());
    for (_tree_rank, bucket_index, bucket, _, expected_matches) in &bucket_scores {
        selected.push((
            *bucket_index,
            bucket.bucket_id,
            bucket.object_key.clone(),
            bucket.vector_count,
        ));
        selected_count = selected_count.saturating_add(1);
        estimated_survivors_budget = estimated_survivors_budget.saturating_add(*expected_matches);
        if selected_count >= initial_probe_target && estimated_survivors_budget >= target_candidates
        {
            break;
        }
    }

    let mut widening_cursor = selected.len();
    let mut widening_passes = 0usize;
    while estimated_survivors_budget < top_k
        && widening_cursor < bucket_scores.len()
        && widening_passes < state.filter_max_widen_passes()
    {
        widening_passes = widening_passes.saturating_add(1);
        let widen_step = initial_probe_target.max(1);
        let end = widening_cursor
            .saturating_add(widen_step)
            .min(bucket_scores.len());
        for (_tree_rank, bucket_index, bucket, _, expected_matches) in
            bucket_scores[widening_cursor..end].iter()
        {
            selected.push((
                *bucket_index,
                bucket.bucket_id,
                bucket.object_key.clone(),
                bucket.vector_count,
            ));
            estimated_survivors_budget =
                estimated_survivors_budget.saturating_add(*expected_matches);
        }
        widening_cursor = end;
    }
    if widening_passes > 0 {
        execution.stats.widen_passes = widening_passes;
    }
    if estimated_survivors_budget < top_k && widening_cursor < bucket_scores.len() {
        execution
            .stats
            .push_fallback_reason("filter_widen_exhausted");
        return execution;
    }

    if selected.is_empty() {
        execution.scored = Some(Vec::new());
        return execution;
    }

    let mut selected_local_filters: Vec<Option<RoaringBitmap>> = vec![None; selected.len()];
    if let Some(plan) = native_filter_plan.as_ref() {
        for (selected_bucket_index, (_bucket_index, bucket_id, _object_key, bucket_vector_count)) in
            selected.iter().enumerate()
        {
            let allowed_locals = resolve_native_filter_allowed_locals(
                state,
                collection,
                namespace,
                manifest.generation,
                plan,
                *bucket_id,
                *bucket_vector_count,
                &mut execution.stats,
                &mut read_budget,
            )
            .await;
            match allowed_locals {
                Ok(locals) => selected_local_filters[selected_bucket_index] = locals,
                Err(error) => {
                    warn!(
                        collection,
                        namespace,
                        bucket_id = *bucket_id,
                        error = ?error,
                        "native filter row index load failed; falling back to exact query"
                    );
                    if execution.stats.object_read_budget_exceeded {
                        execution
                            .stats
                            .push_fallback_reason(FALLBACK_REASON_OBJECT_READ_BUDGET_EXCEEDED);
                    }
                    execution
                        .stats
                        .push_fallback_reason("filter_row_bitmap_load_failure");
                    return execution;
                }
            }
        }
    }

    let active_selected = selected
        .iter()
        .enumerate()
        .filter(|(selected_bucket_index, _)| {
            !selected_local_filters[*selected_bucket_index]
                .as_ref()
                .is_some_and(RoaringBitmap::is_empty)
        })
        .map(|(selected_bucket_index, _)| selected_bucket_index)
        .collect::<Vec<_>>();
    execution.stats.buckets_probed = active_selected.len();
    if active_selected.is_empty() {
        execution.scored = Some(Vec::new());
        return execution;
    }

    let mut loaded_buckets: Vec<Option<Arc<AnnBucketData>>> = vec![None; selected.len()];
    let mut scored_candidates = Vec::with_capacity(target_candidates.max(top_k));
    let mut load_set = JoinSet::new();
    let scoring_metadata_filter = None;

    for (selected_bucket_index, (_bucket_index, bucket_id, object_key, _bucket_vector_count)) in
        selected.iter().enumerate()
    {
        if !active_selected.contains(&selected_bucket_index) {
            continue;
        }
        if let Some(cached_bucket) = state.get_ann_bucket_cache(object_key).await {
            let shard_scope = crate::distributed::shard_scope_label(namespace);
            telemetry::increment_cache_hits_scoped(
                &state.service_name,
                &state.node_id,
                "ann_bucket",
                &shard_scope,
                1,
            );
            if cached_bucket.bucket_id != *bucket_id {
                execution.stats.ann_fetch_errors =
                    execution.stats.ann_fetch_errors.saturating_add(1);
                execution.stats.push_fallback_reason("bucket_id_mismatch");
                return execution;
            }
            score_bucket_candidates(
                metric,
                query_vector,
                query_norm,
                scoring_metadata_filter,
                selected_local_filters[selected_bucket_index].as_ref(),
                selected_bucket_index,
                cached_bucket.as_ref(),
                &mut scored_candidates,
            );
            loaded_buckets[selected_bucket_index] = Some(cached_bucket);
            continue;
        }
        let shard_scope = crate::distributed::shard_scope_label(namespace);
        telemetry::increment_cache_misses_scoped(
            &state.service_name,
            &state.node_id,
            "ann_bucket",
            &shard_scope,
            1,
        );
        if !read_budget.reserve(1) {
            execution.stats.object_read_budget_exceeded = true;
            execution
                .stats
                .push_fallback_reason(FALLBACK_REASON_OBJECT_READ_BUDGET_EXCEEDED);
            return execution;
        }

        let storage = state.storage.clone();
        let semaphore = state.ann_bucket_fetch_semaphore();
        let object_key = object_key.clone();
        let expected_bucket_id = *bucket_id;
        load_set.spawn(async move {
            let _permit = semaphore.acquire_owned().await.map_err(|_| {
                ApiError::internal("ANN bucket fetch semaphore closed unexpectedly")
            })?;
            let raw = storage
                .get_bytes(&object_key)
                .await
                .map_err(map_store_error)?;
            let bucket = decode_ann_bucket(&raw)?;
            if bucket.bucket_id != expected_bucket_id {
                return Err(ApiError::internal(format!(
                    "ANN bucket id mismatch: expected {}, got {}",
                    expected_bucket_id, bucket.bucket_id
                )));
            }
            Ok::<(usize, String, Arc<AnnBucketData>, usize), ApiError>((
                selected_bucket_index,
                object_key,
                Arc::new(bucket),
                raw.len(),
            ))
        });
    }

    while let Some(joined) = load_set.join_next().await {
        match joined {
            Ok(Ok((selected_bucket_index, object_key, bucket, raw_len))) => {
                execution.stats.ann_bucket_object_reads =
                    execution.stats.ann_bucket_object_reads.saturating_add(1);
                execution.stats.ann_bucket_object_bytes = execution
                    .stats
                    .ann_bucket_object_bytes
                    .saturating_add(raw_len);
                state.put_ann_bucket_cache(object_key, bucket.clone()).await;
                score_bucket_candidates(
                    metric,
                    query_vector,
                    query_norm,
                    scoring_metadata_filter,
                    selected_local_filters[selected_bucket_index].as_ref(),
                    selected_bucket_index,
                    bucket.as_ref(),
                    &mut scored_candidates,
                );
                loaded_buckets[selected_bucket_index] = Some(bucket);
            }
            Ok(Err(error)) => {
                execution.stats.ann_fetch_errors =
                    execution.stats.ann_fetch_errors.saturating_add(1);
                warn!(
                    collection,
                    namespace,
                    error = ?error,
                    "ANN bucket fetch failed; falling back to exact query"
                );
                if execution.stats.object_read_budget_exceeded {
                    execution
                        .stats
                        .push_fallback_reason(FALLBACK_REASON_OBJECT_READ_BUDGET_EXCEEDED);
                }
                execution.stats.push_fallback_reason("bucket_fetch_error");
                return execution;
            }
            Err(error) => {
                execution.stats.ann_fetch_errors =
                    execution.stats.ann_fetch_errors.saturating_add(1);
                warn!(
                    collection,
                    namespace,
                    error = ?error,
                    "ANN bucket fetch task failed; falling back to exact query"
                );
                execution
                    .stats
                    .push_fallback_reason("bucket_fetch_task_error");
                return execution;
            }
        }
    }

    execution.stats.candidates_scored = scored_candidates.len();
    if scored_candidates.is_empty() {
        execution.scored = Some(Vec::new());
        return execution;
    }

    scored_candidates
        .sort_by(|left, right| compare_scored_candidates(left, right, &loaded_buckets));
    apply_quantization_bound_pruning(
        &mut scored_candidates,
        top_k,
        rerank_max_candidates,
        state.ann_rerank_prune_ratio(),
        state.ann_quantization_bound_margin(),
        &mut execution.stats,
    );
    if scored_candidates.is_empty() {
        execution.stats.push_fallback_reason("rerank_no_candidates");
        return execution;
    }
    if scored_candidates.len() > state.ann_rerank_max_candidates() {
        execution
            .stats
            .push_fallback_reason(FALLBACK_REASON_RERANK_CANDIDATE_BUDGET_EXCEEDED);
        return execution;
    }

    let mut rerank_candidate_ids = BTreeSet::new();
    for candidate in &scored_candidates {
        let Some(bucket) = loaded_buckets[candidate.selected_bucket_index].as_ref() else {
            continue;
        };
        let candidate_id = bucket.vector_id(candidate.vector_index);
        rerank_candidate_ids.insert(candidate_id.to_string());
    }
    if rerank_candidate_ids.is_empty() {
        execution.stats.push_fallback_reason("rerank_no_candidates");
        return execution;
    }

    let mut rerank_vectors: BTreeMap<String, UpsertVector> = BTreeMap::new();
    let mut cache_miss_ids = BTreeSet::new();
    for candidate_id in rerank_candidate_ids {
        let cache_key = rerank_cache_key(collection, namespace, manifest.generation, &candidate_id);
        if let Some(vector) = state.get_ann_rerank_ssd_cache(&cache_key).await {
            execution.stats.rerank_ssd_cache_hits =
                execution.stats.rerank_ssd_cache_hits.saturating_add(1);
            let shard_scope = crate::distributed::shard_scope_label(namespace);
            telemetry::increment_cache_hits_scoped(
                &state.service_name,
                &state.node_id,
                "ann_rerank_ssd",
                &shard_scope,
                1,
            );
            rerank_vectors.insert(candidate_id, vector);
        } else {
            execution.stats.rerank_ssd_cache_misses =
                execution.stats.rerank_ssd_cache_misses.saturating_add(1);
            let shard_scope = crate::distributed::shard_scope_label(namespace);
            telemetry::increment_cache_misses_scoped(
                &state.service_name,
                &state.node_id,
                "ann_rerank_ssd",
                &shard_scope,
                1,
            );
            cache_miss_ids.insert(candidate_id);
        }
    }

    if cache_miss_ids.is_empty() {
        telemetry::increment_query_temperature(&state.service_name, "warm");
    } else {
        telemetry::increment_query_temperature(&state.service_name, "cold");
    }

    if !cache_miss_ids.is_empty() {
        if read_budget.remaining() == 0 {
            execution.stats.object_read_budget_exceeded = true;
            execution
                .stats
                .push_fallback_reason(FALLBACK_REASON_OBJECT_READ_BUDGET_EXCEEDED);
            return execution;
        }
        let rerank_fetch_started = Instant::now();
        let fetched = load_namespace_vectors_for_ids_with_stats(
            state,
            collection,
            namespace,
            manifest,
            &cache_miss_ids,
            Some(read_budget.remaining()),
        )
        .await;
        let fetched = match fetched {
            Ok(result) => result,
            Err(error) => {
                execution.stats.object_read_budget_exceeded = true;
                warn!(
                    collection,
                    namespace,
                    error = ?error,
                    "ANN rerank selective vector load failed"
                );
                execution
                    .stats
                    .push_fallback_reason(FALLBACK_REASON_RERANK_FETCH_BUDGET_EXCEEDED);
                return execution;
            }
        };
        if !read_budget.reserve(fetched.stats.segment_reads) {
            execution.stats.object_read_budget_exceeded = true;
            execution
                .stats
                .push_fallback_reason(FALLBACK_REASON_OBJECT_READ_BUDGET_EXCEEDED);
            return execution;
        }
        execution.stats.rerank_segment_object_reads = execution
            .stats
            .rerank_segment_object_reads
            .saturating_add(fetched.stats.segment_reads);
        execution.stats.rerank_segment_object_bytes = execution
            .stats
            .rerank_segment_object_bytes
            .saturating_add(fetched.stats.segment_bytes);
        execution.stats.rerank_ssd_fetch_latency_ms +=
            rerank_fetch_started.elapsed().as_secs_f64() * 1_000.0;
        telemetry::record_query_namespace_load_duration(
            &state.service_name,
            rerank_fetch_started.elapsed().as_secs_f64(),
        );
        telemetry::increment_query_cache_fill(&state.service_name, "ann_rerank_ssd");

        for (vector_id, vector) in fetched.vectors {
            let cache_key =
                rerank_cache_key(collection, namespace, manifest.generation, &vector_id);
            let evictions = state.put_ann_rerank_ssd_cache(cache_key, &vector).await;
            execution.stats.rerank_ssd_cache_evictions = execution
                .stats
                .rerank_ssd_cache_evictions
                .saturating_add(evictions as usize);
            rerank_vectors.insert(vector_id, vector);
        }
    }

    let mut exact_scored = Vec::with_capacity(top_k.min(scored_candidates.len()));
    for candidate in scored_candidates {
        let Some(bucket) = loaded_buckets[candidate.selected_bucket_index].as_ref() else {
            continue;
        };
        let candidate_id = bucket.vector_id(candidate.vector_index);
        let Some(stored) = rerank_vectors.get(candidate_id) else {
            continue;
        };
        exact_scored.push((
            stored.clone(),
            compute_score(metric, query_vector, &stored.values),
        ));
    }

    if exact_scored.is_empty() {
        execution.stats.push_fallback_reason("rerank_no_candidates");
        return execution;
    }

    exact_scored.sort_by(|(left_vector, left_score), (right_vector, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_vector.id.cmp(&right_vector.id))
    });
    exact_scored.truncate(top_k);

    execution.scored = Some(exact_scored);
    execution
}

#[cfg(test)]
mod tests {
    use std::{hint::black_box, time::Instant};

    use serde_json::json;
    use turbo_vector_core::Metric;

    use super::{
        decode_ann_bucket, encode_ann_bucket, hamming_distance_bits, hamming_distance_bits_scalar,
        target_candidate_count, target_probe_count, AnnBucketValues, UpsertVector,
        ANN_BUCKET_VERSION_BINARY,
    };

    #[test]
    fn ann_bucket_quantized_roundtrip_preserves_ids_and_metadata() {
        let vectors = vec![
            UpsertVector {
                id: "doc-a".to_string(),
                values: vec![1.0, 2.0, 3.0],
                metadata: Some(json!({"topic": "a"})),
            },
            UpsertVector {
                id: "doc-b".to_string(),
                values: vec![4.0, 5.0, 6.0],
                metadata: None,
            },
        ];
        let encoded =
            encode_ann_bucket(7, 3, &Metric::Cosine, &vectors).expect("ANN bucket should encode");
        assert_eq!(encoded[4], ANN_BUCKET_VERSION_BINARY);
        let decoded = decode_ann_bucket(&encoded).expect("ANN bucket should decode");

        assert_eq!(decoded.bucket_id, 7);
        assert_eq!(decoded.vector_count(), 2);
        assert_eq!(decoded.vector_id(0), "doc-a");
        assert_eq!(decoded.vector_id(1), "doc-b");
        assert_eq!(decoded.vector_metadata(0), Some(&json!({"topic": "a"})));
        assert_eq!(decoded.vector_metadata(1), None);
        assert!(
            matches!(decoded.values, AnnBucketValues::Binary { .. }),
            "new bucket encoding should materialize a binary payload"
        );
    }

    #[test]
    fn ann_bucket_binary_payload_reaches_16x_compression_vs_f16_values() {
        let dimension = 256_usize;
        let vector_count = 64_usize;
        let mut vectors = Vec::with_capacity(vector_count);
        for index in 0..vector_count {
            let values = (0..dimension)
                .map(|dim| if (index + dim) % 3 == 0 { -1.0 } else { 1.0 })
                .collect::<Vec<_>>();
            vectors.push(UpsertVector {
                id: format!("doc-{index:03}"),
                values,
                metadata: None,
            });
        }
        let encoded =
            encode_ann_bucket(11, dimension, &Metric::Dot, &vectors).expect("binary bucket encode");
        let decoded = decode_ann_bucket(&encoded).expect("binary bucket decode");
        let AnnBucketValues::Binary {
            signatures,
            stride_bytes,
        } = decoded.values;
        assert_eq!(stride_bytes, dimension.div_ceil(8));
        let f16_bytes = vector_count.saturating_mul(dimension).saturating_mul(2);
        let binary_bytes = signatures.len();
        assert!(
            binary_bytes.saturating_mul(16) <= f16_bytes,
            "binary payload should be >=16x smaller than f16 values (binary={binary_bytes}, f16={f16_bytes})"
        );
    }

    #[test]
    fn small_corpus_budget_scans_full_quantized_candidate_set() {
        let small_candidates = target_candidate_count(10, 10_000, 100, false, None);
        let small_probes = target_probe_count(10, 10_000, 100, small_candidates, false, None);
        let medium_candidates = target_candidate_count(10, 50_000, 220, false, None);
        assert!(
            small_candidates == 10_000,
            "small corpus ANN should score the full quantized set"
        );
        assert_eq!(small_probes, 100);
        assert!(medium_candidates < small_candidates);
    }

    fn hamming_distance_bits_baseline(left: &[u8], right: &[u8]) -> u32 {
        let mut distance = 0_u32;
        for (lhs, rhs) in left.iter().zip(right.iter()) {
            let mut delta = *lhs ^ *rhs;
            for _ in 0..8 {
                distance = distance.saturating_add((delta & 1) as u32);
                delta >>= 1;
            }
        }
        distance
    }

    #[test]
    fn ann_binary_kernel_fallback_matches_scalar_reference() {
        let left = vec![0xAA_u8; 512];
        let right = vec![0xF0_u8; 512];
        let scalar = hamming_distance_bits_scalar(&left, &right);
        let baseline = hamming_distance_bits_baseline(&left, &right);
        let kernel = hamming_distance_bits(&left, &right);
        assert_eq!(scalar, baseline);
        assert_eq!(kernel, baseline);
    }

    #[test]
    fn ann_binary_kernel_microbenchmark_popcount_path_is_faster_than_scalar_baseline() {
        let left = (0..2048_u32)
            .map(|value| (value % 251) as u8)
            .collect::<Vec<_>>();
        let right = (0..2048_u32)
            .map(|value| ((value * 7) % 251) as u8)
            .collect::<Vec<_>>();
        let iterations = 8_000_u32;

        let scalar_started = Instant::now();
        let mut scalar_checksum = 0_u32;
        for _ in 0..iterations {
            scalar_checksum = scalar_checksum
                .saturating_add(black_box(hamming_distance_bits_baseline(&left, &right)));
        }
        let scalar_elapsed = scalar_started.elapsed();

        let optimized_started = Instant::now();
        let mut optimized_checksum = 0_u32;
        for _ in 0..iterations {
            optimized_checksum =
                optimized_checksum.saturating_add(black_box(hamming_distance_bits(&left, &right)));
        }
        let optimized_elapsed = optimized_started.elapsed();
        assert_eq!(scalar_checksum, optimized_checksum);

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if std::is_x86_feature_detected!("popcnt") {
            let scalar_ns = scalar_elapsed.as_nanos() as f64;
            let optimized_ns = optimized_elapsed.as_nanos() as f64;
            assert!(
                optimized_ns <= scalar_ns * 0.75,
                "popcount kernel should be >=25% faster than scalar baseline (scalar_ns={scalar_ns:.0}, optimized_ns={optimized_ns:.0})"
            );
        }
    }
}
