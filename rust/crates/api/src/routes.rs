use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use tokio::{task::JoinSet, time::timeout};
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use turbo_vector_core::{Metric, TurboVectorError};
use turbo_vector_manifest::Manifest;
use utoipa::{IntoParams, OpenApi};

use crate::{
    ann::query_namespace_with_ann,
    distributed::{
        self, CollectionShardPlacement, RebalanceShardPlacementRequest,
        RebalanceShardPlacementResponse, ShardAssignmentStrategy, ShardMigrationPhase,
        ShardNodeState, UpdateShardPlacementRequest, SHARD_DEGRADED_REASON_DROPPED,
        SHARD_DEGRADED_REASON_ERROR, SHARD_DEGRADED_REASON_STALE, SHARD_DEGRADED_REASON_TIMEOUT,
    },
    error::{map_store_error, ApiError, ErrorEnvelope},
    filters::{parse_metadata_filter_with_limits, MetadataFilterExpression},
    fts::{
        maxscore::LexicalExecutionStats,
        rank_expr::{parse_rank_expr, ParsedRankExpr},
        runtime::{execute_lexical_query, explain_lexical_query},
    },
    keys::{
        collection_metadata_key, collection_registry_key, collection_shard_placement_key,
        idempotency_object_key, new_operation_id, now_rfc3339, segment_object_key, sha256_hex,
        wal_object_key,
    },
    models::{
        CollectionMetadata, CollectionStatsResponse, CreateCollectionRequest,
        DeleteCollectionResponse, DeleteRequest, DeleteResponse, FetchRequest, FetchResponse,
        FetchVector, IdempotencyRecord, ListCollectionsResponse, OperationApplyStatus,
        OperationStatusResponse, QueryAnnObservability, QueryDistributedObservability, QueryMatch,
        QueryRequest, QueryResponse, QuerySearchStrategy, QueryShardStatus, SegmentFile,
        SegmentKind, UpsertAcceptedResponse, UpsertRequest, UpsertResponse, UpsertVector,
        WalRecord,
    },
    state::{AppState, HealthResponse, RuntimeConfigResponse},
    storage_logic::{
        enqueue_wal_operation, flush_wal_queue, load_collection_metadata, load_current_manifest,
        load_namespace_vectors, load_operation_status, publish_manifest, record_operation_accepted,
    },
    telemetry,
    validation::{
        compute_score, metadata_matches_filter, normalize_delete_ids, normalize_namespace,
        validate_collection_name, validate_query_request, validate_upsert_vectors,
    },
};

#[derive(Debug, Deserialize, IntoParams)]
struct StatsQuery {
    namespace: Option<String>,
}

#[derive(Debug, Deserialize, IntoParams)]
struct ListNamespacesQuery {
    cursor: Option<String>,
    prefix: Option<String>,
    page_size: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct NamespacePath {
    namespace: String,
}

#[derive(Debug, Serialize)]
struct NamespaceSummary {
    id: String,
}

#[derive(Debug, Serialize)]
struct ListNamespacesResponse {
    namespaces: Vec<NamespaceSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NamespaceWriteRequest {
    #[serde(default)]
    upsert_rows: Option<Vec<Value>>,
    #[serde(default)]
    upsert_columns: Option<Value>,
    #[serde(default)]
    patch_rows: Option<Value>,
    #[serde(default)]
    patch_columns: Option<Value>,
    #[serde(default)]
    deletes: Option<Vec<Value>>,
    #[serde(default)]
    upsert_condition: Option<Value>,
    #[serde(default)]
    patch_condition: Option<Value>,
    #[serde(default)]
    delete_condition: Option<Value>,
    #[serde(default)]
    patch_by_filter: Option<Value>,
    #[serde(default)]
    delete_by_filter: Option<Value>,
    #[serde(default)]
    patch_by_filter_allow_partial: bool,
    #[serde(default)]
    delete_by_filter_allow_partial: bool,
    #[serde(default)]
    return_affected_ids: bool,
    #[serde(default)]
    distance_metric: Option<String>,
    #[serde(default)]
    schema: Option<Value>,
    #[serde(default)]
    copy_from_namespace: Option<Value>,
    #[serde(default)]
    encryption: Option<Value>,
    #[serde(default)]
    disable_backpressure: bool,
}

#[derive(Debug, Serialize)]
struct NamespaceWriteResponse {
    status: &'static str,
    rows_affected: u64,
    rows_upserted: u64,
    rows_patched: u64,
    rows_deleted: u64,
    rows_remaining: bool,
    upserted_ids: Vec<String>,
    patched_ids: Vec<String>,
    deleted_ids: Vec<String>,
    billing: Value,
}

#[derive(Debug, Deserialize)]
struct NamespaceQueryOverload {
    overload: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(untagged)]
enum NamespaceLimit {
    Int(u32),
    Object {
        total: u32,
        #[serde(rename = "per")]
        _per: Option<u32>,
    },
}

impl NamespaceLimit {
    fn total(&self) -> u32 {
        match self {
            Self::Int(total) => *total,
            Self::Object { total, .. } => *total,
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
struct NamespaceQueryRequest {
    #[serde(default)]
    rank_by: Option<Value>,
    #[serde(default)]
    filters: Option<Value>,
    #[serde(default)]
    top_k: Option<u32>,
    #[serde(default)]
    limit: Option<NamespaceLimit>,
    #[serde(default)]
    include_attributes: Option<Value>,
    #[serde(default)]
    exclude_attributes: Option<Vec<String>>,
    #[serde(default)]
    aggregate_by: Option<Value>,
    #[serde(default)]
    group_by: Option<Vec<String>>,
    #[serde(default)]
    consistency: Option<Value>,
    #[serde(default)]
    distance_metric: Option<String>,
    #[serde(default)]
    vector_encoding: Option<String>,
}

#[derive(Debug, Deserialize)]
struct NamespaceMultiQueryRequest {
    queries: Vec<NamespaceQueryRequest>,
    #[serde(default)]
    consistency: Option<Value>,
    #[serde(default)]
    vector_encoding: Option<String>,
}

#[derive(Debug, Serialize)]
struct NamespaceQueryResponse {
    rows: Vec<Value>,
    billing: Value,
}

#[derive(Debug, Serialize)]
struct NamespaceMultiQueryResponse {
    results: Vec<NamespaceQueryResponse>,
    billing: Value,
}

enum IncludeAttributesMode {
    None,
    All,
    Only(BTreeSet<String>),
}

pub(crate) fn app_router(state: AppState) -> Router {
    let body_limit = state.runtime.api_body_limit_bytes.max(1);
    Router::new()
        .route("/openapi.json", get(openapi_json))
        .route("/health", get(health))
        .route("/v1/system/runtime", get(runtime_config))
        .route("/v1/namespaces", get(list_namespaces))
        .route(
            "/v1/namespaces/:namespace/metadata",
            get(get_namespace_metadata),
        )
        .route(
            "/v1/namespaces/:namespace/schema",
            get(get_namespace_schema).post(update_namespace_schema),
        )
        .route("/v1/namespaces/:namespace/query", post(query_namespace))
        .route(
            "/v1/namespaces/:namespace/explain_query",
            post(explain_namespace_query),
        )
        .route(
            "/v1/namespaces/:namespace",
            post(write_namespace).delete(delete_namespace),
        )
        .route(
            "/v1/collections",
            get(list_collections).post(create_collection),
        )
        .route(
            "/v1/collections/:name",
            get(get_collection).delete(delete_collection),
        )
        .route("/v1/collections/:name/vectors/upsert", post(upsert_vectors))
        .route(
            "/v1/collections/:name/operations/:operation_id",
            get(get_operation_status),
        )
        .route(
            "/v1/collections/:name/shards/placement",
            get(get_shard_placement).put(update_shard_placement),
        )
        .route(
            "/v1/collections/:name/shards/rebalance",
            post(rebalance_shard_placement),
        )
        .route("/v1/collections/:name/vectors/delete", post(delete_vectors))
        .route("/v1/collections/:name/vectors/fetch", post(fetch_vectors))
        .route("/v1/collections/:name/vectors/query", post(query_vectors))
        .route("/v1/collections/:name/stats", get(collection_stats))
        .merge(crate::observability::observability_router())
        .layer(DefaultBodyLimit::max(body_limit))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(ApiDoc::openapi())
}

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Service health", body = HealthResponse)
    )
)]
async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

#[utoipa::path(
    get,
    path = "/v1/system/runtime",
    responses(
        (status = 200, description = "Runtime config", body = RuntimeConfigResponse)
    )
)]
async fn runtime_config(State(state): State<AppState>) -> impl IntoResponse {
    let mut runtime = state.runtime.clone();
    runtime.filter_cache_metrics = state.filter_cache_metrics_snapshot().await;
    Json(runtime)
}

fn is_non_null_json(value: &Option<Value>) -> bool {
    value.as_ref().is_some_and(|entry| !entry.is_null())
}

fn first_unsupported_namespace_write_field(
    payload: &NamespaceWriteRequest,
) -> Option<&'static str> {
    if is_non_null_json(&payload.patch_rows) {
        return Some("patch_rows");
    }
    if is_non_null_json(&payload.patch_columns) {
        return Some("patch_columns");
    }
    if is_non_null_json(&payload.upsert_condition) {
        return Some("upsert_condition");
    }
    if is_non_null_json(&payload.patch_condition) {
        return Some("patch_condition");
    }
    if is_non_null_json(&payload.delete_condition) {
        return Some("delete_condition");
    }
    if is_non_null_json(&payload.patch_by_filter) {
        return Some("patch_by_filter");
    }
    if payload.patch_by_filter_allow_partial {
        return Some("patch_by_filter_allow_partial");
    }
    if payload.delete_by_filter_allow_partial {
        return Some("delete_by_filter_allow_partial");
    }
    if is_non_null_json(&payload.copy_from_namespace) {
        return Some("copy_from_namespace");
    }
    if is_non_null_json(&payload.encryption) {
        return Some("encryption");
    }
    if payload.disable_backpressure {
        return Some("disable_backpressure");
    }
    None
}

fn parse_namespace_distance_metric(metric: Option<&str>) -> Result<Option<Metric>, ApiError> {
    let Some(metric) = metric else {
        return Ok(None);
    };
    let normalized = metric.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Ok(None);
    }
    match normalized.as_str() {
        "cosine_distance" => Ok(Some(Metric::Cosine)),
        "euclidean_squared" => Ok(Some(Metric::Euclidean)),
        _ => Err(ApiError::invalid_argument(format!(
            "unsupported distance_metric '{metric}'"
        ))),
    }
}

fn parse_namespace_id(raw: &Value) -> Result<String, ApiError> {
    match raw {
        Value::String(value) => {
            let normalized = value.trim();
            if normalized.is_empty() {
                return Err(ApiError::invalid_argument("id must not be empty"));
            }
            Ok(normalized.to_string())
        }
        Value::Number(number) => Ok(number.to_string()),
        _ => Err(ApiError::invalid_argument(
            "id must be a string or integer value",
        )),
    }
}

fn parse_namespace_vector(raw: &Value) -> Result<Vec<f32>, ApiError> {
    let values = raw
        .as_array()
        .ok_or_else(|| ApiError::invalid_argument("vector must be an array of numbers"))?;
    if values.is_empty() {
        return Err(ApiError::invalid_argument(
            "vector must contain at least one value",
        ));
    }
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let number = value
            .as_f64()
            .ok_or_else(|| ApiError::invalid_argument("vector must contain only numbers"))?;
        if !number.is_finite() {
            return Err(ApiError::invalid_argument(
                "vector must contain only finite numbers",
            ));
        }
        out.push(number as f32);
    }
    Ok(out)
}

fn parse_namespace_upsert_rows(rows: &[Value]) -> Result<Vec<UpsertVector>, ApiError> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let row_object = row
            .as_object()
            .ok_or_else(|| ApiError::invalid_argument("upsert_rows entries must be objects"))?;
        let id = parse_namespace_id(
            row_object
                .get("id")
                .ok_or_else(|| ApiError::invalid_argument("upsert_rows entry missing 'id'"))?,
        )?;
        let vector = parse_namespace_vector(
            row_object
                .get("vector")
                .or_else(|| row_object.get("values"))
                .ok_or_else(|| ApiError::invalid_argument("upsert_rows entry missing 'vector'"))?,
        )?;
        let mut metadata = Map::new();
        for (key, value) in row_object {
            if key == "id" || key == "vector" || key == "values" {
                continue;
            }
            metadata.insert(key.clone(), value.clone());
        }
        out.push(UpsertVector {
            id,
            values: vector,
            metadata: if metadata.is_empty() {
                None
            } else {
                Some(Value::Object(metadata))
            },
        });
    }
    Ok(out)
}

fn parse_namespace_upsert_columns(columns: &Value) -> Result<Vec<UpsertVector>, ApiError> {
    let columns = columns
        .as_object()
        .ok_or_else(|| ApiError::invalid_argument("upsert_columns must be an object"))?;
    let id_column = columns
        .get("id")
        .ok_or_else(|| ApiError::invalid_argument("upsert_columns missing 'id' column"))?
        .as_array()
        .ok_or_else(|| ApiError::invalid_argument("upsert_columns.id must be an array"))?;
    let vector_column = columns
        .get("vector")
        .or_else(|| columns.get("values"))
        .ok_or_else(|| ApiError::invalid_argument("upsert_columns missing 'vector' column"))?
        .as_array()
        .ok_or_else(|| ApiError::invalid_argument("upsert_columns.vector must be an array"))?;

    if id_column.len() != vector_column.len() {
        return Err(ApiError::invalid_argument(
            "upsert_columns.id and upsert_columns.vector length mismatch",
        ));
    }

    let ids: Result<Vec<_>, _> = id_column.iter().map(parse_namespace_id).collect();
    let ids = ids?;
    let mut row_metadata: Vec<Map<String, Value>> = vec![Map::new(); ids.len()];

    for (column_name, column_values) in columns {
        if column_name == "id" || column_name == "vector" || column_name == "values" {
            continue;
        }
        let values = column_values.as_array().ok_or_else(|| {
            ApiError::invalid_argument(format!("upsert_columns.{column_name} must be an array"))
        })?;
        if values.len() != ids.len() {
            return Err(ApiError::invalid_argument(format!(
                "upsert_columns.{column_name} length mismatch with id column"
            )));
        }
        for (index, value) in values.iter().enumerate() {
            row_metadata[index].insert(column_name.clone(), value.clone());
        }
    }

    let mut out = Vec::with_capacity(ids.len());
    for (index, id) in ids.into_iter().enumerate() {
        let values = parse_namespace_vector(&vector_column[index])?;
        out.push(UpsertVector {
            id,
            values,
            metadata: if row_metadata[index].is_empty() {
                None
            } else {
                Some(Value::Object(std::mem::take(&mut row_metadata[index])))
            },
        });
    }
    Ok(out)
}

fn ensure_unique_upsert_ids(vectors: &[UpsertVector]) -> Result<(), ApiError> {
    let mut seen = BTreeSet::new();
    for vector in vectors {
        if !seen.insert(vector.id.clone()) {
            return Err(ApiError::invalid_argument(format!(
                "duplicate id '{}' in write request",
                vector.id
            )));
        }
    }
    Ok(())
}

fn parse_namespace_delete_ids(ids: Option<Vec<Value>>) -> Result<Vec<String>, ApiError> {
    let Some(ids) = ids else {
        return Ok(Vec::new());
    };
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut normalized = Vec::with_capacity(ids.len());
    for id in ids {
        normalized.push(parse_namespace_id(&id)?);
    }
    normalize_delete_ids(Some(normalized))
}

fn normalize_namespace_schema(schema: Option<Value>) -> Result<Option<Value>, ApiError> {
    match schema {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(_)) => Ok(schema),
        Some(_) => Err(ApiError::invalid_argument("schema must be a JSON object")),
    }
}

fn parse_namespace_top_k(
    top_k: Option<u32>,
    limit: Option<NamespaceLimit>,
) -> Result<u32, ApiError> {
    let resolved = top_k
        .or_else(|| limit.map(|entry| entry.total()))
        .unwrap_or(10);
    if resolved == 0 {
        return Err(ApiError::invalid_argument("top_k must be > 0"));
    }
    if resolved > 10_000 {
        return Err(ApiError::invalid_argument("top_k must be <= 10000"));
    }
    Ok(resolved)
}

fn parse_include_attributes(
    include_attributes: Option<Value>,
) -> Result<IncludeAttributesMode, ApiError> {
    match include_attributes {
        None | Some(Value::Null) => Ok(IncludeAttributesMode::All),
        Some(Value::Bool(false)) => Ok(IncludeAttributesMode::None),
        Some(Value::Bool(true)) => Ok(IncludeAttributesMode::All),
        Some(Value::Array(attributes)) => {
            let mut out = BTreeSet::new();
            for attribute in attributes {
                let key = attribute.as_str().ok_or_else(|| {
                    ApiError::invalid_argument("include_attributes must be a bool or string array")
                })?;
                out.insert(key.to_string());
            }
            Ok(IncludeAttributesMode::Only(out))
        }
        Some(_) => Err(ApiError::invalid_argument(
            "include_attributes must be a bool or string array",
        )),
    }
}

fn include_attribute(
    mode: &IncludeAttributesMode,
    excluded: &BTreeSet<String>,
    attribute: &str,
) -> bool {
    if excluded.contains(attribute) {
        return false;
    }
    match mode {
        IncludeAttributesMode::None => false,
        IncludeAttributesMode::All => true,
        IncludeAttributesMode::Only(allowed) => allowed.contains(attribute),
    }
}

fn build_namespace_rows(
    matches: Vec<QueryMatch>,
    include_attributes: &IncludeAttributesMode,
    excluded_attributes: &BTreeSet<String>,
) -> Vec<Value> {
    let mut rows = Vec::with_capacity(matches.len());
    for entry in matches {
        let QueryMatch {
            id,
            score,
            metadata,
            values,
        } = entry;
        let mut row = Map::new();
        row.insert("id".to_string(), Value::String(id));

        if include_attribute(include_attributes, excluded_attributes, "vector") {
            if let Some(values) = values {
                row.insert("vector".to_string(), json!(values));
            }
        }

        if let Some(Value::Object(metadata)) = metadata {
            for (key, value) in metadata {
                if include_attribute(include_attributes, excluded_attributes, &key) {
                    row.insert(key, value);
                }
            }
        }

        row.insert("$dist".to_string(), json!(-score));
        rows.push(Value::Object(row));
    }
    rows
}

fn lexical_execution_billing(stats: LexicalExecutionStats) -> Value {
    json!({
        "lexical_execution": {
            "header_reads": stats.header_reads,
            "blocks_decoded": stats.blocks_decoded,
            "blocks_scanned": stats.blocks_scanned,
            "blocks_skipped": stats.blocks_skipped,
            "docs_scored": stats.docs_scored,
            "threshold_updates": stats.threshold_updates,
        }
    })
}

async fn persist_collection_metadata(
    state: &AppState,
    metadata: &CollectionMetadata,
) -> Result<(), ApiError> {
    let metadata_key = collection_metadata_key(&metadata.name);
    let bytes = serde_json::to_vec(metadata)
        .map_err(|e| ApiError::internal(format!("failed to serialize collection metadata: {e}")))?;
    state
        .storage
        .put_bytes(&metadata_key, &bytes)
        .await
        .map_err(map_store_error)?;
    state
        .set_cached_collection_metadata(&metadata.name, metadata.clone())
        .await;
    ensure_collection_registry_marker(state, &metadata.name).await?;
    Ok(())
}

async fn ensure_collection_registry_marker(
    state: &AppState,
    collection: &str,
) -> Result<(), ApiError> {
    if state.collection_registry_marker_ensured(collection).await {
        return Ok(());
    }
    let registry_key = collection_registry_key(collection);
    state
        .storage
        .put_bytes_if_absent(&registry_key, b"{}")
        .await
        .map_err(map_store_error)?;
    state
        .mark_collection_registry_marker_ensured(collection)
        .await;
    Ok(())
}

async fn load_collection_shard_placement(
    state: &AppState,
    collection: &str,
) -> Result<CollectionShardPlacement, ApiError> {
    if let Some(cached) = state.get_cached_shard_placement(collection).await {
        return Ok(cached);
    }

    let placement_key = collection_shard_placement_key(collection);
    let placement = match state.storage.get_bytes(&placement_key).await {
        Ok(raw) => serde_json::from_slice::<CollectionShardPlacement>(&raw).map_err(|error| {
            ApiError::internal(format!(
                "failed to parse shard placement metadata for collection '{collection}': {error}"
            ))
        })?,
        Err(TurboVectorError::NotFound(_)) => distributed::default_shard_placement(
            collection,
            state.distributed_shard_count(),
            &state.node_id,
            &state.node_id,
        ),
        Err(error) => return Err(map_store_error(error)),
    };
    if let Err(error) = distributed::validate_placement(&placement) {
        return Err(ApiError::internal(format!(
            "invalid shard placement metadata for collection '{collection}': {error}"
        )));
    }
    state
        .set_cached_shard_placement(collection, placement.clone())
        .await;
    Ok(placement)
}

async fn persist_collection_shard_placement(
    state: &AppState,
    collection: &str,
    placement: &CollectionShardPlacement,
) -> Result<(), ApiError> {
    if let Err(error) = distributed::validate_placement(placement) {
        return Err(ApiError::invalid_argument(format!(
            "invalid shard placement: {error}"
        )));
    }
    let placement_key = collection_shard_placement_key(collection);
    let payload = serde_json::to_vec(placement).map_err(|error| {
        ApiError::internal(format!(
            "failed to encode shard placement metadata for collection '{collection}': {error}"
        ))
    })?;
    state
        .storage
        .put_bytes(&placement_key, &payload)
        .await
        .map_err(map_store_error)?;
    state
        .set_cached_shard_placement(collection, placement.clone())
        .await;
    Ok(())
}

async fn ensure_namespace_collection(
    state: &AppState,
    namespace: &str,
    upserts: &[UpsertVector],
    metric_hint: Option<Metric>,
    schema_hint: Option<Value>,
) -> Result<Option<CollectionMetadata>, ApiError> {
    match load_collection_metadata(state, namespace).await {
        Ok(mut metadata) => {
            if let Some(metric_hint) = metric_hint {
                if metadata.metric != metric_hint {
                    return Err(ApiError::invalid_argument(format!(
                        "distance_metric does not match existing namespace metric '{:?}'",
                        metadata.metric
                    )));
                }
            }
            if let Some(schema_hint) = schema_hint {
                metadata.metadata_schema = Some(schema_hint);
                persist_collection_metadata(state, &metadata).await?;
            }
            Ok(Some(metadata))
        }
        Err(error) if error.status == StatusCode::NOT_FOUND => {
            if upserts.is_empty() {
                return Ok(None);
            }
            let dimension = upserts
                .first()
                .map(|vector| vector.values.len() as u32)
                .unwrap_or(0);
            if dimension == 0 {
                return Err(ApiError::invalid_argument(
                    "upsert vectors must contain at least one dimension",
                ));
            }
            let create_request = CreateCollectionRequest {
                name: namespace.to_string(),
                dimension,
                metric: metric_hint.clone(),
                metadata_schema: schema_hint,
            };
            match create_collection(State(state.clone()), Json(create_request)).await {
                Ok((_, Json(collection))) => Ok(Some(collection)),
                Err(create_error) if create_error.status == StatusCode::CONFLICT => {
                    let metadata = load_collection_metadata(state, namespace).await?;
                    if let Some(metric_hint) = metric_hint {
                        if metadata.metric != metric_hint {
                            return Err(ApiError::invalid_argument(format!(
                                "distance_metric does not match existing namespace metric '{:?}'",
                                metadata.metric
                            )));
                        }
                    }
                    Ok(Some(metadata))
                }
                Err(create_error) => Err(create_error),
            }
        }
        Err(error) => Err(error),
    }
}

async fn run_namespace_query(
    state: &AppState,
    namespace: &str,
    request: NamespaceQueryRequest,
) -> Result<NamespaceQueryResponse, ApiError> {
    let collection = load_collection_metadata(state, namespace).await?;
    let NamespaceQueryRequest {
        rank_by,
        filters,
        top_k: requested_top_k,
        limit,
        include_attributes,
        exclude_attributes,
        aggregate_by,
        group_by,
        consistency: _consistency,
        distance_metric,
        vector_encoding,
    } = request;

    if aggregate_by.is_some() {
        return Err(ApiError::invalid_argument("aggregate_by is not supported"));
    }
    if group_by.as_ref().is_some_and(|groups| !groups.is_empty()) {
        return Err(ApiError::invalid_argument("group_by is not supported"));
    }
    if let Some(vector_encoding) = vector_encoding.as_deref() {
        let vector_encoding = vector_encoding.trim().to_ascii_lowercase();
        if vector_encoding != "float" {
            return Err(ApiError::invalid_argument(
                "only vector_encoding='float' is supported",
            ));
        }
    }
    if let Some(metric) = parse_namespace_distance_metric(distance_metric.as_deref())? {
        if metric != collection.metric {
            return Err(ApiError::invalid_argument(format!(
                "distance_metric does not match namespace metric '{:?}'",
                collection.metric
            )));
        }
    }

    let top_k = parse_namespace_top_k(requested_top_k, limit)?;
    let rank_by = rank_by.ok_or_else(|| ApiError::invalid_argument("rank_by is required"))?;
    let parsed_rank_expr = parse_rank_expr(&rank_by)?;
    let include_attributes = parse_include_attributes(include_attributes)?;
    let excluded_attributes = exclude_attributes
        .unwrap_or_default()
        .into_iter()
        .collect::<BTreeSet<_>>();

    match parsed_rank_expr {
        ParsedRankExpr::Vector(vector_rank) => {
            let Json(query_response) = query_vectors(
                State(state.clone()),
                Path(namespace.to_string()),
                Json(QueryRequest {
                    vector: vector_rank.vector,
                    top_k,
                    namespace: Some("default".to_string()),
                    include_metadata: true,
                    include_values: true,
                    filter: filters,
                    search_strategy: vector_rank.strategy,
                }),
            )
            .await?;
            Ok(NamespaceQueryResponse {
                rows: build_namespace_rows(
                    query_response.matches,
                    &include_attributes,
                    &excluded_attributes,
                ),
                billing: json!({}),
            })
        }
        ParsedRankExpr::Lexical(rank_expr) => {
            let metadata_filter =
                parse_metadata_filter_with_limits(filters, state.filter_parser_limits())?;
            let execution = match load_current_manifest(state, namespace).await? {
                Some(manifest) => {
                    execute_lexical_query(
                        state,
                        namespace,
                        "default",
                        &manifest,
                        &rank_expr,
                        collection.metadata_schema.as_ref(),
                        top_k,
                        metadata_filter.as_ref(),
                    )
                    .await?
                }
                None => crate::fts::runtime::LexicalQueryExecution {
                    scored: Vec::new(),
                    stats: LexicalExecutionStats::default(),
                },
            };
            let matches = build_query_matches(execution.scored, top_k, true, true);
            Ok(NamespaceQueryResponse {
                rows: build_namespace_rows(matches, &include_attributes, &excluded_attributes),
                billing: lexical_execution_billing(execution.stats),
            })
        }
    }
}

async fn explain_namespace_query(
    State(state): State<AppState>,
    Path(NamespacePath { namespace }): Path<NamespacePath>,
    Json(request): Json<NamespaceQueryRequest>,
) -> Result<Json<Value>, ApiError> {
    if !state.runtime.fts_explain_query_enabled {
        return Err(ApiError::not_found(
            "explain_query endpoint is disabled by runtime config",
        ));
    }
    validate_collection_name(&namespace)?;
    let collection = load_collection_metadata(&state, &namespace).await?;

    if request.aggregate_by.is_some() {
        return Err(ApiError::invalid_argument("aggregate_by is not supported"));
    }
    if request
        .group_by
        .as_ref()
        .is_some_and(|groups| !groups.is_empty())
    {
        return Err(ApiError::invalid_argument("group_by is not supported"));
    }

    let top_k = parse_namespace_top_k(request.top_k, request.limit)?;
    if top_k as usize > state.runtime.fts_explain_max_top_k {
        return Err(ApiError::invalid_argument(format!(
            "top_k must be <= {} for explain_query",
            state.runtime.fts_explain_max_top_k
        )));
    }
    let rank_by = request
        .rank_by
        .ok_or_else(|| ApiError::invalid_argument("rank_by is required"))?;
    let parsed_rank_expr = parse_rank_expr(&rank_by)?;
    let ParsedRankExpr::Lexical(rank_expr) = parsed_rank_expr else {
        return Err(ApiError::invalid_argument(
            "explain_query only supports lexical rank expressions",
        ));
    };
    let metadata_filter =
        parse_metadata_filter_with_limits(request.filters, state.filter_parser_limits())?;
    let explain = match load_current_manifest(&state, &namespace).await? {
        Some(manifest) => {
            explain_lexical_query(
                &state,
                &namespace,
                "default",
                &manifest,
                &rank_expr,
                collection.metadata_schema.as_ref(),
                top_k,
                metadata_filter.as_ref(),
            )
            .await?
        }
        None => crate::fts::runtime::LexicalExplainOutput {
            plan: crate::fts::runtime::LexicalExplainPlan {
                selected_terms: Vec::new(),
                conditional_boosts: Vec::new(),
            },
            execution: crate::fts::runtime::LexicalExplainExecution {
                candidate_block_count: 0,
                blocks_decoded: 0,
                blocks_skipped: 0,
                docs_scored: 0,
                final_score_decomposition: Vec::new(),
            },
        },
    };
    let payload = serde_json::to_value(explain).map_err(|error| {
        ApiError::internal(format!("failed to encode explain_query response: {error}"))
    })?;
    Ok(Json(payload))
}

async fn list_namespaces(
    State(state): State<AppState>,
    Query(query): Query<ListNamespacesQuery>,
) -> Result<Json<ListNamespacesResponse>, ApiError> {
    let page_size = query.page_size.unwrap_or(100);
    if page_size == 0 {
        return Err(ApiError::invalid_argument("page_size must be > 0"));
    }
    let page_size = page_size.min(1000) as usize;

    let Json(collections_response) = list_collections(State(state)).await?;
    let mut namespace_ids = collections_response
        .collections
        .into_iter()
        .map(|collection| collection.name)
        .collect::<Vec<_>>();
    namespace_ids.sort();

    if let Some(prefix) = query.prefix.as_deref() {
        namespace_ids.retain(|namespace| namespace.starts_with(prefix));
    }
    if let Some(cursor) = query.cursor.as_deref() {
        namespace_ids.retain(|namespace| namespace.as_str() > cursor);
    }

    let has_more = namespace_ids.len() > page_size;
    let namespace_ids = namespace_ids
        .into_iter()
        .take(page_size)
        .collect::<Vec<_>>();
    let next_cursor = if has_more {
        namespace_ids.last().cloned()
    } else {
        None
    };

    Ok(Json(ListNamespacesResponse {
        namespaces: namespace_ids
            .into_iter()
            .map(|id| NamespaceSummary { id })
            .collect(),
        next_cursor,
    }))
}

async fn get_namespace_schema(
    State(state): State<AppState>,
    Path(NamespacePath { namespace }): Path<NamespacePath>,
) -> Result<Json<Value>, ApiError> {
    validate_collection_name(&namespace)?;
    let metadata = load_collection_metadata(&state, &namespace).await?;
    Ok(Json(
        metadata
            .metadata_schema
            .unwrap_or_else(|| Value::Object(Map::new())),
    ))
}

async fn update_namespace_schema(
    State(state): State<AppState>,
    Path(NamespacePath { namespace }): Path<NamespacePath>,
    Json(schema): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    validate_collection_name(&namespace)?;
    if !schema.is_object() {
        return Err(ApiError::invalid_argument("schema must be a JSON object"));
    }

    let mut metadata = load_collection_metadata(&state, &namespace).await?;
    metadata.metadata_schema = Some(schema.clone());
    persist_collection_metadata(&state, &metadata).await?;
    Ok(Json(schema))
}

async fn get_namespace_metadata(
    State(state): State<AppState>,
    Path(NamespacePath { namespace }): Path<NamespacePath>,
) -> Result<Json<Value>, ApiError> {
    validate_collection_name(&namespace)?;
    let metadata = load_collection_metadata(&state, &namespace).await?;
    let Json(stats) = collection_stats(
        State(state),
        Path(namespace),
        Query(StatsQuery {
            namespace: Some("default".to_string()),
        }),
    )
    .await?;
    let approx_logical_bytes = stats
        .vector_count
        .saturating_mul(metadata.dimension as u64)
        .saturating_mul(4);

    Ok(Json(json!({
        "schema": metadata.metadata_schema.unwrap_or_else(|| Value::Object(Map::new())),
        "approx_logical_bytes": approx_logical_bytes,
        "approx_row_count": stats.vector_count,
        "created_at": metadata.created_at,
        "updated_at": metadata.created_at,
        "encryption": { "sse": true },
        "index": {
            "status": "up-to-date",
            "unindexed_bytes": 0
        }
    })))
}

async fn delete_namespace(
    State(state): State<AppState>,
    Path(NamespacePath { namespace }): Path<NamespacePath>,
) -> Result<Json<Value>, ApiError> {
    validate_collection_name(&namespace)?;
    match delete_collection(State(state), Path(namespace)).await {
        Ok(_) => Ok(Json(json!({ "status": "OK" }))),
        Err(error) if error.status == StatusCode::NOT_FOUND => Ok(Json(json!({ "status": "OK" }))),
        Err(error) => Err(error),
    }
}

async fn write_namespace(
    State(state): State<AppState>,
    Path(NamespacePath { namespace }): Path<NamespacePath>,
    Json(payload): Json<NamespaceWriteRequest>,
) -> Result<Json<NamespaceWriteResponse>, ApiError> {
    validate_collection_name(&namespace)?;

    if let Some(field) = first_unsupported_namespace_write_field(&payload) {
        return Err(ApiError::invalid_argument(format!(
            "{field} is not supported by this API"
        )));
    }

    let distance_metric = parse_namespace_distance_metric(payload.distance_metric.as_deref())?;
    let schema = normalize_namespace_schema(payload.schema)?;

    let mut upserts = Vec::new();
    if let Some(rows) = payload.upsert_rows.as_ref() {
        upserts.extend(parse_namespace_upsert_rows(rows)?);
    }
    if let Some(columns) = payload.upsert_columns.as_ref() {
        if !columns.is_null() {
            upserts.extend(parse_namespace_upsert_columns(columns)?);
        }
    }
    ensure_unique_upsert_ids(&upserts)?;

    let delete_ids = parse_namespace_delete_ids(payload.deletes)?;
    let delete_filter = match payload.delete_by_filter {
        Some(Value::Null) | None => None,
        Some(filter) => Some(filter),
    };

    if upserts.is_empty() && delete_ids.is_empty() && delete_filter.is_none() && schema.is_none() {
        return Err(ApiError::invalid_argument(
            "write request must include at least one operation",
        ));
    }

    let collection_exists =
        ensure_namespace_collection(&state, &namespace, &upserts, distance_metric, schema).await?;

    if collection_exists.is_none() {
        return Ok(Json(NamespaceWriteResponse {
            status: "OK",
            rows_affected: 0,
            rows_upserted: 0,
            rows_patched: 0,
            rows_deleted: 0,
            rows_remaining: false,
            upserted_ids: Vec::new(),
            patched_ids: Vec::new(),
            deleted_ids: Vec::new(),
            billing: json!({}),
        }));
    }

    let mut rows_deleted = 0_u64;
    let mut deleted_ids = Vec::new();
    if !delete_ids.is_empty() || delete_filter.is_some() {
        let Json(delete_response) = delete_vectors(
            State(state.clone()),
            Path(namespace.clone()),
            Json(DeleteRequest {
                ids: if delete_ids.is_empty() {
                    None
                } else {
                    Some(delete_ids.clone())
                },
                filter: delete_filter,
                namespace: Some("default".to_string()),
                delete_all: false,
            }),
        )
        .await?;
        rows_deleted = delete_response.deleted_count;
        if payload.return_affected_ids {
            deleted_ids = delete_ids;
        }
    }

    let mut rows_upserted = 0_u64;
    let mut upserted_ids = Vec::new();
    if !upserts.is_empty() {
        let Json(upsert_response) = upsert_vectors(
            State(state.clone()),
            Path(namespace.clone()),
            HeaderMap::new(),
            Json(UpsertRequest {
                vectors: upserts.clone(),
                namespace: Some("default".to_string()),
            }),
        )
        .await?;
        if let UpsertResponse::Accepted(_) = upsert_response {
            // Compatibility write contract expects newly written rows to be queryable
            // immediately from the same namespace request flow.
            flush_wal_queue(&state, &namespace, None).await?;
        }
        rows_upserted = upserts.len() as u64;
        if payload.return_affected_ids {
            upserted_ids = upserts.into_iter().map(|vector| vector.id).collect();
        }
    }

    Ok(Json(NamespaceWriteResponse {
        status: "OK",
        rows_affected: rows_deleted.saturating_add(rows_upserted),
        rows_upserted,
        rows_patched: 0,
        rows_deleted,
        rows_remaining: false,
        upserted_ids,
        patched_ids: Vec::new(),
        deleted_ids,
        billing: json!({}),
    }))
}

async fn query_namespace(
    State(state): State<AppState>,
    Path(NamespacePath { namespace }): Path<NamespacePath>,
    Query(overload): Query<NamespaceQueryOverload>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    validate_collection_name(&namespace)?;
    if overload
        .overload
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("multiQuery"))
    {
        let mut request: NamespaceMultiQueryRequest = serde_json::from_value(payload)
            .map_err(|e| ApiError::invalid_argument(format!("invalid multiQuery payload: {e}")))?;
        if request.queries.len() > 16 {
            return Err(ApiError::invalid_argument(
                "multiQuery supports at most 16 queries",
            ));
        }

        let inherited_vector_encoding = request.vector_encoding.clone();
        let inherited_consistency = request.consistency.clone();
        let mut results = Vec::with_capacity(request.queries.len());
        for query in &mut request.queries {
            if query.vector_encoding.is_none() {
                query.vector_encoding = inherited_vector_encoding.clone();
            }
            if query.consistency.is_none() {
                query.consistency = inherited_consistency.clone();
            }
            results.push(run_namespace_query(&state, &namespace, query.clone()).await?);
        }
        return Ok(Json(
            serde_json::to_value(NamespaceMultiQueryResponse {
                results,
                billing: json!({}),
            })
            .map_err(|e| {
                ApiError::internal(format!("failed to serialize multiQuery response: {e}"))
            })?,
        ));
    }

    let request: NamespaceQueryRequest = serde_json::from_value(payload)
        .map_err(|e| ApiError::invalid_argument(format!("invalid query payload: {e}")))?;
    let response = run_namespace_query(&state, &namespace, request).await?;
    Ok(Json(serde_json::to_value(response).map_err(|e| {
        ApiError::internal(format!("failed to serialize query response: {e}"))
    })?))
}

#[utoipa::path(
    get,
    path = "/v1/collections",
    responses(
        (status = 200, description = "Collections list", body = ListCollectionsResponse),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn list_collections(
    State(state): State<AppState>,
) -> Result<Json<ListCollectionsResponse>, ApiError> {
    let mut collections = Vec::new();
    let keys = state
        .storage
        .list_prefix("collections/")
        .await
        .map_err(map_store_error)?;

    for key in keys {
        if !key.ends_with("/metadata.json") {
            continue;
        }
        let raw = state
            .storage
            .get_bytes(&key)
            .await
            .map_err(map_store_error)?;
        let parsed: CollectionMetadata = serde_json::from_slice(&raw)
            .map_err(|e| ApiError::internal(format!("failed to parse collection metadata: {e}")))?;
        collections.push(parsed);
    }

    collections.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(Json(ListCollectionsResponse { collections }))
}

#[utoipa::path(
    post,
    path = "/v1/collections",
    request_body = CreateCollectionRequest,
    responses(
        (status = 201, description = "Collection created", body = CollectionMetadata),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 409, description = "Collection already exists", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn create_collection(
    State(state): State<AppState>,
    Json(payload): Json<CreateCollectionRequest>,
) -> Result<(StatusCode, Json<CollectionMetadata>), ApiError> {
    validate_collection_name(&payload.name)?;
    if payload.dimension == 0 {
        return Err(ApiError::invalid_argument("dimension must be > 0"));
    }
    if let Some(schema) = payload.metadata_schema.as_ref() {
        if !schema.is_object() {
            return Err(ApiError::invalid_argument(
                "metadata_schema must be a JSON object",
            ));
        }
    }

    let metadata_key = collection_metadata_key(&payload.name);
    match state.storage.get_bytes(&metadata_key).await {
        Ok(_) => {
            return Err(ApiError::conflict(format!(
                "collection '{}' already exists",
                payload.name
            )));
        }
        Err(TurboVectorError::NotFound(_)) => {}
        Err(e) => return Err(map_store_error(e)),
    }

    let collection = CollectionMetadata {
        name: payload.name,
        dimension: payload.dimension,
        metric: payload.metric.unwrap_or(Metric::Cosine),
        created_at: now_rfc3339(),
        metadata_schema: payload.metadata_schema,
    };

    persist_collection_metadata(&state, &collection).await?;

    Ok((StatusCode::CREATED, Json(collection)))
}

#[utoipa::path(
    get,
    path = "/v1/collections/{name}",
    params(
        ("name" = String, Path, description = "Collection name")
    ),
    responses(
        (status = 200, description = "Collection metadata", body = CollectionMetadata),
        (status = 400, description = "Invalid collection name", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn get_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<CollectionMetadata>, ApiError> {
    validate_collection_name(&name)?;
    let metadata = load_collection_metadata(&state, &name).await?;
    Ok(Json(metadata))
}

#[utoipa::path(
    delete,
    path = "/v1/collections/{name}",
    params(
        ("name" = String, Path, description = "Collection name")
    ),
    responses(
        (status = 200, description = "Collection deleted", body = DeleteCollectionResponse),
        (status = 400, description = "Invalid collection name", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn delete_collection(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<DeleteCollectionResponse>, ApiError> {
    validate_collection_name(&name)?;
    let _collection_guard = state.lock_collection_manifest(&name).await;

    let metadata_key = collection_metadata_key(&name);
    match state.storage.get_bytes(&metadata_key).await {
        Ok(_) => {}
        Err(TurboVectorError::NotFound(_)) => {
            return Err(ApiError::not_found(format!(
                "collection '{}' not found",
                name
            )));
        }
        Err(e) => return Err(map_store_error(e)),
    }

    let collection_prefix = format!("collections/{name}/");
    let keys = state
        .storage
        .list_prefix(&collection_prefix)
        .await
        .map_err(map_store_error)?;
    for key in keys {
        state
            .storage
            .delete_bytes(&key)
            .await
            .map_err(map_store_error)?;
    }
    let registry_key = collection_registry_key(&name);
    match state.storage.delete_bytes(&registry_key).await {
        Ok(_) | Err(TurboVectorError::NotFound(_)) => {}
        Err(error) => return Err(map_store_error(error)),
    }
    state.remove_collection_registry_marker_ensured(&name).await;

    state.invalidate_collection_cache(&name).await;
    state.remove_cached_manifest(&name).await;
    state.remove_cached_collection_metadata(&name).await;
    state.remove_wal_queue_handle(&name).await;

    Ok(Json(DeleteCollectionResponse { deleted: true }))
}

#[utoipa::path(
    post,
    path = "/v1/collections/{name}/vectors/upsert",
    params(
        ("name" = String, Path, description = "Collection name"),
        ("Idempotency-Key" = Option<String>, Header, description = "Optional idempotency key")
    ),
    request_body = UpsertRequest,
    responses(
        (status = 200, description = "Upsert accepted", body = UpsertResponse),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 409, description = "Idempotency conflict", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn upsert_vectors(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    Json(payload): Json<UpsertRequest>,
) -> Result<Json<UpsertResponse>, ApiError> {
    let started = std::time::Instant::now();
    let result = upsert_vectors_inner(
        State(state.clone()),
        Path(collection),
        headers,
        Json(payload),
    )
    .await;
    let status_class = result.as_ref().map_or_else(
        |error| telemetry::status_class(error.status.as_u16()),
        |_| "2xx",
    );
    telemetry::record_upsert_request(
        &state.service_name,
        "/v1/collections/:name/vectors/upsert",
        status_class,
        started.elapsed().as_secs_f64(),
    );
    if result.is_ok() {
        telemetry::increment_upsert_accepted(&state.service_name);
    }
    result
}

async fn upsert_vectors_inner(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    headers: HeaderMap,
    Json(payload): Json<UpsertRequest>,
) -> Result<Json<UpsertResponse>, ApiError> {
    validate_collection_name(&collection)?;
    if payload.vectors.is_empty() {
        return Err(ApiError::invalid_argument("vectors must not be empty"));
    }

    let collection_meta = load_collection_metadata(&state, &collection).await?;
    let namespace = normalize_namespace(payload.namespace.as_deref())?;
    validate_upsert_vectors(collection_meta.dimension, &payload.vectors)?;

    let request_hash_input = serde_json::to_vec(&payload)
        .map_err(|e| ApiError::internal(format!("failed to serialize upsert payload: {e}")))?;
    let request_hash = sha256_hex(&request_hash_input);

    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);

    if let Some(key) = idempotency_key.as_deref() {
        let idem_key = idempotency_object_key(&collection, key);
        match state.storage.get_bytes(&idem_key).await {
            Ok(raw) => {
                let record: IdempotencyRecord = serde_json::from_slice(&raw).map_err(|e| {
                    ApiError::internal(format!("failed to parse idempotency record: {e}"))
                })?;
                if record.request_hash != request_hash {
                    return Err(ApiError::conflict(
                        "idempotency key reused with different payload",
                    ));
                }
                let replayed_response: UpsertResponse = serde_json::from_value(record.response)
                    .map_err(|e| {
                        ApiError::internal(format!(
                            "failed to parse idempotent upsert response: {e}"
                        ))
                    })?;
                return Ok(Json(replayed_response));
            }
            Err(TurboVectorError::NotFound(_)) => {}
            Err(e) => return Err(map_store_error(e)),
        }
    }

    let operation_id = new_operation_id();
    let accepted_at = now_rfc3339();
    let wal = WalRecord {
        operation_id: operation_id.clone(),
        collection: collection.clone(),
        namespace: namespace.clone(),
        accepted_at: accepted_at.clone(),
        idempotency_key: idempotency_key.clone(),
        request: payload.clone(),
    };
    let wal_bytes = serde_json::to_vec(&wal)
        .map_err(|e| ApiError::internal(format!("failed to serialize WAL: {e}")))?;
    let wal_key = wal_object_key(&collection, &operation_id);
    state
        .storage
        .put_bytes(&wal_key, &wal_bytes)
        .await
        .map_err(map_store_error)?;
    record_operation_accepted(&state, &collection, &operation_id, &accepted_at).await?;
    ensure_collection_registry_marker(&state, &collection).await?;
    enqueue_wal_operation(&state, &collection, &operation_id).await?;
    info!(
        collection = %collection,
        namespace = %namespace,
        vectors = payload.vectors.len(),
        operation_id = %operation_id,
        "upsert accepted"
    );

    let response = UpsertResponse::Accepted(UpsertAcceptedResponse {
        accepted: true,
        operation_id,
    });

    if let Some(key) = idempotency_key {
        let idem_key = idempotency_object_key(&collection, &key);
        let response_value = serde_json::to_value(&response)
            .map_err(|e| ApiError::internal(format!("failed to serialize upsert response: {e}")))?;
        let record = IdempotencyRecord {
            request_hash,
            response: response_value,
        };
        let bytes = serde_json::to_vec(&record).map_err(|e| {
            ApiError::internal(format!("failed to serialize idempotency record: {e}"))
        })?;
        state
            .storage
            .put_bytes(&idem_key, &bytes)
            .await
            .map_err(map_store_error)?;
    }

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/collections/{name}/operations/{operation_id}",
    params(
        ("name" = String, Path, description = "Collection name"),
        ("operation_id" = String, Path, description = "Upsert operation ID")
    ),
    responses(
        (status = 200, description = "Operation status", body = OperationStatusResponse),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 404, description = "Collection or operation not found", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn get_operation_status(
    State(state): State<AppState>,
    Path((collection, operation_id)): Path<(String, String)>,
) -> Result<Json<OperationStatusResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let operation_id = operation_id.trim();
    if operation_id.is_empty() {
        return Err(ApiError::invalid_argument("operation_id must not be empty"));
    }

    let _ = load_collection_metadata(&state, &collection).await?;
    let status = match load_operation_status(&state, &collection, operation_id).await {
        Ok(status) => status,
        Err(error) if error.status == StatusCode::NOT_FOUND => {
            return Err(ApiError::not_found(format!(
                "operation '{}' not found for collection '{}'",
                operation_id, collection
            )));
        }
        Err(error) => return Err(error),
    };
    Ok(Json(status))
}

#[utoipa::path(
    get,
    path = "/v1/collections/{name}/shards/placement",
    params(
        ("name" = String, Path, description = "Collection name")
    ),
    responses(
        (status = 200, description = "Shard placement metadata", body = CollectionShardPlacement),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn get_shard_placement(
    State(state): State<AppState>,
    Path(collection): Path<String>,
) -> Result<Json<CollectionShardPlacement>, ApiError> {
    validate_collection_name(&collection)?;
    let _ = load_collection_metadata(&state, &collection).await?;
    let placement = load_collection_shard_placement(&state, &collection).await?;
    Ok(Json(placement))
}

#[utoipa::path(
    put,
    path = "/v1/collections/{name}/shards/placement",
    params(
        ("name" = String, Path, description = "Collection name")
    ),
    request_body = UpdateShardPlacementRequest,
    responses(
        (status = 200, description = "Shard placement metadata updated", body = CollectionShardPlacement),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 409, description = "Version conflict", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn update_shard_placement(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(request): Json<UpdateShardPlacementRequest>,
) -> Result<Json<CollectionShardPlacement>, ApiError> {
    validate_collection_name(&collection)?;
    let _collection = load_collection_metadata(&state, &collection).await?;

    let current = load_collection_shard_placement(&state, &collection).await?;
    if let Some(expected_version) = request.expected_version {
        if expected_version != current.version {
            return Err(ApiError::conflict(format!(
                "placement version conflict: expected {}, current {}",
                expected_version, current.version
            )));
        }
    }
    if request.shard_count == 0 {
        return Err(ApiError::invalid_argument("shard_count must be > 0"));
    }
    if request.shard_count as usize != state.distributed_shard_count() {
        return Err(ApiError::invalid_argument(format!(
            "shard_count must match runtime TV_DISTRIBUTED_SHARD_COUNT={} for this node",
            state.distributed_shard_count()
        )));
    }

    if let Some(migration) = request.migration.as_ref() {
        if migration.phase == ShardMigrationPhase::Cutover {
            let current_generation = load_current_manifest(&state, &collection)
                .await?
                .map(|manifest| manifest.generation)
                .unwrap_or(0);
            if migration
                .min_visible_generation
                .is_some_and(|min_generation| current_generation < min_generation)
            {
                return Err(ApiError::invalid_argument(format!(
                    "cutover requires min_visible_generation <= current generation (current={current_generation})"
                )));
            }
        }
    }

    let updated = CollectionShardPlacement {
        collection: collection.clone(),
        version: current.version.saturating_add(1),
        strategy: ShardAssignmentStrategy::HashIdV1,
        shard_count: request.shard_count,
        assignments: request.assignments,
        migration: request.migration,
        updated_at: now_rfc3339(),
        updated_by: state.node_id.clone(),
    };
    persist_collection_shard_placement(&state, &collection, &updated).await?;
    Ok(Json(updated))
}

#[utoipa::path(
    post,
    path = "/v1/collections/{name}/shards/rebalance",
    params(
        ("name" = String, Path, description = "Collection name")
    ),
    request_body = RebalanceShardPlacementRequest,
    responses(
        (status = 200, description = "Rebalance plan generated/applied", body = RebalanceShardPlacementResponse),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn rebalance_shard_placement(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(request): Json<RebalanceShardPlacementRequest>,
) -> Result<Json<RebalanceShardPlacementResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let _collection = load_collection_metadata(&state, &collection).await?;
    let current = load_collection_shard_placement(&state, &collection).await?;
    let shard_count = request.shard_count.unwrap_or(current.shard_count).max(1);
    if shard_count as usize != state.distributed_shard_count() {
        return Err(ApiError::invalid_argument(format!(
            "rebalance shard_count must match runtime TV_DISTRIBUTED_SHARD_COUNT={} for this node",
            state.distributed_shard_count()
        )));
    }
    let assignments = distributed::rebalance_assignments(shard_count, &request.target_nodes)
        .map_err(ApiError::invalid_argument)?;
    let safety_checks = vec![
        "verify target nodes report healthy queue+query telemetry before apply".to_string(),
        "only execute cutover after target shard generation visibility is confirmed".to_string(),
    ];
    let rebalance_plan = CollectionShardPlacement {
        collection: collection.clone(),
        version: current.version.saturating_add(1),
        strategy: ShardAssignmentStrategy::HashIdV1,
        shard_count,
        assignments,
        migration: Some(crate::distributed::ShardMigrationPlan {
            migration_id: new_operation_id(),
            phase: ShardMigrationPhase::Planned,
            source_node: None,
            target_node: None,
            shard_ids: (0..shard_count).collect(),
            min_visible_generation: load_current_manifest(&state, &collection)
                .await?
                .map(|manifest| manifest.generation),
            safety_checks: safety_checks.clone(),
            updated_at: now_rfc3339(),
        }),
        updated_at: now_rfc3339(),
        updated_by: state.node_id.clone(),
    };
    let applied = !request.dry_run;
    if applied {
        persist_collection_shard_placement(&state, &collection, &rebalance_plan).await?;
    }
    Ok(Json(RebalanceShardPlacementResponse {
        applied,
        safety_checks,
        placement: rebalance_plan,
    }))
}

#[utoipa::path(
    post,
    path = "/v1/collections/{name}/vectors/delete",
    params(
        ("name" = String, Path, description = "Collection name")
    ),
    request_body = DeleteRequest,
    responses(
        (status = 200, description = "Delete applied", body = DeleteResponse),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn delete_vectors(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(payload): Json<DeleteRequest>,
) -> Result<Json<DeleteResponse>, ApiError> {
    validate_collection_name(&collection)?;
    // Ensure collection exists and normalize selectors early.
    let _ = load_collection_metadata(&state, &collection).await?;
    let DeleteRequest {
        ids,
        filter,
        namespace: raw_namespace,
        delete_all,
    } = payload;

    let namespace = normalize_namespace(raw_namespace.as_deref())?;
    let candidate_ids = normalize_delete_ids(ids)?;
    let metadata_filter = parse_metadata_filter_with_limits(filter, state.filter_parser_limits())?;

    if delete_all {
        if !candidate_ids.is_empty() || metadata_filter.is_some() {
            return Err(ApiError::invalid_argument(
                "delete_all cannot be combined with ids or filter",
            ));
        }
    } else if candidate_ids.is_empty() && metadata_filter.is_none() {
        return Err(ApiError::invalid_argument(
            "delete request must include ids, filter, or delete_all=true",
        ));
    }

    // Drain queued writes first so delete observes already-accepted upserts.
    flush_wal_queue(&state, &collection, None).await?;

    let visible_vectors = match load_current_manifest(&state, &collection).await? {
        Some(manifest) => {
            load_namespace_vectors(&state, &collection, &namespace, &manifest).await?
        }
        None => std::sync::Arc::new(BTreeMap::new()),
    };

    let mut deleted_ids = BTreeSet::new();
    if delete_all {
        deleted_ids.extend(visible_vectors.keys().cloned());
    } else {
        for id in candidate_ids {
            if visible_vectors.contains_key(&id) {
                deleted_ids.insert(id);
            }
        }
        if metadata_filter.is_some() {
            for (id, vector) in visible_vectors.iter() {
                if metadata_matches_filter(vector.metadata.as_ref(), metadata_filter.as_ref()) {
                    deleted_ids.insert(id.clone());
                }
            }
        }
    }

    if deleted_ids.is_empty() {
        return Ok(Json(DeleteResponse { deleted_count: 0 }));
    }

    let operation_id = new_operation_id();
    let segment = SegmentFile {
        segment_id: operation_id.clone(),
        collection: collection.clone(),
        namespace: namespace.clone(),
        created_at: now_rfc3339(),
        kind: SegmentKind::Delete,
        vectors: Vec::new(),
        deleted_ids: deleted_ids.into_iter().collect(),
        delete_all: false,
        delete_filter: None,
    };
    let segment_bytes = serde_json::to_vec(&segment)
        .map_err(|e| ApiError::internal(format!("failed to serialize delete segment: {e}")))?;
    let segment_key = segment_object_key(&collection, &operation_id);
    state
        .storage
        .put_bytes(&segment_key, &segment_bytes)
        .await
        .map_err(map_store_error)?;

    publish_manifest(
        &state,
        &collection,
        &namespace,
        &operation_id,
        &segment_key,
        segment.deleted_ids.len() as u64,
        sha256_hex(&segment_bytes),
    )
    .await?;

    Ok(Json(DeleteResponse {
        deleted_count: segment.deleted_ids.len() as u64,
    }))
}

fn build_query_matches(
    mut scored: Vec<(UpsertVector, f32)>,
    top_k: u32,
    include_metadata: bool,
    include_values: bool,
) -> Vec<QueryMatch> {
    scored.sort_by(|(left_vector, left_score), (right_vector, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_vector.id.cmp(&right_vector.id))
    });
    scored
        .into_iter()
        .take(top_k as usize)
        .map(|(stored, score)| QueryMatch {
            id: stored.id,
            score,
            metadata: if include_metadata {
                stored.metadata
            } else {
                None
            },
            values: if include_values {
                Some(stored.values)
            } else {
                None
            },
        })
        .collect()
}

fn query_strategy_label(strategy: &QuerySearchStrategy) -> &'static str {
    match strategy {
        QuerySearchStrategy::Ann => "ann",
        QuerySearchStrategy::Exact => "exact",
        QuerySearchStrategy::Auto => "auto",
    }
}

fn record_distributed_query_metrics(
    service_name: &str,
    node_id: &str,
    strategy: &QuerySearchStrategy,
    status: StatusCode,
    duration_seconds: f64,
    distributed: &QueryDistributedObservability,
) {
    let strategy_label = query_strategy_label(strategy);
    let status_class = telemetry::status_class(status.as_u16());
    telemetry::increment_query_distributed(
        service_name,
        node_id,
        strategy_label,
        distributed.degraded,
        status_class,
    );
    telemetry::record_query_distributed_duration(
        service_name,
        node_id,
        strategy_label,
        distributed.degraded,
        status_class,
        duration_seconds,
    );
    telemetry::record_query_distributed_shard_ratios(
        service_name,
        node_id,
        strategy_label,
        distributed.degraded,
        distributed.planned_shards,
        distributed.successful_shards,
        distributed.required_successful_shards,
    );

    if distributed.degraded {
        if distributed.degradation_reasons.is_empty() {
            telemetry::increment_query_distributed_degradation_reason(
                service_name,
                node_id,
                "unknown",
            );
        } else {
            for reason in &distributed.degradation_reasons {
                telemetry::increment_query_distributed_degradation_reason(
                    service_name,
                    node_id,
                    reason,
                );
            }
        }
    }

    for shard_status in &distributed.shard_statuses {
        telemetry::increment_query_distributed_shard_outcome(
            service_name,
            node_id,
            strategy_label,
            &shard_status.status,
        );
        telemetry::record_query_distributed_shard_latency_ms(
            service_name,
            node_id,
            strategy_label,
            &shard_status.status,
            shard_status.latency_ms,
        );
    }
}

#[utoipa::path(
    post,
    path = "/v1/collections/{name}/vectors/query",
    params(
        ("name" = String, Path, description = "Collection name")
    ),
    request_body = QueryRequest,
    responses(
        (status = 200, description = "Query response", body = QueryResponse),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn query_vectors(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    let strategy = query_strategy_label(&payload.search_strategy);
    let started = std::time::Instant::now();
    let collection_name = collection.clone();
    let result = query_vectors_inner(State(state.clone()), Path(collection), Json(payload)).await;
    let status_class = result.as_ref().map_or_else(
        |error| telemetry::status_class(error.status.as_u16()),
        |_| "2xx",
    );
    let elapsed = started.elapsed();
    telemetry::record_query(
        &state.service_name,
        "/v1/collections/:name/vectors/query",
        strategy,
        status_class,
        elapsed.as_secs_f64(),
    );
    if let Ok(Json(response)) = &result {
        info!(
            collection = %collection_name,
            strategy,
            elapsed_ms = elapsed.as_millis() as u64,
            matches = response.matches.len(),
            ann_used = response.ann.ann_used,
            "query completed"
        );
        telemetry::increment_query_ann_fetch_errors(
            &state.service_name,
            response.ann.ann_fetch_errors,
        );
        if response.ann.ann_fallback_count > 0 {
            if response.ann.fallback_reasons.is_empty() {
                telemetry::increment_query_ann_fallback_reason(&state.service_name, "unknown");
            } else {
                for reason in &response.ann.fallback_reasons {
                    telemetry::increment_query_ann_fallback_reason(&state.service_name, reason);
                }
            }
        }
        telemetry::increment_cache_hits(
            &state.service_name,
            "filter_cluster",
            response.ann.filter_cluster_cache_hits,
        );
        telemetry::increment_cache_misses(
            &state.service_name,
            "filter_cluster",
            response.ann.filter_cluster_cache_misses,
        );
        telemetry::increment_cache_hits(
            &state.service_name,
            "filter_row",
            response.ann.filter_row_cache_hits,
        );
        telemetry::increment_cache_misses(
            &state.service_name,
            "filter_row",
            response.ann.filter_row_cache_misses,
        );
    }
    result
}

fn merge_ann_observability(aggregate: &mut QueryAnnObservability, shard: &QueryAnnObservability) {
    aggregate.ann_used |= shard.ann_used;
    aggregate.ann_fallback_count = aggregate
        .ann_fallback_count
        .saturating_add(shard.ann_fallback_count);
    aggregate.buckets_probed = aggregate
        .buckets_probed
        .saturating_add(shard.buckets_probed);
    aggregate.candidates_scored = aggregate
        .candidates_scored
        .saturating_add(shard.candidates_scored);
    aggregate.ann_fetch_errors = aggregate
        .ann_fetch_errors
        .saturating_add(shard.ann_fetch_errors);
    aggregate.filter_cluster_cache_hits = aggregate
        .filter_cluster_cache_hits
        .saturating_add(shard.filter_cluster_cache_hits);
    aggregate.filter_cluster_cache_misses = aggregate
        .filter_cluster_cache_misses
        .saturating_add(shard.filter_cluster_cache_misses);
    aggregate.filter_cluster_cache_evictions = aggregate
        .filter_cluster_cache_evictions
        .saturating_add(shard.filter_cluster_cache_evictions);
    aggregate.filter_row_cache_hits = aggregate
        .filter_row_cache_hits
        .saturating_add(shard.filter_row_cache_hits);
    aggregate.filter_row_cache_misses = aggregate
        .filter_row_cache_misses
        .saturating_add(shard.filter_row_cache_misses);
    aggregate.filter_row_cache_evictions = aggregate
        .filter_row_cache_evictions
        .saturating_add(shard.filter_row_cache_evictions);
    aggregate.filter_widen_passes = aggregate
        .filter_widen_passes
        .saturating_add(shard.filter_widen_passes);
    aggregate.rerank_ssd_cache_hits = aggregate
        .rerank_ssd_cache_hits
        .saturating_add(shard.rerank_ssd_cache_hits);
    aggregate.rerank_ssd_cache_misses = aggregate
        .rerank_ssd_cache_misses
        .saturating_add(shard.rerank_ssd_cache_misses);
    aggregate.rerank_ssd_cache_evictions = aggregate
        .rerank_ssd_cache_evictions
        .saturating_add(shard.rerank_ssd_cache_evictions);
    aggregate.rerank_ssd_fetch_latency_ms += shard.rerank_ssd_fetch_latency_ms;
    aggregate.first_stage_candidate_count = aggregate
        .first_stage_candidate_count
        .saturating_add(shard.first_stage_candidate_count);
    aggregate.rerank_candidate_count = aggregate
        .rerank_candidate_count
        .saturating_add(shard.rerank_candidate_count);
    aggregate.quantization_bound_margin += shard.quantization_bound_margin;
    aggregate.quantization_bound_threshold += shard.quantization_bound_threshold;
    aggregate.object_read_budget = aggregate
        .object_read_budget
        .saturating_add(shard.object_read_budget);
    aggregate.object_read_budget_exceeded |= shard.object_read_budget_exceeded;
    aggregate.ann_meta_object_reads = aggregate
        .ann_meta_object_reads
        .saturating_add(shard.ann_meta_object_reads);
    aggregate.ann_meta_object_bytes = aggregate
        .ann_meta_object_bytes
        .saturating_add(shard.ann_meta_object_bytes);
    aggregate.ann_bucket_object_reads = aggregate
        .ann_bucket_object_reads
        .saturating_add(shard.ann_bucket_object_reads);
    aggregate.ann_bucket_object_bytes = aggregate
        .ann_bucket_object_bytes
        .saturating_add(shard.ann_bucket_object_bytes);
    aggregate.ann_filter_cluster_object_reads = aggregate
        .ann_filter_cluster_object_reads
        .saturating_add(shard.ann_filter_cluster_object_reads);
    aggregate.ann_filter_cluster_object_bytes = aggregate
        .ann_filter_cluster_object_bytes
        .saturating_add(shard.ann_filter_cluster_object_bytes);
    aggregate.ann_filter_row_object_reads = aggregate
        .ann_filter_row_object_reads
        .saturating_add(shard.ann_filter_row_object_reads);
    aggregate.ann_filter_row_object_bytes = aggregate
        .ann_filter_row_object_bytes
        .saturating_add(shard.ann_filter_row_object_bytes);
    aggregate.rerank_segment_object_reads = aggregate
        .rerank_segment_object_reads
        .saturating_add(shard.rerank_segment_object_reads);
    aggregate.rerank_segment_object_bytes = aggregate
        .rerank_segment_object_bytes
        .saturating_add(shard.rerank_segment_object_bytes);
    if !shard.fallback_reasons.is_empty() {
        let mut merged = aggregate
            .fallback_reasons
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        merged.extend(shard.fallback_reasons.iter().cloned());
        aggregate.fallback_reasons = merged.into_iter().collect();
    }
}

#[allow(clippy::too_many_arguments)]
async fn query_vectors_single_namespace(
    state: &AppState,
    collection: &str,
    namespace: &str,
    collection_meta: &CollectionMetadata,
    vector: &[f32],
    top_k: u32,
    include_metadata: bool,
    include_values: bool,
    metadata_filter: Option<&MetadataFilterExpression>,
    search_strategy: &QuerySearchStrategy,
    current_manifest: Option<Manifest>,
) -> Result<(Vec<QueryMatch>, QueryAnnObservability), ApiError> {
    let mut scored: Vec<(UpsertVector, f32)> = Vec::new();
    let mut ann_used = false;
    let mut ann_observability = QueryAnnObservability::default();

    if let Some(manifest) = current_manifest.as_ref() {
        let ann_requested = match search_strategy {
            QuerySearchStrategy::Exact => false,
            QuerySearchStrategy::Ann => true,
            QuerySearchStrategy::Auto => state.query_ann_enabled(),
        };
        if ann_requested {
            let ann_execution = query_namespace_with_ann(
                state,
                collection,
                namespace,
                manifest,
                &collection_meta.metric,
                collection_meta.dimension,
                vector,
                top_k,
                metadata_filter,
            )
            .await;

            ann_observability.buckets_probed = ann_execution.stats.buckets_probed as u64;
            ann_observability.candidates_scored = ann_execution.stats.candidates_scored as u64;
            ann_observability.ann_fetch_errors = ann_execution.stats.ann_fetch_errors as u64;
            ann_observability.filter_cluster_cache_hits =
                ann_execution.stats.filter_cluster_cache_hits as u64;
            ann_observability.filter_cluster_cache_misses =
                ann_execution.stats.filter_cluster_cache_misses as u64;
            ann_observability.filter_cluster_cache_evictions =
                ann_execution.stats.filter_cluster_cache_evictions as u64;
            ann_observability.filter_row_cache_hits =
                ann_execution.stats.filter_row_cache_hits as u64;
            ann_observability.filter_row_cache_misses =
                ann_execution.stats.filter_row_cache_misses as u64;
            ann_observability.filter_row_cache_evictions =
                ann_execution.stats.filter_row_cache_evictions as u64;
            ann_observability.filter_widen_passes = ann_execution.stats.widen_passes as u64;
            ann_observability.rerank_ssd_cache_hits =
                ann_execution.stats.rerank_ssd_cache_hits as u64;
            ann_observability.rerank_ssd_cache_misses =
                ann_execution.stats.rerank_ssd_cache_misses as u64;
            ann_observability.rerank_ssd_cache_evictions =
                ann_execution.stats.rerank_ssd_cache_evictions as u64;
            ann_observability.rerank_ssd_fetch_latency_ms =
                ann_execution.stats.rerank_ssd_fetch_latency_ms;
            ann_observability.first_stage_candidate_count =
                ann_execution.stats.first_stage_candidate_count as u64;
            ann_observability.rerank_candidate_count =
                ann_execution.stats.rerank_candidate_count as u64;
            ann_observability.quantization_bound_margin =
                ann_execution.stats.quantization_bound_margin as f64;
            ann_observability.quantization_bound_threshold =
                ann_execution.stats.quantization_bound_threshold as f64;
            ann_observability.object_read_budget = ann_execution.stats.object_read_budget as u64;
            ann_observability.object_read_budget_exceeded =
                ann_execution.stats.object_read_budget_exceeded;
            ann_observability.ann_meta_object_reads =
                ann_execution.stats.ann_meta_object_reads as u64;
            ann_observability.ann_meta_object_bytes =
                ann_execution.stats.ann_meta_object_bytes as u64;
            ann_observability.ann_bucket_object_reads =
                ann_execution.stats.ann_bucket_object_reads as u64;
            ann_observability.ann_bucket_object_bytes =
                ann_execution.stats.ann_bucket_object_bytes as u64;
            ann_observability.ann_filter_cluster_object_reads =
                ann_execution.stats.ann_filter_cluster_object_reads as u64;
            ann_observability.ann_filter_cluster_object_bytes =
                ann_execution.stats.ann_filter_cluster_object_bytes as u64;
            ann_observability.ann_filter_row_object_reads =
                ann_execution.stats.ann_filter_row_object_reads as u64;
            ann_observability.ann_filter_row_object_bytes =
                ann_execution.stats.ann_filter_row_object_bytes as u64;
            ann_observability.rerank_segment_object_reads =
                ann_execution.stats.rerank_segment_object_reads as u64;
            ann_observability.rerank_segment_object_bytes =
                ann_execution.stats.rerank_segment_object_bytes as u64;
            match ann_execution.scored {
                Some(ann_scored) => {
                    scored = ann_scored;
                    ann_used = true;
                    ann_observability.ann_used = true;
                }
                None => {
                    ann_observability.ann_fallback_count = 1;
                    ann_observability.fallback_reasons = ann_execution.stats.fallback_reasons;
                    warn!(
                        collection,
                        namespace,
                        search_strategy = ?search_strategy,
                        fallback_reasons = ?ann_observability.fallback_reasons,
                        "ANN query unavailable; falling back to exact query"
                    );
                }
            }
        }
    }

    if !ann_used {
        let namespace_cache_warm = if let Some(manifest) = current_manifest.as_ref() {
            state
                .get_namespace_cache_entry(collection, namespace)
                .await
                .is_some_and(|entry| entry.generation == manifest.generation)
        } else {
            true
        };
        if namespace_cache_warm {
            telemetry::increment_query_temperature(&state.service_name, "warm");
        } else {
            telemetry::increment_query_temperature(&state.service_name, "cold");
        }
        let namespace_load_started = std::time::Instant::now();
        let visible_vectors = match current_manifest.as_ref() {
            Some(manifest) => {
                load_namespace_vectors(state, collection, namespace, manifest).await?
            }
            None => std::sync::Arc::new(BTreeMap::new()),
        };
        if !namespace_cache_warm {
            telemetry::record_query_namespace_load_duration(
                &state.service_name,
                namespace_load_started.elapsed().as_secs_f64(),
            );
            telemetry::increment_query_cache_fill(&state.service_name, "namespace_vectors");
        }

        for stored in visible_vectors.values() {
            if !metadata_matches_filter(stored.metadata.as_ref(), metadata_filter) {
                continue;
            }
            let score = compute_score(&collection_meta.metric, vector, &stored.values);
            scored.push((stored.clone(), score));
        }
    }

    let matches = build_query_matches(scored, top_k, include_metadata, include_values);
    Ok((matches, ann_observability))
}

pub(crate) async fn query_vectors_inner(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(payload): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let collection_meta = load_collection_metadata(&state, &collection).await?;

    let QueryRequest {
        vector,
        top_k,
        namespace: raw_namespace,
        include_metadata,
        include_values,
        filter,
        search_strategy,
    } = payload;
    let namespace = normalize_namespace(raw_namespace.as_deref())?;
    validate_query_request(collection_meta.dimension, &vector, top_k)?;
    let metadata_filter = parse_metadata_filter_with_limits(filter, state.filter_parser_limits())?;
    let current_manifest = load_current_manifest(&state, &collection).await?;

    let placement = load_collection_shard_placement(&state, &collection).await?;
    let shard_count = placement.shard_count.max(1) as usize;
    if shard_count <= 1 {
        let (matches, ann) = query_vectors_single_namespace(
            &state,
            &collection,
            &namespace,
            &collection_meta,
            &vector,
            top_k,
            include_metadata,
            include_values,
            metadata_filter.as_ref(),
            &search_strategy,
            current_manifest,
        )
        .await?;
        return Ok(Json(QueryResponse {
            matches,
            namespace,
            ann,
            distributed: None,
        }));
    }

    let current_generation = current_manifest
        .as_ref()
        .map(|manifest| manifest.generation)
        .unwrap_or(0);
    let required_successful_shards = state
        .distributed_required_successful_shards()
        .min(shard_count)
        .max(1);
    let distributed_started = std::time::Instant::now();
    let shard_timeout = state.distributed_shard_timeout();
    let shard_timeout_ms = shard_timeout.as_millis() as u64;

    let assignment_by_shard = placement
        .assignments
        .into_iter()
        .map(|assignment| (assignment.shard_id, assignment))
        .collect::<BTreeMap<_, _>>();

    let mut distributed = QueryDistributedObservability {
        planned_shards: shard_count as u32,
        required_successful_shards: required_successful_shards as u32,
        shard_timeout_ms,
        ..Default::default()
    };
    let mut degradation_reasons = BTreeSet::new();
    let mut shard_matches: Vec<QueryMatch> = Vec::new();
    let mut ann_observability = QueryAnnObservability::default();
    let mut shard_tasks = JoinSet::new();

    for (shard_id, shard_namespace) in state.shard_namespaces(&namespace) {
        let Some(assignment) = assignment_by_shard.get(&shard_id).cloned() else {
            distributed.failed_shards = distributed.failed_shards.saturating_add(1);
            distributed.shard_statuses.push(QueryShardStatus {
                shard_id,
                node_id: "unassigned".to_string(),
                status: SHARD_DEGRADED_REASON_ERROR.to_string(),
                generation: Some(current_generation),
                latency_ms: 0.0,
                match_count: 0,
                reasons: vec![SHARD_DEGRADED_REASON_ERROR.to_string()],
            });
            degradation_reasons.insert(SHARD_DEGRADED_REASON_ERROR.to_string());
            continue;
        };
        if assignment.state == ShardNodeState::Offline {
            distributed.dropped_shards = distributed.dropped_shards.saturating_add(1);
            distributed.shard_statuses.push(QueryShardStatus {
                shard_id,
                node_id: assignment.node_id.clone(),
                status: SHARD_DEGRADED_REASON_DROPPED.to_string(),
                generation: Some(current_generation),
                latency_ms: 0.0,
                match_count: 0,
                reasons: vec![SHARD_DEGRADED_REASON_DROPPED.to_string()],
            });
            degradation_reasons.insert(SHARD_DEGRADED_REASON_DROPPED.to_string());
            continue;
        }
        if assignment
            .min_generation
            .is_some_and(|min_generation| current_generation < min_generation)
        {
            distributed.stale_shards = distributed.stale_shards.saturating_add(1);
            distributed.shard_statuses.push(QueryShardStatus {
                shard_id,
                node_id: assignment.node_id.clone(),
                status: SHARD_DEGRADED_REASON_STALE.to_string(),
                generation: Some(current_generation),
                latency_ms: 0.0,
                match_count: 0,
                reasons: vec![SHARD_DEGRADED_REASON_STALE.to_string()],
            });
            degradation_reasons.insert(SHARD_DEGRADED_REASON_STALE.to_string());
            continue;
        }

        let state = state.clone();
        let collection = collection.clone();
        let collection_meta = collection_meta.clone();
        let vector = vector.clone();
        let search_strategy = search_strategy.clone();
        let metadata_filter = metadata_filter.clone();
        let current_manifest = current_manifest.clone();
        let node_id = assignment.node_id.clone();
        let simulated_delay_ms = assignment.simulated_delay_ms.unwrap_or(0);
        shard_tasks.spawn(async move {
            let started = std::time::Instant::now();
            let outcome = timeout(shard_timeout, async {
                if simulated_delay_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(simulated_delay_ms)).await;
                }
                query_vectors_single_namespace(
                    &state,
                    &collection,
                    &shard_namespace,
                    &collection_meta,
                    &vector,
                    top_k,
                    include_metadata,
                    include_values,
                    metadata_filter.as_ref(),
                    &search_strategy,
                    current_manifest,
                )
                .await
            })
            .await;
            (
                shard_id,
                node_id,
                started.elapsed().as_secs_f64() * 1_000.0,
                outcome,
            )
        });
    }

    while let Some(joined) = shard_tasks.join_next().await {
        match joined {
            Ok((shard_id, node_id, latency_ms, outcome)) => match outcome {
                Ok(Ok((matches, shard_ann))) => {
                    distributed.successful_shards = distributed.successful_shards.saturating_add(1);
                    distributed.shard_statuses.push(QueryShardStatus {
                        shard_id,
                        node_id,
                        status: "ok".to_string(),
                        generation: Some(current_generation),
                        latency_ms,
                        match_count: matches.len() as u64,
                        reasons: Vec::new(),
                    });
                    shard_matches.extend(matches);
                    merge_ann_observability(&mut ann_observability, &shard_ann);
                }
                Ok(Err(error)) => {
                    distributed.failed_shards = distributed.failed_shards.saturating_add(1);
                    distributed.shard_statuses.push(QueryShardStatus {
                        shard_id,
                        node_id,
                        status: SHARD_DEGRADED_REASON_ERROR.to_string(),
                        generation: Some(current_generation),
                        latency_ms,
                        match_count: 0,
                        reasons: vec![format!(
                            "{SHARD_DEGRADED_REASON_ERROR}:status_{}",
                            error.status.as_u16()
                        )],
                    });
                    degradation_reasons.insert(SHARD_DEGRADED_REASON_ERROR.to_string());
                }
                Err(_) => {
                    distributed.timed_out_shards = distributed.timed_out_shards.saturating_add(1);
                    distributed.shard_statuses.push(QueryShardStatus {
                        shard_id,
                        node_id,
                        status: SHARD_DEGRADED_REASON_TIMEOUT.to_string(),
                        generation: Some(current_generation),
                        latency_ms,
                        match_count: 0,
                        reasons: vec![SHARD_DEGRADED_REASON_TIMEOUT.to_string()],
                    });
                    degradation_reasons.insert(SHARD_DEGRADED_REASON_TIMEOUT.to_string());
                }
            },
            Err(_) => {
                distributed.failed_shards = distributed.failed_shards.saturating_add(1);
                degradation_reasons.insert(SHARD_DEGRADED_REASON_ERROR.to_string());
            }
        }
    }

    distributed.degraded = distributed.successful_shards < distributed.planned_shards
        || distributed.failed_shards > 0
        || distributed.timed_out_shards > 0
        || distributed.dropped_shards > 0
        || distributed.stale_shards > 0;
    distributed.degradation_reasons = degradation_reasons.iter().cloned().collect();
    if distributed.degraded && distributed.degradation_reasons.is_empty() {
        distributed
            .degradation_reasons
            .push(SHARD_DEGRADED_REASON_ERROR.to_string());
    }

    if distributed.successful_shards < required_successful_shards as u32
        && !state.distributed_fail_open()
    {
        record_distributed_query_metrics(
            &state.service_name,
            &state.node_id,
            &search_strategy,
            StatusCode::SERVICE_UNAVAILABLE,
            distributed_started.elapsed().as_secs_f64(),
            &distributed,
        );
        let reasons = distributed.degradation_reasons.join(",");
        return Err(ApiError::store_unavailable(format!(
            "insufficient healthy shards for query: successful={} required={} reasons={}",
            distributed.successful_shards, required_successful_shards, reasons
        )));
    }

    shard_matches.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.id.cmp(&right.id))
    });
    let mut deduped_matches = Vec::with_capacity(top_k as usize);
    let mut seen_ids = BTreeSet::new();
    for candidate in shard_matches {
        if !seen_ids.insert(candidate.id.clone()) {
            continue;
        }
        deduped_matches.push(candidate);
        if deduped_matches.len() >= top_k as usize {
            break;
        }
    }

    record_distributed_query_metrics(
        &state.service_name,
        &state.node_id,
        &search_strategy,
        StatusCode::OK,
        distributed_started.elapsed().as_secs_f64(),
        &distributed,
    );

    Ok(Json(QueryResponse {
        matches: deduped_matches,
        namespace,
        ann: ann_observability,
        distributed: Some(distributed),
    }))
}

#[utoipa::path(
    post,
    path = "/v1/collections/{name}/vectors/fetch",
    params(
        ("name" = String, Path, description = "Collection name")
    ),
    request_body = FetchRequest,
    responses(
        (status = 200, description = "Fetch response", body = FetchResponse),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn fetch_vectors(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(payload): Json<FetchRequest>,
) -> Result<Json<FetchResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let _ = load_collection_metadata(&state, &collection).await?;
    let namespace = normalize_namespace(payload.namespace.as_deref())?;
    let ids = normalize_delete_ids(Some(payload.ids))?;

    let visible_vectors = match load_current_manifest(&state, &collection).await? {
        Some(manifest) => {
            load_namespace_vectors(&state, &collection, &namespace, &manifest).await?
        }
        None => std::sync::Arc::new(BTreeMap::new()),
    };

    let vectors = ids
        .into_iter()
        .filter_map(|id| visible_vectors.get(&id))
        .map(|stored| FetchVector {
            id: stored.id.clone(),
            metadata: if payload.include_metadata {
                stored.metadata.clone()
            } else {
                None
            },
            values: if payload.include_values {
                Some(stored.values.clone())
            } else {
                None
            },
        })
        .collect();

    Ok(Json(FetchResponse { vectors, namespace }))
}

#[utoipa::path(
    get,
    path = "/v1/collections/{name}/stats",
    params(
        ("name" = String, Path, description = "Collection name"),
        StatsQuery
    ),
    responses(
        (status = 200, description = "Collection stats", body = CollectionStatsResponse),
        (status = 400, description = "Invalid request", body = ErrorEnvelope),
        (status = 404, description = "Collection not found", body = ErrorEnvelope),
        (status = 503, description = "Object store unavailable", body = ErrorEnvelope),
        (status = 500, description = "Internal error", body = ErrorEnvelope)
    )
)]
async fn collection_stats(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Query(params): Query<StatsQuery>,
) -> Result<Json<CollectionStatsResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let collection_meta = load_collection_metadata(&state, &collection).await?;
    let namespace = normalize_namespace(params.namespace.as_deref())?;

    let (vector_count, segments, generation) =
        match load_current_manifest(&state, &collection).await? {
            Some(manifest) => {
                let vectors =
                    load_namespace_vectors(&state, &collection, &namespace, &manifest).await?;
                let segment_count = manifest
                    .namespace_partitions
                    .get(&namespace)
                    .map(|segment_ids| segment_ids.len() as u64)
                    .unwrap_or(0);
                (vectors.len() as u64, segment_count, manifest.generation)
            }
            None => (0, 0, 0),
        };

    Ok(Json(CollectionStatsResponse {
        dimension: collection_meta.dimension,
        vector_count,
        segments,
        generation,
    }))
}

#[derive(OpenApi)]
#[openapi(
    paths(
        health,
        runtime_config,
        list_collections,
        create_collection,
        get_collection,
        delete_collection,
        upsert_vectors,
        get_operation_status,
        get_shard_placement,
        update_shard_placement,
        rebalance_shard_placement,
        delete_vectors,
        fetch_vectors,
        query_vectors,
        collection_stats
    ),
    components(
        schemas(
            HealthResponse,
            RuntimeConfigResponse,
            ErrorEnvelope,
            CollectionMetadata,
            CreateCollectionRequest,
            ListCollectionsResponse,
            UpsertRequest,
            UpsertResponse,
            OperationApplyStatus,
            OperationStatusResponse,
            DeleteRequest,
            DeleteResponse,
            DeleteCollectionResponse,
            QueryRequest,
            QuerySearchStrategy,
            QueryResponse,
            QueryMatch,
            QueryAnnObservability,
            QueryDistributedObservability,
            QueryShardStatus,
            CollectionShardPlacement,
            UpdateShardPlacementRequest,
            RebalanceShardPlacementRequest,
            RebalanceShardPlacementResponse,
            FetchRequest,
            FetchResponse,
            FetchVector,
            CollectionStatsResponse
        )
    ),
    tags(
        (name = "turbo-vector-api", description = "Object-storage-native vector API")
    )
)]
struct ApiDoc;
