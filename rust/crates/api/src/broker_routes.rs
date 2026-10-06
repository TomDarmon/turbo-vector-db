use axum::{
    extract::{Path, State},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde::Serialize;
use tower_http::trace::TraceLayer;

use crate::{
    error::ApiError,
    queue_broker::{
        QueueAckBatchRequest, QueueAckBatchResponse, QueueBoolResponse, QueueClaimBatchRequest,
        QueueClaimBatchResponse, QueueClaimRequest, QueueClaimResponse, QueueClaimedJob,
        QueueEnqueueRequest, QueueEnqueueResponse, QueueSnapshotResponse, QueueWorkerJobRequest,
    },
    state::{AppState, HealthResponse},
    telemetry,
    validation::validate_collection_name,
};

fn map_queue_error(error: turbo_vector_queue::QueueError) -> ApiError {
    ApiError::store_unavailable(format!("queue broker error: {error}"))
}

pub(crate) fn broker_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/internal/broker/ownership", get(broker_queue_ownership))
        .route("/v1/internal/queues/:collection/enqueue", post(enqueue))
        .route("/v1/internal/queues/:collection/claim", post(claim))
        .route(
            "/v1/internal/queues/:collection/claim_batch",
            post(claim_batch),
        )
        .route("/v1/internal/queues/:collection/heartbeat", post(heartbeat))
        .route("/v1/internal/queues/:collection/ack", post(ack))
        .route("/v1/internal/queues/:collection/ack_batch", post(ack_batch))
        .route("/v1/internal/queues/:collection/snapshot", get(snapshot))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

#[derive(Debug, Serialize)]
struct BrokerOwnershipResponse {
    broker_id: String,
    collections: Vec<String>,
}

async fn broker_queue_ownership(
    State(state): State<AppState>,
) -> Result<Json<BrokerOwnershipResponse>, ApiError> {
    let mut collections = state
        .wal_queue_handles
        .lock()
        .await
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    collections.sort();
    Ok(Json(BrokerOwnershipResponse {
        broker_id: state.node_id,
        collections,
    }))
}

async fn enqueue(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(request): Json<QueueEnqueueRequest>,
) -> Result<Json<QueueEnqueueResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let queue = state.wal_queue_handle(&collection).await;
    let enqueued = queue
        .enqueue(request.payload)
        .await
        .map_err(map_queue_error)?;
    Ok(Json(QueueEnqueueResponse {
        job_id: enqueued.id,
    }))
}

async fn claim(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(request): Json<QueueClaimRequest>,
) -> Result<Json<QueueClaimResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let queue = state.wal_queue_handle(&collection).await;
    let job = queue
        .claim(request.worker_id)
        .await
        .map_err(map_queue_error)?
        .map(QueueClaimedJob::from);
    if job.is_some() {
        telemetry::increment_queue_claim(&state.service_name, 1);
    }
    Ok(Json(QueueClaimResponse { job }))
}

async fn claim_batch(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(request): Json<QueueClaimBatchRequest>,
) -> Result<Json<QueueClaimBatchResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let queue = state.wal_queue_handle(&collection).await;
    let jobs = queue
        .claim_batch(request.worker_id, request.max_jobs)
        .await
        .map_err(map_queue_error)?
        .into_iter()
        .map(QueueClaimedJob::from)
        .collect::<Vec<_>>();
    if !jobs.is_empty() {
        telemetry::increment_queue_claim(&state.service_name, jobs.len() as u64);
    }
    Ok(Json(QueueClaimBatchResponse { jobs }))
}

async fn heartbeat(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(request): Json<QueueWorkerJobRequest>,
) -> Result<Json<QueueBoolResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let queue = state.wal_queue_handle(&collection).await;
    let ok = queue
        .heartbeat(request.worker_id, request.job_id)
        .await
        .map_err(map_queue_error)?;
    Ok(Json(QueueBoolResponse { ok }))
}

async fn ack(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(request): Json<QueueWorkerJobRequest>,
) -> Result<Json<QueueBoolResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let queue = state.wal_queue_handle(&collection).await;
    let ok = queue
        .ack(request.worker_id, request.job_id)
        .await
        .map_err(map_queue_error)?;
    if ok {
        telemetry::increment_queue_ack(&state.service_name, 1);
    }
    Ok(Json(QueueBoolResponse { ok }))
}

async fn ack_batch(
    State(state): State<AppState>,
    Path(collection): Path<String>,
    Json(request): Json<QueueAckBatchRequest>,
) -> Result<Json<QueueAckBatchResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let queue = state.wal_queue_handle(&collection).await;
    let acked = queue
        .ack_batch(request.worker_id, request.job_ids)
        .await
        .map_err(map_queue_error)?;
    if acked > 0 {
        telemetry::increment_queue_ack(&state.service_name, acked as u64);
    }
    Ok(Json(QueueAckBatchResponse { acked }))
}

async fn snapshot(
    State(state): State<AppState>,
    Path(collection): Path<String>,
) -> Result<Json<QueueSnapshotResponse>, ApiError> {
    validate_collection_name(&collection)?;
    let queue = state.wal_queue_handle(&collection).await;
    let snapshot = queue.snapshot().await.map_err(map_queue_error)?;
    Ok(Json(QueueSnapshotResponse { snapshot }))
}
