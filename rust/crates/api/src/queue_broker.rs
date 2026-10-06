use async_trait::async_trait;
use reqwest::Method;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use std::collections::BTreeMap;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use tokio::sync::Mutex;
#[cfg(test)]
use turbo_vector_queue::{spawn_queue_broker, QueueBrokerConfig, QueueHandle};
use turbo_vector_queue::{ClaimedJob, EnqueuedJob, QueueError, QueueFile};
#[cfg(test)]
use turbo_vector_storage::ObjectStore;

#[cfg(test)]
use crate::keys::wal_queue_object_key;
#[cfg(test)]
use crate::queue_store_adapter::ObjectStoreCasAdapter;

#[cfg(test)]
const DEFAULT_WAL_QUEUE_LEASE_TIMEOUT_MS: u64 = 30_000;
#[cfg(test)]
const DEFAULT_WAL_QUEUE_CHANNEL_CAPACITY: usize = 4_096;
const BROKER_QUEUE_API_PREFIX: &str = "/v1/internal/queues";

#[async_trait]
pub(crate) trait QueueBrokerClient: Send + Sync {
    async fn enqueue(
        &self,
        collection: &str,
        payload: Value,
    ) -> turbo_vector_queue::Result<EnqueuedJob>;

    async fn claim_batch(
        &self,
        collection: &str,
        worker_id: String,
        max_jobs: usize,
    ) -> turbo_vector_queue::Result<Vec<ClaimedJob>>;

    async fn ack_batch(
        &self,
        collection: &str,
        worker_id: String,
        job_ids: Vec<String>,
    ) -> turbo_vector_queue::Result<usize>;

    async fn snapshot(&self, collection: &str) -> turbo_vector_queue::Result<QueueFile>;
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct InProcessQueueBrokerClient {
    storage: Arc<dyn ObjectStore>,
    broker_id: String,
    lease_timeout_ms: u64,
    channel_capacity: usize,
    handles: Arc<Mutex<BTreeMap<String, QueueHandle>>>,
}

#[cfg(test)]
impl InProcessQueueBrokerClient {
    pub(crate) fn new(storage: Arc<dyn ObjectStore>, broker_id: impl Into<String>) -> Self {
        Self::with_config(
            storage,
            broker_id,
            DEFAULT_WAL_QUEUE_LEASE_TIMEOUT_MS,
            DEFAULT_WAL_QUEUE_CHANNEL_CAPACITY,
        )
    }

    pub(crate) fn with_config(
        storage: Arc<dyn ObjectStore>,
        broker_id: impl Into<String>,
        lease_timeout_ms: u64,
        channel_capacity: usize,
    ) -> Self {
        Self {
            storage,
            broker_id: broker_id.into(),
            lease_timeout_ms,
            channel_capacity,
            handles: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    async fn queue_handle(&self, collection: &str) -> QueueHandle {
        let mut handles = self.handles.lock().await;
        if let Some(existing) = handles.get(collection) {
            return existing.clone();
        }

        let handle = spawn_queue_broker(
            Arc::new(ObjectStoreCasAdapter::new(self.storage.clone())),
            QueueBrokerConfig {
                queue_key: wal_queue_object_key(collection),
                broker_id: Some(self.broker_id.clone()),
                lease_timeout_ms: self.lease_timeout_ms,
                channel_capacity: self.channel_capacity,
                ..QueueBrokerConfig::default()
            },
        );
        handles.insert(collection.to_string(), handle.clone());
        handle
    }
}

#[cfg(test)]
#[async_trait]
impl QueueBrokerClient for InProcessQueueBrokerClient {
    async fn enqueue(
        &self,
        collection: &str,
        payload: Value,
    ) -> turbo_vector_queue::Result<EnqueuedJob> {
        self.queue_handle(collection).await.enqueue(payload).await
    }

    async fn claim_batch(
        &self,
        collection: &str,
        worker_id: String,
        max_jobs: usize,
    ) -> turbo_vector_queue::Result<Vec<ClaimedJob>> {
        self.queue_handle(collection)
            .await
            .claim_batch(worker_id, max_jobs)
            .await
    }

    async fn ack_batch(
        &self,
        collection: &str,
        worker_id: String,
        job_ids: Vec<String>,
    ) -> turbo_vector_queue::Result<usize> {
        self.queue_handle(collection)
            .await
            .ack_batch(worker_id, job_ids)
            .await
    }

    async fn snapshot(&self, collection: &str) -> turbo_vector_queue::Result<QueueFile> {
        self.queue_handle(collection).await.snapshot().await
    }
}

#[derive(Clone)]
pub(crate) struct UnavailableQueueBrokerClient {
    message: String,
}

impl UnavailableQueueBrokerClient {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    fn unavailable(&self, operation: &str) -> QueueError {
        QueueError::Storage(format!(
            "queue broker unavailable during {operation}: {}",
            self.message
        ))
    }
}

#[async_trait]
impl QueueBrokerClient for UnavailableQueueBrokerClient {
    async fn enqueue(
        &self,
        _collection: &str,
        _payload: Value,
    ) -> turbo_vector_queue::Result<EnqueuedJob> {
        Err(self.unavailable("enqueue"))
    }

    async fn claim_batch(
        &self,
        _collection: &str,
        _worker_id: String,
        _max_jobs: usize,
    ) -> turbo_vector_queue::Result<Vec<ClaimedJob>> {
        Err(self.unavailable("claim_batch"))
    }

    async fn ack_batch(
        &self,
        _collection: &str,
        _worker_id: String,
        _job_ids: Vec<String>,
    ) -> turbo_vector_queue::Result<usize> {
        Err(self.unavailable("ack_batch"))
    }

    async fn snapshot(&self, _collection: &str) -> turbo_vector_queue::Result<QueueFile> {
        Err(self.unavailable("snapshot"))
    }
}

#[derive(Clone)]
pub(crate) struct HttpQueueBrokerClient {
    base_url: String,
    client: reqwest::Client,
}

impl HttpQueueBrokerClient {
    pub(crate) fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    fn endpoint(&self, collection: &str, operation: &str) -> String {
        format!(
            "{}{}/{collection}/{operation}",
            self.base_url, BROKER_QUEUE_API_PREFIX
        )
    }

    async fn send<Req, Resp>(
        &self,
        method: Method,
        url: String,
        body: Option<&Req>,
        operation: &str,
    ) -> turbo_vector_queue::Result<Resp>
    where
        Req: Serialize + ?Sized,
        Resp: DeserializeOwned,
    {
        let request = self.client.request(method, url);
        let request = if let Some(payload) = body {
            request.json(payload)
        } else {
            request
        };
        let response = request.send().await.map_err(|error| {
            QueueError::Storage(format!("queue broker {operation} request failed: {error}"))
        })?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let detail = if body.trim().is_empty() {
                String::new()
            } else {
                format!(": {body}")
            };
            return Err(QueueError::Storage(format!(
                "queue broker {operation} returned {status}{detail}"
            )));
        }
        response.json::<Resp>().await.map_err(|error| {
            QueueError::CorruptedState(format!(
                "queue broker {operation} response parse failed: {error}"
            ))
        })
    }
}

#[async_trait]
impl QueueBrokerClient for HttpQueueBrokerClient {
    async fn enqueue(
        &self,
        collection: &str,
        payload: Value,
    ) -> turbo_vector_queue::Result<EnqueuedJob> {
        let response: QueueEnqueueResponse = self
            .send(
                Method::POST,
                self.endpoint(collection, "enqueue"),
                Some(&QueueEnqueueRequest { payload }),
                "enqueue",
            )
            .await?;
        Ok(EnqueuedJob {
            id: response.job_id,
        })
    }

    async fn claim_batch(
        &self,
        collection: &str,
        worker_id: String,
        max_jobs: usize,
    ) -> turbo_vector_queue::Result<Vec<ClaimedJob>> {
        let response: QueueClaimBatchResponse = self
            .send(
                Method::POST,
                self.endpoint(collection, "claim_batch"),
                Some(&QueueClaimBatchRequest {
                    worker_id,
                    max_jobs,
                }),
                "claim_batch",
            )
            .await?;
        Ok(response.jobs.into_iter().map(Into::into).collect())
    }

    async fn ack_batch(
        &self,
        collection: &str,
        worker_id: String,
        job_ids: Vec<String>,
    ) -> turbo_vector_queue::Result<usize> {
        let response: QueueAckBatchResponse = self
            .send(
                Method::POST,
                self.endpoint(collection, "ack_batch"),
                Some(&QueueAckBatchRequest { worker_id, job_ids }),
                "ack_batch",
            )
            .await?;
        Ok(response.acked)
    }

    async fn snapshot(&self, collection: &str) -> turbo_vector_queue::Result<QueueFile> {
        let response: QueueSnapshotResponse = self
            .send(
                Method::GET,
                self.endpoint(collection, "snapshot"),
                Option::<&()>::None,
                "snapshot",
            )
            .await?;
        Ok(response.snapshot)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueEnqueueRequest {
    pub(crate) payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueEnqueueResponse {
    pub(crate) job_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueClaimRequest {
    pub(crate) worker_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueClaimResponse {
    pub(crate) job: Option<QueueClaimedJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueClaimBatchRequest {
    pub(crate) worker_id: String,
    pub(crate) max_jobs: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueClaimBatchResponse {
    pub(crate) jobs: Vec<QueueClaimedJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueClaimedJob {
    pub(crate) id: String,
    pub(crate) payload: Value,
    pub(crate) attempt: u32,
    pub(crate) claimed_at_ms: u64,
}

impl From<ClaimedJob> for QueueClaimedJob {
    fn from(value: ClaimedJob) -> Self {
        Self {
            id: value.id,
            payload: value.payload,
            attempt: value.attempt,
            claimed_at_ms: value.claimed_at_ms,
        }
    }
}

impl From<QueueClaimedJob> for ClaimedJob {
    fn from(value: QueueClaimedJob) -> Self {
        Self {
            id: value.id,
            payload: value.payload,
            attempt: value.attempt,
            claimed_at_ms: value.claimed_at_ms,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueWorkerJobRequest {
    pub(crate) worker_id: String,
    pub(crate) job_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueBoolResponse {
    pub(crate) ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueAckBatchRequest {
    pub(crate) worker_id: String,
    pub(crate) job_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueAckBatchResponse {
    pub(crate) acked: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct QueueSnapshotResponse {
    pub(crate) snapshot: QueueFile,
}
