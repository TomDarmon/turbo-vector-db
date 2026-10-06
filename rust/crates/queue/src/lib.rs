use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::Write as _;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, RwLock};
use tracing::info;

pub type Result<T> = std::result::Result<T, QueueError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum QueueError {
    #[error("storage error: {0}")]
    Storage(String),
    #[error("queue state is corrupted: {0}")]
    CorruptedState(String),
    #[error("queue broker request channel closed")]
    BrokerClosed,
    #[error("queue broker dropped a response")]
    BrokerResponseDropped,
    #[error("CAS contention after {attempts} attempts")]
    CasContention { attempts: usize },
    #[error("internal queue broker mismatch")]
    InternalMismatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedBytes {
    pub bytes: Vec<u8>,
    pub version: String,
}

#[async_trait]
pub trait CasObjectStore: Send + Sync {
    async fn read(&self, key: &str) -> Result<Option<VersionedBytes>>;
    async fn write_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> Result<bool>;
}

#[derive(Debug, Default)]
pub struct InMemoryCasObjectStore {
    objects: RwLock<BTreeMap<String, StoredObject>>,
    write_count: AtomicU64,
}

#[derive(Debug, Clone)]
struct StoredObject {
    bytes: Vec<u8>,
    version: u64,
}

impl InMemoryCasObjectStore {
    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl CasObjectStore for InMemoryCasObjectStore {
    async fn read(&self, key: &str) -> Result<Option<VersionedBytes>> {
        let objects = self.objects.read().await;
        Ok(objects.get(key).map(|stored| VersionedBytes {
            bytes: stored.bytes.clone(),
            version: stored.version.to_string(),
        }))
    }

    async fn write_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> Result<bool> {
        let mut objects = self.objects.write().await;
        let current = objects.get(key).map(|stored| stored.version.to_string());
        let matches = match (current.as_deref(), expected_version) {
            (None, None) => true,
            (Some(current_version), Some(expected)) => current_version == expected,
            _ => false,
        };
        if !matches {
            return Ok(false);
        }

        let next_version = objects
            .get(key)
            .map_or(1, |stored| stored.version.saturating_add(1));
        objects.insert(
            key.to_string(),
            StoredObject {
                bytes: data.to_vec(),
                version: next_version,
            },
        );
        self.write_count.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }
}

#[derive(Debug, Clone)]
pub struct QueueBrokerConfig {
    pub queue_key: String,
    pub broker_id: Option<String>,
    pub lease_timeout_ms: u64,
    pub max_batch_wait: Duration,
    pub min_commit_interval: Duration,
    pub max_cas_retries: usize,
    pub channel_capacity: usize,
}

impl Default for QueueBrokerConfig {
    fn default() -> Self {
        Self {
            queue_key: "queue.json".to_string(),
            broker_id: None,
            lease_timeout_ms: 30_000,
            max_batch_wait: Duration::from_millis(5),
            min_commit_interval: Duration::ZERO,
            max_cas_retries: 16,
            channel_capacity: 1_024,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct QueueFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker: Option<String>,
    #[serde(default)]
    pub jobs: Vec<QueueJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueJob {
    pub id: String,
    pub payload: Value,
    pub enqueued_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease: Option<JobLease>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JobLease {
    pub worker_id: String,
    pub claimed_at_ms: u64,
    pub heartbeat_at_ms: u64,
    pub attempt: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EnqueuedJob {
    pub id: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClaimedJob {
    pub id: String,
    pub payload: Value,
    pub attempt: u32,
    pub claimed_at_ms: u64,
}

#[derive(Clone)]
pub struct QueueHandle {
    tx: mpsc::Sender<BrokerCommand>,
    id_prefix: String,
    id_counter: Arc<AtomicU64>,
}

impl QueueHandle {
    pub async fn enqueue(&self, payload: Value) -> Result<EnqueuedJob> {
        self.enqueue_at(payload, now_ms()).await
    }

    pub async fn enqueue_at(&self, payload: Value, enqueued_at_ms: u64) -> Result<EnqueuedJob> {
        let (responder, receiver) = oneshot::channel();
        let command = BrokerCommand::Enqueue {
            job_id: self.next_job_id(),
            payload,
            enqueued_at_ms,
            responder,
        };
        self.tx
            .send(command)
            .await
            .map_err(|_| QueueError::BrokerClosed)?;
        receiver
            .await
            .map_err(|_| QueueError::BrokerResponseDropped)?
    }

    pub async fn claim(&self, worker_id: impl Into<String>) -> Result<Option<ClaimedJob>> {
        self.claim_at(worker_id, now_ms()).await
    }

    pub async fn claim_at(
        &self,
        worker_id: impl Into<String>,
        now_ms: u64,
    ) -> Result<Option<ClaimedJob>> {
        let (responder, receiver) = oneshot::channel();
        let command = BrokerCommand::Claim {
            worker_id: worker_id.into(),
            now_ms,
            responder,
        };
        self.tx
            .send(command)
            .await
            .map_err(|_| QueueError::BrokerClosed)?;
        receiver
            .await
            .map_err(|_| QueueError::BrokerResponseDropped)?
    }

    pub async fn claim_batch(
        &self,
        worker_id: impl Into<String>,
        max_jobs: usize,
    ) -> Result<Vec<ClaimedJob>> {
        self.claim_batch_at(worker_id, now_ms(), max_jobs).await
    }

    pub async fn claim_batch_at(
        &self,
        worker_id: impl Into<String>,
        now_ms: u64,
        max_jobs: usize,
    ) -> Result<Vec<ClaimedJob>> {
        let (responder, receiver) = oneshot::channel();
        let command = BrokerCommand::ClaimBatch {
            worker_id: worker_id.into(),
            now_ms,
            max_jobs,
            responder,
        };
        self.tx
            .send(command)
            .await
            .map_err(|_| QueueError::BrokerClosed)?;
        receiver
            .await
            .map_err(|_| QueueError::BrokerResponseDropped)?
    }

    pub async fn heartbeat(
        &self,
        worker_id: impl Into<String>,
        job_id: impl Into<String>,
    ) -> Result<bool> {
        self.heartbeat_at(worker_id, job_id, now_ms()).await
    }

    pub async fn heartbeat_at(
        &self,
        worker_id: impl Into<String>,
        job_id: impl Into<String>,
        now_ms: u64,
    ) -> Result<bool> {
        let (responder, receiver) = oneshot::channel();
        let command = BrokerCommand::Heartbeat {
            worker_id: worker_id.into(),
            job_id: job_id.into(),
            now_ms,
            responder,
        };
        self.tx
            .send(command)
            .await
            .map_err(|_| QueueError::BrokerClosed)?;
        receiver
            .await
            .map_err(|_| QueueError::BrokerResponseDropped)?
    }

    pub async fn ack(
        &self,
        worker_id: impl Into<String>,
        job_id: impl Into<String>,
    ) -> Result<bool> {
        let (responder, receiver) = oneshot::channel();
        let command = BrokerCommand::Ack {
            worker_id: worker_id.into(),
            job_id: job_id.into(),
            responder,
        };
        self.tx
            .send(command)
            .await
            .map_err(|_| QueueError::BrokerClosed)?;
        receiver
            .await
            .map_err(|_| QueueError::BrokerResponseDropped)?
    }

    pub async fn ack_batch(
        &self,
        worker_id: impl Into<String>,
        job_ids: Vec<String>,
    ) -> Result<usize> {
        let (responder, receiver) = oneshot::channel();
        let command = BrokerCommand::AckBatch {
            worker_id: worker_id.into(),
            job_ids,
            responder,
        };
        self.tx
            .send(command)
            .await
            .map_err(|_| QueueError::BrokerClosed)?;
        receiver
            .await
            .map_err(|_| QueueError::BrokerResponseDropped)?
    }

    pub async fn snapshot(&self) -> Result<QueueFile> {
        let (responder, receiver) = oneshot::channel();
        let command = BrokerCommand::Snapshot { responder };
        self.tx
            .send(command)
            .await
            .map_err(|_| QueueError::BrokerClosed)?;
        receiver
            .await
            .map_err(|_| QueueError::BrokerResponseDropped)?
    }

    pub async fn requeue_expired(&self, lease_timeout_ms: u64) -> Result<usize> {
        self.requeue_expired_at(now_ms(), lease_timeout_ms).await
    }

    pub async fn requeue_expired_at(&self, now_ms: u64, lease_timeout_ms: u64) -> Result<usize> {
        let (responder, receiver) = oneshot::channel();
        let command = BrokerCommand::RequeueExpired {
            now_ms,
            lease_timeout_ms,
            responder,
        };
        self.tx
            .send(command)
            .await
            .map_err(|_| QueueError::BrokerClosed)?;
        receiver
            .await
            .map_err(|_| QueueError::BrokerResponseDropped)?
    }

    fn next_job_id(&self) -> String {
        let sequence = self.id_counter.fetch_add(1, Ordering::Relaxed);
        format!("{}-{sequence}", self.id_prefix)
    }
}

pub fn spawn_queue_broker<S>(store: Arc<S>, mut config: QueueBrokerConfig) -> QueueHandle
where
    S: CasObjectStore + 'static,
{
    config.max_cas_retries = config.max_cas_retries.max(1);
    config.channel_capacity = config.channel_capacity.max(1);

    let (tx, rx) = mpsc::channel(config.channel_capacity);
    tokio::spawn(run_broker_loop(store, config, rx));

    let id_counter = Arc::new(AtomicU64::new(0));
    let id_prefix = format!(
        "{}-{:x}",
        now_ms(),
        Arc::as_ptr(&id_counter).cast::<()>() as usize
    );
    QueueHandle {
        tx,
        id_prefix,
        id_counter,
    }
}

async fn run_broker_loop<S>(
    store: Arc<S>,
    config: QueueBrokerConfig,
    mut receiver: mpsc::Receiver<BrokerCommand>,
) where
    S: CasObjectStore + 'static,
{
    let mut next_mutating_write_at = tokio::time::Instant::now();
    while let Some(first) = receiver.recv().await {
        let mut batch = vec![first];
        if !config.max_batch_wait.is_zero() {
            tokio::time::sleep(config.max_batch_wait).await;
        }
        while let Ok(command) = receiver.try_recv() {
            batch.push(command);
        }
        process_batch_with_commit_gate(store.as_ref(), &config, batch, &mut next_mutating_write_at)
            .await;
    }
}

async fn process_batch_with_commit_gate<S>(
    store: &S,
    config: &QueueBrokerConfig,
    batch: Vec<BrokerCommand>,
    next_mutating_write_at: &mut tokio::time::Instant,
) where
    S: CasObjectStore,
{
    for _ in 0..config.max_cas_retries {
        let state_and_version = match store.read(&config.queue_key).await {
            Ok(Some(blob)) => {
                let state: QueueFile = match serde_json::from_slice(&blob.bytes) {
                    Ok(parsed) => parsed,
                    Err(e) => {
                        send_batch_error(batch, QueueError::CorruptedState(e.to_string()));
                        return;
                    }
                };
                (state, Some(blob.version))
            }
            Ok(None) => (QueueFile::default(), None),
            Err(e) => {
                send_batch_error(batch, e);
                return;
            }
        };

        let (mut state, version) = state_and_version;
        let mut mutated = false;
        if let Some(broker_id) = config.broker_id.as_ref() {
            if state.broker.as_deref() != Some(broker_id) {
                state.broker = Some(broker_id.clone());
                mutated = true;
            }
        }

        let (ops_mutated, outputs) = apply_commands(&mut state, batch.as_slice(), config);
        mutated |= ops_mutated;

        if !mutated {
            send_batch_outputs(batch, outputs);
            return;
        }

        let serialized = match serde_json::to_vec(&state) {
            Ok(bytes) => bytes,
            Err(e) => {
                send_batch_error(batch, QueueError::CorruptedState(e.to_string()));
                return;
            }
        };

        throttle_mutating_commit(config, next_mutating_write_at).await;

        let wrote = match store
            .write_cas(&config.queue_key, &serialized, version.as_deref())
            .await
        {
            Ok(value) => value,
            Err(e) => {
                send_batch_error(batch, e);
                return;
            }
        };
        if wrote {
            let mut summary = String::new();
            let mut enqueues = 0u32;
            let mut claims = 0u32;
            let mut acks = 0u32;
            for output in &outputs {
                match output {
                    CommandOutput::Enqueue(_) => enqueues += 1,
                    CommandOutput::Claim(Some(_)) => claims += 1,
                    CommandOutput::ClaimBatch(jobs) => claims += jobs.len() as u32,
                    CommandOutput::Ack(true) => acks += 1,
                    CommandOutput::AckBatch(n) => acks += *n as u32,
                    _ => {}
                }
            }
            if enqueues > 0 { let _ = write!(summary, "enqueued={enqueues} "); }
            if claims > 0 { let _ = write!(summary, "claimed={claims} "); }
            if acks > 0 { let _ = write!(summary, "acked={acks} "); }
            if !summary.is_empty() {
                info!(
                    queue_key = %config.queue_key,
                    jobs_in_file = state.jobs.len(),
                    "broker queue CAS write: {summary}"
                );
            }
            send_batch_outputs(batch, outputs);
            return;
        }
    }

    send_batch_error(
        batch,
        QueueError::CasContention {
            attempts: config.max_cas_retries,
        },
    );
}

async fn throttle_mutating_commit(
    config: &QueueBrokerConfig,
    next_mutating_write_at: &mut tokio::time::Instant,
) {
    if config.min_commit_interval.is_zero() {
        return;
    }
    tokio::time::sleep_until(*next_mutating_write_at).await;
    *next_mutating_write_at = tokio::time::Instant::now() + config.min_commit_interval;
}

fn apply_commands(
    state: &mut QueueFile,
    commands: &[BrokerCommand],
    config: &QueueBrokerConfig,
) -> (bool, Vec<CommandOutput>) {
    let mut mutated = false;
    let mut outputs = Vec::with_capacity(commands.len());
    for command in commands {
        match command {
            BrokerCommand::Enqueue {
                job_id,
                payload,
                enqueued_at_ms,
                ..
            } => {
                state.jobs.push(QueueJob {
                    id: job_id.clone(),
                    payload: payload.clone(),
                    enqueued_at_ms: *enqueued_at_ms,
                    lease: None,
                });
                mutated = true;
                outputs.push(CommandOutput::Enqueue(EnqueuedJob { id: job_id.clone() }));
            }
            BrokerCommand::Claim {
                worker_id, now_ms, ..
            } => {
                let claimed = claim_next_job(state, worker_id, *now_ms, config.lease_timeout_ms);
                mutated |= claimed.is_some();
                outputs.push(CommandOutput::Claim(claimed));
            }
            BrokerCommand::ClaimBatch {
                worker_id,
                now_ms,
                max_jobs,
                ..
            } => {
                let (claimed, changed) = claim_batch_jobs(
                    state,
                    worker_id,
                    *now_ms,
                    *max_jobs,
                    config.lease_timeout_ms,
                );
                mutated |= changed;
                outputs.push(CommandOutput::ClaimBatch(claimed));
            }
            BrokerCommand::Heartbeat {
                worker_id,
                job_id,
                now_ms,
                ..
            } => {
                let (accepted, changed) = heartbeat_job(state, worker_id, job_id, *now_ms);
                mutated |= changed;
                outputs.push(CommandOutput::Heartbeat(accepted));
            }
            BrokerCommand::Ack {
                worker_id, job_id, ..
            } => {
                let removed = ack_job(state, worker_id, job_id);
                mutated |= removed;
                outputs.push(CommandOutput::Ack(removed));
            }
            BrokerCommand::AckBatch {
                worker_id, job_ids, ..
            } => {
                let acked = ack_jobs(state, worker_id, job_ids);
                mutated |= acked > 0;
                outputs.push(CommandOutput::AckBatch(acked));
            }
            BrokerCommand::Snapshot { .. } => {
                outputs.push(CommandOutput::Snapshot(state.clone()));
            }
            BrokerCommand::RequeueExpired {
                now_ms,
                lease_timeout_ms,
                ..
            } => {
                let released = requeue_expired_leases(state, *now_ms, *lease_timeout_ms);
                mutated |= released > 0;
                outputs.push(CommandOutput::RequeueExpired(released));
            }
        }
    }
    (mutated, outputs)
}

fn claim_next_job(
    state: &mut QueueFile,
    worker_id: &str,
    now_ms: u64,
    lease_timeout_ms: u64,
) -> Option<ClaimedJob> {
    for job in &mut state.jobs {
        let claimable = match job.lease.as_ref() {
            None => true,
            Some(lease) => now_ms.saturating_sub(lease.heartbeat_at_ms) > lease_timeout_ms,
        };
        if !claimable {
            continue;
        }

        let attempt = job
            .lease
            .as_ref()
            .map(|lease| lease.attempt.saturating_add(1))
            .unwrap_or(1);
        job.lease = Some(JobLease {
            worker_id: worker_id.to_string(),
            claimed_at_ms: now_ms,
            heartbeat_at_ms: now_ms,
            attempt,
        });
        return Some(ClaimedJob {
            id: job.id.clone(),
            payload: job.payload.clone(),
            attempt,
            claimed_at_ms: now_ms,
        });
    }
    None
}

fn claim_batch_jobs(
    state: &mut QueueFile,
    worker_id: &str,
    now_ms: u64,
    max_jobs: usize,
    lease_timeout_ms: u64,
) -> (Vec<ClaimedJob>, bool) {
    if max_jobs == 0 {
        return (Vec::new(), false);
    }

    let mut claimed = Vec::new();
    let mut changed = false;
    for job in &mut state.jobs {
        if claimed.len() >= max_jobs {
            break;
        }

        match job.lease.as_ref() {
            Some(lease) => {
                let stale = now_ms.saturating_sub(lease.heartbeat_at_ms) > lease_timeout_ms;
                if !stale && lease.worker_id != worker_id {
                    break;
                }
                if stale {
                    let attempt = lease.attempt.saturating_add(1);
                    job.lease = Some(JobLease {
                        worker_id: worker_id.to_string(),
                        claimed_at_ms: now_ms,
                        heartbeat_at_ms: now_ms,
                        attempt,
                    });
                    claimed.push(ClaimedJob {
                        id: job.id.clone(),
                        payload: job.payload.clone(),
                        attempt,
                        claimed_at_ms: now_ms,
                    });
                    changed = true;
                } else {
                    claimed.push(ClaimedJob {
                        id: job.id.clone(),
                        payload: job.payload.clone(),
                        attempt: lease.attempt,
                        claimed_at_ms: lease.claimed_at_ms,
                    });
                }
            }
            None => {
                job.lease = Some(JobLease {
                    worker_id: worker_id.to_string(),
                    claimed_at_ms: now_ms,
                    heartbeat_at_ms: now_ms,
                    attempt: 1,
                });
                claimed.push(ClaimedJob {
                    id: job.id.clone(),
                    payload: job.payload.clone(),
                    attempt: 1,
                    claimed_at_ms: now_ms,
                });
                changed = true;
            }
        }
    }

    (claimed, changed)
}

fn heartbeat_job(
    state: &mut QueueFile,
    worker_id: &str,
    job_id: &str,
    now_ms: u64,
) -> (bool, bool) {
    for job in &mut state.jobs {
        if job.id != job_id {
            continue;
        }
        let Some(lease) = job.lease.as_mut() else {
            return (false, false);
        };
        if lease.worker_id != worker_id {
            return (false, false);
        }
        let changed = lease.heartbeat_at_ms != now_ms;
        lease.heartbeat_at_ms = now_ms;
        return (true, changed);
    }
    (false, false)
}

fn ack_job(state: &mut QueueFile, worker_id: &str, job_id: &str) -> bool {
    if let Some(index) = state.jobs.iter().position(|job| {
        job.id == job_id
            && job
                .lease
                .as_ref()
                .is_some_and(|lease| lease.worker_id == worker_id)
    }) {
        state.jobs.remove(index);
        return true;
    }
    false
}

fn ack_jobs(state: &mut QueueFile, worker_id: &str, job_ids: &[String]) -> usize {
    if job_ids.is_empty() {
        return 0;
    }

    let wanted: std::collections::BTreeSet<&str> = job_ids.iter().map(String::as_str).collect();
    let before = state.jobs.len();
    state.jobs.retain(|job| {
        if !wanted.contains(job.id.as_str()) {
            return true;
        }
        job.lease
            .as_ref()
            .is_none_or(|lease| lease.worker_id != worker_id)
    });
    before.saturating_sub(state.jobs.len())
}

fn requeue_expired_leases(state: &mut QueueFile, now_ms: u64, lease_timeout_ms: u64) -> usize {
    let mut released = 0usize;
    for job in &mut state.jobs {
        let Some(lease) = job.lease.as_ref() else {
            continue;
        };
        if now_ms.saturating_sub(lease.heartbeat_at_ms) > lease_timeout_ms {
            job.lease = None;
            released += 1;
        }
    }
    released
}

enum BrokerCommand {
    Enqueue {
        job_id: String,
        payload: Value,
        enqueued_at_ms: u64,
        responder: oneshot::Sender<Result<EnqueuedJob>>,
    },
    Claim {
        worker_id: String,
        now_ms: u64,
        responder: oneshot::Sender<Result<Option<ClaimedJob>>>,
    },
    ClaimBatch {
        worker_id: String,
        now_ms: u64,
        max_jobs: usize,
        responder: oneshot::Sender<Result<Vec<ClaimedJob>>>,
    },
    Heartbeat {
        worker_id: String,
        job_id: String,
        now_ms: u64,
        responder: oneshot::Sender<Result<bool>>,
    },
    Ack {
        worker_id: String,
        job_id: String,
        responder: oneshot::Sender<Result<bool>>,
    },
    AckBatch {
        worker_id: String,
        job_ids: Vec<String>,
        responder: oneshot::Sender<Result<usize>>,
    },
    Snapshot {
        responder: oneshot::Sender<Result<QueueFile>>,
    },
    RequeueExpired {
        now_ms: u64,
        lease_timeout_ms: u64,
        responder: oneshot::Sender<Result<usize>>,
    },
}

enum CommandOutput {
    Enqueue(EnqueuedJob),
    Claim(Option<ClaimedJob>),
    ClaimBatch(Vec<ClaimedJob>),
    Heartbeat(bool),
    Ack(bool),
    AckBatch(usize),
    Snapshot(QueueFile),
    RequeueExpired(usize),
}

fn send_batch_outputs(batch: Vec<BrokerCommand>, outputs: Vec<CommandOutput>) {
    if batch.len() != outputs.len() {
        send_batch_error(batch, QueueError::InternalMismatch);
        return;
    }

    for (command, output) in batch.into_iter().zip(outputs.into_iter()) {
        match (command, output) {
            (BrokerCommand::Enqueue { responder, .. }, CommandOutput::Enqueue(result)) => {
                let _ = responder.send(Ok(result));
            }
            (BrokerCommand::Claim { responder, .. }, CommandOutput::Claim(result)) => {
                let _ = responder.send(Ok(result));
            }
            (BrokerCommand::ClaimBatch { responder, .. }, CommandOutput::ClaimBatch(result)) => {
                let _ = responder.send(Ok(result));
            }
            (BrokerCommand::Heartbeat { responder, .. }, CommandOutput::Heartbeat(result)) => {
                let _ = responder.send(Ok(result));
            }
            (BrokerCommand::Ack { responder, .. }, CommandOutput::Ack(result)) => {
                let _ = responder.send(Ok(result));
            }
            (BrokerCommand::AckBatch { responder, .. }, CommandOutput::AckBatch(result)) => {
                let _ = responder.send(Ok(result));
            }
            (BrokerCommand::Snapshot { responder }, CommandOutput::Snapshot(result)) => {
                let _ = responder.send(Ok(result));
            }
            (
                BrokerCommand::RequeueExpired { responder, .. },
                CommandOutput::RequeueExpired(result),
            ) => {
                let _ = responder.send(Ok(result));
            }
            (mismatch_command, _) => {
                send_batch_error(vec![mismatch_command], QueueError::InternalMismatch);
            }
        }
    }
}

fn send_batch_error(batch: Vec<BrokerCommand>, error: QueueError) {
    for command in batch {
        match command {
            BrokerCommand::Enqueue { responder, .. } => {
                let _ = responder.send(Err(error.clone()));
            }
            BrokerCommand::Claim { responder, .. } => {
                let _ = responder.send(Err(error.clone()));
            }
            BrokerCommand::ClaimBatch { responder, .. } => {
                let _ = responder.send(Err(error.clone()));
            }
            BrokerCommand::Heartbeat { responder, .. } => {
                let _ = responder.send(Err(error.clone()));
            }
            BrokerCommand::Ack { responder, .. } => {
                let _ = responder.send(Err(error.clone()));
            }
            BrokerCommand::AckBatch { responder, .. } => {
                let _ = responder.send(Err(error.clone()));
            }
            BrokerCommand::Snapshot { responder } => {
                let _ = responder.send(Err(error.clone()));
            }
            BrokerCommand::RequeueExpired { responder, .. } => {
                let _ = responder.send(Err(error.clone()));
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests;
