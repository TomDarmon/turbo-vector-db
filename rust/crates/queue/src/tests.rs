use super::*;
use async_trait::async_trait;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, task::JoinSet};

#[derive(Debug, Default)]
struct TrackingCasObjectStore {
    inner: InMemoryCasObjectStore,
    successful_writes: Mutex<Vec<Instant>>,
}

impl TrackingCasObjectStore {
    async fn successful_writes(&self) -> Vec<Instant> {
        self.successful_writes.lock().await.clone()
    }
}

#[async_trait]
impl CasObjectStore for TrackingCasObjectStore {
    async fn read(&self, key: &str) -> Result<Option<VersionedBytes>> {
        self.inner.read(key).await
    }

    async fn write_cas(
        &self,
        key: &str,
        data: &[u8],
        expected_version: Option<&str>,
    ) -> Result<bool> {
        let wrote = self.inner.write_cas(key, data, expected_version).await?;
        if wrote {
            self.successful_writes.lock().await.push(Instant::now());
        }
        Ok(wrote)
    }
}

#[tokio::test]
async fn broker_throttles_mutating_commits_per_queue_object() {
    let store = Arc::new(TrackingCasObjectStore::default());
    let queue = spawn_queue_broker(
        store.clone(),
        QueueBrokerConfig {
            queue_key: "queue.json".to_string(),
            lease_timeout_ms: 1_000,
            max_batch_wait: Duration::ZERO,
            min_commit_interval: Duration::from_millis(60),
            max_cas_retries: 16,
            channel_capacity: 128,
            ..QueueBrokerConfig::default()
        },
    );

    let enqueued = queue
        .enqueue(json!({"op":"index"}))
        .await
        .expect("enqueue should succeed");
    let claimed = queue
        .claim("worker-a")
        .await
        .expect("claim should succeed")
        .expect("job should be claimable");
    assert_eq!(claimed.id, enqueued.id);
    assert!(queue
        .ack("worker-a", &enqueued.id)
        .await
        .expect("ack should succeed"));

    let writes = store.successful_writes().await;
    assert_eq!(writes.len(), 3, "expected enqueue/claim/ack commits");
    for window in writes.windows(2) {
        let delta = window[1].duration_since(window[0]);
        assert!(
            delta >= Duration::from_millis(50),
            "commit spacing should be throttled (delta={delta:?})"
        );
    }
}

#[tokio::test]
async fn queue_reclaims_stale_claims_after_heartbeat_timeout() {
    let store = Arc::new(InMemoryCasObjectStore::default());
    let queue = spawn_queue_broker(
        store.clone(),
        QueueBrokerConfig {
            queue_key: "queues/indexing/queue.json".to_string(),
            broker_id: Some("10.0.0.42:3000".to_string()),
            lease_timeout_ms: 1_000,
            max_batch_wait: Duration::from_millis(1),
            min_commit_interval: Duration::ZERO,
            max_cas_retries: 16,
            channel_capacity: 128,
        },
    );

    let first = queue
        .enqueue_at(json!({"op":"index","ns":"alpha"}), 10)
        .await
        .expect("enqueue first");
    let second = queue
        .enqueue_at(json!({"op":"index","ns":"beta"}), 11)
        .await
        .expect("enqueue second");
    assert_ne!(first.id, second.id);

    let claimed_first = queue
        .claim_at("worker-a", 20)
        .await
        .expect("claim first")
        .expect("first job should be claimable");
    assert_eq!(claimed_first.id, first.id);
    assert_eq!(claimed_first.attempt, 1);

    let claimed_second = queue
        .claim_at("worker-b", 30)
        .await
        .expect("claim second")
        .expect("second job should be claimable");
    assert_eq!(claimed_second.id, second.id);
    assert_eq!(claimed_second.attempt, 1);

    assert!(queue
        .heartbeat_at("worker-a", &first.id, 900)
        .await
        .expect("heartbeat first"));
    assert_eq!(
        queue
            .claim_at("worker-c", 950)
            .await
            .expect("claim with no stale jobs"),
        None
    );

    assert!(queue.ack("worker-a", &first.id).await.expect("ack first"));
    assert!(!queue
        .heartbeat_at("worker-b", &first.id, 1_200)
        .await
        .expect("heartbeat removed first"));

    let reclaimed = queue
        .claim_at("worker-c", 1_500)
        .await
        .expect("claim stale second")
        .expect("second should be reclaimed");
    assert_eq!(reclaimed.id, second.id);
    assert_eq!(reclaimed.attempt, 2);
    assert_eq!(reclaimed.payload["ns"], "beta");

    assert!(!queue
        .ack("worker-b", &second.id)
        .await
        .expect("stale worker cannot ack"));
    assert!(queue
        .ack("worker-c", &second.id)
        .await
        .expect("ack reclaimed"));

    let snapshot = queue.snapshot().await.expect("queue snapshot");
    assert_eq!(snapshot.broker.as_deref(), Some("10.0.0.42:3000"));
    assert!(snapshot.jobs.is_empty());
}

#[tokio::test]
async fn broker_group_commits_concurrent_enqueue_bursts() {
    let store = Arc::new(InMemoryCasObjectStore::default());
    let queue = spawn_queue_broker(
        store.clone(),
        QueueBrokerConfig {
            queue_key: "queue.json".to_string(),
            broker_id: None,
            lease_timeout_ms: 1_000,
            max_batch_wait: Duration::from_millis(25),
            min_commit_interval: Duration::ZERO,
            max_cas_retries: 16,
            channel_capacity: 1_024,
        },
    );

    let total_jobs = 24usize;
    let mut join_set = JoinSet::new();
    for index in 0..total_jobs {
        let handle = queue.clone();
        join_set.spawn(async move {
            handle
                .enqueue(json!({"job": index}))
                .await
                .expect("enqueue burst")
        });
    }

    let mut seen = BTreeMap::new();
    while let Some(join_result) = join_set.join_next().await {
        let job = join_result.expect("join task");
        seen.insert(job.id, true);
    }
    assert_eq!(seen.len(), total_jobs);

    let snapshot = queue.snapshot().await.expect("snapshot after burst");
    assert_eq!(snapshot.jobs.len(), total_jobs);
    assert!(
        store.write_count() < total_jobs as u64,
        "expected group commit writes < request count"
    );
}

#[tokio::test]
async fn broker_can_requeue_expired_leases_without_claim_traffic() {
    let store = Arc::new(InMemoryCasObjectStore::default());
    let queue = spawn_queue_broker(
        store,
        QueueBrokerConfig {
            queue_key: "queue.json".to_string(),
            lease_timeout_ms: 1_000,
            ..QueueBrokerConfig::default()
        },
    );

    let enqueued = queue
        .enqueue_at(json!({"op":"index"}), 10)
        .await
        .expect("enqueue should succeed");
    let claimed = queue
        .claim_at("worker-a", 20)
        .await
        .expect("claim should succeed")
        .expect("job should exist");
    assert_eq!(claimed.id, enqueued.id);

    let released = queue
        .requeue_expired_at(2_000, 1_000)
        .await
        .expect("requeue scan should succeed");
    assert_eq!(released, 1);

    let reclaimed = queue
        .claim_at("worker-b", 2_001)
        .await
        .expect("claim after requeue should succeed")
        .expect("job should be claimable again");
    assert_eq!(reclaimed.id, enqueued.id);
}

#[tokio::test]
async fn claim_and_ack_batch_preserve_prefix_ownership() {
    let store = Arc::new(InMemoryCasObjectStore::default());
    let queue = spawn_queue_broker(
        store,
        QueueBrokerConfig {
            queue_key: "queue.json".to_string(),
            lease_timeout_ms: 1_000,
            max_batch_wait: Duration::from_millis(1),
            ..QueueBrokerConfig::default()
        },
    );

    let first = queue
        .enqueue_at(json!({"op":"index","ns":"alpha"}), 10)
        .await
        .expect("enqueue first");
    let second = queue
        .enqueue_at(json!({"op":"index","ns":"beta"}), 11)
        .await
        .expect("enqueue second");
    let third = queue
        .enqueue_at(json!({"op":"index","ns":"gamma"}), 12)
        .await
        .expect("enqueue third");

    let claimed_first = queue
        .claim_at("worker-a", 20)
        .await
        .expect("claim first")
        .expect("first should be claimable");
    assert_eq!(claimed_first.id, first.id);

    let blocked = queue
        .claim_batch_at("worker-b", 30, 3)
        .await
        .expect("claim batch with blocked head");
    assert!(
        blocked.is_empty(),
        "another worker must not claim behind a healthy leased head"
    );

    let owned_prefix = queue
        .claim_batch_at("worker-a", 31, 3)
        .await
        .expect("claim owned prefix");
    assert_eq!(owned_prefix.len(), 3);
    assert_eq!(owned_prefix[0].id, first.id);
    assert_eq!(owned_prefix[1].id, second.id);
    assert_eq!(owned_prefix[2].id, third.id);

    let acked = queue
        .ack_batch(
            "worker-a",
            vec![first.id.clone(), second.id.clone(), third.id.clone()],
        )
        .await
        .expect("ack batch");
    assert_eq!(acked, 3);

    let snapshot = queue.snapshot().await.expect("snapshot after ack batch");
    assert!(snapshot.jobs.is_empty());
}
