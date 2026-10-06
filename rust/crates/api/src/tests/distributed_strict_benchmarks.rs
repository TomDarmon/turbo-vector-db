use axum::http::Method;
use serde_json::json;
use std::time::Instant;

use crate::distributed::shard_for_vector_id;

use super::support::TestApi;

#[derive(Debug, Clone)]
struct DistributedProfileResult {
    name: &'static str,
    throughput_qps: f64,
    p99_ms: f64,
    degraded_rate: f64,
}

fn percentile_ms(mut values: Vec<f64>, percentile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let rank = ((values.len() as f64) * percentile).ceil() as usize;
    values[rank.saturating_sub(1).min(values.len().saturating_sub(1))]
}

async fn seed_collection(api: &TestApi, collection: &str, namespace: &str, vectors: usize) {
    api.create_collection(collection, 4, "dot").await;
    let payload_vectors = (0..vectors)
        .map(|index| {
            json!({
                "id": format!("doc-{index:05}"),
                "values": [1.0 - ((index as f32) / 10_000.0), 0.5, 0.0, 0.0],
                "metadata": {
                    "bucket": if index % 2 == 0 { "a" } else { "b" }
                }
            })
        })
        .collect::<Vec<_>>();
    let (status, body) = api
        .upsert_and_wait_applied(
            collection,
            json!({
                "vectors": payload_vectors,
                "namespace": namespace
            }),
            &[],
        )
        .await;
    assert_eq!(status, axum::http::StatusCode::OK, "seed failed: {body:?}");
}

async fn seed_skewed_collection(
    api: &TestApi,
    collection: &str,
    namespace: &str,
    vectors: usize,
    target_shard: u32,
    shard_count: usize,
) {
    api.create_collection(collection, 4, "dot").await;
    let mut payload_vectors = Vec::with_capacity(vectors);
    let mut cursor = 0_u64;
    while payload_vectors.len() < vectors {
        let id = format!("skew-{cursor:08}");
        let shard = shard_for_vector_id(collection, namespace, &id, shard_count);
        cursor = cursor.saturating_add(1);
        if shard != target_shard {
            continue;
        }
        payload_vectors.push(json!({
            "id": id,
            "values": [1.0, 0.25, 0.0, 0.0],
            "metadata": {"bucket": "skew"}
        }));
    }
    let (status, body) = api
        .upsert_and_wait_applied(
            collection,
            json!({
                "vectors": payload_vectors,
                "namespace": namespace
            }),
            &[],
        )
        .await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "skew seed failed: {body:?}"
    );
}

async fn run_query_profile(
    api: &TestApi,
    collection: &str,
    namespace: &str,
    query_count: usize,
    top_k: u32,
) -> DistributedProfileResult {
    let mut latencies_ms = Vec::with_capacity(query_count);
    let mut degraded_count = 0_u64;
    let profile_started = Instant::now();
    for _ in 0..query_count {
        let query_started = Instant::now();
        let (status, body) = api
            .query(
                collection,
                json!({
                    "vector": [1.0, 0.5, 0.0, 0.0],
                    "top_k": top_k,
                    "namespace": namespace,
                    "search_strategy": "exact"
                }),
            )
            .await;
        assert_eq!(
            status,
            axum::http::StatusCode::OK,
            "profile query failed: {body:?}"
        );
        latencies_ms.push(query_started.elapsed().as_secs_f64() * 1_000.0);
        if body["distributed"]["degraded"].as_bool().unwrap_or(false) {
            degraded_count = degraded_count.saturating_add(1);
        }
    }
    let elapsed = profile_started.elapsed().as_secs_f64();
    let throughput_qps = if elapsed > 0.0 {
        query_count as f64 / elapsed
    } else {
        0.0
    };
    let p95_ms = percentile_ms(latencies_ms.clone(), 0.95);
    let p99_ms = percentile_ms(latencies_ms.clone(), 0.99);
    DistributedProfileResult {
        name: "profile",
        throughput_qps,
        p99_ms,
        degraded_rate: degraded_count as f64 / query_count.max(1) as f64,
    }
}

#[tokio::test]
async fn distributed_scale_profiles_meet_strict_gates() {
    let baseline_api = TestApi::new_with_distributed_sharding(1, 250, true, 1);
    seed_collection(&baseline_api, "dist_profile_baseline", "tenant_a", 512).await;
    let mut baseline =
        run_query_profile(&baseline_api, "dist_profile_baseline", "tenant_a", 60, 10).await;
    baseline.name = "1_node_baseline";

    let fanout_api = TestApi::new_with_distributed_sharding(4, 250, true, 3);
    seed_collection(&fanout_api, "dist_profile_fanout", "tenant_a", 512).await;
    let mut fanout =
        run_query_profile(&fanout_api, "dist_profile_fanout", "tenant_a", 60, 10).await;
    fanout.name = "4_shard_fanout";

    let skew_api = TestApi::new_with_distributed_sharding(4, 250, true, 3);
    seed_skewed_collection(&skew_api, "dist_profile_skew", "tenant_a", 512, 0, 4).await;
    let mut skew = run_query_profile(&skew_api, "dist_profile_skew", "tenant_a", 60, 10).await;
    skew.name = "shard_skew";

    let node_loss_api = TestApi::new_with_distributed_sharding(4, 250, true, 3);
    seed_collection(&node_loss_api, "dist_profile_node_loss", "tenant_a", 512).await;
    let (status, placement) = node_loss_api
        .request(
            Method::GET,
            "/v1/collections/dist_profile_node_loss/shards/placement",
            None,
            &[],
        )
        .await;
    assert_eq!(status, axum::http::StatusCode::OK);
    let version = placement["version"].as_u64().expect("placement version");
    let mut assignments = placement["assignments"]
        .as_array()
        .expect("assignments")
        .to_vec();
    assignments[0]["state"] = json!("offline");
    let (status, update_response) = node_loss_api
        .request(
            Method::PUT,
            "/v1/collections/dist_profile_node_loss/shards/placement",
            Some(json!({
                "shard_count": 4,
                "expected_version": version,
                "assignments": assignments
            })),
            &[],
        )
        .await;
    assert_eq!(
        status,
        axum::http::StatusCode::OK,
        "failed to apply node-loss placement: {update_response:?}"
    );
    let mut node_loss =
        run_query_profile(&node_loss_api, "dist_profile_node_loss", "tenant_a", 60, 10).await;
    node_loss.name = "node_loss";

    let scaling_efficiency = if baseline.throughput_qps > 0.0 {
        fanout.throughput_qps / baseline.throughput_qps
    } else {
        0.0
    };
    let node_loss_p99_regression = if fanout.p99_ms > 0.0 {
        (node_loss.p99_ms - fanout.p99_ms) / fanout.p99_ms
    } else {
        0.0
    };

    println!(
        "distributed profile report:\n  baseline={:?}\n  fanout={:?}\n  skew={:?}\n  node_loss={:?}\n  scaling_efficiency={:.4}\n  node_loss_p99_regression={:.4}",
        baseline, fanout, skew, node_loss, scaling_efficiency, node_loss_p99_regression
    );

    assert!(
        scaling_efficiency >= 0.60,
        "scaling efficiency gate failed: {:.4} < 0.60",
        scaling_efficiency
    );
    assert!(
        node_loss_p99_regression <= 0.25,
        "node-loss p99 regression gate failed: {:.4} > 0.25",
        node_loss_p99_regression
    );
    assert!(
        node_loss.degraded_rate > 0.0,
        "node-loss profile should expose degraded-mode behavior"
    );
}
