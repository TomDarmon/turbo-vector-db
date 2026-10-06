use axum::http::StatusCode;
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc, time::Instant};
use turbo_vector_storage::ObjectStore;

use super::support::{InMemoryObjectStore, TestApi};

const ANN_STRICT_COLLECTION: &str = "ann_v3_strict_profiles";
const ANN_STRICT_NAMESPACE: &str = "ns_a";
const ANN_STRICT_DIMENSION: u32 = 8;
const ANN_STRICT_TOTAL_VECTORS: u32 = 20_800;
const ANN_STRICT_TOP_K: u64 = 3;
const ANN_STRICT_OBJECT_READ_BUDGET: usize = 512;
const ANN_STRICT_ROOT_BEAM: usize = 32;
const ANN_STRICT_LEAF_PROBES: usize = 256;

fn profile_vector_values(index: u32) -> (Vec<f32>, bool) {
    let phase = index as f32 / ANN_STRICT_TOTAL_VECTORS as f32;
    let band = (index % 257) as f32 / 257.0;
    let bucket = (index % 128) as f32 / 128.0;
    let centered_phase = phase * 2.0 - 1.0;
    let centered_bucket = bucket * 2.0 - 1.0;
    let cohort_b = index % 61 == 0;
    let values = vec![
        centered_phase,
        centered_bucket,
        centered_phase * centered_bucket,
        centered_phase * 0.5 - centered_bucket * 0.3,
        centered_bucket * 0.7 + 0.1,
        (centered_phase - centered_bucket) * 0.6,
        (centered_phase + centered_bucket) * 0.3 + band * 0.1 - 0.05,
        0.25 - phase,
    ];
    (values, cohort_b)
}

fn build_profile_vectors(start: u32, end: u32) -> Vec<Value> {
    let mut vectors = Vec::with_capacity((end.saturating_sub(start)) as usize);
    for index in start..end {
        let (values, cohort_b) = profile_vector_values(index);
        let strict_group = if index % 331 == 0 { "needle" } else { "other" };
        vectors.push(json!({
            "id": format!("doc-{index:05}"),
            "values": values,
            "metadata": {
                "cohort": if cohort_b { "b" } else { "a" },
                "strict_group": strict_group
            }
        }));
    }
    vectors
}

fn cohort_b_query_vector(seed: u32) -> Vec<f32> {
    let cohort_slots = (ANN_STRICT_TOTAL_VECTORS / 61).max(1);
    let cohort_index = (seed % cohort_slots).saturating_mul(61);
    profile_vector_values(cohort_index).0
}

fn long_tail_query_vector(seed: u32) -> Vec<f32> {
    let _ = seed;
    cohort_b_query_vector(5)
}

fn strict_filter_query_vector(seed: u32) -> Vec<f32> {
    let needle_slots = (ANN_STRICT_TOTAL_VECTORS / 331).max(1);
    let needle_index = (seed % needle_slots).saturating_mul(331);
    profile_vector_values(needle_index).0
}

fn p95_ms(samples: &mut [f64]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|left, right| left.total_cmp(right));
    let index = ((samples.len() - 1) as f64 * 0.95).round() as usize;
    samples[index.min(samples.len() - 1)]
}

fn median(values: &mut [f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.sort_by(|left, right| left.total_cmp(right));
    let mid = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn match_ids(response: &Value) -> Vec<String> {
    response["matches"]
        .as_array()
        .expect("matches array")
        .iter()
        .filter_map(|entry| entry["id"].as_str())
        .map(ToString::to_string)
        .collect()
}

fn recall_at_k(ann_response: &Value, exact_response: &Value) -> f64 {
    let ann_ids = match_ids(ann_response).into_iter().collect::<BTreeSet<_>>();
    let exact_ids = match_ids(exact_response);
    let k = exact_ids.len().max(1);
    let hits = exact_ids
        .iter()
        .filter(|exact_id| ann_ids.contains(*exact_id))
        .count();
    hits as f64 / k as f64
}

fn ann_total_object_reads(response: &Value) -> u64 {
    response["ann"]["ann_meta_object_reads"]
        .as_u64()
        .unwrap_or(0)
        + response["ann"]["ann_bucket_object_reads"]
            .as_u64()
            .unwrap_or(0)
        + response["ann"]["ann_filter_cluster_object_reads"]
            .as_u64()
            .unwrap_or(0)
        + response["ann"]["ann_filter_row_object_reads"]
            .as_u64()
            .unwrap_or(0)
        + response["ann"]["rerank_segment_object_reads"]
            .as_u64()
            .unwrap_or(0)
}

fn assert_ann_query_success(response: &Value) {
    assert_eq!(
        response["ann"]["ann_used"].as_bool().unwrap_or(false),
        true,
        "ANN strict profile query must use ANN path: {response:?}"
    );
    assert_eq!(
        response["ann"]["ann_fallback_count"].as_u64().unwrap_or(0),
        0,
        "ANN strict profile query must not fallback: {response:?}"
    );
    assert_eq!(
        response["ann"]["ann_fetch_errors"].as_u64().unwrap_or(0),
        0,
        "ANN strict profile query must not report fetch errors: {response:?}"
    );
    assert_eq!(
        response["ann"]["object_read_budget_exceeded"]
            .as_bool()
            .unwrap_or(false),
        false,
        "ANN strict profile query must remain within read budget: {response:?}"
    );
    let configured_budget = response["ann"]["object_read_budget"].as_u64().unwrap_or(0);
    let observed_reads = ann_total_object_reads(response);
    if configured_budget > 0 {
        assert!(
            observed_reads <= configured_budget,
            "ANN strict profile query exceeded configured read budget (reads={observed_reads}, budget={configured_budget})"
        );
    }
}

#[tokio::test]
async fn ann_v3_strict_profiles_cover_warm_cold_selective_filter_and_long_tail() {
    let backing_store = Arc::new(InMemoryObjectStore::default());
    let build_storage = backing_store.clone() as Arc<dyn ObjectStore>;
    let build_api = TestApi::new_with_storage_and_ann_tuning(
        build_storage,
        backing_store.clone(),
        ANN_STRICT_OBJECT_READ_BUDGET,
        ANN_STRICT_ROOT_BEAM,
        ANN_STRICT_LEAF_PROBES,
    );
    build_api
        .create_collection(ANN_STRICT_COLLECTION, ANN_STRICT_DIMENSION, "cosine")
        .await;
    let batch_size = 1_600_u32;
    for start in (0..ANN_STRICT_TOTAL_VECTORS).step_by(batch_size as usize) {
        let end = (start + batch_size).min(ANN_STRICT_TOTAL_VECTORS);
        let vectors = build_profile_vectors(start, end);
        let (status, upsert_response) = build_api
            .upsert_and_wait_applied(
                ANN_STRICT_COLLECTION,
                json!({
                    "vectors": vectors,
                    "namespace": ANN_STRICT_NAMESPACE,
                }),
                &[],
            )
            .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "strict profile upsert batch [{start}, {end}) failed: {upsert_response:?}"
        );
    }

    // Prebuild ANN artifacts before running strict profile measurements so the
    // profile gates exercise steady-state query behavior.
    let mut prebuild_response = Value::Null;
    for _ in 0..3 {
        let (status, response) = build_api
            .query(
                ANN_STRICT_COLLECTION,
                json!({
                    "vector": cohort_b_query_vector(0),
                    "top_k": ANN_STRICT_TOP_K,
                    "namespace": ANN_STRICT_NAMESPACE,
                    "search_strategy": "ann"
                }),
            )
            .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "strict profile ANN prebuild query failed: {response:?}"
        );
        prebuild_response = response;
        if prebuild_response["ann"]["ann_used"] == true {
            break;
        }
    }
    assert_eq!(
        prebuild_response["ann"]["ann_used"],
        true,
        "strict profile prebuild query must use ANN path before benchmark sampling: {prebuild_response:?}"
    );

    // Cold-cache ANN profile: each sample uses a fresh API process cache.
    let mut cold_samples_ms = Vec::new();
    let mut cold_fetch_latency_ms = Vec::new();
    for sample in 0..8_u32 {
        let cold_storage = backing_store.clone() as Arc<dyn ObjectStore>;
        let cold_api = TestApi::new_with_storage_and_ann_tuning(
            cold_storage,
            backing_store.clone(),
            ANN_STRICT_OBJECT_READ_BUDGET,
            ANN_STRICT_ROOT_BEAM,
            ANN_STRICT_LEAF_PROBES,
        );
        let query_vector = cohort_b_query_vector(sample);
        let started = Instant::now();
        let (status, response) = cold_api
            .query(
                ANN_STRICT_COLLECTION,
                json!({
                    "vector": query_vector,
                    "top_k": ANN_STRICT_TOP_K,
                    "namespace": ANN_STRICT_NAMESPACE,
                    "search_strategy": "ann"
                }),
            )
            .await;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
        assert_eq!(
            status,
            StatusCode::OK,
            "cold ANN query failed: {response:?}"
        );
        assert_ann_query_success(&response);
        assert!(
            response["ann"]["rerank_segment_object_reads"]
                .as_u64()
                .unwrap_or(0)
                > 0,
            "cold ANN query should load rerank vectors from object storage at least once: {response:?}"
        );
        cold_samples_ms.push(elapsed_ms);
        cold_fetch_latency_ms.push(
            response["ann"]["rerank_ssd_fetch_latency_ms"]
                .as_f64()
                .unwrap_or(0.0),
        );
    }
    let cold_p95_ms = p95_ms(cold_samples_ms.as_mut_slice());
    let cold_fetch_p95_ms = p95_ms(cold_fetch_latency_ms.as_mut_slice());
    assert!(
        cold_fetch_p95_ms > 0.0,
        "cold profile should observe non-zero rerank fetch latency"
    );

    // Warm-cache ANN profile: reuse one API so rerank vectors remain on SSD tier.
    let warm_storage = backing_store.clone() as Arc<dyn ObjectStore>;
    let warm_api = TestApi::new_with_storage_and_ann_tuning(
        warm_storage,
        backing_store.clone(),
        ANN_STRICT_OBJECT_READ_BUDGET,
        ANN_STRICT_ROOT_BEAM,
        ANN_STRICT_LEAF_PROBES,
    );
    let primer_query = cohort_b_query_vector(999);
    let (status, primer_response) = warm_api
        .query(
            ANN_STRICT_COLLECTION,
            json!({
                "vector": primer_query,
                "top_k": ANN_STRICT_TOP_K,
                "namespace": ANN_STRICT_NAMESPACE,
                "search_strategy": "ann"
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "warm profile primer ANN query failed: {primer_response:?}"
    );
    assert_ann_query_success(&primer_response);

    let mut warm_samples_ms = Vec::new();
    let mut warm_fetch_latency_ms = Vec::new();
    for _ in 0..16 {
        let query_vector = cohort_b_query_vector(999);
        let started = Instant::now();
        let (status, response) = warm_api
            .query(
                ANN_STRICT_COLLECTION,
                json!({
                    "vector": query_vector,
                    "top_k": ANN_STRICT_TOP_K,
                    "namespace": ANN_STRICT_NAMESPACE,
                    "search_strategy": "ann"
                }),
            )
            .await;
        let elapsed_ms = started.elapsed().as_secs_f64() * 1_000.0;
        assert_eq!(
            status,
            StatusCode::OK,
            "warm ANN query failed: {response:?}"
        );
        assert_ann_query_success(&response);
        assert!(
            response["ann"]["rerank_ssd_cache_hits"]
                .as_u64()
                .unwrap_or(0)
                > 0,
            "warm ANN query should hit rerank SSD cache: {response:?}"
        );
        assert_eq!(
            response["ann"]["rerank_segment_object_reads"]
                .as_u64()
                .unwrap_or(0),
            0,
            "warm ANN query should avoid rerank segment object reads: {response:?}"
        );
        warm_samples_ms.push(elapsed_ms);
        warm_fetch_latency_ms.push(
            response["ann"]["rerank_ssd_fetch_latency_ms"]
                .as_f64()
                .unwrap_or(0.0),
        );
    }
    let warm_p95_ms = p95_ms(warm_samples_ms.as_mut_slice());
    let warm_fetch_p95_ms = p95_ms(warm_fetch_latency_ms.as_mut_slice());
    println!(
        "ann_strict_profile=warm_vs_cold cold_p95_ms={cold_p95_ms:.3} warm_p95_ms={warm_p95_ms:.3} cold_fetch_p95_ms={cold_fetch_p95_ms:.3} warm_fetch_p95_ms={warm_fetch_p95_ms:.3}"
    );
    assert!(
        warm_fetch_p95_ms <= cold_fetch_p95_ms * 0.70,
        "warm ANN rerank-fetch p95 should be at least 30% lower than cold (cold_fetch_p95_ms={cold_fetch_p95_ms:.3}, warm_fetch_p95_ms={warm_fetch_p95_ms:.3})"
    );

    // Selective-filter ANN profile: strict recall and pruning-ratio gates.
    let mut selective_recall_total = 0.0_f64;
    let selective_samples = 16_u32;
    for sample in 0..selective_samples {
        let query_vector = strict_filter_query_vector(sample.saturating_mul(3).saturating_add(1));
        let (ann_status, ann_response) = warm_api
            .query(
                ANN_STRICT_COLLECTION,
                json!({
                    "vector": query_vector,
                    "top_k": ANN_STRICT_TOP_K,
                    "namespace": ANN_STRICT_NAMESPACE,
                    "search_strategy": "ann",
                    "filter": ["strict_group", "Eq", "needle"]
                }),
            )
            .await;
        assert_eq!(
            ann_status,
            StatusCode::OK,
            "selective ANN query failed: {ann_response:?}"
        );
        assert_ann_query_success(&ann_response);
        assert!(
            ann_response["ann"]["quantization_bound_margin"]
                .as_f64()
                .unwrap_or(0.0)
                > 0.0,
            "selective profile should report quantization bound margin: {ann_response:?}"
        );
        let (exact_status, exact_response) = warm_api
            .query(
                ANN_STRICT_COLLECTION,
                json!({
                    "vector": strict_filter_query_vector(sample.saturating_mul(3).saturating_add(1)),
                    "top_k": ANN_STRICT_TOP_K,
                    "namespace": ANN_STRICT_NAMESPACE,
                    "search_strategy": "exact",
                    "filter": ["strict_group", "Eq", "needle"]
                }),
            )
            .await;
        assert_eq!(
            exact_status,
            StatusCode::OK,
            "selective exact query failed: {exact_response:?}"
        );
        selective_recall_total += recall_at_k(&ann_response, &exact_response);
    }
    let selective_mean_recall = selective_recall_total / selective_samples as f64;
    println!("ann_strict_profile=selective_filter mean_recall_at_k={selective_mean_recall:.4}");
    assert!(
        selective_mean_recall >= 0.98,
        "selective-filter ANN strict mean recall@k should be >= 0.98 (got {selective_mean_recall:.4})"
    );

    // Long-tail ANN profile: enforce baseline recall and no fallback/fetch errors.
    let mut long_tail_recall_total = 0.0_f64;
    let mut long_tail_prune_ratios = Vec::new();
    let long_tail_samples = 16_u32;
    for sample in 0..long_tail_samples {
        let query_vector = long_tail_query_vector(sample.saturating_mul(11).saturating_add(3));
        let (ann_status, ann_response) = warm_api
            .query(
                ANN_STRICT_COLLECTION,
                json!({
                    "vector": query_vector,
                    "top_k": ANN_STRICT_TOP_K,
                    "namespace": ANN_STRICT_NAMESPACE,
                    "search_strategy": "ann"
                }),
            )
            .await;
        assert_eq!(
            ann_status,
            StatusCode::OK,
            "long-tail ANN query failed: {ann_response:?}"
        );
        assert_ann_query_success(&ann_response);

        let (exact_status, exact_response) = warm_api
            .query(
                ANN_STRICT_COLLECTION,
                json!({
                    "vector": long_tail_query_vector(sample.saturating_mul(11).saturating_add(3)),
                    "top_k": ANN_STRICT_TOP_K,
                    "namespace": ANN_STRICT_NAMESPACE,
                    "search_strategy": "exact"
                }),
            )
            .await;
        assert_eq!(
            exact_status,
            StatusCode::OK,
            "long-tail exact query failed: {exact_response:?}"
        );
        let first_stage = ann_response["ann"]["first_stage_candidate_count"]
            .as_u64()
            .unwrap_or(0);
        let rerank = ann_response["ann"]["rerank_candidate_count"]
            .as_u64()
            .unwrap_or(0);
        if first_stage > 0 {
            long_tail_prune_ratios.push(rerank as f64 / first_stage as f64);
        }
        long_tail_recall_total += recall_at_k(&ann_response, &exact_response);
    }
    let long_tail_mean_recall = long_tail_recall_total / long_tail_samples as f64;
    let long_tail_median_prune_ratio = median(long_tail_prune_ratios.as_mut_slice());
    println!(
        "ann_strict_profile=long_tail mean_recall_at_k={long_tail_mean_recall:.4} median_rerank_ratio={long_tail_median_prune_ratio:.4}"
    );
    assert!(
        long_tail_mean_recall >= 0.95,
        "long-tail ANN strict mean recall@k should be >= 0.95 (got {long_tail_mean_recall:.4})"
    );
    assert!(
        long_tail_median_prune_ratio <= 0.05,
        "long-tail ANN median rerank ratio should be <=5% (got {long_tail_median_prune_ratio:.4})"
    );

    // Strict budget-failure profile: query should fallback when read budget is exhausted.
    let low_budget_storage = backing_store.clone() as Arc<dyn ObjectStore>;
    let low_budget_api =
        TestApi::new_with_storage_and_ann_object_read_budget(low_budget_storage, backing_store, 0);
    let (status, low_budget_response) = low_budget_api
        .query(
            ANN_STRICT_COLLECTION,
            json!({
                "vector": cohort_b_query_vector(1234),
                "top_k": ANN_STRICT_TOP_K,
                "namespace": ANN_STRICT_NAMESPACE,
                "search_strategy": "ann"
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "low-budget ANN query should return graceful fallback: {low_budget_response:?}"
    );
    assert_eq!(low_budget_response["ann"]["ann_used"], false);
    assert_eq!(low_budget_response["ann"]["ann_fallback_count"], 1);
    assert_eq!(
        low_budget_response["ann"]["object_read_budget_exceeded"], true,
        "low-budget profile should report budget exhaustion"
    );
    let fallback_reasons = low_budget_response["ann"]["fallback_reasons"]
        .as_array()
        .expect("fallback reasons array");
    assert!(
        fallback_reasons
            .iter()
            .any(|entry| entry.as_str() == Some("object_read_budget_exceeded")),
        "low-budget profile should include object_read_budget_exceeded fallback reason: {low_budget_response:?}"
    );
}
