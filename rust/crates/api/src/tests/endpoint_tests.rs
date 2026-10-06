use axum::http::{Method, StatusCode};
use serde_json::json;
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Barrier;
use turbo_vector_storage::ObjectStore;

use crate::{
    distributed::{extract_shard_id, shard_for_vector_id},
    filters::planner::filter_value_term_hash,
    fts::{
        keyspace::{
            field_hash as fts_field_hash, fts_block_pack_key, fts_index_meta_key, stable_doc_id,
            term_hash as fts_term_hash,
        },
        term_meta::{FtsIndexMeta, TermMeta},
    },
    keys::{
        ann_filter_cluster_object_key, ann_index_meta_key, collection_registry_key, now_rfc3339,
        segment_object_key, wal_object_key,
    },
    models::{UpsertRequest, UpsertVector, WalRecord},
};

use super::support::{
    CountingGetStore, DelayedFailOncePutIfAbsentStore, FailNTimesGetStore, FailOnceGetStore,
    FailOncePutIfAbsentStore, InMemoryObjectStore, KeyWriteBarrierStore, SegmentReadLagStore,
    StaleCurrentPointerReadStore, TestApi,
};

#[tokio::test]
async fn health_endpoint_reports_status() {
    let api = TestApi::new();
    let (status, body) = api.request(Method::GET, "/health", None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert!(body.get("phase").is_none());
}

#[tokio::test]
async fn openapi_endpoint_serves_spec_document() {
    let api = TestApi::new();
    let (status, body) = api.request(Method::GET, "/openapi.json", None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    let openapi_version = body["openapi"].as_str().expect("openapi version string");
    assert!(openapi_version.starts_with("3."));
    assert!(body["paths"].get("/v1/collections").is_some());
}

#[tokio::test]
async fn runtime_endpoint_reports_queue_broker_url() {
    let api = TestApi::new();
    let (status, body) = api
        .request(Method::GET, "/v1/system/runtime", None, &[])
        .await;
    assert_eq!(status, StatusCode::OK);
    let queue_broker_url = body["queue_broker_url"]
        .as_str()
        .expect("runtime must expose queue_broker_url");
    assert!(
        !queue_broker_url.trim().is_empty(),
        "queue_broker_url must not be empty"
    );
    assert_eq!(body["wal_worker_build_fts"], true);
    assert_eq!(body["wal_worker_build_ann"], true);
}

#[tokio::test]
async fn observability_query_explain_returns_stepwise_payload() {
    let api = TestApi::new();
    api.create_collection("docs", 2, "dot").await;
    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [
                    {"id": "v1", "values": [1.0, 0.0]},
                    {"id": "v2", "values": [0.0, 1.0]}
                ],
                "namespace": "ns_obs",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = api
        .request(
            Method::POST,
            "/v1/observability/collections/docs/query-explain",
            Some(json!({
                "vector": [1.0, 0.0],
                "top_k": 2,
                "namespace": "ns_obs",
                "search_strategy": "auto",
                "scenario_tag": "unit-test",
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "body={body:?}");
    assert!(body["query_response"]["matches"].is_array());
    assert!(body["explain"]["steps"].is_array());
    assert!(body["explain"]["path"].is_string());
    assert!(body["explain"]["temperature"].is_string());
}

#[tokio::test]
async fn observability_query_explain_reports_cold_then_warm_for_exact_replay() {
    let api = TestApi::new();
    api.create_collection("docs", 2, "dot").await;
    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [
                    {"id": "a", "values": [1.0, 0.0]},
                    {"id": "b", "values": [0.0, 1.0]},
                ],
                "namespace": "ns_runtime",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let payload = json!({
        "vector": [1.0, 0.0],
        "top_k": 2,
        "namespace": "ns_runtime",
        "search_strategy": "exact",
    });

    let (first_status, first_body) = api
        .request(
            Method::POST,
            "/v1/observability/collections/docs/query-explain",
            Some(payload.clone()),
            &[],
        )
        .await;
    assert_eq!(first_status, StatusCode::OK, "first_body={first_body:?}");
    assert_eq!(first_body["explain"]["path"], "exact");
    assert_eq!(first_body["explain"]["temperature"], "cold");

    let (second_status, second_body) = api
        .request(
            Method::POST,
            "/v1/observability/collections/docs/query-explain",
            Some(payload),
            &[],
        )
        .await;
    assert_eq!(second_status, StatusCode::OK, "second_body={second_body:?}");
    assert_eq!(second_body["explain"]["path"], "exact");
    assert_eq!(second_body["explain"]["temperature"], "warm");
}

#[tokio::test]
async fn observability_query_explain_reports_ann_happy_and_fallback_paths() {
    let api = TestApi::new();
    api.create_collection("docs", 8, "cosine").await;

    let ann_vectors = (0..2050)
        .map(|index| {
            let base = index as f32;
            json!({
                "id": format!("ann-{index}"),
                "values": [
                    base * 0.001 + 0.1,
                    base * 0.001 + 0.2,
                    base * 0.001 + 0.3,
                    base * 0.001 + 0.4,
                    base * 0.001 + 0.5,
                    base * 0.001 + 0.6,
                    base * 0.001 + 0.7,
                    base * 0.001 + 0.8
                ],
                "metadata": {
                    "bucket": index % 10
                }
            })
        })
        .collect::<Vec<_>>();

    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": ann_vectors,
                "namespace": "ns_ann_happy",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (happy_status, happy_body) = api
        .request(
            Method::POST,
            "/v1/observability/collections/docs/query-explain",
            Some(json!({
                "vector": [0.11, 0.21, 0.31, 0.41, 0.51, 0.61, 0.71, 0.81],
                "top_k": 10,
                "namespace": "ns_ann_happy",
                "search_strategy": "ann",
            })),
            &[],
        )
        .await;
    assert_eq!(happy_status, StatusCode::OK, "happy_body={happy_body:?}");
    assert_eq!(happy_body["explain"]["path"], "ann");
    assert_eq!(happy_body["query_response"]["ann"]["ann_used"], true);

    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [
                    {"id":"small-1","values":[0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8]},
                    {"id":"small-2","values":[0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]}
                ],
                "namespace": "ns_ann_fallback",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (fallback_status, fallback_body) = api
        .request(
            Method::POST,
            "/v1/observability/collections/docs/query-explain",
            Some(json!({
                "vector": [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8],
                "top_k": 2,
                "namespace": "ns_ann_fallback",
                "search_strategy": "ann",
            })),
            &[],
        )
        .await;
    assert_eq!(
        fallback_status,
        StatusCode::OK,
        "fallback_body={fallback_body:?}"
    );
    assert_eq!(fallback_body["explain"]["path"], "ann_fallback");
    let reasons = fallback_body["explain"]["fallback_reasons"]
        .as_array()
        .expect("fallback reasons array");
    assert!(
        !reasons.is_empty(),
        "expected at least one fallback reason: {fallback_body:?}"
    );
    assert!(
        reasons.iter().any(|reason| {
            matches!(
                reason.as_str(),
                Some("insufficient_vectors") | Some("index_unavailable")
            )
        }),
        "expected insufficient_vectors or index_unavailable fallback reason: {fallback_body:?}"
    );
}

#[tokio::test]
async fn observability_queue_and_storage_inventory_return_read_only_summaries() {
    let api = TestApi::new();
    api.create_collection("docs", 2, "dot").await;
    let (status, upsert_body) = api
        .upsert(
            "docs",
            json!({
                "vectors": [{"id": "v1", "values": [1.0, 0.0]}],
                "namespace": "ns_q",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert_body={upsert_body:?}");

    let (status, queue_body) = api
        .request(
            Method::GET,
            "/v1/observability/collections/docs/queue",
            None,
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "queue_body={queue_body:?}");
    assert!(queue_body["queue_depth"].is_u64());
    assert!(queue_body["jobs"].is_array());

    let (status, inventory_body) = api
        .request(
            Method::GET,
            "/v1/observability/storage/inventory?prefix=collections/docs&limit=100",
            None,
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "inventory_body={inventory_body:?}");
    let items = inventory_body["items"]
        .as_array()
        .expect("inventory items array");
    assert!(
        items.iter().any(|item| item["category"] == "metadata"),
        "expected metadata key classification in inventory: {inventory_body:?}"
    );
}

#[tokio::test]
async fn upsert_fails_when_queue_broker_is_unavailable() {
    let api = TestApi::new_without_queue_broker();
    api.create_collection("docs", 2, "dot").await;
    let (status, body) = api
        .upsert(
            "docs",
            json!({
                "vectors": [{"id": "v1", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "upsert should require external queue broker: {body:?}"
    );
    assert_eq!(body["error"]["code"], "STORE_UNAVAILABLE");
}

#[tokio::test]
async fn collection_endpoints_create_list_and_get_collection() {
    let api = TestApi::new();
    api.create_collection("docs", 3, "dot").await;

    let (status, list_response) = api.request(Method::GET, "/v1/collections", None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    let collections = list_response["collections"]
        .as_array()
        .expect("collections array");
    assert_eq!(collections.len(), 1);
    assert_eq!(collections[0]["name"], "docs");

    let (status, get_response) = api
        .request(Method::GET, "/v1/collections/docs", None, &[])
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(get_response["name"], "docs");
    assert_eq!(get_response["dimension"], 3);
    assert_eq!(get_response["metric"], "dot");
}

#[tokio::test]
async fn collection_delete_removes_metadata_and_hides_collection_from_listing() {
    let api = TestApi::new();
    api.create_collection("docs", 3, "dot").await;
    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [{"id": "v1", "values": [1.0, 0.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, delete_response) = api
        .request(Method::DELETE, "/v1/collections/docs", None, &[])
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "delete response: {delete_response:?}"
    );
    assert_eq!(delete_response["deleted"], true);

    let (status, get_response) = api
        .request(Method::GET, "/v1/collections/docs", None, &[])
        .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "deleted collection should no longer be readable: {get_response:?}"
    );

    let (status, list_response) = api.request(Method::GET, "/v1/collections", None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    let collections = list_response["collections"]
        .as_array()
        .expect("collections array");
    assert!(
        collections
            .iter()
            .all(|collection| collection["name"] != "docs"),
        "deleted collection must be excluded from listings"
    );
}

#[tokio::test]
async fn collection_registry_marker_is_eventually_created_after_concurrent_ensure_failure() {
    let store = Arc::new(InMemoryObjectStore::default());
    let registry_key = collection_registry_key("docs");
    let bootstrap_storage = store.clone() as Arc<dyn ObjectStore>;
    let bootstrap_api = TestApi::new_with_storage(bootstrap_storage, store.clone());
    bootstrap_api.create_collection("docs", 2, "dot").await;
    store
        .delete_bytes(&registry_key)
        .await
        .expect("delete marker to force ensure path during upsert");

    let storage = Arc::new(DelayedFailOncePutIfAbsentStore::new(
        store.clone(),
        registry_key.clone(),
        Duration::from_millis(200),
    )) as Arc<dyn ObjectStore>;
    let api = Arc::new(TestApi::new_with_storage(storage, store.clone()));

    let first_api = api.clone();
    let first = tokio::spawn(async move {
        first_api
            .upsert(
                "docs",
                json!({
                    "vectors": [{"id": "v1", "values": [1.0, 0.0]}],
                    "namespace": "ns_a",
                }),
                &[],
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    let second_api = api.clone();
    let second = tokio::spawn(async move {
        second_api
            .upsert(
                "docs",
                json!({
                    "vectors": [{"id": "v2", "values": [0.0, 1.0]}],
                    "namespace": "ns_a",
                }),
                &[],
            )
            .await
    });

    let (first_status, first_body) = first.await.expect("first upsert join");
    let (second_status, second_body) = second.await.expect("second upsert join");
    let statuses = [first_status, second_status];
    assert!(
        statuses.contains(&StatusCode::SERVICE_UNAVAILABLE),
        "one upsert should fail from injected marker failure: first={first_body:?} second={second_body:?}"
    );
    assert!(
        statuses.contains(&StatusCode::OK),
        "a concurrent upsert should still succeed: first={first_body:?} second={second_body:?}"
    );

    let marker_keys = store.keys_with_prefix(&registry_key).await;
    assert!(
        marker_keys.iter().any(|key| key == &registry_key),
        "successful concurrent ensure should leave a durable collection registry marker"
    );
}

#[tokio::test]
async fn worker_cache_does_not_resurrect_deleted_manifest_on_collection_recreate() {
    let backing_store = Arc::new(InMemoryObjectStore::default());
    let api = Arc::new(TestApi::new_with_storage(
        backing_store.clone(),
        backing_store.clone(),
    ));
    let worker_api = Arc::new(TestApi::new_with_storage(
        backing_store.clone(),
        backing_store,
    ));

    api.create_collection("docs", 2, "dot").await;
    let (status, body) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [{"id": "legacy-v1", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "initial upsert failed: {body:?}");

    // Prime a separate worker process cache so delete+recreate can expose stale cache reuse bugs.
    let (status, worker_stats) = worker_api.stats("docs", Some("ns_a")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "worker failed to warm manifest cache: {worker_stats:?}"
    );
    assert_eq!(worker_stats["vector_count"], 1);

    let (status, delete_response) = api
        .request(Method::DELETE, "/v1/collections/docs", None, &[])
        .await;
    assert_eq!(status, StatusCode::OK, "delete failed: {delete_response:?}");
    assert_eq!(delete_response["deleted"], true);

    api.create_collection("docs", 2, "dot").await;

    let worker = worker_api.spawn_worker(
        "stale-manifest-recreate-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        false,
        false,
    );
    let (status, body) = api
        .upsert(
            "docs",
            json!({
                "vectors": [{"id": "fresh-v1", "values": [0.0, 1.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "recreate upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("upsert should return operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "docs",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("docs", Some("ns_a")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "stats failed after recreate apply: {stats:?}"
    );
    assert_eq!(
        stats["vector_count"], 1,
        "recreated collection should contain only fresh vectors"
    );

    let (status, query_response) = api
        .query(
            "docs",
            json!({
                "vector": [0.0, 1.0],
                "top_k": 10,
                "namespace": "ns_a",
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "query failed after recreate apply: {query_response:?}"
    );
    let ids: BTreeSet<String> = query_response["matches"]
        .as_array()
        .expect("matches array")
        .iter()
        .map(|entry| {
            entry["id"]
                .as_str()
                .expect("match id should be string")
                .to_string()
        })
        .collect();
    assert!(
        ids.contains("fresh-v1"),
        "fresh vector must remain queryable after recreate; ids={ids:?}"
    );
    assert!(
        !ids.contains("legacy-v1"),
        "legacy vector from deleted collection must not leak into recreated collection"
    );

    worker.abort();
}

#[tokio::test]
async fn namespace_api_surface_routes_roundtrip_supported_contracts() {
    let api = TestApi::new();

    let (status, write_response) = api
        .request(
            Method::POST,
            "/v1/namespaces/books",
            Some(json!({
                "upsert_rows": [
                    {"id": "doc-1", "vector": [1.0, 0.0], "topic": "rust", "year": 2026},
                    {"id": "doc-2", "vector": [0.0, 1.0], "topic": "db", "year": 2025}
                ],
                "schema": {
                    "topic": {"type": "string"},
                    "year": {"type": "int"}
                },
                "distance_metric": "cosine_distance",
                "return_affected_ids": true
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "write failed: {write_response:?}");
    assert_eq!(write_response["status"], "OK");
    assert_eq!(write_response["rows_upserted"], 2);
    assert_eq!(write_response["rows_deleted"], 0);
    assert_eq!(write_response["upserted_ids"], json!(["doc-1", "doc-2"]));

    let (status, second_namespace_write) = api
        .request(
            Method::POST,
            "/v1/namespaces/notes",
            Some(json!({
                "upsert_rows": [{"id": "n1", "vector": [0.5, 0.5], "topic": "notes"}]
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{second_namespace_write:?}");

    let (status, first_page) = api
        .request(Method::GET, "/v1/namespaces?page_size=1", None, &[])
        .await;
    assert_eq!(status, StatusCode::OK, "{first_page:?}");
    assert_eq!(
        first_page["namespaces"]
            .as_array()
            .expect("namespaces")
            .len(),
        1
    );
    let first_cursor = first_page["next_cursor"]
        .as_str()
        .expect("next_cursor")
        .to_string();

    let (status, second_page) = api
        .request(
            Method::GET,
            &format!("/v1/namespaces?page_size=10&cursor={first_cursor}"),
            None,
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{second_page:?}");
    let page_ids: Vec<String> = second_page["namespaces"]
        .as_array()
        .expect("namespaces")
        .iter()
        .filter_map(|row| row["id"].as_str().map(ToString::to_string))
        .collect();
    assert!(
        page_ids.contains(&"notes".to_string()),
        "expected notes namespace on second page, got {page_ids:?}"
    );

    let (status, query_response) = api
        .request(
            Method::POST,
            "/v1/namespaces/books/query",
            Some(json!({
                "rank_by": ["vector", "kNN", [1.0, 0.0]],
                "top_k": 2,
                "include_attributes": ["topic", "vector"],
                "exclude_attributes": ["year"]
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{query_response:?}");
    let rows = query_response["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], "doc-1");
    assert_eq!(rows[0]["topic"], "rust");
    assert!(rows[0].get("vector").is_some());
    assert!(rows[0].get("year").is_none());
    assert!(rows[0].get("$dist").is_some());

    let (status, schema_response) = api
        .request(Method::GET, "/v1/namespaces/books/schema", None, &[])
        .await;
    assert_eq!(status, StatusCode::OK, "{schema_response:?}");
    assert_eq!(schema_response["topic"]["type"], "string");

    let (status, schema_update_response) = api
        .request(
            Method::POST,
            "/v1/namespaces/books/schema",
            Some(json!({
                "topic": {"type": "string"},
                "lang": {"type": "string"}
            })),
            &[],
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "schema update failed: {schema_update_response:?}"
    );
    assert!(schema_update_response.get("lang").is_some());

    let (status, metadata_response) = api
        .request(Method::GET, "/v1/namespaces/books/metadata", None, &[])
        .await;
    assert_eq!(status, StatusCode::OK, "{metadata_response:?}");
    assert_eq!(metadata_response["approx_row_count"], 2);
    assert_eq!(metadata_response["index"]["status"], "up-to-date");

    let (status, delete_response) = api
        .request(Method::DELETE, "/v1/namespaces/books", None, &[])
        .await;
    assert_eq!(status, StatusCode::OK, "{delete_response:?}");
    assert_eq!(delete_response["status"], "OK");

    let (status, second_delete_response) = api
        .request(Method::DELETE, "/v1/namespaces/books", None, &[])
        .await;
    assert_eq!(status, StatusCode::OK, "{second_delete_response:?}");
    assert_eq!(second_delete_response["status"], "OK");

    let (status, deleted_query_response) = api
        .request(
            Method::POST,
            "/v1/namespaces/books/query",
            Some(json!({
                "rank_by": ["vector", "kNN", [1.0, 0.0]],
                "top_k": 1
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{deleted_query_response:?}");
}

#[tokio::test]
async fn namespace_api_supports_upsert_columns_and_multi_query_overload() {
    let api = TestApi::new();

    let (status, write_response) = api
        .request(
            Method::POST,
            "/v1/namespaces/column_ns",
            Some(json!({
                "upsert_columns": {
                    "id": ["c1", "c2"],
                    "vector": [[1.0, 0.0], [0.0, 1.0]],
                    "topic": ["rust", "db"]
                }
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{write_response:?}");
    assert_eq!(write_response["rows_upserted"], 2);

    let (status, multi_query_response) = api
        .request(
            Method::POST,
            "/v1/namespaces/column_ns/query?overload=multiQuery",
            Some(json!({
                "queries": [
                    {
                        "rank_by": ["vector", "kNN", [1.0, 0.0]],
                        "top_k": 1,
                        "include_attributes": true
                    },
                    {
                        "rank_by": ["vector", "ANN", [0.0, 1.0]],
                        "top_k": 1,
                        "include_attributes": false
                    }
                ]
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{multi_query_response:?}");
    let results = multi_query_response["results"].as_array().expect("results");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["rows"][0]["id"], "c1");
    assert_eq!(results[1]["rows"][0]["id"], "c2");
    assert!(results[1]["rows"][0].get("topic").is_none());
    assert!(results[1]["rows"][0].get("vector").is_none());
}

#[tokio::test]
async fn namespace_api_bm25_supports_text_and_field_rank_forms() {
    let api = TestApi::new();
    api.create_collection("bm25_rank_forms", 2, "dot").await;
    let worker = api.spawn_worker(
        "bm25-rank-forms-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let (status, body) = api
        .upsert(
            "bm25_rank_forms",
            json!({
                "vectors": [
                    {
                        "id": "doc-1",
                        "values": [1.0, 0.0],
                        "metadata": {
                            "title": "rust database guide",
                            "body": "rust vector retrieval",
                            "year": 2026
                        }
                    },
                    {
                        "id": "doc-2",
                        "values": [0.9, 0.1],
                        "metadata": {
                            "title": "database internals",
                            "body": "storage engine",
                            "year": 2025
                        }
                    },
                    {
                        "id": "doc-3",
                        "values": [0.0, 1.0],
                        "metadata": {
                            "title": "rust rust rust",
                            "body": "compiler basics",
                            "year": 2024
                        }
                    }
                ],
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "bm25_rank_forms",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("bm25_rank_forms", Some("default")).await;
    assert_eq!(status, StatusCode::OK, "{stats:?}");
    let generation = stats["generation"].as_u64().expect("generation");
    let _ = api
        .wait_for_fts_index_meta(
            "bm25_rank_forms",
            "default",
            generation,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;

    let (status, text_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/bm25_rank_forms/query",
            Some(json!({
                "rank_by": ["text", "BM25", "rust database"],
                "top_k": 3,
                "include_attributes": ["title", "year"]
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{text_query:?}");
    let rows = text_query["rows"].as_array().expect("rows");
    assert!(!rows.is_empty(), "BM25 text query should return rows");
    assert_eq!(rows[0]["id"], "doc-1");
    assert!(rows[0].get("$dist").is_some(), "rows must include $dist");

    let (status, field_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/bm25_rank_forms/query",
            Some(json!({
                "rank_by": ["title", "BM25", "database"],
                "top_k": 2,
                "include_attributes": true
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{field_query:?}");
    let ids: Vec<String> = field_query["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(ids, vec!["doc-2".to_string(), "doc-1".to_string()]);
    worker.abort();
}

#[tokio::test]
async fn namespace_api_bm25_prefix_rank_by_filter_and_explain_surface() {
    let api = TestApi::new();
    api.create_collection("bm25_prefix_explain", 2, "dot").await;
    let worker = api.spawn_worker(
        "bm25-prefix-explain-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let (status, body) = api
        .upsert(
            "bm25_prefix_explain",
            json!({
                "vectors": [
                    {
                        "id": "doc-a",
                        "values": [1.0, 0.0],
                        "metadata": {
                            "title": "rustacean handbook",
                            "body": "rustacean rust",
                            "tier": "gold"
                        }
                    },
                    {
                        "id": "doc-b",
                        "values": [0.9, 0.1],
                        "metadata": {
                            "title": "rust fundamentals",
                            "body": "rust fundamentals",
                            "tier": "silver"
                        }
                    },
                    {
                        "id": "doc-d",
                        "values": [0.8, 0.2],
                        "metadata": {
                            "title": "rust fundamentals",
                            "body": "rust fundamentals",
                            "tier": "gold"
                        }
                    },
                    {
                        "id": "doc-f",
                        "values": [0.7, 0.3],
                        "metadata": {
                            "title": "rust fundamentals",
                            "body": "rust fundamentals",
                            "tier": "gold"
                        }
                    }
                ],
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "bm25_prefix_explain",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, prefix_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/bm25_prefix_explain/query",
            Some(json!({
                "rank_by": ["title", "BM25_PREFIX", "rusta"],
                "top_k": 3
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{prefix_query:?}");
    let prefix_ids: Vec<String> = prefix_query["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(prefix_ids, vec!["doc-a".to_string()]);

    let (status, boosted_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/bm25_prefix_explain/query",
            Some(json!({
                "rank_by": [
                    "rank_by_filter",
                    ["tier", "Eq", "gold"],
                    ["text", "BM25", "rust fundamentals"],
                    2.0
                ],
                "top_k": 3
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{boosted_query:?}");
    let boosted_ids: Vec<String> = boosted_query["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(
        boosted_ids,
        vec![
            "doc-d".to_string(),
            "doc-f".to_string(),
            "doc-b".to_string()
        ],
        "rank_by_filter should deterministically boost matched docs and break ties by id"
    );

    let (status, explain_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/bm25_prefix_explain/explain_query",
            Some(json!({
                "rank_by": [
                    "rank_by_filter",
                    ["tier", "Eq", "gold"],
                    ["text", "BM25", "rust fundamentals"],
                    2.0
                ],
                "top_k": 3
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{explain_query:?}");
    assert!(
        explain_query["plan"]["selected_terms"]
            .as_array()
            .is_some_and(|terms| !terms.is_empty()),
        "explain output must include selected terms/fields"
    );
    assert!(
        explain_query["execution"]["candidate_block_count"]
            .as_u64()
            .unwrap_or(0)
            > 0,
        "explain output must include candidate block counts"
    );
    assert!(
        explain_query["execution"]["blocks_decoded"]
            .as_u64()
            .unwrap_or(0)
            > 0,
        "explain output must include decoded block counts"
    );
    assert!(
        explain_query["execution"]["final_score_decomposition"]
            .as_array()
            .is_some_and(|docs| !docs.is_empty()),
        "explain output must include score decomposition"
    );
    let first_doc = &explain_query["execution"]["final_score_decomposition"][0];
    assert!(first_doc.get("base_score").is_some());
    assert!(first_doc.get("boost_multiplier").is_some());
    assert!(first_doc.get("final_score").is_some());

    worker.abort();
}

#[tokio::test]
async fn namespace_api_bm25_supports_weighted_composition_and_filters() {
    let api = TestApi::new();
    api.create_collection("bm25_weighted_filters", 2, "dot")
        .await;
    let worker = api.spawn_worker(
        "bm25-weighted-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let (status, body) = api
        .upsert(
            "bm25_weighted_filters",
            json!({
                "vectors": [
                    {
                        "id": "doc-a",
                        "values": [1.0, 0.0],
                        "metadata": {
                            "title": "rust",
                            "body": "noise terms",
                            "topic": "alpha",
                            "year": 2026
                        }
                    },
                    {
                        "id": "doc-b",
                        "values": [0.9, 0.1],
                        "metadata": {
                            "title": "noise title",
                            "body": "rust rust rust rust",
                            "topic": "beta",
                            "year": 2026
                        }
                    },
                    {
                        "id": "doc-c",
                        "values": [0.8, 0.2],
                        "metadata": {
                            "title": "rust",
                            "body": "rust",
                            "topic": "alpha",
                            "year": 2024
                        }
                    }
                ],
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "bm25_weighted_filters",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("bm25_weighted_filters", Some("default")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let _ = api
        .wait_for_fts_index_meta(
            "bm25_weighted_filters",
            "default",
            generation,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;

    let (status, weighted_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/bm25_weighted_filters/query",
            Some(json!({
                "rank_by": ["Sum", [
                    ["Product", 3.0, ["title", "BM25", "rust"]],
                    ["Product", 0.5, ["body", "BM25", "rust"]]
                ]],
                "top_k": 2,
                "filters": ["year", "Eq", 2026],
                "include_attributes": ["topic", "body", "vector", "year"],
                "exclude_attributes": ["body"]
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{weighted_query:?}");
    let rows = weighted_query["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], "doc-a");
    assert_eq!(rows[0]["topic"], "alpha");
    assert!(rows[0].get("year").is_some());
    assert!(
        rows[0].get("body").is_none(),
        "exclude_attributes must be applied"
    );
    assert!(rows.iter().all(|row| row["year"] == json!(2026)));

    let (status, max_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/bm25_weighted_filters/query",
            Some(json!({
                "rank_by": ["Max", [
                    ["title", "BM25", "rust"],
                    ["body", "BM25", "rust"]
                ]],
                "top_k": 2
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{max_query:?}");
    assert!(
        !max_query["rows"].as_array().expect("rows").is_empty(),
        "Max composition should produce rows"
    );
    worker.abort();
}

#[tokio::test]
async fn namespace_api_bm25_multi_query_mixed_with_ann_preserves_shape() {
    let api = TestApi::new();
    api.create_collection("bm25_multi_query", 2, "dot").await;
    let worker = api.spawn_worker(
        "bm25-multi-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        true,
    );

    let (status, body) = api
        .upsert(
            "bm25_multi_query",
            json!({
                "vectors": [
                    {
                        "id": "doc-rust",
                        "values": [1.0, 0.0],
                        "metadata": {"body": "rust systems programming"}
                    },
                    {
                        "id": "doc-ann",
                        "values": [0.0, 1.0],
                        "metadata": {"body": "vector nearest neighbor"}
                    }
                ],
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "bm25_multi_query",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("bm25_multi_query", Some("default")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let _ = api
        .wait_for_fts_index_meta(
            "bm25_multi_query",
            "default",
            generation,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;

    let (status, response) = api
        .request(
            Method::POST,
            "/v1/namespaces/bm25_multi_query/query?overload=multiQuery",
            Some(json!({
                "queries": [
                    {
                        "rank_by": ["text", "BM25", "rust"],
                        "top_k": 1,
                        "include_attributes": false
                    },
                    {
                        "rank_by": ["vector", "ANN", [0.0, 1.0]],
                        "top_k": 1,
                        "include_attributes": false
                    }
                ]
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    let results = response["results"].as_array().expect("results");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["rows"][0]["id"], "doc-rust");
    assert_eq!(results[1]["rows"][0]["id"], "doc-ann");
    worker.abort();
}

#[tokio::test]
async fn namespace_api_bm25_long_query_exposes_skip_telemetry() {
    let api = TestApi::new();
    api.create_collection("bm25_skip_metrics", 2, "dot").await;
    let worker = api.spawn_worker(
        "bm25-skip-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let mut vectors = Vec::new();
    vectors.push(json!({
        "id": "doc-champion",
        "values": [1.0, 0.0],
        "metadata": {
            "body": "alpha alpha alpha alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu"
        }
    }));
    for index in 0..48 {
        vectors.push(json!({
            "id": format!("doc-{index:03}"),
            "values": [0.0, 1.0],
            "metadata": {
                "body": "alpha"
            }
        }));
    }
    let (status, body) = api
        .upsert(
            "bm25_skip_metrics",
            json!({
                "vectors": vectors,
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "bm25_skip_metrics",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("bm25_skip_metrics", Some("default")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let _ = api
        .wait_for_fts_index_meta(
            "bm25_skip_metrics",
            "default",
            generation,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;

    let (status, response) = api
        .request(
            Method::POST,
            "/v1/namespaces/bm25_skip_metrics/query",
            Some(json!({
                "rank_by": ["text", "BM25", "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu"],
                "top_k": 1,
                "include_attributes": false
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response["rows"][0]["id"], "doc-champion");
    let lexical = &response["billing"]["lexical_execution"];
    assert!(
        lexical["header_reads"].as_u64().unwrap_or(0) > 0,
        "expected header_reads > 0 in lexical telemetry: {response:?}"
    );
    assert!(
        lexical["blocks_decoded"].as_u64().unwrap_or(0) > 0,
        "expected blocks_decoded > 0 in lexical telemetry: {response:?}"
    );
    assert!(
        lexical["blocks_skipped"].as_u64().unwrap_or(0) > 0,
        "expected blocks_skipped > 0 for long BM25 query: {response:?}"
    );
    assert!(
        lexical["docs_scored"].as_u64().unwrap_or(0) > 0,
        "expected docs_scored > 0 in lexical telemetry: {response:?}"
    );
    worker.abort();
}

#[tokio::test]
async fn namespace_api_rejects_unsupported_patch_fields() {
    let api = TestApi::new();

    let (status, write_response) = api
        .request(
            Method::POST,
            "/v1/namespaces/patch_ns",
            Some(json!({
                "upsert_rows": [{"id": "p1", "vector": [1.0, 0.0]}]
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{write_response:?}");

    let (status, unsupported_response) = api
        .request(
            Method::POST,
            "/v1/namespaces/patch_ns",
            Some(json!({
                "patch_rows": [{"id": "p1", "topic": "patched"}]
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{unsupported_response:?}");
    assert_eq!(unsupported_response["error"]["code"], "INVALID_ARGUMENT");
    assert!(unsupported_response["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .contains("patch_rows"));
}

#[tokio::test]
async fn upsert_rejects_vectors_with_wrong_dimension() {
    let api = TestApi::new();
    api.create_collection("docs", 3, "cosine").await;

    let (status, body) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [
                    {"id": "v1", "values": [1.0, 2.0]}
                ],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], "INVALID_ARGUMENT");
}

#[tokio::test]
async fn idempotency_key_replays_response_without_extra_writes() {
    let api = TestApi::new();
    api.create_collection("docs", 2, "cosine").await;

    let payload = json!({
        "vectors": [
            {"id": "v1", "values": [1.0, 0.0], "metadata": {"topic": "rust"}}
        ],
        "namespace": "ns_a",
    });
    let headers = [("Idempotency-Key", "same-write")];
    let (first_status, first_response) = api
        .upsert_and_wait_applied("docs", payload.clone(), &headers)
        .await;
    let (second_status, second_response) =
        api.upsert_and_wait_applied("docs", payload, &headers).await;

    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(first_response, second_response);

    let manifest_keys = api
        .store
        .keys_with_prefix("collections/docs/manifests/")
        .await;
    let manifest_generation_count = manifest_keys
        .iter()
        .filter(|key| key.ends_with(".json") && !key.ends_with("current.json"))
        .count();
    assert_eq!(
        manifest_generation_count, 1,
        "idempotent retry must not publish a new manifest generation"
    );

    let segment_keys = api
        .store
        .keys_with_prefix("collections/docs/segments/")
        .await;
    assert_eq!(
        segment_keys.len(),
        1,
        "idempotent retry must not write a second segment"
    );

    let wal_keys = api.store.keys_with_prefix("collections/docs/wal/").await;
    assert_eq!(
        wal_keys.len(),
        1,
        "idempotent retry must not write a second WAL record"
    );
}

#[tokio::test]
async fn query_respects_namespace_filter_and_include_flags() {
    let api = TestApi::new();
    api.create_collection("search", 2, "dot").await;

    let (status, _) = api
        .upsert_and_wait_applied(
            "search",
            json!({
                "vectors": [
                    {"id": "a1", "values": [1.0, 0.0], "metadata": {"topic": "rust"}},
                    {"id": "a2", "values": [0.2, 0.1], "metadata": {"topic": "db"}}
                ],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = api
        .upsert_and_wait_applied(
            "search",
            json!({
                "vectors": [
                    {"id": "b1", "values": [1.0, 0.0], "metadata": {"topic": "rust"}}
                ],
                "namespace": "ns_b",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, query_response) = api
        .query(
            "search",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "include_metadata": false,
                "include_values": false
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(query_response["namespace"], "ns_a");
    let matches = query_response["matches"].as_array().expect("matches array");
    assert_eq!(matches.len(), 2);
    assert_eq!(matches[0]["id"], "a1");
    assert_eq!(matches[1]["id"], "a2");
    assert!(matches[0].get("metadata").is_none());
    assert!(matches[0].get("values").is_none());
    assert!(matches.iter().all(|entry| entry["id"] != "b1"));

    let (status, filtered_response) = api
        .query(
            "search",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "include_metadata": true,
                "include_values": true,
                "filter": ["topic", "Eq", "db"]
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let filtered_matches = filtered_response["matches"]
        .as_array()
        .expect("filtered matches array");
    assert_eq!(filtered_matches.len(), 1);
    assert_eq!(filtered_matches[0]["id"], "a2");
    assert_eq!(filtered_matches[0]["metadata"]["topic"], "db");
    assert_eq!(filtered_matches[0]["values"], json!([0.2, 0.1]));
}

#[tokio::test]
async fn query_accepts_tuple_filter_with_in_and_glob() {
    let api = TestApi::new();
    api.create_collection("tuple_filter_docs", 2, "dot").await;

    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "tuple_filter_docs",
            json!({
                "vectors": [
                    {
                        "id": "m1",
                        "values": [1.0, 0.0],
                        "metadata": {"topic": "rust", "path": "foo/src/main.rs"}
                    },
                    {
                        "id": "m2",
                        "values": [0.9, 0.1],
                        "metadata": {"topic": "db", "path": "foo/src/db.rs"}
                    },
                    {
                        "id": "m3",
                        "values": [0.8, 0.2],
                        "metadata": {"topic": "rust", "path": "foo/tests/test.rs"}
                    },
                    {
                        "id": "m4",
                        "values": [0.7, 0.3],
                        "metadata": {"topic": "ops", "path": "foo/src/ops.rs"}
                    }
                ],
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let (status, query_response) = api
        .query(
            "tuple_filter_docs",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "exact",
                "filter": ["And",
                    ["topic", "In", ["rust", "db"]],
                    ["path", "Glob", "foo/src/*"]
                ]
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "tuple-expression filter query failed: {query_response:?}"
    );
    let ids: Vec<String> = query_response["matches"]
        .as_array()
        .expect("matches")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(ids, vec!["m1".to_string(), "m2".to_string()]);
}

#[tokio::test]
async fn query_supports_nested_operator_parity_filter_expression() {
    let api = TestApi::new();
    api.create_collection("tuple_filter_parity", 2, "dot").await;

    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "tuple_filter_parity",
            json!({
                "vectors": [
                    {
                        "id": "a1",
                        "values": [1.0, 0.0],
                        "metadata": {
                            "topic": "rust",
                            "score": 10,
                            "tag": "stable",
                            "labels": ["vec", "search"],
                            "title": "native filtering guide",
                            "path": "foo/src/main.rs"
                        }
                    },
                    {
                        "id": "a2",
                        "values": [0.9, 0.1],
                        "metadata": {
                            "topic": "rust",
                            "score": 4,
                            "tag": "stable",
                            "labels": ["vec"],
                            "title": "native filtering guide",
                            "path": "foo/src/legacy.rs"
                        }
                    },
                    {
                        "id": "a3",
                        "values": [0.8, 0.2],
                        "metadata": {
                            "topic": "db",
                            "score": 12,
                            "tag": "legacy",
                            "labels": ["search"],
                            "title": "native filtering guide",
                            "path": "foo/src/db.rs"
                        }
                    },
                    {
                        "id": "a4",
                        "values": [0.7, 0.3],
                        "metadata": {
                            "topic": "rust",
                            "score": 11,
                            "tag": "stable",
                            "labels": ["ops"],
                            "title": "native filtering guide",
                            "path": "foo/tests/main.rs"
                        }
                    }
                ],
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let (status, query_response) = api
        .query(
            "tuple_filter_parity",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "exact",
                "filter": ["And",
                    ["topic", "Eq", "rust"],
                    ["score", "Gte", 8],
                    ["tag", "NotIn", ["legacy"]],
                    ["labels", "ContainsAny", ["vec", "index"]],
                    ["title", "ContainsAllTokens", ["native", "filtering"]],
                    ["path", "Glob", "foo/src/*"],
                    ["Regex", "title", "native\\s+filtering"],
                    ["Not", ["score", "Lt", 9]],
                    ["Or", ["topic", "Eq", "rust"], ["topic", "Eq", "db"]]
                ]
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "query failed: {query_response:?}");
    let ids: Vec<String> = query_response["matches"]
        .as_array()
        .expect("matches array")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(ids, vec!["a1".to_string()]);
}

#[tokio::test]
async fn ann_query_builds_index_artifacts_and_returns_ranked_matches() {
    let api = TestApi::new();
    api.create_collection("ann_docs", 8, "dot").await;

    let mut vectors = Vec::new();
    // Keep this test above the small-corpus exact shortcut.
    for index in 0..20_500_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:04}"),
            "values": [
                ((index as f32) / 20_500.0) * 2.0 - 1.0,
                0.25,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            ]
        }));
    }
    vectors.push(json!({
        "id": "doc-best",
        "values": [1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    }));

    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "ann_docs",
            json!({
                "vectors": vectors,
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let (status, ann_query) = api
        .query(
            "ann_docs",
            json!({
                "vector": [1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "ann"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "ANN query failed: {ann_query:?}");
    assert_eq!(ann_query["ann"]["ann_used"], true);
    assert_eq!(ann_query["ann"]["ann_fallback_count"], 0);
    assert!(
        ann_query["ann"]["buckets_probed"].as_u64().unwrap_or(0) > 0,
        "ANN query should probe at least one bucket"
    );
    let ann_matches = ann_query["matches"].as_array().expect("ANN matches array");
    assert!(!ann_matches.is_empty(), "ANN query returned no matches");

    let (status, exact_query) = api
        .query(
            "ann_docs",
            json!({
                "vector": [1.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "exact"
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "exact query failed: {exact_query:?}"
    );
    assert_eq!(exact_query["ann"]["ann_used"], false);
    let exact_matches = exact_query["matches"]
        .as_array()
        .expect("exact matches array");
    assert!(!exact_matches.is_empty(), "exact query returned no matches");

    assert_eq!(exact_matches[0]["id"], "doc-best");
    let ann_ids = ann_matches
        .iter()
        .filter_map(|entry| entry["id"].as_str())
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    assert!(
        ann_ids.iter().any(|id| id == "doc-best"),
        "ANN top-k should include the best exact match: {ann_ids:?}"
    );

    let ann_keys = api
        .store
        .keys_with_prefix("collections/ann_docs/ann/ns_a/")
        .await;
    assert!(
        ann_keys.iter().any(|key| key.ends_with("/meta.json")),
        "ANN metadata must be materialized in object store"
    );
    let meta_key = ann_keys
        .iter()
        .find(|key| key.ends_with("/meta.json"))
        .expect("ANN metadata key should exist");
    let meta_bytes = api
        .store
        .get_bytes(meta_key)
        .await
        .expect("ANN metadata bytes should load");
    let ann_meta: serde_json::Value =
        serde_json::from_slice(&meta_bytes).expect("ANN metadata should parse");
    let tree_levels = ann_meta["tree_levels"]
        .as_array()
        .expect("ANN metadata must expose tree_levels");
    assert!(
        !tree_levels.is_empty(),
        "ANN metadata tree_levels should not be empty"
    );
    let root_nodes = tree_levels[0]["nodes"]
        .as_array()
        .expect("tree root level should include nodes");
    assert!(
        !root_nodes.is_empty(),
        "ANN root tree level should include at least one node"
    );
    let has_tree_links = tree_levels.iter().any(|level| {
        level["nodes"].as_array().is_some_and(|nodes| {
            nodes.iter().any(|node| {
                node["bucket_id"].is_number()
                    || node["child_node_ids"]
                        .as_array()
                        .is_some_and(|children| !children.is_empty())
            })
        })
    });
    assert!(
        has_tree_links,
        "ANN tree levels should include child links and/or leaf bucket references"
    );
    assert!(
        ann_keys
            .iter()
            .any(|key| key.contains("/buckets/") && key.ends_with(".bin")),
        "ANN bucket files must be materialized in object store"
    );
    let bucket_key = ann_keys
        .iter()
        .find(|key| key.contains("/buckets/") && key.ends_with(".bin"))
        .expect("ANN bucket key should exist");
    let bucket_bytes = api
        .store
        .get_bytes(bucket_key)
        .await
        .expect("ANN bucket bytes should load");
    assert!(
        bucket_bytes.starts_with(b"TVAB"),
        "ANN bucket payload should use binary encoding magic"
    );
}

#[tokio::test]
async fn ann_filtered_query_uses_native_filtering_for_selective_cohort() {
    let api = TestApi::new();
    api.create_collection("ann_native_filter", 8, "dot").await;

    let batch_size = 2_000_u32;
    let total_vectors = 24_000_u32;
    let selective_start = 11_500_u32;
    let selective_end = 11_700_u32;
    // Keep this test above the small-corpus exact shortcut.
    for batch_start in (0..total_vectors).step_by(batch_size as usize) {
        let batch_end = (batch_start + batch_size).min(total_vectors);
        let mut vectors = Vec::new();
        for index in batch_start..batch_end {
            let angle = (index as f32 / total_vectors as f32) * std::f32::consts::TAU;
            let is_selective = index >= selective_start && index < selective_end;
            vectors.push(json!({
                "id": if is_selective {
                    format!("b-{index:05}")
                } else {
                    format!("a-{index:05}")
                },
                "values": [
                    angle.cos(),
                    angle.sin(),
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0
                ],
                "metadata": {
                    "group": if is_selective { "b" } else { "a" }
                }
            }));
        }

        let (status, upsert_response) = api
            .upsert_and_wait_applied(
                "ann_native_filter",
                json!({
                    "vectors": vectors,
                    "namespace": "ns_a",
                }),
                &[],
            )
            .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "upsert batch [{batch_start},{batch_end}) failed: {upsert_response:?}"
        );
    }

    let (status, unfiltered_ann) = api
        .query(
            "ann_native_filter",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "ann"
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "unfiltered ann failed: {unfiltered_ann:?}"
    );
    assert_eq!(unfiltered_ann["ann"]["ann_used"], true);
    let unfiltered_probes = unfiltered_ann["ann"]["buckets_probed"]
        .as_u64()
        .expect("unfiltered probes");

    let (status, filtered_ann) = api
        .query(
            "ann_native_filter",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "ann",
                "filter": ["And", ["group", "Eq", "b"]]
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "filtered ann failed: {filtered_ann:?}"
    );
    assert_eq!(filtered_ann["ann"]["ann_used"], true);
    let filtered_probes = filtered_ann["ann"]["buckets_probed"]
        .as_u64()
        .expect("filtered probes");

    let (status, filtered_exact) = api
        .query(
            "ann_native_filter",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "exact",
                "filter": ["And", ["group", "Eq", "b"]]
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "filtered exact failed: {filtered_exact:?}"
    );

    let ann_ids: Vec<String> = filtered_ann["matches"]
        .as_array()
        .expect("filtered ann matches")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    let exact_ids: Vec<String> = filtered_exact["matches"]
        .as_array()
        .expect("filtered exact matches")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(ann_ids.len(), 10, "filtered ANN should return top_k");
    assert_eq!(exact_ids.len(), 10, "filtered exact should return top_k");
    assert!(
        ann_ids.iter().all(|id| id.starts_with("b-")),
        "filtered ANN should only return cohort b ids: {ann_ids:?}"
    );
    assert!(
        exact_ids.iter().all(|id| id.starts_with("b-")),
        "filtered exact should only return cohort b ids: {exact_ids:?}"
    );
    let overlap = exact_ids.iter().filter(|id| ann_ids.contains(*id)).count();
    assert!(
        overlap >= 8,
        "filtered ANN recall should stay high for selective cohort; overlap={overlap}, ann={ann_ids:?}, exact={exact_ids:?}"
    );
    assert!(
        filtered_probes <= unfiltered_probes,
        "native filtering should avoid probing more buckets than unfiltered ANN (filtered={filtered_probes}, unfiltered={unfiltered_probes})"
    );
}

#[tokio::test]
async fn ann_query_orders_equal_scores_deterministically_by_id() {
    let api = TestApi::new();
    api.create_collection("ann_ties", 8, "dot").await;

    let mut vectors = Vec::new();
    for index in 0..2200_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:04}"),
            "values": [
                (index as f32) / 10000.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            ]
        }));
    }
    vectors.push(json!({
        "id": "tie-2",
        "values": [9.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    }));
    vectors.push(json!({
        "id": "tie-1",
        "values": [9.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    }));

    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "ann_ties",
            json!({
                "vectors": vectors,
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let (status, query_response) = api
        .query(
            "ann_ties",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 5,
                "namespace": "ns_a",
                "search_strategy": "ann"
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "ANN query failed: {query_response:?}"
    );
    assert_eq!(query_response["ann"]["ann_used"], true);
    let matches = query_response["matches"].as_array().expect("matches array");
    assert!(matches.len() >= 2, "expected at least two ANN matches");
    assert_eq!(matches[0]["id"], "tie-1");
    assert_eq!(matches[1]["id"], "tie-2");
}

#[tokio::test]
async fn distributed_query_fanout_merges_deterministically_and_upsert_artifacts_are_sharded() {
    let api = TestApi::new_with_distributed_sharding(4, 250, true, 3);
    api.create_collection("dist_merge", 2, "dot").await;
    let (runtime_status, runtime) = api
        .request(Method::GET, "/v1/system/runtime", None, &[])
        .await;
    assert_eq!(runtime_status, StatusCode::OK);
    assert_eq!(runtime["distributed_shard_count"], 4);

    let mut vectors = Vec::new();
    for index in 0..24_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:03}"),
            "values": [((index as f32) / 100.0), 0.0],
        }));
    }
    let tie_ids = vec![
        "tie-000", "tie-001", "tie-002", "tie-003", "tie-004", "tie-005", "tie-006", "tie-007",
    ];
    for id in &tie_ids {
        vectors.push(json!({
            "id": id,
            "values": [9.0, 0.0],
        }));
    }

    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "dist_merge",
            json!({
                "vectors": vectors,
                "namespace": "tenant_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let segment_keys = api
        .store
        .keys_with_prefix("collections/dist_merge/segments/")
        .await;
    assert!(
        !segment_keys.is_empty(),
        "expected sharded segment artifacts"
    );
    let mut seen_shards = HashSet::new();
    for segment_key in segment_keys {
        let raw = api
            .store
            .get_bytes(&segment_key)
            .await
            .expect("segment bytes should load");
        let segment: serde_json::Value =
            serde_json::from_slice(&raw).expect("segment should parse as JSON");
        let namespace = segment["namespace"].as_str().expect("segment namespace");
        let shard_id = extract_shard_id(namespace).expect("namespace should include shard suffix");
        seen_shards.insert(shard_id);
        for vector in segment["vectors"]
            .as_array()
            .expect("segment vectors array")
        {
            let vector_id = vector["id"].as_str().expect("vector id");
            let expected_shard = shard_for_vector_id("dist_merge", "tenant_a", vector_id, 4);
            assert_eq!(
                shard_id, expected_shard,
                "vector '{vector_id}' should be routed to deterministic shard"
            );
        }
    }
    assert!(
        seen_shards.len() > 1,
        "test workload should span multiple shards: {seen_shards:?}"
    );

    let (status, query_response) = api
        .query(
            "dist_merge",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 8,
                "namespace": "tenant_a",
                "search_strategy": "exact"
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "distributed query should succeed: {query_response:?}"
    );
    assert_eq!(query_response["distributed"]["planned_shards"], 4);
    assert_eq!(query_response["distributed"]["successful_shards"], 4);
    assert_eq!(
        query_response["distributed"]["degraded"]
            .as_bool()
            .unwrap_or(false),
        false
    );
    let ids: Vec<String> = query_response["matches"]
        .as_array()
        .expect("matches array")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    let mut expected = tie_ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    expected.sort();
    assert_eq!(ids, expected, "global top-k must be deterministic by id");
}

#[tokio::test]
async fn distributed_query_marks_dropped_shard_degradation() {
    let api = TestApi::new_with_distributed_sharding(4, 250, true, 3);
    api.create_collection("dist_drop", 2, "dot").await;
    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "dist_drop",
            json!({
                "vectors": [
                    {"id": "a", "values": [1.0, 0.0]},
                    {"id": "b", "values": [0.5, 0.0]},
                    {"id": "c", "values": [0.25, 0.0]},
                    {"id": "d", "values": [0.125, 0.0]}
                ],
                "namespace": "tenant_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let (status, placement) = api
        .request(
            Method::GET,
            "/v1/collections/dist_drop/shards/placement",
            None,
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let version = placement["version"].as_u64().expect("placement version");
    let mut assignments = placement["assignments"]
        .as_array()
        .expect("placement assignments")
        .to_vec();
    assignments[0]["state"] = json!("offline");
    let (status, update_response) = api
        .request(
            Method::PUT,
            "/v1/collections/dist_drop/shards/placement",
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
        StatusCode::OK,
        "placement update failed: {update_response:?}"
    );

    let (status, query_response) = api
        .query(
            "dist_drop",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 4,
                "namespace": "tenant_a",
                "search_strategy": "exact"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "query failed: {query_response:?}");
    assert_eq!(query_response["distributed"]["degraded"], true);
    assert!(
        query_response["distributed"]["dropped_shards"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    let reasons = query_response["distributed"]["degradation_reasons"]
        .as_array()
        .expect("degradation reasons");
    assert!(
        !reasons.is_empty(),
        "partial responses must always be labelled"
    );
    assert!(reasons
        .iter()
        .any(|reason| reason.as_str() == Some("dropped_shard")));
}

#[tokio::test]
async fn distributed_query_marks_timeout_and_stale_shard_degradation() {
    let api = TestApi::new_with_distributed_sharding(4, 5, true, 2);
    api.create_collection("dist_timeout_stale", 2, "dot").await;
    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "dist_timeout_stale",
            json!({
                "vectors": [
                    {"id": "a", "values": [1.0, 0.0]},
                    {"id": "b", "values": [0.5, 0.0]},
                    {"id": "c", "values": [0.25, 0.0]},
                    {"id": "d", "values": [0.125, 0.0]}
                ],
                "namespace": "tenant_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let (status, stats) = api.stats("dist_timeout_stale", Some("tenant_a")).await;
    assert_eq!(status, StatusCode::OK, "stats failed: {stats:?}");
    let generation = stats["generation"].as_u64().expect("generation");

    let (status, placement) = api
        .request(
            Method::GET,
            "/v1/collections/dist_timeout_stale/shards/placement",
            None,
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let version = placement["version"].as_u64().expect("placement version");
    let mut assignments = placement["assignments"]
        .as_array()
        .expect("placement assignments")
        .to_vec();
    assignments[0]["simulated_delay_ms"] = json!(50);
    assignments[1]["min_generation"] = json!(generation + 100);
    let (status, update_response) = api
        .request(
            Method::PUT,
            "/v1/collections/dist_timeout_stale/shards/placement",
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
        StatusCode::OK,
        "placement update failed: {update_response:?}"
    );

    let (status, query_response) = api
        .query(
            "dist_timeout_stale",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 4,
                "namespace": "tenant_a",
                "search_strategy": "exact"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "query failed: {query_response:?}");
    assert_eq!(query_response["distributed"]["degraded"], true);
    assert!(
        query_response["distributed"]["timed_out_shards"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    assert!(
        query_response["distributed"]["stale_shards"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    let reasons = query_response["distributed"]["degradation_reasons"]
        .as_array()
        .expect("degradation reasons");
    assert!(reasons
        .iter()
        .any(|reason| reason.as_str() == Some("slow_shard_timeout")));
    assert!(reasons
        .iter()
        .any(|reason| reason.as_str() == Some("stale_generation_shard")));
}

#[tokio::test]
async fn shard_rebalance_endpoint_supports_dry_run_and_apply() {
    let api = TestApi::new_with_distributed_sharding(4, 250, true, 3);
    api.create_collection("dist_rebalance", 2, "dot").await;

    let (status, dry_run) = api
        .request(
            Method::POST,
            "/v1/collections/dist_rebalance/shards/rebalance",
            Some(json!({
                "target_nodes": ["node-a", "node-b"],
                "dry_run": true
            })),
            &[],
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "rebalance dry-run failed: {dry_run:?}"
    );
    assert_eq!(dry_run["applied"], false);
    assert!(
        dry_run["safety_checks"]
            .as_array()
            .is_some_and(|checks| !checks.is_empty()),
        "rebalance response should include safety checks"
    );

    let (status, apply_response) = api
        .request(
            Method::POST,
            "/v1/collections/dist_rebalance/shards/rebalance",
            Some(json!({
                "target_nodes": ["node-a", "node-b"],
                "dry_run": false
            })),
            &[],
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "rebalance apply failed: {apply_response:?}"
    );
    assert_eq!(apply_response["applied"], true);

    let (status, placement) = api
        .request(
            Method::GET,
            "/v1/collections/dist_rebalance/shards/placement",
            None,
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let nodes: HashSet<String> = placement["assignments"]
        .as_array()
        .expect("assignments")
        .iter()
        .filter_map(|assignment| assignment["node_id"].as_str())
        .map(ToString::to_string)
        .collect();
    assert!(
        nodes.contains("node-a") && nodes.contains("node-b"),
        "rebalance should assign shards across requested nodes: {nodes:?}"
    );
}

#[tokio::test]
async fn ann_query_fallback_path_is_explicit_when_bucket_decode_fails() {
    let store = Arc::new(InMemoryObjectStore::default());
    let storage = store.clone() as Arc<dyn ObjectStore>;
    let api = TestApi::new_with_storage(storage.clone(), store.clone());
    api.create_collection("ann_fallback", 8, "dot").await;

    let mut vectors = Vec::new();
    // Keep this test above the small-corpus exact shortcut.
    for index in 0..20_500_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:04}"),
            "values": [
                (index as f32) / 20_500.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            ]
        }));
    }
    vectors.push(json!({
        "id": "doc-best",
        "values": [11.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    }));
    let (status, _) = api
        .upsert_and_wait_applied(
            "ann_fallback",
            json!({
                "vectors": vectors,
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, initial_ann_query) = api
        .query(
            "ann_fallback",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "ann"
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "initial ANN query failed: {initial_ann_query:?}"
    );
    assert_eq!(initial_ann_query["ann"]["ann_used"], true);

    let ann_keys = store
        .keys_with_prefix("collections/ann_fallback/ann/ns_a/")
        .await;
    let bucket_keys: Vec<_> = ann_keys
        .iter()
        .filter(|key| key.contains("/buckets/") && key.ends_with(".bin"))
        .cloned()
        .collect();
    assert!(!bucket_keys.is_empty(), "expected at least one ANN bucket");
    for bucket_key in bucket_keys {
        store
            .put_bytes(&bucket_key, b"invalid-bucket-payload")
            .await
            .expect("corrupting ANN bucket should succeed");
    }

    let reader_api = TestApi::new_with_storage(storage, store.clone());
    let (status, fallback_query) = reader_api
        .query(
            "ann_fallback",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "ann"
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "fallback query failed: {fallback_query:?}"
    );
    assert_eq!(
        fallback_query["ann"]["ann_used"], false,
        "unexpected ANN observability payload: {fallback_query:?}"
    );
    assert_eq!(fallback_query["ann"]["ann_fallback_count"], 1);
    assert!(
        fallback_query["ann"]["ann_fetch_errors"]
            .as_u64()
            .unwrap_or(0)
            >= 1
    );
    let fallback_reasons = fallback_query["ann"]["fallback_reasons"]
        .as_array()
        .expect("fallback reasons");
    assert!(
        fallback_reasons
            .iter()
            .any(|value| value.as_str() == Some("bucket_fetch_error")),
        "fallback reason should indicate bucket fetch/decode failure"
    );

    let fallback_matches = fallback_query["matches"].as_array().expect("matches");
    assert_eq!(fallback_matches[0]["id"], "doc-best");
}

#[tokio::test]
async fn ann_query_reports_cluster_filter_load_failure_reason() {
    let store = Arc::new(InMemoryObjectStore::default());
    let storage = store.clone() as Arc<dyn ObjectStore>;
    let api = TestApi::new_with_storage(storage.clone(), store.clone());
    api.create_collection("ann_filter_cluster_fail", 8, "dot")
        .await;

    let mut vectors = Vec::new();
    for index in 0..20_500_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:04}"),
            "values": [
                (index as f32) / 20_500.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            ],
            "metadata": {
                "group": if index % 2 == 0 { "a" } else { "b" }
            }
        }));
    }
    let (status, _) = api
        .upsert_and_wait_applied(
            "ann_filter_cluster_fail",
            json!({
                "vectors": vectors,
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, initial_query) = api
        .query(
            "ann_filter_cluster_fail",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "ann",
                "filter": ["group", "Eq", "b"]
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "initial query failed: {initial_query:?}"
    );
    assert_eq!(initial_query["ann"]["ann_used"], true);

    let (status, stats) = api.stats("ann_filter_cluster_fail", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let term_hash = filter_value_term_hash("group", &json!("b")).expect("term hash");
    let cluster_key =
        ann_filter_cluster_object_key("ann_filter_cluster_fail", "ns_a", generation, &term_hash);
    let cluster_keys = store.keys_with_prefix(&cluster_key).await;
    assert!(
        !cluster_keys.is_empty(),
        "expected cluster summary for target term hash"
    );
    let corrupt_key = &cluster_keys[0];
    store
        .put_bytes(corrupt_key, b"corrupt-cluster-summary")
        .await
        .expect("should overwrite cluster summary payload");

    let reader_api = TestApi::new_with_storage(storage, store.clone());
    let (status, fallback_query) = reader_api
        .query(
            "ann_filter_cluster_fail",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "ann",
                "filter": ["group", "Eq", "b"]
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "fallback query failed: {fallback_query:?}"
    );
    assert_eq!(fallback_query["ann"]["ann_used"], false);
    let reasons = fallback_query["ann"]["fallback_reasons"]
        .as_array()
        .expect("fallback reasons");
    assert!(
        reasons
            .iter()
            .any(|reason| reason.as_str() == Some("filter_cluster_summary_load_failure")),
        "missing expected fallback reason: {fallback_query:?}"
    );
}

#[tokio::test]
async fn ann_query_reports_row_filter_load_failure_reason() {
    let store = Arc::new(InMemoryObjectStore::default());
    let storage = store.clone() as Arc<dyn ObjectStore>;
    let api = TestApi::new_with_storage(storage.clone(), store.clone());
    api.create_collection("ann_filter_row_fail", 8, "dot").await;

    let mut vectors = Vec::new();
    for index in 0..20_500_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:04}"),
            "values": [
                (index as f32) / 20_500.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            ],
            "metadata": {
                "group": if index % 2 == 0 { "a" } else { "b" }
            }
        }));
    }
    let (status, _) = api
        .upsert_and_wait_applied(
            "ann_filter_row_fail",
            json!({
                "vectors": vectors,
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, initial_query) = api
        .query(
            "ann_filter_row_fail",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "ann",
                "filter": ["group", "Eq", "b"]
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "initial query failed: {initial_query:?}"
    );
    assert_eq!(initial_query["ann"]["ann_used"], true);

    let (status, stats) = api.stats("ann_filter_row_fail", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let term_hash = filter_value_term_hash("group", &json!("b")).expect("term hash");
    let row_prefix =
        format!("collections/ann_filter_row_fail/ann/ns_a/{generation}/filters/row/{term_hash}/");
    let row_keys = store.keys_with_prefix(&row_prefix).await;
    assert!(!row_keys.is_empty(), "expected filter row bitmaps");
    let corrupt_key = &row_keys[0];
    store
        .put_bytes(corrupt_key, b"corrupt-row-bitmap")
        .await
        .expect("should overwrite row bitmap payload");

    let reader_api = TestApi::new_with_storage(storage, store.clone());
    let (status, fallback_query) = reader_api
        .query(
            "ann_filter_row_fail",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "search_strategy": "ann",
                "filter": ["group", "Eq", "b"]
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "fallback query failed: {fallback_query:?}"
    );
    assert_eq!(fallback_query["ann"]["ann_used"], false);
    let reasons = fallback_query["ann"]["fallback_reasons"]
        .as_array()
        .expect("fallback reasons");
    assert!(
        reasons
            .iter()
            .any(|reason| reason.as_str() == Some("filter_row_bitmap_load_failure")),
        "missing expected fallback reason: {fallback_query:?}"
    );
}

#[tokio::test]
async fn ann_query_adaptive_probe_budget_increases_for_topk_and_filters() {
    let api = TestApi::new();
    api.create_collection("ann_adaptive", 8, "dot").await;

    let mut vectors = Vec::new();
    // Keep this test above the small-corpus exact shortcut.
    for index in 0..20_600_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:04}"),
            "values": [
                (index as f32) / 20_600.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            ],
            "metadata": {
                "group": if index % 2 == 0 { "a" } else { "b" }
            }
        }));
    }

    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "ann_adaptive",
            json!({
                "vectors": vectors,
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let (status, low_topk_query) = api
        .query(
            "ann_adaptive",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 5,
                "namespace": "ns_a",
                "search_strategy": "ann"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(low_topk_query["ann"]["ann_used"], true);
    let low_topk_probes = low_topk_query["ann"]["buckets_probed"]
        .as_u64()
        .expect("low-top-k probes");

    let (status, high_topk_query) = api
        .query(
            "ann_adaptive",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 64,
                "namespace": "ns_a",
                "search_strategy": "ann"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(high_topk_query["ann"]["ann_used"], true);
    let high_topk_probes = high_topk_query["ann"]["buckets_probed"]
        .as_u64()
        .expect("high-top-k probes");
    assert!(
        high_topk_probes >= low_topk_probes,
        "higher top_k should not probe fewer buckets"
    );

    let (status, filtered_query) = api
        .query(
            "ann_adaptive",
            json!({
                "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                "top_k": 5,
                "namespace": "ns_a",
                "search_strategy": "ann",
                "filter": ["group", "Eq", "a"]
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(filtered_query["ann"]["ann_used"], true);
    let filtered_probes = filtered_query["ann"]["buckets_probed"]
        .as_u64()
        .expect("filtered probes");
    assert!(
        filtered_probes <= high_topk_probes,
        "filter-aware ANN path should avoid probing more buckets than broad high-top-k ANN path"
    );
}

#[tokio::test]
async fn ann_query_concurrency_stress_has_no_fallback_storm() {
    let api = Arc::new(TestApi::new());
    api.create_collection("ann_stress", 8, "dot").await;

    let mut vectors = Vec::new();
    for index in 0..3200_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:04}"),
            "values": [
                (index as f32) / 3200.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            ]
        }));
    }
    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "ann_stress",
            json!({
                "vectors": vectors,
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let mut join_set = tokio::task::JoinSet::new();
    for _ in 0..64 {
        let api = api.clone();
        join_set.spawn(async move {
            api.query(
                "ann_stress",
                json!({
                    "vector": [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
                    "top_k": 10,
                    "namespace": "ns_a",
                    "search_strategy": "ann"
                }),
            )
            .await
        });
    }

    let mut fallback_total = 0_u64;
    let mut fetch_errors_total = 0_u64;
    while let Some(joined) = join_set.join_next().await {
        let (status, body) = joined.expect("query task join");
        assert_eq!(status, StatusCode::OK, "ANN stress query failed: {body:?}");
        fallback_total += body["ann"]["ann_fallback_count"].as_u64().unwrap_or(0);
        fetch_errors_total += body["ann"]["ann_fetch_errors"].as_u64().unwrap_or(0);
    }
    assert_eq!(
        fallback_total, 0,
        "ANN stress queries should avoid fallback storms"
    );
    assert_eq!(
        fetch_errors_total, 0,
        "ANN stress queries should avoid fetch errors"
    );
}

#[tokio::test]
async fn ann_query_recall_guardrail_stays_high_against_exact() {
    let api = TestApi::new();
    api.create_collection("ann_recall_guardrail", 8, "cosine")
        .await;

    let make_values = |index: u32| -> Vec<f32> {
        let phase = index as f32 / 3200.0;
        let bucket = (index % 128) as f32 / 128.0;
        vec![
            phase,
            bucket,
            phase * bucket,
            (phase * 0.5) + 0.1,
            (bucket * 0.7) + 0.05,
            (phase - bucket).abs(),
            (phase + bucket) * 0.3,
            1.0,
        ]
    };

    let vectors: Vec<_> = (0..3200_u32)
        .map(|index| {
            json!({
                "id": format!("doc-{index:04}"),
                "values": make_values(index),
            })
        })
        .collect();
    let (status, upsert_response) = api
        .upsert_and_wait_applied(
            "ann_recall_guardrail",
            json!({
                "vectors": vectors,
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {upsert_response:?}");

    let sample_count = 40_u32;
    let mut recall_total = 0.0_f32;
    for sample in 0..sample_count {
        let probe_index = (sample * 73) % 3200;
        let query_values = make_values(probe_index);

        let (ann_status, ann_query) = api
            .query(
                "ann_recall_guardrail",
                json!({
                    "vector": query_values,
                    "top_k": 10,
                    "namespace": "ns_a",
                    "search_strategy": "ann"
                }),
            )
            .await;
        assert_eq!(
            ann_status,
            StatusCode::OK,
            "ANN query failed: {ann_query:?}"
        );
        assert_eq!(ann_query["ann"]["ann_used"], true);

        let (exact_status, exact_query) = api
            .query(
                "ann_recall_guardrail",
                json!({
                    "vector": make_values(probe_index),
                    "top_k": 10,
                    "namespace": "ns_a",
                    "search_strategy": "exact"
                }),
            )
            .await;
        assert_eq!(
            exact_status,
            StatusCode::OK,
            "exact query failed: {exact_query:?}"
        );

        let ann_ids: HashSet<String> = ann_query["matches"]
            .as_array()
            .expect("ANN matches array")
            .iter()
            .filter_map(|value| value["id"].as_str())
            .map(ToString::to_string)
            .collect();
        let exact_ids: Vec<String> = exact_query["matches"]
            .as_array()
            .expect("exact matches array")
            .iter()
            .filter_map(|value| value["id"].as_str())
            .map(ToString::to_string)
            .collect();
        let k = exact_ids.len().max(1);
        let hits = exact_ids
            .iter()
            .filter(|exact_id| ann_ids.contains(*exact_id))
            .count();
        recall_total += hits as f32 / k as f32;
    }

    let mean_recall = recall_total / sample_count as f32;
    assert!(
        mean_recall >= 0.95,
        "ANN mean recall@k should stay >= 0.95, got {mean_recall:.4}"
    );
}

#[tokio::test]
async fn fetch_returns_only_requested_existing_ids_for_namespace() {
    let api = TestApi::new();
    api.create_collection("search", 2, "dot").await;

    let (status, _) = api
        .upsert_and_wait_applied(
            "search",
            json!({
                "vectors": [
                    {"id": "a1", "values": [1.0, 0.0], "metadata": {"topic": "rust"}},
                    {"id": "a2", "values": [0.2, 0.1], "metadata": {"topic": "db"}}
                ],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = api
        .upsert_and_wait_applied(
            "search",
            json!({
                "vectors": [
                    {"id": "b1", "values": [1.0, 0.0], "metadata": {"topic": "rust"}}
                ],
                "namespace": "ns_b",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, fetch_response) = api
        .request(
            Method::POST,
            "/v1/collections/search/vectors/fetch",
            Some(json!({
                "ids": ["missing", "a2", "b1"],
                "namespace": "ns_a",
                "include_metadata": false,
                "include_values": true
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "fetch failed: {fetch_response:?}");
    assert_eq!(fetch_response["namespace"], "ns_a");
    let vectors = fetch_response["vectors"].as_array().expect("vectors array");
    assert_eq!(vectors.len(), 1);
    assert_eq!(vectors[0]["id"], "a2");
    assert!(vectors[0].get("metadata").is_none());
    assert_eq!(vectors[0]["values"], json!([0.2, 0.1]));
}

#[tokio::test]
async fn delete_by_ids_is_namespace_scoped_and_stats_reflect_changes() {
    let api = TestApi::new();
    api.create_collection("search", 2, "dot").await;

    let (status, _) = api
        .upsert_and_wait_applied(
            "search",
            json!({
                "vectors": [
                    {"id": "a1", "values": [1.0, 0.0], "metadata": {"topic": "rust"}},
                    {"id": "a2", "values": [0.2, 0.1], "metadata": {"topic": "db"}}
                ],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _) = api
        .upsert_and_wait_applied(
            "search",
            json!({
                "vectors": [
                    {"id": "b1", "values": [1.0, 0.0], "metadata": {"topic": "rust"}}
                ],
                "namespace": "ns_b",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, delete_response) = api
        .delete_vectors(
            "search",
            json!({
                "ids": ["a1"],
                "namespace": "ns_a",
                "delete_all": false
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(delete_response["deleted_count"], 1);

    let (status, ns_a_query) = api
        .query(
            "search",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let ns_a_matches = ns_a_query["matches"].as_array().expect("matches array");
    assert_eq!(ns_a_matches.len(), 1);
    assert_eq!(ns_a_matches[0]["id"], "a2");

    let (status, ns_b_query) = api
        .query(
            "search",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_b"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let ns_b_matches = ns_b_query["matches"].as_array().expect("matches array");
    assert_eq!(ns_b_matches.len(), 1);
    assert_eq!(ns_b_matches[0]["id"], "b1");

    let (status, ns_a_stats) = api.stats("search", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ns_a_stats["dimension"], 2);
    assert_eq!(ns_a_stats["vector_count"], 1);
    assert_eq!(ns_a_stats["segments"], 2);
    assert_eq!(ns_a_stats["generation"], 3);

    let (status, ns_b_stats) = api.stats("search", Some("ns_b")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ns_b_stats["dimension"], 2);
    assert_eq!(ns_b_stats["vector_count"], 1);
    assert_eq!(ns_b_stats["segments"], 1);
    assert_eq!(ns_b_stats["generation"], 3);
}

#[tokio::test]
async fn delete_filter_and_delete_all_work_for_namespace() {
    let api = TestApi::new();
    api.create_collection("docs", 2, "dot").await;

    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [
                    {"id": "r1", "values": [1.0, 0.0], "metadata": {"topic": "rust"}},
                    {"id": "r2", "values": [0.9, 0.1], "metadata": {"topic": "rust"}},
                    {"id": "d1", "values": [0.1, 1.0], "metadata": {"topic": "db"}}
                ],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, filtered_delete_response) = api
        .delete_vectors(
            "docs",
            json!({
                "filter": ["topic", "Eq", "rust"],
                "namespace": "ns_a",
                "delete_all": false
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(filtered_delete_response["deleted_count"], 2);

    let (status, after_filter_query) = api
        .query(
            "docs",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let after_filter_matches = after_filter_query["matches"]
        .as_array()
        .expect("matches array");
    assert_eq!(after_filter_matches.len(), 1);
    assert_eq!(after_filter_matches[0]["id"], "d1");

    let (status, delete_all_response) = api
        .delete_vectors(
            "docs",
            json!({
                "namespace": "ns_a",
                "delete_all": true
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(delete_all_response["deleted_count"], 1);

    let (status, final_query) = api
        .query(
            "docs",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let final_matches = final_query["matches"].as_array().expect("matches array");
    assert!(final_matches.is_empty());

    let (status, final_stats) = api.stats("docs", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(final_stats["dimension"], 2);
    assert_eq!(final_stats["vector_count"], 0);
}

#[tokio::test]
async fn concurrent_deletes_from_two_nodes_do_not_drop_one_manifest_update() {
    let backing_store = Arc::new(InMemoryObjectStore::default());
    let storage = Arc::new(KeyWriteBarrierStore::new(
        backing_store.clone(),
        "/manifests/2.json".to_string(),
        2,
    ));
    let api_a = Arc::new(TestApi::new_with_storage(
        storage.clone(),
        backing_store.clone(),
    ));
    let api_b = Arc::new(TestApi::new_with_storage(storage, backing_store));

    api_a.create_collection("docs", 2, "dot").await;
    let (status, body) = api_a
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [
                    {"id": "v1", "values": [1.0, 0.0]},
                    {"id": "v2", "values": [0.0, 1.0]}
                ],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "initial upsert failed: {body:?}");

    let a = api_a.clone();
    let b = api_b.clone();
    let delete_a = tokio::spawn(async move {
        a.delete_vectors(
            "docs",
            json!({
                "ids": ["v1"],
                "namespace": "ns_a",
                "delete_all": false
            }),
        )
        .await
    });
    let delete_b = tokio::spawn(async move {
        b.delete_vectors(
            "docs",
            json!({
                "ids": ["v2"],
                "namespace": "ns_a",
                "delete_all": false
            }),
        )
        .await
    });

    let (status_a, body_a) = delete_a.await.expect("delete_a join");
    let (status_b, body_b) = delete_b.await.expect("delete_b join");
    assert_eq!(status_a, StatusCode::OK, "delete_a failed: {body_a:?}");
    assert_eq!(status_b, StatusCode::OK, "delete_b failed: {body_b:?}");

    let (status, query_response) = api_a
        .query(
            "docs",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let matches = query_response["matches"].as_array().expect("matches");
    assert!(
        matches.is_empty(),
        "both deletes should persist; got remaining matches: {matches:?}"
    );
}

#[tokio::test]
async fn delete_requires_a_selector_and_stats_supports_empty_collections() {
    let api = TestApi::new();
    api.create_collection("empty", 3, "cosine").await;

    let (status, stats_before_ingest) = api.stats("empty", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stats_before_ingest["dimension"], 3);
    assert_eq!(stats_before_ingest["vector_count"], 0);
    assert_eq!(stats_before_ingest["segments"], 0);
    assert_eq!(stats_before_ingest["generation"], 0);

    let (status, delete_response) = api
        .delete_vectors(
            "empty",
            json!({
                "namespace": "default",
                "delete_all": false
            }),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(delete_response["error"]["code"], "INVALID_ARGUMENT");
}

#[tokio::test]
async fn concurrent_upserts_keep_all_vectors_even_with_stale_pointer_reads() {
    let backing_store = Arc::new(InMemoryObjectStore::default());
    let storage = Arc::new(StaleCurrentPointerReadStore::new(
        backing_store.clone(),
        Duration::from_millis(40),
    ));
    let api = Arc::new(TestApi::new_with_storage(storage, backing_store));
    api.create_collection("docs", 2, "dot").await;
    let worker = api.spawn_worker(
        "stale-pointer-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        false,
        false,
    );

    let writer_count = 16;
    let barrier = Arc::new(Barrier::new(writer_count));
    let mut handles = Vec::new();
    for idx in 0..writer_count {
        let api = api.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            api.upsert(
                "docs",
                json!({
                    "vectors": [
                        {
                            "id": format!("v-{idx}"),
                            "values": [1.0, (idx as f32) / 100.0]
                        }
                    ],
                    "namespace": "ns_a",
                }),
                &[],
            )
            .await
        }));
    }

    for handle in handles {
        let (status, body) = handle.await.expect("writer task should join");
        assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    }

    let stats = api
        .wait_for_vector_count(
            "docs",
            Some("ns_a"),
            writer_count as u64,
            Duration::from_secs(5),
            Duration::from_millis(20),
        )
        .await;
    assert_eq!(stats["vector_count"], writer_count);
    let generation = stats["generation"]
        .as_u64()
        .expect("generation should be an integer");
    assert!(
        generation >= 1 && generation <= writer_count as u64,
        "generation should remain monotonic and not exceed applied write count"
    );
    worker.abort();
}

#[tokio::test]
async fn upsert_retries_when_previous_manifest_is_transiently_missing() {
    let backing_store = Arc::new(InMemoryObjectStore::default());
    let storage = Arc::new(FailOnceGetStore::new(
        backing_store.clone(),
        ["collections/docs/manifests/1.json".to_string()],
    ));
    let api = TestApi::new_with_storage(storage, backing_store);
    api.create_collection("docs", 2, "dot").await;

    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [{"id": "v1", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [{"id": "v2", "values": [0.0, 1.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "second upsert failed: {body:?}");

    let (status, stats) = api.stats("docs", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stats["vector_count"], 2);
    assert_eq!(stats["generation"], 2);
}

#[tokio::test]
async fn upserts_are_enqueue_only_until_worker_applies() {
    let api = TestApi::new();
    api.create_collection("queue_docs", 2, "dot").await;

    let (status, wait_false_body) = api
        .upsert(
            "queue_docs",
            json!({
                "vectors": [{"id": "queued-false", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "first upsert failed: {wait_false_body:?}"
    );
    assert_eq!(wait_false_body["accepted"], true);
    let operation_false = wait_false_body["operation_id"]
        .as_str()
        .expect("first operation_id")
        .to_string();

    let (status, wait_true_body) = api
        .upsert(
            "queue_docs",
            json!({
                "vectors": [{"id": "queued-true", "values": [0.0, 1.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "second upsert failed: {wait_true_body:?}"
    );
    assert_eq!(wait_true_body["accepted"], true);
    let operation_true = wait_true_body["operation_id"]
        .as_str()
        .expect("second operation_id")
        .to_string();
    assert_ne!(operation_false, operation_true);

    let (status, queued_stats) = api.stats("queue_docs", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(queued_stats["vector_count"], 0);
    assert_eq!(queued_stats["generation"], 0);

    let (status, wait_false_status) = api.operation_status("queue_docs", &operation_false).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(wait_false_status["status"], "accepted");
    assert!(wait_false_status.get("generation").is_none());
    assert!(wait_false_status.get("applied_at").is_none());

    let (status, wait_true_status) = api.operation_status("queue_docs", &operation_true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(wait_true_status["status"], "accepted");
    assert!(wait_true_status.get("generation").is_none());
    assert!(wait_true_status.get("applied_at").is_none());
}

#[tokio::test]
async fn worker_auto_flush_applies_accepted_upserts_within_bounded_time() {
    let api = TestApi::new();
    api.create_collection("bg_queue_docs", 2, "dot").await;
    let mut operation_ids = Vec::new();

    for idx in 0..4 {
        let (status, body) = api
            .upsert(
                "bg_queue_docs",
                json!({
                    "vectors": [{"id": format!("queued-{idx}"), "values": [1.0, (idx as f32) / 10.0]}],
                    "namespace": "ns_a",
                }),
                &[],
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["accepted"], true);
        operation_ids.push(
            body["operation_id"]
                .as_str()
                .expect("operation_id for accepted upsert")
                .to_string(),
        );
    }

    let worker = api.spawn_worker(
        "auto-flush-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        8,
        Duration::from_millis(5),
        false,
        false,
    );
    for operation_id in &operation_ids {
        let status_body = api
            .wait_for_operation_applied(
                "bg_queue_docs",
                operation_id,
                Duration::from_secs(3),
                Duration::from_millis(20),
            )
            .await;
        assert_eq!(status_body["status"], "applied");
        assert!(status_body["generation"].as_u64().is_some());
    }

    let stats_after_drain = api
        .wait_for_vector_count(
            "bg_queue_docs",
            Some("ns_a"),
            4,
            Duration::from_secs(3),
            Duration::from_millis(20),
        )
        .await;
    assert_eq!(stats_after_drain["vector_count"], 4);
    let generation = stats_after_drain["generation"]
        .as_u64()
        .expect("generation should be integer");
    assert!(
        (1..=4).contains(&generation),
        "worker auto-flush may publish one or multiple generations, got {generation}"
    );
    worker.abort();
}

#[tokio::test]
async fn stats_refreshes_manifest_cache_after_worker_publishes_new_generation() {
    let api = TestApi::new();
    api.create_collection("cache_refresh_docs", 2, "dot").await;
    let worker = api.spawn_worker(
        "cache-refresh-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        false,
        false,
    );

    let (status, first_upsert_body) = api
        .upsert(
            "cache_refresh_docs",
            json!({
                "vectors": [{"id": "v1", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "first upsert failed: {first_upsert_body:?}"
    );
    let first_operation_id = first_upsert_body["operation_id"]
        .as_str()
        .expect("first operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "cache_refresh_docs",
        &first_operation_id,
        Duration::from_secs(3),
        Duration::from_millis(20),
    )
    .await;

    let (status, first_stats) = api.stats("cache_refresh_docs", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_stats["vector_count"], 1);
    let first_generation = first_stats["generation"]
        .as_u64()
        .expect("first generation should be integer");

    let (status, second_upsert_body) = api
        .upsert(
            "cache_refresh_docs",
            json!({
                "vectors": [{"id": "v2", "values": [0.0, 1.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "second upsert failed: {second_upsert_body:?}"
    );
    let second_operation_id = second_upsert_body["operation_id"]
        .as_str()
        .expect("second operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "cache_refresh_docs",
        &second_operation_id,
        Duration::from_secs(3),
        Duration::from_millis(20),
    )
    .await;

    let refreshed_stats = api
        .wait_for_vector_count(
            "cache_refresh_docs",
            Some("ns_a"),
            2,
            Duration::from_secs(4),
            Duration::from_millis(20),
        )
        .await;
    assert_eq!(refreshed_stats["vector_count"], 2);
    let refreshed_generation = refreshed_stats["generation"]
        .as_u64()
        .expect("refreshed generation should be integer");
    assert!(
        refreshed_generation > first_generation,
        "manifest generation should advance after later worker apply: first={first_generation}, refreshed={refreshed_generation}"
    );
    worker.abort();
}

#[tokio::test]
async fn worker_apply_does_not_skip_when_a_wal_entry_is_temporarily_missing() {
    let backing_store = Arc::new(InMemoryObjectStore::default());
    let storage = Arc::new(FailNTimesGetStore::new(backing_store.clone()));
    let api = TestApi::new_with_storage(storage.clone(), backing_store);
    api.create_collection("queue_safe", 2, "dot").await;

    let (status, body) = api
        .upsert(
            "queue_safe",
            json!({
                "vectors": [{"id": "v1", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let operation_1 = body["operation_id"]
        .as_str()
        .expect("operation_id for first queued upsert")
        .to_string();

    let (status, body) = api
        .upsert(
            "queue_safe",
            json!({
                "vectors": [{"id": "v2", "values": [0.5, 0.5]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let operation_2 = body["operation_id"]
        .as_str()
        .expect("operation_id for second queued upsert")
        .to_string();

    storage
        .set_failures(wal_object_key("queue_safe", &operation_2), 4)
        .await;

    let (status, body) = api
        .upsert(
            "queue_safe",
            json!({
                "vectors": [{"id": "v3", "values": [0.0, 1.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "queued upsert failed: {body:?}");
    let operation_3 = body["operation_id"]
        .as_str()
        .expect("operation_id for third queued upsert")
        .to_string();

    let worker = api.spawn_worker(
        "queue-safe-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        false,
        false,
    );

    let stats = api
        .wait_for_vector_count(
            "queue_safe",
            Some("ns_a"),
            3,
            Duration::from_secs(4),
            Duration::from_millis(20),
        )
        .await;
    assert_eq!(
        stats["vector_count"], 3,
        "transient WAL miss must not skip and drop earlier queued operations"
    );
    for operation_id in [&operation_1, &operation_2, &operation_3] {
        let status = api
            .wait_for_operation_applied(
                "queue_safe",
                operation_id,
                Duration::from_secs(4),
                Duration::from_millis(20),
            )
            .await;
        assert_eq!(status["status"], "applied");
    }

    let (status, query_response) = api
        .query(
            "queue_safe",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "include_values": true
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let ids: Vec<String> = query_response["matches"]
        .as_array()
        .expect("matches")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert!(
        ids.contains(&"v1".to_string())
            && ids.contains(&"v2".to_string())
            && ids.contains(&"v3".to_string()),
        "expected all queued writes to remain visible; operation_1={operation_1}, operation_2={operation_2}, ids={ids:?}"
    );
    worker.abort();
}

#[tokio::test]
async fn worker_apply_waits_for_segment_visibility_before_manifest_publish() {
    let backing_store = Arc::new(InMemoryObjectStore::default());
    let storage = Arc::new(SegmentReadLagStore::new(backing_store.clone(), 3));
    let api = TestApi::new_with_storage(storage, backing_store);
    api.create_collection("segment_visibility_docs", 2, "dot")
        .await;

    let (status, body) = api
        .upsert(
            "segment_visibility_docs",
            json!({
                "vectors": [{"id": "v1", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "queued upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();

    let worker = api.spawn_worker(
        "segment-visibility-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        false,
        false,
    );

    api.wait_for_operation_applied(
        "segment_visibility_docs",
        &operation_id,
        Duration::from_secs(4),
        Duration::from_millis(20),
    )
    .await;

    let (status, query_response) = api
        .query(
            "segment_visibility_docs",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 1,
                "namespace": "ns_a",
            }),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "manifest must not reference a segment before the segment is readable: {query_response:?}"
    );
    assert_eq!(
        query_response["matches"][0]["id"].as_str(),
        Some("v1"),
        "query should return the applied vector after segment visibility settles"
    );
    worker.abort();
}

#[tokio::test]
async fn operation_status_endpoint_transitions_from_accepted_to_applied() {
    let api = TestApi::new();
    api.create_collection("op_status_docs", 2, "dot").await;

    let (status, body) = api
        .upsert(
            "op_status_docs",
            json!({
                "vectors": [{"id": "v1", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "accepted upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();

    let (status, accepted_status) = api.operation_status("op_status_docs", &operation_id).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(accepted_status["status"], "accepted");
    assert_eq!(accepted_status["operation_id"], operation_id);
    assert!(accepted_status.get("generation").is_none());
    assert!(accepted_status.get("applied_at").is_none());

    let (status, missing_status) = api
        .operation_status("op_status_docs", "missing-00000000000000000000000000000000")
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(missing_status["error"]["code"], "NOT_FOUND");

    let worker = api.spawn_worker(
        "op-status-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        false,
        false,
    );
    let applied_status = api
        .wait_for_operation_applied(
            "op_status_docs",
            &operation_id,
            Duration::from_secs(3),
            Duration::from_millis(20),
        )
        .await;
    assert_eq!(applied_status["status"], "applied");
    assert!(applied_status["generation"].as_u64().is_some());
    assert!(applied_status["applied_at"].as_str().is_some());
    worker.abort();
}

#[tokio::test]
async fn multi_worker_apply_under_concurrency_has_no_drop_or_duplication() {
    let api = Arc::new(TestApi::new());
    api.create_collection("multi_worker_docs", 2, "dot").await;

    let writer_count = 48usize;
    let barrier = Arc::new(Barrier::new(writer_count));
    let mut join_set = tokio::task::JoinSet::new();
    for idx in 0..writer_count {
        let api = api.clone();
        let barrier = barrier.clone();
        join_set.spawn(async move {
            barrier.wait().await;
            api.upsert(
                "multi_worker_docs",
                json!({
                    "vectors": [{"id": format!("mw-{idx}"), "values": [1.0, (idx as f32) / 100.0]}],
                    "namespace": "ns_a",
                }),
                &[],
            )
            .await
        });
    }

    let mut operation_ids = Vec::new();
    while let Some(joined) = join_set.join_next().await {
        let (status, body) = joined.expect("upsert join");
        assert_eq!(status, StatusCode::OK, "accepted upsert failed: {body:?}");
        operation_ids.push(
            body["operation_id"]
                .as_str()
                .expect("operation_id")
                .to_string(),
        );
    }
    let unique_operation_ids: HashSet<String> = operation_ids.iter().cloned().collect();
    assert_eq!(
        unique_operation_ids.len(),
        writer_count,
        "each accepted write should have a unique operation_id"
    );

    let worker_a = api.spawn_worker(
        "multi-worker-a",
        Duration::from_millis(10),
        Duration::from_millis(10),
        16,
        Duration::from_millis(5),
        false,
        false,
    );
    let worker_b = api.spawn_worker(
        "multi-worker-b",
        Duration::from_millis(250),
        Duration::from_millis(250),
        16,
        Duration::from_millis(50),
        false,
        false,
    );

    let stats = api
        .wait_for_vector_count(
            "multi_worker_docs",
            Some("ns_a"),
            writer_count as u64,
            Duration::from_secs(12),
            Duration::from_millis(20),
        )
        .await;
    assert_eq!(stats["vector_count"], writer_count as u64);
    assert_eq!(
        stats["segments"], writer_count as u64,
        "each operation should be applied exactly once (no duplicate segments)"
    );

    let request_ids: Vec<String> = (0..writer_count).map(|idx| format!("mw-{idx}")).collect();
    let (status, fetch_response) = api
        .request(
            Method::POST,
            "/v1/collections/multi_worker_docs/vectors/fetch",
            Some(json!({
                "ids": request_ids,
                "namespace": "ns_a",
                "include_metadata": false,
                "include_values": false
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "fetch failed: {fetch_response:?}");
    let fetched = fetch_response["vectors"]
        .as_array()
        .expect("fetched vectors");
    assert_eq!(fetched.len(), writer_count);
    let fetched_ids: HashSet<String> = fetched
        .iter()
        .map(|value| value["id"].as_str().expect("id").to_string())
        .collect();
    assert_eq!(fetched_ids.len(), writer_count);

    for operation_id in operation_ids {
        let applied_status = api
            .wait_for_operation_applied(
                "multi_worker_docs",
                &operation_id,
                Duration::from_secs(12),
                Duration::from_millis(20),
            )
            .await;
        assert_eq!(applied_status["status"], "applied");
    }

    worker_a.abort();
    worker_b.abort();
}

#[tokio::test]
async fn query_rejects_segments_with_checksum_mismatch() {
    let api = TestApi::new();
    api.create_collection("docs", 2, "dot").await;

    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [{"id": "v1", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let segment_keys = api
        .store
        .keys_with_prefix("collections/docs/segments/")
        .await;
    assert_eq!(segment_keys.len(), 1);
    let segment_key = segment_keys[0].clone();
    let raw_segment = ObjectStore::get_bytes(&*api.store, &segment_key)
        .await
        .expect("load segment bytes");
    let mut parsed: serde_json::Value =
        serde_json::from_slice(&raw_segment).expect("parse segment JSON");
    parsed["vectors"][0]["values"] = json!([9.0, 9.0]);
    let tampered_bytes = serde_json::to_vec(&parsed).expect("serialize tampered segment");
    ObjectStore::put_bytes(&*api.store, &segment_key, &tampered_bytes)
        .await
        .expect("overwrite segment with tampered payload");

    let (status, body) = api
        .query(
            "docs",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body:?}");
    assert_eq!(body["error"]["code"], "STORE_UNAVAILABLE");
}

#[tokio::test]
async fn worker_apply_ignores_out_of_band_wal_not_enqueued_in_write_queue() {
    let api = TestApi::new();
    api.create_collection("docs", 2, "dot").await;

    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [{"id": "first", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let late_low_operation_id = "00000000000000000000000000000000-0000000000000000".to_string();
    let late_low_wal = WalRecord {
        operation_id: late_low_operation_id.clone(),
        collection: "docs".to_string(),
        namespace: "ns_a".to_string(),
        accepted_at: now_rfc3339(),
        idempotency_key: None,
        request: UpsertRequest {
            vectors: vec![UpsertVector {
                id: "late-low".to_string(),
                values: vec![0.0, 1.0],
                metadata: None,
            }],
            namespace: Some("ns_a".to_string()),
        },
    };
    let wal_bytes = serde_json::to_vec(&late_low_wal).expect("serialize custom wal");
    let wal_key = wal_object_key("docs", &late_low_operation_id);
    ObjectStore::put_bytes(&*api.store, &wal_key, &wal_bytes)
        .await
        .expect("write custom wal");

    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [{"id": "trigger", "values": [0.5, 0.5]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, stats) = api.stats("docs", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        stats["vector_count"], 2,
        "flush should only apply operations observed in the write queue"
    );

    let (status, query_response) = api
        .query(
            "docs",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a",
                "include_values": true
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let ids: Vec<String> = query_response["matches"]
        .as_array()
        .expect("matches")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert!(
        !ids.contains(&"late-low".to_string()),
        "out-of-band WAL should not be visible without queue admission, ids={ids:?}"
    );
}

#[tokio::test]
async fn delete_respects_queued_upserts_that_were_already_accepted() {
    let api = TestApi::new();
    api.create_collection("docs", 2, "dot").await;

    let (status, body) = api
        .upsert(
            "docs",
            json!({
                "vectors": [{"id": "queued-v1", "values": [1.0, 0.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "queueing upsert failed: {body:?}");
    assert_eq!(body["accepted"], true);

    let (status, delete_response) = api
        .delete_vectors(
            "docs",
            json!({
                "ids": ["queued-v1"],
                "namespace": "ns_a",
                "delete_all": false
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "delete failed: {delete_response:?}");
    assert_eq!(
        delete_response["deleted_count"], 1,
        "delete should observe queued accepted writes for ordering correctness"
    );

    let (status, _) = api
        .upsert_and_wait_applied(
            "docs",
            json!({
                "vectors": [{"id": "trigger", "values": [0.0, 1.0]}],
                "namespace": "ns_a",
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK);

    let (status, query_response) = api
        .query(
            "docs",
            json!({
                "vector": [1.0, 0.0],
                "top_k": 10,
                "namespace": "ns_a"
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let ids: Vec<String> = query_response["matches"]
        .as_array()
        .expect("matches")
        .iter()
        .map(|entry| entry["id"].as_str().expect("id").to_string())
        .collect();
    assert!(
        !ids.contains(&"queued-v1".to_string()),
        "queued-v1 should have been deleted, ids={ids:?}"
    );
}

#[tokio::test]
async fn fts_delta_apply_rewrites_only_touched_blocks_for_hot_term() {
    let api = TestApi::new();
    api.create_collection("fts_delta_rewrite", 2, "dot").await;
    let worker = api.spawn_worker(
        "fts-delta-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        true,
    );

    let initial_vectors = (0..1100_u32)
        .map(|index| {
            json!({
                "id": format!("doc-{index:04}"),
                "values": [1.0, 0.0],
                "metadata": {"body": "hot term"}
            })
        })
        .collect::<Vec<_>>();
    let (status, body) = api
        .upsert(
            "fts_delta_rewrite",
            json!({
                "vectors": initial_vectors,
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_delta_rewrite",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_delta_rewrite", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    let generation_1 = stats["generation"].as_u64().expect("generation");
    let body_field_hash = fts_field_hash("body");
    let hot_term_hash = fts_term_hash("hot");
    let index_meta_v1: FtsIndexMeta = api
        .wait_for_fts_index_meta(
            "fts_delta_rewrite",
            "ns_a",
            generation_1,
            Duration::from_secs(3),
            Duration::from_millis(25),
        )
        .await;
    let field_meta_v1 = index_meta_v1
        .fields
        .get(&body_field_hash)
        .expect("body field should exist in v1");
    let hot_term_ref_v1 = field_meta_v1
        .terms
        .get(&hot_term_hash)
        .expect("hot term should exist in v1");
    let term_meta_v1: TermMeta = api
        .load_fts_term_meta(
            "fts_delta_rewrite",
            "ns_a",
            hot_term_ref_v1.generation,
            &body_field_hash,
            &hot_term_hash,
        )
        .await;
    assert!(
        term_meta_v1.blocks.len() >= 2,
        "hot term should span multiple blocks to validate touched-block rewrites"
    );

    let (status, body) = api
        .upsert(
            "fts_delta_rewrite",
            json!({
                "vectors": [{
                    "id": "doc-0000",
                    "values": [1.0, 0.0],
                    "metadata": {"body": "warm term"}
                }],
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "delta upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_delta_rewrite",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_delta_rewrite", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    let generation_2 = stats["generation"].as_u64().expect("generation");
    assert!(generation_2 > generation_1);
    let index_meta_v2: FtsIndexMeta = api
        .wait_for_fts_index_meta(
            "fts_delta_rewrite",
            "ns_a",
            generation_2,
            Duration::from_secs(3),
            Duration::from_millis(25),
        )
        .await;
    let field_meta_v2 = index_meta_v2
        .fields
        .get(&body_field_hash)
        .expect("body field should exist in v2");
    let hot_term_ref_v2 = field_meta_v2
        .terms
        .get(&hot_term_hash)
        .expect("hot term should exist in v2");
    let term_meta_v2: TermMeta = api
        .load_fts_term_meta(
            "fts_delta_rewrite",
            "ns_a",
            hot_term_ref_v2.generation,
            &body_field_hash,
            &hot_term_hash,
        )
        .await;

    let reused_from_previous = term_meta_v2
        .blocks
        .iter()
        .filter(|block| block.generation == generation_1)
        .count();
    let rewritten_in_current = term_meta_v2
        .blocks
        .iter()
        .filter(|block| block.generation == generation_2)
        .count();
    assert!(
        reused_from_previous > 0,
        "delta apply should retain untouched blocks from previous generation"
    );
    assert!(
        rewritten_in_current > 0,
        "delta apply should rewrite at least one touched block in new generation"
    );
    assert!(
        rewritten_in_current < term_meta_v1.blocks.len(),
        "single-doc delta should not rewrite every hot-term block"
    );
    worker.abort();
}

#[tokio::test]
async fn fts_long_query_metadata_pass_reduces_payload_decodes() {
    let api = TestApi::new();
    api.create_collection("fts_metadata_skip", 2, "dot").await;
    let worker = api.spawn_worker(
        "fts-metadata-skip-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let mut vectors = Vec::new();
    vectors.push(json!({
        "id": "doc-champion",
        "values": [1.0, 0.0],
        "metadata": {
            "body": "alpha alpha alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi omicron"
        }
    }));
    for index in 0..3600_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:04}"),
            "values": [0.0, 1.0],
            "metadata": {
                "body": "alpha"
            }
        }));
    }

    let (status, body) = api
        .upsert(
            "fts_metadata_skip",
            json!({
                "vectors": vectors,
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_metadata_skip",
        &operation_id,
        Duration::from_secs(10),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_metadata_skip", Some("default")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let _ = api
        .wait_for_fts_index_meta(
            "fts_metadata_skip",
            "default",
            generation,
            Duration::from_secs(8),
            Duration::from_millis(25),
        )
        .await;

    let (status, response) = api
        .request(
            Method::POST,
            "/v1/namespaces/fts_metadata_skip/query",
            Some(json!({
                "rank_by": ["text", "BM25", "alpha beta gamma delta epsilon"],
                "top_k": 1,
                "include_attributes": false
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response:?}");
    assert_eq!(response["rows"][0]["id"], "doc-champion");
    let lexical = &response["billing"]["lexical_execution"];
    let header_reads = lexical["header_reads"].as_u64().unwrap_or(0);
    let blocks_decoded = lexical["blocks_decoded"].as_u64().unwrap_or(0);
    assert!(
        header_reads > 0 && blocks_decoded > 0,
        "expected non-zero lexical telemetry counters: {response:?}"
    );
    assert!(
        blocks_decoded < header_reads,
        "metadata pass should decode fewer payload blocks than headers visited (decoded={blocks_decoded}, headers={header_reads})"
    );
    assert!(
        (blocks_decoded as f64) <= (header_reads as f64 * 0.50),
        "decoded blocks should be <=50% of visited headers for long query (decoded={blocks_decoded}, headers={header_reads})"
    );
    worker.abort();
}

#[tokio::test]
async fn fts_block_packs_reduce_cold_query_object_get_count() {
    let store = Arc::new(InMemoryObjectStore::default());
    let build_storage = store.clone() as Arc<dyn ObjectStore>;
    let api = TestApi::new_with_storage(build_storage, store.clone());
    api.create_collection("fts_pack_gets", 2, "dot").await;
    let worker = api.spawn_worker(
        "fts-pack-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let vectors = (0..3200_u32)
        .map(|index| {
            json!({
                "id": format!("doc-{index:04}"),
                "values": [1.0, 0.0],
                "metadata": {"body": "alpha alpha alpha"}
            })
        })
        .collect::<Vec<_>>();
    let (status, body) = api
        .upsert(
            "fts_pack_gets",
            json!({
                "vectors": vectors,
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_pack_gets",
        &operation_id,
        Duration::from_secs(10),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_pack_gets", Some("default")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let index_meta: FtsIndexMeta = api
        .wait_for_fts_index_meta(
            "fts_pack_gets",
            "default",
            generation,
            Duration::from_secs(8),
            Duration::from_millis(25),
        )
        .await;
    let field_hash = fts_field_hash("body");
    let term_hash = fts_term_hash("alpha");
    let term_ref = index_meta
        .fields
        .get(&field_hash)
        .expect("field meta")
        .terms
        .get(&term_hash)
        .expect("term ref");
    let term_meta = api
        .load_fts_term_meta(
            "fts_pack_gets",
            "default",
            term_ref.generation,
            &field_hash,
            &term_hash,
        )
        .await;
    assert!(
        term_meta.blocks.len() >= 8,
        "expected enough blocks to validate pack GET reduction"
    );
    let unique_pack_ids = term_meta
        .blocks
        .iter()
        .map(|block| block.pack_id.clone())
        .collect::<BTreeSet<_>>();
    assert!(
        unique_pack_ids.len() < term_meta.blocks.len(),
        "expected multiple blocks to share pack objects"
    );
    worker.abort();

    let counting_store = Arc::new(CountingGetStore::new(store.clone()));
    let query_storage = counting_store.clone() as Arc<dyn ObjectStore>;
    let query_api = TestApi::new_with_storage(query_storage, store.clone());
    let (status, response) = query_api
        .request(
            Method::POST,
            "/v1/namespaces/fts_pack_gets/query",
            Some(json!({
                "rank_by": ["text", "BM25", "alpha alpha alpha alpha alpha"],
                "top_k": 5,
                "include_attributes": false
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{response:?}");

    let mut pack_get_count = 0_u64;
    for pack_id in &unique_pack_ids {
        let key = fts_block_pack_key(
            "fts_pack_gets",
            "default",
            generation,
            &field_hash,
            &term_hash,
            pack_id,
        );
        pack_get_count = pack_get_count.saturating_add(counting_store.get_count(&key).await);
    }
    assert!(
        pack_get_count > 0,
        "expected at least one cold pack fetch during lexical query"
    );
    let baseline_block_gets = term_meta.blocks.len() as f64;
    let cold_get_ratio = pack_get_count as f64 / baseline_block_gets;
    assert!(
        cold_get_ratio <= 0.60,
        "cold lexical query pack GET ratio should be <=60% of one-object-per-block baseline (ratio={cold_get_ratio:.4})"
    );
}

#[tokio::test]
async fn fts_lexical_cold_and_warm_query_gates_report_fetch_and_skip_metrics() {
    let store = Arc::new(InMemoryObjectStore::default());
    let build_storage = store.clone() as Arc<dyn ObjectStore>;
    let api = TestApi::new_with_storage(build_storage, store.clone());
    api.create_collection("fts_cold_warm_gates", 2, "dot").await;
    let worker = api.spawn_worker(
        "fts-cold-warm-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let mut vectors = Vec::new();
    vectors.push(json!({
        "id": "doc-best",
        "values": [1.0, 0.0],
        "metadata": {
            "body": "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu"
        }
    }));
    for index in 0..2200_u32 {
        vectors.push(json!({
            "id": format!("doc-{index:04}"),
            "values": [0.0, 1.0],
            "metadata": {"body": "alpha"}
        }));
    }

    let (status, body) = api
        .upsert(
            "fts_cold_warm_gates",
            json!({
                "vectors": vectors,
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_cold_warm_gates",
        &operation_id,
        Duration::from_secs(12),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_cold_warm_gates", Some("default")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let _ = api
        .wait_for_fts_index_meta(
            "fts_cold_warm_gates",
            "default",
            generation,
            Duration::from_secs(8),
            Duration::from_millis(25),
        )
        .await;
    let fts_prefix = format!("collections/fts_cold_warm_gates/indexes/default/{generation}/fts/");
    let all_fts_keys = store.keys_with_prefix(&fts_prefix).await;
    let pack_keys = all_fts_keys
        .iter()
        .filter(|key| key.contains("/packs/"))
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        !pack_keys.is_empty(),
        "fixture should produce packed postings for cold/warm benchmark gate"
    );

    let counting_store = Arc::new(CountingGetStore::new(store.clone()));
    let query_storage = counting_store.clone() as Arc<dyn ObjectStore>;
    let query_api = TestApi::new_with_storage(query_storage, store.clone());
    let request_body = json!({
        "rank_by": ["text", "BM25", "alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu"],
        "top_k": 5,
        "include_attributes": false
    });

    let cold_started = Instant::now();
    let (status, cold_response) = query_api
        .request(
            Method::POST,
            "/v1/namespaces/fts_cold_warm_gates/query",
            Some(request_body.clone()),
            &[],
        )
        .await;
    let cold_ms = cold_started.elapsed().as_secs_f64() * 1_000.0;
    assert_eq!(status, StatusCode::OK, "{cold_response:?}");

    let warm_started = Instant::now();
    let (status, warm_response) = query_api
        .request(
            Method::POST,
            "/v1/namespaces/fts_cold_warm_gates/query",
            Some(request_body),
            &[],
        )
        .await;
    let warm_ms = warm_started.elapsed().as_secs_f64() * 1_000.0;
    assert_eq!(status, StatusCode::OK, "{warm_response:?}");

    let cold_lexical = &cold_response["billing"]["lexical_execution"];
    let blocks_decoded = cold_lexical["blocks_decoded"].as_u64().unwrap_or(0);
    let blocks_skipped = cold_lexical["blocks_skipped"].as_u64().unwrap_or(0);
    let skip_ratio = if blocks_decoded + blocks_skipped == 0 {
        0.0
    } else {
        blocks_skipped as f64 / (blocks_decoded + blocks_skipped) as f64
    };

    let mut object_fetch_count = 0_u64;
    let mut decoded_bytes = 0_u64;
    for key in &all_fts_keys {
        let count = counting_store.get_count(key).await;
        object_fetch_count = object_fetch_count.saturating_add(count);
        if key.contains("/packs/") && count > 0 {
            let bytes = store.get_bytes(key).await.expect("pack bytes");
            decoded_bytes = decoded_bytes.saturating_add((bytes.len() as u64) * count);
        }
    }

    println!(
        "lexical_cold_warm_gate cold_ms={cold_ms:.2} warm_ms={warm_ms:.2} object_fetches={} decoded_bytes={} skip_ratio={skip_ratio:.4}",
        object_fetch_count, decoded_bytes
    );

    assert!(
        warm_ms <= 150.0,
        "warm lexical p95 proxy should stay <=150ms (observed={warm_ms:.2}ms)"
    );
    assert!(
        cold_ms <= 500.0,
        "cold lexical p95 proxy should stay <=500ms (observed={cold_ms:.2}ms)"
    );
    assert!(
        object_fetch_count <= 400,
        "cold lexical object fetch count should remain bounded (observed={object_fetch_count})"
    );
    assert!(
        decoded_bytes > 0,
        "decoded-bytes reporting should be non-zero for cold lexical query"
    );
    assert!(
        skip_ratio > 0.0,
        "skip ratio should be >0 for long lexical query (ratio={skip_ratio:.4})"
    );
    worker.abort();
}

#[tokio::test]
async fn fts_publish_and_reload_restores_exact_postings_content() {
    let store = Arc::new(InMemoryObjectStore::default());
    let storage = store.clone() as Arc<dyn ObjectStore>;
    let api = TestApi::new_with_storage(storage.clone(), store.clone());
    api.create_collection("fts_publish_reload", 2, "dot").await;
    let worker = api.spawn_worker(
        "fts-reload-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        true,
    );

    let (status, body) = api
        .upsert(
            "fts_publish_reload",
            json!({
                "vectors": [
                    {"id": "doc-a", "values": [1.0, 0.0], "metadata": {"body": "alpha beta"}},
                    {"id": "doc-b", "values": [0.9, 0.1], "metadata": {"body": "alpha"}},
                    {"id": "doc-c", "values": [0.1, 0.9], "metadata": {"body": "gamma"}}
                ],
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_publish_reload",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_publish_reload", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let index_meta: FtsIndexMeta = api
        .wait_for_fts_index_meta(
            "fts_publish_reload",
            "ns_a",
            generation,
            Duration::from_secs(3),
            Duration::from_millis(25),
        )
        .await;
    let field_hash = fts_field_hash("body");
    let alpha_hash = fts_term_hash("alpha");
    let field_meta = index_meta
        .fields
        .get(&field_hash)
        .expect("body field should exist");
    let term_ref = field_meta
        .terms
        .get(&alpha_hash)
        .expect("alpha term should exist");

    let term_meta: TermMeta = api
        .load_fts_term_meta(
            "fts_publish_reload",
            "ns_a",
            term_ref.generation,
            &field_hash,
            &alpha_hash,
        )
        .await;
    let reader_api = TestApi::new_with_storage(storage, store.clone());
    let mut actual_doc_ids = Vec::new();
    let decoded_blocks = reader_api
        .load_decode_fts_term_blocks(
            "fts_publish_reload",
            "ns_a",
            &field_hash,
            &alpha_hash,
            &term_meta,
        )
        .await;
    for decoded in decoded_blocks {
        actual_doc_ids.extend(decoded.postings.into_iter().map(|posting| posting.doc_id));
    }
    actual_doc_ids.sort_unstable();
    let mut expected_doc_ids = vec![stable_doc_id("doc-a"), stable_doc_id("doc-b")];
    expected_doc_ids.sort_unstable();
    assert_eq!(actual_doc_ids, expected_doc_ids);
    worker.abort();
}

#[tokio::test]
async fn fts_publish_recovery_hides_partial_artifacts_until_meta_is_visible() {
    let store = Arc::new(InMemoryObjectStore::default());
    let failure_key = fts_index_meta_key("fts_crash_safe", "ns_a", 1);
    let storage = Arc::new(FailOncePutIfAbsentStore::new(
        store.clone(),
        failure_key.clone(),
    )) as Arc<dyn ObjectStore>;
    let api = TestApi::new_with_storage(storage, store.clone());
    api.create_collection("fts_crash_safe", 2, "dot").await;
    let worker = api.spawn_worker(
        "fts-crash-worker",
        Duration::from_millis(300),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        true,
    );

    let vectors = (0..300_u32)
        .map(|index| {
            json!({
                "id": format!("doc-{index:04}"),
                "values": [1.0, 0.0],
                "metadata": {"body": "crash hot"}
            })
        })
        .collect::<Vec<_>>();
    let (status, body) = api
        .upsert(
            "fts_crash_safe",
            json!({
                "vectors": vectors,
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_crash_safe",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    tokio::time::sleep(Duration::from_millis(40)).await;
    let partial_keys = store
        .keys_with_prefix("collections/fts_crash_safe/indexes/ns_a/1/fts/fields/")
        .await;
    assert!(
        !partial_keys.is_empty(),
        "expected partial term/block artifacts to exist after injected publish failure"
    );
    let meta_keys_before_retry = store.keys_with_prefix(&failure_key).await;
    assert!(
        meta_keys_before_retry.is_empty(),
        "FTS generation meta should not be visible after failed publish attempt"
    );

    let mut published = false;
    for _ in 0..20 {
        let keys = store.keys_with_prefix(&failure_key).await;
        if keys.iter().any(|key| key == &failure_key) {
            published = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        published,
        "worker should recover and publish FTS generation metadata on retry"
    );
    worker.abort();
}

#[tokio::test]
async fn fts_lexical_reindex_serves_old_or_new_generation_during_publish_recovery() {
    let store = Arc::new(InMemoryObjectStore::default());
    let failure_key = fts_index_meta_key("fts_reindex_recovery", "default", 2);
    let storage = Arc::new(FailOncePutIfAbsentStore::new(
        store.clone(),
        failure_key.clone(),
    )) as Arc<dyn ObjectStore>;
    let api = TestApi::new_with_storage(storage, store.clone());
    api.create_collection("fts_reindex_recovery", 2, "dot")
        .await;
    let worker = api.spawn_worker(
        "fts-reindex-recovery-worker",
        Duration::from_millis(50),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let (status, schema_v1) = api
        .request(
            Method::POST,
            "/v1/namespaces/fts_reindex_recovery/schema",
            Some(json!({
                "body": {
                    "type": "string",
                    "full_text_search": {"enabled": true, "tokenizer": "word_v1", "bm25": {"k1": 1.1, "b": 0.75}}
                }
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{schema_v1:?}");

    let (status, body) = api
        .upsert(
            "fts_reindex_recovery",
            json!({
                "vectors": [{
                    "id": "doc-1",
                    "values": [1.0, 0.0],
                    "metadata": {"body": "alpha token"}
                }],
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_reindex_recovery",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_reindex_recovery", Some("default")).await;
    assert_eq!(status, StatusCode::OK);
    let generation_1 = stats["generation"].as_u64().expect("generation");
    let _ = api
        .wait_for_fts_index_meta(
            "fts_reindex_recovery",
            "default",
            generation_1,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;

    let (status, pre_migration_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/fts_reindex_recovery/query",
            Some(json!({
                "rank_by": ["body", "BM25", "alpha"],
                "top_k": 3
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{pre_migration_query:?}");
    assert_eq!(pre_migration_query["rows"][0]["id"], "doc-1");

    let (status, schema_v2) = api
        .request(
            Method::POST,
            "/v1/namespaces/fts_reindex_recovery/schema",
            Some(json!({
                "body": {
                    "type": "string",
                    "full_text_search": {"enabled": true, "tokenizer": "word_v1", "bm25": {"k1": 1.8, "b": 0.2}}
                }
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{schema_v2:?}");

    let (status, body) = api
        .upsert(
            "fts_reindex_recovery",
            json!({
                "vectors": [{
                    "id": "doc-1",
                    "values": [1.0, 0.0],
                    "metadata": {"body": "alpha token"}
                }],
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "migration upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_reindex_recovery",
        &operation_id,
        Duration::from_secs(8),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_reindex_recovery", Some("default")).await;
    assert_eq!(status, StatusCode::OK);
    let generation_2 = stats["generation"].as_u64().expect("generation");
    assert_eq!(generation_2, 2, "migration write should advance generation");

    tokio::time::sleep(Duration::from_millis(40)).await;
    let meta_keys_before_retry = store.keys_with_prefix(&failure_key).await;
    assert!(
        meta_keys_before_retry.is_empty(),
        "new lexical generation meta should remain hidden after failed publish attempt"
    );

    // Reader queries should keep serving prior generation while migration publish is pending.
    let (status, stale_window_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/fts_reindex_recovery/query",
            Some(json!({
                "rank_by": ["body", "BM25", "alpha"],
                "top_k": 3
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{stale_window_query:?}");
    assert_eq!(stale_window_query["rows"][0]["id"], "doc-1");

    let mut published = false;
    for _ in 0..40 {
        let keys = store.keys_with_prefix(&failure_key).await;
        if keys.iter().any(|key| key == &failure_key) {
            published = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        published,
        "worker should recover and publish migrated tokenizer generation metadata"
    );
    let _ = api
        .wait_for_fts_index_meta(
            "fts_reindex_recovery",
            "default",
            generation_2,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;

    let (status, post_migration_query) = api
        .request(
            Method::POST,
            "/v1/namespaces/fts_reindex_recovery/query",
            Some(json!({
                "rank_by": ["body", "BM25", "alpha"],
                "top_k": 3
            })),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{post_migration_query:?}");
    assert_eq!(post_migration_query["rows"][0]["id"], "doc-1");
    worker.abort();
}

#[tokio::test]
async fn worker_index_flags_support_fts_only_ann_only_and_both() {
    let api = TestApi::new();
    let scenarios = vec![
        ("fts_only", true, false, true, false, 16_u32),
        ("ann_only", false, true, false, true, 2_000_u32),
        ("both", true, true, true, true, 2_000_u32),
    ];

    for (suffix, build_fts, build_ann, expect_fts, expect_ann, vector_count) in scenarios {
        let collection = format!("worker_flags_{suffix}");
        api.create_collection(&collection, 2, "dot").await;
        let worker = api.spawn_worker(
            &format!("{suffix}-worker"),
            Duration::from_millis(10),
            Duration::from_millis(10),
            0,
            Duration::from_millis(10),
            build_fts,
            build_ann,
        );

        let vectors = (0..vector_count)
            .map(|index| {
                json!({
                    "id": format!("{suffix}-doc-{index:04}"),
                    "values": [1.0, (index as f32) / (vector_count as f32)],
                    "metadata": {"body": format!("{suffix} token")}
                })
            })
            .collect::<Vec<_>>();
        let (status, body) = api
            .upsert(
                &collection,
                json!({
                    "vectors": vectors,
                    "namespace": "ns_a"
                }),
                &[],
            )
            .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "upsert failed for {suffix}: {body:?}"
        );
        let operation_id = body["operation_id"]
            .as_str()
            .expect("operation_id")
            .to_string();
        api.wait_for_operation_applied(
            &collection,
            &operation_id,
            Duration::from_secs(8),
            Duration::from_millis(25),
        )
        .await;

        let (status, stats) = api.stats(&collection, Some("ns_a")).await;
        assert_eq!(status, StatusCode::OK);
        let generation = stats["generation"].as_u64().expect("generation");
        let fts_exists = if expect_fts {
            let _: FtsIndexMeta = api
                .wait_for_fts_index_meta(
                    &collection,
                    "ns_a",
                    generation,
                    Duration::from_secs(6),
                    Duration::from_millis(25),
                )
                .await;
            true
        } else {
            api.store
                .get_bytes(&fts_index_meta_key(&collection, "ns_a", generation))
                .await
                .is_ok()
        };
        let ann_key = ann_index_meta_key(&collection, "ns_a", generation);
        let ann_exists = if expect_ann {
            let mut found = false;
            for _ in 0..240 {
                if api.store.get_bytes(&ann_key).await.is_ok() {
                    found = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            found
        } else {
            api.store.get_bytes(&ann_key).await.is_ok()
        };
        assert_eq!(
            fts_exists, expect_fts,
            "unexpected FTS index publish state for {suffix}"
        );
        assert_eq!(
            ann_exists, expect_ann,
            "unexpected ANN index publish state for {suffix}"
        );
        worker.abort();
    }
}

#[tokio::test]
async fn fts_bootstrap_materializes_from_current_vectors_without_previous_fts_meta() {
    let api = TestApi::new();
    api.create_collection("fts_bootstrap_recovery", 2, "dot")
        .await;

    let queue_only_worker = api.spawn_worker(
        "bootstrap-queue-only-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        false,
        false,
    );
    let (status, body) = api
        .upsert(
            "fts_bootstrap_recovery",
            json!({
                "vectors": [{
                    "id": "doc-1",
                    "values": [1.0, 0.0],
                    "metadata": {"body": "alpha alpha"}
                }],
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "initial upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_bootstrap_recovery",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_bootstrap_recovery", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    let generation_1 = stats["generation"].as_u64().expect("generation");
    assert!(
        api.store
            .get_bytes(&fts_index_meta_key(
                "fts_bootstrap_recovery",
                "ns_a",
                generation_1
            ))
            .await
            .is_err(),
        "first generation should intentionally have no FTS metadata in this setup"
    );
    queue_only_worker.abort();

    let fts_worker = api.spawn_worker(
        "bootstrap-fts-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );
    let (status, body) = api
        .upsert(
            "fts_bootstrap_recovery",
            json!({
                "vectors": [{
                    "id": "doc-1",
                    "values": [0.7, 0.3],
                    "metadata": {"body": "alpha alpha"}
                }],
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "follow-up upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_bootstrap_recovery",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_bootstrap_recovery", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    let generation_2 = stats["generation"].as_u64().expect("generation");
    assert!(generation_2 > generation_1);

    let body_field_hash = fts_field_hash("body");
    let alpha_hash = fts_term_hash("alpha");
    let index_meta: FtsIndexMeta = api
        .wait_for_fts_index_meta(
            "fts_bootstrap_recovery",
            "ns_a",
            generation_2,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;
    let field_meta = index_meta
        .fields
        .get(&body_field_hash)
        .expect("body field should be materialized");
    let alpha_ref = field_meta
        .terms
        .get(&alpha_hash)
        .expect("alpha term should be materialized");
    let alpha_term_meta: TermMeta = api
        .load_fts_term_meta(
            "fts_bootstrap_recovery",
            "ns_a",
            alpha_ref.generation,
            &body_field_hash,
            &alpha_hash,
        )
        .await;
    assert_eq!(alpha_term_meta.document_frequency, 1);
    assert_eq!(
        alpha_term_meta.total_term_frequency, 2,
        "bootstrap materialization should account full current-visible tf"
    );
    fts_worker.abort();
}

#[tokio::test]
async fn fts_term_meta_total_term_frequency_matches_exact_tf_sum() {
    let api = TestApi::new();
    api.create_collection("fts_tf_exact", 2, "dot").await;
    let worker = api.spawn_worker(
        "fts-tf-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let (status, body) = api
        .upsert(
            "fts_tf_exact",
            json!({
                "vectors": [
                    {"id": "doc-a", "values": [1.0, 0.0], "metadata": {"body": "alpha alpha beta"}},
                    {"id": "doc-b", "values": [0.9, 0.1], "metadata": {"body": "alpha"}},
                    {"id": "doc-c", "values": [0.2, 0.8], "metadata": {"body": "beta beta beta"}}
                ],
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_tf_exact",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_tf_exact", Some("ns_a")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let body_field_hash = fts_field_hash("body");
    let alpha_hash = fts_term_hash("alpha");
    let index_meta: FtsIndexMeta = api
        .wait_for_fts_index_meta(
            "fts_tf_exact",
            "ns_a",
            generation,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;
    let alpha_ref = index_meta
        .fields
        .get(&body_field_hash)
        .expect("body field")
        .terms
        .get(&alpha_hash)
        .expect("alpha term");
    let alpha_meta: TermMeta = api
        .load_fts_term_meta(
            "fts_tf_exact",
            "ns_a",
            alpha_ref.generation,
            &body_field_hash,
            &alpha_hash,
        )
        .await;
    let decoded_blocks = api
        .load_decode_fts_term_blocks(
            "fts_tf_exact",
            "ns_a",
            &body_field_hash,
            &alpha_hash,
            &alpha_meta,
        )
        .await;
    let exact_tf_sum = decoded_blocks
        .iter()
        .flat_map(|block| block.postings.iter())
        .fold(0_u64, |sum, posting| sum.saturating_add(posting.tf as u64));
    assert_eq!(alpha_meta.document_frequency, 2);
    assert_eq!(exact_tf_sum, 3);
    assert_eq!(alpha_meta.total_term_frequency, exact_tf_sum);
    worker.abort();
}

#[tokio::test]
async fn fts_index_meta_persists_field_corpus_stats_for_bm25() {
    let api = TestApi::new();
    api.create_collection("fts_corpus_stats", 2, "dot").await;
    let worker = api.spawn_worker(
        "fts-corpus-stats-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        false,
    );

    let (status, body) = api
        .upsert(
            "fts_corpus_stats",
            json!({
                "vectors": [
                    {"id": "doc-a", "values": [1.0, 0.0], "metadata": {"body": "alpha alpha beta"}},
                    {"id": "doc-b", "values": [0.9, 0.1], "metadata": {"body": "alpha"}},
                    {"id": "doc-c", "values": [0.2, 0.8], "metadata": {"body": "beta beta beta"}}
                ],
                "namespace": "default"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_corpus_stats",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, stats) = api.stats("fts_corpus_stats", Some("default")).await;
    assert_eq!(status, StatusCode::OK);
    let generation = stats["generation"].as_u64().expect("generation");
    let index_meta: FtsIndexMeta = api
        .wait_for_fts_index_meta(
            "fts_corpus_stats",
            "default",
            generation,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;
    let body_field = index_meta
        .fields
        .get(&fts_field_hash("body"))
        .expect("body field metadata");
    assert_eq!(body_field.corpus_stats.document_count, 3);
    assert_eq!(body_field.corpus_stats.sum_doc_len, 7);
    assert!(
        (body_field.corpus_stats.avg_doc_len - (7.0_f32 / 3.0_f32)).abs() < 1e-6,
        "unexpected avg_doc_len: {}",
        body_field.corpus_stats.avg_doc_len
    );
    worker.abort();
}

#[tokio::test]
async fn fts_then_ann_build_reuses_current_generation_segment_load() {
    let store = Arc::new(InMemoryObjectStore::default());
    let counting_store = Arc::new(CountingGetStore::new(store.clone()));
    let storage = counting_store.clone() as Arc<dyn ObjectStore>;
    let api = TestApi::new_with_storage(storage, store);
    api.create_collection("fts_ann_reload_churn", 2, "dot")
        .await;
    let worker = api.spawn_worker(
        "fts-ann-reload-worker",
        Duration::from_millis(10),
        Duration::from_millis(10),
        0,
        Duration::from_millis(10),
        true,
        true,
    );

    let (status, body) = api
        .upsert(
            "fts_ann_reload_churn",
            json!({
                "vectors": [{
                    "id": "doc-1",
                    "values": [1.0, 0.0],
                    "metadata": {"body": "alpha"}
                }],
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "seed upsert failed: {body:?}");
    let operation_id = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    api.wait_for_operation_applied(
        "fts_ann_reload_churn",
        &operation_id,
        Duration::from_secs(5),
        Duration::from_millis(25),
    )
    .await;

    let (status, body) = api
        .upsert(
            "fts_ann_reload_churn",
            json!({
                "vectors": [{
                    "id": "doc-2",
                    "values": [0.9, 0.1],
                    "metadata": {"body": "alpha"}
                }],
                "namespace": "ns_a"
            }),
            &[],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "delta upsert failed: {body:?}");
    let operation_id_2 = body["operation_id"]
        .as_str()
        .expect("operation_id")
        .to_string();
    let apply_status = api
        .wait_for_operation_applied(
            "fts_ann_reload_churn",
            &operation_id_2,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;
    let generation_2 = apply_status["generation"].as_u64().expect("generation");
    let _: FtsIndexMeta = api
        .wait_for_fts_index_meta(
            "fts_ann_reload_churn",
            "ns_a",
            generation_2,
            Duration::from_secs(5),
            Duration::from_millis(25),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(150)).await;

    let second_segment_key = segment_object_key("fts_ann_reload_churn", &operation_id_2);
    let current_segment_reads = counting_store.get_count(&second_segment_key).await;
    assert_eq!(
        current_segment_reads, 1,
        "FTS->ANN index sequence should reuse current-generation namespace cache without reloading segment '{second_segment_key}'"
    );
    worker.abort();
}
