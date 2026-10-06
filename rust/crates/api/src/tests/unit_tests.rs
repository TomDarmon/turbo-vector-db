use axum::http::StatusCode;
use serde_json::{json, Value};
use turbo_vector_core::Metric;

use crate::error::ApiError;
use crate::filters::{
    parse_metadata_filter_with_limits, parser::FilterParserLimits, MetadataFilterExpression,
};
use crate::keys::new_operation_id;
use crate::validation::{
    compute_score, metadata_matches_filter, normalize_delete_ids, validate_collection_name,
    validate_query_request,
};
use crate::{env_process_role, ProcessRole};

fn parse_metadata_filter(
    filter: Option<Value>,
) -> Result<Option<MetadataFilterExpression>, ApiError> {
    parse_metadata_filter_with_limits(filter, FilterParserLimits::default())
}

#[test]
fn validate_query_request_rejects_invalid_shapes() {
    let err = validate_query_request(3, &[1.0, 2.0], 10).expect_err("dimension must match");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);

    let err = validate_query_request(3, &[1.0, 2.0, 3.0], 0).expect_err("top_k must be > 0");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);

    let err = validate_query_request(3, &[1.0, f32::NAN, 3.0], 10).expect_err("finite values only");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
}

#[test]
fn parse_metadata_filter_rejects_non_tuple_expression() {
    let err = parse_metadata_filter(Some(Value::String("nope".to_string())))
        .expect_err("filter must be tuple-expression");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);

    let err = parse_metadata_filter(Some(json!({ "topic": "rust" })))
        .expect_err("object filter shape should be rejected");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
}

#[test]
fn metadata_filter_supports_comparison_and_not_logic() {
    let filter = parse_metadata_filter(Some(json!([
        "And",
        ["topic", "Eq", "rust"],
        ["lang", "Ne", "fr"],
        ["score", "Gte", 10],
        ["Not", ["priority", "Lt", 3]]
    ])))
    .expect("valid tuple filter")
    .expect("parsed filter");

    assert!(metadata_matches_filter(
        Some(&json!({ "topic": "rust", "lang": "en", "score": 12, "priority": 5 })),
        Some(&filter)
    ));
    assert!(!metadata_matches_filter(
        Some(&json!({ "topic": "rust", "lang": "fr", "score": 12, "priority": 5 })),
        Some(&filter)
    ));
    assert!(!metadata_matches_filter(
        Some(&json!({ "topic": "rust", "lang": "en", "score": 12, "priority": 2 })),
        Some(&filter)
    ));
}

#[test]
fn metadata_filter_supports_set_and_pattern_operators() {
    let filter = parse_metadata_filter(Some(json!([
        "And",
        ["topic", "In", ["rust", "db", "rust"]],
        ["tag", "NotIn", ["legacy"]],
        ["labels", "ContainsAny", ["vec", "search"]],
        ["title", "ContainsAllTokens", ["Native", "Filtering"]],
        ["path", "Glob", "foo/src/*"],
        ["Regex", "title", "native\\s+filtering"]
    ])))
    .expect("tuple filter should parse")
    .expect("tuple filter");

    assert!(metadata_matches_filter(
        Some(&json!({
            "topic": "rust",
            "tag": "stable",
            "labels": ["vec", "ann"],
            "title": "native filtering for ann",
            "path": "foo/src/main.rs"
        })),
        Some(&filter)
    ));
    assert!(!metadata_matches_filter(
        Some(&json!({
            "topic": "rust",
            "tag": "legacy",
            "labels": ["vec", "ann"],
            "title": "native filtering for ann",
            "path": "foo/src/main.rs"
        })),
        Some(&filter)
    ));
}

#[test]
fn metadata_filter_supports_operator_first_tuple_form() {
    let filter = parse_metadata_filter(Some(json!(["Eq", "topic", "rust"])))
        .expect("operator-first tuple should parse")
        .expect("operator-first filter");
    assert!(metadata_matches_filter(
        Some(&json!({ "topic": "rust" })),
        Some(&filter)
    ));
    assert!(!metadata_matches_filter(
        Some(&json!({ "topic": "db" })),
        Some(&filter)
    ));
}

#[test]
fn parse_metadata_filter_rejects_invalid_operator_shapes() {
    let err = parse_metadata_filter(Some(json!(["topic", "In", "rust"])))
        .expect_err("In requires array values");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);

    let err =
        parse_metadata_filter(Some(json!(["path", "Regex", "["]))).expect_err("Regex must compile");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
}

#[test]
fn compute_score_prefers_closer_vectors() {
    let query = [1.0, 0.0];
    let aligned = [1.0, 0.0];
    let orthogonal = [0.0, 1.0];
    assert!(
        compute_score(&Metric::Cosine, &query, &aligned)
            > compute_score(&Metric::Cosine, &query, &orthogonal)
    );
    assert!(
        compute_score(&Metric::Dot, &query, &aligned)
            > compute_score(&Metric::Dot, &query, &orthogonal)
    );

    let near = [1.1, 0.1];
    let far = [5.0, 5.0];
    assert!(
        compute_score(&Metric::Euclidean, &query, &near)
            > compute_score(&Metric::Euclidean, &query, &far)
    );
}

#[test]
fn normalize_delete_ids_rejects_empty_entries_and_dedupes() {
    let ids = normalize_delete_ids(Some(vec![
        "  a ".to_string(),
        "a".to_string(),
        "b".to_string(),
    ]))
    .expect("ids should normalize");
    assert_eq!(ids, vec!["a", "b"]);

    let err =
        normalize_delete_ids(Some(vec!["".to_string()])).expect_err("empty ids must be rejected");
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
}

#[test]
fn operation_ids_are_monotonic_per_process() {
    let mut previous: Option<String> = None;
    for _ in 0..512 {
        let next = new_operation_id();
        if let Some(prev) = previous.as_ref() {
            assert!(
                next > *prev,
                "operation ids must increase to keep queue watermark ordering safe"
            );
        }
        previous = Some(next);
    }
}

#[test]
fn collection_name_validation_accepts_period_for_compatibility() {
    validate_collection_name("namespace.with.dots")
        .expect("dot-separated namespace should be accepted");
}

#[test]
fn process_role_parser_accepts_broker_role() {
    let key = "TV_PROCESS_ROLE_TEST_ACCEPTS_BROKER";
    std::env::set_var(key, "broker");
    let parsed = env_process_role(key, ProcessRole::Api).expect("broker role should be supported");
    std::env::remove_var(key);
    assert_eq!(parsed.as_str(), "broker");
}
