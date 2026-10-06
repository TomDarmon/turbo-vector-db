use serde_json::Value;
use std::collections::BTreeSet;
use turbo_vector_core::Metric;

use crate::{
    error::ApiError,
    filters::{metadata_matches_filter as metadata_matches_filter_impl, MetadataFilterExpression},
    models::UpsertVector,
    scoring::exact_metric_score,
};

pub(crate) fn validate_upsert_vectors(
    dimension: u32,
    vectors: &[UpsertVector],
) -> Result<(), ApiError> {
    for vector in vectors {
        if vector.id.trim().is_empty() {
            return Err(ApiError::invalid_argument("vector id must not be empty"));
        }
        if vector.values.len() != dimension as usize {
            return Err(ApiError::invalid_argument(format!(
                "vector '{}' has dimension {}, expected {}",
                vector.id,
                vector.values.len(),
                dimension
            )));
        }
        if vector.values.iter().any(|v| !v.is_finite()) {
            return Err(ApiError::invalid_argument(format!(
                "vector '{}' contains non-finite float values",
                vector.id
            )));
        }
        if let Some(metadata) = vector.metadata.as_ref() {
            if !metadata.is_object() {
                return Err(ApiError::invalid_argument(format!(
                    "vector '{}' metadata must be a JSON object",
                    vector.id
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_query_request(
    dimension: u32,
    vector: &[f32],
    top_k: u32,
) -> Result<(), ApiError> {
    if top_k == 0 {
        return Err(ApiError::invalid_argument("top_k must be > 0"));
    }
    if vector.len() != dimension as usize {
        return Err(ApiError::invalid_argument(format!(
            "query vector has dimension {}, expected {}",
            vector.len(),
            dimension
        )));
    }
    if vector.iter().any(|v| !v.is_finite()) {
        return Err(ApiError::invalid_argument(
            "query vector contains non-finite float values",
        ));
    }
    Ok(())
}

pub(crate) fn metadata_matches_filter(
    metadata: Option<&Value>,
    filter: Option<&MetadataFilterExpression>,
) -> bool {
    metadata_matches_filter_impl(metadata, filter)
}

pub(crate) fn compute_score(metric: &Metric, query: &[f32], candidate: &[f32]) -> f32 {
    exact_metric_score(metric, query, candidate)
}

pub(crate) fn validate_collection_name(name: &str) -> Result<(), ApiError> {
    validate_key_component(name, "collection name")
}

pub(crate) fn normalize_namespace(namespace: Option<&str>) -> Result<String, ApiError> {
    let out = namespace.unwrap_or("default").trim();
    validate_key_component(out, "namespace")?;
    Ok(out.to_string())
}

pub(crate) fn normalize_delete_ids(ids: Option<Vec<String>>) -> Result<Vec<String>, ApiError> {
    let Some(ids) = ids else {
        return Ok(Vec::new());
    };
    if ids.is_empty() {
        return Err(ApiError::invalid_argument(
            "ids must not be empty when provided",
        ));
    }

    let mut deduped = Vec::new();
    let mut seen = BTreeSet::new();
    for id in ids {
        let normalized = id.trim();
        if normalized.is_empty() {
            return Err(ApiError::invalid_argument("vector id must not be empty"));
        }
        if seen.insert(normalized.to_string()) {
            deduped.push(normalized.to_string());
        }
    }
    Ok(deduped)
}

fn validate_key_component(value: &str, field: &str) -> Result<(), ApiError> {
    if value.is_empty() {
        return Err(ApiError::invalid_argument(format!(
            "{field} must not be empty"
        )));
    }
    if value.len() > 128 {
        return Err(ApiError::invalid_argument(format!("{field} is too long")));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
    {
        return Err(ApiError::invalid_argument(format!(
            "{field} can only contain [A-Za-z0-9_.-]"
        )));
    }
    Ok(())
}
