use std::cmp::Ordering;

use serde_json::Value;

use super::ast::{compare_scalar_json_values, tokenize_text, MetadataFilterExpression};

pub(crate) fn metadata_matches_filter(
    metadata: Option<&Value>,
    filter: Option<&MetadataFilterExpression>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };
    evaluate_filter(metadata, filter)
}

fn evaluate_filter(metadata: Option<&Value>, filter: &MetadataFilterExpression) -> bool {
    match filter {
        MetadataFilterExpression::Eq { field, value } => {
            metadata_field(metadata, field).is_some_and(|actual| actual == value)
        }
        MetadataFilterExpression::Ne { field, value } => {
            metadata_field(metadata, field).is_some_and(|actual| actual != value)
        }
        MetadataFilterExpression::Lt { field, value } => metadata_field(metadata, field)
            .and_then(|actual| compare_scalar_json_values(actual, value))
            .is_some_and(Ordering::is_lt),
        MetadataFilterExpression::Lte { field, value } => metadata_field(metadata, field)
            .and_then(|actual| compare_scalar_json_values(actual, value))
            .is_some_and(|ordering| ordering.is_lt() || ordering.is_eq()),
        MetadataFilterExpression::Gt { field, value } => metadata_field(metadata, field)
            .and_then(|actual| compare_scalar_json_values(actual, value))
            .is_some_and(Ordering::is_gt),
        MetadataFilterExpression::Gte { field, value } => metadata_field(metadata, field)
            .and_then(|actual| compare_scalar_json_values(actual, value))
            .is_some_and(|ordering| ordering.is_gt() || ordering.is_eq()),
        MetadataFilterExpression::In { field, values } => {
            metadata_field(metadata, field).is_some_and(|actual| contains_any_value(actual, values))
        }
        MetadataFilterExpression::NotIn { field, values } => metadata_field(metadata, field)
            .is_some_and(|actual| !contains_any_value(actual, values)),
        MetadataFilterExpression::ContainsAny { field, values } => {
            metadata_field(metadata, field).is_some_and(|actual| contains_any_value(actual, values))
        }
        MetadataFilterExpression::ContainsAllTokens { field, tokens } => {
            metadata_field(metadata, field)
                .and_then(extract_token_set)
                .is_some_and(|actual_tokens| {
                    tokens.iter().all(|token| actual_tokens.contains(token))
                })
        }
        MetadataFilterExpression::Glob { field, matcher, .. } => metadata_field(metadata, field)
            .and_then(Value::as_str)
            .is_some_and(|actual| matcher.is_match(actual)),
        MetadataFilterExpression::Regex { field, matcher, .. } => metadata_field(metadata, field)
            .and_then(Value::as_str)
            .is_some_and(|actual| matcher.is_match(actual)),
        MetadataFilterExpression::And(children) => children
            .iter()
            .all(|child| evaluate_filter(metadata, child)),
        MetadataFilterExpression::Or(children) => children
            .iter()
            .any(|child| evaluate_filter(metadata, child)),
        MetadataFilterExpression::Not(child) => !evaluate_filter(metadata, child),
    }
}

fn metadata_field<'a>(metadata: Option<&'a Value>, field: &str) -> Option<&'a Value> {
    let Value::Object(metadata) = metadata? else {
        return None;
    };
    metadata.get(field)
}

fn contains_any_value(actual: &Value, values: &[Value]) -> bool {
    match actual {
        Value::Array(entries) => entries
            .iter()
            .any(|entry| values.iter().any(|candidate| candidate == entry)),
        _ => values.iter().any(|candidate| candidate == actual),
    }
}

fn extract_token_set(actual: &Value) -> Option<Vec<String>> {
    match actual {
        Value::String(text) => Some(tokenize_text(text)),
        Value::Array(entries) => {
            let mut tokens = Vec::new();
            for entry in entries {
                let Value::String(text) = entry else {
                    continue;
                };
                tokens.extend(tokenize_text(text));
            }
            Some(tokens)
        }
        _ => None,
    }
}
