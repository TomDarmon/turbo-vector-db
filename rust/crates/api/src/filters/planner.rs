use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
};

use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{error::ApiError, keys::sha256_hex};

use super::ast::{compare_scalar_json_values, MetadataFilterExpression};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FilterValueType {
    String,
    Number,
    Bool,
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FilterValueLexiconEntry {
    pub(crate) value: Value,
    pub(crate) value_type: FilterValueType,
    pub(crate) term_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct FilterTokenLexiconEntry {
    pub(crate) token: String,
    pub(crate) term_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct FilterIndexMeta {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) value_lexicon: BTreeMap<String, Vec<FilterValueLexiconEntry>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) token_lexicon: BTreeMap<String, Vec<FilterTokenLexiconEntry>>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct NativeFilterClusterSummary {
    pub(crate) bucket_bitmap: RoaringBitmap,
    pub(crate) bucket_match_counts: BTreeMap<u32, u32>,
}

impl NativeFilterClusterSummary {
    pub(crate) fn estimated_size_bytes(&self) -> usize {
        self.bucket_bitmap.serialized_size().saturating_add(
            self.bucket_match_counts
                .len()
                .saturating_mul(size_of::<u32>().saturating_mul(2)),
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) enum NativeFilterExpression {
    MatchAll,
    MatchNone,
    Term(String),
    And(Vec<NativeFilterExpression>),
    Or(Vec<NativeFilterExpression>),
    Not(Box<NativeFilterExpression>),
}

#[derive(Debug, Clone)]
pub(crate) struct NativeFilterPlan {
    pub(crate) expression: NativeFilterExpression,
    pub(crate) term_hashes: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct NativeFilterClusterEstimate {
    pub(crate) allowed_buckets: RoaringBitmap,
    pub(crate) expected_matches: BTreeMap<u32, u32>,
    pub(crate) expected_total: u64,
}

pub(crate) fn scalar_value_type(value: &Value) -> Option<FilterValueType> {
    match value {
        Value::String(_) => Some(FilterValueType::String),
        Value::Number(_) => Some(FilterValueType::Number),
        Value::Bool(_) => Some(FilterValueType::Bool),
        Value::Null => Some(FilterValueType::Null),
        _ => None,
    }
}

pub(crate) fn filter_value_term_hash(field: &str, value: &Value) -> Result<String, ApiError> {
    let encoded_value = serde_json::to_vec(value).map_err(|error| {
        ApiError::internal(format!(
            "failed to encode filter value for hashing: {error}"
        ))
    })?;
    let mut input = Vec::with_capacity(field.len().saturating_add(encoded_value.len() + 1));
    input.extend_from_slice(field.as_bytes());
    input.push(0_u8);
    input.extend_from_slice(&encoded_value);
    Ok(sha256_hex(&input))
}

pub(crate) fn filter_token_term_hash(field: &str, token: &str) -> String {
    let mut input = Vec::with_capacity(field.len().saturating_add(token.len() + 1));
    input.extend_from_slice(field.as_bytes());
    input.push(0xff_u8);
    input.extend_from_slice(token.as_bytes());
    sha256_hex(&input)
}

pub(crate) fn compile_native_filter_plan(
    filter: Option<&MetadataFilterExpression>,
    index_meta: &FilterIndexMeta,
) -> Result<Option<NativeFilterPlan>, ApiError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    let expression = normalize_native_expression(compile_expression(filter, index_meta)?);
    let mut terms = BTreeSet::new();
    collect_native_filter_term_hashes(&expression, &mut terms);
    Ok(Some(NativeFilterPlan {
        expression,
        term_hashes: terms.into_iter().collect(),
    }))
}

fn compile_expression(
    filter: &MetadataFilterExpression,
    index_meta: &FilterIndexMeta,
) -> Result<NativeFilterExpression, ApiError> {
    match filter {
        MetadataFilterExpression::Eq { field, value } => Ok(NativeFilterExpression::Term(
            filter_value_term_hash(field, value)?,
        )),
        MetadataFilterExpression::Ne { field, value } => Ok(NativeFilterExpression::Not(Box::new(
            NativeFilterExpression::Term(filter_value_term_hash(field, value)?),
        ))),
        MetadataFilterExpression::Lt { field, value } => {
            compile_range_expression(field, value, index_meta, RangeComparator::Lt)
        }
        MetadataFilterExpression::Lte { field, value } => {
            compile_range_expression(field, value, index_meta, RangeComparator::Lte)
        }
        MetadataFilterExpression::Gt { field, value } => {
            compile_range_expression(field, value, index_meta, RangeComparator::Gt)
        }
        MetadataFilterExpression::Gte { field, value } => {
            compile_range_expression(field, value, index_meta, RangeComparator::Gte)
        }
        MetadataFilterExpression::In { field, values } => compile_or_terms(
            values
                .iter()
                .map(|value| filter_value_term_hash(field, value))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        MetadataFilterExpression::NotIn { field, values } => {
            Ok(NativeFilterExpression::Not(Box::new(compile_or_terms(
                values
                    .iter()
                    .map(|value| filter_value_term_hash(field, value))
                    .collect::<Result<Vec<_>, _>>()?,
            )?)))
        }
        MetadataFilterExpression::ContainsAny { field, values } => compile_or_terms(
            values
                .iter()
                .map(|value| filter_value_term_hash(field, value))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        MetadataFilterExpression::ContainsAllTokens { field, tokens } => {
            let terms = tokens
                .iter()
                .map(|token| NativeFilterExpression::Term(filter_token_term_hash(field, token)))
                .collect::<Vec<_>>();
            Ok(NativeFilterExpression::And(terms))
        }
        MetadataFilterExpression::Glob { field, matcher, .. } => {
            let mut terms = BTreeSet::new();
            if let Some(entries) = index_meta.value_lexicon.get(field) {
                for entry in entries {
                    if !matches!(entry.value_type, FilterValueType::String) {
                        continue;
                    }
                    let Some(string_value) = entry.value.as_str() else {
                        continue;
                    };
                    if matcher.is_match(string_value) {
                        terms.insert(entry.term_hash.clone());
                    }
                }
            }
            compile_or_terms(terms.into_iter().collect())
        }
        MetadataFilterExpression::Regex { field, matcher, .. } => {
            let mut terms = BTreeSet::new();
            if let Some(entries) = index_meta.value_lexicon.get(field) {
                for entry in entries {
                    if !matches!(entry.value_type, FilterValueType::String) {
                        continue;
                    }
                    let Some(string_value) = entry.value.as_str() else {
                        continue;
                    };
                    if matcher.is_match(string_value) {
                        terms.insert(entry.term_hash.clone());
                    }
                }
            }
            compile_or_terms(terms.into_iter().collect())
        }
        MetadataFilterExpression::And(children) => {
            let mut compiled = Vec::with_capacity(children.len());
            for child in children {
                compiled.push(compile_expression(child, index_meta)?);
            }
            Ok(NativeFilterExpression::And(compiled))
        }
        MetadataFilterExpression::Or(children) => {
            let mut compiled = Vec::with_capacity(children.len());
            for child in children {
                compiled.push(compile_expression(child, index_meta)?);
            }
            Ok(NativeFilterExpression::Or(compiled))
        }
        MetadataFilterExpression::Not(child) => Ok(NativeFilterExpression::Not(Box::new(
            compile_expression(child, index_meta)?,
        ))),
    }
}

fn compile_or_terms(terms: Vec<String>) -> Result<NativeFilterExpression, ApiError> {
    if terms.iter().any(|term| term.is_empty()) {
        return Err(ApiError::internal(
            "native filter term hash must not be empty",
        ));
    }
    if terms.is_empty() {
        return Ok(NativeFilterExpression::MatchNone);
    }
    if terms.len() == 1 {
        return Ok(NativeFilterExpression::Term(terms[0].clone()));
    }
    Ok(NativeFilterExpression::Or(
        terms
            .into_iter()
            .map(NativeFilterExpression::Term)
            .collect(),
    ))
}

#[derive(Debug, Clone, Copy)]
enum RangeComparator {
    Lt,
    Lte,
    Gt,
    Gte,
}

fn compile_range_expression(
    field: &str,
    value: &Value,
    index_meta: &FilterIndexMeta,
    comparator: RangeComparator,
) -> Result<NativeFilterExpression, ApiError> {
    let mut terms = BTreeSet::new();
    if let Some(entries) = index_meta.value_lexicon.get(field) {
        for entry in entries {
            if range_matches(&entry.value, value, comparator) {
                terms.insert(entry.term_hash.clone());
            }
        }
    }
    compile_or_terms(terms.into_iter().collect())
}

fn range_matches(left: &Value, right: &Value, comparator: RangeComparator) -> bool {
    let ordering = compare_scalar_json_values(left, right);
    match comparator {
        RangeComparator::Lt => ordering.is_some_and(Ordering::is_lt),
        RangeComparator::Lte => ordering.is_some_and(|value| value.is_lt() || value.is_eq()),
        RangeComparator::Gt => ordering.is_some_and(Ordering::is_gt),
        RangeComparator::Gte => ordering.is_some_and(|value| value.is_gt() || value.is_eq()),
    }
}

fn normalize_native_expression(expression: NativeFilterExpression) -> NativeFilterExpression {
    match expression {
        NativeFilterExpression::And(children) => {
            let mut flattened = Vec::new();
            for child in children {
                let normalized = normalize_native_expression(child);
                match normalized {
                    NativeFilterExpression::MatchAll => {}
                    NativeFilterExpression::MatchNone => return NativeFilterExpression::MatchNone,
                    NativeFilterExpression::And(nested) => flattened.extend(nested),
                    other => flattened.push(other),
                }
            }
            if flattened.is_empty() {
                NativeFilterExpression::MatchAll
            } else if flattened.len() == 1 {
                flattened.remove(0)
            } else {
                NativeFilterExpression::And(flattened)
            }
        }
        NativeFilterExpression::Or(children) => {
            let mut flattened = Vec::new();
            for child in children {
                let normalized = normalize_native_expression(child);
                match normalized {
                    NativeFilterExpression::MatchNone => {}
                    NativeFilterExpression::MatchAll => return NativeFilterExpression::MatchAll,
                    NativeFilterExpression::Or(nested) => flattened.extend(nested),
                    other => flattened.push(other),
                }
            }
            if flattened.is_empty() {
                NativeFilterExpression::MatchNone
            } else if flattened.len() == 1 {
                flattened.remove(0)
            } else {
                NativeFilterExpression::Or(flattened)
            }
        }
        NativeFilterExpression::Not(child) => {
            let normalized_child = normalize_native_expression(*child);
            match normalized_child {
                NativeFilterExpression::MatchAll => NativeFilterExpression::MatchNone,
                NativeFilterExpression::MatchNone => NativeFilterExpression::MatchAll,
                NativeFilterExpression::Not(inner) => *inner,
                other => NativeFilterExpression::Not(Box::new(other)),
            }
        }
        other => other,
    }
}

pub(crate) fn collect_native_filter_term_hashes(
    expression: &NativeFilterExpression,
    out: &mut BTreeSet<String>,
) {
    match expression {
        NativeFilterExpression::Term(term_hash) => {
            out.insert(term_hash.clone());
        }
        NativeFilterExpression::And(children) | NativeFilterExpression::Or(children) => {
            for child in children {
                collect_native_filter_term_hashes(child, out);
            }
        }
        NativeFilterExpression::Not(child) => collect_native_filter_term_hashes(child, out),
        NativeFilterExpression::MatchAll | NativeFilterExpression::MatchNone => {}
    }
}

pub(crate) fn evaluate_native_filter_bitmap(
    expression: &NativeFilterExpression,
    term_bitmaps: &BTreeMap<String, RoaringBitmap>,
    all_bitmap: &RoaringBitmap,
) -> RoaringBitmap {
    match expression {
        NativeFilterExpression::MatchAll => all_bitmap.clone(),
        NativeFilterExpression::MatchNone => RoaringBitmap::new(),
        NativeFilterExpression::Term(term_hash) => term_bitmaps
            .get(term_hash)
            .cloned()
            .unwrap_or_else(RoaringBitmap::new),
        NativeFilterExpression::And(children) => {
            if children.is_empty() {
                return all_bitmap.clone();
            }
            let mut iter = children.iter();
            let mut out = evaluate_native_filter_bitmap(
                iter.next().expect("And children checked non-empty"),
                term_bitmaps,
                all_bitmap,
            );
            for child in iter {
                out &= evaluate_native_filter_bitmap(child, term_bitmaps, all_bitmap);
            }
            out
        }
        NativeFilterExpression::Or(children) => {
            let mut out = RoaringBitmap::new();
            for child in children {
                out |= evaluate_native_filter_bitmap(child, term_bitmaps, all_bitmap);
            }
            out
        }
        NativeFilterExpression::Not(child) => {
            let child_bitmap = evaluate_native_filter_bitmap(child, term_bitmaps, all_bitmap);
            let mut out = all_bitmap.clone();
            out -= child_bitmap;
            out
        }
    }
}

pub(crate) fn evaluate_native_filter_row_bitmap(
    expression: &NativeFilterExpression,
    term_bitmaps: &BTreeMap<String, RoaringBitmap>,
    vector_count: usize,
) -> Option<RoaringBitmap> {
    if matches!(expression, NativeFilterExpression::MatchAll) {
        return None;
    }
    let all_locals = build_all_local_bitmap(vector_count);
    Some(evaluate_native_filter_bitmap(
        expression,
        term_bitmaps,
        &all_locals,
    ))
}

fn build_all_local_bitmap(vector_count: usize) -> RoaringBitmap {
    let mut out = RoaringBitmap::new();
    for local_id in 0..vector_count {
        out.insert(local_id as u32);
    }
    out
}

#[derive(Debug, Clone)]
struct ClusterEvalState {
    bitmap: RoaringBitmap,
    counts: BTreeMap<u32, u32>,
}

pub(crate) fn evaluate_native_filter_cluster_estimate(
    expression: &NativeFilterExpression,
    term_summaries: &BTreeMap<String, NativeFilterClusterSummary>,
    bucket_sizes: &BTreeMap<u32, u32>,
    all_buckets: &RoaringBitmap,
) -> NativeFilterClusterEstimate {
    let state = evaluate_cluster_state(expression, term_summaries, bucket_sizes, all_buckets);
    let expected_total = state
        .counts
        .values()
        .fold(0_u64, |total, value| total.saturating_add(*value as u64));
    NativeFilterClusterEstimate {
        allowed_buckets: state.bitmap,
        expected_matches: state.counts,
        expected_total,
    }
}

fn evaluate_cluster_state(
    expression: &NativeFilterExpression,
    term_summaries: &BTreeMap<String, NativeFilterClusterSummary>,
    bucket_sizes: &BTreeMap<u32, u32>,
    all_buckets: &RoaringBitmap,
) -> ClusterEvalState {
    match expression {
        NativeFilterExpression::MatchAll => ClusterEvalState {
            bitmap: all_buckets.clone(),
            counts: bucket_sizes
                .iter()
                .filter_map(|(bucket_id, size)| {
                    all_buckets
                        .contains(*bucket_id)
                        .then_some((*bucket_id, (*size).max(1)))
                })
                .collect(),
        },
        NativeFilterExpression::MatchNone => ClusterEvalState {
            bitmap: RoaringBitmap::new(),
            counts: BTreeMap::new(),
        },
        NativeFilterExpression::Term(term_hash) => {
            let summary = term_summaries.get(term_hash).cloned().unwrap_or_default();
            let mut bitmap = summary.bucket_bitmap;
            bitmap &= all_buckets;
            let mut counts = BTreeMap::new();
            for bucket_id in &bitmap {
                let Some(bucket_size) = bucket_sizes.get(&bucket_id).copied() else {
                    continue;
                };
                if bucket_size == 0 {
                    continue;
                }
                let raw = summary
                    .bucket_match_counts
                    .get(&bucket_id)
                    .copied()
                    .unwrap_or(1);
                let clamped = raw.clamp(1, bucket_size);
                counts.insert(bucket_id, clamped);
            }
            ClusterEvalState { bitmap, counts }
        }
        NativeFilterExpression::And(children) => {
            if children.is_empty() {
                return evaluate_cluster_state(
                    &NativeFilterExpression::MatchAll,
                    term_summaries,
                    bucket_sizes,
                    all_buckets,
                );
            }
            let child_states = children
                .iter()
                .map(|child| {
                    evaluate_cluster_state(child, term_summaries, bucket_sizes, all_buckets)
                })
                .collect::<Vec<_>>();
            let mut bitmap = child_states
                .first()
                .map(|state| state.bitmap.clone())
                .unwrap_or_default();
            for state in child_states.iter().skip(1) {
                bitmap &= state.bitmap.clone();
            }
            let mut counts = BTreeMap::new();
            for bucket_id in &bitmap {
                let Some(bucket_size) = bucket_sizes.get(&bucket_id).copied() else {
                    continue;
                };
                if bucket_size == 0 {
                    continue;
                }
                let mut min_count = bucket_size;
                for state in &child_states {
                    let child_count = state.counts.get(&bucket_id).copied().unwrap_or(1);
                    min_count = min_count.min(child_count);
                }
                if min_count > 0 {
                    counts.insert(bucket_id, min_count);
                }
            }
            ClusterEvalState { bitmap, counts }
        }
        NativeFilterExpression::Or(children) => {
            let child_states = children
                .iter()
                .map(|child| {
                    evaluate_cluster_state(child, term_summaries, bucket_sizes, all_buckets)
                })
                .collect::<Vec<_>>();
            let mut bitmap = RoaringBitmap::new();
            for state in &child_states {
                bitmap |= state.bitmap.clone();
            }
            let mut counts = BTreeMap::new();
            for bucket_id in &bitmap {
                let Some(bucket_size) = bucket_sizes.get(&bucket_id).copied() else {
                    continue;
                };
                if bucket_size == 0 {
                    continue;
                }
                let mut total = 0_u32;
                for state in &child_states {
                    total =
                        total.saturating_add(state.counts.get(&bucket_id).copied().unwrap_or(0));
                    if total >= bucket_size {
                        total = bucket_size;
                        break;
                    }
                }
                if total > 0 {
                    counts.insert(bucket_id, total);
                }
            }
            ClusterEvalState { bitmap, counts }
        }
        NativeFilterExpression::Not(child) => {
            let child_state =
                evaluate_cluster_state(child, term_summaries, bucket_sizes, all_buckets);
            let mut bitmap = all_buckets.clone();
            bitmap -= child_state.bitmap;
            let mut counts = BTreeMap::new();
            for bucket_id in &bitmap {
                let Some(bucket_size) = bucket_sizes.get(&bucket_id).copied() else {
                    continue;
                };
                if bucket_size == 0 {
                    continue;
                }
                let child_count = child_state
                    .counts
                    .get(&bucket_id)
                    .copied()
                    .unwrap_or(0)
                    .min(bucket_size);
                let estimate = bucket_size.saturating_sub(child_count).max(1);
                counts.insert(bucket_id, estimate);
            }
            ClusterEvalState { bitmap, counts }
        }
    }
}
