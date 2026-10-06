use std::collections::BTreeSet;

use regex_lite::Regex;
use serde_json::Value;

use crate::error::ApiError;

use super::ast::{
    is_scalar_json_value, tokenize_text, FilterExpr, GlobMatcher, MetadataFilterExpression,
    RegexMatcher,
};

const DEFAULT_FILTER_MAX_REGEX_BYTES: usize = 512;
const DEFAULT_FILTER_MAX_GLOB_BYTES: usize = 512;

#[derive(Debug, Clone, Copy)]
pub(crate) struct FilterParserLimits {
    pub(crate) max_regex_bytes: usize,
    pub(crate) max_glob_bytes: usize,
}

impl Default for FilterParserLimits {
    fn default() -> Self {
        Self {
            max_regex_bytes: DEFAULT_FILTER_MAX_REGEX_BYTES,
            max_glob_bytes: DEFAULT_FILTER_MAX_GLOB_BYTES,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogicalOperator {
    And,
    Or,
    Not,
}

impl LogicalOperator {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "and" => Some(Self::And),
            "or" => Some(Self::Or),
            "not" => Some(Self::Not),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AtomicOperator {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
    In,
    NotIn,
    ContainsAny,
    ContainsAllTokens,
    Glob,
    Regex,
}

impl AtomicOperator {
    fn parse(raw: &str) -> Option<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "eq" | "=" | "==" => Some(Self::Eq),
            "ne" | "!=" => Some(Self::Ne),
            "lt" | "<" => Some(Self::Lt),
            "lte" | "<=" => Some(Self::Lte),
            "gt" | ">" => Some(Self::Gt),
            "gte" | ">=" => Some(Self::Gte),
            "in" => Some(Self::In),
            "notin" | "not_in" => Some(Self::NotIn),
            "containsany" | "contains_any" => Some(Self::ContainsAny),
            "containsalltokens" | "contains_all_tokens" => Some(Self::ContainsAllTokens),
            "glob" => Some(Self::Glob),
            "regex" => Some(Self::Regex),
            _ => None,
        }
    }
}

pub(crate) fn parse_metadata_filter_with_limits(
    filter: Option<Value>,
    limits: FilterParserLimits,
) -> Result<Option<MetadataFilterExpression>, ApiError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    parse_filter_expression(filter, limits).map(Some)
}

fn parse_filter_expression(
    filter: Value,
    limits: FilterParserLimits,
) -> Result<FilterExpr, ApiError> {
    let Value::Array(items) = filter else {
        return Err(ApiError::invalid_argument(
            "filter must be a tuple-expression array",
        ));
    };
    parse_filter_array(items, limits)
}

fn parse_filter_array(
    items: Vec<Value>,
    limits: FilterParserLimits,
) -> Result<FilterExpr, ApiError> {
    if items.is_empty() {
        return Err(ApiError::invalid_argument(
            "tuple-expression filter must not be empty",
        ));
    }

    if let Some(operator) = items.first().and_then(Value::as_str) {
        if let Some(logical) = LogicalOperator::parse(operator) {
            return parse_logical_expression(logical, items, limits);
        }
    }

    parse_atomic_expression(items, limits)
}

fn parse_logical_expression(
    logical: LogicalOperator,
    items: Vec<Value>,
    limits: FilterParserLimits,
) -> Result<FilterExpr, ApiError> {
    match logical {
        LogicalOperator::And | LogicalOperator::Or => {
            let operands = collect_logical_operands(items, logical)?;
            let mut parsed_children = Vec::with_capacity(operands.len());
            for operand in operands {
                parsed_children.push(parse_filter_expression(operand, limits)?);
            }
            Ok(normalize_logical(logical, parsed_children))
        }
        LogicalOperator::Not => {
            if items.len() != 2 {
                return Err(ApiError::invalid_argument(
                    "Not filter must include exactly one operand",
                ));
            }
            let child = parse_filter_expression(items[1].clone(), limits)?;
            Ok(FilterExpr::Not(Box::new(child)))
        }
    }
}

fn collect_logical_operands(
    items: Vec<Value>,
    logical: LogicalOperator,
) -> Result<Vec<Value>, ApiError> {
    if items.len() < 2 {
        return Err(ApiError::invalid_argument(match logical {
            LogicalOperator::And => "And filter must include at least one operand",
            LogicalOperator::Or => "Or filter must include at least one operand",
            LogicalOperator::Not => "Not filter must include one operand",
        }));
    }

    if items.len() == 2 {
        let operand = items[1].clone();
        if let Value::Array(entries) = operand {
            if entries.is_empty() {
                return Err(ApiError::invalid_argument(match logical {
                    LogicalOperator::And => "And filter must include at least one operand",
                    LogicalOperator::Or => "Or filter must include at least one operand",
                    LogicalOperator::Not => "Not filter must include one operand",
                }));
            }
            if entries.iter().all(|entry| matches!(entry, Value::Array(_))) {
                return Ok(entries);
            }
            return Ok(vec![Value::Array(entries)]);
        }
        return Ok(vec![operand]);
    }

    Ok(items.into_iter().skip(1).collect())
}

fn normalize_logical(logical: LogicalOperator, parsed_children: Vec<FilterExpr>) -> FilterExpr {
    let mut flattened = Vec::new();
    for child in parsed_children {
        match (logical, child) {
            (LogicalOperator::And, FilterExpr::And(nested)) => flattened.extend(nested),
            (LogicalOperator::Or, FilterExpr::Or(nested)) => flattened.extend(nested),
            (_, node) => flattened.push(node),
        }
    }
    if flattened.len() == 1 {
        return flattened.pop().expect("single flattened child");
    }
    match logical {
        LogicalOperator::And => FilterExpr::And(flattened),
        LogicalOperator::Or => FilterExpr::Or(flattened),
        LogicalOperator::Not => FilterExpr::Not(Box::new(flattened.remove(0))),
    }
}

fn parse_atomic_expression(
    items: Vec<Value>,
    limits: FilterParserLimits,
) -> Result<FilterExpr, ApiError> {
    if items.len() != 3 {
        return Err(ApiError::invalid_argument(
            "atomic tuple-expression filter must be [field, operator, value] or [operator, field, value]",
        ));
    }

    let value = items[2].clone();

    let attempt_field_first = items
        .get(0)
        .and_then(Value::as_str)
        .zip(items.get(1).and_then(Value::as_str));
    if let Some((field, operator_raw)) = attempt_field_first {
        if let Some(operator) = AtomicOperator::parse(operator_raw) {
            return parse_atomic_clause(field, operator, value, limits);
        }
    }

    let attempt_operator_first = items
        .get(0)
        .and_then(Value::as_str)
        .zip(items.get(1).and_then(Value::as_str));
    if let Some((operator_raw, field)) = attempt_operator_first {
        if let Some(operator) = AtomicOperator::parse(operator_raw) {
            return parse_atomic_clause(field, operator, value, limits);
        }
    }

    Err(ApiError::invalid_argument(
        "tuple-expression filter must use a supported operator and string field",
    ))
}

fn parse_atomic_clause(
    field_raw: &str,
    operator: AtomicOperator,
    value: Value,
    limits: FilterParserLimits,
) -> Result<FilterExpr, ApiError> {
    let field = field_raw.trim();
    if field.is_empty() {
        return Err(ApiError::invalid_argument(
            "tuple-expression filter field must not be empty",
        ));
    }
    let field = field.to_string();

    match operator {
        AtomicOperator::Eq => {
            ensure_scalar_value(&value, "Eq")?;
            Ok(FilterExpr::Eq { field, value })
        }
        AtomicOperator::Ne => {
            ensure_scalar_value(&value, "Ne")?;
            Ok(FilterExpr::Ne { field, value })
        }
        AtomicOperator::Lt => {
            ensure_scalar_value(&value, "Lt")?;
            Ok(FilterExpr::Lt { field, value })
        }
        AtomicOperator::Lte => {
            ensure_scalar_value(&value, "Lte")?;
            Ok(FilterExpr::Lte { field, value })
        }
        AtomicOperator::Gt => {
            ensure_scalar_value(&value, "Gt")?;
            Ok(FilterExpr::Gt { field, value })
        }
        AtomicOperator::Gte => {
            ensure_scalar_value(&value, "Gte")?;
            Ok(FilterExpr::Gte { field, value })
        }
        AtomicOperator::In => {
            let values = parse_scalar_values(value, "In")?;
            Ok(FilterExpr::In { field, values })
        }
        AtomicOperator::NotIn => {
            let values = parse_scalar_values(value, "NotIn")?;
            Ok(FilterExpr::NotIn { field, values })
        }
        AtomicOperator::ContainsAny => {
            let values = parse_scalar_values(value, "ContainsAny")?;
            Ok(FilterExpr::ContainsAny { field, values })
        }
        AtomicOperator::ContainsAllTokens => {
            let tokens = parse_token_values(value)?;
            Ok(FilterExpr::ContainsAllTokens { field, tokens })
        }
        AtomicOperator::Glob => {
            let Some(pattern) = value.as_str() else {
                return Err(ApiError::invalid_argument(
                    "Glob filter expects a string pattern",
                ));
            };
            if pattern.len() > limits.max_glob_bytes {
                return Err(ApiError::invalid_argument(format!(
                    "Glob pattern exceeds max bytes limit ({})",
                    limits.max_glob_bytes
                )));
            }
            let matcher = compile_glob_matcher(pattern)?;
            Ok(FilterExpr::Glob {
                field,
                pattern: pattern.to_string(),
                matcher,
            })
        }
        AtomicOperator::Regex => {
            let Some(pattern) = value.as_str() else {
                return Err(ApiError::invalid_argument(
                    "Regex filter expects a string pattern",
                ));
            };
            if pattern.len() > limits.max_regex_bytes {
                return Err(ApiError::invalid_argument(format!(
                    "Regex pattern exceeds max bytes limit ({})",
                    limits.max_regex_bytes
                )));
            }
            let matcher = Regex::new(pattern)
                .map(RegexMatcher::new)
                .map_err(|error| {
                    ApiError::invalid_argument(format!("invalid Regex pattern: {error}"))
                })?;
            Ok(FilterExpr::Regex {
                field,
                pattern: pattern.to_string(),
                matcher,
            })
        }
    }
}

fn ensure_scalar_value(value: &Value, operator: &str) -> Result<(), ApiError> {
    if !is_scalar_json_value(value) {
        return Err(ApiError::invalid_argument(format!(
            "{operator} filter expects a scalar value"
        )));
    }
    Ok(())
}

fn parse_scalar_values(value: Value, operator: &str) -> Result<Vec<Value>, ApiError> {
    let Value::Array(values) = value else {
        return Err(ApiError::invalid_argument(format!(
            "{operator} filter expects an array of scalar values"
        )));
    };
    if values.is_empty() {
        return Err(ApiError::invalid_argument(format!(
            "{operator} filter expects a non-empty array"
        )));
    }
    let mut deduped = Vec::new();
    let mut seen = BTreeSet::new();
    for value in values {
        if !is_scalar_json_value(&value) {
            return Err(ApiError::invalid_argument(format!(
                "{operator} filter expects scalar array values"
            )));
        }
        let encoded = serde_json::to_vec(&value).map_err(|error| {
            ApiError::internal(format!("failed to normalize filter value: {error}"))
        })?;
        if seen.insert(encoded) {
            deduped.push(value);
        }
    }
    Ok(deduped)
}

fn parse_token_values(value: Value) -> Result<Vec<String>, ApiError> {
    let Value::Array(values) = value else {
        return Err(ApiError::invalid_argument(
            "ContainsAllTokens filter expects an array of strings",
        ));
    };
    if values.is_empty() {
        return Err(ApiError::invalid_argument(
            "ContainsAllTokens filter expects a non-empty array",
        ));
    }
    let mut deduped = Vec::new();
    let mut seen = BTreeSet::new();
    for raw in values {
        let Some(text) = raw.as_str() else {
            return Err(ApiError::invalid_argument(
                "ContainsAllTokens filter expects string tokens",
            ));
        };
        for token in tokenize_text(text) {
            if token.is_empty() {
                continue;
            }
            if seen.insert(token.clone()) {
                deduped.push(token);
            }
        }
    }
    if deduped.is_empty() {
        return Err(ApiError::invalid_argument(
            "ContainsAllTokens filter expects at least one token",
        ));
    }
    Ok(deduped)
}

fn compile_glob_matcher(pattern: &str) -> Result<GlobMatcher, ApiError> {
    let regex_pattern = glob_to_regex_pattern(pattern);
    Regex::new(&regex_pattern)
        .map(GlobMatcher::new)
        .map_err(|error| ApiError::invalid_argument(format!("invalid Glob pattern: {error}")))
}

fn glob_to_regex_pattern(pattern: &str) -> String {
    let mut out = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            _ => push_regex_escaped_char(&mut out, ch),
        }
    }
    out.push('$');
    out
}

fn push_regex_escaped_char(out: &mut String, ch: char) {
    if matches!(
        ch,
        '.' | '+' | '(' | ')' | '|' | '[' | ']' | '{' | '}' | '^' | '$' | '\\'
    ) {
        out.push('\\');
    }
    out.push(ch);
}
