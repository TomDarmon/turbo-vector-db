use std::{cmp::Ordering, fmt, sync::Arc};

use regex_lite::Regex;
use serde_json::Value;

pub(crate) type MetadataFilterExpression = FilterExpr;

#[derive(Clone)]
pub(crate) struct GlobMatcher {
    compiled: Arc<Regex>,
}

impl GlobMatcher {
    pub(crate) fn new(compiled: Regex) -> Self {
        Self {
            compiled: Arc::new(compiled),
        }
    }

    pub(crate) fn is_match(&self, value: &str) -> bool {
        self.compiled.is_match(value)
    }
}

impl fmt::Debug for GlobMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("GlobMatcher")
            .field(&self.compiled.as_str())
            .finish()
    }
}

impl PartialEq for GlobMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.compiled.as_str() == other.compiled.as_str()
    }
}

impl Eq for GlobMatcher {}

#[derive(Clone)]
pub(crate) struct RegexMatcher {
    compiled: Arc<Regex>,
}

impl RegexMatcher {
    pub(crate) fn new(compiled: Regex) -> Self {
        Self {
            compiled: Arc::new(compiled),
        }
    }

    pub(crate) fn is_match(&self, value: &str) -> bool {
        self.compiled.is_match(value)
    }
}

impl fmt::Debug for RegexMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RegexMatcher")
            .field(&self.compiled.as_str())
            .finish()
    }
}

impl PartialEq for RegexMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.compiled.as_str() == other.compiled.as_str()
    }
}

impl Eq for RegexMatcher {}

#[derive(Debug, Clone)]
pub(crate) enum FilterExpr {
    Eq {
        field: String,
        value: Value,
    },
    Ne {
        field: String,
        value: Value,
    },
    Lt {
        field: String,
        value: Value,
    },
    Lte {
        field: String,
        value: Value,
    },
    Gt {
        field: String,
        value: Value,
    },
    Gte {
        field: String,
        value: Value,
    },
    In {
        field: String,
        values: Vec<Value>,
    },
    NotIn {
        field: String,
        values: Vec<Value>,
    },
    ContainsAny {
        field: String,
        values: Vec<Value>,
    },
    ContainsAllTokens {
        field: String,
        tokens: Vec<String>,
    },
    Glob {
        field: String,
        pattern: String,
        matcher: GlobMatcher,
    },
    Regex {
        field: String,
        pattern: String,
        matcher: RegexMatcher,
    },
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
}

impl PartialEq for FilterExpr {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Eq {
                    field: left_field,
                    value: left_value,
                },
                Self::Eq {
                    field: right_field,
                    value: right_value,
                },
            )
            | (
                Self::Ne {
                    field: left_field,
                    value: left_value,
                },
                Self::Ne {
                    field: right_field,
                    value: right_value,
                },
            )
            | (
                Self::Lt {
                    field: left_field,
                    value: left_value,
                },
                Self::Lt {
                    field: right_field,
                    value: right_value,
                },
            )
            | (
                Self::Lte {
                    field: left_field,
                    value: left_value,
                },
                Self::Lte {
                    field: right_field,
                    value: right_value,
                },
            )
            | (
                Self::Gt {
                    field: left_field,
                    value: left_value,
                },
                Self::Gt {
                    field: right_field,
                    value: right_value,
                },
            )
            | (
                Self::Gte {
                    field: left_field,
                    value: left_value,
                },
                Self::Gte {
                    field: right_field,
                    value: right_value,
                },
            ) => left_field == right_field && left_value == right_value,
            (
                Self::In {
                    field: left_field,
                    values: left_values,
                },
                Self::In {
                    field: right_field,
                    values: right_values,
                },
            )
            | (
                Self::NotIn {
                    field: left_field,
                    values: left_values,
                },
                Self::NotIn {
                    field: right_field,
                    values: right_values,
                },
            )
            | (
                Self::ContainsAny {
                    field: left_field,
                    values: left_values,
                },
                Self::ContainsAny {
                    field: right_field,
                    values: right_values,
                },
            ) => left_field == right_field && left_values == right_values,
            (
                Self::ContainsAllTokens {
                    field: left_field,
                    tokens: left_tokens,
                },
                Self::ContainsAllTokens {
                    field: right_field,
                    tokens: right_tokens,
                },
            ) => left_field == right_field && left_tokens == right_tokens,
            (
                Self::Glob {
                    field: left_field,
                    pattern: left_pattern,
                    ..
                },
                Self::Glob {
                    field: right_field,
                    pattern: right_pattern,
                    ..
                },
            )
            | (
                Self::Regex {
                    field: left_field,
                    pattern: left_pattern,
                    ..
                },
                Self::Regex {
                    field: right_field,
                    pattern: right_pattern,
                    ..
                },
            ) => left_field == right_field && left_pattern == right_pattern,
            (Self::And(left), Self::And(right)) | (Self::Or(left), Self::Or(right)) => {
                left == right
            }
            (Self::Not(left), Self::Not(right)) => left == right,
            _ => false,
        }
    }
}

impl Eq for FilterExpr {}

pub(crate) fn is_scalar_json_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

pub(crate) fn compare_scalar_json_values(left: &Value, right: &Value) -> Option<Ordering> {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => left
            .as_f64()
            .zip(right.as_f64())
            .and_then(|(left, right)| left.partial_cmp(&right)),
        (Value::String(left), Value::String(right)) => Some(left.cmp(right)),
        (Value::Bool(left), Value::Bool(right)) => Some(left.cmp(right)),
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        _ => None,
    }
}

pub(crate) fn tokenize_text(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_ascii_alphanumeric() {
            current.push(ch.to_ascii_lowercase());
            continue;
        }
        if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}
