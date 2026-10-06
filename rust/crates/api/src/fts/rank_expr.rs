use serde_json::Value;

use crate::{error::ApiError, models::QuerySearchStrategy};

const MAX_EXPR_DEPTH: usize = 32;

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RankExpr {
    Vector(VectorRankExpr),
    Bm25(Bm25Expr),
    Sum(Vec<RankExpr>),
    Max(Vec<RankExpr>),
    Product {
        weight: f32,
        expr: Box<RankExpr>,
    },
    RankByFilter {
        filter: Value,
        boost: f32,
        expr: Box<RankExpr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct VectorRankExpr {
    pub(crate) strategy: QuerySearchStrategy,
    pub(crate) vector: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Bm25Field {
    Text,
    Field(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Bm25MatchMode {
    Exact,
    Prefix,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Bm25Expr {
    pub(crate) field: Bm25Field,
    pub(crate) query: String,
    pub(crate) match_mode: Bm25MatchMode,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ParsedRankExpr {
    Vector(VectorRankExpr),
    Lexical(RankExpr),
}

impl RankExpr {
    fn contains_vector(&self) -> bool {
        match self {
            Self::Vector(_) => true,
            Self::Bm25(_) => false,
            Self::Sum(children) | Self::Max(children) => children.iter().any(Self::contains_vector),
            Self::Product { expr, .. } => expr.contains_vector(),
            Self::RankByFilter { expr, .. } => expr.contains_vector(),
        }
    }

    fn contains_bm25(&self) -> bool {
        match self {
            Self::Vector(_) => false,
            Self::Bm25(_) => true,
            Self::Sum(children) | Self::Max(children) => children.iter().any(Self::contains_bm25),
            Self::Product { expr, .. } => expr.contains_bm25(),
            Self::RankByFilter { expr, .. } => expr.contains_bm25(),
        }
    }

    fn normalize(self) -> Self {
        match self {
            Self::Sum(children) => {
                let mut out = Vec::new();
                for child in children {
                    let normalized = child.normalize();
                    if let Self::Sum(nested) = normalized {
                        out.extend(nested);
                    } else {
                        out.push(normalized);
                    }
                }
                out.sort_by_cached_key(Self::sort_key);
                if out.len() == 1 {
                    out.into_iter().next().unwrap_or(Self::Sum(Vec::new()))
                } else {
                    Self::Sum(out)
                }
            }
            Self::Max(children) => {
                let mut out = Vec::new();
                for child in children {
                    let normalized = child.normalize();
                    if let Self::Max(nested) = normalized {
                        out.extend(nested);
                    } else {
                        out.push(normalized);
                    }
                }
                out.sort_by_cached_key(Self::sort_key);
                if out.len() == 1 {
                    out.into_iter().next().unwrap_or(Self::Max(Vec::new()))
                } else {
                    Self::Max(out)
                }
            }
            Self::Product { weight, expr } => {
                let normalized = expr.normalize();
                if let Self::Product {
                    weight: nested_weight,
                    expr: nested_expr,
                } = normalized
                {
                    return Self::Product {
                        weight: weight * nested_weight,
                        expr: nested_expr,
                    }
                    .normalize();
                }
                if (weight - 1.0).abs() <= f32::EPSILON {
                    normalized
                } else {
                    Self::Product {
                        weight,
                        expr: Box::new(normalized),
                    }
                }
            }
            Self::RankByFilter {
                filter,
                boost,
                expr,
            } => {
                let normalized_expr = expr.normalize();
                if boost <= 0.0 {
                    normalized_expr
                } else {
                    Self::RankByFilter {
                        filter,
                        boost,
                        expr: Box::new(normalized_expr),
                    }
                }
            }
            leaf => leaf,
        }
    }

    fn sort_key(&self) -> String {
        match self {
            Self::Vector(vector) => format!("vector:{:?}:{:?}", vector.strategy, vector.vector),
            Self::Bm25(expr) => {
                let field = match &expr.field {
                    Bm25Field::Text => "text".to_string(),
                    Bm25Field::Field(name) => name.clone(),
                };
                format!(
                    "bm25:{field}:{:?}:{}",
                    expr.match_mode,
                    expr.query.to_ascii_lowercase()
                )
            }
            Self::Sum(children) => format!(
                "sum:[{}]",
                children
                    .iter()
                    .map(Self::sort_key)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Max(children) => format!(
                "max:[{}]",
                children
                    .iter()
                    .map(Self::sort_key)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Self::Product { weight, expr } => format!("prod:{weight:.6}:{}", expr.sort_key()),
            Self::RankByFilter {
                filter,
                boost,
                expr,
            } => format!("rank_by_filter:{boost:.6}:{}:{}", filter, expr.sort_key()),
        }
    }
}

fn rank_expr_error(class: &str, message: impl Into<String>) -> ApiError {
    ApiError::invalid_argument(format!("RANK_EXPR_{class}: {}", message.into()))
}

pub(crate) fn parse_rank_expr(raw: &Value) -> Result<ParsedRankExpr, ApiError> {
    let parsed = parse_rank_expr_inner(raw, 0)?.normalize();
    match parsed {
        RankExpr::Vector(vector) => Ok(ParsedRankExpr::Vector(vector)),
        lexical => {
            if lexical.contains_vector() && lexical.contains_bm25() {
                return Err(rank_expr_error(
                    "MIXED_SEMANTICS",
                    "rank expression cannot mix vector and BM25 operators",
                ));
            }
            if lexical.contains_vector() {
                return Err(rank_expr_error(
                    "VECTOR_SHAPE",
                    "vector rank expression must be a leaf [\"vector\", <operator>, <vector>]",
                ));
            }
            Ok(ParsedRankExpr::Lexical(lexical))
        }
    }
}

fn parse_rank_expr_inner(raw: &Value, depth: usize) -> Result<RankExpr, ApiError> {
    if depth >= MAX_EXPR_DEPTH {
        return Err(rank_expr_error(
            "DEPTH",
            "rank expression exceeds max nesting depth",
        ));
    }
    let array = raw
        .as_array()
        .ok_or_else(|| rank_expr_error("SHAPE", "rank_by must be an array"))?;
    if array.is_empty() {
        return Err(rank_expr_error("SHAPE", "rank_by must not be empty"));
    }
    let head = array[0]
        .as_str()
        .ok_or_else(|| rank_expr_error("SHAPE", "rank_by operator must be a string"))?;
    match head.to_ascii_lowercase().as_str() {
        "sum" => parse_list_operator(array, "Sum", depth, true),
        "max" => parse_list_operator(array, "Max", depth, false),
        "product" => parse_product(array, depth),
        "rank_by_filter" | "rank-by-filter" | "rankbyfilter" => parse_rank_by_filter(array, depth),
        _ => parse_leaf(array),
    }
}

fn parse_list_operator(
    array: &[Value],
    operator: &str,
    depth: usize,
    is_sum: bool,
) -> Result<RankExpr, ApiError> {
    if array.len() != 2 {
        return Err(rank_expr_error(
            "SHAPE",
            format!("{operator} rank expression must be ['{operator}', [rank_expr...]]"),
        ));
    }
    let children = array[1]
        .as_array()
        .ok_or_else(|| {
            rank_expr_error(
                "SHAPE",
                format!("{operator} rank expression children must be an array"),
            )
        })?
        .iter()
        .map(|child| parse_rank_expr_inner(child, depth + 1))
        .collect::<Result<Vec<_>, _>>()?;
    if children.is_empty() {
        return Err(rank_expr_error(
            "SHAPE",
            format!("{operator} rank expression requires at least one child"),
        ));
    }
    if is_sum {
        Ok(RankExpr::Sum(children))
    } else {
        Ok(RankExpr::Max(children))
    }
}

fn parse_product(array: &[Value], depth: usize) -> Result<RankExpr, ApiError> {
    if array.len() != 3 {
        return Err(rank_expr_error(
            "SHAPE",
            "Product rank expression must be ['Product', <weight>, rank_expr]",
        ));
    }
    let weight = array[1].as_f64().ok_or_else(|| {
        rank_expr_error("PRODUCT_WEIGHT", "Product weight must be a finite number")
    })?;
    if !weight.is_finite() {
        return Err(rank_expr_error(
            "PRODUCT_WEIGHT",
            "Product weight must be a finite number",
        ));
    }
    if weight < 0.0 {
        return Err(rank_expr_error(
            "PRODUCT_WEIGHT",
            "Product weight must be >= 0",
        ));
    }
    let child = parse_rank_expr_inner(&array[2], depth + 1)?;
    Ok(RankExpr::Product {
        weight: weight as f32,
        expr: Box::new(child),
    })
}

fn parse_rank_by_filter(array: &[Value], depth: usize) -> Result<RankExpr, ApiError> {
    if array.len() != 4 {
        return Err(rank_expr_error(
            "SHAPE",
            "rank_by_filter expression must be ['rank_by_filter', filter_expr, rank_expr, boost]",
        ));
    }
    if !array[1].is_array() {
        return Err(rank_expr_error(
            "RANK_BY_FILTER",
            "rank_by_filter filter_expr must be a tuple-expression array",
        ));
    }
    let expr = parse_rank_expr_inner(&array[2], depth + 1)?;
    let boost = array[3].as_f64().ok_or_else(|| {
        rank_expr_error(
            "RANK_BY_FILTER",
            "rank_by_filter boost must be a finite number",
        )
    })?;
    if !boost.is_finite() || boost < 0.0 {
        return Err(rank_expr_error(
            "RANK_BY_FILTER",
            "rank_by_filter boost must be a finite number >= 0",
        ));
    }
    Ok(RankExpr::RankByFilter {
        filter: array[1].clone(),
        boost: boost as f32,
        expr: Box::new(expr),
    })
}

fn parse_leaf(array: &[Value]) -> Result<RankExpr, ApiError> {
    if array.len() < 3 {
        return Err(rank_expr_error(
            "SHAPE",
            "rank_by must include [field, operator, value]",
        ));
    }
    let field = array[0]
        .as_str()
        .ok_or_else(|| rank_expr_error("SHAPE", "rank_by field must be a string"))?;
    let operator = array[1]
        .as_str()
        .ok_or_else(|| rank_expr_error("SHAPE", "rank_by operator must be a string"))?;
    let operator_lc = operator.to_ascii_lowercase();
    match operator_lc.as_str() {
        "bm25" => {
            let query = array[2]
                .as_str()
                .ok_or_else(|| rank_expr_error("BM25_QUERY", "BM25 query text must be a string"))?;
            let field = if field.eq_ignore_ascii_case("text") {
                Bm25Field::Text
            } else {
                Bm25Field::Field(field.to_string())
            };
            Ok(RankExpr::Bm25(Bm25Expr {
                field,
                query: query.to_string(),
                match_mode: Bm25MatchMode::Exact,
            }))
        }
        "bm25_prefix" | "bm25prefix" | "prefix" => {
            let query = array[2].as_str().ok_or_else(|| {
                rank_expr_error("PREFIX_QUERY", "BM25 prefix query text must be a string")
            })?;
            let field = if field.eq_ignore_ascii_case("text") {
                Bm25Field::Text
            } else {
                Bm25Field::Field(field.to_string())
            };
            Ok(RankExpr::Bm25(Bm25Expr {
                field,
                query: query.to_string(),
                match_mode: Bm25MatchMode::Prefix,
            }))
        }
        "ann" | "knn" | "exact" | "auto" => {
            if !field.eq_ignore_ascii_case("vector") {
                return Err(rank_expr_error(
                    "VECTOR_SHAPE",
                    "vector rank expression must use field 'vector'",
                ));
            }
            let strategy = match operator_lc.as_str() {
                "ann" => QuerySearchStrategy::Ann,
                "knn" | "exact" => QuerySearchStrategy::Exact,
                "auto" => QuerySearchStrategy::Auto,
                _ => QuerySearchStrategy::Exact,
            };
            let vector = parse_rank_vector(&array[2])?;
            Ok(RankExpr::Vector(VectorRankExpr { strategy, vector }))
        }
        _ => Err(rank_expr_error(
            "UNSUPPORTED_OPERATOR",
            format!("unsupported rank expression operator '{operator}'"),
        )),
    }
}

pub(crate) fn parse_rank_vector(raw: &Value) -> Result<Vec<f32>, ApiError> {
    let values = raw
        .as_array()
        .ok_or_else(|| rank_expr_error("VECTOR_SHAPE", "vector must be an array of numbers"))?;
    if values.is_empty() {
        return Err(rank_expr_error(
            "VECTOR_SHAPE",
            "vector must contain at least one value",
        ));
    }
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        let number = value
            .as_f64()
            .ok_or_else(|| rank_expr_error("VECTOR_SHAPE", "vector must contain only numbers"))?;
        if !number.is_finite() {
            return Err(rank_expr_error(
                "VECTOR_SHAPE",
                "vector must contain only finite numbers",
            ));
        }
        out.push(number as f32);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::{parse_rank_expr, Bm25Field, Bm25MatchMode, ParsedRankExpr, RankExpr};
    use crate::models::QuerySearchStrategy;

    #[test]
    fn parser_accepts_bm25_text_and_field_forms() {
        let text =
            parse_rank_expr(&json!(["text", "BM25", "rust vector"])).expect("parse text BM25");
        match text {
            ParsedRankExpr::Lexical(RankExpr::Bm25(expr)) => {
                assert_eq!(expr.field, Bm25Field::Text);
                assert_eq!(expr.query, "rust vector");
                assert_eq!(expr.match_mode, Bm25MatchMode::Exact);
            }
            other => panic!("unexpected parsed expression: {other:?}"),
        }

        let field = parse_rank_expr(&json!(["title", "bm25", "rust"])).expect("parse field BM25");
        match field {
            ParsedRankExpr::Lexical(RankExpr::Bm25(expr)) => {
                assert_eq!(expr.field, Bm25Field::Field("title".to_string()));
                assert_eq!(expr.query, "rust");
                assert_eq!(expr.match_mode, Bm25MatchMode::Exact);
            }
            other => panic!("unexpected parsed expression: {other:?}"),
        }
    }

    #[test]
    fn parser_accepts_bm25_prefix_operator() {
        let parsed =
            parse_rank_expr(&json!(["text", "BM25_PREFIX", "rust"])).expect("parse prefix BM25");
        match parsed {
            ParsedRankExpr::Lexical(RankExpr::Bm25(expr)) => {
                assert_eq!(expr.field, Bm25Field::Text);
                assert_eq!(expr.query, "rust");
                assert_eq!(expr.match_mode, Bm25MatchMode::Prefix);
            }
            other => panic!("unexpected parsed expression: {other:?}"),
        }
    }

    #[test]
    fn parser_accepts_vector_leaf_forms() {
        let parsed = parse_rank_expr(&json!(["vector", "ann", [1.0, 2.0]])).expect("parse vector");
        match parsed {
            ParsedRankExpr::Vector(vector) => {
                assert_eq!(vector.strategy, QuerySearchStrategy::Ann);
                assert_eq!(vector.vector, vec![1.0_f32, 2.0_f32]);
            }
            other => panic!("unexpected parsed expression: {other:?}"),
        }
    }

    #[test]
    fn parser_normalizes_nested_sum_and_product() {
        let parsed = parse_rank_expr(&json!([
            "Sum",
            [
                ["text", "BM25", "a"],
                ["Sum", [["title", "BM25", "b"]]],
                ["Product", 2.0, ["Product", 3.0, ["body", "BM25", "c"]]]
            ]
        ]))
        .expect("parse nested expression");
        match parsed {
            ParsedRankExpr::Lexical(RankExpr::Sum(children)) => {
                assert_eq!(children.len(), 3);
                match &children[2] {
                    RankExpr::Product { weight, .. } => {
                        assert!((*weight - 6.0).abs() < 1e-6);
                    }
                    other => panic!("expected product child, got {other:?}"),
                }
            }
            other => panic!("unexpected parsed expression: {other:?}"),
        }
    }

    #[test]
    fn parser_rejects_malformed_and_unsupported_forms() {
        let malformed = parse_rank_expr(&json!("oops")).expect_err("must fail");
        assert!(
            format!("{malformed:?}").contains("RANK_EXPR_SHAPE"),
            "malformed error should include explicit class: {malformed:?}"
        );
        assert!(
            format!("{malformed:?}").contains("rank_by must be an array"),
            "unexpected malformed error: {malformed:?}"
        );

        let unsupported =
            parse_rank_expr(&json!(["title", "tfidf", "rust"])).expect_err("must fail");
        assert!(
            format!("{unsupported:?}").contains("RANK_EXPR_UNSUPPORTED_OPERATOR"),
            "unsupported error should include explicit class: {unsupported:?}"
        );
        assert!(
            format!("{unsupported:?}").contains("unsupported rank expression operator 'tfidf'"),
            "unexpected unsupported error: {unsupported:?}"
        );
    }

    #[test]
    fn parser_rejects_mixed_vector_and_bm25_composition() {
        let mixed = parse_rank_expr(&json!([
            "Sum",
            [["text", "BM25", "rust"], ["vector", "ANN", [1.0, 0.0]]]
        ]))
        .expect_err("mixed vector and BM25 should fail");
        assert!(
            format!("{mixed:?}").contains("cannot mix vector and BM25"),
            "unexpected mixed error: {mixed:?}"
        );
    }

    #[test]
    fn parser_accepts_rank_by_filter_construct() {
        let parsed = parse_rank_expr(&json!([
            "rank_by_filter",
            ["topic", "Eq", "boosted"],
            ["text", "BM25", "rust"],
            2.5
        ]))
        .expect("rank_by_filter should parse");
        match parsed {
            ParsedRankExpr::Lexical(RankExpr::RankByFilter { boost, expr, .. }) => {
                assert!((boost - 2.5).abs() < 1e-6);
                match expr.as_ref() {
                    RankExpr::Bm25(bm25) => {
                        assert_eq!(bm25.field, Bm25Field::Text);
                    }
                    other => panic!("unexpected wrapped expression: {other:?}"),
                }
            }
            other => panic!("unexpected parsed expression: {other:?}"),
        }
    }

    #[test]
    fn parser_canonicalization_is_stable_for_commutative_trees() {
        let left = parse_rank_expr(&json!([
            "Sum",
            [
                ["title", "BM25", "rust"],
                ["body", "BM25", "vector"],
                ["Product", 2.0, ["text", "BM25", "database"]]
            ]
        ]))
        .expect("left parse");
        let right = parse_rank_expr(&json!([
            "Sum",
            [
                ["Product", 2.0, ["text", "BM25", "database"]],
                ["body", "BM25", "vector"],
                ["title", "BM25", "rust"]
            ]
        ]))
        .expect("right parse");
        assert_eq!(left, right, "canonicalization should be deterministic");
    }

    fn next_u64(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        *seed
    }

    fn random_leaf(seed: &mut u64) -> Value {
        match next_u64(seed) % 6 {
            0 => json!(null),
            1 => json!(true),
            2 => json!(next_u64(seed) as i64),
            3 => json!(format!("s-{}", next_u64(seed) % 32)),
            4 => json!([next_u64(seed) as i64, next_u64(seed) as i64]),
            _ => json!({"k": next_u64(seed) as i64}),
        }
    }

    fn random_value(seed: &mut u64, depth: usize) -> Value {
        if depth >= 4 {
            return random_leaf(seed);
        }
        match next_u64(seed) % 8 {
            0 => random_leaf(seed),
            1 => json!([
                "Sum",
                [random_value(seed, depth + 1), random_value(seed, depth + 1)]
            ]),
            2 => json!([
                "Product",
                next_u64(seed) as f64 / 3.0,
                random_value(seed, depth + 1)
            ]),
            3 => json!([
                "rank_by_filter",
                random_leaf(seed),
                random_value(seed, depth + 1),
                1.0
            ]),
            _ => {
                let mut items = Vec::new();
                let count = ((next_u64(seed) % 4) + 1) as usize;
                for _ in 0..count {
                    items.push(random_value(seed, depth + 1));
                }
                Value::Array(items)
            }
        }
    }

    #[test]
    fn parser_fuzz_style_malformed_inputs_do_not_panic() {
        for case in 0..512_u64 {
            let mut seed = case
                .wrapping_mul(7919)
                .wrapping_add(104_729)
                .wrapping_add(17);
            let value = random_value(&mut seed, 0);
            let _ = parse_rank_expr(&value);
        }
    }
}
