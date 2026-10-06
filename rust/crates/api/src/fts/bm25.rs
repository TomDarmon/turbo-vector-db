use crate::error::ApiError;

pub(crate) const DEFAULT_BM25_K1: f32 = 1.2;
pub(crate) const DEFAULT_BM25_B: f32 = 0.75;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Bm25Params {
    pub(crate) k1: f32,
    pub(crate) b: f32,
}

impl Default for Bm25Params {
    fn default() -> Self {
        Self {
            k1: DEFAULT_BM25_K1,
            b: DEFAULT_BM25_B,
        }
    }
}

impl Bm25Params {
    pub(crate) fn validate(self) -> Result<Self, ApiError> {
        if !self.k1.is_finite() || self.k1 < 0.0 {
            return Err(ApiError::invalid_argument(
                "BM25 k1 must be a finite number >= 0",
            ));
        }
        if !self.b.is_finite() || !(0.0..=1.0).contains(&self.b) {
            return Err(ApiError::invalid_argument(
                "BM25 b must be a finite number in [0, 1]",
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ScoreExpression {
    Leaf(usize),
    Sum(Vec<ScoreExpression>),
    Max(Vec<ScoreExpression>),
    Product {
        weight: f32,
        expression: Box<ScoreExpression>,
    },
}

impl ScoreExpression {
    pub(crate) fn evaluate(&self, leaf_scores: &[f32]) -> f32 {
        match self {
            Self::Leaf(index) => leaf_scores.get(*index).copied().unwrap_or(0.0),
            Self::Sum(children) => children
                .iter()
                .map(|child| child.evaluate(leaf_scores))
                .sum(),
            Self::Max(children) => children
                .iter()
                .map(|child| child.evaluate(leaf_scores))
                .fold(0.0_f32, f32::max),
            Self::Product { weight, expression } => *weight * expression.evaluate(leaf_scores),
        }
    }
}

pub(crate) fn linear_leaf_weights(
    expression: &ScoreExpression,
    leaf_count: usize,
) -> Option<Vec<f32>> {
    let mut out = vec![0.0_f32; leaf_count];
    if !accumulate_linear_weights(expression, 1.0, &mut out) {
        return None;
    }
    Some(out)
}

fn accumulate_linear_weights(
    expression: &ScoreExpression,
    prefix_weight: f32,
    out: &mut [f32],
) -> bool {
    match expression {
        ScoreExpression::Leaf(index) => {
            if let Some(slot) = out.get_mut(*index) {
                *slot += prefix_weight;
                true
            } else {
                false
            }
        }
        ScoreExpression::Sum(children) => {
            for child in children {
                if !accumulate_linear_weights(child, prefix_weight, out) {
                    return false;
                }
            }
            true
        }
        ScoreExpression::Product { weight, expression } => {
            accumulate_linear_weights(expression, prefix_weight * *weight, out)
        }
        ScoreExpression::Max(_) => false,
    }
}

pub(crate) fn bm25_idf(total_docs: u64, doc_frequency: u64) -> f32 {
    if total_docs == 0 || doc_frequency == 0 {
        return 0.0;
    }
    let total = total_docs as f32;
    let df = doc_frequency.min(total_docs) as f32;
    (1.0 + ((total - df + 0.5) / (df + 0.5))).ln()
}

pub(crate) fn bm25_term_score(
    idf: f32,
    tf: u16,
    doc_len: u16,
    avg_doc_len: f32,
    params: Bm25Params,
) -> f32 {
    if tf == 0 || !idf.is_finite() || idf <= 0.0 {
        return 0.0;
    }
    let avg_doc_len = if avg_doc_len.is_finite() && avg_doc_len > 0.0 {
        avg_doc_len
    } else {
        1.0
    };
    let tf = tf as f32;
    let doc_len = doc_len as f32;
    let length_norm = 1.0 - params.b + params.b * (doc_len / avg_doc_len);
    let denominator = tf + params.k1 * length_norm;
    if denominator <= 0.0 || !denominator.is_finite() {
        return 0.0;
    }
    idf * ((tf * (params.k1 + 1.0)) / denominator)
}

#[cfg(test)]
mod tests {
    use super::{
        bm25_idf, bm25_term_score, linear_leaf_weights, Bm25Params, ScoreExpression,
        DEFAULT_BM25_B, DEFAULT_BM25_K1,
    };

    #[test]
    fn bm25_formula_matches_reference_fixture() {
        let params = Bm25Params {
            k1: DEFAULT_BM25_K1,
            b: DEFAULT_BM25_B,
        };
        let idf = bm25_idf(1000, 10);
        assert!((idf - 4.5573797).abs() < 1e-5, "unexpected idf: {idf}");
        let score = bm25_term_score(idf, 3, 120, 100.0, params);
        assert!(
            (score - 6.8672843).abs() < 1e-5,
            "unexpected score: {score}"
        );
    }

    #[test]
    fn score_expression_supports_sum_product_and_max() {
        let expression = ScoreExpression::Sum(vec![
            ScoreExpression::Product {
                weight: 2.0,
                expression: Box::new(ScoreExpression::Leaf(0)),
            },
            ScoreExpression::Max(vec![ScoreExpression::Leaf(1), ScoreExpression::Leaf(2)]),
        ]);
        let score = expression.evaluate(&[1.5, 0.7, 0.9]);
        assert!(
            (score - 3.9).abs() < 1e-6,
            "unexpected expression score: {score}"
        );
    }

    #[test]
    fn linear_weights_extract_for_sum_and_product_trees() {
        let expression = ScoreExpression::Sum(vec![
            ScoreExpression::Product {
                weight: 2.0,
                expression: Box::new(ScoreExpression::Leaf(0)),
            },
            ScoreExpression::Leaf(1),
            ScoreExpression::Product {
                weight: 0.5,
                expression: Box::new(ScoreExpression::Leaf(0)),
            },
        ]);
        let weights = linear_leaf_weights(&expression, 2).expect("linear weights");
        assert!((weights[0] - 2.5).abs() < 1e-6);
        assert!((weights[1] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn linear_weights_reject_max_trees() {
        let expression =
            ScoreExpression::Max(vec![ScoreExpression::Leaf(0), ScoreExpression::Leaf(1)]);
        assert!(
            linear_leaf_weights(&expression, 2).is_none(),
            "max expression must not produce linear weights"
        );
    }

    #[test]
    fn bm25_params_validation_enforces_domain() {
        assert!(Bm25Params { k1: 1.0, b: 0.5 }.validate().is_ok());
        assert!(Bm25Params { k1: -1.0, b: 0.5 }.validate().is_err());
        assert!(Bm25Params { k1: 1.0, b: 1.5 }.validate().is_err());
    }
}
