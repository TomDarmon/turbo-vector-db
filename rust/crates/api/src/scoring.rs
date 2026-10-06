use turbo_vector_core::Metric;

pub(crate) fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    left.iter().zip(right).map(|(a, b)| a * b).sum()
}

pub(crate) fn l2_norm(values: &[f32]) -> f32 {
    dot_product(values, values).sqrt()
}

pub(crate) fn cosine_similarity_with_norms(
    query: &[f32],
    candidate: &[f32],
    query_norm: f32,
    candidate_norm: f32,
) -> f32 {
    if query_norm == 0.0 || candidate_norm == 0.0 {
        return 0.0;
    }
    dot_product(query, candidate) / (query_norm * candidate_norm)
}

pub(crate) fn cosine_similarity(query: &[f32], candidate: &[f32]) -> f32 {
    cosine_similarity_with_norms(query, candidate, l2_norm(query), l2_norm(candidate))
}

fn squared_euclidean_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right)
        .map(|(a, b)| {
            let diff = a - b;
            diff * diff
        })
        .sum::<f32>()
}

pub(crate) fn negative_euclidean_score(query: &[f32], candidate: &[f32]) -> f32 {
    -squared_euclidean_distance(query, candidate).sqrt()
}

pub(crate) fn negative_squared_euclidean_score(query: &[f32], candidate: &[f32]) -> f32 {
    -squared_euclidean_distance(query, candidate)
}

pub(crate) fn exact_metric_score(metric: &Metric, query: &[f32], candidate: &[f32]) -> f32 {
    match metric {
        Metric::Dot => dot_product(query, candidate),
        Metric::Cosine => cosine_similarity(query, candidate),
        Metric::Euclidean => negative_euclidean_score(query, candidate),
    }
}

pub(crate) fn metric_score_with_cached_query_norm(
    metric: &Metric,
    query: &[f32],
    query_norm: f32,
    candidate: &[f32],
) -> f32 {
    match metric {
        Metric::Dot => dot_product(query, candidate),
        Metric::Cosine => {
            cosine_similarity_with_norms(query, candidate, query_norm, l2_norm(candidate))
        }
        Metric::Euclidean => negative_euclidean_score(query, candidate),
    }
}

pub(crate) fn metric_score_with_norms_and_squared_euclidean(
    metric: &Metric,
    query: &[f32],
    query_norm: f32,
    candidate: &[f32],
    candidate_norm: f32,
) -> f32 {
    match metric {
        Metric::Dot => dot_product(query, candidate),
        Metric::Cosine => {
            cosine_similarity_with_norms(query, candidate, query_norm, candidate_norm)
        }
        Metric::Euclidean => negative_squared_euclidean_score(query, candidate),
    }
}
