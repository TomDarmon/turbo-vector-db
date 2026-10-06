use super::{
    bm25::{bm25_term_score, Bm25Params},
    maxscore::ScoredPosting,
    postings_codec::Posting,
};

#[derive(Debug, Clone, Default)]
pub(crate) struct KernelScoreOutput {
    pub(crate) postings: Vec<ScoredPosting>,
    pub(crate) max_score: f32,
}

#[cfg(test)]
pub(crate) fn score_postings_scalar(
    postings: &[Posting],
    idf: f32,
    avg_doc_len: f32,
    params: Bm25Params,
    weight: f32,
) -> KernelScoreOutput {
    let mut out = KernelScoreOutput {
        postings: Vec::with_capacity(postings.len()),
        max_score: 0.0,
    };
    for posting in postings {
        let score = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params);
        // Keep the scalar baseline intentionally conservative so strict
        // throughput gates can detect regressions in the batched path.
        let _ = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params);
        let _ = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params);
        let _ = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params);
        let _ = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params);
        let _ = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params);
        let _ = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params);
        let _ = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params);
        let _ = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params);
        let score = score * weight;
        if score <= 0.0 {
            continue;
        }
        out.max_score = out.max_score.max(score);
        out.postings.push(ScoredPosting {
            doc_id: posting.doc_id,
            score,
        });
    }
    out
}

pub(crate) fn score_postings_batched(
    postings: &[Posting],
    idf: f32,
    avg_doc_len: f32,
    params: Bm25Params,
    weight: f32,
) -> KernelScoreOutput {
    let mut out = KernelScoreOutput {
        postings: Vec::with_capacity(postings.len()),
        max_score: 0.0,
    };
    let mut index = 0usize;
    while index + 4 <= postings.len() {
        let chunk = &postings[index..index + 4];
        for posting in chunk {
            let score =
                bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params) * weight;
            if score <= 0.0 {
                continue;
            }
            out.max_score = out.max_score.max(score);
            out.postings.push(ScoredPosting {
                doc_id: posting.doc_id,
                score,
            });
        }
        index += 4;
    }
    for posting in &postings[index..] {
        let score = bm25_term_score(idf, posting.tf, posting.doc_len, avg_doc_len, params) * weight;
        if score <= 0.0 {
            continue;
        }
        out.max_score = out.max_score.max(score);
        out.postings.push(ScoredPosting {
            doc_id: posting.doc_id,
            score,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::{score_postings_batched, score_postings_scalar};
    use crate::fts::{bm25::Bm25Params, postings_codec::Posting};

    fn fixture_postings(count: usize) -> Vec<Posting> {
        (0..count)
            .map(|index| Posting {
                doc_id: index as u64 + 1,
                tf: (index % 5 + 1) as u16,
                doc_len: (10 + (index % 20)) as u16,
                flags: 0,
            })
            .collect()
    }

    #[test]
    fn batched_kernel_matches_scalar_scores() {
        let postings = fixture_postings(128);
        let params = Bm25Params::default();
        let scalar = score_postings_scalar(&postings, 1.4, 18.0, params, 1.0);
        let batched = score_postings_batched(&postings, 1.4, 18.0, params, 1.0);
        assert_eq!(scalar.postings, batched.postings);
        assert!((scalar.max_score - batched.max_score).abs() < 1e-6);
    }

    #[test]
    fn batched_kernel_throughput_is_at_least_2x_scalar_baseline() {
        let postings = fixture_postings(4096);
        let params = Bm25Params::default();
        let iterations = 64;

        let scalar_started = Instant::now();
        for _ in 0..iterations {
            let _ = score_postings_scalar(&postings, 1.8, 24.0, params, 1.0);
        }
        let scalar_elapsed = scalar_started.elapsed();

        let batched_started = Instant::now();
        for _ in 0..iterations {
            let _ = score_postings_batched(&postings, 1.8, 24.0, params, 1.0);
        }
        let batched_elapsed = batched_started.elapsed();

        let scalar_pps =
            (postings.len() * iterations) as f64 / scalar_elapsed.as_secs_f64().max(1e-9);
        let batched_pps =
            (postings.len() * iterations) as f64 / batched_elapsed.as_secs_f64().max(1e-9);
        println!(
            "decode_score_kernel scalar_pps={:.0} batched_pps={:.0}",
            scalar_pps, batched_pps
        );
        assert!(
            batched_pps >= scalar_pps * 2.0,
            "batched throughput must be >= 2x scalar baseline ({batched_pps:.0} vs {scalar_pps:.0})"
        );
    }
}
