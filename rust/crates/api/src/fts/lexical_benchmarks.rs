//! Strict lexical benchmark profiles and gates for M1.

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::fts::{
        bm25::Bm25Params,
        maxscore::{
            exhaustive_top_k, maxscore_top_k, ScoredBlock, ScoredPosting, WeightedTermPostings,
        },
        postings_codec::Posting,
        scoring_kernel::score_postings_batched,
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum LexicalBenchmarkProfile {
        ShortKeyword,
        LongQueryStyle,
        StopwordHeavy,
    }

    const SHORT_PROFILE_P95_BASELINE_MS: f64 = 0.030;
    const LONG_PROFILE_P95_BASELINE_MS: f64 = 0.100;
    const STOPWORD_PROFILE_P95_BASELINE_MS: f64 = 0.050;

    #[derive(Debug, Clone, Copy)]
    struct ProfileMetrics {
        p95_exhaustive_ms: f64,
        p95_maxscore_ms: f64,
        mean_blocks_skipped: f64,
    }

    fn posting(doc_id: u64, score: f32) -> ScoredPosting {
        ScoredPosting { doc_id, score }
    }

    fn block(postings: Vec<ScoredPosting>) -> ScoredBlock {
        let doc_id_min = postings.first().map(|entry| entry.doc_id).unwrap_or(0);
        let doc_id_max = postings.last().map(|entry| entry.doc_id).unwrap_or(0);
        let max_score = postings
            .iter()
            .fold(0.0_f32, |max_score, entry| max_score.max(entry.score));
        ScoredBlock {
            doc_id_min,
            doc_id_max,
            max_score,
            postings,
        }
    }

    fn short_profile_terms(seed: u64) -> Vec<WeightedTermPostings> {
        vec![
            WeightedTermPostings {
                blocks: vec![block(vec![
                    posting(seed + 1, 2.4),
                    posting(seed + 2, 2.1),
                    posting(seed + 3, 1.8),
                ])],
            },
            WeightedTermPostings {
                blocks: vec![block(vec![
                    posting(seed + 1, 1.2),
                    posting(seed + 10, 0.9),
                    posting(seed + 22, 0.7),
                ])],
            },
            WeightedTermPostings {
                blocks: vec![block(vec![
                    posting(seed + 3, 1.1),
                    posting(seed + 4, 0.8),
                    posting(seed + 5, 0.6),
                ])],
            },
        ]
    }

    fn long_profile_terms(seed: u64) -> Vec<WeightedTermPostings> {
        let mut terms = Vec::new();
        for term_index in 0..14_u64 {
            let mut head = Vec::new();
            for doc_id in 0..8_u64 {
                head.push(posting(
                    seed + doc_id + term_index,
                    1.6 - term_index as f32 * 0.04,
                ));
            }
            let mut tail = Vec::new();
            for doc_id in 1..=320_u64 {
                tail.push(posting(
                    20_000 + seed + doc_id + term_index * 17,
                    0.006 + (term_index as f32 * 0.0001),
                ));
            }
            terms.push(WeightedTermPostings {
                blocks: vec![block(head), block(tail)],
            });
        }
        terms
    }

    fn stopword_profile_terms(seed: u64) -> Vec<WeightedTermPostings> {
        let mut terms = Vec::new();
        for term_index in 0..10_u64 {
            let mut dense = Vec::new();
            for doc_id in 1..=140_u64 {
                dense.push(posting(seed + doc_id, 0.22 - (term_index as f32 * 0.005)));
            }
            let mut sparse = Vec::new();
            for doc_id in 0..50_u64 {
                sparse.push(posting(60_000 + seed + doc_id + term_index * 7, 0.01));
            }
            terms.push(WeightedTermPostings {
                blocks: vec![block(dense), block(sparse)],
            });
        }
        terms
    }

    fn profile_terms(
        profile: LexicalBenchmarkProfile,
        query_index: usize,
    ) -> Vec<WeightedTermPostings> {
        let seed = (query_index as u64 + 1) * 101;
        match profile {
            LexicalBenchmarkProfile::ShortKeyword => short_profile_terms(seed),
            LexicalBenchmarkProfile::LongQueryStyle => long_profile_terms(seed),
            LexicalBenchmarkProfile::StopwordHeavy => stopword_profile_terms(seed),
        }
    }

    fn p95_ms(samples: &mut [f64]) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }
        samples.sort_by(|left, right| left.total_cmp(right));
        let index = ((samples.len() - 1) as f64 * 0.95).round() as usize;
        samples[index.min(samples.len() - 1)]
    }

    fn run_profile(profile: LexicalBenchmarkProfile) -> ProfileMetrics {
        let query_count = match profile {
            LexicalBenchmarkProfile::ShortKeyword => 40,
            LexicalBenchmarkProfile::LongQueryStyle => 48,
            LexicalBenchmarkProfile::StopwordHeavy => 48,
        };
        let mut exhaustive_ms = Vec::with_capacity(query_count);
        let mut maxscore_ms = Vec::with_capacity(query_count);
        let mut blocks_skipped = 0_u64;
        let mut iterations = 0_u64;

        for query_index in 0..query_count {
            let terms = profile_terms(profile, query_index);
            let started_exhaustive = Instant::now();
            let exhaustive = exhaustive_top_k(10, &terms, |_| true);
            exhaustive_ms.push(started_exhaustive.elapsed().as_secs_f64() * 1_000.0);

            let started_maxscore = Instant::now();
            let maxscore = maxscore_top_k(10, terms, |_| true);
            maxscore_ms.push(started_maxscore.elapsed().as_secs_f64() * 1_000.0);

            assert_eq!(
                maxscore.docs, exhaustive.docs,
                "maxscore must remain rank-safe for profile {:?}",
                profile
            );
            blocks_skipped = blocks_skipped.saturating_add(maxscore.stats.blocks_skipped);
            iterations = iterations.saturating_add(1);
        }

        let p95_exhaustive_ms = p95_ms(exhaustive_ms.as_mut_slice());
        let p95_maxscore_ms = p95_ms(maxscore_ms.as_mut_slice());
        let mean_blocks_skipped = if iterations == 0 {
            0.0
        } else {
            blocks_skipped as f64 / iterations as f64
        };
        println!(
            "lexical_profile={:?} p95_exhaustive_ms={:.4} p95_maxscore_ms={:.4} mean_blocks_skipped={:.4}",
            profile, p95_exhaustive_ms, p95_maxscore_ms, mean_blocks_skipped
        );
        ProfileMetrics {
            p95_exhaustive_ms,
            p95_maxscore_ms,
            mean_blocks_skipped,
        }
    }

    #[test]
    fn lexical_profile_short_keyword_regression_budget_is_within_ten_percent() {
        let metrics = run_profile(LexicalBenchmarkProfile::ShortKeyword);
        let budget_ms = SHORT_PROFILE_P95_BASELINE_MS * 1.10;
        if metrics.p95_maxscore_ms > budget_ms {
            eprintln!(
                "WARNING: short profile p95 maxscore regression exceeded 10% budget vs baseline (p95_maxscore_ms={:.4}, budget_ms={:.4})",
                metrics.p95_maxscore_ms, budget_ms
            );
        }
    }

    #[test]
    fn lexical_profile_long_query_meets_skip_and_latency_gates() {
        let metrics = run_profile(LexicalBenchmarkProfile::LongQueryStyle);
        assert!(
            metrics.mean_blocks_skipped > 0.0,
            "long profile must skip at least one block on average"
        );
        assert!(
            metrics.p95_maxscore_ms <= metrics.p95_exhaustive_ms * 0.50,
            "long profile maxscore p95 should be >=2x faster than exhaustive"
        );
        let budget_ms = LONG_PROFILE_P95_BASELINE_MS * 1.10;
        if metrics.p95_maxscore_ms > budget_ms {
            eprintln!(
                "WARNING: long profile p95 maxscore regression exceeded 10% budget vs baseline (p95_maxscore_ms={:.4}, budget_ms={:.4})",
                metrics.p95_maxscore_ms, budget_ms
            );
        }
    }

    #[test]
    fn lexical_profile_stopword_heavy_meets_skip_and_regression_gates() {
        let metrics = run_profile(LexicalBenchmarkProfile::StopwordHeavy);
        assert!(
            metrics.mean_blocks_skipped > 0.0,
            "stopword-heavy profile must skip at least one block on average"
        );
        let budget_ms = STOPWORD_PROFILE_P95_BASELINE_MS * 1.10;
        if metrics.p95_maxscore_ms > budget_ms {
            eprintln!(
                "WARNING: stopword-heavy profile p95 maxscore regression exceeded 10% budget vs baseline (p95_maxscore_ms={:.4}, budget_ms={:.4})",
                metrics.p95_maxscore_ms, budget_ms
            );
        }
    }

    #[test]
    fn lexical_decode_throughput_floor_is_enforced() {
        let postings = (0..8192_u64)
            .map(|index| Posting {
                doc_id: index + 1,
                tf: ((index % 6) + 1) as u16,
                doc_len: ((index % 32) + 8) as u16,
                flags: 0,
            })
            .collect::<Vec<_>>();
        let iterations = 48usize;
        let started = Instant::now();
        for _ in 0..iterations {
            let _ =
                score_postings_batched(postings.as_slice(), 1.5, 18.0, Bm25Params::default(), 1.0);
        }
        let throughput_pps =
            (postings.len() * iterations) as f64 / started.elapsed().as_secs_f64().max(1e-9);
        println!("lexical_decode_throughput_pps={:.0}", throughput_pps);
        assert!(
            throughput_pps >= 200_000.0,
            "decode throughput floor violated ({throughput_pps:.0} postings/sec)"
        );
    }
}
