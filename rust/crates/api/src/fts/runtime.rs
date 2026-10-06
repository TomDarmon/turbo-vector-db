use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::Value;
use turbo_vector_manifest::Manifest;

use crate::{
    error::ApiError,
    filters::{parse_metadata_filter_with_limits, MetadataFilterExpression},
    models::UpsertVector,
    state::AppState,
    storage_logic::load_namespace_vectors_for_ids,
    validation::metadata_matches_filter,
};

use super::{
    bm25::{bm25_idf, linear_leaf_weights, Bm25Params},
    delta_apply::TermKey,
    maxscore::{
        maxscore_top_k, LexicalExecutionStats, ScoredBlock, ScoredPosting, TopKResult,
        WeightedTermPostings,
    },
    planner::{build_lexical_plan, LexicalPlan, PrefixExpansionLimits},
    rank_expr::RankExpr,
    scoring_kernel::score_postings_batched,
    term_meta::{FtsDocEntry, FtsDocLookup, FtsFieldMeta, PostingsBlockDescriptor},
};

#[derive(Debug, Clone)]
pub(crate) struct LexicalQueryExecution {
    pub(crate) scored: Vec<(UpsertVector, f32)>,
    pub(crate) stats: LexicalExecutionStats,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LexicalExplainOutput {
    pub(crate) plan: LexicalExplainPlan,
    pub(crate) execution: LexicalExplainExecution,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LexicalExplainPlan {
    pub(crate) selected_terms: Vec<LexicalExplainTerm>,
    pub(crate) conditional_boosts: Vec<LexicalExplainConditionalBoost>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LexicalExplainTerm {
    pub(crate) leaf_id: usize,
    pub(crate) field_name: String,
    pub(crate) term: String,
    pub(crate) term_hash: String,
    pub(crate) candidate_block_count: u64,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LexicalExplainConditionalBoost {
    pub(crate) index: usize,
    pub(crate) filter: Value,
    pub(crate) boost: f32,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LexicalExplainExecution {
    pub(crate) candidate_block_count: u64,
    pub(crate) blocks_decoded: u64,
    pub(crate) blocks_skipped: u64,
    pub(crate) docs_scored: u64,
    pub(crate) final_score_decomposition: Vec<LexicalExplainScoredDoc>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct LexicalExplainScoredDoc {
    pub(crate) vector_id: String,
    pub(crate) doc_id: u64,
    pub(crate) leaf_scores: Vec<f32>,
    pub(crate) base_score: f32,
    pub(crate) boost_multiplier: f32,
    pub(crate) final_score: f32,
    pub(crate) matched_boost_indexes: Vec<usize>,
}

#[derive(Debug, Clone)]
struct LoadedTerm {
    leaf_id: usize,
    blocks: Vec<ScoredBlock>,
}

#[derive(Debug, Clone)]
struct TermCandidateBlock {
    descriptor: PostingsBlockDescriptor,
    weighted_upper_bound: f32,
    selected_for_decode: bool,
    decode_attempted: bool,
    decoded: Option<ScoredBlock>,
}

#[derive(Debug, Clone)]
struct TermCandidate {
    leaf_id: usize,
    term_key: TermKey,
    idf: f32,
    avg_doc_len: f32,
    params: Bm25Params,
    blocks: Vec<TermCandidateBlock>,
}

#[derive(Debug, Clone, Copy, Default)]
struct PlanningStats {
    header_reads: u64,
    blocks_decoded: u64,
    metadata_blocks_skipped: u64,
}

#[derive(Debug, Clone)]
struct CompiledConditionalBoost {
    index: usize,
    filter_raw: Value,
    filter: MetadataFilterExpression,
    boost: f32,
}

pub(crate) async fn execute_lexical_query(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
    rank_expr: &RankExpr,
    schema: Option<&serde_json::Value>,
    top_k: u32,
    metadata_filter: Option<&MetadataFilterExpression>,
) -> Result<LexicalQueryExecution, ApiError> {
    let Some(index_meta) =
        load_queryable_index_meta(state, collection, namespace, manifest).await?
    else {
        return Ok(LexicalQueryExecution {
            scored: Vec::new(),
            stats: LexicalExecutionStats::default(),
        });
    };

    let plan = build_lexical_plan(
        rank_expr,
        &index_meta,
        schema,
        PrefixExpansionLimits {
            max_expansions: state.runtime.fts_prefix_max_expansions,
            max_expansion_bytes: state.runtime.fts_prefix_max_expansion_bytes,
        },
    )?;
    if plan.leaves.is_empty() {
        return Ok(LexicalQueryExecution {
            scored: Vec::new(),
            stats: LexicalExecutionStats::default(),
        });
    }

    let doc_lookup_payload =
        super::load_doc_lookup_for_index(state, collection, namespace, &index_meta).await?;
    let doc_lookup = build_doc_lookup(&doc_lookup_payload)?;
    let conditional_boosts = compile_conditional_boosts(state, &plan)?;

    let filter_doc = |doc_id: u64| -> bool {
        let Some(entry) = doc_lookup.get(&doc_id) else {
            return false;
        };
        metadata_matches_filter(entry.metadata.as_ref(), metadata_filter)
    };

    let top_k = top_k as usize;
    let (mut result, planning_stats) = if !conditional_boosts.is_empty() {
        let (loaded_terms, planning_stats) =
            load_term_postings_exhaustive(state, collection, namespace, &index_meta.fields, &plan)
                .await?;
        (
            exhaustive_expression_top_k(
                top_k,
                &plan,
                &loaded_terms,
                &doc_lookup,
                &conditional_boosts,
                |doc_id| filter_doc(doc_id),
            ),
            planning_stats,
        )
    } else if let Some(weights) = linear_leaf_weights(&plan.expression, plan.leaves.len()) {
        let (loaded_terms, planning_stats) = load_term_postings_linear(
            state,
            collection,
            namespace,
            &index_meta.fields,
            &plan,
            &weights,
            top_k,
            &filter_doc,
        )
        .await?;
        let weighted_terms = apply_linear_weights(&loaded_terms, &weights);
        (
            maxscore_top_k(top_k, weighted_terms, |doc_id| filter_doc(doc_id)),
            planning_stats,
        )
    } else {
        let (loaded_terms, planning_stats) =
            load_term_postings_exhaustive(state, collection, namespace, &index_meta.fields, &plan)
                .await?;
        (
            exhaustive_expression_top_k(
                top_k,
                &plan,
                &loaded_terms,
                &doc_lookup,
                &conditional_boosts,
                |doc_id| filter_doc(doc_id),
            ),
            planning_stats,
        )
    };

    result.stats.header_reads = result
        .stats
        .header_reads
        .saturating_add(planning_stats.header_reads);
    result.stats.blocks_decoded = result
        .stats
        .blocks_decoded
        .saturating_add(planning_stats.blocks_decoded);
    result.stats.blocks_skipped = result
        .stats
        .blocks_skipped
        .saturating_add(planning_stats.metadata_blocks_skipped);
    // Backward-compatible alias preserved for existing clients/tests.
    result.stats.blocks_scanned = result.stats.blocks_decoded;

    let scored =
        materialize_scored_docs(state, collection, namespace, manifest, &result, &doc_lookup)
            .await?;
    Ok(LexicalQueryExecution {
        scored,
        stats: result.stats,
    })
}

pub(crate) async fn explain_lexical_query(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
    rank_expr: &RankExpr,
    schema: Option<&serde_json::Value>,
    top_k: u32,
    metadata_filter: Option<&MetadataFilterExpression>,
) -> Result<LexicalExplainOutput, ApiError> {
    let Some(index_meta) =
        load_queryable_index_meta(state, collection, namespace, manifest).await?
    else {
        return Ok(LexicalExplainOutput {
            plan: LexicalExplainPlan {
                selected_terms: Vec::new(),
                conditional_boosts: Vec::new(),
            },
            execution: LexicalExplainExecution {
                candidate_block_count: 0,
                blocks_decoded: 0,
                blocks_skipped: 0,
                docs_scored: 0,
                final_score_decomposition: Vec::new(),
            },
        });
    };

    let plan = build_lexical_plan(
        rank_expr,
        &index_meta,
        schema,
        PrefixExpansionLimits {
            max_expansions: state.runtime.fts_prefix_max_expansions,
            max_expansion_bytes: state.runtime.fts_prefix_max_expansion_bytes,
        },
    )?;
    if plan.leaves.is_empty() {
        return Ok(LexicalExplainOutput {
            plan: LexicalExplainPlan {
                selected_terms: Vec::new(),
                conditional_boosts: Vec::new(),
            },
            execution: LexicalExplainExecution {
                candidate_block_count: 0,
                blocks_decoded: 0,
                blocks_skipped: 0,
                docs_scored: 0,
                final_score_decomposition: Vec::new(),
            },
        });
    }

    let doc_lookup_payload =
        super::load_doc_lookup_for_index(state, collection, namespace, &index_meta).await?;
    let doc_lookup = build_doc_lookup(&doc_lookup_payload)?;
    let conditional_boosts = compile_conditional_boosts(state, &plan)?;

    let (loaded_terms, planning_stats, selected_terms) = load_term_postings_exhaustive_with_trace(
        state,
        collection,
        namespace,
        &index_meta.fields,
        &plan,
    )
    .await?;
    let (final_score_decomposition, docs_scored) = score_docs_with_decomposition(
        top_k as usize,
        &plan,
        &loaded_terms,
        &doc_lookup,
        &conditional_boosts,
        metadata_filter,
    );

    Ok(LexicalExplainOutput {
        plan: LexicalExplainPlan {
            selected_terms,
            conditional_boosts: conditional_boosts
                .iter()
                .map(|conditional| LexicalExplainConditionalBoost {
                    index: conditional.index,
                    filter: conditional.filter_raw.clone(),
                    boost: conditional.boost,
                })
                .collect(),
        },
        execution: LexicalExplainExecution {
            candidate_block_count: planning_stats.header_reads,
            blocks_decoded: planning_stats.blocks_decoded,
            blocks_skipped: planning_stats.metadata_blocks_skipped,
            docs_scored,
            final_score_decomposition,
        },
    })
}

async fn load_queryable_index_meta(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
) -> Result<Option<super::term_meta::FtsIndexMeta>, ApiError> {
    if let Some(current) =
        super::load_fts_index_meta_for_generation(state, collection, namespace, manifest.generation)
            .await?
    {
        return Ok(Some(current));
    }
    let Some(previous_generation) = manifest.previous_generation else {
        return Ok(None);
    };
    super::load_fts_index_meta_for_generation(state, collection, namespace, previous_generation)
        .await
}

fn apply_linear_weights(loaded_terms: &[LoadedTerm], weights: &[f32]) -> Vec<WeightedTermPostings> {
    let mut out = Vec::new();
    for term in loaded_terms {
        let weight = weights.get(term.leaf_id).copied().unwrap_or(0.0);
        if weight <= 0.0 {
            continue;
        }
        let mut weighted_blocks = Vec::with_capacity(term.blocks.len());
        for block in &term.blocks {
            let postings = block
                .postings
                .iter()
                .map(|posting| ScoredPosting {
                    doc_id: posting.doc_id,
                    score: posting.score * weight,
                })
                .collect::<Vec<_>>();
            if postings.is_empty() {
                continue;
            }
            weighted_blocks.push(ScoredBlock {
                doc_id_min: block.doc_id_min,
                doc_id_max: block.doc_id_max,
                max_score: block.max_score * weight,
                postings,
            });
        }
        if !weighted_blocks.is_empty() {
            out.push(WeightedTermPostings {
                blocks: weighted_blocks,
            });
        }
    }
    out
}

fn exhaustive_expression_top_k<F>(
    top_k: usize,
    plan: &LexicalPlan,
    loaded_terms: &[LoadedTerm],
    doc_lookup: &BTreeMap<u64, &FtsDocEntry>,
    conditional_boosts: &[CompiledConditionalBoost],
    mut filter_doc: F,
) -> TopKResult
where
    F: FnMut(u64) -> bool,
{
    if top_k == 0 {
        return TopKResult {
            docs: Vec::new(),
            stats: LexicalExecutionStats::default(),
        };
    }

    let mut stats = LexicalExecutionStats::default();
    let mut leaf_scores_by_doc: BTreeMap<u64, Vec<f32>> = BTreeMap::new();
    for term in loaded_terms {
        for block in &term.blocks {
            stats.blocks_scanned = stats.blocks_scanned.saturating_add(1);
            for posting in &block.postings {
                let leaf_scores = leaf_scores_by_doc
                    .entry(posting.doc_id)
                    .or_insert_with(|| vec![0.0_f32; plan.leaves.len()]);
                if let Some(slot) = leaf_scores.get_mut(term.leaf_id) {
                    *slot += posting.score;
                }
            }
        }
    }

    let mut docs = leaf_scores_by_doc
        .into_iter()
        .filter_map(|(doc_id, leaf_scores)| {
            if !filter_doc(doc_id) {
                return None;
            }
            let mut score = plan.expression.evaluate(&leaf_scores);
            if !conditional_boosts.is_empty() {
                let entry = doc_lookup.get(&doc_id)?;
                let mut multiplier = 1.0_f32;
                for conditional in conditional_boosts {
                    if metadata_matches_filter(entry.metadata.as_ref(), Some(&conditional.filter)) {
                        multiplier *= conditional.boost;
                    }
                }
                score *= multiplier;
            }
            if score <= 0.0 {
                return None;
            }
            Some((doc_id, score))
        })
        .collect::<Vec<_>>();
    stats.docs_scored = docs.len() as u64;
    docs.sort_by(|(left_doc_id, left_score), (right_doc_id, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_doc_id.cmp(right_doc_id))
    });
    docs.truncate(top_k);
    TopKResult { docs, stats }
}

fn compile_conditional_boosts(
    state: &AppState,
    plan: &LexicalPlan,
) -> Result<Vec<CompiledConditionalBoost>, ApiError> {
    let mut out = Vec::with_capacity(plan.conditional_boosts.len());
    for (index, conditional) in plan.conditional_boosts.iter().enumerate() {
        let parsed = parse_metadata_filter_with_limits(
            Some(conditional.filter.clone()),
            state.filter_parser_limits(),
        )?;
        let Some(parsed) = parsed else {
            continue;
        };
        if conditional.boost <= 0.0 {
            continue;
        }
        out.push(CompiledConditionalBoost {
            index,
            filter_raw: conditional.filter.clone(),
            filter: parsed,
            boost: conditional.boost,
        });
    }
    Ok(out)
}

async fn load_term_postings_exhaustive(
    state: &AppState,
    collection: &str,
    namespace: &str,
    fields: &BTreeMap<String, FtsFieldMeta>,
    plan: &LexicalPlan,
) -> Result<(Vec<LoadedTerm>, PlanningStats), ApiError> {
    let mut loaded_terms = Vec::new();
    let mut planning = PlanningStats::default();
    for leaf in &plan.leaves {
        for term in &leaf.terms {
            let Some(field_meta) = fields.get(&term.field_hash) else {
                continue;
            };
            field_meta.corpus_stats.validate()?;
            let term_key = TermKey {
                field_name: term.field_name.clone(),
                field_hash: term.field_hash.clone(),
                term: term.term.clone(),
                term_hash: term.term_hash.clone(),
            };
            let term_meta = super::load_term_meta_for_ref(
                state,
                collection,
                namespace,
                &term_key,
                &term.term_ref,
            )
            .await?;
            planning.header_reads = planning
                .header_reads
                .saturating_add(term_meta.blocks.len() as u64);
            let idf = bm25_idf(
                field_meta.corpus_stats.document_count,
                term_meta.document_frequency,
            );
            if idf <= 0.0 {
                continue;
            }
            let blocks =
                super::load_term_blocks(state, collection, namespace, &term_key, &term_meta)
                    .await?;
            planning.blocks_decoded = planning.blocks_decoded.saturating_add(blocks.len() as u64);
            let mut scored_blocks = Vec::with_capacity(blocks.len());
            for block in blocks {
                if let Some(scored) = score_block(
                    block.postings.as_slice(),
                    block
                        .postings
                        .first()
                        .map(|posting| posting.doc_id)
                        .unwrap_or(0),
                    block
                        .postings
                        .last()
                        .map(|posting| posting.doc_id)
                        .unwrap_or(0),
                    idf,
                    field_meta.corpus_stats.avg_doc_len,
                    term.params,
                ) {
                    scored_blocks.push(scored);
                }
            }
            if scored_blocks.is_empty() {
                continue;
            }
            loaded_terms.push(LoadedTerm {
                leaf_id: leaf.leaf_id,
                blocks: scored_blocks,
            });
        }
    }
    Ok((loaded_terms, planning))
}

async fn load_term_postings_exhaustive_with_trace(
    state: &AppState,
    collection: &str,
    namespace: &str,
    fields: &BTreeMap<String, FtsFieldMeta>,
    plan: &LexicalPlan,
) -> Result<(Vec<LoadedTerm>, PlanningStats, Vec<LexicalExplainTerm>), ApiError> {
    let mut loaded_terms = Vec::new();
    let mut planning = PlanningStats::default();
    let mut selected_terms = Vec::new();
    for leaf in &plan.leaves {
        for term in &leaf.terms {
            let Some(field_meta) = fields.get(&term.field_hash) else {
                continue;
            };
            field_meta.corpus_stats.validate()?;
            let term_key = TermKey {
                field_name: term.field_name.clone(),
                field_hash: term.field_hash.clone(),
                term: term.term.clone(),
                term_hash: term.term_hash.clone(),
            };
            let term_meta = super::load_term_meta_for_ref(
                state,
                collection,
                namespace,
                &term_key,
                &term.term_ref,
            )
            .await?;
            selected_terms.push(LexicalExplainTerm {
                leaf_id: leaf.leaf_id,
                field_name: term.field_name.clone(),
                term: term.term.clone(),
                term_hash: term.term_hash.clone(),
                candidate_block_count: term_meta.blocks.len() as u64,
            });
            planning.header_reads = planning
                .header_reads
                .saturating_add(term_meta.blocks.len() as u64);
            let idf = bm25_idf(
                field_meta.corpus_stats.document_count,
                term_meta.document_frequency,
            );
            if idf <= 0.0 {
                continue;
            }
            let blocks =
                super::load_term_blocks(state, collection, namespace, &term_key, &term_meta)
                    .await?;
            planning.blocks_decoded = planning.blocks_decoded.saturating_add(blocks.len() as u64);
            let mut scored_blocks = Vec::with_capacity(blocks.len());
            for block in blocks {
                if let Some(scored) = score_block(
                    block.postings.as_slice(),
                    block
                        .postings
                        .first()
                        .map(|posting| posting.doc_id)
                        .unwrap_or(0),
                    block
                        .postings
                        .last()
                        .map(|posting| posting.doc_id)
                        .unwrap_or(0),
                    idf,
                    field_meta.corpus_stats.avg_doc_len,
                    term.params,
                ) {
                    scored_blocks.push(scored);
                }
            }
            if scored_blocks.is_empty() {
                continue;
            }
            loaded_terms.push(LoadedTerm {
                leaf_id: leaf.leaf_id,
                blocks: scored_blocks,
            });
        }
    }
    Ok((loaded_terms, planning, selected_terms))
}

fn score_docs_with_decomposition(
    top_k: usize,
    plan: &LexicalPlan,
    loaded_terms: &[LoadedTerm],
    doc_lookup: &BTreeMap<u64, &FtsDocEntry>,
    conditional_boosts: &[CompiledConditionalBoost],
    metadata_filter: Option<&MetadataFilterExpression>,
) -> (Vec<LexicalExplainScoredDoc>, u64) {
    if top_k == 0 {
        return (Vec::new(), 0);
    }
    let mut leaf_scores_by_doc: BTreeMap<u64, Vec<f32>> = BTreeMap::new();
    for term in loaded_terms {
        for block in &term.blocks {
            for posting in &block.postings {
                let leaf_scores = leaf_scores_by_doc
                    .entry(posting.doc_id)
                    .or_insert_with(|| vec![0.0_f32; plan.leaves.len()]);
                if let Some(slot) = leaf_scores.get_mut(term.leaf_id) {
                    *slot += posting.score;
                }
            }
        }
    }

    let mut docs = Vec::new();
    for (doc_id, leaf_scores) in leaf_scores_by_doc {
        let Some(entry) = doc_lookup.get(&doc_id) else {
            continue;
        };
        if !metadata_matches_filter(entry.metadata.as_ref(), metadata_filter) {
            continue;
        }
        let base_score = plan.expression.evaluate(&leaf_scores);
        if base_score <= 0.0 {
            continue;
        }
        let mut boost_multiplier = 1.0_f32;
        let mut matched_boost_indexes = Vec::new();
        for conditional in conditional_boosts {
            if metadata_matches_filter(entry.metadata.as_ref(), Some(&conditional.filter)) {
                boost_multiplier *= conditional.boost;
                matched_boost_indexes.push(conditional.index);
            }
        }
        let final_score = base_score * boost_multiplier;
        if final_score <= 0.0 {
            continue;
        }
        docs.push(LexicalExplainScoredDoc {
            vector_id: entry.vector_id.clone(),
            doc_id,
            leaf_scores,
            base_score,
            boost_multiplier,
            final_score,
            matched_boost_indexes,
        });
    }

    let docs_scored = docs.len() as u64;
    docs.sort_by(|left, right| {
        right
            .final_score
            .total_cmp(&left.final_score)
            .then_with(|| left.vector_id.cmp(&right.vector_id))
    });
    docs.truncate(top_k);
    (docs, docs_scored)
}

async fn load_term_postings_linear<F>(
    state: &AppState,
    collection: &str,
    namespace: &str,
    fields: &BTreeMap<String, FtsFieldMeta>,
    plan: &LexicalPlan,
    weights: &[f32],
    top_k: usize,
    filter_doc: &F,
) -> Result<(Vec<LoadedTerm>, PlanningStats), ApiError>
where
    F: Fn(u64) -> bool,
{
    let mut candidates = Vec::new();
    let mut planning = PlanningStats::default();
    for leaf in &plan.leaves {
        let weight = weights.get(leaf.leaf_id).copied().unwrap_or(0.0);
        if weight <= 0.0 {
            continue;
        }
        for term in &leaf.terms {
            let Some(field_meta) = fields.get(&term.field_hash) else {
                continue;
            };
            field_meta.corpus_stats.validate()?;
            let term_key = TermKey {
                field_name: term.field_name.clone(),
                field_hash: term.field_hash.clone(),
                term: term.term.clone(),
                term_hash: term.term_hash.clone(),
            };
            let term_meta = super::load_term_meta_for_ref(
                state,
                collection,
                namespace,
                &term_key,
                &term.term_ref,
            )
            .await?;
            planning.header_reads = planning
                .header_reads
                .saturating_add(term_meta.blocks.len() as u64);
            let idf = bm25_idf(
                field_meta.corpus_stats.document_count,
                term_meta.document_frequency,
            );
            if idf <= 0.0 {
                continue;
            }
            let mut blocks = term_meta
                .blocks
                .iter()
                .cloned()
                .map(|descriptor| TermCandidateBlock {
                    weighted_upper_bound: block_upper_bound(
                        idf,
                        descriptor.max_tf,
                        descriptor.min_doc_len,
                        field_meta.corpus_stats.avg_doc_len,
                        term.params,
                    ) * weight,
                    descriptor,
                    selected_for_decode: false,
                    decode_attempted: false,
                    decoded: None,
                })
                .collect::<Vec<_>>();
            if blocks.is_empty() {
                continue;
            }
            if let Some((best_index, _)) =
                blocks.iter().enumerate().max_by(|(_, left), (_, right)| {
                    left.weighted_upper_bound
                        .total_cmp(&right.weighted_upper_bound)
                })
            {
                if blocks[best_index].weighted_upper_bound > 0.0 {
                    blocks[best_index].selected_for_decode = true;
                }
            }
            candidates.push(TermCandidate {
                leaf_id: leaf.leaf_id,
                term_key,
                idf,
                avg_doc_len: field_meta.corpus_stats.avg_doc_len,
                params: term.params,
                blocks,
            });
        }
    }

    if candidates.is_empty() {
        return Ok((Vec::new(), planning));
    }

    loop {
        let mut decoded_this_round = 0_u64;
        for candidate in &mut candidates {
            decoded_this_round = decoded_this_round.saturating_add(
                decode_selected_term_blocks(state, collection, namespace, candidate).await? as u64,
            );
        }
        planning.blocks_decoded = planning.blocks_decoded.saturating_add(decoded_this_round);

        let loaded_terms = materialize_loaded_terms(&candidates);
        let weighted_terms = apply_linear_weights(&loaded_terms, weights);
        let result = maxscore_top_k(top_k, weighted_terms, |doc_id| filter_doc(doc_id));
        let threshold = current_top_k_threshold(result.docs.as_slice(), top_k);

        let mut to_select = Vec::new();
        for (term_index, candidate) in candidates.iter().enumerate() {
            for (block_index, block) in candidate.blocks.iter().enumerate() {
                if block.selected_for_decode {
                    continue;
                }
                let potential = block.weighted_upper_bound
                    + overlap_upper_from_other_terms(
                        &candidates,
                        term_index,
                        block.descriptor.doc_id_min,
                        block.descriptor.doc_id_max,
                    );
                if potential > threshold {
                    to_select.push((term_index, block_index));
                }
            }
        }
        if to_select.is_empty() {
            break;
        }
        for (term_index, block_index) in to_select {
            if let Some(block) = candidates
                .get_mut(term_index)
                .and_then(|term| term.blocks.get_mut(block_index))
            {
                block.selected_for_decode = true;
            }
        }
    }

    planning.metadata_blocks_skipped = candidates
        .iter()
        .flat_map(|candidate| candidate.blocks.iter())
        .filter(|block| !block.selected_for_decode)
        .count() as u64;
    Ok((materialize_loaded_terms(&candidates), planning))
}

async fn decode_selected_term_blocks(
    state: &AppState,
    collection: &str,
    namespace: &str,
    candidate: &mut TermCandidate,
) -> Result<usize, ApiError> {
    let mut descriptors = Vec::new();
    for block in &candidate.blocks {
        if block.selected_for_decode && !block.decode_attempted {
            descriptors.push(block.descriptor.clone());
        }
    }
    if descriptors.is_empty() {
        return Ok(0);
    }

    let loaded_blocks = super::load_term_blocks_for_descriptors(
        state,
        collection,
        namespace,
        &candidate.term_key,
        descriptors.as_slice(),
    )
    .await?;
    let mut scored_by_block = BTreeMap::new();
    for block in loaded_blocks {
        let first_doc_id = block
            .postings
            .first()
            .map(|posting| posting.doc_id)
            .unwrap_or(0);
        let last_doc_id = block
            .postings
            .last()
            .map(|posting| posting.doc_id)
            .unwrap_or(0);
        if let Some(scored) = score_block(
            block.postings.as_slice(),
            first_doc_id,
            last_doc_id,
            candidate.idf,
            candidate.avg_doc_len,
            candidate.params,
        ) {
            scored_by_block.insert(block.block_id, scored);
        }
    }

    for block in &mut candidate.blocks {
        if block.selected_for_decode && !block.decode_attempted {
            block.decode_attempted = true;
            block.decoded = scored_by_block.remove(&block.descriptor.block_id);
        }
    }
    Ok(descriptors.len())
}

fn materialize_loaded_terms(candidates: &[TermCandidate]) -> Vec<LoadedTerm> {
    let mut loaded = Vec::new();
    for candidate in candidates {
        let mut blocks = candidate
            .blocks
            .iter()
            .filter_map(|block| block.decoded.clone())
            .collect::<Vec<_>>();
        blocks.sort_by_key(|block| block.doc_id_min);
        if blocks.is_empty() {
            continue;
        }
        loaded.push(LoadedTerm {
            leaf_id: candidate.leaf_id,
            blocks,
        });
    }
    loaded
}

fn overlap_upper_from_other_terms(
    candidates: &[TermCandidate],
    current_term_index: usize,
    doc_id_min: u64,
    doc_id_max: u64,
) -> f32 {
    let mut sum = 0.0_f32;
    for (index, candidate) in candidates.iter().enumerate() {
        if index == current_term_index {
            continue;
        }
        let max_overlap = candidate
            .blocks
            .iter()
            .filter(|block| {
                ranges_overlap(
                    doc_id_min,
                    doc_id_max,
                    block.descriptor.doc_id_min,
                    block.descriptor.doc_id_max,
                )
            })
            .fold(0.0_f32, |max_upper, block| {
                max_upper.max(block.weighted_upper_bound)
            });
        sum += max_overlap;
    }
    sum
}

fn ranges_overlap(left_min: u64, left_max: u64, right_min: u64, right_max: u64) -> bool {
    !(left_max < right_min || right_max < left_min)
}

fn block_upper_bound(
    idf: f32,
    max_tf: u16,
    min_doc_len: u16,
    avg_doc_len: f32,
    params: Bm25Params,
) -> f32 {
    if idf <= 0.0 || max_tf == 0 {
        return 0.0;
    }
    let tf = max_tf as f32;
    let avg_doc_len = if avg_doc_len.is_finite() && avg_doc_len > 0.0 {
        avg_doc_len
    } else {
        1.0
    };
    let min_doc_len = min_doc_len.max(1) as f32;
    let length_norm = 1.0 - params.b + params.b * (min_doc_len / avg_doc_len);
    let denominator = tf + params.k1 * length_norm;
    if denominator <= 0.0 || !denominator.is_finite() {
        return 0.0;
    }
    idf * ((tf * (params.k1 + 1.0)) / denominator)
}

fn score_block(
    postings: &[super::postings_codec::Posting],
    doc_id_min: u64,
    doc_id_max: u64,
    idf: f32,
    avg_doc_len: f32,
    params: Bm25Params,
) -> Option<ScoredBlock> {
    let scored = score_postings_batched(postings, idf, avg_doc_len, params, 1.0);
    if scored.postings.is_empty() {
        return None;
    }
    Some(ScoredBlock {
        doc_id_min,
        doc_id_max,
        max_score: scored.max_score,
        postings: scored.postings,
    })
}

fn current_top_k_threshold(docs: &[(u64, f32)], top_k: usize) -> f32 {
    if docs.len() < top_k {
        return 0.0;
    }
    docs.last().map(|(_, score)| *score).unwrap_or(0.0)
}

fn build_doc_lookup<'a>(
    lookup: &'a FtsDocLookup,
) -> Result<BTreeMap<u64, &'a FtsDocEntry>, ApiError> {
    let mut out = BTreeMap::new();
    for entry in &lookup.docs {
        if let Some(existing) = out.insert(entry.doc_id, entry) {
            return Err(ApiError::internal(format!(
                "duplicate doc_id {} for vectors '{}' and '{}'",
                entry.doc_id, existing.vector_id, entry.vector_id
            )));
        }
    }
    Ok(out)
}

async fn materialize_scored_docs(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
    result: &TopKResult,
    doc_lookup: &BTreeMap<u64, &FtsDocEntry>,
) -> Result<Vec<(UpsertVector, f32)>, ApiError> {
    let mut target_ids = BTreeSet::new();
    for (doc_id, _) in &result.docs {
        if let Some(entry) = doc_lookup.get(doc_id) {
            target_ids.insert(entry.vector_id.clone());
        }
    }
    let vectors =
        load_namespace_vectors_for_ids(state, collection, namespace, manifest, &target_ids).await?;

    let mut scored = Vec::with_capacity(result.docs.len());
    for (doc_id, score) in &result.docs {
        let Some(entry) = doc_lookup.get(doc_id) else {
            continue;
        };
        let mut vector = if let Some(vector) = vectors.get(&entry.vector_id) {
            vector.clone()
        } else {
            continue;
        };
        if vector.metadata.is_none() {
            vector.metadata = entry.metadata.clone();
        }
        scored.push((vector, *score));
    }
    scored.sort_by(|(left_vector, left_score), (right_vector, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_vector.id.cmp(&right_vector.id))
    });
    Ok(scored)
}

#[cfg(test)]
mod tests {
    use super::{block_upper_bound, ranges_overlap};
    use crate::fts::bm25::Bm25Params;

    #[test]
    fn ranges_overlap_handles_disjoint_and_intersecting_ranges() {
        assert!(ranges_overlap(10, 20, 20, 30));
        assert!(ranges_overlap(1, 5, 2, 4));
        assert!(!ranges_overlap(1, 5, 6, 9));
    }

    #[test]
    fn block_upper_bound_is_positive_for_valid_tf_inputs() {
        let upper = block_upper_bound(1.5, 4, 8, 12.0, Bm25Params::default());
        assert!(upper > 0.0);
    }
}
