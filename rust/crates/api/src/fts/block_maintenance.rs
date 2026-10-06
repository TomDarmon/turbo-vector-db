use std::collections::{BTreeMap, BTreeSet};

use crate::{error::ApiError, fts::postings_codec::Posting};

#[derive(Debug, Clone, Copy)]
pub(crate) struct BlockPolicy {
    pub(crate) target_postings: usize,
    pub(crate) split_threshold: usize,
    pub(crate) merge_threshold: usize,
    pub(crate) max_term_blocks_touched_per_doc_update: usize,
    pub(crate) enable_delta_rebalance: bool,
}

impl BlockPolicy {
    pub(crate) fn normalized(self) -> Self {
        let split_threshold = self.split_threshold.max(2);
        let merge_threshold = self
            .merge_threshold
            .max(1)
            .min(split_threshold.saturating_sub(1));
        let target_lower_bound = merge_threshold.max(1);
        let target_upper_bound = split_threshold.saturating_sub(1).max(target_lower_bound);
        let target_postings = self
            .target_postings
            .clamp(target_lower_bound, target_upper_bound);
        Self {
            target_postings,
            split_threshold,
            merge_threshold,
            max_term_blocks_touched_per_doc_update: self
                .max_term_blocks_touched_per_doc_update
                .max(1),
            enable_delta_rebalance: self.enable_delta_rebalance,
        }
    }
}

impl Default for BlockPolicy {
    fn default() -> Self {
        Self {
            target_postings: 256,
            split_threshold: 512,
            merge_threshold: 128,
            max_term_blocks_touched_per_doc_update: 8,
            enable_delta_rebalance: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostingMutation {
    pub(crate) doc_id: u64,
    pub(crate) posting: Option<Posting>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MutablePostingBlock {
    pub(crate) block_id: String,
    pub(crate) source_generation: u64,
    pub(crate) source_pack_id: String,
    pub(crate) source_pack_offset: u32,
    pub(crate) source_pack_len: u32,
    pub(crate) source_checksum: String,
    pub(crate) source_byte_len: u32,
    pub(crate) source_max_term_score: f32,
    pub(crate) source_max_tf: u16,
    pub(crate) source_min_doc_len: u16,
    pub(crate) postings: Vec<Posting>,
    pub(crate) dirty: bool,
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub(crate) struct MaintenanceOutcome {
    pub(crate) blocks: Vec<MutablePostingBlock>,
    pub(crate) rewritten_block_ids: BTreeSet<String>,
    pub(crate) removed_block_ids: BTreeSet<String>,
}

const BLOCK_ID_HEX_WIDTH: usize = 16;

pub(crate) fn apply_block_maintenance(
    mut blocks: Vec<MutablePostingBlock>,
    mutations: &[PostingMutation],
    policy: BlockPolicy,
) -> Result<MaintenanceOutcome, ApiError> {
    let policy = policy.normalized();
    validate_blocks(&blocks)?;
    if mutations.is_empty() {
        return Ok(MaintenanceOutcome {
            blocks,
            rewritten_block_ids: BTreeSet::new(),
            removed_block_ids: BTreeSet::new(),
        });
    }

    let mut deduped_mutations = BTreeMap::new();
    for mutation in mutations {
        deduped_mutations.insert(mutation.doc_id, mutation.posting);
    }

    let mut rewritten_block_ids = BTreeSet::new();
    let mut removed_block_ids = BTreeSet::new();
    let mut used_block_ids: BTreeSet<String> =
        blocks.iter().map(|block| block.block_id.clone()).collect();
    let mut block_id_seed = next_block_seed_start(&used_block_ids);

    for (doc_id, maybe_posting) in deduped_mutations {
        let mut touched_for_mutation = BTreeSet::new();
        apply_single_mutation(
            &mut blocks,
            doc_id,
            maybe_posting,
            policy,
            &mut rewritten_block_ids,
            &mut removed_block_ids,
            &mut touched_for_mutation,
            &mut block_id_seed,
            &mut used_block_ids,
        )?;
        if touched_for_mutation.len() > policy.max_term_blocks_touched_per_doc_update {
            return Err(ApiError::internal(format!(
                "delta rebalance touched {} blocks for a single document update (max={})",
                touched_for_mutation.len(),
                policy.max_term_blocks_touched_per_doc_update
            )));
        }
    }

    validate_blocks(&blocks)?;
    Ok(MaintenanceOutcome {
        blocks,
        rewritten_block_ids,
        removed_block_ids,
    })
}

#[allow(clippy::too_many_arguments)]
fn apply_single_mutation(
    blocks: &mut Vec<MutablePostingBlock>,
    doc_id: u64,
    maybe_posting: Option<Posting>,
    policy: BlockPolicy,
    rewritten_block_ids: &mut BTreeSet<String>,
    removed_block_ids: &mut BTreeSet<String>,
    touched_for_mutation: &mut BTreeSet<String>,
    block_id_seed: &mut u64,
    used_block_ids: &mut BTreeSet<String>,
) -> Result<(), ApiError> {
    if blocks.is_empty() {
        if let Some(posting) = maybe_posting {
            let block_id = next_block_id(block_id_seed, used_block_ids);
            rewritten_block_ids.insert(block_id.clone());
            touched_for_mutation.insert(block_id.clone());
            blocks.push(MutablePostingBlock {
                block_id,
                source_generation: 0,
                source_pack_id: String::new(),
                source_pack_offset: 0,
                source_pack_len: 0,
                source_checksum: String::new(),
                source_byte_len: 0,
                source_max_term_score: 0.0,
                source_max_tf: 0,
                source_min_doc_len: 0,
                postings: vec![posting],
                dirty: true,
            });
        }
        return Ok(());
    }

    let index = find_candidate_block_index(blocks, doc_id).unwrap_or_else(|| blocks.len() - 1);
    let block = blocks
        .get_mut(index)
        .ok_or_else(|| ApiError::internal("block maintenance index resolution failed"))?;
    let posting_index = block
        .postings
        .binary_search_by_key(&doc_id, |posting| posting.doc_id);
    match (maybe_posting, posting_index) {
        (Some(posting), Ok(existing_index)) => {
            if block.postings[existing_index] != posting {
                block.postings[existing_index] = posting;
                mark_block_dirty(
                    block,
                    rewritten_block_ids,
                    touched_for_mutation,
                    policy.enable_delta_rebalance,
                );
            }
        }
        (Some(posting), Err(insert_index)) => {
            block.postings.insert(insert_index, posting);
            mark_block_dirty(
                block,
                rewritten_block_ids,
                touched_for_mutation,
                policy.enable_delta_rebalance,
            );
        }
        (None, Ok(existing_index)) => {
            block.postings.remove(existing_index);
            mark_block_dirty(
                block,
                rewritten_block_ids,
                touched_for_mutation,
                policy.enable_delta_rebalance,
            );
        }
        (None, Err(_)) => {
            return Ok(());
        }
    }

    if blocks[index].postings.is_empty() {
        let removed = blocks.remove(index);
        rewritten_block_ids.insert(removed.block_id.clone());
        touched_for_mutation.insert(removed.block_id.clone());
        removed_block_ids.insert(removed.block_id);
        return Ok(());
    }

    if !policy.enable_delta_rebalance {
        return Ok(());
    }

    rebalance_at_index(
        blocks,
        index,
        policy,
        rewritten_block_ids,
        removed_block_ids,
        touched_for_mutation,
        block_id_seed,
        used_block_ids,
    )
}

#[allow(clippy::too_many_arguments)]
fn rebalance_at_index(
    blocks: &mut Vec<MutablePostingBlock>,
    mut index: usize,
    policy: BlockPolicy,
    rewritten_block_ids: &mut BTreeSet<String>,
    removed_block_ids: &mut BTreeSet<String>,
    touched_for_mutation: &mut BTreeSet<String>,
    block_id_seed: &mut u64,
    used_block_ids: &mut BTreeSet<String>,
) -> Result<(), ApiError> {
    loop {
        let len = blocks[index].postings.len();
        if len > policy.split_threshold {
            let split_at = len / 2;
            let right_postings = blocks[index].postings.split_off(split_at);
            if right_postings.is_empty() || blocks[index].postings.is_empty() {
                return Err(ApiError::internal(
                    "split produced empty postings block during rebalance",
                ));
            }
            mark_block_dirty(
                &mut blocks[index],
                rewritten_block_ids,
                touched_for_mutation,
                true,
            );
            let right_block_id = next_block_id(block_id_seed, used_block_ids);
            rewritten_block_ids.insert(right_block_id.clone());
            touched_for_mutation.insert(right_block_id.clone());
            blocks.insert(
                index + 1,
                MutablePostingBlock {
                    block_id: right_block_id,
                    source_generation: blocks[index].source_generation,
                    source_pack_id: String::new(),
                    source_pack_offset: 0,
                    source_pack_len: 0,
                    source_checksum: String::new(),
                    source_byte_len: 0,
                    source_max_term_score: 0.0,
                    source_max_tf: 0,
                    source_min_doc_len: 0,
                    postings: right_postings,
                    dirty: true,
                },
            );
            continue;
        }

        if len >= policy.merge_threshold || blocks.len() == 1 {
            break;
        }

        let merge_right = if index + 1 < blocks.len() {
            true
        } else {
            index > 0
        };
        if !merge_right && index == 0 {
            break;
        }

        if merge_right && index + 1 < blocks.len() {
            let right = blocks.remove(index + 1);
            removed_block_ids.insert(right.block_id.clone());
            rewritten_block_ids.insert(right.block_id.clone());
            touched_for_mutation.insert(right.block_id.clone());
            merge_postings_into_left(&mut blocks[index], right.postings)?;
            mark_block_dirty(
                &mut blocks[index],
                rewritten_block_ids,
                touched_for_mutation,
                true,
            );
            continue;
        }

        let current = blocks.remove(index);
        removed_block_ids.insert(current.block_id.clone());
        rewritten_block_ids.insert(current.block_id.clone());
        touched_for_mutation.insert(current.block_id.clone());
        index = index.saturating_sub(1);
        merge_postings_into_left(&mut blocks[index], current.postings)?;
        mark_block_dirty(
            &mut blocks[index],
            rewritten_block_ids,
            touched_for_mutation,
            true,
        );
    }
    Ok(())
}

fn merge_postings_into_left(
    left: &mut MutablePostingBlock,
    right_postings: Vec<Posting>,
) -> Result<(), ApiError> {
    let mut merged = Vec::with_capacity(left.postings.len().saturating_add(right_postings.len()));
    let mut left_index = 0usize;
    let mut right_index = 0usize;
    while left_index < left.postings.len() && right_index < right_postings.len() {
        let left_posting = left.postings[left_index];
        let right_posting = right_postings[right_index];
        if left_posting.doc_id < right_posting.doc_id {
            merged.push(left_posting);
            left_index += 1;
        } else if left_posting.doc_id > right_posting.doc_id {
            merged.push(right_posting);
            right_index += 1;
        } else {
            // Keep right-side value on conflict for deterministic "newest wins".
            merged.push(right_posting);
            left_index += 1;
            right_index += 1;
        }
    }
    if left_index < left.postings.len() {
        merged.extend_from_slice(&left.postings[left_index..]);
    }
    if right_index < right_postings.len() {
        merged.extend_from_slice(&right_postings[right_index..]);
    }
    left.postings = merged;
    validate_postings(&left.postings)?;
    Ok(())
}

fn mark_block_dirty(
    block: &mut MutablePostingBlock,
    rewritten_block_ids: &mut BTreeSet<String>,
    touched_for_mutation: &mut BTreeSet<String>,
    dirty: bool,
) {
    if dirty {
        block.dirty = true;
    }
    if block.dirty {
        rewritten_block_ids.insert(block.block_id.clone());
        touched_for_mutation.insert(block.block_id.clone());
    }
}

fn find_candidate_block_index(blocks: &[MutablePostingBlock], doc_id: u64) -> Option<usize> {
    for (index, block) in blocks.iter().enumerate() {
        let Some(first) = block.postings.first() else {
            continue;
        };
        let Some(last) = block.postings.last() else {
            continue;
        };
        if doc_id <= last.doc_id || doc_id < first.doc_id {
            return Some(index);
        }
    }
    if blocks.is_empty() {
        None
    } else {
        Some(blocks.len() - 1)
    }
}

fn validate_blocks(blocks: &[MutablePostingBlock]) -> Result<(), ApiError> {
    let mut previous_doc_id_max = None;
    for block in blocks {
        if block.postings.is_empty() {
            return Err(ApiError::internal("postings block contains zero postings"));
        }
        validate_postings(&block.postings)?;
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
        if first_doc_id > last_doc_id {
            return Err(ApiError::internal("postings block doc_id range is invalid"));
        }
        if let Some(previous) = previous_doc_id_max {
            if previous >= first_doc_id {
                return Err(ApiError::internal(
                    "postings blocks must be ordered and disjoint",
                ));
            }
        }
        previous_doc_id_max = Some(last_doc_id);
    }
    Ok(())
}

fn validate_postings(postings: &[Posting]) -> Result<(), ApiError> {
    for window in postings.windows(2) {
        if window[0].doc_id >= window[1].doc_id {
            return Err(ApiError::internal(
                "postings within a block must be strictly sorted by doc_id",
            ));
        }
    }
    Ok(())
}

fn next_block_id(seed: &mut u64, used_block_ids: &mut BTreeSet<String>) -> String {
    loop {
        let candidate = format!(
            "b{:0width$x}",
            next_block_seed_value(seed),
            width = BLOCK_ID_HEX_WIDTH
        );
        if used_block_ids.insert(candidate.clone()) {
            return candidate;
        }
    }
}

fn next_block_seed_value(seed: &mut u64) -> u64 {
    let value = *seed;
    *seed = seed.saturating_add(1);
    value
}

fn next_block_seed_start(existing_block_ids: &BTreeSet<String>) -> u64 {
    existing_block_ids
        .iter()
        .filter_map(|block_id| parse_seed_from_block_id(block_id))
        .max()
        .map(|seed| seed.saturating_add(1))
        .unwrap_or(0)
}

fn parse_seed_from_block_id(block_id: &str) -> Option<u64> {
    if block_id.len() != BLOCK_ID_HEX_WIDTH.saturating_add(1) || !block_id.starts_with('b') {
        return None;
    }
    u64::from_str_radix(&block_id[1..], 16).ok()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        apply_block_maintenance, BlockPolicy, MutablePostingBlock, PostingMutation,
        BLOCK_ID_HEX_WIDTH,
    };
    use crate::fts::postings_codec::Posting;

    fn posting(doc_id: u64) -> Posting {
        Posting {
            doc_id,
            tf: 1,
            doc_len: 10,
            flags: 0,
        }
    }

    fn block(id: &str, docs: std::ops::Range<u64>) -> MutablePostingBlock {
        MutablePostingBlock {
            block_id: id.to_string(),
            source_generation: 1,
            source_pack_id: "p0".to_string(),
            source_pack_offset: 0,
            source_pack_len: 1024,
            source_checksum: "source-checksum".to_string(),
            source_byte_len: 1024,
            source_max_term_score: 1.0,
            source_max_tf: 1,
            source_min_doc_len: 10,
            postings: docs.map(posting).collect(),
            dirty: false,
        }
    }

    #[test]
    fn split_threshold_rebalances_when_block_exceeds_limit() {
        let initial = vec![block("b0", 0..512)];
        let outcome = apply_block_maintenance(
            initial,
            &[PostingMutation {
                doc_id: 1024,
                posting: Some(posting(1024)),
            }],
            BlockPolicy::default(),
        )
        .expect("maintenance should succeed");

        assert_eq!(outcome.blocks.len(), 2);
        for block in &outcome.blocks {
            assert!(
                block.postings.len() <= 512,
                "split must keep every block <= split threshold"
            );
            assert!(
                !block.postings.is_empty(),
                "split must not produce empty blocks"
            );
        }
    }

    #[test]
    fn merge_threshold_rebalances_when_block_drops_below_limit() {
        let initial = vec![block("b0", 0..130), block("b1", 130..260)];
        let mut removals = Vec::new();
        for doc_id in 0..5 {
            removals.push(PostingMutation {
                doc_id,
                posting: None,
            });
        }
        let outcome = apply_block_maintenance(initial, &removals, BlockPolicy::default())
            .expect("maintenance should succeed");

        assert_eq!(outcome.blocks.len(), 1);
        assert_eq!(outcome.blocks[0].postings.len(), 255);
        assert!(
            outcome.removed_block_ids.contains("b1"),
            "merge should remove adjacent block"
        );
    }

    #[test]
    fn split_and_merge_rebalance_is_deterministic() {
        let initial = vec![block("b0", 0..400), block("b1", 400..800)];
        let mutations = vec![
            PostingMutation {
                doc_id: 50,
                posting: None,
            },
            PostingMutation {
                doc_id: 801,
                posting: Some(posting(801)),
            },
            PostingMutation {
                doc_id: 802,
                posting: Some(posting(802)),
            },
            PostingMutation {
                doc_id: 803,
                posting: Some(posting(803)),
            },
        ];

        let left = apply_block_maintenance(initial.clone(), &mutations, BlockPolicy::default())
            .expect("maintenance should succeed");
        let right = apply_block_maintenance(initial, &mutations, BlockPolicy::default())
            .expect("maintenance should succeed");

        assert_eq!(left.blocks, right.blocks);
        assert_eq!(left.rewritten_block_ids, right.rewritten_block_ids);
        assert_eq!(left.removed_block_ids, right.removed_block_ids);
    }

    #[test]
    fn block_ids_remain_compact_under_many_tail_splits() {
        let mut inserts = Vec::new();
        for doc_id in 0..300 {
            inserts.push(PostingMutation {
                doc_id,
                posting: Some(posting(doc_id)),
            });
        }
        let outcome = apply_block_maintenance(
            Vec::new(),
            &inserts,
            BlockPolicy {
                target_postings: 4,
                split_threshold: 8,
                merge_threshold: 2,
                max_term_blocks_touched_per_doc_update: 64,
                enable_delta_rebalance: true,
            },
        )
        .expect("maintenance should succeed");
        let max_block_id_len = outcome
            .blocks
            .iter()
            .map(|block| block.block_id.len())
            .max()
            .unwrap_or(0);
        assert_eq!(
            max_block_id_len,
            BLOCK_ID_HEX_WIDTH + 1,
            "block IDs should stay fixed-width to avoid key-length blowups"
        );
        let unique_ids = outcome
            .blocks
            .iter()
            .map(|block| block.block_id.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            unique_ids.len(),
            outcome.blocks.len(),
            "block IDs must remain unique after heavy split rebalance"
        );
    }
}
