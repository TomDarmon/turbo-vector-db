#[cfg(test)]
use std::collections::BTreeMap;
use std::{cmp::Ordering, collections::BinaryHeap};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScoredPosting {
    pub(crate) doc_id: u64,
    pub(crate) score: f32,
}

#[derive(Debug, Clone)]
pub(crate) struct ScoredBlock {
    pub(crate) doc_id_min: u64,
    pub(crate) doc_id_max: u64,
    pub(crate) max_score: f32,
    pub(crate) postings: Vec<ScoredPosting>,
}

#[derive(Debug, Clone)]
pub(crate) struct WeightedTermPostings {
    pub(crate) blocks: Vec<ScoredBlock>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LexicalExecutionStats {
    pub(crate) header_reads: u64,
    pub(crate) blocks_decoded: u64,
    pub(crate) blocks_scanned: u64,
    pub(crate) blocks_skipped: u64,
    pub(crate) docs_scored: u64,
    pub(crate) threshold_updates: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct TopKResult {
    pub(crate) docs: Vec<(u64, f32)>,
    pub(crate) stats: LexicalExecutionStats,
}

#[derive(Debug, Clone, Copy)]
struct HeapEntry {
    doc_id: u64,
    score: f32,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id == other.doc_id && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering makes BinaryHeap::peek() the worst-ranked entry.
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.doc_id.cmp(&other.doc_id))
    }
}

struct TermCursor {
    blocks: Vec<ScoredBlock>,
    suffix_max: Vec<f32>,
    block_index: usize,
    posting_index: usize,
    counted_current_block: bool,
}

impl TermCursor {
    fn new(mut term: WeightedTermPostings) -> Self {
        term.blocks.sort_by_key(|block| block.doc_id_min);
        let mut suffix_max = vec![0.0_f32; term.blocks.len()];
        let mut running = 0.0_f32;
        for index in (0..term.blocks.len()).rev() {
            running = running.max(term.blocks[index].max_score.max(0.0));
            suffix_max[index] = running;
        }
        Self {
            blocks: term.blocks,
            suffix_max,
            block_index: 0,
            posting_index: 0,
            counted_current_block: false,
        }
    }

    fn remaining_upper(&self) -> f32 {
        self.suffix_max
            .get(self.block_index)
            .copied()
            .unwrap_or(0.0)
    }

    fn remaining_block_count(&self) -> u64 {
        self.blocks.len().saturating_sub(self.block_index) as u64
    }

    fn advance_to(
        &mut self,
        target_doc_id: u64,
        threshold: f32,
        other_upper: f32,
        stats: &mut LexicalExecutionStats,
    ) {
        loop {
            self.advance_exhausted_blocks();
            let Some(block) = self.blocks.get(self.block_index) else {
                return;
            };

            if block.doc_id_max < target_doc_id {
                self.block_index = self.block_index.saturating_add(1);
                self.posting_index = 0;
                self.counted_current_block = false;
                continue;
            }

            if block.max_score + other_upper <= threshold {
                stats.blocks_skipped = stats.blocks_skipped.saturating_add(1);
                self.block_index = self.block_index.saturating_add(1);
                self.posting_index = 0;
                self.counted_current_block = false;
                continue;
            }

            if !self.counted_current_block {
                stats.blocks_scanned = stats.blocks_scanned.saturating_add(1);
                self.counted_current_block = true;
            }

            while self.posting_index < block.postings.len()
                && block.postings[self.posting_index].doc_id < target_doc_id
            {
                self.posting_index = self.posting_index.saturating_add(1);
            }
            self.advance_exhausted_blocks();
            return;
        }
    }

    fn peek_doc_id(&self) -> Option<u64> {
        let block = self.blocks.get(self.block_index)?;
        let posting = block.postings.get(self.posting_index)?;
        Some(posting.doc_id)
    }

    fn consume_if_matches(
        &mut self,
        doc_id: u64,
        threshold: f32,
        other_upper: f32,
        stats: &mut LexicalExecutionStats,
    ) -> f32 {
        self.advance_to(doc_id, threshold, other_upper, stats);
        let Some(block) = self.blocks.get(self.block_index) else {
            return 0.0;
        };
        let Some(posting) = block.postings.get(self.posting_index) else {
            return 0.0;
        };
        if posting.doc_id != doc_id {
            return 0.0;
        }
        let score = posting.score;
        self.posting_index = self.posting_index.saturating_add(1);
        self.advance_exhausted_blocks();
        score
    }

    fn advance_exhausted_blocks(&mut self) {
        loop {
            let Some(block) = self.blocks.get(self.block_index) else {
                return;
            };
            if self.posting_index < block.postings.len() {
                return;
            }
            self.block_index = self.block_index.saturating_add(1);
            self.posting_index = 0;
            self.counted_current_block = false;
        }
    }
}

#[cfg(test)]
pub(crate) fn exhaustive_top_k<F>(
    top_k: usize,
    terms: &[WeightedTermPostings],
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
    let mut scores: BTreeMap<u64, f32> = BTreeMap::new();
    for term in terms {
        for block in &term.blocks {
            stats.blocks_scanned = stats.blocks_scanned.saturating_add(1);
            for posting in &block.postings {
                let entry = scores.entry(posting.doc_id).or_insert(0.0);
                *entry += posting.score;
            }
        }
    }

    let mut heap = BinaryHeap::new();
    for (doc_id, score) in scores {
        if score <= 0.0 || !filter_doc(doc_id) {
            continue;
        }
        stats.docs_scored = stats.docs_scored.saturating_add(1);
        upsert_heap(&mut heap, top_k, doc_id, score, &mut stats);
    }
    TopKResult {
        docs: sorted_heap_docs(heap),
        stats,
    }
}

pub(crate) fn maxscore_top_k<F>(
    top_k: usize,
    mut terms: Vec<WeightedTermPostings>,
    mut filter_doc: F,
) -> TopKResult
where
    F: FnMut(u64) -> bool,
{
    if top_k == 0 || terms.is_empty() {
        return TopKResult {
            docs: Vec::new(),
            stats: LexicalExecutionStats::default(),
        };
    }

    terms.sort_by(|left, right| {
        let left_upper = left
            .blocks
            .iter()
            .fold(0.0_f32, |max_score, block| max_score.max(block.max_score));
        let right_upper = right
            .blocks
            .iter()
            .fold(0.0_f32, |max_score, block| max_score.max(block.max_score));
        right_upper.total_cmp(&left_upper)
    });

    let mut cursors = terms.into_iter().map(TermCursor::new).collect::<Vec<_>>();
    let mut stats = LexicalExecutionStats::default();
    let mut heap = BinaryHeap::new();
    let mut candidate_floor = 0_u64;

    loop {
        let threshold = current_threshold(&heap, top_k);
        let remaining_upper_total = cursors
            .iter()
            .fold(0.0_f32, |sum, cursor| sum + cursor.remaining_upper());
        if remaining_upper_total <= threshold {
            let skipped = cursors.iter().fold(0_u64, |sum, cursor| {
                sum.saturating_add(cursor.remaining_block_count())
            });
            stats.blocks_skipped = stats.blocks_skipped.saturating_add(skipped);
            break;
        }

        let essential_count = essential_prefix(&cursors, threshold);
        if essential_count == 0 {
            break;
        }

        let mut next_doc_id: Option<u64> = None;
        for index in 0..essential_count {
            let own_upper = cursors[index].remaining_upper();
            let other_upper = remaining_upper_total - own_upper;
            cursors[index].advance_to(candidate_floor, threshold, other_upper, &mut stats);
            if let Some(doc_id) = cursors[index].peek_doc_id() {
                next_doc_id = match next_doc_id {
                    Some(existing) => Some(existing.min(doc_id)),
                    None => Some(doc_id),
                };
            }
        }
        let Some(doc_id) = next_doc_id else {
            break;
        };

        if doc_id == u64::MAX {
            break;
        }
        candidate_floor = doc_id.saturating_add(1);

        let mut score = 0.0_f32;
        for index in 0..essential_count {
            let own_upper = cursors[index].remaining_upper();
            let other_upper = remaining_upper_total - own_upper;
            score += cursors[index].consume_if_matches(doc_id, threshold, other_upper, &mut stats);
        }

        let nonessential_upper = cursors[essential_count..]
            .iter()
            .fold(0.0_f32, |sum, cursor| sum + cursor.remaining_upper());
        if score + nonessential_upper <= threshold {
            continue;
        }

        if !filter_doc(doc_id) {
            continue;
        }

        for index in essential_count..cursors.len() {
            let own_upper = cursors[index].remaining_upper();
            let other_upper = remaining_upper_total - own_upper;
            score += cursors[index].consume_if_matches(doc_id, threshold, other_upper, &mut stats);
        }

        if score <= 0.0 {
            continue;
        }

        stats.docs_scored = stats.docs_scored.saturating_add(1);
        upsert_heap(&mut heap, top_k, doc_id, score, &mut stats);
    }

    TopKResult {
        docs: sorted_heap_docs(heap),
        stats,
    }
}

fn upsert_heap(
    heap: &mut BinaryHeap<HeapEntry>,
    top_k: usize,
    doc_id: u64,
    score: f32,
    stats: &mut LexicalExecutionStats,
) {
    let before_threshold = current_threshold(heap, top_k);
    let candidate = HeapEntry { doc_id, score };
    if heap.len() < top_k {
        heap.push(candidate);
    } else if let Some(worst) = heap.peek() {
        if candidate < *worst {
            let _ = heap.pop();
            heap.push(candidate);
        }
    }
    let after_threshold = current_threshold(heap, top_k);
    if after_threshold > before_threshold {
        stats.threshold_updates = stats.threshold_updates.saturating_add(1);
    }
}

fn current_threshold(heap: &BinaryHeap<HeapEntry>, top_k: usize) -> f32 {
    if heap.len() < top_k {
        0.0
    } else {
        heap.peek().map(|entry| entry.score).unwrap_or(0.0)
    }
}

fn essential_prefix(cursors: &[TermCursor], threshold: f32) -> usize {
    if cursors.is_empty() {
        return 0;
    }
    let mut tail_sum = cursors
        .iter()
        .fold(0.0_f32, |sum, cursor| sum + cursor.remaining_upper());
    if tail_sum <= threshold {
        return 0;
    }
    for (index, cursor) in cursors.iter().enumerate() {
        tail_sum -= cursor.remaining_upper();
        if tail_sum <= threshold {
            return index + 1;
        }
    }
    cursors.len()
}

fn sorted_heap_docs(heap: BinaryHeap<HeapEntry>) -> Vec<(u64, f32)> {
    let mut docs = heap
        .into_vec()
        .into_iter()
        .map(|entry| (entry.doc_id, entry.score))
        .collect::<Vec<_>>();
    docs.sort_by(|(left_doc_id, left_score), (right_doc_id, right_score)| {
        right_score
            .total_cmp(left_score)
            .then_with(|| left_doc_id.cmp(right_doc_id))
    });
    docs
}

#[cfg(test)]
mod tests {
    use super::{
        exhaustive_top_k, maxscore_top_k, ScoredBlock, ScoredPosting, WeightedTermPostings,
    };

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

    #[test]
    fn maxscore_matches_exhaustive_top_k() {
        let terms = vec![
            WeightedTermPostings {
                blocks: vec![
                    block(vec![
                        posting(1, 5.0),
                        posting(2, 4.0),
                        posting(3, 0.4),
                        posting(4, 0.3),
                    ]),
                    block(vec![posting(40, 0.2), posting(41, 0.1)]),
                ],
            },
            WeightedTermPostings {
                blocks: vec![
                    block(vec![posting(1, 2.0), posting(2, 0.5), posting(8, 0.4)]),
                    block(vec![posting(42, 0.1), posting(43, 0.1)]),
                ],
            },
            WeightedTermPostings {
                blocks: vec![
                    block(vec![posting(5, 3.0), posting(6, 2.5), posting(7, 2.0)]),
                    block(vec![posting(44, 0.1), posting(45, 0.1)]),
                ],
            },
        ];
        let exhaustive = exhaustive_top_k(3, &terms, |_| true);
        let maxscore = maxscore_top_k(3, terms, |_| true);
        assert_eq!(maxscore.docs, exhaustive.docs, "top-k results must match");
    }

    #[test]
    fn maxscore_emits_skip_telemetry_on_long_tail_query() {
        let mut terms = Vec::new();
        for term_id in 0..12 {
            let mut high_postings = Vec::new();
            for doc_id in 1..=8 {
                high_postings.push(posting(
                    doc_id + term_id as u64,
                    1.5 - term_id as f32 * 0.05,
                ));
            }
            let mut low_postings = Vec::new();
            for doc_id in 200..=260 {
                low_postings.push(posting(doc_id + term_id as u64, 0.01));
            }
            terms.push(WeightedTermPostings {
                blocks: vec![block(high_postings), block(low_postings)],
            });
        }
        let exhaustive = exhaustive_top_k(1, &terms, |_| true);
        let maxscore = maxscore_top_k(1, terms, |_| true);
        assert_eq!(maxscore.docs, exhaustive.docs, "maxscore must be rank-safe");
        assert!(
            maxscore.stats.blocks_skipped > 0,
            "expected skipped blocks telemetry to be > 0"
        );
        assert!(
            maxscore.stats.docs_scored <= exhaustive.stats.docs_scored,
            "maxscore should score fewer or equal docs than exhaustive"
        );
        assert!(
            maxscore.stats.threshold_updates > 0,
            "threshold updates should be tracked"
        );
    }
}
