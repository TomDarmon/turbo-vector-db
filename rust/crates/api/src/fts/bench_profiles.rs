#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::fts::{
        block_maintenance::{
            apply_block_maintenance, BlockPolicy, MutablePostingBlock, PostingMutation,
        },
        postings_codec::{encode_postings_block, estimate_max_term_score, Posting},
    };

    fn posting(doc_id: u64, tf: u16) -> Posting {
        Posting {
            doc_id,
            tf,
            doc_len: 16,
            flags: 0,
        }
    }

    fn build_initial_blocks(policy: BlockPolicy, posting_count: usize) -> Vec<MutablePostingBlock> {
        let mut blocks = Vec::new();
        let mut doc_id = 1_u64;
        let chunk_size = policy.target_postings.max(1);
        let mut block_id = 0_u64;
        while (doc_id as usize) <= posting_count {
            let mut postings = Vec::new();
            for _ in 0..chunk_size {
                if (doc_id as usize) > posting_count {
                    break;
                }
                postings.push(posting(doc_id, 1));
                doc_id = doc_id.saturating_add(1);
            }
            let max_term_score = estimate_max_term_score(&postings);
            let encoded = encode_postings_block(&postings, max_term_score)
                .expect("initial block should encode");
            blocks.push(MutablePostingBlock {
                block_id: format!("b{block_id:04x}"),
                source_generation: 1,
                source_pack_id: "p0000".to_string(),
                source_pack_offset: 0,
                source_pack_len: encoded.len() as u32,
                source_checksum: crate::keys::sha256_hex(&encoded),
                source_byte_len: encoded.len() as u32,
                source_max_term_score: max_term_score,
                source_max_tf: 1,
                source_min_doc_len: 16,
                postings,
                dirty: false,
            });
            block_id = block_id.saturating_add(1);
        }
        blocks
    }

    fn freeze_blocks_after_rewrite(blocks: &mut [MutablePostingBlock]) -> usize {
        let mut bytes_rewritten = 0usize;
        for block in blocks {
            if !block.dirty {
                continue;
            }
            let max_term_score = estimate_max_term_score(&block.postings);
            let encoded = encode_postings_block(&block.postings, max_term_score)
                .expect("rewritten block should encode");
            bytes_rewritten = bytes_rewritten.saturating_add(encoded.len());
            block.source_generation = block.source_generation.saturating_add(1);
            block.source_pack_id = "p0001".to_string();
            block.source_pack_offset = 0;
            block.source_pack_len = encoded.len() as u32;
            block.source_checksum = crate::keys::sha256_hex(&encoded);
            block.source_byte_len = encoded.len() as u32;
            block.source_max_term_score = max_term_score;
            block.source_max_tf = block
                .postings
                .iter()
                .map(|posting| posting.tf)
                .max()
                .unwrap_or(1);
            block.source_min_doc_len = block
                .postings
                .iter()
                .map(|posting| posting.doc_len.max(1))
                .min()
                .unwrap_or(1);
            block.dirty = false;
        }
        bytes_rewritten
    }

    fn total_encoded_bytes(blocks: &[MutablePostingBlock]) -> usize {
        blocks
            .iter()
            .map(|block| {
                encode_postings_block(&block.postings, estimate_max_term_score(&block.postings))
                    .expect("block should encode")
                    .len()
            })
            .sum()
    }

    fn legacy_fixed_width_bytes(block: &MutablePostingBlock) -> usize {
        // Legacy codec layout (version 2): 36-byte header + fixed 14 bytes/posting.
        36usize.saturating_add(block.postings.len().saturating_mul(14))
    }

    fn total_legacy_fixed_width_bytes(blocks: &[MutablePostingBlock]) -> usize {
        blocks.iter().map(legacy_fixed_width_bytes).sum()
    }

    #[test]
    fn postings_bench_profile_bytes_per_posting_vs_json_baseline() {
        let policy = BlockPolicy::default();
        let blocks = build_initial_blocks(policy, 20_000);
        let encoded_bytes = total_encoded_bytes(&blocks);
        let legacy_fixed_bytes = total_legacy_fixed_width_bytes(&blocks);
        let postings: Vec<_> = (1_u64..=20_000_u64)
            .map(|doc_id| {
                json!({
                    "doc_id": format!("doc-{doc_id:08}"),
                    "tf": 1,
                    "doc_len_norm": 16,
                    "payload_flags": 0
                })
            })
            .collect();
        let naive_json_bytes = serde_json::to_vec(&postings)
            .expect("naive baseline should serialize")
            .len();

        let bytes_per_posting = encoded_bytes as f64 / 20_000_f64;
        let compression_ratio = encoded_bytes as f64 / naive_json_bytes as f64;
        let legacy_ratio = encoded_bytes as f64 / legacy_fixed_bytes as f64;
        println!(
            "postings_profile=zipfish_small encoded_bytes={} legacy_fixed_bytes={} naive_json_bytes={} bytes_per_posting={:.3} compression_ratio={:.4} legacy_ratio={:.4}",
            encoded_bytes, legacy_fixed_bytes, naive_json_bytes, bytes_per_posting, compression_ratio, legacy_ratio
        );

        assert!(
            compression_ratio <= 0.30,
            "encoded postings must stay within 30% of naive JSON size (ratio={compression_ratio:.4})"
        );
        assert!(
            legacy_ratio <= 0.70,
            "encoded postings must improve >=30% vs legacy fixed-width codec (ratio={legacy_ratio:.4})"
        );
    }

    #[test]
    fn postings_bench_profile_bytes_rewritten_per_hot_update() {
        let policy = BlockPolicy::default();
        let mut blocks = build_initial_blocks(policy, 4_096);
        let mut rng_state = 0xC0FFEE_u64;
        let operation_count = 256usize;
        let mut rewritten_bytes_total = 0usize;
        let full_rewrite_bytes_total = total_encoded_bytes(&blocks).saturating_mul(operation_count);

        for op in 0..operation_count {
            rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let doc_id = (rng_state % 4_096).saturating_add(1);
            let tf = ((op % 7) + 1) as u16;
            let outcome = apply_block_maintenance(
                blocks,
                &[PostingMutation {
                    doc_id,
                    posting: Some(posting(doc_id, tf)),
                }],
                policy,
            )
            .expect("maintenance should succeed");
            blocks = outcome.blocks;
            rewritten_bytes_total =
                rewritten_bytes_total.saturating_add(freeze_blocks_after_rewrite(&mut blocks));
        }

        let rewritten_per_operation = rewritten_bytes_total as f64 / operation_count as f64;
        let full_rewrite_per_operation = full_rewrite_bytes_total as f64 / operation_count as f64;
        let rewrite_ratio = rewritten_per_operation / full_rewrite_per_operation.max(1.0);
        println!(
            "postings_profile=hot_term_updates rewritten_bytes_total={} rewritten_per_op={:.2} full_rewrite_per_op={:.2} rewrite_ratio={:.4}",
            rewritten_bytes_total, rewritten_per_operation, full_rewrite_per_operation, rewrite_ratio
        );
        assert!(
            rewrite_ratio < 0.20,
            "hot-term rewrite ratio should stay bounded (<20% of full rewrite, got {rewrite_ratio:.4})"
        );
    }
}
