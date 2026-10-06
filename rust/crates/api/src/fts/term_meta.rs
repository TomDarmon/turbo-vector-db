use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ApiError;

use super::tokenize::TokenizerKind;

pub(crate) const FTS_INDEX_META_VERSION: u32 = 4;
pub(crate) const FTS_TERM_META_VERSION: u32 = 3;
pub(crate) const FTS_DOC_LOOKUP_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct FtsIndexMeta {
    pub(crate) version: u32,
    pub(crate) generation: u64,
    pub(crate) collection: String,
    pub(crate) namespace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) previous_generation: Option<u64>,
    pub(crate) published_at: String,
    pub(crate) fields: BTreeMap<String, FtsFieldMeta>,
    pub(crate) doc_lookup: DocLookupRef,
    #[serde(default)]
    pub(crate) term_meta_checksums: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct FtsFieldMeta {
    pub(crate) field_hash: String,
    pub(crate) field_name: String,
    #[serde(default = "default_field_tokenizer")]
    pub(crate) tokenizer: TokenizerKind,
    pub(crate) term_count: u64,
    pub(crate) corpus_stats: FtsFieldCorpusStats,
    pub(crate) lexicon: LexiconRef,
    pub(crate) terms: BTreeMap<String, TermMetaRef>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) term_literals: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) prefix_terms: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct FtsFieldCorpusStats {
    pub(crate) document_count: u64,
    pub(crate) sum_doc_len: u64,
    pub(crate) avg_doc_len: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct LexiconRef {
    pub(crate) generation: u64,
    pub(crate) checksum: String,
    pub(crate) byte_len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TermMetaRef {
    pub(crate) generation: u64,
    pub(crate) checksum: String,
    pub(crate) byte_len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct DocLookupRef {
    pub(crate) generation: u64,
    pub(crate) checksum: String,
    pub(crate) byte_len: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct FtsDocLookup {
    pub(crate) version: u32,
    pub(crate) generation: u64,
    pub(crate) doc_count: u64,
    pub(crate) docs: Vec<FtsDocEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct FtsDocEntry {
    pub(crate) doc_id: u64,
    pub(crate) vector_id: String,
    pub(crate) doc_len: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) metadata: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct TermMeta {
    pub(crate) version: u32,
    pub(crate) generation: u64,
    pub(crate) field_hash: String,
    pub(crate) term_hash: String,
    pub(crate) document_frequency: u64,
    pub(crate) total_term_frequency: u64,
    pub(crate) block_count: u32,
    pub(crate) blocks: Vec<PostingsBlockDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) struct PostingsBlockDescriptor {
    pub(crate) generation: u64,
    pub(crate) block_id: String,
    pub(crate) pack_id: String,
    pub(crate) pack_offset: u32,
    pub(crate) pack_len: u32,
    pub(crate) doc_id_min: u64,
    pub(crate) doc_id_max: u64,
    pub(crate) posting_count: u32,
    pub(crate) max_term_score: f32,
    pub(crate) max_tf: u16,
    pub(crate) min_doc_len: u16,
    pub(crate) checksum: String,
    pub(crate) byte_len: u32,
}

impl FtsIndexMeta {
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        if self.version != FTS_INDEX_META_VERSION {
            return Err(ApiError::internal(format!(
                "unsupported FTS index metadata version {}",
                self.version
            )));
        }
        if self.collection.trim().is_empty() || self.namespace.trim().is_empty() {
            return Err(ApiError::internal(
                "FTS index metadata collection/namespace cannot be empty",
            ));
        }
        if self.doc_lookup.generation == 0
            || self.doc_lookup.checksum.is_empty()
            || self.doc_lookup.byte_len == 0
        {
            return Err(ApiError::internal(
                "FTS index metadata is missing doc_lookup reference",
            ));
        }
        for (field_hash, field_meta) in &self.fields {
            if field_hash != &field_meta.field_hash {
                return Err(ApiError::internal("FTS field metadata hash key mismatch"));
            }
            if field_meta.field_name.trim().is_empty() {
                return Err(ApiError::internal(
                    "FTS field metadata field_name cannot be empty",
                ));
            }
            if field_meta.term_count != field_meta.terms.len() as u64 {
                return Err(ApiError::internal(
                    "FTS field metadata term_count does not match terms length",
                ));
            }
            if field_meta.term_literals.len() != field_meta.terms.len() {
                return Err(ApiError::internal(
                    "FTS field metadata term_literals length must match terms length",
                ));
            }
            for (term_hash, term_ref) in &field_meta.terms {
                if term_ref.generation == 0
                    || term_ref.byte_len == 0
                    || term_ref.checksum.is_empty()
                {
                    return Err(ApiError::internal(
                        "FTS field metadata contains invalid term reference",
                    ));
                }
                let Some(literal) = field_meta.term_literals.get(term_hash) else {
                    return Err(ApiError::internal(
                        "FTS field metadata term_literals missing term hash key",
                    ));
                };
                if literal.trim().is_empty() {
                    return Err(ApiError::internal(
                        "FTS field metadata term literal must not be empty",
                    ));
                }
            }
            for (prefix, term_hashes) in &field_meta.prefix_terms {
                if prefix.trim().is_empty() {
                    return Err(ApiError::internal(
                        "FTS field metadata prefix key must not be empty",
                    ));
                }
                if term_hashes.is_empty() {
                    return Err(ApiError::internal(
                        "FTS field metadata prefix must map to at least one term hash",
                    ));
                }
                let mut previous = None::<&String>;
                for term_hash in term_hashes {
                    if !field_meta.terms.contains_key(term_hash) {
                        return Err(ApiError::internal(
                            "FTS field metadata prefix map references unknown term hash",
                        ));
                    }
                    if let Some(prev) = previous {
                        if prev >= term_hash {
                            return Err(ApiError::internal(
                                "FTS field metadata prefix term hashes must be sorted/unique",
                            ));
                        }
                    }
                    previous = Some(term_hash);
                }
            }
            field_meta.corpus_stats.validate()?;
        }
        Ok(())
    }
}

impl FtsFieldCorpusStats {
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        if self.document_count == 0 {
            return Err(ApiError::internal(
                "FTS corpus stats document_count must be > 0",
            ));
        }
        if self.sum_doc_len == 0 {
            return Err(ApiError::internal(
                "FTS corpus stats sum_doc_len must be > 0",
            ));
        }
        if !self.avg_doc_len.is_finite() || self.avg_doc_len <= 0.0 {
            return Err(ApiError::internal(
                "FTS corpus stats avg_doc_len must be finite and > 0",
            ));
        }
        let expected_avg = self.sum_doc_len as f32 / self.document_count as f32;
        if (expected_avg - self.avg_doc_len).abs() > 1e-3 {
            return Err(ApiError::internal(
                "FTS corpus stats avg_doc_len does not match sum/doc_count",
            ));
        }
        Ok(())
    }
}

impl FtsDocLookup {
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        if self.version != FTS_DOC_LOOKUP_VERSION {
            return Err(ApiError::internal(format!(
                "unsupported FTS doc lookup version {}",
                self.version
            )));
        }
        if self.doc_count != self.docs.len() as u64 {
            return Err(ApiError::internal(
                "FTS doc lookup doc_count does not match docs length",
            ));
        }
        let mut previous_doc_id = None;
        for doc in &self.docs {
            if doc.vector_id.trim().is_empty() {
                return Err(ApiError::internal(
                    "FTS doc lookup contains empty vector_id",
                ));
            }
            if let Some(previous) = previous_doc_id {
                if previous >= doc.doc_id {
                    return Err(ApiError::internal(
                        "FTS doc lookup doc_ids must be strictly increasing",
                    ));
                }
            }
            previous_doc_id = Some(doc.doc_id);
        }
        Ok(())
    }
}

impl TermMeta {
    pub(crate) fn validate(&self) -> Result<(), ApiError> {
        if self.version != FTS_TERM_META_VERSION {
            return Err(ApiError::internal(format!(
                "unsupported term meta version {}",
                self.version
            )));
        }
        if self.blocks.len() != self.block_count as usize {
            return Err(ApiError::internal(
                "term meta block_count does not match blocks length",
            ));
        }
        let mut previous_max = None;
        for block in &self.blocks {
            if block.posting_count == 0 {
                return Err(ApiError::internal(
                    "term meta contains block with zero postings",
                ));
            }
            if block.doc_id_min > block.doc_id_max {
                return Err(ApiError::internal("term meta block range is invalid"));
            }
            if block.pack_id.trim().is_empty() {
                return Err(ApiError::internal(
                    "term meta block pack_id cannot be empty",
                ));
            }
            if block.pack_len == 0 {
                return Err(ApiError::internal("term meta block pack_len must be > 0"));
            }
            if block.max_tf == 0 {
                return Err(ApiError::internal("term meta block max_tf must be > 0"));
            }
            if block.min_doc_len == 0 {
                return Err(ApiError::internal(
                    "term meta block min_doc_len must be > 0",
                ));
            }
            if !block.max_term_score.is_finite() {
                return Err(ApiError::internal(
                    "term meta block max_term_score must be finite",
                ));
            }
            if let Some(previous_doc_id_max) = previous_max {
                if previous_doc_id_max >= block.doc_id_min {
                    return Err(ApiError::internal(
                        "term meta blocks must be strictly ordered and disjoint",
                    ));
                }
            }
            previous_max = Some(block.doc_id_max);
        }
        Ok(())
    }
}

fn default_field_tokenizer() -> TokenizerKind {
    TokenizerKind::WordV1
}

#[cfg(test)]
mod tests {
    use super::{
        FtsDocEntry, FtsDocLookup, FtsFieldCorpusStats, PostingsBlockDescriptor, TermMeta,
        TokenizerKind, FTS_DOC_LOOKUP_VERSION, FTS_TERM_META_VERSION,
    };
    use crate::keys::sha256_hex;

    fn checksum_json<T: serde::Serialize>(value: &T) -> String {
        let bytes = serde_json::to_vec(value).expect("serialize checksum payload");
        sha256_hex(&bytes)
    }

    fn select_candidate_blocks(
        term_meta: &TermMeta,
        doc_id_min: u64,
        doc_id_max: u64,
        prefetch_adjacent: bool,
    ) -> Vec<PostingsBlockDescriptor> {
        let mut selected_indexes = Vec::new();
        for (index, block) in term_meta.blocks.iter().enumerate() {
            if block.doc_id_max < doc_id_min || block.doc_id_min > doc_id_max {
                continue;
            }
            selected_indexes.push(index);
        }
        if selected_indexes.is_empty() {
            return Vec::new();
        }
        if prefetch_adjacent {
            let first = selected_indexes[0];
            let last = *selected_indexes.last().unwrap_or(&first);
            if first > 0 {
                selected_indexes.insert(0, first - 1);
            }
            if last + 1 < term_meta.blocks.len() {
                selected_indexes.push(last + 1);
            }
        }
        selected_indexes.sort_unstable();
        selected_indexes.dedup();
        selected_indexes
            .into_iter()
            .map(|index| term_meta.blocks[index].clone())
            .collect()
    }

    fn fixture_term_meta() -> TermMeta {
        TermMeta {
            version: FTS_TERM_META_VERSION,
            generation: 3,
            field_hash: "f".to_string(),
            term_hash: "t".to_string(),
            document_frequency: 600,
            total_term_frequency: 900,
            block_count: 3,
            blocks: vec![
                PostingsBlockDescriptor {
                    generation: 2,
                    block_id: "b0".to_string(),
                    pack_id: "p0".to_string(),
                    pack_offset: 0,
                    pack_len: 512,
                    doc_id_min: 10,
                    doc_id_max: 199,
                    posting_count: 190,
                    max_term_score: 0.7,
                    max_tf: 3,
                    min_doc_len: 5,
                    checksum: "c0".to_string(),
                    byte_len: 2048,
                },
                PostingsBlockDescriptor {
                    generation: 3,
                    block_id: "b1".to_string(),
                    pack_id: "p0".to_string(),
                    pack_offset: 512,
                    pack_len: 520,
                    doc_id_min: 200,
                    doc_id_max: 399,
                    posting_count: 200,
                    max_term_score: 0.6,
                    max_tf: 2,
                    min_doc_len: 7,
                    checksum: "c1".to_string(),
                    byte_len: 2120,
                },
                PostingsBlockDescriptor {
                    generation: 3,
                    block_id: "b2".to_string(),
                    pack_id: "p1".to_string(),
                    pack_offset: 0,
                    pack_len: 500,
                    doc_id_min: 400,
                    doc_id_max: 599,
                    posting_count: 200,
                    max_term_score: 0.5,
                    max_tf: 2,
                    min_doc_len: 9,
                    checksum: "c2".to_string(),
                    byte_len: 2104,
                },
            ],
        }
    }

    #[test]
    fn term_meta_validation_accepts_sorted_disjoint_blocks() {
        let term_meta = fixture_term_meta();
        assert!(term_meta.validate().is_ok());
    }

    #[test]
    fn term_meta_checksum_is_deterministic() {
        let term_meta = fixture_term_meta();
        let left = checksum_json(&term_meta);
        let right = checksum_json(&term_meta);
        assert_eq!(left, right);
    }

    #[test]
    fn block_selection_uses_ranges_and_optional_adjacent_prefetch() {
        let term_meta = fixture_term_meta();
        let exact = select_candidate_blocks(&term_meta, 210, 230, false);
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].block_id, "b1");

        let prefetched = select_candidate_blocks(&term_meta, 210, 230, true);
        let ids = prefetched
            .iter()
            .map(|block| block.block_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, vec!["b0", "b1", "b2"]);
    }

    #[test]
    fn field_corpus_stats_validation_rejects_missing_inputs() {
        let stats = FtsFieldCorpusStats {
            document_count: 0,
            sum_doc_len: 0,
            avg_doc_len: 0.0,
        };
        assert!(stats.validate().is_err());
    }

    #[test]
    fn field_corpus_stats_validation_rejects_inconsistent_average() {
        let stats = FtsFieldCorpusStats {
            document_count: 10,
            sum_doc_len: 100,
            avg_doc_len: 42.0,
        };
        assert!(stats.validate().is_err());
    }

    #[test]
    fn doc_lookup_validation_accepts_sorted_docs() {
        let lookup = FtsDocLookup {
            version: FTS_DOC_LOOKUP_VERSION,
            generation: 8,
            doc_count: 2,
            docs: vec![
                FtsDocEntry {
                    doc_id: 10,
                    vector_id: "doc-a".to_string(),
                    doc_len: 3,
                    metadata: None,
                },
                FtsDocEntry {
                    doc_id: 12,
                    vector_id: "doc-b".to_string(),
                    doc_len: 4,
                    metadata: None,
                },
            ],
        };
        assert!(lookup.validate().is_ok());
    }

    #[test]
    fn field_meta_requires_term_literals_and_sorted_prefix_map() {
        use std::collections::BTreeMap;

        let mut terms = BTreeMap::new();
        terms.insert(
            "thash-a".to_string(),
            super::TermMetaRef {
                generation: 2,
                checksum: "c1".to_string(),
                byte_len: 12,
            },
        );
        let mut term_literals = BTreeMap::new();
        term_literals.insert("thash-a".to_string(), "alpha".to_string());
        let mut prefix_terms = BTreeMap::new();
        prefix_terms.insert("a".to_string(), vec!["thash-a".to_string()]);
        let meta = super::FtsIndexMeta {
            version: super::FTS_INDEX_META_VERSION,
            generation: 2,
            collection: "docs".to_string(),
            namespace: "ns_a".to_string(),
            previous_generation: Some(1),
            published_at: "2026-03-01T00:00:00Z".to_string(),
            fields: BTreeMap::from([(
                "fhash".to_string(),
                super::FtsFieldMeta {
                    field_hash: "fhash".to_string(),
                    field_name: "body".to_string(),
                    tokenizer: TokenizerKind::WordV1,
                    term_count: 1,
                    corpus_stats: FtsFieldCorpusStats {
                        document_count: 3,
                        sum_doc_len: 9,
                        avg_doc_len: 3.0,
                    },
                    lexicon: super::LexiconRef {
                        generation: 2,
                        checksum: "lx".to_string(),
                        byte_len: 24,
                    },
                    terms,
                    term_literals,
                    prefix_terms,
                },
            )]),
            doc_lookup: super::DocLookupRef {
                generation: 2,
                checksum: "docs".to_string(),
                byte_len: 42,
            },
            term_meta_checksums: BTreeMap::new(),
        };
        assert!(meta.validate().is_ok());
    }
}
