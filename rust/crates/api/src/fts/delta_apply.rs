use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::{
    fts::{
        block_maintenance::PostingMutation,
        keyspace::{field_hash, stable_doc_id, term_hash},
        postings_codec::Posting,
        tokenize::TokenizerKind,
    },
    models::UpsertVector,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DocumentDelta {
    pub(crate) vector_id: String,
    pub(crate) doc_id: u64,
    pub(crate) previous_terms: BTreeMap<(String, String), u16>,
    pub(crate) next_terms: BTreeMap<(String, String), u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TermKey {
    pub(crate) field_name: String,
    pub(crate) field_hash: String,
    pub(crate) term: String,
    pub(crate) term_hash: String,
}

pub(crate) fn collect_document_deltas_with_tokenizers(
    previous_vectors: &BTreeMap<String, UpsertVector>,
    next_vectors: &BTreeMap<String, UpsertVector>,
    previous_tokenizers: &BTreeMap<String, Option<TokenizerKind>>,
    next_tokenizers: &BTreeMap<String, Option<TokenizerKind>>,
) -> Vec<DocumentDelta> {
    let mut ids = BTreeSet::new();
    ids.extend(previous_vectors.keys().cloned());
    ids.extend(next_vectors.keys().cloned());

    let mut deltas = Vec::new();
    for vector_id in ids {
        let previous_terms = terms_from_vector_with_tokenizers(
            previous_vectors.get(&vector_id),
            previous_tokenizers,
        );
        let next_terms =
            terms_from_vector_with_tokenizers(next_vectors.get(&vector_id), next_tokenizers);
        if previous_terms == next_terms {
            continue;
        }
        deltas.push(DocumentDelta {
            doc_id: stable_doc_id(&vector_id),
            vector_id,
            previous_terms,
            next_terms,
        });
    }
    deltas.sort_by(|left, right| {
        left.doc_id
            .cmp(&right.doc_id)
            .then_with(|| left.vector_id.cmp(&right.vector_id))
    });
    deltas
}

pub(crate) fn build_term_mutations(
    document_deltas: &[DocumentDelta],
) -> BTreeMap<TermKey, Vec<PostingMutation>> {
    let mut out: BTreeMap<TermKey, Vec<PostingMutation>> = BTreeMap::new();
    for delta in document_deltas {
        let mut touched_terms = BTreeSet::new();
        touched_terms.extend(delta.previous_terms.keys().cloned());
        touched_terms.extend(delta.next_terms.keys().cloned());

        for (field_name, term_value) in touched_terms {
            let previous_tf = delta
                .previous_terms
                .get(&(field_name.clone(), term_value.clone()))
                .copied()
                .unwrap_or(0);
            let next_tf = delta
                .next_terms
                .get(&(field_name.clone(), term_value.clone()))
                .copied()
                .unwrap_or(0);
            if previous_tf == 0 && next_tf == 0 {
                continue;
            }
            let previous_doc_len = document_len_for_field(&delta.previous_terms, &field_name);
            let next_doc_len = document_len_for_field(&delta.next_terms, &field_name);
            let key = TermKey {
                field_hash: field_hash(&field_name),
                term_hash: term_hash(&term_value),
                field_name,
                term: term_value,
            };
            let entry = out.entry(key).or_default();
            if next_tf == 0 {
                entry.push(PostingMutation {
                    doc_id: delta.doc_id,
                    posting: None,
                });
                continue;
            }
            if previous_tf == next_tf && previous_doc_len == next_doc_len {
                continue;
            }
            entry.push(PostingMutation {
                doc_id: delta.doc_id,
                posting: Some(Posting {
                    doc_id: delta.doc_id,
                    tf: next_tf,
                    doc_len: next_doc_len,
                    flags: 0,
                }),
            });
        }
    }
    for mutations in out.values_mut() {
        mutations.sort_by_key(|mutation| mutation.doc_id);
    }
    out
}

pub(crate) fn terms_from_vector_with_tokenizers(
    vector: Option<&UpsertVector>,
    field_tokenizers: &BTreeMap<String, Option<TokenizerKind>>,
) -> BTreeMap<(String, String), u16> {
    let mut out = BTreeMap::new();
    let Some(vector) = vector else {
        return out;
    };
    let Some(Value::Object(metadata)) = vector.metadata.as_ref() else {
        return out;
    };
    for (field_name, value) in metadata {
        let tokenizer = match field_tokenizers.get(field_name) {
            Some(Some(tokenizer)) => *tokenizer,
            Some(None) => continue,
            None => TokenizerKind::WordV1,
        };
        collect_terms_for_value(field_name, value, tokenizer, &mut out);
    }
    out
}

fn collect_terms_for_value(
    field_name: &str,
    value: &Value,
    tokenizer: TokenizerKind,
    out: &mut BTreeMap<(String, String), u16>,
) {
    match value {
        Value::String(text) => {
            for token in tokenizer.tokenize(text) {
                if token.is_empty() {
                    continue;
                }
                let key = (field_name.to_string(), token);
                let next = out.get(&key).copied().unwrap_or(0).saturating_add(1);
                out.insert(key, next);
            }
        }
        Value::Array(entries) => {
            for entry in entries {
                collect_terms_for_value(field_name, entry, tokenizer, out);
            }
        }
        _ => {}
    }
}

fn document_len_for_field(terms: &BTreeMap<(String, String), u16>, field_name: &str) -> u16 {
    terms
        .iter()
        .filter(|((field, _), _)| field == field_name)
        .fold(0_u16, |sum, (_, value)| sum.saturating_add(*value))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        build_term_mutations, collect_document_deltas_with_tokenizers,
        terms_from_vector_with_tokenizers, TermKey,
    };
    use crate::models::UpsertVector;

    fn vector(id: &str, metadata: serde_json::Value) -> UpsertVector {
        UpsertVector {
            id: id.to_string(),
            values: vec![0.0, 0.0],
            metadata: Some(metadata),
        }
    }

    #[test]
    fn term_extraction_counts_text_tokens_across_scalars_and_arrays() {
        let vector = vector(
            "doc-1",
            json!({
                "title": "Rust rust",
                "tags": ["DB", "rust"]
            }),
        );
        let terms = terms_from_vector_with_tokenizers(Some(&vector), &BTreeMap::new());
        assert_eq!(
            terms.get(&(String::from("title"), String::from("rust"))),
            Some(&2)
        );
        assert_eq!(
            terms.get(&(String::from("tags"), String::from("db"))),
            Some(&1)
        );
        assert_eq!(
            terms.get(&(String::from("tags"), String::from("rust"))),
            Some(&1)
        );
    }

    #[test]
    fn term_mutations_are_deterministic_under_equivalent_delta_order() {
        let mut previous = BTreeMap::new();
        previous.insert(
            "doc-a".to_string(),
            vector("doc-a", json!({"title": "one"})),
        );
        let mut next = BTreeMap::new();
        next.insert(
            "doc-a".to_string(),
            vector("doc-a", json!({"title": "one two"})),
        );
        next.insert(
            "doc-b".to_string(),
            vector("doc-b", json!({"title": "two"})),
        );
        let mut deltas = collect_document_deltas_with_tokenizers(
            &previous,
            &next,
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        let baseline = build_term_mutations(&deltas);
        deltas.reverse();
        let reversed = build_term_mutations(&deltas);
        assert_eq!(baseline, reversed);
    }

    #[test]
    fn term_mutations_emit_removals_and_upserts() {
        let mut previous = BTreeMap::new();
        previous.insert(
            "doc-a".to_string(),
            vector("doc-a", json!({"title": "old"})),
        );
        let mut next = BTreeMap::new();
        next.insert(
            "doc-a".to_string(),
            vector("doc-a", json!({"title": "new"})),
        );
        let deltas = collect_document_deltas_with_tokenizers(
            &previous,
            &next,
            &BTreeMap::new(),
            &BTreeMap::new(),
        );
        let mutations = build_term_mutations(&deltas);

        let old_key = mutations
            .keys()
            .find(|key| key.term == "old" && key.field_name == "title")
            .cloned()
            .expect("old term key");
        let new_key = mutations
            .keys()
            .find(|key| key.term == "new" && key.field_name == "title")
            .cloned()
            .expect("new term key");
        assert!(
            mutations[&old_key]
                .iter()
                .all(|mutation| mutation.posting.is_none()),
            "old term should be removed"
        );
        assert!(
            mutations[&new_key]
                .iter()
                .all(|mutation| mutation.posting.is_some()),
            "new term should be upserted"
        );
    }

    #[test]
    fn term_key_ordering_is_stable_for_btreemap_serialization() {
        let left = TermKey {
            field_name: "title".to_string(),
            field_hash: "a".to_string(),
            term: "rust".to_string(),
            term_hash: "b".to_string(),
        };
        let right = TermKey {
            field_name: "title".to_string(),
            field_hash: "a".to_string(),
            term: "z".to_string(),
            term_hash: "c".to_string(),
        };
        assert!(left < right);
    }
}
