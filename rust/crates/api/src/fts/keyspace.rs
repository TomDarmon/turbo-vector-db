use crate::keys::sha256_hex;

pub(crate) fn fts_index_meta_key(collection: &str, namespace: &str, generation: u64) -> String {
    format!("collections/{collection}/indexes/{namespace}/{generation}/fts/meta.json")
}

pub(crate) fn fts_doc_lookup_key(collection: &str, namespace: &str, generation: u64) -> String {
    format!("collections/{collection}/indexes/{namespace}/{generation}/fts/docs.json")
}

pub(crate) fn fts_field_lexicon_key(
    collection: &str,
    namespace: &str,
    generation: u64,
    field_hash: &str,
) -> String {
    format!("collections/{collection}/indexes/{namespace}/{generation}/fts/fields/{field_hash}/lexicon.bin")
}

pub(crate) fn fts_term_meta_key(
    collection: &str,
    namespace: &str,
    generation: u64,
    field_hash: &str,
    term_hash: &str,
) -> String {
    format!(
        "collections/{collection}/indexes/{namespace}/{generation}/fts/fields/{field_hash}/terms/{term_hash}/meta.json"
    )
}

pub(crate) fn fts_block_pack_key(
    collection: &str,
    namespace: &str,
    generation: u64,
    field_hash: &str,
    term_hash: &str,
    pack_id: &str,
) -> String {
    format!(
        "collections/{collection}/indexes/{namespace}/{generation}/fts/fields/{field_hash}/terms/{term_hash}/packs/{pack_id}.bin"
    )
}

pub(crate) fn field_hash(field: &str) -> String {
    sha256_hex(field.as_bytes())
}

pub(crate) fn term_hash(term: &str) -> String {
    // Canonicalize for deterministic term identity.
    sha256_hex(term.trim().to_ascii_lowercase().as_bytes())
}

pub(crate) fn stable_doc_id(vector_id: &str) -> u64 {
    let hex = sha256_hex(vector_id.as_bytes());
    u64::from_str_radix(&hex[..16], 16).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{
        field_hash, fts_block_pack_key, fts_doc_lookup_key, fts_field_lexicon_key,
        fts_index_meta_key, fts_term_meta_key, stable_doc_id, term_hash,
    };

    #[test]
    fn keyspace_paths_match_expected_layout() {
        assert_eq!(
            fts_index_meta_key("docs", "default", 7),
            "collections/docs/indexes/default/7/fts/meta.json"
        );
        assert_eq!(
            fts_field_lexicon_key("docs", "default", 7, "fhash"),
            "collections/docs/indexes/default/7/fts/fields/fhash/lexicon.bin"
        );
        assert_eq!(
            fts_doc_lookup_key("docs", "default", 7),
            "collections/docs/indexes/default/7/fts/docs.json"
        );
        assert_eq!(
            fts_term_meta_key("docs", "default", 7, "fhash", "thash"),
            "collections/docs/indexes/default/7/fts/fields/fhash/terms/thash/meta.json"
        );
        assert_eq!(
            fts_block_pack_key("docs", "default", 7, "fhash", "thash", "p0"),
            "collections/docs/indexes/default/7/fts/fields/fhash/terms/thash/packs/p0.bin"
        );
    }

    #[test]
    fn hashes_and_doc_ids_are_stable() {
        assert_eq!(field_hash("title"), field_hash("title"));
        assert_eq!(term_hash("Hello"), term_hash("hello"));
        assert_eq!(stable_doc_id("doc-1"), stable_doc_id("doc-1"));
        assert_ne!(stable_doc_id("doc-1"), stable_doc_id("doc-2"));
    }
}
