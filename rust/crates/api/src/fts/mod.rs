pub(crate) mod block_maintenance;
pub(crate) mod block_pack;
pub(crate) mod bm25;
pub(crate) mod delta_apply;
pub(crate) mod keyspace;
pub(crate) mod maxscore;
pub(crate) mod planner;
pub(crate) mod postings_codec;
pub(crate) mod rank_expr;
pub(crate) mod runtime;
pub(crate) mod scoring_kernel;
pub(crate) mod term_meta;
pub(crate) mod tokenize;

#[cfg(test)]
mod bench_profiles;
#[cfg(test)]
pub(crate) mod lexical_benchmarks;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use tracing::debug;
use turbo_vector_core::TurboVectorError;
use turbo_vector_manifest::Manifest;

use crate::{
    error::{map_store_error, ApiError},
    keys::{manifest_generation_key, now_rfc3339, sha256_hex},
    models::UpsertVector,
    state::AppState,
    storage_logic::{load_collection_metadata, load_namespace_vectors},
};

use self::{
    block_maintenance::{apply_block_maintenance, BlockPolicy, MutablePostingBlock},
    block_pack::{decode_block_pack, encode_block_pack, DecodedBlockPack},
    delta_apply::{
        build_term_mutations, collect_document_deltas_with_tokenizers,
        terms_from_vector_with_tokenizers, TermKey,
    },
    keyspace::{
        fts_block_pack_key, fts_doc_lookup_key, fts_field_lexicon_key, fts_index_meta_key,
        fts_term_meta_key, stable_doc_id,
    },
    planner::resolve_field_search_config,
    postings_codec::{decode_postings_block, encode_postings_block, estimate_max_term_score},
    term_meta::{
        DocLookupRef, FtsDocEntry, FtsDocLookup, FtsFieldCorpusStats, FtsFieldMeta, FtsIndexMeta,
        LexiconRef, PostingsBlockDescriptor, TermMeta, TermMetaRef, FTS_DOC_LOOKUP_VERSION,
        FTS_INDEX_META_VERSION, FTS_TERM_META_VERSION,
    },
    tokenize::TokenizerKind,
};

const FTS_BLOCK_PACK_TARGET_BLOCKS: usize = 8;
const DEFAULT_PREFIX_INDEX_MAX_CHARS: usize = 8;

pub(crate) async fn ensure_fts_indexes_for_manifest(
    state: &AppState,
    collection: &str,
    manifest: &Manifest,
) -> Result<(), ApiError> {
    for namespace in manifest.namespace_partitions.keys() {
        let _ = load_or_build_fts_index_meta(state, collection, namespace, manifest).await?;
    }
    Ok(())
}

pub(crate) async fn load_fts_index_meta_for_generation(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
) -> Result<Option<FtsIndexMeta>, ApiError> {
    if let Some(cached) = state
        .get_cached_fts_index_meta(collection, namespace, generation)
        .await
    {
        crate::telemetry::increment_cache_hits(&state.service_name, "fts_index_meta", 1);
        return Ok(Some(cached));
    }
    crate::telemetry::increment_cache_misses(&state.service_name, "fts_index_meta", 1);
    let key = fts_index_meta_key(collection, namespace, generation);
    let raw = match state.storage.get_bytes(&key).await {
        Ok(raw) => raw,
        Err(TurboVectorError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(map_store_error(error)),
    };
    let meta: FtsIndexMeta = serde_json::from_slice(&raw).map_err(|error| {
        ApiError::internal(format!("failed to parse FTS index metadata: {error}"))
    })?;
    meta.validate()?;
    state
        .set_cached_fts_index_meta(collection, namespace, generation, meta.clone())
        .await;
    Ok(Some(meta))
}

pub(crate) async fn load_doc_lookup_for_index(
    state: &AppState,
    collection: &str,
    namespace: &str,
    index_meta: &FtsIndexMeta,
) -> Result<FtsDocLookup, ApiError> {
    if let Some(cached) = state
        .get_cached_fts_doc_lookup(collection, namespace, index_meta.doc_lookup.generation)
        .await
    {
        crate::telemetry::increment_cache_hits(&state.service_name, "fts_doc_lookup", 1);
        return Ok(cached);
    }
    crate::telemetry::increment_cache_misses(&state.service_name, "fts_doc_lookup", 1);
    let key = fts_doc_lookup_key(collection, namespace, index_meta.doc_lookup.generation);
    let raw = state
        .storage
        .get_bytes(&key)
        .await
        .map_err(map_store_error)?;
    if raw.len() != index_meta.doc_lookup.byte_len as usize {
        return Err(ApiError::store_unavailable(format!(
            "FTS doc lookup length mismatch for '{}'",
            key
        )));
    }
    let checksum = sha256_hex(&raw);
    if checksum != index_meta.doc_lookup.checksum {
        return Err(ApiError::store_unavailable(format!(
            "FTS doc lookup checksum mismatch for '{}'",
            key
        )));
    }
    let lookup: FtsDocLookup = serde_json::from_slice(&raw).map_err(|error| {
        ApiError::internal(format!("failed to parse FTS doc lookup metadata: {error}"))
    })?;
    lookup.validate()?;
    if lookup.generation != index_meta.doc_lookup.generation {
        return Err(ApiError::store_unavailable(format!(
            "FTS doc lookup generation mismatch for '{}'",
            key
        )));
    }
    state
        .set_cached_fts_doc_lookup(collection, namespace, lookup.generation, lookup.clone())
        .await;
    Ok(lookup)
}

async fn load_or_build_fts_index_meta(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
) -> Result<Option<FtsIndexMeta>, ApiError> {
    if let Some(meta) =
        load_fts_index_meta_for_generation(state, collection, namespace, manifest.generation)
            .await?
    {
        return Ok(Some(meta));
    }

    let _build_guard = state
        .lock_fts_index_build(collection, namespace, manifest.generation)
        .await;
    if let Some(meta) =
        load_fts_index_meta_for_generation(state, collection, namespace, manifest.generation)
            .await?
    {
        return Ok(Some(meta));
    }
    build_fts_index_for_namespace(state, collection, namespace, manifest).await
}

async fn build_fts_index_for_namespace(
    state: &AppState,
    collection: &str,
    namespace: &str,
    manifest: &Manifest,
) -> Result<Option<FtsIndexMeta>, ApiError> {
    let previous_index_meta = if let Some(previous_generation) = manifest.previous_generation {
        load_fts_index_meta_for_generation(state, collection, namespace, previous_generation)
            .await?
    } else {
        None
    };
    let previous_manifest = if let Some(previous_generation) = manifest.previous_generation {
        load_manifest_generation(state, collection, previous_generation).await?
    } else {
        None
    };

    let previous_vectors = match (previous_index_meta.as_ref(), previous_manifest.as_ref()) {
        // Missing previous FTS metadata means we must bootstrap from current visible vectors,
        // not from term deltas against prior manifest vectors.
        (Some(_), Some(previous_manifest)) => {
            load_namespace_vectors(state, collection, namespace, previous_manifest).await?
        }
        _ => Arc::new(BTreeMap::<String, UpsertVector>::new()),
    };
    // Keep cache hot for the current generation to avoid FTS->ANN reload churn.
    let next_vectors = load_namespace_vectors(state, collection, namespace, manifest).await?;
    let collection_meta = load_collection_metadata(state, collection).await?;
    let (previous_field_tokenizers, next_field_tokenizers) = resolve_field_tokenizers(
        collection_meta.metadata_schema.as_ref(),
        previous_index_meta.as_ref(),
        previous_vectors.as_ref(),
        next_vectors.as_ref(),
    )?;
    ensure_no_stable_doc_id_collisions(
        collection,
        namespace,
        manifest.generation,
        next_vectors.as_ref(),
    )?;
    let corpus_stats_by_field =
        compute_field_corpus_stats(next_vectors.as_ref(), &next_field_tokenizers);
    let doc_lookup_ref = materialize_doc_lookup(
        state,
        collection,
        namespace,
        manifest.generation,
        next_vectors.as_ref(),
        &next_field_tokenizers,
    )
    .await?;

    let document_deltas = collect_document_deltas_with_tokenizers(
        previous_vectors.as_ref(),
        next_vectors.as_ref(),
        &previous_field_tokenizers,
        &next_field_tokenizers,
    );
    let term_mutations = build_term_mutations(&document_deltas);
    let block_policy = block_policy_from_state(state);

    let mut fields = previous_index_meta
        .as_ref()
        .map(|meta| meta.fields.clone())
        .unwrap_or_default();
    let mut touched_field_hashes = BTreeSet::new();
    let mut terms_touched = 0_u64;
    let mut blocks_rewritten = 0_u64;
    let mut bytes_rewritten = 0_u64;

    for (term_key, mutations) in term_mutations {
        terms_touched = terms_touched.saturating_add(1);
        touched_field_hashes.insert(term_key.field_hash.clone());
        let field_entry = fields
            .entry(term_key.field_hash.clone())
            .or_insert_with(|| FtsFieldMeta {
                field_hash: term_key.field_hash.clone(),
                field_name: term_key.field_name.clone(),
                tokenizer: next_field_tokenizers
                    .get(&term_key.field_name)
                    .copied()
                    .flatten()
                    .unwrap_or(TokenizerKind::WordV1),
                term_count: 0,
                corpus_stats: FtsFieldCorpusStats {
                    document_count: 1,
                    sum_doc_len: 1,
                    avg_doc_len: 1.0,
                },
                lexicon: LexiconRef {
                    generation: manifest.generation,
                    checksum: String::new(),
                    byte_len: 0,
                },
                terms: BTreeMap::new(),
                term_literals: BTreeMap::new(),
                prefix_terms: BTreeMap::new(),
            });
        if field_entry.field_name.is_empty() {
            field_entry.field_name = term_key.field_name.clone();
        }
        if let Some(Some(tokenizer)) = next_field_tokenizers.get(&field_entry.field_name) {
            field_entry.tokenizer = *tokenizer;
        }

        let previous_term_ref = field_entry.terms.get(&term_key.term_hash).cloned();
        let previous_term_meta = if let Some(term_ref) = previous_term_ref {
            Some(load_term_meta_for_ref(state, collection, namespace, &term_key, &term_ref).await?)
        } else {
            None
        };
        let existing_blocks = match previous_term_meta.as_ref() {
            Some(term_meta) => {
                load_term_blocks(state, collection, namespace, &term_key, term_meta).await?
            }
            None => Vec::new(),
        };

        let outcome = apply_block_maintenance(existing_blocks, &mutations, block_policy)?;
        if outcome.blocks.is_empty() {
            field_entry.terms.remove(&term_key.term_hash);
            field_entry.term_literals.remove(&term_key.term_hash);
            continue;
        }

        let (descriptors, rewritten_count, rewritten_bytes, total_term_frequency) =
            materialize_term_blocks(
                state,
                collection,
                namespace,
                manifest.generation,
                &term_key,
                outcome.blocks,
            )
            .await?;
        blocks_rewritten = blocks_rewritten.saturating_add(rewritten_count as u64);
        bytes_rewritten = bytes_rewritten.saturating_add(rewritten_bytes as u64);

        let document_frequency = descriptors.iter().fold(0_u64, |sum, descriptor| {
            sum.saturating_add(descriptor.posting_count as u64)
        });

        let term_meta = TermMeta {
            version: FTS_TERM_META_VERSION,
            generation: manifest.generation,
            field_hash: term_key.field_hash.clone(),
            term_hash: term_key.term_hash.clone(),
            document_frequency,
            total_term_frequency,
            block_count: descriptors.len() as u32,
            blocks: descriptors,
        };
        term_meta.validate()?;
        let term_meta_bytes = serde_json::to_vec(&term_meta).map_err(|error| {
            ApiError::internal(format!("failed to encode FTS term metadata: {error}"))
        })?;
        let term_meta_checksum = sha256_hex(&term_meta_bytes);
        let term_meta_len = u32::try_from(term_meta_bytes.len())
            .map_err(|_| ApiError::internal("FTS term metadata length exceeds u32"))?;
        let term_meta_key = fts_term_meta_key(
            collection,
            namespace,
            manifest.generation,
            &term_key.field_hash,
            &term_key.term_hash,
        );
        state
            .storage
            .put_bytes_if_absent(&term_meta_key, &term_meta_bytes)
            .await
            .map_err(map_store_error)?;
        field_entry.terms.insert(
            term_key.term_hash.clone(),
            TermMetaRef {
                generation: manifest.generation,
                checksum: term_meta_checksum,
                byte_len: term_meta_len,
            },
        );
        field_entry
            .term_literals
            .insert(term_key.term_hash.clone(), term_key.term.clone());
    }

    let mut empty_fields = Vec::new();
    for (field_hash, field_meta) in &mut fields {
        if field_meta.terms.is_empty() {
            empty_fields.push(field_hash.clone());
            continue;
        }
        field_meta
            .term_literals
            .retain(|term_hash, _| field_meta.terms.contains_key(term_hash));
        if field_meta.term_literals.len() != field_meta.terms.len() {
            return Err(ApiError::internal(format!(
                "missing term literals for field '{}'",
                field_meta.field_name
            )));
        }
        if next_field_tokenizers
            .get(&field_meta.field_name)
            .is_some_and(|tokenizer| tokenizer.is_none())
        {
            return Err(ApiError::internal(format!(
                "field '{}' is disabled for full_text_search but still contains terms",
                field_meta.field_name
            )));
        }
        if let Some(Some(tokenizer)) = next_field_tokenizers.get(&field_meta.field_name) {
            field_meta.tokenizer = *tokenizer;
        }
        let Some(stats) = corpus_stats_by_field.get(field_hash).cloned() else {
            return Err(ApiError::internal(format!(
                "missing corpus stats for indexed field '{}'",
                field_meta.field_name
            )));
        };
        field_meta.corpus_stats = stats;
        field_meta.term_count = field_meta.terms.len() as u64;
        if touched_field_hashes.contains(field_hash) || field_meta.lexicon.checksum.is_empty() {
            field_meta.prefix_terms = build_prefix_term_lookup(
                &field_meta.term_literals,
                state.runtime.fts_prefix_max_index_chars,
            );
            let mut lexicon_terms = field_meta
                .term_literals
                .values()
                .cloned()
                .collect::<Vec<_>>();
            lexicon_terms.sort();
            let lexicon_bytes = serde_json::to_vec(&lexicon_terms).map_err(|error| {
                ApiError::internal(format!("failed to encode FTS field lexicon: {error}"))
            })?;
            let lexicon_checksum = sha256_hex(&lexicon_bytes);
            let lexicon_len = u32::try_from(lexicon_bytes.len())
                .map_err(|_| ApiError::internal("FTS field lexicon length exceeds u32"))?;
            let lexicon_key =
                fts_field_lexicon_key(collection, namespace, manifest.generation, field_hash);
            state
                .storage
                .put_bytes_if_absent(&lexicon_key, &lexicon_bytes)
                .await
                .map_err(map_store_error)?;
            field_meta.lexicon = LexiconRef {
                generation: manifest.generation,
                checksum: lexicon_checksum,
                byte_len: lexicon_len,
            };
        }
    }
    for field_hash in empty_fields {
        fields.remove(&field_hash);
    }

    if fields.is_empty() && previous_index_meta.is_none() {
        return Ok(None);
    }

    let mut term_meta_checksums = BTreeMap::new();
    for (field_hash, field_meta) in &fields {
        for (term_hash, term_meta_ref) in &field_meta.terms {
            term_meta_checksums.insert(
                format!("{field_hash}:{term_hash}@{}", term_meta_ref.generation),
                term_meta_ref.checksum.clone(),
            );
        }
    }

    let index_meta = FtsIndexMeta {
        version: FTS_INDEX_META_VERSION,
        generation: manifest.generation,
        collection: collection.to_string(),
        namespace: namespace.to_string(),
        previous_generation: manifest.previous_generation,
        published_at: now_rfc3339(),
        fields,
        doc_lookup: doc_lookup_ref,
        term_meta_checksums,
    };
    index_meta.validate()?;
    let index_meta_bytes = serde_json::to_vec(&index_meta).map_err(|error| {
        ApiError::internal(format!("failed to encode FTS index metadata: {error}"))
    })?;
    let index_meta_key = fts_index_meta_key(collection, namespace, manifest.generation);
    let published = state
        .storage
        .put_bytes_if_absent(&index_meta_key, &index_meta_bytes)
        .await
        .map_err(map_store_error)?;
    if published {
        debug!(
            collection,
            namespace,
            generation = manifest.generation,
            terms_touched,
            blocks_rewritten,
            bytes_rewritten,
            "published FTS postings index generation"
        );
        return Ok(Some(index_meta));
    }

    if let Some(existing) =
        load_fts_index_meta_for_generation(state, collection, namespace, manifest.generation)
            .await?
    {
        return Ok(Some(existing));
    }

    Err(ApiError::store_unavailable(format!(
        "FTS index metadata publish contention for collection '{collection}' namespace '{namespace}' generation {}",
        manifest.generation
    )))
}

async fn materialize_term_blocks(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
    term_key: &TermKey,
    mut blocks: Vec<MutablePostingBlock>,
) -> Result<(Vec<PostingsBlockDescriptor>, usize, usize, u64), ApiError> {
    blocks.sort_by_key(|block| {
        block
            .postings
            .first()
            .map(|posting| posting.doc_id)
            .unwrap_or(0)
    });
    let mut pending = Vec::with_capacity(blocks.len());
    let mut rewritten_count = 0usize;
    let mut rewritten_bytes = 0usize;
    let mut total_term_frequency = 0_u64;
    let mut dirty_blocks = Vec::new();

    for block in blocks {
        if block.postings.is_empty() {
            continue;
        }
        total_term_frequency = total_term_frequency.saturating_add(
            block
                .postings
                .iter()
                .fold(0_u64, |sum, posting| sum.saturating_add(posting.tf as u64)),
        );
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
        let posting_count = u32::try_from(block.postings.len())
            .map_err(|_| ApiError::internal("FTS block posting_count exceeds u32"))?;
        if block.dirty || block.source_generation == 0 {
            let max_term_score = estimate_max_term_score(&block.postings);
            let encoded = encode_postings_block(&block.postings, max_term_score)?;
            let checksum = sha256_hex(&encoded);
            let byte_len = u32::try_from(encoded.len())
                .map_err(|_| ApiError::internal("FTS block payload length exceeds u32"))?;
            let (max_tf, min_doc_len) = block_posting_bounds(&block.postings);
            dirty_blocks.push(PendingDirtyBlock {
                block_id: block.block_id,
                doc_id_min: first_doc_id,
                doc_id_max: last_doc_id,
                posting_count,
                max_term_score,
                max_tf,
                min_doc_len,
                checksum,
                byte_len,
                encoded,
                pack_id: None,
                pack_offset: None,
                pack_len: None,
            });
            pending.push(PendingDescriptor::Dirty(dirty_blocks.len() - 1));
            rewritten_count = rewritten_count.saturating_add(1);
            rewritten_bytes = rewritten_bytes.saturating_add(byte_len as usize);
        } else {
            if block.source_checksum.is_empty()
                || block.source_byte_len == 0
                || block.source_pack_id.is_empty()
                || block.source_pack_len == 0
            {
                return Err(ApiError::internal(
                    "unchanged FTS block is missing source pack/checksum metadata",
                ));
            }
            pending.push(PendingDescriptor::Ready(PostingsBlockDescriptor {
                generation: block.source_generation,
                block_id: block.block_id,
                pack_id: block.source_pack_id,
                pack_offset: block.source_pack_offset,
                pack_len: block.source_pack_len,
                doc_id_min: first_doc_id,
                doc_id_max: last_doc_id,
                posting_count,
                max_term_score: block.source_max_term_score,
                max_tf: block.source_max_tf,
                min_doc_len: block.source_min_doc_len,
                checksum: block.source_checksum,
                byte_len: block.source_byte_len,
            }));
        }
    }

    if !dirty_blocks.is_empty() {
        let mut cursor = 0usize;
        let mut pack_index = 0usize;
        while cursor < dirty_blocks.len() {
            let end = (cursor + FTS_BLOCK_PACK_TARGET_BLOCKS).min(dirty_blocks.len());
            let pack_id = format!("p{pack_index:04x}");
            let pack_blocks = dirty_blocks[cursor..end]
                .iter()
                .map(|block| (block.block_id.clone(), block.encoded.clone()))
                .collect::<Vec<_>>();
            let pack_bytes = encode_block_pack(&pack_blocks)?;
            let pack_key = fts_block_pack_key(
                collection,
                namespace,
                generation,
                &term_key.field_hash,
                &term_key.term_hash,
                &pack_id,
            );
            let _published = state
                .storage
                .put_bytes_if_absent(&pack_key, &pack_bytes)
                .await
                .map_err(map_store_error)?;
            let decoded_pack = decode_block_pack(&pack_bytes)?;
            for block in &mut dirty_blocks[cursor..end] {
                let entry = decoded_pack.entry(&block.block_id).ok_or_else(|| {
                    ApiError::internal("encoded block pack missing block directory entry")
                })?;
                block.pack_id = Some(pack_id.clone());
                block.pack_offset = Some(entry.offset);
                block.pack_len = Some(entry.len);
            }
            cursor = end;
            pack_index = pack_index.saturating_add(1);
        }
    }

    let mut descriptors = Vec::with_capacity(pending.len());
    for pending_descriptor in pending {
        match pending_descriptor {
            PendingDescriptor::Ready(descriptor) => descriptors.push(descriptor),
            PendingDescriptor::Dirty(index) => {
                let dirty = dirty_blocks.get(index).ok_or_else(|| {
                    ApiError::internal("FTS dirty descriptor index out of bounds")
                })?;
                let pack_id = dirty.pack_id.clone().ok_or_else(|| {
                    ApiError::internal("FTS dirty block missing pack_id assignment")
                })?;
                let pack_offset = dirty.pack_offset.ok_or_else(|| {
                    ApiError::internal("FTS dirty block missing pack_offset assignment")
                })?;
                let pack_len = dirty.pack_len.ok_or_else(|| {
                    ApiError::internal("FTS dirty block missing pack_len assignment")
                })?;
                descriptors.push(PostingsBlockDescriptor {
                    generation,
                    block_id: dirty.block_id.clone(),
                    pack_id,
                    pack_offset,
                    pack_len,
                    doc_id_min: dirty.doc_id_min,
                    doc_id_max: dirty.doc_id_max,
                    posting_count: dirty.posting_count,
                    max_term_score: dirty.max_term_score,
                    max_tf: dirty.max_tf,
                    min_doc_len: dirty.min_doc_len,
                    checksum: dirty.checksum.clone(),
                    byte_len: dirty.byte_len,
                });
            }
        }
    }

    descriptors.sort_by_key(|descriptor| descriptor.doc_id_min);
    Ok((
        descriptors,
        rewritten_count,
        rewritten_bytes,
        total_term_frequency,
    ))
}

#[derive(Debug, Clone)]
struct PendingDirtyBlock {
    block_id: String,
    doc_id_min: u64,
    doc_id_max: u64,
    posting_count: u32,
    max_term_score: f32,
    max_tf: u16,
    min_doc_len: u16,
    checksum: String,
    byte_len: u32,
    encoded: Vec<u8>,
    pack_id: Option<String>,
    pack_offset: Option<u32>,
    pack_len: Option<u32>,
}

#[derive(Debug, Clone)]
enum PendingDescriptor {
    Ready(PostingsBlockDescriptor),
    Dirty(usize),
}

fn block_posting_bounds(postings: &[postings_codec::Posting]) -> (u16, u16) {
    let mut max_tf = 0_u16;
    let mut min_doc_len = u16::MAX;
    for posting in postings {
        max_tf = max_tf.max(posting.tf);
        let doc_len = posting.doc_len.max(1);
        min_doc_len = min_doc_len.min(doc_len);
    }
    (max_tf.max(1), min_doc_len.max(1))
}

async fn load_term_meta_for_ref(
    state: &AppState,
    collection: &str,
    namespace: &str,
    term_key: &TermKey,
    term_ref: &TermMetaRef,
) -> Result<TermMeta, ApiError> {
    let key = fts_term_meta_key(
        collection,
        namespace,
        term_ref.generation,
        &term_key.field_hash,
        &term_key.term_hash,
    );
    let raw = state
        .storage
        .get_bytes(&key)
        .await
        .map_err(map_store_error)?;
    if raw.len() != term_ref.byte_len as usize {
        return Err(ApiError::store_unavailable(format!(
            "FTS term metadata length mismatch for '{}'",
            key
        )));
    }
    let checksum = sha256_hex(&raw);
    if checksum != term_ref.checksum {
        return Err(ApiError::store_unavailable(format!(
            "FTS term metadata checksum mismatch for '{}'",
            key
        )));
    }
    let term_meta: TermMeta = serde_json::from_slice(&raw).map_err(|error| {
        ApiError::internal(format!("failed to parse FTS term metadata: {error}"))
    })?;
    if term_meta.generation != term_ref.generation {
        return Err(ApiError::store_unavailable(format!(
            "FTS term metadata generation mismatch for '{}'",
            key
        )));
    }
    term_meta.validate()?;
    Ok(term_meta)
}

async fn load_term_blocks(
    state: &AppState,
    collection: &str,
    namespace: &str,
    term_key: &TermKey,
    term_meta: &TermMeta,
) -> Result<Vec<MutablePostingBlock>, ApiError> {
    load_term_blocks_for_descriptors(state, collection, namespace, term_key, &term_meta.blocks)
        .await
}

pub(crate) async fn load_term_blocks_for_descriptors(
    state: &AppState,
    collection: &str,
    namespace: &str,
    term_key: &TermKey,
    descriptors: &[PostingsBlockDescriptor],
) -> Result<Vec<MutablePostingBlock>, ApiError> {
    let mut blocks = Vec::with_capacity(descriptors.len());
    let mut pack_cache: BTreeMap<(u64, String), DecodedBlockPack> = BTreeMap::new();
    for descriptor in descriptors {
        let pack_cache_key = (descriptor.generation, descriptor.pack_id.clone());
        if !pack_cache.contains_key(&pack_cache_key) {
            let pack_key = fts_block_pack_key(
                collection,
                namespace,
                descriptor.generation,
                &term_key.field_hash,
                &term_key.term_hash,
                &descriptor.pack_id,
            );
            let pack_raw = state
                .storage
                .get_bytes(&pack_key)
                .await
                .map_err(map_store_error)?;
            let decoded_pack = decode_block_pack(&pack_raw)?;
            pack_cache.insert(pack_cache_key.clone(), decoded_pack);
        }
        let pack = pack_cache
            .get(&pack_cache_key)
            .ok_or_else(|| ApiError::internal("FTS pack cache entry missing after load"))?;
        let raw = pack.block_slice(
            &descriptor.block_id,
            descriptor.pack_offset,
            descriptor.pack_len,
        )?;
        if raw.len() != descriptor.byte_len as usize {
            return Err(ApiError::store_unavailable(format!(
                "FTS postings block length mismatch for '{}'",
                descriptor.block_id
            )));
        }
        let checksum = sha256_hex(raw);
        if checksum != descriptor.checksum {
            return Err(ApiError::store_unavailable(format!(
                "FTS postings block checksum mismatch for '{}'",
                descriptor.block_id
            )));
        }
        let decoded = decode_postings_block(raw)?;
        if decoded.doc_id_min != descriptor.doc_id_min
            || decoded.doc_id_max != descriptor.doc_id_max
            || decoded.postings.len() != descriptor.posting_count as usize
        {
            return Err(ApiError::store_unavailable(format!(
                "FTS postings block descriptor mismatch for '{}'",
                descriptor.block_id
            )));
        }
        if (decoded.max_term_score - descriptor.max_term_score).abs() > 1e-5 {
            return Err(ApiError::store_unavailable(format!(
                "FTS postings block max_term_score mismatch for '{}'",
                descriptor.block_id
            )));
        }
        let (max_tf, min_doc_len) = block_posting_bounds(&decoded.postings);
        if max_tf != descriptor.max_tf || min_doc_len != descriptor.min_doc_len {
            return Err(ApiError::store_unavailable(format!(
                "FTS postings block tf/doc_len descriptor mismatch for '{}'",
                descriptor.block_id
            )));
        }
        blocks.push(MutablePostingBlock {
            block_id: descriptor.block_id.clone(),
            source_generation: descriptor.generation,
            source_pack_id: descriptor.pack_id.clone(),
            source_pack_offset: descriptor.pack_offset,
            source_pack_len: descriptor.pack_len,
            source_checksum: descriptor.checksum.clone(),
            source_byte_len: descriptor.byte_len,
            source_max_term_score: descriptor.max_term_score,
            source_max_tf: descriptor.max_tf,
            source_min_doc_len: descriptor.min_doc_len,
            postings: decoded.postings,
            dirty: false,
        });
    }
    Ok(blocks)
}

async fn materialize_doc_lookup(
    state: &AppState,
    collection: &str,
    namespace: &str,
    generation: u64,
    vectors: &BTreeMap<String, UpsertVector>,
    field_tokenizers: &BTreeMap<String, Option<TokenizerKind>>,
) -> Result<DocLookupRef, ApiError> {
    let mut docs = Vec::with_capacity(vectors.len());
    for vector in vectors.values() {
        let doc_len = terms_from_vector_with_tokenizers(Some(vector), field_tokenizers)
            .values()
            .fold(0_u32, |sum, tf| sum.saturating_add(*tf as u32))
            .max(1)
            .min(u16::MAX as u32) as u16;
        docs.push(FtsDocEntry {
            doc_id: stable_doc_id(&vector.id),
            vector_id: vector.id.clone(),
            doc_len,
            metadata: vector.metadata.clone(),
        });
    }
    docs.sort_by(|left, right| {
        left.doc_id
            .cmp(&right.doc_id)
            .then_with(|| left.vector_id.cmp(&right.vector_id))
    });
    let lookup = FtsDocLookup {
        version: FTS_DOC_LOOKUP_VERSION,
        generation,
        doc_count: docs.len() as u64,
        docs,
    };
    lookup.validate()?;
    let bytes = serde_json::to_vec(&lookup)
        .map_err(|error| ApiError::internal(format!("failed to encode FTS doc lookup: {error}")))?;
    let checksum = sha256_hex(&bytes);
    let byte_len = u32::try_from(bytes.len())
        .map_err(|_| ApiError::internal("FTS doc lookup length exceeds u32"))?;
    let key = fts_doc_lookup_key(collection, namespace, generation);
    let published = state
        .storage
        .put_bytes_if_absent(&key, &bytes)
        .await
        .map_err(map_store_error)?;
    if published {
        return Ok(DocLookupRef {
            generation,
            checksum,
            byte_len,
        });
    }
    let existing = state
        .storage
        .get_bytes(&key)
        .await
        .map_err(map_store_error)?;
    Ok(DocLookupRef {
        generation,
        checksum: sha256_hex(&existing),
        byte_len: u32::try_from(existing.len())
            .map_err(|_| ApiError::internal("existing FTS doc lookup length exceeds u32"))?,
    })
}

fn compute_field_corpus_stats(
    vectors: &BTreeMap<String, UpsertVector>,
    field_tokenizers: &BTreeMap<String, Option<TokenizerKind>>,
) -> BTreeMap<String, FtsFieldCorpusStats> {
    let mut stats_by_field: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    for vector in vectors.values() {
        let terms = terms_from_vector_with_tokenizers(Some(vector), field_tokenizers);
        let mut field_lengths: BTreeMap<String, u64> = BTreeMap::new();
        for ((field_name, _term), tf) in terms {
            let entry = field_lengths.entry(field_name).or_insert(0);
            *entry = entry.saturating_add(tf as u64);
        }
        for (field_name, doc_len) in field_lengths {
            if doc_len == 0 {
                continue;
            }
            let field_hash = keyspace::field_hash(&field_name);
            let entry = stats_by_field.entry(field_hash).or_insert((0, 0));
            entry.0 = entry.0.saturating_add(1);
            entry.1 = entry.1.saturating_add(doc_len);
        }
    }
    stats_by_field
        .into_iter()
        .map(|(field_hash, (document_count, sum_doc_len))| {
            let avg_doc_len = if document_count == 0 {
                1.0
            } else {
                sum_doc_len as f32 / document_count as f32
            };
            (
                field_hash,
                FtsFieldCorpusStats {
                    document_count,
                    sum_doc_len,
                    avg_doc_len,
                },
            )
        })
        .collect()
}

fn resolve_field_tokenizers(
    schema: Option<&serde_json::Value>,
    previous_index_meta: Option<&FtsIndexMeta>,
    previous_vectors: &BTreeMap<String, UpsertVector>,
    next_vectors: &BTreeMap<String, UpsertVector>,
) -> Result<
    (
        BTreeMap<String, Option<TokenizerKind>>,
        BTreeMap<String, Option<TokenizerKind>>,
    ),
    ApiError,
> {
    let mut field_names = BTreeSet::new();
    collect_metadata_field_names(previous_vectors, &mut field_names);
    collect_metadata_field_names(next_vectors, &mut field_names);
    if let Some(previous_meta) = previous_index_meta {
        for field_meta in previous_meta.fields.values() {
            if !field_meta.field_name.trim().is_empty() {
                field_names.insert(field_meta.field_name.clone());
            }
        }
    }

    let mut next_field_tokenizers = BTreeMap::new();
    for field_name in &field_names {
        let config = resolve_field_search_config(schema, field_name)?;
        next_field_tokenizers.insert(
            field_name.clone(),
            if config.enabled {
                Some(config.tokenizer)
            } else {
                None
            },
        );
    }

    let previous_field_tokenizers = if let Some(previous_meta) = previous_index_meta {
        let tokenizer_by_field_name = previous_meta
            .fields
            .values()
            .map(|field_meta| (field_meta.field_name.clone(), field_meta.tokenizer))
            .collect::<BTreeMap<_, _>>();
        field_names
            .iter()
            .map(|field_name| {
                (
                    field_name.clone(),
                    tokenizer_by_field_name.get(field_name).copied(),
                )
            })
            .collect()
    } else {
        next_field_tokenizers.clone()
    };

    Ok((previous_field_tokenizers, next_field_tokenizers))
}

fn collect_metadata_field_names(
    vectors: &BTreeMap<String, UpsertVector>,
    out: &mut BTreeSet<String>,
) {
    for vector in vectors.values() {
        let Some(serde_json::Value::Object(metadata)) = vector.metadata.as_ref() else {
            continue;
        };
        out.extend(metadata.keys().cloned());
    }
}

fn build_prefix_term_lookup(
    term_literals: &BTreeMap<String, String>,
    max_prefix_chars: usize,
) -> BTreeMap<String, Vec<String>> {
    let max_prefix_chars = if max_prefix_chars == 0 {
        DEFAULT_PREFIX_INDEX_MAX_CHARS
    } else {
        max_prefix_chars
    };
    let mut prefixes: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (term_hash, term_literal) in term_literals {
        let chars = term_literal.chars().collect::<Vec<_>>();
        for prefix_len in 1..=chars.len().min(max_prefix_chars) {
            let prefix = chars[0..prefix_len].iter().collect::<String>();
            if prefix.is_empty() {
                continue;
            }
            prefixes.entry(prefix).or_default().push(term_hash.clone());
        }
    }
    for hashes in prefixes.values_mut() {
        hashes.sort();
        hashes.dedup();
    }
    prefixes
}

async fn load_manifest_generation(
    state: &AppState,
    collection: &str,
    generation: u64,
) -> Result<Option<Manifest>, ApiError> {
    let key = manifest_generation_key(collection, generation);
    let raw = match state.storage.get_bytes(&key).await {
        Ok(raw) => raw,
        Err(TurboVectorError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(map_store_error(error)),
    };
    let manifest: Manifest = serde_json::from_slice(&raw).map_err(|error| {
        ApiError::internal(format!("failed to parse manifest '{key}': {error}"))
    })?;
    Ok(Some(manifest))
}

fn block_policy_from_state(state: &AppState) -> BlockPolicy {
    BlockPolicy {
        target_postings: state.runtime.fts_block_target_postings,
        split_threshold: state.runtime.fts_block_split_threshold,
        merge_threshold: state.runtime.fts_block_merge_threshold,
        max_term_blocks_touched_per_doc_update: state
            .runtime
            .fts_max_term_blocks_touched_per_doc_update,
        enable_delta_rebalance: state.runtime.fts_enable_delta_rebalance,
    }
    .normalized()
}

fn ensure_no_stable_doc_id_collisions(
    collection: &str,
    namespace: &str,
    generation: u64,
    vectors: &BTreeMap<String, UpsertVector>,
) -> Result<(), ApiError> {
    ensure_no_doc_id_collisions(
        collection,
        namespace,
        generation,
        vectors.keys(),
        stable_doc_id,
    )
}

fn ensure_no_doc_id_collisions<'a, I, F>(
    collection: &str,
    namespace: &str,
    generation: u64,
    vector_ids: I,
    map_doc_id: F,
) -> Result<(), ApiError>
where
    I: IntoIterator<Item = &'a String>,
    F: Fn(&str) -> u64,
{
    let mut seen_by_doc_id: BTreeMap<u64, String> = BTreeMap::new();
    for vector_id in vector_ids {
        let vector_id = vector_id.as_str();
        let doc_id = map_doc_id(vector_id);
        if let Some(existing_vector_id) = seen_by_doc_id.insert(doc_id, vector_id.to_string()) {
            return Err(ApiError::internal(format!(
                "FTS stable_doc_id collision for collection '{collection}' namespace '{namespace}' generation {generation}: vectors '{existing_vector_id}' and '{vector_id}' map to doc_id {doc_id:#018x}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        ensure_no_doc_id_collisions, keyspace::stable_doc_id, FtsDocEntry, FtsDocLookup,
        FTS_DOC_LOOKUP_VERSION,
    };
    use crate::models::UpsertVector;

    #[test]
    fn stable_doc_id_collision_guard_rejects_aliasing_ids() {
        let doc_ids = vec!["doc-a".to_string(), "doc-b".to_string()];
        let error = ensure_no_doc_id_collisions("docs", "ns_a", 7, doc_ids.iter(), |_| 42)
            .expect_err("collision must be rejected");
        let debug_error = format!("{error:?}");
        assert!(
            debug_error.contains(
                "FTS stable_doc_id collision for collection 'docs' namespace 'ns_a' generation 7"
            ),
            "collision error must include collection/namespace/generation context: {:?}",
            error
        );
    }

    #[test]
    fn stable_doc_id_collision_guard_allows_unique_ids() {
        let doc_ids = vec!["doc-a".to_string(), "doc-b".to_string()];
        ensure_no_doc_id_collisions("docs", "ns_a", 8, doc_ids.iter(), |vector_id| {
            if vector_id == "doc-a" {
                1
            } else {
                2
            }
        })
        .expect("unique mapped doc ids should pass");
    }

    #[test]
    fn lightweight_doc_lookup_payload_is_at_least_40_percent_smaller_than_full_vectors() {
        let mut vectors = BTreeMap::new();
        for index in 0..256_u32 {
            vectors.insert(
                format!("doc-{index:04}"),
                UpsertVector {
                    id: format!("doc-{index:04}"),
                    values: vec![0.25_f32; 512],
                    metadata: Some(json!({
                        "body": "alpha beta gamma delta epsilon",
                        "cohort": if index % 2 == 0 { "a" } else { "b" }
                    })),
                },
            );
        }
        let full_bytes = serde_json::to_vec(&vectors)
            .expect("serialize full vectors")
            .len();

        let mut docs = vectors
            .values()
            .map(|vector| FtsDocEntry {
                doc_id: stable_doc_id(&vector.id),
                vector_id: vector.id.clone(),
                doc_len: 5,
                metadata: vector.metadata.clone(),
            })
            .collect::<Vec<_>>();
        docs.sort_by_key(|entry| entry.doc_id);
        let lookup = FtsDocLookup {
            version: FTS_DOC_LOOKUP_VERSION,
            generation: 3,
            doc_count: docs.len() as u64,
            docs,
        };
        lookup.validate().expect("doc lookup should validate");
        let lookup_bytes = serde_json::to_vec(&lookup)
            .expect("serialize doc lookup")
            .len();

        let ratio = lookup_bytes as f64 / full_bytes as f64;
        assert!(
            ratio <= 0.60,
            "doc lookup payload should be <=60% of full vector payload (ratio={ratio:.4})"
        );
    }
}
