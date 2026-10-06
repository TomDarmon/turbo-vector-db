use serde_json::Value;

use crate::error::ApiError;

use super::{
    bm25::{Bm25Params, ScoreExpression},
    keyspace::term_hash,
    rank_expr::{Bm25Expr, Bm25Field, Bm25MatchMode, RankExpr},
    term_meta::{FtsFieldMeta, FtsIndexMeta, TermMetaRef},
    tokenize::{tokenize_query_text, TokenizerKind},
};

#[derive(Debug, Clone)]
pub(crate) struct LexicalPlan {
    pub(crate) expression: ScoreExpression,
    pub(crate) leaves: Vec<Bm25LeafPlan>,
    pub(crate) conditional_boosts: Vec<ConditionalBoostPlan>,
}

#[derive(Debug, Clone)]
pub(crate) struct Bm25LeafPlan {
    pub(crate) leaf_id: usize,
    pub(crate) terms: Vec<PlannedTerm>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedTerm {
    pub(crate) field_name: String,
    pub(crate) field_hash: String,
    pub(crate) term: String,
    pub(crate) term_hash: String,
    pub(crate) term_ref: TermMetaRef,
    pub(crate) params: Bm25Params,
}

#[derive(Debug, Clone)]
pub(crate) struct ConditionalBoostPlan {
    pub(crate) filter: Value,
    pub(crate) boost: f32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PrefixExpansionLimits {
    pub(crate) max_expansions: usize,
    pub(crate) max_expansion_bytes: usize,
}

impl Default for PrefixExpansionLimits {
    fn default() -> Self {
        Self {
            max_expansions: 64,
            max_expansion_bytes: 4096,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct FieldSearchConfig {
    pub(crate) enabled: bool,
    pub(crate) tokenizer: TokenizerKind,
    pub(crate) bm25: Bm25Params,
}

impl Default for FieldSearchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tokenizer: TokenizerKind::WordV1,
            bm25: Bm25Params::default(),
        }
    }
}

pub(crate) fn build_lexical_plan(
    expression: &RankExpr,
    index_meta: &FtsIndexMeta,
    schema: Option<&Value>,
    prefix_limits: PrefixExpansionLimits,
) -> Result<LexicalPlan, ApiError> {
    let mut leaves = Vec::new();
    let mut conditional_boosts = Vec::new();
    let compiled = compile_expression(
        expression,
        index_meta,
        schema,
        prefix_limits,
        &mut leaves,
        &mut conditional_boosts,
    )?;
    Ok(LexicalPlan {
        expression: compiled,
        leaves,
        conditional_boosts,
    })
}

pub(crate) fn resolve_field_search_config(
    schema: Option<&Value>,
    field_name: &str,
) -> Result<FieldSearchConfig, ApiError> {
    let mut config = FieldSearchConfig::default();
    let Some(schema) = schema else {
        return Ok(config);
    };
    let Some(schema_obj) = schema.as_object() else {
        return Ok(config);
    };
    let Some(field_schema) = schema_obj.get(field_name) else {
        return Ok(config);
    };
    let Some(field_obj) = field_schema.as_object() else {
        return Ok(config);
    };
    let Some(fts_config) = field_obj.get("full_text_search") else {
        return Ok(config);
    };

    match fts_config {
        Value::Bool(enabled) => {
            config.enabled = *enabled;
            return Ok(config);
        }
        Value::Object(object) => {
            if let Some(enabled) = object.get("enabled") {
                config.enabled = enabled.as_bool().ok_or_else(|| {
                    ApiError::invalid_argument("full_text_search.enabled must be a boolean")
                })?;
            }
            config.tokenizer = TokenizerKind::from_schema_value(object.get("tokenizer"))?;
            if let Some(k1) = object.get("k1") {
                config.bm25.k1 = parse_f32(k1, "full_text_search.k1")?;
            }
            if let Some(b) = object.get("b") {
                config.bm25.b = parse_f32(b, "full_text_search.b")?;
            }
            if let Some(bm25_obj) = object.get("bm25") {
                let bm25_obj = bm25_obj.as_object().ok_or_else(|| {
                    ApiError::invalid_argument("full_text_search.bm25 must be an object")
                })?;
                if let Some(k1) = bm25_obj.get("k1") {
                    config.bm25.k1 = parse_f32(k1, "full_text_search.bm25.k1")?;
                }
                if let Some(b) = bm25_obj.get("b") {
                    config.bm25.b = parse_f32(b, "full_text_search.bm25.b")?;
                }
            }
            config.bm25 = config.bm25.validate()?;
            Ok(config)
        }
        _ => Err(ApiError::invalid_argument(
            "full_text_search must be a boolean or object",
        )),
    }
}

fn compile_expression(
    expression: &RankExpr,
    index_meta: &FtsIndexMeta,
    schema: Option<&Value>,
    prefix_limits: PrefixExpansionLimits,
    leaves: &mut Vec<Bm25LeafPlan>,
    conditional_boosts: &mut Vec<ConditionalBoostPlan>,
) -> Result<ScoreExpression, ApiError> {
    match expression {
        RankExpr::Bm25(bm25_expr) => {
            let leaf_id = leaves.len();
            let leaf = plan_bm25_leaf(leaf_id, bm25_expr, index_meta, schema, prefix_limits)?;
            leaves.push(leaf);
            Ok(ScoreExpression::Leaf(leaf_id))
        }
        RankExpr::Sum(children) => {
            let mut compiled = Vec::with_capacity(children.len());
            for child in children {
                compiled.push(compile_expression(
                    child,
                    index_meta,
                    schema,
                    prefix_limits,
                    leaves,
                    conditional_boosts,
                )?);
            }
            Ok(ScoreExpression::Sum(compiled))
        }
        RankExpr::Max(children) => {
            let mut compiled = Vec::with_capacity(children.len());
            for child in children {
                compiled.push(compile_expression(
                    child,
                    index_meta,
                    schema,
                    prefix_limits,
                    leaves,
                    conditional_boosts,
                )?);
            }
            Ok(ScoreExpression::Max(compiled))
        }
        RankExpr::Product { weight, expr } => Ok(ScoreExpression::Product {
            weight: *weight,
            expression: Box::new(compile_expression(
                expr,
                index_meta,
                schema,
                prefix_limits,
                leaves,
                conditional_boosts,
            )?),
        }),
        RankExpr::RankByFilter {
            filter,
            boost,
            expr,
        } => {
            conditional_boosts.push(ConditionalBoostPlan {
                filter: filter.clone(),
                boost: *boost,
            });
            compile_expression(
                expr,
                index_meta,
                schema,
                prefix_limits,
                leaves,
                conditional_boosts,
            )
        }
        RankExpr::Vector(_) => Err(ApiError::invalid_argument(
            "vector rank expressions cannot be compiled as lexical plans",
        )),
    }
}

fn plan_bm25_leaf(
    leaf_id: usize,
    expression: &Bm25Expr,
    index_meta: &FtsIndexMeta,
    schema: Option<&Value>,
    prefix_limits: PrefixExpansionLimits,
) -> Result<Bm25LeafPlan, ApiError> {
    let selected_fields = select_fields(&expression.field, index_meta);
    let mut terms = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for (field_hash, field_meta) in selected_fields {
        let config = resolve_field_search_config(schema, &field_meta.field_name)?;
        if !config.enabled {
            continue;
        }
        let tokens = tokenize_query_text(&expression.query, config.tokenizer);
        for token in tokens {
            if token.is_empty() {
                continue;
            }
            match expression.match_mode {
                Bm25MatchMode::Exact => {
                    let token_hash = term_hash(&token);
                    let Some(term_ref) = field_meta.terms.get(&token_hash).cloned() else {
                        continue;
                    };
                    if !seen.insert((field_hash.clone(), token_hash.clone())) {
                        continue;
                    }
                    terms.push(PlannedTerm {
                        field_name: field_meta.field_name.clone(),
                        field_hash: field_hash.clone(),
                        term: token,
                        term_hash: token_hash,
                        term_ref,
                        params: config.bm25,
                    });
                }
                Bm25MatchMode::Prefix => {
                    let expansions = expand_prefix_terms(field_meta, &token, prefix_limits);
                    for (expanded_term, expanded_hash) in expansions {
                        let Some(term_ref) = field_meta.terms.get(&expanded_hash).cloned() else {
                            continue;
                        };
                        if !seen.insert((field_hash.clone(), expanded_hash.clone())) {
                            continue;
                        }
                        terms.push(PlannedTerm {
                            field_name: field_meta.field_name.clone(),
                            field_hash: field_hash.clone(),
                            term: expanded_term,
                            term_hash: expanded_hash,
                            term_ref,
                            params: config.bm25,
                        });
                    }
                }
            }
        }
    }

    Ok(Bm25LeafPlan { leaf_id, terms })
}

fn expand_prefix_terms(
    field_meta: &FtsFieldMeta,
    prefix: &str,
    limits: PrefixExpansionLimits,
) -> Vec<(String, String)> {
    let normalized_prefix = prefix.trim().to_string();
    if normalized_prefix.is_empty() {
        return Vec::new();
    }
    let Some(term_hashes) = field_meta.prefix_terms.get(&normalized_prefix) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut used_bytes = 0usize;
    for term_hash in term_hashes {
        if out.len() >= limits.max_expansions {
            break;
        }
        let Some(term) = field_meta.term_literals.get(term_hash).cloned() else {
            continue;
        };
        let bytes = term
            .len()
            .saturating_add(term_hash.len())
            .saturating_add(std::mem::size_of::<String>() * 2);
        if used_bytes.saturating_add(bytes) > limits.max_expansion_bytes {
            break;
        }
        used_bytes = used_bytes.saturating_add(bytes);
        out.push((term, term_hash.clone()));
    }
    out
}

fn select_fields<'a>(
    field: &Bm25Field,
    index_meta: &'a FtsIndexMeta,
) -> Vec<(String, &'a FtsFieldMeta)> {
    match field {
        Bm25Field::Text => index_meta
            .fields
            .iter()
            .map(|(field_hash, field_meta)| (field_hash.clone(), field_meta))
            .collect(),
        Bm25Field::Field(target_name) => index_meta
            .fields
            .iter()
            .filter(|(_, field_meta)| field_meta.field_name == *target_name)
            .map(|(field_hash, field_meta)| (field_hash.clone(), field_meta))
            .collect(),
    }
}

fn parse_f32(raw: &Value, field: &str) -> Result<f32, ApiError> {
    let value = raw
        .as_f64()
        .ok_or_else(|| ApiError::invalid_argument(format!("{field} must be a number")))?;
    if !value.is_finite() {
        return Err(ApiError::invalid_argument(format!(
            "{field} must be a finite number",
        )));
    }
    Ok(value as f32)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{build_lexical_plan, resolve_field_search_config, PrefixExpansionLimits};
    use crate::fts::{
        keyspace::{field_hash, term_hash},
        rank_expr::{parse_rank_expr, ParsedRankExpr},
        term_meta::{
            DocLookupRef, FtsFieldCorpusStats, FtsFieldMeta, FtsIndexMeta, LexiconRef, TermMetaRef,
            FTS_INDEX_META_VERSION,
        },
        tokenize::TokenizerKind,
    };

    fn fixture_index_meta() -> FtsIndexMeta {
        let mut fields = BTreeMap::new();
        let title_hash = field_hash("title");
        let body_hash = field_hash("body");

        let mut title_terms = BTreeMap::new();
        title_terms.insert(
            term_hash("rust"),
            TermMetaRef {
                generation: 2,
                checksum: "c1".to_string(),
                byte_len: 10,
            },
        );
        let mut body_terms = BTreeMap::new();
        body_terms.insert(
            term_hash("rust"),
            TermMetaRef {
                generation: 2,
                checksum: "c2".to_string(),
                byte_len: 10,
            },
        );
        body_terms.insert(
            term_hash("vector"),
            TermMetaRef {
                generation: 2,
                checksum: "c3".to_string(),
                byte_len: 10,
            },
        );
        body_terms.insert(
            term_hash("rustacean"),
            TermMetaRef {
                generation: 2,
                checksum: "c4".to_string(),
                byte_len: 10,
            },
        );
        let mut title_literals = BTreeMap::new();
        title_literals.insert(term_hash("rust"), "rust".to_string());
        let mut body_literals = BTreeMap::new();
        body_literals.insert(term_hash("rust"), "rust".to_string());
        body_literals.insert(term_hash("vector"), "vector".to_string());
        body_literals.insert(term_hash("rustacean"), "rustacean".to_string());
        let mut title_prefix_terms = BTreeMap::new();
        title_prefix_terms.insert("r".to_string(), vec![term_hash("rust")]);
        title_prefix_terms.insert("ru".to_string(), vec![term_hash("rust")]);
        let mut body_prefix_terms = BTreeMap::new();
        let mut ru_hashes = vec![term_hash("rust"), term_hash("rustacean")];
        ru_hashes.sort();
        body_prefix_terms.insert("r".to_string(), ru_hashes.clone());
        body_prefix_terms.insert("ru".to_string(), ru_hashes);
        body_prefix_terms.insert("v".to_string(), vec![term_hash("vector")]);

        fields.insert(
            title_hash.clone(),
            FtsFieldMeta {
                field_hash: title_hash.clone(),
                field_name: "title".to_string(),
                tokenizer: TokenizerKind::WordV1,
                term_count: title_terms.len() as u64,
                corpus_stats: FtsFieldCorpusStats {
                    document_count: 10,
                    sum_doc_len: 120,
                    avg_doc_len: 12.0,
                },
                lexicon: LexiconRef {
                    generation: 2,
                    checksum: "l1".to_string(),
                    byte_len: 10,
                },
                terms: title_terms,
                term_literals: title_literals,
                prefix_terms: title_prefix_terms,
            },
        );
        fields.insert(
            body_hash.clone(),
            FtsFieldMeta {
                field_hash: body_hash.clone(),
                field_name: "body".to_string(),
                tokenizer: TokenizerKind::WordV1,
                term_count: body_terms.len() as u64,
                corpus_stats: FtsFieldCorpusStats {
                    document_count: 10,
                    sum_doc_len: 140,
                    avg_doc_len: 14.0,
                },
                lexicon: LexiconRef {
                    generation: 2,
                    checksum: "l2".to_string(),
                    byte_len: 10,
                },
                terms: body_terms,
                term_literals: body_literals,
                prefix_terms: body_prefix_terms,
            },
        );

        FtsIndexMeta {
            version: FTS_INDEX_META_VERSION,
            generation: 2,
            collection: "docs".to_string(),
            namespace: "ns_a".to_string(),
            previous_generation: Some(1),
            published_at: "2026-02-01T00:00:00Z".to_string(),
            fields,
            doc_lookup: DocLookupRef {
                generation: 2,
                checksum: "d1".to_string(),
                byte_len: 42,
            },
            term_meta_checksums: BTreeMap::new(),
        }
    }

    #[test]
    fn planner_expands_text_bm25_to_all_indexed_fields() {
        let rank = parse_rank_expr(&json!(["text", "BM25", "rust vector"])).expect("rank parse");
        let ParsedRankExpr::Lexical(expression) = rank else {
            panic!("expected lexical expression");
        };
        let plan = build_lexical_plan(
            &expression,
            &fixture_index_meta(),
            None,
            PrefixExpansionLimits::default(),
        )
        .expect("plan");
        assert_eq!(plan.leaves.len(), 1);
        let fields = plan.leaves[0]
            .terms
            .iter()
            .map(|term| term.field_name.as_str())
            .collect::<Vec<_>>();
        assert!(fields.contains(&"title"), "title field should be planned");
        assert!(fields.contains(&"body"), "body field should be planned");
    }

    #[test]
    fn planner_applies_schema_overrides_and_disables_fields() {
        let rank = parse_rank_expr(&json!(["text", "BM25", "rust"])).expect("rank parse");
        let ParsedRankExpr::Lexical(expression) = rank else {
            panic!("expected lexical expression");
        };
        let schema = json!({
            "title": {"type": "string", "full_text_search": {"enabled": false}},
            "body": {
                "type": "string",
                "full_text_search": {"bm25": {"k1": 1.9, "b": 0.2}, "tokenizer": "word_v1"}
            }
        });
        let plan = build_lexical_plan(
            &expression,
            &fixture_index_meta(),
            Some(&schema),
            PrefixExpansionLimits::default(),
        )
        .expect("plan");
        let terms = &plan.leaves[0].terms;
        assert!(
            terms.iter().all(|term| term.field_name == "body"),
            "title should be disabled by schema"
        );
        assert!(terms.iter().all(|term| (term.params.k1 - 1.9).abs() < 1e-6));
        assert!(terms.iter().all(|term| (term.params.b - 0.2).abs() < 1e-6));
    }

    #[test]
    fn field_config_parser_validates_supported_shapes() {
        let schema = json!({
            "body": {
                "full_text_search": {
                    "enabled": true,
                    "tokenizer": "word_v1",
                    "bm25": {"k1": 1.4, "b": 0.4}
                }
            }
        });
        let config = resolve_field_search_config(Some(&schema), "body").expect("config");
        assert!(config.enabled);
        assert!((config.bm25.k1 - 1.4).abs() < 1e-6);
        assert!((config.bm25.b - 0.4).abs() < 1e-6);
    }

    #[test]
    fn planner_expands_prefix_terms_with_expansion_and_byte_limits() {
        let rank = parse_rank_expr(&json!(["body", "BM25_PREFIX", "ru"])).expect("rank parse");
        let ParsedRankExpr::Lexical(expression) = rank else {
            panic!("expected lexical expression");
        };
        let plan = build_lexical_plan(
            &expression,
            &fixture_index_meta(),
            None,
            PrefixExpansionLimits {
                max_expansions: 1,
                max_expansion_bytes: 1024,
            },
        )
        .expect("plan");
        assert_eq!(plan.leaves.len(), 1);
        assert_eq!(plan.leaves[0].terms.len(), 1, "expansion must be capped");
        assert!(
            plan.leaves[0].terms[0].term.starts_with("ru"),
            "expanded term should retain prefix semantics"
        );
    }

    #[test]
    fn planner_collects_rank_by_filter_boosts_for_runtime_scoring() {
        let rank = parse_rank_expr(&json!([
            "rank_by_filter",
            ["topic", "Eq", "boosted"],
            ["text", "BM25", "rust"],
            2.5
        ]))
        .expect("rank parse");
        let ParsedRankExpr::Lexical(expression) = rank else {
            panic!("expected lexical expression");
        };
        let plan = build_lexical_plan(
            &expression,
            &fixture_index_meta(),
            None,
            PrefixExpansionLimits::default(),
        )
        .expect("plan");
        assert_eq!(plan.conditional_boosts.len(), 1);
        assert!((plan.conditional_boosts[0].boost - 2.5).abs() < 1e-6);
        assert_eq!(
            plan.conditional_boosts[0].filter,
            json!(["topic", "Eq", "boosted"])
        );
    }
}
