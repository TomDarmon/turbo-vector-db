use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::ApiError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TokenizerKind {
    WordV1,
}

impl TokenizerKind {
    pub(crate) fn from_schema_value(raw: Option<&Value>) -> Result<Self, ApiError> {
        let Some(raw) = raw else {
            return Ok(Self::WordV1);
        };
        let tokenizer = raw.as_str().ok_or_else(|| {
            ApiError::invalid_argument("full_text_search.tokenizer must be a string")
        })?;
        match tokenizer.trim().to_ascii_lowercase().as_str() {
            "word_v1" | "word-v1" | "wordv1" | "word" => Ok(Self::WordV1),
            other => Err(ApiError::invalid_argument(format!(
                "unsupported tokenizer '{other}'",
            ))),
        }
    }

    pub(crate) fn tokenize(self, text: &str) -> Vec<String> {
        let _ = self;
        tokenize_unicode_word_v1(text)
    }
}

pub(crate) fn tokenize_query_text(query: &str, tokenizer: TokenizerKind) -> Vec<String> {
    tokenizer.tokenize(query)
}

fn tokenize_unicode_word_v1(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        if ch.is_alphanumeric() {
            for lower in ch.to_lowercase() {
                current.push(lower);
            }
            continue;
        }
        if !current.is_empty() {
            out.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{tokenize_query_text, TokenizerKind};

    #[test]
    fn query_tokenization_is_deterministic() {
        let tokens = tokenize_query_text("Rust, BM25! rust?", TokenizerKind::WordV1);
        assert_eq!(tokens, vec!["rust", "bm25", "rust"]);
    }

    #[test]
    fn word_v1_tokenization_handles_multilingual_and_punctuation_text() {
        let tokens = tokenize_query_text(
            "¡Hola, señor! Привет-мир 你好，世界 Rust—2026",
            TokenizerKind::WordV1,
        );
        assert_eq!(
            tokens,
            vec![
                "hola",
                "señor",
                "привет",
                "мир",
                "你好",
                "世界",
                "rust",
                "2026"
            ]
        );
    }

    #[test]
    fn word_v1_tokenization_is_explicitly_versioned_in_schema() {
        let parsed = TokenizerKind::from_schema_value(Some(&json!("word_v1")))
            .expect("word_v1 should parse");
        assert_eq!(parsed, TokenizerKind::WordV1);
    }

    #[test]
    fn unsupported_legacy_tokenizer_versions_are_rejected() {
        let err = TokenizerKind::from_schema_value(Some(&json!("word_v3")))
            .expect_err("legacy tokenizer should be rejected");
        assert!(
            format!("{err:?}").contains("unsupported tokenizer"),
            "unexpected error: {err:?}"
        );
    }
}
