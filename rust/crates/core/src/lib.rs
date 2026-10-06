use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    Cosine,
    Dot,
    Euclidean,
}

#[derive(Debug, Error)]
pub enum TurboVectorError {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("conflict error: {0}")]
    Conflict(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, TurboVectorError>;
