use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use turbo_vector_core::TurboVectorError;
use utoipa::ToSchema;

#[derive(Debug)]
pub(crate) struct ApiError {
    pub(crate) status: StatusCode,
    code: &'static str,
    message: String,
    retryable: bool,
    details: Option<serde_json::Value>,
}

impl ApiError {
    pub(crate) fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "INVALID_ARGUMENT",
            message: message.into(),
            retryable: false,
            details: None,
        }
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NOT_FOUND",
            message: message.into(),
            retryable: false,
            details: None,
        }
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "CONFLICT",
            message: message.into(),
            retryable: false,
            details: None,
        }
    }

    pub(crate) fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "INTERNAL",
            message: message.into(),
            retryable: false,
            details: None,
        }
    }

    pub(crate) fn store_unavailable(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "STORE_UNAVAILABLE",
            message: message.into(),
            retryable: true,
            details: None,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ErrorEnvelope {
    pub(crate) error: ErrorBody,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct ErrorBody {
    pub(crate) code: &'static str,
    pub(crate) message: String,
    pub(crate) retryable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) details: Option<serde_json::Value>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorEnvelope {
            error: ErrorBody {
                code: self.code,
                message: self.message,
                retryable: self.retryable,
                details: self.details,
            },
        };
        (self.status, Json(body)).into_response()
    }
}

pub(crate) fn map_store_error(err: TurboVectorError) -> ApiError {
    match err {
        TurboVectorError::Validation(m) => ApiError::invalid_argument(m),
        TurboVectorError::Conflict(m) => ApiError::conflict(m),
        TurboVectorError::NotFound(m) => ApiError::not_found(m),
        TurboVectorError::Storage(m) => ApiError::store_unavailable(m),
        TurboVectorError::Internal(m) => ApiError::internal(m),
    }
}
