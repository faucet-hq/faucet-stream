//! HTTP-facing error type. Every fallible serve handler returns `ServeError`,
//! which renders to a JSON `ApiError` body with the right status code.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// JSON error envelope: `{ "error": { "code": "...", "message": "..." } }`.
#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: ApiErrorBody,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorBody {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

/// All error outcomes a serve handler can produce.
#[derive(Debug)]
pub enum ServeError {
    Unauthorized,
    /// 403 — the server refuses to fulfil the request. Either the principal's
    /// role lacks the required permission, or the capability is switched off on
    /// this server (e.g. the local-output preview without
    /// `--preview-local-outputs`, #586) — in which case no role would help, and
    /// the message names the flag.
    Forbidden(String),
    NotFound,
    BadConfig(String),
    /// 422 — expand/validation failure or a failed `doctor_first` preflight;
    /// `details` carries the doctor report when present.
    Unprocessable {
        message: String,
        details: Option<serde_json::Value>,
    },
    /// 409 — delete on a running run, or idempotency key reused with a new payload.
    Conflict(String),
    /// 429 — the run queue is full.
    QueueFull {
        retry_after_secs: u64,
    },
    /// 429 — a per-tenant limit refused the request (#709).
    TooManyRequests(String),
    /// 503 — a required dependency is temporarily unavailable (e.g. idempotency
    /// can't be honored while the run-history backend is degraded).
    Unavailable(String),
    Internal(String),
}

impl ServeError {
    pub fn status(&self) -> StatusCode {
        match self {
            ServeError::Unauthorized => StatusCode::UNAUTHORIZED,
            ServeError::Forbidden(_) => StatusCode::FORBIDDEN,
            ServeError::NotFound => StatusCode::NOT_FOUND,
            ServeError::BadConfig(_) => StatusCode::BAD_REQUEST,
            ServeError::Unprocessable { .. } => StatusCode::UNPROCESSABLE_ENTITY,
            ServeError::Conflict(_) => StatusCode::CONFLICT,
            ServeError::QueueFull { .. } => StatusCode::TOO_MANY_REQUESTS,
            ServeError::TooManyRequests(_) => StatusCode::TOO_MANY_REQUESTS,
            ServeError::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            ServeError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn code(&self) -> &'static str {
        match self {
            ServeError::Unauthorized => "unauthorized",
            ServeError::Forbidden(_) => "forbidden",
            ServeError::NotFound => "not_found",
            ServeError::BadConfig(_) => "bad_config",
            ServeError::Unprocessable { .. } => "unprocessable",
            ServeError::Conflict(_) => "conflict",
            ServeError::QueueFull { .. } => "queue_full",
            ServeError::TooManyRequests(_) => "limit_exceeded",
            ServeError::Unavailable(_) => "unavailable",
            ServeError::Internal(_) => "internal",
        }
    }

    fn message(&self) -> String {
        match self {
            ServeError::Unauthorized => "missing or invalid bearer token".into(),
            ServeError::Forbidden(m) => m.clone(),
            ServeError::NotFound => "not found".into(),
            ServeError::BadConfig(m) => m.clone(),
            ServeError::Unprocessable { message, .. } => message.clone(),
            ServeError::Conflict(m) => m.clone(),
            ServeError::QueueFull { .. } => "run queue is full; retry later".into(),
            ServeError::TooManyRequests(m) => m.clone(),
            ServeError::Unavailable(m) => m.clone(),
            ServeError::Internal(m) => m.clone(),
        }
    }

    fn details(&self) -> Option<serde_json::Value> {
        match self {
            ServeError::Unprocessable { details, .. } => details.clone(),
            _ => None,
        }
    }

    pub fn api_error(&self) -> ApiError {
        // Scrub any resolved secret that reached the message or details.
        let message = crate::secrets::registry::redact(&self.message()).into_owned();
        let details = self.details().map(|d| {
            let scrubbed = crate::secrets::registry::redact(&d.to_string()).into_owned();
            serde_json::from_str(&scrubbed)
                .unwrap_or_else(|_| serde_json::json!({ "redacted": true }))
        });
        ApiError {
            error: ApiErrorBody {
                code: self.code().to_string(),
                message,
                details,
            },
        }
    }
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl IntoResponse for ServeError {
    fn into_response(self) -> Response {
        let status = self.status();
        let mut resp = (status, Json(self.api_error())).into_response();
        if let ServeError::QueueFull { retry_after_secs } = &self
            && let Ok(v) = axum::http::HeaderValue::from_str(&retry_after_secs.to_string())
        {
            resp.headers_mut()
                .insert(axum::http::header::RETRY_AFTER, v);
        }
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[test]
    fn maps_variants_to_status_codes() {
        assert_eq!(ServeError::Unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(ServeError::NotFound.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            ServeError::BadConfig("nope".into()).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServeError::Internal("boom".into()).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn body_carries_code_and_message() {
        let body = ServeError::NotFound.api_error();
        assert_eq!(body.error.code, "not_found");
        assert!(!body.error.message.is_empty());

        // Fixed-message variant carries the expected static text.
        let body = ServeError::Unauthorized.api_error();
        assert_eq!(body.error.code, "unauthorized");
        assert_eq!(body.error.message, "missing or invalid bearer token");
    }

    #[test]
    fn dynamic_message_variants_round_trip_to_body() {
        // BadConfig / Internal carry caller-supplied text — confirm it reaches
        // the body intact (the redaction pass leaves non-secret text unchanged).
        let body = ServeError::BadConfig("bad thing".into()).api_error();
        assert_eq!(body.error.code, "bad_config");
        assert_eq!(body.error.message, "bad thing");

        let body = ServeError::Internal("boom".into()).api_error();
        assert_eq!(body.error.code, "internal");
        assert_eq!(body.error.message, "boom");
    }

    #[test]
    fn new_variants_map_to_status_codes() {
        assert_eq!(
            ServeError::Unprocessable {
                message: "x".into(),
                details: None
            }
            .status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            ServeError::Conflict("x".into()).status(),
            StatusCode::CONFLICT
        );
        let limit = ServeError::TooManyRequests("tenant acme at its limit".into());
        assert_eq!(limit.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(limit.api_error().error.code, "limit_exceeded");
        assert_eq!(limit.api_error().error.message, "tenant acme at its limit");
        assert_eq!(
            ServeError::QueueFull {
                retry_after_secs: 5
            }
            .status(),
            StatusCode::TOO_MANY_REQUESTS
        );
    }

    #[test]
    fn unprocessable_carries_details() {
        let body = ServeError::Unprocessable {
            message: "doctor failed".into(),
            details: Some(serde_json::json!({"invocations": []})),
        }
        .api_error();
        assert_eq!(body.error.code, "unprocessable");
        assert!(body.error.details.is_some());
    }

    #[test]
    fn phase1_variants_omit_details_on_the_wire() {
        // details uses skip_serializing_if=Option::is_none, so non-422 variants
        // must not emit a "details" key (backward-compatible wire shape).
        let body = ServeError::NotFound.api_error();
        let v = serde_json::to_value(&body).unwrap();
        assert!(v["error"].get("details").is_none());
    }

    #[tokio::test]
    async fn queue_full_sets_retry_after_header() {
        let resp = ServeError::QueueFull {
            retry_after_secs: 7,
        }
        .into_response();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            resp.headers().get(axum::http::header::RETRY_AFTER).unwrap(),
            "7"
        );
    }

    #[test]
    fn display_is_code_then_message() {
        let e = ServeError::Conflict("busy".into());
        assert_eq!(e.to_string(), format!("{}: {}", e.code(), e.message()));
    }
}
