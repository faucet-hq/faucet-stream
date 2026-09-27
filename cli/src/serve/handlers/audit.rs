//! `GET /v1/audit` — read the control-plane audit log (RBAC, #205). The route
//! requires the `AuditRead` permission (admin-only), enforced by the auth
//! middleware, so reaching this handler already means the caller is authorized.

use crate::serve::error::ServeError;
use crate::serve::history::{AuditEntry, AuditFilter};
use crate::serve::state::ServerState;
use axum::Json;
use axum::extract::{Query, State};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;

/// `GET /v1/audit` query string.
#[derive(Debug, Deserialize)]
pub struct AuditQuery {
    pub principal: Option<String>,
    pub action: Option<String>,
    /// Only actions taken for this tenant (#709).
    pub tenant: Option<String>,
    pub since: Option<String>,
    pub until: Option<String>,
    pub limit: Option<usize>,
}

/// `GET /v1/audit` response body.
#[derive(Debug, Serialize)]
pub struct AuditListResponse {
    pub entries: Vec<AuditEntry>,
}

/// Parse an RFC3339 query timestamp, restoring the `+` that form-encoding turns
/// into a space (so explicit offsets like `+05:30` survive).
fn parse_ts(raw: &str, field: &str) -> Result<DateTime<Utc>, ServeError> {
    DateTime::parse_from_rfc3339(&raw.replace(' ', "+"))
        .map(|d| d.to_utc())
        .map_err(|e| ServeError::BadConfig(format!("invalid `{field}` timestamp: {e}")))
}

/// `GET /v1/audit` → 200.
pub async fn list_audit(
    State(state): State<ServerState>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<AuditListResponse>, ServeError> {
    let filter = AuditFilter {
        principal: query.principal,
        tenant: query.tenant,
        action: query.action,
        since: query
            .since
            .as_deref()
            .map(|s| parse_ts(s, "since"))
            .transpose()?,
        until: query
            .until
            .as_deref()
            .map(|s| parse_ts(s, "until"))
            .transpose()?,
        limit: query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT),
    };
    let entries = state
        .history()
        .list_audit(&filter)
        .await
        .map_err(|e| ServeError::Internal(e.to_string()))?;
    Ok(Json(AuditListResponse { entries }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ts_accepts_z_and_offset() {
        assert!(parse_ts("2026-01-01T00:00:00Z", "since").is_ok());
        // Space in place of '+' (form-encoded offset) is restored.
        assert!(parse_ts("2026-01-01T00:00:00 05:30", "since").is_ok());
        assert!(parse_ts("not-a-date", "since").is_err());
    }
}
