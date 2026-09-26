//! Bearer-token authentication + RBAC authorization for `/v1/*` (#205).
//! Constant-time comparison via `subtle`; the `Authorization` header is the only
//! accepted credential. The bearer token resolves (via
//! [`AuthMode::resolve`](crate::serve::config::AuthMode::resolve)) to an
//! [`AuthContext`](crate::serve::rbac::AuthContext), the request's matched route declares the required
//! [`Permission`](crate::serve::rbac::Permission), and a role that lacks it is
//! denied (`403`) with an audit record.

use crate::serve::audit;
use crate::serve::error::ServeError;
use crate::serve::rbac::{self, Role};
use crate::serve::state::ServerState;
use axum::extract::{ConnectInfo, MatchedPath, Request, State};
use axum::middleware::Next;
use axum::response::Response;
use std::net::SocketAddr;
use subtle::ConstantTimeEq;

/// Timing-safe byte-slice equality. Differing lengths return `false` after a
/// constant-time length check (subtle short-circuits unequal lengths — this
/// leaks only the token *length*, never its content); equal-length inputs are
/// compared byte-by-byte with no early exit on the first mismatch.
pub(crate) fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

/// Validate a raw `Authorization` header value against the expected token.
pub fn authorize_header(header: Option<&str>, expected: &str) -> Result<(), ServeError> {
    let value = header.ok_or(ServeError::Unauthorized)?;
    let token = value
        .strip_prefix("Bearer ")
        .ok_or(ServeError::Unauthorized)?;
    if constant_time_eq(token.as_bytes(), expected.as_bytes()) {
        Ok(())
    } else {
        Err(ServeError::Unauthorized)
    }
}

/// Extract the raw bearer token from an `Authorization: Bearer <token>` header.
fn bearer_token(headers: &axum::http::HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}

/// Best-effort `run_id` from a `/v1/runs/{id}[/…]` path (for denial audit
/// attribution). `None` for non-run routes.
fn extract_run_id(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/v1/runs/")?;
    let id = rest.split('/').next()?;
    (!id.is_empty()).then(|| id.to_string())
}

/// Axum middleware enforcing bearer auth + RBAC on `/v1/*`. Under `--no-auth`
/// every request resolves to an implicit `anonymous` admin (all permitted), so
/// the authz path is uniform. CORS preflight (`OPTIONS`) is allowed through so
/// browsers (which omit `Authorization` on preflight) work behind a CORS policy.
///
/// On success the resolved [`AuthContext`](crate::serve::rbac::AuthContext) is
/// inserted into the request extensions for handlers (and the audit writer). A
/// principal whose role lacks the route's required permission gets a `403` and a
/// `denied` audit record.
pub async fn require_auth(
    State(state): State<ServerState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ServeError> {
    if req.method() == axum::http::Method::OPTIONS {
        return Ok(next.run(req).await);
    }

    let bearer = bearer_token(req.headers());
    let mut ctx = state
        .auth_mode()
        .resolve(bearer)
        .ok_or(ServeError::Unauthorized)?;
    // Best-effort source IP (present when the server is served with connect-info;
    // absent when a handler is called directly in tests).
    ctx.source_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip().to_string());

    let method = req.method().clone();
    let matched = req
        .extensions()
        .get::<MatchedPath>()
        .map(|m| m.as_str().to_string());

    // A mapped route requires its permission; an unmapped `/v1` route is
    // admin-only (fail closed for any endpoint added without an explicit entry).
    let allowed = match matched.as_deref() {
        Some(mp) => match rbac::required_permission(&method, mp) {
            Some(perm) => ctx.role.grants(perm),
            None => ctx.role == Role::Admin,
        },
        None => ctx.role == Role::Admin,
    };

    // A tenant-scoped principal (#709) reaches only its own tenant's routes
    // and the tenant-filtered reads; another tenant's route is a 404.
    if allowed && let Some(scoped) = ctx.tenant.clone() {
        match matched.as_deref().map(|mp| {
            rbac::tenant_scope_decision(&method, mp, req.uri().path(), &scoped)
        }) {
            Some(rbac::TenantScopeDecision::Allow) => {}
            Some(rbac::TenantScopeDecision::NotFound) => return Err(ServeError::NotFound),
            Some(rbac::TenantScopeDecision::Deny) | None => {
                let action = matched
                    .as_deref()
                    .map(|mp| rbac::audit_action(&method, mp))
                    .unwrap_or("unknown");
                audit::write(&state, &ctx, action, None, None, "denied").await;
                return Err(ServeError::Forbidden(format!(
                    "principal '{}' is confined to tenant '{scoped}' and may not perform \
                     this action",
                    ctx.principal
                )));
            }
        }
    }

    if !allowed {
        let action = matched
            .as_deref()
            .map(|mp| rbac::audit_action(&method, mp))
            .unwrap_or("unknown");
        let run_id = extract_run_id(req.uri().path());
        tracing::warn!(
            principal = %ctx.principal, role = ctx.role.as_str(), action,
            "RBAC denied a control-plane action"
        );
        audit::write(&state, &ctx, action, run_id, None, "denied").await;
        return Err(ServeError::Forbidden(format!(
            "principal '{}' (role {}) is not permitted to perform this action",
            ctx.principal,
            ctx.role.as_str()
        )));
    }

    req.extensions_mut().insert(ctx);
    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches_only_identical() {
        assert!(constant_time_eq(b"abc123", b"abc123"));
        assert!(!constant_time_eq(b"abc123", b"abc124"));
        assert!(!constant_time_eq(b"abc", b"abc123")); // length differs
    }

    #[test]
    fn authorize_accepts_correct_bearer() {
        assert!(authorize_header(Some("Bearer s3cret"), "s3cret").is_ok());
    }

    #[test]
    fn authorize_rejects_wrong_or_missing() {
        assert!(authorize_header(Some("Bearer nope"), "s3cret").is_err());
        assert!(authorize_header(None, "s3cret").is_err());
        assert!(authorize_header(Some("s3cret"), "s3cret").is_err()); // no "Bearer " prefix
        assert!(authorize_header(Some("Basic s3cret"), "s3cret").is_err());
    }
}
