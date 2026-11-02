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
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;

/// Failed attempts one source address may make in [`FAILURE_WINDOW`] before
/// its requests are refused (429) until the window ends.
pub const MAX_FAILURES: u32 = 20;
/// The window failed attempts are counted over.
pub const FAILURE_WINDOW: Duration = Duration::from_secs(60);
/// Source addresses tracked at once; expired windows are dropped first.
const MAX_TRACKED: usize = 10_000;
/// `auth.failed` audit records written per window across all addresses.
const MAX_AUDITS_PER_WINDOW: u32 = 60;
/// A token shorter than this draws a startup warning.
pub const MIN_TOKEN_LEN: usize = 16;

#[derive(Debug, Clone, Copy)]
struct Window {
    start: Instant,
    failures: u32,
}

impl Window {
    fn open(&self, now: Instant) -> bool {
        now.duration_since(self.start) < FAILURE_WINDOW
    }
}

/// Failed bearer-token attempts per source address, counted over a fixed
/// window.
#[derive(Debug, Default)]
pub struct FailedAuth {
    by_source: Mutex<HashMap<String, Window>>,
    audits: Mutex<Option<Window>>,
}

/// What recording one failed attempt decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failure {
    /// The attempt is worth an audit record: the first of its window, or the
    /// one that reached the limit.
    pub audit: bool,
}

impl FailedAuth {
    /// Whether `source` is refused until its window ends.
    pub fn locked(&self, source: &str, now: Instant) -> bool {
        let map = self.by_source.lock().unwrap_or_else(|e| e.into_inner());
        map.get(source)
            .is_some_and(|w| w.open(now) && w.failures >= MAX_FAILURES)
    }

    /// Count a failed attempt from `source`.
    pub fn record(&self, source: &str, now: Instant) -> Failure {
        let mut map = self.by_source.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() >= MAX_TRACKED && !map.contains_key(source) {
            map.retain(|_, w| w.open(now));
            if map.len() >= MAX_TRACKED {
                map.clear();
            }
        }
        let w = map.entry(source.to_string()).or_insert(Window {
            start: now,
            failures: 0,
        });
        if !w.open(now) {
            *w = Window {
                start: now,
                failures: 0,
            };
        }
        w.failures = w.failures.saturating_add(1);
        let notable = w.failures == 1 || w.failures == MAX_FAILURES;
        drop(map);
        Failure {
            audit: notable && self.audit_budget(now),
        }
    }

    fn audit_budget(&self, now: Instant) -> bool {
        let mut slot = self.audits.lock().unwrap_or_else(|e| e.into_inner());
        let w = match slot.as_mut() {
            Some(w) if w.open(now) => w,
            _ => slot.insert(Window {
                start: now,
                failures: 0,
            }),
        };
        if w.failures >= MAX_AUDITS_PER_WINDOW {
            return false;
        }
        w.failures += 1;
        true
    }
}

/// The configured bearer tokens shorter than [`MIN_TOKEN_LEN`].
pub fn short_tokens(mode: &crate::serve::config::AuthMode) -> usize {
    use crate::serve::config::AuthMode;
    match mode {
        AuthMode::Token(t) => usize::from(t.len() < MIN_TOKEN_LEN),
        AuthMode::Rbac(cfg) => cfg.tokens().filter(|t| t.len() < MIN_TOKEN_LEN).count(),
        AuthMode::None => 0,
    }
}

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
    // Best-effort source IP (present when the server is served with connect-info;
    // absent when a handler is called directly in tests).
    let source_ip = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip().to_string());
    let source = source_ip.as_deref().unwrap_or("unknown");
    let presented = bearer.is_some_and(|t| !t.is_empty());
    if presented && state.failed_auth().locked(source, Instant::now()) {
        return Err(ServeError::TooManyRequests(
            "too many failed authentication attempts from this address; retry in a minute".into(),
        ));
    }
    let Some(mut ctx) = state.auth_mode().resolve(bearer) else {
        if presented && state.failed_auth().record(source, Instant::now()).audit {
            tracing::warn!(source = %source, "a request presented an invalid bearer token");
            let who = rbac::AuthContext {
                principal: "unauthenticated".into(),
                role: Role::Viewer,
                source_ip: source_ip.clone(),
                tenant: None,
            };
            audit::write(&state, &who, "auth.failed", None, None, "denied").await;
        }
        return Err(ServeError::Unauthorized);
    };
    ctx.source_ip = source_ip;

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
        match matched
            .as_deref()
            .map(|mp| rbac::tenant_scope_decision(&method, mp, req.uri().path(), &scoped))
        {
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
    fn failed_attempts_lock_a_source_for_the_window() {
        let f = FailedAuth::default();
        let t0 = Instant::now();
        assert!(
            f.record("10.0.0.1", t0).audit,
            "the first failure is audited"
        );
        for _ in 1..MAX_FAILURES - 1 {
            assert!(!f.record("10.0.0.1", t0).audit);
        }
        assert!(!f.locked("10.0.0.1", t0));
        assert!(
            f.record("10.0.0.1", t0).audit,
            "reaching the limit is audited"
        );
        assert!(f.locked("10.0.0.1", t0));
        assert!(!f.locked("10.0.0.2", t0), "other sources are unaffected");
        let later = t0 + FAILURE_WINDOW;
        assert!(!f.locked("10.0.0.1", later));
        assert!(
            f.record("10.0.0.1", later).audit,
            "a new window starts over"
        );
    }

    #[test]
    fn failure_audits_are_bounded_across_sources() {
        let f = FailedAuth::default();
        let t0 = Instant::now();
        let audited = (0..MAX_AUDITS_PER_WINDOW + 10)
            .filter(|i| f.record(&format!("10.1.0.{i}"), t0).audit)
            .count();
        assert_eq!(audited as u32, MAX_AUDITS_PER_WINDOW);
        assert!(f.record("10.9.9.9", t0 + FAILURE_WINDOW).audit);
    }

    #[test]
    fn short_tokens_are_counted() {
        use crate::serve::config::AuthMode;
        assert_eq!(short_tokens(&AuthMode::Token("short".into())), 1);
        assert_eq!(short_tokens(&AuthMode::Token("x".repeat(MIN_TOKEN_LEN))), 0);
        assert_eq!(short_tokens(&AuthMode::None), 0);
    }

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
