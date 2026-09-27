//! Audit-log writing for the control plane (RBAC, #205). One choke point both
//! the auth middleware (denials) and the run handlers (submit / cancel / delete)
//! funnel through, so every audit record is built the same way and a write
//! failure is logged — never silently dropped, and never fails the action.

use crate::serve::history::AuditEntry;
use crate::serve::rbac::AuthContext;
use crate::serve::state::ServerState;

/// Build + persist one audit record (best-effort). A backend write failure is
/// logged at WARN and swallowed: auditing must never fail the underlying action,
/// but the failure is made visible rather than lost.
pub async fn write(
    state: &ServerState,
    ctx: &AuthContext,
    action: &str,
    run_id: Option<String>,
    config_fingerprint: Option<String>,
    result: &str,
) {
    let entry = AuditEntry {
        id: uuid::Uuid::now_v7().to_string(),
        timestamp: chrono::Utc::now(),
        principal: ctx.principal.clone(),
        role: ctx.role.as_str().to_string(),
        action: action.to_string(),
        run_id,
        config_fingerprint,
        source_ip: ctx.source_ip.clone(),
        tenant: ctx.tenant.clone(),
        result: result.to_string(),
    };
    if let Err(e) = state.history().record_audit(&entry).await {
        tracing::warn!(
            action, principal = %ctx.principal, error = %e,
            "failed to write audit record"
        );
    }
}
