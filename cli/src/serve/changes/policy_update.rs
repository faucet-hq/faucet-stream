//! Planning and executing a data-flow policy change (#1043): the server's
//! server-managed policy (`policy_update`) or one tenant's add-only policy
//! (`tenant_policy_update`).
//!
//! The plan's material is the scope, the version in force and the draft, so
//! a request approved after another policy change landed is invalidated, and
//! the new version is written only if it is still the next one.

use super::{ChangeKind, ChangePlan, ChangeRequest, sha_hex};
use crate::serve::error::ServeError;
use crate::serve::history::policies::PolicyVersionRecord;
use crate::serve::policies::{self, Layer};
use crate::serve::rbac::AuthContext;
use crate::serve::state::ServerState;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// The payload of a policy change request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyPayload {
    /// The tenant, for a `tenant_policy_update`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
    /// The proposed policy document (YAML or JSON), as written.
    pub policy: String,
    /// The version the draft was edited from (`None`: there was none).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_version: Option<u32>,
    /// Set for a rollback: the earlier version this re-enacts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restores: Option<u32>,
}

impl PolicyPayload {
    /// The layer this payload changes, checked against `kind`.
    pub fn layer(&self, kind: ChangeKind) -> Result<Layer, ServeError> {
        match (kind, &self.tenant) {
            (ChangeKind::PolicyUpdate, None) => Ok(Layer::Server),
            (ChangeKind::TenantPolicyUpdate, Some(t)) => {
                crate::serve::history::tenants::validate_tenant_id(t)
                    .map_err(ServeError::BadConfig)?;
                Ok(Layer::Tenant(t.clone()))
            }
            (ChangeKind::PolicyUpdate, Some(_)) => Err(ServeError::BadConfig(
                "a policy_update changes the server policy; it names no tenant".into(),
            )),
            _ => Err(ServeError::BadConfig(
                "a tenant_policy_update names the tenant whose policy it changes".into(),
            )),
        }
    }
}

/// A draft's problems as a 422.
pub fn problems_error(problems: Vec<policies::Problem>) -> ServeError {
    ServeError::Unprocessable {
        message: format!(
            "the policy has {} problem(s): {}",
            problems.len(),
            problems
                .iter()
                .map(|p| match p.line {
                    Some(l) => format!("line {l}: {}", p.message),
                    None => p.message.clone(),
                })
                .collect::<Vec<_>>()
                .join("; ")
        ),
        details: Some(json!({ "problems": problems })),
    }
}

/// Refuse to change the server policy of a server that loads it from a file.
pub fn require_server_managed(state: &ServerState, layer: &Layer) -> Result<(), ServeError> {
    if *layer == Layer::Server && state.policy_file().is_some() {
        return Err(ServeError::Conflict(
            "this server's policy is managed by its --policy file; change the file and restart \
             the server to update it"
                .into(),
        ));
    }
    Ok(())
}

/// The refusal of a policy edit on a server whose history is in memory.
pub const MEMORY_HISTORY: &str = "set a persistent --history to edit the policy here: an approved change on the in-memory \
     history would be lost on restart";

/// Refuse to propose a policy change a restart would lose: with the
/// in-memory history backend nothing approved survives.
pub fn require_persistent(state: &ServerState) -> Result<(), ServeError> {
    if state.persistent_history() {
        Ok(())
    } else {
        Err(ServeError::Conflict(MEMORY_HISTORY.into()))
    }
}

/// Validate a policy change and compute its plan.
pub async fn plan(
    state: &ServerState,
    kind: ChangeKind,
    payload: &Value,
) -> Result<ChangePlan, ServeError> {
    let payload: PolicyPayload = serde_json::from_value(payload.clone())
        .map_err(|e| ServeError::BadConfig(format!("{} payload: {e}", kind.as_str())))?;
    let layer = payload.layer(kind)?;
    require_server_managed(state, &layer)?;
    let draft = policies::parse_draft(&payload.policy).map_err(problems_error)?;
    let problems = policies::check_draft(state, &layer, &draft, &payload.policy).await?;
    if !problems.is_empty() {
        return Err(problems_error(problems));
    }
    let scope = layer.scope();
    let current = policies::latest(state, &scope).await?;
    let current_version = current.as_ref().map(|r| r.version);
    let before = match &current {
        Some(r) => policies::canonical_yaml(&policies::spec_of(r)?),
        None => String::new(),
    };
    let after = policies::canonical_yaml(&draft);
    let next = current_version.unwrap_or(0) + 1;
    let rules = draft.rules.len();
    let classifications = draft.classifications.len();
    // Review material for the approver; not part of what approval pins.
    let impact = policies::draft_impact(state, &layer, draft).await?;
    Ok(ChangePlan {
        material: sha_hex(&format!(
            "{scope}\n{}\n{after}",
            current_version.map(|v| v.to_string()).unwrap_or_default()
        )),
        summary: json!({
            "scope": scope,
            "tenant": layer.tenant(),
            "current_version": current_version,
            "next_version": next,
            "restores": payload.restores,
            "rules": rules,
            "classifications": classifications,
            "diff": policies::line_diff(&before, &after),
            "impact": impact,
        }),
        rows: Vec::new(),
    })
}

/// The version this change already wrote, when an interrupted execution got
/// as far as storing it.
pub async fn already_applied(
    state: &ServerState,
    change: &ChangeRequest,
) -> Result<Option<u32>, ServeError> {
    let Ok(payload) = serde_json::from_value::<PolicyPayload>(change.payload.clone()) else {
        return Ok(None);
    };
    let layer = payload.layer(change.kind)?;
    Ok(policies::latest(state, &layer.scope())
        .await?
        .filter(|r| r.change_id.as_deref() == Some(change.id.as_str()))
        .map(|r| r.version))
}

/// Store the version an approved change puts in force. `fresh` is the plan
/// re-computed at approval (its material already matched); `approver`
/// completed the quorum. Returns the version written.
pub async fn execute(
    state: &ServerState,
    change: &ChangeRequest,
    fresh: &ChangePlan,
    approver: &AuthContext,
) -> Result<u32, ServeError> {
    let payload: PolicyPayload = serde_json::from_value(change.payload.clone())
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;
    let layer = payload.layer(change.kind)?;
    let draft = policies::parse_draft(&payload.policy).map_err(problems_error)?;
    let version = fresh
        .summary
        .get("next_version")
        .and_then(Value::as_u64)
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| ServeError::Internal("policy plan carries no next version".into()))?;
    // Another approved change may have landed since this one was planned:
    // the layers must still merge for every tenant, or this one is
    // invalidated rather than leaving a tenant whose runs all refuse (#1051).
    let problems = policies::check_draft(state, &layer, &draft, &payload.policy).await?;
    if !problems.is_empty() {
        return Err(ServeError::Conflict(format!(
            "the policy no longer merges with the versions in force: {}",
            problems
                .iter()
                .map(|p| p.message.clone())
                .collect::<Vec<_>>()
                .join("; ")
        )));
    }
    let record = PolicyVersionRecord {
        scope: layer.scope(),
        version,
        spec: serde_json::to_value(&draft).map_err(|e| ServeError::Internal(e.to_string()))?,
        created_at: chrono::Utc::now(),
        created_by: change.requester.clone(),
        approved_by: Some(approver.principal.clone()),
        reason: change.reason.clone(),
        change_id: Some(change.id.clone()),
        restores: payload.restores,
    };
    let stored = state
        .history()
        .policy_version_insert(&record)
        .await
        .map_err(ServeError::from)?;
    if !stored {
        return Err(ServeError::Conflict(format!(
            "another change stored {} policy version {version} first; propose this one again",
            layer.scope()
        )));
    }
    let mut actor = approver.clone();
    if actor.tenant.is_none() {
        actor.tenant = layer.tenant().map(str::to_string);
    }
    crate::serve::audit::write_target_detail(
        state,
        &actor,
        "policy.updated",
        format!("policy:{}@{version}", layer.scope()),
        json!({
            "version": version,
            "requested_by": change.requester,
            "approved_by": approver.principal,
            "change": change.id,
            "restores": payload.restores,
        })
        .to_string(),
        "ok",
    )
    .await;
    metrics::counter!(
        "faucet_serve_policy_versions_total",
        "layer" => if layer.tenant().is_some() { "tenant" } else { "server" }
    )
    .increment(1);
    if layer == Layer::Server {
        // The new version may now match (or no longer match) the --policy file.
        let _ = policies::file_drift(state).await;
    }
    Ok(version)
}

/// Whether `actor` may act on a policy change for `change_tenant`: a
/// tenant-scoped principal only on its own tenant's, never the server's.
pub fn approver_in_scope(actor: &AuthContext, change_tenant: Option<&str>) -> Result<(), String> {
    match (&actor.tenant, change_tenant) {
        (None, _) => Ok(()),
        (Some(own), Some(t)) if own == t => Ok(()),
        (Some(own), _) => Err(format!(
            "'{}' is confined to tenant {own}; only a server admin or an admin of the tenant \
             the change is for may approve it",
            actor.principal
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layer_checks_kind_against_tenant() {
        let p = |t: Option<&str>| PolicyPayload {
            tenant: t.map(str::to_string),
            policy: String::new(),
            base_version: None,
            restores: None,
        };
        assert_eq!(
            p(None).layer(ChangeKind::PolicyUpdate).unwrap(),
            Layer::Server
        );
        assert_eq!(
            p(Some("acme"))
                .layer(ChangeKind::TenantPolicyUpdate)
                .unwrap(),
            Layer::Tenant("acme".into())
        );
        assert!(p(Some("acme")).layer(ChangeKind::PolicyUpdate).is_err());
        assert!(p(None).layer(ChangeKind::TenantPolicyUpdate).is_err());
        assert!(
            p(Some("Bad Id"))
                .layer(ChangeKind::TenantPolicyUpdate)
                .is_err()
        );
    }

    #[test]
    fn approver_scope() {
        let ctx = |t: Option<&str>| AuthContext {
            principal: "a".into(),
            role: crate::serve::rbac::Role::Admin,
            source_ip: None,
            tenant: t.map(str::to_string),
        };
        assert!(approver_in_scope(&ctx(None), None).is_ok());
        assert!(approver_in_scope(&ctx(None), Some("acme")).is_ok());
        assert!(approver_in_scope(&ctx(Some("acme")), Some("acme")).is_ok());
        assert!(approver_in_scope(&ctx(Some("acme")), Some("globex")).is_err());
        assert!(approver_in_scope(&ctx(Some("acme")), None).is_err());
    }

    #[test]
    fn problems_render_with_lines() {
        let e = problems_error(vec![
            policies::Problem {
                line: Some(3),
                message: "bad".into(),
            },
            policies::Problem {
                line: None,
                message: "worse".into(),
            },
        ]);
        assert!(matches!(
            &e,
            ServeError::Unprocessable { message, details: Some(d) }
                if message.contains("line 3: bad; worse")
                    && d["problems"].as_array().map(Vec::len) == Some(2)
        ));
    }
}
