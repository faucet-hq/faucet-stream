//! Tenant records (#709) — the stored side of multi-tenant embedded
//! integrations: tenants, their sealed connections, pending hosted-OAuth
//! connect sessions, which runs belong to which tenant, and the ledger of
//! state keys a tenant's runs used (so deleting a tenant can delete exactly
//! those keys).
//!
//! Credentials never reach this module in the clear. A connection's provider
//! config and a connect session's PKCE verifier are sealed by the server's
//! vault key before they are handed to storage; this module only carries the
//! ciphertext.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Longest tenant id. Matches `^[a-z0-9][a-z0-9_-]{0,62}$`.
pub const MAX_TENANT_ID_LEN: usize = 63;

/// Longest connection name. Same alphabet as a tenant id.
pub const MAX_CONNECTION_NAME_LEN: usize = 63;

/// How long a hosted-OAuth connect session stays valid.
pub const CONNECT_SESSION_TTL_SECS: i64 = 600;

/// Validate a tenant id or connection name: a lowercase slug of at most `max`
/// characters, starting with a letter or digit. `what` names it in the error.
pub fn validate_slug(s: &str, what: &str, max: usize) -> Result<(), String> {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return Err(format!("{what} must not be empty"));
    };
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return Err(format!(
            "{what} '{s}' must start with a lowercase letter or a digit"
        ));
    }
    if s.len() > max {
        return Err(format!("{what} '{s}' is longer than {max} characters"));
    }
    if let Some(bad) = s
        .chars()
        .find(|c| !(c.is_ascii_lowercase() || c.is_ascii_digit() || *c == '_' || *c == '-'))
    {
        return Err(format!(
            "{what} '{s}' contains '{bad}'; use lowercase letters, digits, '_' and '-'"
        ));
    }
    Ok(())
}

/// Validate a tenant id.
pub fn validate_tenant_id(id: &str) -> Result<(), String> {
    validate_slug(id, "tenant id", MAX_TENANT_ID_LEN)
}

/// Validate a connection name.
pub fn validate_connection_name(name: &str) -> Result<(), String> {
    validate_slug(name, "connection name", MAX_CONNECTION_NAME_LEN)
}

/// Per-tenant ceilings. The concurrency limit is enforced when a run is
/// submitted; the rest become a budget merged into every run for the tenant.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TenantLimits {
    /// Most runs for this tenant that may be queued or running at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrent_runs: Option<u32>,
    /// Most records one invocation may write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_records_per_run: Option<u64>,
    /// Most estimated bytes one invocation may write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes_per_run: Option<u64>,
    /// Longest one invocation may run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_duration_secs: Option<u64>,
}

impl TenantLimits {
    /// Reject zero ceilings: a zero limit would refuse every run, which is
    /// what suspending the tenant is for.
    pub fn validate(&self) -> Result<(), String> {
        let zero = |name: &str| Err(format!("limits.{name} must be greater than 0"));
        if self.max_concurrent_runs == Some(0) {
            return zero("max_concurrent_runs");
        }
        if self.max_records_per_run == Some(0) {
            return zero("max_records_per_run");
        }
        if self.max_bytes_per_run == Some(0) {
            return zero("max_bytes_per_run");
        }
        if self.max_duration_secs == Some(0) {
            return zero("max_duration_secs");
        }
        Ok(())
    }

    /// The per-run ceilings as a budget, or `None` when none is set.
    pub fn budget(&self) -> Option<faucet_core::BudgetSpec> {
        if self.max_records_per_run.is_none()
            && self.max_bytes_per_run.is_none()
            && self.max_duration_secs.is_none()
        {
            return None;
        }
        Some(faucet_core::BudgetSpec {
            max_records: self.max_records_per_run,
            max_bytes: self.max_bytes_per_run,
            max_duration_secs: self.max_duration_secs,
            allowed_sinks: Vec::new(),
        })
    }
}

/// One tenant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TenantRecord {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub limits: TenantLimits,
    /// Notification rules for tenant-level events, in the shape a config's
    /// `notifications:` list takes. Kept as JSON here so storage does not
    /// depend on the `notify` feature; validated when the tenant is written.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notifications: Vec<serde_json::Value>,
    /// A suspended tenant's runs are refused until it is resumed.
    #[serde(default)]
    pub suspended: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: String,
}

/// Whether a connection can be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatus {
    #[default]
    Active,
    /// The provider revoked the grant; runs referencing the connection are
    /// refused until the tenant reconnects.
    NeedsReauth,
}

impl ConnectionStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            ConnectionStatus::Active => "active",
            ConnectionStatus::NeedsReauth => "needs_reauth",
        }
    }
}

/// One stored connection: a provider spec sealed under the vault key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionRecord {
    pub tenant: String,
    pub name: String,
    /// The provider `type` (`static`, `oauth2`, `oauth2_refresh`,
    /// `token_endpoint`, `flow`, …). Stored in the clear so listings can show
    /// it without unsealing.
    pub provider_type: String,
    /// The hosted-OAuth provider that created it, when it came from a connect
    /// flow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_provider: Option<String>,
    /// The sealed `{type, config}` provider spec.
    pub sealed: String,
    #[serde(default)]
    pub status: ConnectionStatus,
    /// Why the connection needs re-authorization.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reauth_reason: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub updated_by: String,
}

/// A pending hosted-OAuth connect flow. Single-use: taking it deletes it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectSession {
    /// The OAuth `state` parameter — the session's key and its credential.
    pub state: String,
    pub tenant: String,
    pub provider: String,
    pub connection: String,
    /// Where the browser goes when the flow finishes.
    pub redirect: String,
    /// The sealed PKCE code verifier.
    pub sealed_verifier: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// One state key a tenant run used, with the (possibly sealed) store spec
/// needed to delete it later. `spec` is `None` when the server had no way to
/// store the spec safely; deleting the tenant then reports the key instead of
/// deleting it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantStateRef {
    pub tenant: String,
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<String>,
}

/// Which tenants a fan-out or a scheduled trigger targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum TenantSelector {
    /// `"all"` — every tenant that is not suspended.
    All(String),
    /// Named tenants.
    Named(Vec<String>),
}

impl TenantSelector {
    /// Validate the selector's shape.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            TenantSelector::All(s) if s == "all" => Ok(()),
            TenantSelector::All(s) => Err(format!(
                "tenants must be \"all\" or a list of tenant ids (got \"{s}\")"
            )),
            TenantSelector::Named(ids) if ids.is_empty() => {
                Err("tenants must name at least one tenant".into())
            }
            TenantSelector::Named(ids) => ids.iter().try_for_each(|id| validate_tenant_id(id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_shapes_validate() {
        let all: TenantSelector = serde_json::from_value(serde_json::json!("all")).unwrap();
        assert!(all.validate().is_ok());
        let named: TenantSelector = serde_json::from_value(serde_json::json!(["a", "b"])).unwrap();
        assert!(named.validate().is_ok());
        assert!(
            TenantSelector::All("every".into())
                .validate()
                .unwrap_err()
                .contains("\"all\"")
        );
        assert!(
            TenantSelector::Named(vec![])
                .validate()
                .unwrap_err()
                .contains("at least one")
        );
        assert!(TenantSelector::Named(vec!["Bad".into()]).validate().is_err());
    }

    #[test]
    fn tenant_ids_are_slugs() {
        assert!(validate_tenant_id("acme").is_ok());
        assert!(validate_tenant_id("acme-co_2").is_ok());
        assert!(validate_tenant_id("9lives").is_ok());
        assert!(validate_tenant_id(&"a".repeat(MAX_TENANT_ID_LEN)).is_ok());
        assert!(
            validate_tenant_id(&"a".repeat(MAX_TENANT_ID_LEN + 1))
                .unwrap_err()
                .contains("longer than")
        );
        assert!(validate_tenant_id("").unwrap_err().contains("empty"));
        assert!(validate_tenant_id("-x").unwrap_err().contains("start with"));
        assert!(validate_tenant_id("Acme").unwrap_err().contains("start with"));
        assert!(validate_tenant_id("a.b").unwrap_err().contains("'.'"));
        assert!(
            validate_connection_name("a/b")
                .unwrap_err()
                .starts_with("connection name")
        );
    }

    #[test]
    fn limits_validate_and_become_a_budget() {
        assert!(TenantLimits::default().validate().is_ok());
        assert_eq!(TenantLimits::default().budget(), None);
        for (limits, field) in [
            (
                TenantLimits {
                    max_concurrent_runs: Some(0),
                    ..Default::default()
                },
                "max_concurrent_runs",
            ),
            (
                TenantLimits {
                    max_records_per_run: Some(0),
                    ..Default::default()
                },
                "max_records_per_run",
            ),
            (
                TenantLimits {
                    max_bytes_per_run: Some(0),
                    ..Default::default()
                },
                "max_bytes_per_run",
            ),
            (
                TenantLimits {
                    max_duration_secs: Some(0),
                    ..Default::default()
                },
                "max_duration_secs",
            ),
        ] {
            assert!(limits.validate().unwrap_err().contains(field));
        }
        let only_concurrency = TenantLimits {
            max_concurrent_runs: Some(2),
            ..Default::default()
        };
        assert_eq!(only_concurrency.budget(), None);
        let b = TenantLimits {
            max_records_per_run: Some(10),
            max_bytes_per_run: Some(20),
            max_duration_secs: Some(30),
            ..Default::default()
        }
        .budget()
        .unwrap();
        assert_eq!(
            (b.max_records, b.max_bytes, b.max_duration_secs),
            (Some(10), Some(20), Some(30))
        );
        assert!(b.allowed_sinks.is_empty());
    }

    #[test]
    fn connection_status_strings() {
        assert_eq!(ConnectionStatus::Active.as_str(), "active");
        assert_eq!(ConnectionStatus::NeedsReauth.as_str(), "needs_reauth");
        assert_eq!(ConnectionStatus::default(), ConnectionStatus::Active);
        assert_eq!(
            serde_json::to_value(ConnectionStatus::NeedsReauth).unwrap(),
            "needs_reauth"
        );
    }
}
