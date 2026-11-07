//! Stored data-flow policy versions (#1043): the server's own policy when it
//! is managed by the server (no `--policy` file), and each tenant's add-only
//! policy. Append-only per scope; the newest version of a scope is in force.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The scope of the server-wide policy.
pub const SERVER_SCOPE: &str = "server";

/// The scope a `--policy` file's canonical policy is stored under when a
/// run is admitted under it (#1051): content-addressed, version 1.
pub fn file_scope(sha256: &str) -> String {
    format!("file:{sha256}")
}

/// The scope of `tenant`'s policy.
pub fn tenant_scope(tenant: &str) -> String {
    format!("tenant:{tenant}")
}

/// Versions a policy listing returns when the caller sets no limit.
pub const DEFAULT_VERSION_LIMIT: usize = 100;

/// One stored policy version.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyVersionRecord {
    /// [`SERVER_SCOPE`] or [`tenant_scope`].
    pub scope: String,
    /// 1, 2, 3 … per scope; the newest is in force.
    pub version: u32,
    /// The `PolicySpec` document.
    pub spec: Value,
    pub created_at: DateTime<Utc>,
    /// Who requested the change.
    pub created_by: String,
    /// Who approved it (the principal that completed the quorum).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
    /// Why, as the requester wrote it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The change request that produced it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    /// Set when this version re-enacts an earlier one (a rollback).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restores: Option<u32>,
}

/// The stored policy versions a run was admitted under (#1043), kept on its
/// record so a claim, a shard or a resume on another replica enforces the
/// same versions rather than whatever is newest when it starts. `None`
/// means the run used no stored version on that layer (no policy, or the
/// `--policy` file). A run admitted under a `--policy` file names its
/// canonical content instead (`file_sha256`), stored under [`file_scope`],
/// so every replica enforces that content (#1051).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyPin {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_sha256: Option<String>,
}

impl PolicyPin {
    /// Whether no policy was in force when the run was admitted.
    pub fn is_empty(&self) -> bool {
        self.server.is_none() && self.tenant.is_none() && self.file_sha256.is_none()
    }
}

/// Zero-padded so the SQL backends order versions as text.
pub fn padded(version: u32) -> String {
    format!("{version:010}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_and_padding() {
        assert_eq!(tenant_scope("acme"), "tenant:acme");
        assert_eq!(padded(7), "0000000007");
        assert!(padded(10) > padded(9));
    }

    #[test]
    fn record_round_trips_without_optional_fields() {
        let r = PolicyVersionRecord {
            scope: SERVER_SCOPE.into(),
            version: 1,
            spec: serde_json::json!({"rules": []}),
            created_at: Utc::now(),
            created_by: "alice".into(),
            approved_by: None,
            reason: None,
            change_id: None,
            restores: None,
        };
        let text = serde_json::to_string(&r).unwrap();
        assert!(!text.contains("approved_by"));
        assert_eq!(
            serde_json::from_str::<PolicyVersionRecord>(&text).unwrap(),
            r
        );
    }
}
