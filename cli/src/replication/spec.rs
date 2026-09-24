//! Serde config types for the `mirror:` block (`faucet mirror`; the pre-#670
//! spellings `replication:` / `faucet replicate` are still accepted).
//!
//! The main `pipeline` is the CDC pipeline (its `source` is a CDC connector,
//! its `sink` the destination). `replication:` adds the one-time bulk-read
//! snapshot source used to back-fill before CDC starts. Consumed only by
//! `faucet replicate`; ignored by `faucet run` (like `schedule:`).

use crate::config::ConnectorSpec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_true() -> bool {
    true
}

/// Top-level `mirror:` block (alias `replication:`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ReplicationSpec {
    /// Replication strategy. Only `snapshot_then_cdc` is available in v1.
    pub mode: ReplicationMode,
    /// One-time bulk-read source used to back-fill the destination before CDC.
    pub snapshot: SnapshotSpec,
    /// After the snapshot completes, keep streaming CDC until SIGTERM/SIGINT.
    /// When `false`, drain CDC once and exit (useful for tests / batch runs).
    #[serde(default = "default_true")]
    pub continuous: bool,
}

/// Replication strategy discriminator.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplicationMode {
    /// Capture the CDC position, bulk-snapshot the table, then stream CDC from
    /// that position. Pair with `write_mode: upsert` for a true mirror.
    SnapshotThenCdc,
}

/// The one-time snapshot source (a non-CDC bulk reader of the same upstream DB).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSpec {
    /// Bulk-read source connector (e.g. `postgres` running `SELECT * FROM t`).
    pub source: ConnectorSpec,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_replication_block() {
        let yaml = r#"
mode: snapshot_then_cdc
snapshot:
  source:
    type: postgres
    config: { connection_url: "postgres://x", query: "SELECT * FROM orders" }
"#;
        let spec: ReplicationSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.mode, ReplicationMode::SnapshotThenCdc);
        assert_eq!(spec.snapshot.source.kind, "postgres");
        assert!(spec.continuous, "continuous defaults to true");
    }

    #[test]
    fn continuous_false_parses() {
        let yaml = r#"
mode: snapshot_then_cdc
continuous: false
snapshot:
  source: { type: postgres, config: {} }
"#;
        let spec: ReplicationSpec = serde_yaml::from_str(yaml).unwrap();
        assert!(!spec.continuous);
    }

    #[test]
    fn rejects_unknown_field() {
        let yaml = r#"
mode: snapshot_then_cdc
snapshot: { source: { type: postgres, config: {} } }
bogus: true
"#;
        assert!(serde_yaml::from_str::<ReplicationSpec>(yaml).is_err());
    }
}
