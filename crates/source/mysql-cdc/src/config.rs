//! Configuration for [`MysqlCdcSource`](crate::MysqlCdcSourceConfig).

use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::time::Duration;

fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_include_columns() -> bool {
    true
}

/// Where to start reading the binlog on a fresh run (no persisted bookmark).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StartPosition {
    /// Start at the server's current binlog position (skip history). Default.
    #[default]
    Current,
    /// Start at the earliest available binlog (errors if purged at open).
    Earliest,
    /// Start after a specific executed GTID set.
    GtidSet { value: String },
    /// Start at an explicit binlog file + position.
    FilePos { file: String, pos: u64 },
}

/// TLS configuration for the binlog replication connection.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum CdcTls {
    /// No TLS (default, back-compatible). Credentials + row data travel cleartext.
    #[default]
    Disable,
    /// Require TLS but do not verify the server certificate.
    Require,
    /// Require TLS and verify the chain against `ca_path` (or system roots).
    VerifyCa {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_path: Option<String>,
    },
    /// Require TLS and verify both the chain and the hostname.
    VerifyFull {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_path: Option<String>,
    },
}

/// Configuration for the MySQL CDC (binlog) source.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MysqlCdcSourceConfig {
    /// Connection URL, e.g. `mysql://repl:pass@host:3306/db`.
    pub connection_url: String,
    /// Replica server id. **Required** and must be unique across all replicas.
    pub server_id: u32,
    /// Optional client-side allowlist of `db.table` names. Empty = all tables.
    #[serde(default)]
    pub include_tables: Vec<String>,
    /// Optional client-side blocklist of `db.table` names (allowlist wins).
    #[serde(default)]
    pub exclude_tables: Vec<String>,
    /// Start position on a fresh run (ignored once a bookmark exists).
    #[serde(default)]
    pub start_position: StartPosition,
    /// Emit the pre-image (`before`) on updates/deletes. Default true.
    #[serde(default = "default_include_columns")]
    pub include_columns: bool,
    /// Emit DDL statements as `{op:"ddl"}` records. Default false.
    #[serde(default)]
    pub emit_schema_changes: bool,
    /// TLS settings for the replication connection. Default `disable`.
    #[serde(default)]
    pub tls: CdcTls,
    /// Terminator: end the fetch cycle after this much quiet. Default 30s.
    #[serde(
        default = "default_idle_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub idle_timeout: Duration,
    /// Max records buffered for a single in-progress transaction before abort.
    /// `None` = unbounded.
    #[serde(default)]
    pub max_staged_records: Option<usize>,
    /// Advisory per-page record count. `0` = accumulate all txns into one page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl MysqlCdcSourceConfig {
    /// Validate fail-fast invariants. Called from `MysqlCdcSource::new`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.connection_url.trim().is_empty() {
            return Err(FaucetError::Config(
                "mysql-cdc: connection_url must not be empty".into(),
            ));
        }
        if self.server_id == 0 {
            return Err(FaucetError::Config(
                "mysql-cdc: server_id must be a non-zero value unique across replicas".into(),
            ));
        }
        if self.idle_timeout.is_zero() {
            return Err(FaucetError::Config(
                "mysql-cdc: idle_timeout must be > 0".into(),
            ));
        }
        for t in self.include_tables.iter().chain(self.exclude_tables.iter()) {
            if !t.contains('.') {
                return Err(FaucetError::Config(format!(
                    "mysql-cdc: table filter '{t}' must be fully qualified as 'database.table'"
                )));
            }
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        let key = crate::state::state_key(self.server_id);
        faucet_core::state::validate_state_key(&key)?;
        Ok(())
    }

    /// Whether a `db.table` passes the include/exclude filters (allowlist wins).
    pub fn table_included(&self, db: &str, table: &str) -> bool {
        let q = format!("{db}.{table}");
        if !self.include_tables.is_empty() {
            return self.include_tables.iter().any(|t| t == &q);
        }
        !self.exclude_tables.iter().any(|t| t == &q)
    }
}

impl fmt::Debug for MysqlCdcSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MysqlCdcSourceConfig")
            .field("connection_url", &"***")
            .field("server_id", &self.server_id)
            .field("include_tables", &self.include_tables)
            .field("exclude_tables", &self.exclude_tables)
            .field("start_position", &self.start_position)
            .field("include_columns", &self.include_columns)
            .field("emit_schema_changes", &self.emit_schema_changes)
            .field("tls", &self.tls)
            .field("idle_timeout", &self.idle_timeout)
            .field("max_staged_records", &self.max_staged_records)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal() -> MysqlCdcSourceConfig {
        serde_json::from_value(json!({
            "connection_url": "mysql://repl:pass@localhost:3306/db",
            "server_id": 1001
        }))
        .unwrap()
    }

    #[test]
    fn defaults_via_serde() {
        let c = minimal();
        assert_eq!(c.idle_timeout.as_secs(), 30);
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(c.start_position, StartPosition::Current);
        assert!(c.include_columns);
        assert!(!c.emit_schema_changes);
        assert_eq!(c.tls, CdcTls::Disable);
    }

    #[test]
    fn start_position_tagged_enum() {
        let c: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://h/db", "server_id": 5,
            "start_position": { "type": "file_pos", "file": "binlog.000003", "pos": 4 }
        }))
        .unwrap();
        assert_eq!(
            c.start_position,
            StartPosition::FilePos {
                file: "binlog.000003".into(),
                pos: 4
            }
        );
    }

    #[test]
    fn rejects_zero_server_id() {
        let mut c = minimal();
        c.server_id = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_unqualified_table_filter() {
        let mut c = minimal();
        c.include_tables = vec!["users".into()];
        assert!(c.validate().is_err());
    }

    #[test]
    fn table_filter_allowlist_wins() {
        let mut c = minimal();
        c.include_tables = vec!["db.users".into()];
        c.exclude_tables = vec!["db.users".into()];
        assert!(c.table_included("db", "users"));
        assert!(!c.table_included("db", "orders"));
    }

    #[test]
    fn table_filter_blocklist() {
        let mut c = minimal();
        c.exclude_tables = vec!["db.secret".into()];
        assert!(c.table_included("db", "users"));
        assert!(!c.table_included("db", "secret"));
    }

    #[test]
    fn debug_redacts_connection_url() {
        let c: MysqlCdcSourceConfig = serde_json::from_value(json!({
            "connection_url": "mysql://repl:secret@h:3306/db", "server_id": 1
        }))
        .unwrap();
        let dbg = format!("{c:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("secret"));
    }

    #[test]
    fn accepts_minimal() {
        assert!(minimal().validate().is_ok());
    }
}
