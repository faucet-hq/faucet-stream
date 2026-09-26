//! Configuration for the Oracle query source.

use faucet_common_oracle::OracleConnectionConfig;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_max_connections() -> u32 {
    10
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_statement_timeout_secs() -> u64 {
    300
}

/// How the source replicates rows across runs.
///
/// Serializes as `{ type: full }` or
/// `{ type: incremental, column: "UPDATED_AT", initial_value: ... }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OracleReplication {
    /// Every run fetches the full result set (default).
    #[default]
    Full,
    /// Only rows whose `column` is strictly greater than the stored bookmark
    /// (or `initial_value` on the first run) are emitted.
    ///
    /// Put the `:bookmark` placeholder in the query to push the cursor down to
    /// the server (`WHERE UPDATED_AT > :bookmark`); the source also filters
    /// client-side as a correctness backstop. `column` names the key exactly
    /// as it appears in the output — Oracle upper-cases unquoted names.
    Incremental {
        /// Output column holding the replication cursor (e.g. `UPDATED_AT`).
        column: String,
        /// Lower bound used on the first run, before any bookmark is stored.
        initial_value: Value,
    },
}

/// Primary-key range sharding for clustered (Mode B) execution: each shard runs
/// `SELECT * FROM (<query>) WHERE "<key>" >= lo AND "<key>" < hi`. The key must
/// be an integer-valued output column, named exactly as Oracle reports it.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ShardConfig {
    /// Integer output column to range-partition on (quoted as an identifier).
    pub key: String,
}

/// Configuration for [`OracleSource`](crate::OracleSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct OracleSourceConfig {
    /// Connection settings (`connect_string` or `host` + `service_name`/`sid`).
    #[serde(flatten)]
    pub connection: OracleConnectionConfig,
    /// SQL query. Bind [`params`](Self::params) with `:1`, `:2`, …, and the
    /// incremental cursor with `:bookmark`.
    pub query: String,
    /// Positional bind values for `:1`…`:n`.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Maximum pooled sessions. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Records per emitted page, also used as the fetch array size. `0` emits
    /// the whole result set as one page. Defaults to [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Per-round-trip call timeout in seconds (`0` disables). Defaults to 300.
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    /// Replication mode. Defaults to [`OracleReplication::Full`].
    #[serde(default)]
    pub replication: OracleReplication,
    /// Explicit state-store key for the bookmark. Derived from the database and
    /// a query fingerprint when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
    /// Optional PK-range sharding for clustered execution. Inert outside the
    /// cluster coordinator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<ShardConfig>,
    /// Output columns whose text holds JSON (a `JSON_SERIALIZE(...)` or an
    /// `IS JSON` `VARCHAR2`/`CLOB`); their values are emitted as parsed JSON.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub json_columns: Vec<String>,
}

impl std::fmt::Debug for OracleSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleSourceConfig")
            .field("connection", &self.connection)
            .field("query", &self.query)
            .field("params", &self.params)
            .field("max_connections", &self.max_connections)
            .field("batch_size", &self.batch_size)
            .field("statement_timeout_secs", &self.statement_timeout_secs)
            .field("replication", &self.replication)
            .field("state_key", &self.state_key)
            .field("shard", &self.shard)
            .field("json_columns", &self.json_columns)
            .finish()
    }
}

impl OracleSourceConfig {
    /// A full-replication config with defaults elsewhere.
    pub fn new(connection: OracleConnectionConfig, query: impl Into<String>) -> Self {
        Self {
            connection,
            query: query.into(),
            params: Vec::new(),
            max_connections: default_max_connections(),
            batch_size: default_batch_size(),
            statement_timeout_secs: default_statement_timeout_secs(),
            replication: OracleReplication::Full,
            state_key: None,
            shard: None,
            json_columns: Vec::new(),
        }
    }

    /// Validate the fail-fast invariants.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_batch_size(self.batch_size)?;
        if self.query.trim().is_empty() {
            return Err(FaucetError::Config(
                "oracle source requires a `query`".into(),
            ));
        }
        if let OracleReplication::Incremental { column, .. } = &self.replication
            && column.trim().is_empty()
        {
            return Err(FaucetError::Config(
                "oracle incremental replication requires a non-empty `column`".into(),
            ));
        }
        if let Some(shard) = &self.shard {
            faucet_common_oracle::quote_ident_oracle(&shard.key)?;
        }
        if let Some(key) = &self.state_key {
            faucet_core::state::validate_state_key(key)?;
        }
        if self.incremental_without_bookmark_pushdown() {
            tracing::warn!(
                "oracle incremental replication query has no `:bookmark` placeholder: the \
                 cursor is applied client-side only, so every run re-reads the whole result \
                 set. Add `WHERE <column> > :bookmark` to push it down."
            );
        }
        Ok(())
    }

    /// Incremental replication whose query cannot push the cursor down.
    pub(crate) fn incremental_without_bookmark_pushdown(&self) -> bool {
        matches!(self.replication, OracleReplication::Incremental { .. })
            && !has_bookmark_placeholder(&self.query)
    }
}

/// True when `sql` contains the `:bookmark` placeholder (case-insensitive).
pub(crate) fn has_bookmark_placeholder(sql: &str) -> bool {
    sql.to_ascii_lowercase().contains(":bookmark")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn conn() -> OracleConnectionConfig {
        OracleConnectionConfig::new("h", 1521, "FREEPDB1", "u", "p")
    }

    #[test]
    fn serde_defaults_and_flattened_connection() {
        let cfg: OracleSourceConfig = serde_json::from_value(json!({
            "host": "h", "service_name": "FREEPDB1", "username": "u", "password": "p",
            "query": "SELECT * FROM T"
        }))
        .unwrap();
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(cfg.max_connections, 10);
        assert_eq!(cfg.statement_timeout_secs, 300);
        assert_eq!(cfg.replication, OracleReplication::Full);
        cfg.validate().unwrap();
    }

    #[test]
    fn replication_round_trips() {
        let r: OracleReplication = serde_json::from_value(json!({
            "type": "incremental", "column": "UPDATED_AT", "initial_value": 0
        }))
        .unwrap();
        assert_eq!(
            r,
            OracleReplication::Incremental {
                column: "UPDATED_AT".into(),
                initial_value: json!(0)
            }
        );
    }

    #[test]
    fn validate_rejects_bad_fields() {
        let mut cfg = OracleSourceConfig::new(conn(), " ");
        assert!(cfg.validate().is_err());
        cfg.query = "SELECT 1 FROM DUAL".into();
        cfg.replication = OracleReplication::Incremental {
            column: " ".into(),
            initial_value: json!(0),
        };
        assert!(cfg.validate().is_err());
        cfg.replication = OracleReplication::Full;
        cfg.shard = Some(ShardConfig { key: "a\"b".into() });
        assert!(cfg.validate().is_err());
        cfg.shard = None;
        cfg.state_key = Some("bad key with spaces".into());
        assert!(cfg.validate().is_err());
        cfg.state_key = None;
        cfg.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn pushdown_detection() {
        let mut cfg = OracleSourceConfig::new(conn(), "SELECT * FROM T");
        assert!(!cfg.incremental_without_bookmark_pushdown());
        cfg.replication = OracleReplication::Incremental {
            column: "C".into(),
            initial_value: json!(0),
        };
        assert!(cfg.incremental_without_bookmark_pushdown());
        cfg.validate().unwrap();
        cfg.query = "SELECT * FROM T WHERE C > :BOOKMARK".into();
        assert!(!cfg.incremental_without_bookmark_pushdown());
    }

    #[test]
    fn debug_masks_password() {
        let cfg = OracleSourceConfig::new(
            OracleConnectionConfig::new("h", 1521, "S", "u", "hunter2"),
            "SELECT 1 FROM DUAL",
        );
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("hunter2"));
        assert!(dbg.contains("SELECT 1 FROM DUAL"));
    }
}
