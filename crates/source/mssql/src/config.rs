//! Configuration for the MSSQL query source.

use faucet_common_mssql::MssqlConnectionConfig;
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
/// `{ type: incremental, column: "...", initial_value: ... }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MssqlReplication {
    /// Every run fetches the full result set (default).
    #[default]
    Full,
    /// Only rows not yet read, in `column` order, resuming from the stored
    /// bookmark (or `initial_value` on the first run).
    ///
    /// The source wraps the query as
    /// `SELECT * FROM (<query>) AS q WHERE q.[column] >= <bookmark> ORDER BY q.[column]`
    /// and drops the rows it already emitted at the bookmark value, so rows
    /// that share a cursor value (ties, late commits, `DATETIME`'s 1/300 s
    /// ticks) are never skipped. Every page carries a bookmark, so a crash
    /// replays at most one page. Write `@bookmark` in the query (with `>=`)
    /// to apply the cursor further inside it, e.g. in a CTE; a strict `>` is
    /// refused. The query must not end in a top-level `ORDER BY` (T-SQL
    /// forbids it in a derived table). Rows whose `column` is NULL are never
    /// read.
    Incremental {
        /// Column whose value is the replication cursor (e.g. `updated_at`).
        column: String,
        /// Lower bound (inclusive) used on the first run, before any bookmark
        /// is stored.
        initial_value: Value,
    },
}

/// Configuration for [`MssqlSource`](crate::MssqlSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct MssqlSourceConfig {
    /// Connection + TLS settings (`connection_url` or `connection_string`).
    #[serde(flatten)]
    pub connection: MssqlConnectionConfig,
    /// SQL query to run. Use `@P1`, `@P2`, … for [`params`](Self::params), and
    /// the literal `@bookmark` token to bind the incremental cursor server-side.
    pub query: String,
    /// Positional bind parameters for the query (`@P1`…`@Pn`). Defaults to empty.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Maximum pooled connections. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). `0` emits the
    /// whole result set as a single page. Defaults to [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Per-query timeout in seconds (`0` disables). Defaults to 300.
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    /// Replication mode. Defaults to [`MssqlReplication::Full`].
    #[serde(default)]
    pub replication: MssqlReplication,
    /// Explicit state-store key for the bookmark. When unset, a key is derived
    /// from the connection host and a query fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
    /// Optional primary-key range sharding for clustered (Mode B) execution.
    ///
    /// When set, the source advertises itself as shardable: the cluster
    /// coordinator splits the query's `key` range into contiguous slices that
    /// different workers process concurrently. Has **no effect** outside the
    /// cluster coordinator (a plain `faucet run` streams the whole query), so
    /// it is fully backward compatible. See [`ShardConfig`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<ShardConfig>,
}

/// Primary-key range sharding settings for the MSSQL source.
///
/// The source is split by contiguous ranges of an **integer-typed** column:
/// each shard runs `SELECT * FROM (<query>) WHERE [key] >= lo AND [key] < hi`.
/// The column must be present in the query's output and orderable as a 64-bit
/// integer (e.g. a `BIGINT`/`INT` `IDENTITY` primary key).
///
/// **`ORDER BY`:** T-SQL forbids `ORDER BY` inside a derived table (without
/// `TOP`/`OFFSET`), so a sharded query must not end in a top-level `ORDER BY`
/// — ordering across concurrently-executing shards is meaningless anyway.
///
/// **Nullable keys:** if the key column contains NULLs, those rows are not
/// visible to the `MIN`/`MAX` range enumeration. They are still read — exactly
/// one shard (the last) additionally matches `[key] IS NULL`, so NULL-key rows
/// are covered by precisely one shard with no loss and no duplication.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ShardConfig {
    /// Integer column to range-partition on. Quoted as an identifier (brackets)
    /// before use, so it is safe against injection but must name a real output
    /// column.
    pub key: String,
}

impl std::fmt::Debug for MssqlSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlSourceConfig")
            .field("connection", &"***")
            .field("query", &self.query)
            .field("params", &self.params)
            .field("max_connections", &self.max_connections)
            .field("batch_size", &self.batch_size)
            .field("statement_timeout_secs", &self.statement_timeout_secs)
            .field("replication", &self.replication)
            .field("state_key", &self.state_key)
            .finish()
    }
}

impl MssqlSourceConfig {
    /// Build a config from a connection URL and query, with defaults elsewhere.
    pub fn new(connection_url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            connection: MssqlConnectionConfig {
                connection_url: Some(connection_url.into()),
                ..Default::default()
            },
            query: query.into(),
            params: Vec::new(),
            max_connections: default_max_connections(),
            batch_size: default_batch_size(),
            statement_timeout_secs: default_statement_timeout_secs(),
            replication: MssqlReplication::Full,
            state_key: None,
            shard: None,
        }
    }

    /// Validate connection source, batch size, and replication settings.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_batch_size(self.batch_size)?;
        let MssqlReplication::Incremental {
            column,
            initial_value,
        } = &self.replication
        else {
            return Ok(());
        };
        if column.trim().is_empty() {
            return Err(FaucetError::Config(
                "MSSQL incremental replication requires a non-empty `column`".into(),
            ));
        }
        if column.contains('\0') {
            return Err(FaucetError::Config(
                "MSSQL incremental replication: `column` must not contain a NUL byte".into(),
            ));
        }
        if initial_value.is_null() {
            return Err(FaucetError::Config(
                "MSSQL incremental replication requires a non-null `initial_value`".into(),
            ));
        }
        if faucet_core::replication::cursor::strict_comparison(&self.query, BOOKMARK_TOKEN) {
            return Err(FaucetError::Config(format!(
                "MSSQL: compare `{BOOKMARK_TOKEN}` with `>=`, not `>`: the source re-reads the \
                 bookmark value and drops the rows it already emitted, and a strict comparison \
                 skips rows that share it"
            )));
        }
        Ok(())
    }
}

/// The token a query uses to read the incremental bookmark
/// (`WHERE updated_at >= @bookmark`).
pub const BOOKMARK_TOKEN: &str = "@bookmark";

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn replication_full_is_default_and_round_trips() {
        let r: MssqlReplication = serde_json::from_value(json!({"type": "full"})).unwrap();
        assert_eq!(r, MssqlReplication::Full);
        assert_eq!(MssqlReplication::default(), MssqlReplication::Full);
    }

    #[test]
    fn replication_incremental_parses_column_and_initial_value() {
        let r: MssqlReplication = serde_json::from_value(json!({
            "type": "incremental",
            "column": "updated_at",
            "initial_value": "1970-01-01T00:00:00Z"
        }))
        .unwrap();
        assert_eq!(
            r,
            MssqlReplication::Incremental {
                column: "updated_at".into(),
                initial_value: json!("1970-01-01T00:00:00Z"),
            }
        );
    }

    #[test]
    fn config_flattens_connection_fields() {
        let cfg: MssqlSourceConfig = serde_json::from_value(json!({
            "connection_url": "mssql://sa:pw@host:1433/db",
            "query": "SELECT 1",
        }))
        .unwrap();
        assert_eq!(
            cfg.connection.connection_url.as_deref(),
            Some("mssql://sa:pw@host:1433/db")
        );
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(cfg.max_connections, 10);
        assert_eq!(cfg.statement_timeout_secs, 300);
    }

    #[test]
    fn validate_rejects_incremental_without_column() {
        let cfg = MssqlSourceConfig {
            replication: MssqlReplication::Incremental {
                column: "  ".into(),
                initial_value: json!(0),
            },
            ..MssqlSourceConfig::new("mssql://sa:pw@h/db", "SELECT 1")
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_incremental_settings() {
        for (query, column, initial, needle) in [
            ("SELECT * FROM t", "c\0", json!(0), "NUL"),
            ("SELECT * FROM t", "c", json!(null), "initial_value"),
            ("SELECT * FROM t WHERE c > @bookmark", "c", json!(0), ">="),
            ("SELECT * FROM t WHERE @bookmark < c", "c", json!(0), ">="),
        ] {
            let cfg = MssqlSourceConfig {
                replication: MssqlReplication::Incremental {
                    column: column.into(),
                    initial_value: initial,
                },
                ..MssqlSourceConfig::new("mssql://sa:pw@h/db", query)
            };
            let err = cfg.validate().unwrap_err().to_string();
            assert!(err.contains(needle), "{err}");
        }
        for query in ["SELECT * FROM t", "SELECT * FROM t WHERE c >= @bookmark"] {
            let cfg = MssqlSourceConfig {
                replication: MssqlReplication::Incremental {
                    column: "c".into(),
                    initial_value: json!(0),
                },
                ..MssqlSourceConfig::new("mssql://sa:pw@h/db", query)
            };
            assert!(cfg.validate().is_ok(), "{query}");
        }
    }

    #[test]
    fn validate_rejects_bad_batch_size() {
        let cfg = MssqlSourceConfig {
            batch_size: faucet_core::MAX_BATCH_SIZE + 1,
            ..MssqlSourceConfig::new("mssql://sa:pw@h/db", "SELECT 1")
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn debug_masks_connection() {
        let cfg = MssqlSourceConfig::new("mssql://sa:secret@h/db", "SELECT 1");
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("secret"));
    }
}
