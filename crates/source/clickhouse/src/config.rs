//! Configuration for the ClickHouse query source.

use faucet_common_clickhouse::ClickHouseConnection;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// How the source replicates rows across runs.
///
/// Serializes as `{ type: full }` or
/// `{ type: incremental, column: "...", initial_value: ... }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClickHouseReplication {
    /// Every run fetches the full result set (default).
    #[default]
    Full,
    /// Only rows not yet read, in `column` order, resuming from the stored
    /// bookmark (or `initial_value` on the first run).
    ///
    /// The source wraps the query as
    /// `SELECT * FROM (<query>) AS q WHERE q."column" >= <bookmark> ORDER BY q."column"`
    /// and drops the rows it already emitted at the bookmark value, so rows
    /// that share a cursor value (ties, late inserts in the same second,
    /// async-insert flushes) are never skipped. Every page carries a bookmark,
    /// so a crash replays at most one page. Write `@bookmark` in the query
    /// (with `>=`) to apply the cursor further inside it; it is substituted as
    /// an injection-safe SQL literal, and a strict `>` is refused. Rows whose
    /// `column` is NULL are never read.
    Incremental {
        /// Column whose value is the replication cursor (e.g. `updated_at`).
        column: String,
        /// Lower bound (inclusive) used on the first run, before any bookmark
        /// is stored.
        initial_value: Value,
    },
}

/// Configuration for [`ClickHouseSource`](crate::ClickHouseSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct ClickHouseSourceConfig {
    /// Connection settings (`url` or `host`, `database`, credentials).
    #[serde(flatten)]
    pub connection: ClickHouseConnection,
    /// SQL `SELECT` query to run. The output format is set via the request
    /// settings (`JSONCompactEachRowWithNamesAndTypes`, decoded by column
    /// type), so **do not** append a `FORMAT` clause. Use
    /// the literal `@bookmark` token to push the incremental cursor down into
    /// the `WHERE` clause; use `{key}` tokens to inject parent-context values in
    /// a matrix child.
    pub query: String,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). `0` emits
    /// the whole result set as a single page. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Replication mode. Defaults to [`ClickHouseReplication::Full`].
    #[serde(default)]
    pub replication: ClickHouseReplication,
    /// Explicit state-store key for the bookmark. When unset, a key is derived
    /// from the connection host and a query fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
}

impl std::fmt::Debug for ClickHouseSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseSourceConfig")
            .field("connection", &self.connection)
            .field("query", &self.query)
            .field("batch_size", &self.batch_size)
            .field("replication", &self.replication)
            .field("state_key", &self.state_key)
            .finish()
    }
}

impl ClickHouseSourceConfig {
    /// Build a config from a base URL and query, with defaults elsewhere.
    pub fn new(url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            connection: ClickHouseConnection::from_url(url),
            query: query.into(),
            batch_size: default_batch_size(),
            replication: ClickHouseReplication::Full,
            state_key: None,
        }
    }

    /// Set the per-page record count.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Configure incremental replication on `column`, starting at `initial`.
    pub fn incremental(mut self, column: impl Into<String>, initial: Value) -> Self {
        self.replication = ClickHouseReplication::Incremental {
            column: column.into(),
            initial_value: initial,
        };
        self
    }

    /// Validate connection, batch size, and replication settings.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_batch_size(self.batch_size)?;
        let ClickHouseReplication::Incremental {
            column,
            initial_value,
        } = &self.replication
        else {
            return Ok(());
        };
        if column.trim().is_empty() {
            return Err(FaucetError::Config(
                "ClickHouse incremental replication requires a non-empty `column`".into(),
            ));
        }
        if initial_value.is_null() {
            return Err(FaucetError::Config(
                "ClickHouse incremental replication requires a non-null `initial_value`".into(),
            ));
        }
        if faucet_core::replication::cursor::strict_comparison(&self.query, BOOKMARK_TOKEN) {
            return Err(FaucetError::Config(format!(
                "ClickHouse: compare `{BOOKMARK_TOKEN}` with `>=`, not `>`: the source re-reads \
                 the bookmark value and drops the rows it already emitted, and a strict \
                 comparison skips rows that share it"
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
    fn config_flattens_connection_fields() {
        let cfg: ClickHouseSourceConfig = serde_json::from_value(json!({
            "url": "http://localhost:8123",
            "database": "analytics",
            "query": "SELECT 1",
        }))
        .unwrap();
        assert_eq!(cfg.connection.url.as_deref(), Some("http://localhost:8123"));
        assert_eq!(cfg.connection.database, "analytics");
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn replication_full_is_default() {
        let cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT 1");
        assert_eq!(cfg.replication, ClickHouseReplication::Full);
    }

    #[test]
    fn replication_incremental_parses() {
        let r: ClickHouseReplication = serde_json::from_value(json!({
            "type": "incremental",
            "column": "updated_at",
            "initial_value": "1970-01-01",
        }))
        .unwrap();
        assert_eq!(
            r,
            ClickHouseReplication::Incremental {
                column: "updated_at".into(),
                initial_value: json!("1970-01-01"),
            }
        );
    }

    #[test]
    fn validate_rejects_incremental_without_column() {
        let cfg = ClickHouseSourceConfig {
            replication: ClickHouseReplication::Incremental {
                column: "  ".into(),
                initial_value: json!(0),
            },
            ..ClickHouseSourceConfig::new("http://h:8123", "SELECT 1")
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_batch_size() {
        let cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT 1")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_missing_endpoint() {
        let mut cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT 1");
        cfg.connection.url = None;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_incremental_settings() {
        for (query, initial, needle) in [
            ("SELECT * FROM t", json!(null), "initial_value"),
            ("SELECT * FROM t WHERE c > @bookmark", json!(0), ">="),
        ] {
            let cfg = ClickHouseSourceConfig::new("http://h:8123", query).incremental("c", initial);
            let err = cfg.validate().unwrap_err().to_string();
            assert!(err.contains(needle), "{err}");
        }
        for query in ["SELECT * FROM t", "SELECT * FROM t WHERE c >= @bookmark"] {
            let cfg =
                ClickHouseSourceConfig::new("http://h:8123", query).incremental("c", json!(0));
            assert!(cfg.validate().is_ok(), "{query}");
        }
    }

    #[test]
    fn debug_masks_password() {
        let mut cfg = ClickHouseSourceConfig::new("http://h:8123", "SELECT 1");
        cfg.connection.password = Some("s3cret".into());
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("s3cret"));
    }
}
