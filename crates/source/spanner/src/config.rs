//! Configuration for the Cloud Spanner query source.

use std::collections::HashMap;

use faucet_common_spanner::SpannerConnection;
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
pub enum SpannerReplication {
    /// Every run fetches the full result set (default).
    #[default]
    Full,
    /// Only rows whose `column` is strictly greater than the stored bookmark
    /// (or `initial_value` on the first run) are emitted.
    ///
    /// The bookmark is applied two ways: if the query contains the named
    /// parameter `@bookmark`, it is bound so the server filters (efficient);
    /// the source *also* filters client-side as a correctness backstop. The
    /// new maximum of `column` is persisted on the final page.
    ///
    /// Spanner does not implicitly coerce parameter types: a string bookmark
    /// compared against a `TIMESTAMP` column needs an explicit cast in the
    /// query, e.g. `WHERE updated_at > TIMESTAMP(@bookmark)`.
    Incremental {
        /// Column whose value is the replication cursor (e.g. `updated_at`).
        column: String,
        /// Lower bound used on the first run, before any bookmark is stored.
        initial_value: Value,
    },
}

/// Primary-key range sharding settings for the Spanner source (clustered
/// Mode B execution).
///
/// The source is split by contiguous ranges of an **INT64-typed** column:
/// each shard runs ``SELECT * FROM (<query>) AS _faucet_shard WHERE `key` >=
/// lo AND `key` < hi``. The column must be present in the query's output.
///
/// **Nullable keys:** rows whose key is NULL are invisible to the `MIN`/`MAX`
/// range enumeration but are still read — exactly one shard (the last)
/// additionally matches ``key IS NULL``, so NULL-key rows are covered by
/// precisely one shard with no loss and no duplication.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
pub struct ShardConfig {
    /// INT64 column to range-partition on. Backtick-quoted before use, so it
    /// is safe against injection but must name a real output column.
    pub key: String,
}

/// Configuration for [`SpannerSource`](crate::SpannerSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct SpannerSourceConfig {
    /// Connection settings (`project_id` / `instance` / `database` / `auth` /
    /// `max_sessions` / `emulator_host`), flattened.
    #[serde(flatten)]
    pub connection: SpannerConnection,
    /// GoogleSQL query to run. Use named parameters (`@name`) for
    /// [`params`](Self::params), and the named parameter `@bookmark` to bind
    /// the incremental cursor server-side.
    pub query: String,
    /// Named bind parameters for the query. Values must be scalars (string,
    /// integer, float, or boolean) — Spanner parameters are typed, and
    /// null/array/object values have no unambiguous parameter type.
    #[serde(default)]
    pub params: HashMap<String, Value>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). `0` emits
    /// the whole result set as a single page. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Read at a timestamp this many seconds in the past instead of a strong
    /// read. Stale reads can be served by any replica, offloading the leader
    /// — at the cost of missing rows committed within the staleness window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exact_staleness_secs: Option<u64>,
    /// Replication mode. Defaults to [`SpannerReplication::Full`].
    #[serde(default)]
    pub replication: SpannerReplication,
    /// Explicit state-store key for the bookmark. When unset, a key is
    /// derived from the database path and a query fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
    /// Optional primary-key range sharding for clustered (Mode B) execution.
    ///
    /// When set, the source advertises itself as shardable: the cluster
    /// coordinator splits the query's `key` range into contiguous slices that
    /// different workers process concurrently. Has **no effect** outside the
    /// cluster coordinator (a plain `faucet run` streams the whole query).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub shard: Option<ShardConfig>,
}

impl std::fmt::Debug for SpannerSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `SpannerConnection`'s Debug masks inline key material already; keep
        // the connector-level Debug conservative anyway.
        f.debug_struct("SpannerSourceConfig")
            .field("connection", &self.connection)
            .field("query", &self.query)
            .field("params", &self.params)
            .field("batch_size", &self.batch_size)
            .field("exact_staleness_secs", &self.exact_staleness_secs)
            .field("replication", &self.replication)
            .field("state_key", &self.state_key)
            .field("shard", &self.shard)
            .finish()
    }
}

/// `true` when `name` is a valid Spanner parameter name
/// (`[A-Za-z_][A-Za-z0-9_]*`).
fn valid_param_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl SpannerSourceConfig {
    /// Build a config from connection identifiers and a query, with defaults
    /// elsewhere.
    pub fn new(
        project_id: impl Into<String>,
        instance: impl Into<String>,
        database: impl Into<String>,
        query: impl Into<String>,
    ) -> Self {
        Self {
            connection: SpannerConnection {
                project_id: project_id.into(),
                instance: instance.into(),
                database: database.into(),
                auth: Default::default(),
                max_sessions: 100,
                emulator_host: None,
                dialect: Default::default(),
            },
            query: query.into(),
            params: HashMap::new(),
            batch_size: default_batch_size(),
            exact_staleness_secs: None,
            replication: SpannerReplication::Full,
            state_key: None,
            shard: None,
        }
    }

    /// Validate connection, batch size, params, and replication settings.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_batch_size(self.batch_size)?;
        if self.query.trim().is_empty() {
            return Err(FaucetError::Config(
                "spanner: `query` must not be empty".into(),
            ));
        }
        for (name, value) in &self.params {
            if !valid_param_name(name) {
                return Err(FaucetError::Config(format!(
                    "spanner: invalid parameter name `{name}` \
                     (must match [A-Za-z_][A-Za-z0-9_]*)"
                )));
            }
            match value {
                Value::String(_) | Value::Bool(_) => {}
                Value::Number(n) => {
                    if n.as_u64().is_some_and(|u| i64::try_from(u).is_err()) {
                        return Err(FaucetError::Config(format!(
                            "spanner: parameter `{name}` ({n}) overflows INT64"
                        )));
                    }
                }
                _ => {
                    return Err(FaucetError::Config(format!(
                        "spanner: parameter `{name}` must be a scalar \
                         (string / integer / float / boolean) — Spanner \
                         parameters are typed and null/array/object values \
                         have no unambiguous type"
                    )));
                }
            }
        }
        if let SpannerReplication::Incremental { column, .. } = &self.replication {
            if column.trim().is_empty() {
                return Err(FaucetError::Config(
                    "spanner: incremental replication requires a non-empty `column`".into(),
                ));
            }
            if self.params.contains_key("bookmark") {
                return Err(FaucetError::Config(
                    "spanner: the parameter name `bookmark` is reserved for the \
                     incremental replication cursor"
                        .into(),
                ));
            }
        }
        if self.incremental_without_bookmark_pushdown() {
            tracing::warn!(
                "spanner incremental replication query has no `@bookmark` parameter: the \
                 cursor is applied client-side only, so the server returns the ENTIRE \
                 table on every run (correctness is preserved, but it is a full re-scan). \
                 Add `@bookmark` to the WHERE clause to push the cursor down, e.g. \
                 `... WHERE {column} > @bookmark`",
                column = match &self.replication {
                    SpannerReplication::Incremental { column, .. } => column.as_str(),
                    _ => "<column>",
                }
            );
        }
        Ok(())
    }

    /// `true` when replication is `Incremental` but the query omits the
    /// `@bookmark` parameter, so the cursor cannot be pushed down to the
    /// server and every run re-scans the whole table. Pure predicate so the
    /// load-time warning's condition is unit-testable.
    pub(crate) fn incremental_without_bookmark_pushdown(&self) -> bool {
        matches!(self.replication, SpannerReplication::Incremental { .. })
            && !self.query.contains("@bookmark")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> SpannerSourceConfig {
        SpannerSourceConfig::new("p", "i", "d", "SELECT * FROM t")
    }

    #[test]
    fn replication_full_is_default_and_round_trips() {
        let r: SpannerReplication = serde_json::from_value(json!({"type": "full"})).unwrap();
        assert_eq!(r, SpannerReplication::Full);
        assert_eq!(SpannerReplication::default(), SpannerReplication::Full);
    }

    #[test]
    fn replication_incremental_parses_column_and_initial_value() {
        let r: SpannerReplication = serde_json::from_value(json!({
            "type": "incremental",
            "column": "updated_at",
            "initial_value": "1970-01-01T00:00:00Z"
        }))
        .unwrap();
        assert_eq!(
            r,
            SpannerReplication::Incremental {
                column: "updated_at".into(),
                initial_value: json!("1970-01-01T00:00:00Z"),
            }
        );
    }

    #[test]
    fn config_flattens_connection_fields() {
        let cfg: SpannerSourceConfig = serde_json::from_value(json!({
            "project_id": "p",
            "instance": "i",
            "database": "d",
            "query": "SELECT 1",
        }))
        .unwrap();
        assert_eq!(cfg.connection.project_id, "p");
        assert_eq!(cfg.connection.max_sessions, 100);
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert!(cfg.exact_staleness_secs.is_none());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_query_and_bad_batch() {
        let mut cfg = base();
        cfg.query = "  ".into();
        assert!(cfg.validate().is_err());
        let mut cfg = base();
        cfg.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_non_scalar_and_ill_named_params() {
        let mut cfg = base();
        cfg.params.insert("ok".into(), json!("x"));
        assert!(cfg.validate().is_ok());

        let mut cfg = base();
        cfg.params.insert("bad".into(), json!(null));
        assert!(cfg.validate().is_err());
        let mut cfg = base();
        cfg.params.insert("bad".into(), json!([1]));
        assert!(cfg.validate().is_err());
        let mut cfg = base();
        cfg.params.insert("bad".into(), json!({"a": 1}));
        assert!(cfg.validate().is_err());
        let mut cfg = base();
        cfg.params.insert("9lives".into(), json!(1));
        assert!(cfg.validate().is_err(), "names must not start with a digit");
        let mut cfg = base();
        cfg.params.insert("we-ird".into(), json!(1));
        assert!(cfg.validate().is_err(), "names must be [A-Za-z0-9_]");
        let mut cfg = base();
        cfg.params.insert("big".into(), json!(u64::MAX));
        assert!(
            cfg.validate().is_err(),
            "u64 above i64::MAX overflows INT64"
        );
    }

    #[test]
    fn validate_rejects_incremental_without_column_or_with_reserved_param() {
        let mut cfg = base();
        cfg.replication = SpannerReplication::Incremental {
            column: " ".into(),
            initial_value: json!(0),
        };
        assert!(cfg.validate().is_err());

        let mut cfg = base();
        cfg.query = "SELECT * FROM t WHERE c > @bookmark".into();
        cfg.replication = SpannerReplication::Incremental {
            column: "c".into(),
            initial_value: json!(0),
        };
        cfg.params.insert("bookmark".into(), json!(1));
        assert!(cfg.validate().is_err(), "`bookmark` is reserved");
    }

    #[test]
    fn incremental_without_bookmark_pushdown_flags_missing_token() {
        let mut missing = base();
        missing.replication = SpannerReplication::Incremental {
            column: "updated_at".into(),
            initial_value: json!("1970-01-01"),
        };
        assert!(missing.incremental_without_bookmark_pushdown());
        // validate still succeeds (warn, not hard-error).
        assert!(missing.validate().is_ok());

        let mut with_token = missing.clone();
        with_token.query = "SELECT * FROM t WHERE updated_at > @bookmark".into();
        assert!(!with_token.incremental_without_bookmark_pushdown());

        assert!(!base().incremental_without_bookmark_pushdown());
    }

    #[test]
    fn valid_param_names() {
        assert!(valid_param_name("cursor"));
        assert!(valid_param_name("_x9"));
        assert!(valid_param_name("A_b_1"));
        assert!(!valid_param_name(""));
        assert!(!valid_param_name("1abc"));
        assert!(!valid_param_name("a-b"));
        assert!(!valid_param_name("a b"));
    }

    #[test]
    fn debug_does_not_leak_inline_key() {
        let mut cfg = base();
        cfg.connection.auth = faucet_common_spanner::SpannerCredentials::ServiceAccountKey {
            json: "{\"private_key\":\"SECRET\"}".into(),
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("SECRET"));
    }
}
