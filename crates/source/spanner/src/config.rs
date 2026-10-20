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
    /// Only rows not yet read, in `column` order, resuming from the stored
    /// bookmark (or `initial_value` on the first run).
    ///
    /// The source wraps the query as
    /// ``SELECT * FROM (<query>) AS q WHERE q.`column` >= CAST(@bookmark AS <type>) ORDER BY q.`column` ``
    /// (`<type>` is the column's type, read from the query's result metadata)
    /// and drops the rows it already emitted at the bookmark value, so rows
    /// that share a cursor value (ties, late commits) are never skipped.
    /// Every page carries a bookmark, so a crash replays at most one page.
    /// Write `@bookmark` in the query (with `>=`) to apply the cursor further
    /// inside it; a strict `>` is refused. Rows whose `column` is NULL are
    /// never read.
    ///
    /// `@bookmark` inside the query is bound by its JSON type (a timestamp
    /// bookmark is a STRING), and Spanner does not coerce parameter types: cast
    /// it where you use it, e.g. `WHERE updated_at >= TIMESTAMP(@bookmark)`.
    Incremental {
        /// Column whose value is the replication cursor (e.g. `updated_at`).
        column: String,
        /// Lower bound (inclusive) used on the first run, before any bookmark
        /// is stored.
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
        if let SpannerReplication::Incremental {
            column,
            initial_value,
        } = &self.replication
        {
            if column.trim().is_empty() {
                return Err(FaucetError::Config(
                    "spanner: incremental replication requires a non-empty `column`".into(),
                ));
            }
            if !matches!(
                initial_value,
                Value::String(_) | Value::Number(_) | Value::Bool(_)
            ) {
                return Err(FaucetError::Config(
                    "spanner: incremental replication requires a scalar `initial_value` \
                     (string / integer / float / boolean)"
                        .into(),
                ));
            }
            if self.params.contains_key("bookmark") {
                return Err(FaucetError::Config(
                    "spanner: the parameter name `bookmark` is reserved for the \
                     incremental replication cursor"
                        .into(),
                ));
            }
            if strict_bookmark_comparison(&self.query) {
                return Err(FaucetError::Config(format!(
                    "spanner: compare `{BOOKMARK_TOKEN}` with `>=`, not `>`: the source \
                     re-reads the bookmark value and drops the rows it already emitted, and a \
                     strict comparison skips rows that share it"
                )));
            }
        }
        Ok(())
    }
}

/// The named parameter a query uses to read the incremental bookmark
/// (`WHERE updated_at >= TIMESTAMP(@bookmark)`).
pub const BOOKMARK_TOKEN: &str = "@bookmark";

/// `true` when the query compares `@bookmark` strictly (`> @bookmark`,
/// `> TIMESTAMP(@bookmark)`, `> CAST(@bookmark AS NUMERIC)`, `@bookmark < c`).
fn strict_bookmark_comparison(query: &str) -> bool {
    let ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    let mut unwrapped = String::with_capacity(query.len());
    let mut rest = query;
    while let Some(i) = rest.find(BOOKMARK_TOKEN) {
        let end = i + BOOKMARK_TOKEN.len();
        if rest[end..].starts_with(ident) {
            // Another parameter (`@bookmark_x`): keep it out of the check.
            unwrapped.push_str(&rest[..i]);
            unwrapped.push_str("@_param");
            rest = &rest[end..];
            continue;
        }
        let (mut start, mut stop) = (i, end);
        let before = rest[..i].trim_end();
        if let Some(open) = before.strip_suffix('(') {
            let name = open.trim_end();
            let fname = name.trim_end_matches(ident);
            let after = rest[end..].trim_start();
            let after = match after.get(..3) {
                Some(kw) if kw.eq_ignore_ascii_case("AS ") => after[3..]
                    .trim_start()
                    .trim_start_matches(ident)
                    .trim_start(),
                _ => after,
            };
            if let Some(tail) = after.strip_prefix(')') {
                start = fname.len();
                stop = rest.len() - tail.len();
            }
        }
        unwrapped.push_str(&rest[..start]);
        unwrapped.push_str(BOOKMARK_TOKEN);
        rest = &rest[stop..];
    }
    unwrapped.push_str(rest);
    faucet_core::replication::cursor::strict_comparison(&unwrapped, BOOKMARK_TOKEN)
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
    fn validate_rejects_bad_incremental_settings() {
        for (query, column, initial, needle) in [
            (
                "SELECT * FROM t",
                "updated_at",
                json!(null),
                "initial_value",
            ),
            ("SELECT * FROM t", "updated_at", json!([1]), "initial_value"),
            ("SELECT * FROM t WHERE c > @bookmark", "c", json!(0), ">="),
            ("SELECT * FROM t WHERE @bookmark < c", "c", json!(0), ">="),
            (
                "SELECT * FROM t WHERE ts > TIMESTAMP(@bookmark)",
                "ts",
                json!("1970-01-01T00:00:00Z"),
                ">=",
            ),
            (
                "SELECT * FROM t WHERE n > CAST( @bookmark AS NUMERIC ) ORDER BY n",
                "n",
                json!("0"),
                ">=",
            ),
            ("SELECT * FROM t WHERE c > (@bookmark)", "c", json!(0), ">="),
        ] {
            let mut cfg = base();
            cfg.query = query.into();
            cfg.replication = SpannerReplication::Incremental {
                column: column.into(),
                initial_value: initial,
            };
            let err = cfg.validate().unwrap_err().to_string();
            assert!(err.contains(needle), "{err}");
        }
        let mut ok = base();
        ok.query = "SELECT * FROM t WHERE c >= @bookmark".into();
        ok.replication = SpannerReplication::Incremental {
            column: "c".into(),
            initial_value: json!(0),
        };
        assert!(ok.validate().is_ok());
        for query in [
            "SELECT * FROM t WHERE ts >= TIMESTAMP(@bookmark)",
            "SELECT * FROM t WHERE n >= CAST(@bookmark AS NUMERIC)",
            "SELECT * FROM t WHERE c > @bookmark_other AND c >= @bookmark",
            "SELECT * FROM t WHERE c > f (x) AND c >= @bookmark",
        ] {
            assert!(!strict_bookmark_comparison(query), "{query}");
        }
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
