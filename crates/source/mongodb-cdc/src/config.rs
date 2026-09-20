//! Configuration for [`MongoCdcSource`](crate::MongoCdcSource).

use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::time::Duration;

fn default_idle_timeout() -> Duration {
    Duration::from_secs(30)
}
fn default_max_await_time_ms() -> u64 {
    1000
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Which change stream to open.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Scope {
    /// Watch one collection.
    Collection {
        database: String,
        collection: String,
    },
    /// Watch every collection in one database.
    Database { database: String },
    /// Watch the whole deployment.
    #[default]
    Cluster,
}

/// Where to start the change stream on a fresh run (no persisted bookmark).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StartFrom {
    /// Start at the current cluster time (default — do not replay history).
    #[default]
    Now,
    /// Start at the earliest retained oplog entry (errors if it has rolled).
    Earliest,
    /// Resume after a specific opaque token. Overrides a persisted bookmark.
    ResumeToken { token: Value },
    /// Start at a wall-clock second. Overrides a persisted bookmark.
    Timestamp { timestamp_secs: u32 },
}

/// Post-image inclusion mode (maps to the driver `FullDocumentType`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FullDocument {
    /// Do not request the post-image (default). `after` is null on update/replace.
    #[default]
    Off,
    /// Include the post-image when the server can supply it.
    WhenAvailable,
    /// Require the post-image (errors if unavailable).
    Required,
    /// Look up the current full document at emit time (read-skew caveat).
    UpdateLookup,
}

/// Pre-image inclusion mode (maps to the driver `FullDocumentBeforeChangeType`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FullDocumentBeforeChange {
    /// Do not request the pre-image (default).
    #[default]
    Off,
    /// Include the pre-image when available (MongoDB 6.0+, pre-images enabled).
    WhenAvailable,
    /// Require the pre-image (errors if unavailable).
    Required,
}

/// Configuration for [`MongoCdcSource`](crate::MongoCdcSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MongoCdcSourceConfig {
    /// MongoDB connection URI. Must point at a replica set or sharded cluster.
    pub connection_uri: String,
    /// Which change stream to open.
    #[serde(default)]
    pub scope: Scope,
    /// Allowlist of operation types (`insert`/`update`/`replace`/`delete`/...).
    /// Empty = all. Prepended server-side as a `$match` on `operationType`.
    #[serde(default)]
    pub operation_types: Vec<String>,
    /// Post-image inclusion mode.
    #[serde(default)]
    pub full_document: FullDocument,
    /// Pre-image inclusion mode (MongoDB 6.0+).
    #[serde(default)]
    pub full_document_before_change: FullDocumentBeforeChange,
    /// Start position on a fresh run (ignored once a bookmark exists, except an
    /// explicit `resume_token`/`timestamp` which overrides the bookmark).
    #[serde(default)]
    pub start_from: StartFrom,
    /// Optional extra aggregation-pipeline stages, each a JSON object, appended
    /// after the operation-type `$match`.
    #[serde(default)]
    pub aggregation_pipeline: Vec<Value>,
    /// Terminator: flush the buffer and end the fetch cycle after this much
    /// quiet. Default 30s.
    #[serde(
        default = "default_idle_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub idle_timeout: Duration,
    /// Server-side blocking `getMore` wait. Must be `< idle_timeout`. Default 1000.
    #[serde(default = "default_max_await_time_ms")]
    pub max_await_time_ms: u64,
    /// Records per emitted `StreamPage`. `0` = drain until idle into one page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Max change records buffered in memory before a page is emitted, after
    /// which the run aborts with a typed [`FaucetError::Source`]. `None` =
    /// unbounded. The OOM safety valve: with `batch_size: 0` (or a library
    /// caller using `fetch_all`) the change-event buffer would otherwise grow
    /// without bound for the whole fetch cycle and risk an OOM-kill on a
    /// high-throughput stream. Mirrors the same field on the sibling
    /// `postgres-cdc` / `mysql-cdc` sources.
    #[serde(default)]
    pub max_staged_records: Option<usize>,
}

impl MongoCdcSourceConfig {
    /// Validate fail-fast invariants. Called from `MongoCdcSource::new`.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.connection_uri.trim().is_empty() {
            return Err(FaucetError::Config(
                "mongodb-cdc: connection_uri must not be empty".into(),
            ));
        }
        match &self.scope {
            Scope::Collection {
                database,
                collection,
            } => {
                if database.trim().is_empty() || collection.trim().is_empty() {
                    return Err(FaucetError::Config(
                        "mongodb-cdc: scope.database and scope.collection must not be empty".into(),
                    ));
                }
            }
            Scope::Database { database } => {
                if database.trim().is_empty() {
                    return Err(FaucetError::Config(
                        "mongodb-cdc: scope.database must not be empty".into(),
                    ));
                }
            }
            Scope::Cluster => {}
        }
        let idle_ms = self.idle_timeout.as_millis();
        if idle_ms == 0 {
            return Err(FaucetError::Config(
                "mongodb-cdc: idle_timeout must be > 0".into(),
            ));
        }
        if u128::from(self.max_await_time_ms) >= idle_ms {
            return Err(FaucetError::Config(format!(
                "mongodb-cdc: max_await_time_ms ({}) must be strictly less than \
                 idle_timeout ({}ms)",
                self.max_await_time_ms, idle_ms
            )));
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        // Validate the derived state key is store-safe.
        let key = crate::state::state_key(&self.scope);
        faucet_core::state::validate_state_key(&key)?;
        Ok(())
    }
}

impl fmt::Debug for MongoCdcSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoCdcSourceConfig")
            .field("connection_uri", &"***")
            .field("scope", &self.scope)
            .field("operation_types", &self.operation_types)
            .field("full_document", &self.full_document)
            .field(
                "full_document_before_change",
                &self.full_document_before_change,
            )
            .field("start_from", &self.start_from)
            .field("aggregation_pipeline", &self.aggregation_pipeline)
            .field("idle_timeout", &self.idle_timeout)
            .field("max_await_time_ms", &self.max_await_time_ms)
            .field("batch_size", &self.batch_size)
            .field("max_staged_records", &self.max_staged_records)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn minimal() -> MongoCdcSourceConfig {
        serde_json::from_value(json!({
            "connection_uri": "mongodb://localhost:27017/?replicaSet=rs0",
            "scope": { "type": "collection", "database": "app", "collection": "users" }
        }))
        .unwrap()
    }

    #[test]
    fn defaults_via_serde() {
        let c = minimal();
        assert_eq!(c.idle_timeout.as_secs(), 30);
        assert_eq!(c.max_await_time_ms, 1000);
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(c.start_from, StartFrom::Now);
        assert_eq!(c.full_document, FullDocument::Off);
        assert!(c.operation_types.is_empty());
        // Defaults to unbounded, matching postgres-cdc / mysql-cdc.
        assert_eq!(c.max_staged_records, None);
    }

    #[test]
    fn max_staged_records_round_trips() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" },
            "max_staged_records": 100000
        }))
        .unwrap();
        assert_eq!(c.max_staged_records, Some(100_000));
        assert!(c.validate().is_ok());
    }

    #[test]
    fn scope_tagged_enum_round_trips() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "cluster" }
        }))
        .unwrap();
        assert_eq!(c.scope, Scope::Cluster);
    }

    #[test]
    fn validate_rejects_empty_uri() {
        let mut c = minimal();
        c.connection_uri = "  ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_max_await_ge_idle() {
        let mut c = minimal();
        c.idle_timeout = Duration::from_millis(500);
        c.max_await_time_ms = 500;
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_collection() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "collection", "database": "app", "collection": "" }
        }))
        .unwrap();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_database_scope() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://h/?replicaSet=rs0",
            "scope": { "type": "database", "database": "" }
        }))
        .unwrap();
        assert!(c.validate().is_err());
    }

    #[test]
    fn validate_accepts_batch_size_zero_sentinel() {
        let mut c = minimal();
        c.batch_size = 0;
        assert!(c.validate().is_ok());
    }

    #[test]
    fn validate_accepts_minimal() {
        assert!(minimal().validate().is_ok());
    }

    #[test]
    fn debug_redacts_connection_uri() {
        let c: MongoCdcSourceConfig = serde_json::from_value(json!({
            "connection_uri": "mongodb://user:secret@h:27017/?replicaSet=rs0",
            "scope": { "type": "cluster" }
        }))
        .unwrap();
        let dbg = format!("{c:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("secret"));
    }

    #[test]
    fn schema_includes_connection_uri_required() {
        let schema = schemars::schema_for!(MongoCdcSourceConfig);
        let v = serde_json::to_value(&schema).unwrap();
        let req = v["required"].as_array().unwrap();
        let names: Vec<_> = req.iter().filter_map(|x| x.as_str()).collect();
        assert!(names.contains(&"connection_uri"));
    }
}
