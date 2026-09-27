//! MongoDB sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Configuration for the MongoDB sink connector.
///
/// # Example
///
/// ```
/// use faucet_sink_mongodb::MongoSinkConfig;
///
/// let config = MongoSinkConfig::new(
///     "mongodb://localhost:27017",
///     "my_database",
///     "my_collection",
/// )
/// .with_batch_size(1000);
/// ```
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct MongoSinkConfig {
    /// MongoDB connection URI (e.g. `mongodb://localhost:27017`).
    pub connection_uri: String,
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// Maximum number of documents per `insert_many` call. Defaults to
    /// [`DEFAULT_BATCH_SIZE`] (1000), which is a good balance for MongoDB's
    /// per-request limits and round-trip cost.
    ///
    /// When `write_batch` is handed a slice larger than `batch_size`, the
    /// sink re-chunks it into `batch_size` slices and issues one
    /// `insert_many` per chunk. `batch_size = 0` is the **"no batching"
    /// sentinel** — the records slice is forwarded as a single
    /// `insert_many`, no matter how large, so upstream `StreamPage` framing
    /// flows through untouched.
    ///
    /// **Not applied on the exactly-once path**: `write_batch_idempotent`
    /// always writes the whole page in one transaction (one page = one
    /// transaction), so re-chunking would break page↔watermark atomicity.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Whether `insert_many` is **ordered**. Default `false` (unordered).
    ///
    /// With the MongoDB default of `ordered = true`, the first failing
    /// document (a duplicate `_id`, a validation error, …) aborts the rest of
    /// the batch — the documents before it commit, those after are silently
    /// dropped. Unordered (`false`) instead attempts every document and only
    /// the genuinely-bad ones fail, so a single poison record can't drop the
    /// rest of the batch (#78/#20).
    #[serde(default)]
    pub ordered: bool,
    /// Write mode, key columns, and optional delete marker.
    ///
    /// `write_mode` defaults to `append` (the `insert_many` fast path). For
    /// `upsert` / `delete`, `key` must be non-empty — MongoDB being schemaless,
    /// the key columns become the **match filter** (typically `["_id"]`), not
    /// table columns. Each upsert is committed with a per-document
    /// `replace_one(upsert = true)`; each delete with a `delete_one`.
    #[serde(flatten)]
    pub write: faucet_core::WriteSpec,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl MongoSinkConfig {
    /// Create a new config with the required connection URI, database, and collection.
    pub fn new(
        connection_uri: impl Into<String>,
        database: impl Into<String>,
        collection: impl Into<String>,
    ) -> Self {
        Self {
            connection_uri: connection_uri.into(),
            database: database.into(),
            collection: collection.into(),
            batch_size: DEFAULT_BATCH_SIZE,
            ordered: false,
            write: faucet_core::WriteSpec::default(),
        }
    }

    /// Set whether `insert_many` is ordered (default `false`).
    pub fn with_ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    /// Set the maximum number of documents per `insert_many` call.
    ///
    /// Pass `0` to opt out of re-chunking — the entire records slice handed
    /// to `write_batch` is sent in a single `insert_many` call, preserving
    /// upstream `StreamPage` framing.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl fmt::Debug for MongoSinkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoSinkConfig")
            .field("connection_uri", &"***")
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("batch_size", &self.batch_size)
            .field("ordered", &self.ordered)
            .field("write", &self.write)
            .finish()
    }
}

impl MongoSinkConfig {
    /// What a failed batch write leaves behind (#737): insert_many and per-document upserts are not transactional.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        faucet_core::BatchAtomicity::BestEffort
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = MongoSinkConfig::new("mongodb://localhost:27017", "testdb", "users");
        assert_eq!(config.database, "testdb");
        assert_eq!(config.collection, "users");
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = MongoSinkConfig::new("mongodb://localhost:27017", "testdb", "users");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = MongoSinkConfig::new("mongodb://localhost:27017", "testdb", "users")
            .with_batch_size(2000);
        assert_eq!(config.batch_size, 2000);
    }

    #[test]
    fn debug_masks_connection_uri() {
        let config = MongoSinkConfig::new("mongodb://user:secret@host:27017/db", "testdb", "users");
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config =
            MongoSinkConfig::new("mongodb://localhost:27017", "db", "c").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = MongoSinkConfig::new("mongodb://localhost:27017", "db", "c")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "connection_uri": "mongodb://localhost:27017",
            "database": "db",
            "collection": "c",
            "batch_size": 250
        }"#;
        let config: MongoSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_in_json() {
        let json = r#"{
            "connection_uri": "mongodb://localhost:27017",
            "database": "db",
            "collection": "c"
        }"#;
        let config: MongoSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn batch_atomicity_matches_the_write_path() {
        let c: MongoSinkConfig = serde_json::from_value(serde_json::json!({"connection_uri": "mongodb://localhost:27017", "database": "db", "collection": "c"})).unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }
}
