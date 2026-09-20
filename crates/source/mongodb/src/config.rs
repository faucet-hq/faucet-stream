//! MongoDB source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

/// Configuration for the MongoDB source connector.
///
/// # Example
///
/// ```
/// use faucet_source_mongodb::MongoSourceConfig;
/// use serde_json::json;
///
/// let config = MongoSourceConfig::new(
///     "mongodb://localhost:27017",
///     "my_database",
///     "my_collection",
/// )
/// .filter(json!({"status": "active"}))
/// .projection(json!({"_id": 0, "name": 1, "email": 1}))
/// .sort(json!({"created_at": -1}))
/// .limit(1000)
/// .cursor_batch_size(200)
/// .with_batch_size(500);
/// ```
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MongoSourceConfig {
    /// MongoDB connection URI (e.g. `mongodb://localhost:27017`).
    pub connection_uri: String,
    /// Database name.
    pub database: String,
    /// Collection name.
    pub collection: String,
    /// Optional query filter as JSON, converted to a BSON `Document` at query time.
    pub filter: Option<Value>,
    /// Optional field projection as JSON.
    pub projection: Option<Value>,
    /// Optional sort specification as JSON.
    pub sort: Option<Value>,
    /// Maximum number of documents to return.
    pub limit: Option<i64>,
    /// Cursor batch size: documents per server round-trip on the MongoDB
    /// driver cursor. This is a wire-level tuning knob — distinct from
    /// [`Self::batch_size`], which controls the size of each emitted
    /// [`StreamPage`](faucet_core::StreamPage).
    pub cursor_batch_size: Option<u32>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Documents
    /// are pulled from the cursor and yielded whenever the buffer reaches
    /// this size. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the cursor is fully
    /// drained and the entire result set is emitted in a single page. Useful
    /// for small lookup tables or for sinks (e.g. SQL `COPY`, BigQuery load
    /// jobs) that prefer one large request to many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl MongoSourceConfig {
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
            filter: None,
            projection: None,
            sort: None,
            limit: None,
            cursor_batch_size: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the query filter (JSON object converted to BSON at query time).
    pub fn filter(mut self, filter: Value) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set the field projection.
    pub fn projection(mut self, projection: Value) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Set the sort specification.
    pub fn sort(mut self, sort: Value) -> Self {
        self.sort = Some(sort);
        self
    }

    /// Set the maximum number of documents to return.
    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the MongoDB driver cursor batch size (documents per server
    /// round-trip). This is independent of [`Self::with_batch_size`], which
    /// controls the size of each emitted `StreamPage`.
    pub fn cursor_batch_size(mut self, cursor_batch_size: u32) -> Self {
        self.cursor_batch_size = Some(cursor_batch_size);
        self
    }

    /// Set the per-page document count for [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire result set is emitted in
    /// a single [`StreamPage`](faucet_core::StreamPage).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl fmt::Debug for MongoSourceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MongoSourceConfig")
            .field("connection_uri", &"***")
            .field("database", &self.database)
            .field("collection", &self.collection)
            .field("filter", &self.filter)
            .field("projection", &self.projection)
            .field("sort", &self.sort)
            .field("limit", &self.limit)
            .field("cursor_batch_size", &self.cursor_batch_size)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_config() {
        let config = MongoSourceConfig::new("mongodb://localhost:27017", "testdb", "users");
        assert_eq!(config.database, "testdb");
        assert_eq!(config.collection, "users");
        assert!(config.filter.is_none());
        assert!(config.projection.is_none());
        assert!(config.sort.is_none());
        assert!(config.limit.is_none());
        assert!(config.cursor_batch_size.is_none());
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn builder_methods() {
        let config = MongoSourceConfig::new("mongodb://localhost:27017", "testdb", "users")
            .filter(json!({"active": true}))
            .projection(json!({"_id": 0, "name": 1}))
            .sort(json!({"name": 1}))
            .limit(500)
            .cursor_batch_size(100)
            .with_batch_size(2000);

        assert_eq!(config.filter.unwrap(), json!({"active": true}));
        assert_eq!(config.projection.unwrap(), json!({"_id": 0, "name": 1}));
        assert_eq!(config.sort.unwrap(), json!({"name": 1}));
        assert_eq!(config.limit, Some(500));
        assert_eq!(config.cursor_batch_size, Some(100));
        assert_eq!(config.batch_size, 2000);
    }

    #[test]
    fn debug_masks_connection_uri() {
        let config =
            MongoSourceConfig::new("mongodb://user:secret@host:27017/db", "testdb", "users");
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = MongoSourceConfig::new("mongodb://localhost:27017", "db", "c");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config =
            MongoSourceConfig::new("mongodb://localhost:27017", "db", "c").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config =
            MongoSourceConfig::new("mongodb://localhost:27017", "db", "c").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = MongoSourceConfig::new("mongodb://localhost:27017", "db", "c")
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
        let config: MongoSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
        assert!(config.cursor_batch_size.is_none());
    }

    #[test]
    fn cursor_batch_size_deserializes_from_json() {
        let json = r#"{
            "connection_uri": "mongodb://localhost:27017",
            "database": "db",
            "collection": "c",
            "cursor_batch_size": 100,
            "batch_size": 500
        }"#;
        let config: MongoSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.cursor_batch_size, Some(100));
        assert_eq!(config.batch_size, 500);
    }
}
