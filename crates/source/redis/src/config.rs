//! Redis source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The type of Redis data structure to read from.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum RedisSourceType {
    /// Read all elements from a Redis list via `LRANGE`.
    List {
        /// The list key.
        key: String,
    },
    /// Read entries from a Redis stream via `XRANGE` (or `XREAD` / `XREADGROUP`
    /// for the consumer-group convenience path on `fetch_all`).
    Stream {
        /// The stream key.
        key: String,
        /// Optional consumer group name. When set, [`fetch_all`](super::RedisSource::fetch_all)
        /// uses `XREADGROUP`. Streaming via [`stream_pages`](faucet_core::Source::stream_pages)
        /// always uses `XRANGE` (consumer-group semantics are incompatible
        /// with the page-then-write streaming contract).
        group: Option<String>,
        /// Consumer name within the group (required when `group` is set).
        consumer: Option<String>,
        /// Maximum number of entries to read per call. Used by
        /// [`fetch_all`](super::RedisSource::fetch_all)'s `XREAD` / `XREADGROUP`
        /// path; ignored by streaming, which uses
        /// [`RedisSourceConfig::batch_size`] as both the per-page count and
        /// the `XRANGE COUNT` hint.
        count: Option<usize>,
    },
    /// Scan for keys matching a pattern, then `MGET` each batch.
    Keys {
        /// Glob pattern for `SCAN` (e.g. `"user:*"`).
        pattern: String,
    },
}

/// Configuration for the Redis source connector.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RedisSourceConfig {
    /// Redis connection URL (e.g. `"redis://127.0.0.1:6379"`).
    pub url: String,
    /// The type of Redis data structure to read from.
    pub source_type: RedisSourceType,
    /// Optional maximum number of records to return.
    pub max_records: Option<usize>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Each mode
    /// maps the knob onto its native paging primitive:
    ///
    /// - `Keys`: `SCAN COUNT batch_size` hint, followed by an `MGET` batched
    ///   to `batch_size` records per page.
    /// - `Stream`: `XRANGE - + COUNT batch_size`, advancing the start ID per
    ///   page.
    /// - `List`: `LRANGE start stop`, sliding the window by `batch_size`.
    ///
    /// Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: every mode drains its
    /// underlying primitive in a single round-trip (or, for `Keys`, the
    /// minimum number of round-trips the `SCAN` cursor needs) and emits the
    /// entire result set as a single page. Useful for small lookup tables or
    /// for sinks like SQL `COPY` / BigQuery load jobs that prefer one large
    /// request to many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl std::fmt::Debug for RedisSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSourceConfig")
            .field("url", &"***")
            .field("source_type", &self.source_type)
            .field("max_records", &self.max_records)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl RedisSourceConfig {
    /// Create a new config with the given URL and source type.
    pub fn new(url: impl Into<String>, source_type: RedisSourceType) -> Self {
        Self {
            url: url.into(),
            source_type,
            max_records: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the maximum number of records to return.
    pub fn max_records(mut self, max: usize) -> Self {
        self.max_records = Some(max);
        self
    }

    /// Set the per-page record count for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire result set is emitted in
    /// a single [`StreamPage`](faucet_core::StreamPage).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_list() {
        let config = RedisSourceConfig::new(
            "redis://127.0.0.1:6379",
            RedisSourceType::List {
                key: "my_list".into(),
            },
        );
        assert!(config.max_records.is_none());
        // Debug should mask URL
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("127.0.0.1"));
    }

    #[test]
    fn builder_methods() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::Stream {
                key: "events".into(),
                group: Some("mygroup".into()),
                consumer: Some("worker1".into()),
                count: Some(100),
            },
        )
        .max_records(500);
        assert_eq!(config.max_records, Some(500));
    }

    #[test]
    fn keys_source_type() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::Keys {
                pattern: "user:*".into(),
            },
        );
        if let RedisSourceType::Keys { ref pattern } = config.source_type {
            assert_eq!(pattern, "user:*");
        } else {
            panic!("expected Keys variant");
        }
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "k".into() },
        );
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "k".into() },
        )
        .with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "k".into() },
        )
        .with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = RedisSourceConfig::new(
            "redis://localhost",
            RedisSourceType::List { key: "k".into() },
        )
        .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "url": "redis://localhost",
            "source_type": { "type": "List", "key": "items" },
            "batch_size": 250
        }"#;
        let config: RedisSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_field_omitted_defaults_to_default_batch_size() {
        let json = r#"{
            "url": "redis://localhost",
            "source_type": { "type": "List", "key": "items" }
        }"#;
        let config: RedisSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
