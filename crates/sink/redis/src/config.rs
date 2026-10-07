//! Redis sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The type of Redis data structure to write to.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum RedisSinkType {
    /// Append records to a Redis list via `RPUSH`.
    List {
        /// The list key.
        key: String,
    },
    /// Add entries to a Redis stream via `XADD`.
    Stream {
        /// The stream key.
        key: String,
    },
    /// Set individual keys via `SET`, using a field from each record as the key.
    KeyValue {
        /// The field name in each record whose value becomes the Redis key.
        key_field: String,
    },
}

/// Configuration for the Redis sink connector.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RedisSinkConfig {
    /// Redis connection URL (e.g. `"redis://127.0.0.1:6379"`).
    pub url: String,
    /// The type of Redis data structure to write to.
    pub sink_type: RedisSinkType,
    /// Maximum number of commands packed into a single Redis pipeline.
    /// Defaults to [`DEFAULT_BATCH_SIZE`] (1000), which is a comfortable
    /// per-pipeline working set for Redis: pipelined commands are cheap,
    /// so bigger windows amortise the round-trip but very large pipelines
    /// can stall other clients sharing the server.
    ///
    /// When `write_batch` receives a slice larger than `batch_size`, the
    /// sink re-chunks it into `batch_size` slices and issues one Redis
    /// pipeline per chunk. `batch_size = 0` is the **"no batching"
    /// sentinel**: the entire records slice is packed into a single
    /// Redis pipeline, no matter how large, preserving upstream
    /// [`StreamPage`](faucet_core::StreamPage) framing.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// `KeyValue` only: expire every written key after this many seconds
    /// (`SET … EX`). Keys never expire when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u64>,
    /// `Stream` only: cap the stream at about this many entries
    /// (`XADD … MAXLEN ~ n`), trimming the oldest. Unbounded when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_max_len: Option<u64>,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl std::fmt::Debug for RedisSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisSinkConfig")
            .field("url", &"***")
            .field("sink_type", &self.sink_type)
            .field("batch_size", &self.batch_size)
            .field("ttl_secs", &self.ttl_secs)
            .field("stream_max_len", &self.stream_max_len)
            .finish()
    }
}

impl RedisSinkConfig {
    /// Create a new config with the given URL and sink type.
    pub fn new(url: impl Into<String>, sink_type: RedisSinkType) -> Self {
        Self {
            url: url.into(),
            sink_type,
            batch_size: DEFAULT_BATCH_SIZE,
            ttl_secs: None,
            stream_max_len: None,
        }
    }

    /// Refuse options that do not apply to the configured `sink_type`, or a
    /// zero TTL / length (which Redis rejects or would empty the stream).
    pub fn validate(&self) -> Result<(), faucet_core::FaucetError> {
        faucet_core::validate_batch_size(self.batch_size)?;
        let err = |m: &str| Err(faucet_core::FaucetError::Config(format!("redis sink: {m}")));
        match (&self.sink_type, self.ttl_secs, self.stream_max_len) {
            (_, Some(0), _) => err("ttl_secs must be greater than 0"),
            (_, _, Some(0)) => err("stream_max_len must be greater than 0"),
            (RedisSinkType::KeyValue { .. }, _, Some(_))
            | (RedisSinkType::List { .. }, _, Some(_)) => {
                err("stream_max_len applies only to the Stream sink type")
            }
            (RedisSinkType::Stream { .. }, Some(_), _)
            | (RedisSinkType::List { .. }, Some(_), _) => {
                err("ttl_secs applies only to the KeyValue sink type")
            }
            _ => Ok(()),
        }
    }

    /// Set the maximum number of commands per Redis pipeline.
    ///
    /// Pass `0` to opt out of re-chunking — the entire records slice handed
    /// to `write_batch` is packed into a single Redis pipeline, preserving
    /// upstream [`StreamPage`](faucet_core::StreamPage) framing.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

impl RedisSinkConfig {
    /// What a failed batch write leaves behind (#737): pipelines are not MULTI/EXEC transactions.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        faucet_core::BatchAtomicity::BestEffort
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = RedisSinkConfig::new(
            "redis://127.0.0.1:6379",
            RedisSinkType::List {
                key: "my_list".into(),
            },
        );
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        // Debug should mask URL
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("127.0.0.1"));
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = RedisSinkConfig::new(
            "redis://localhost",
            RedisSinkType::Stream {
                key: "events".into(),
            },
        )
        .with_batch_size(100);
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn key_value_sink_type() {
        let config = RedisSinkConfig::new(
            "redis://localhost",
            RedisSinkType::KeyValue {
                key_field: "id".into(),
            },
        );
        if let RedisSinkType::KeyValue { ref key_field } = config.sink_type {
            assert_eq!(key_field, "id");
        } else {
            panic!("expected KeyValue variant");
        }
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config =
            RedisSinkConfig::new("redis://localhost", RedisSinkType::List { key: "k".into() })
                .with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config =
            RedisSinkConfig::new("redis://localhost", RedisSinkType::List { key: "k".into() })
                .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "url": "redis://localhost",
            "sink_type": { "type": "List", "key": "items" },
            "batch_size": 250
        }"#;
        let config: RedisSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_from_json() {
        let json = r#"{
            "url": "redis://localhost",
            "sink_type": { "type": "List", "key": "items" }
        }"#;
        let config: RedisSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn options_must_match_the_sink_type() {
        let kv = RedisSinkType::KeyValue {
            key_field: "id".into(),
        };
        let stream = RedisSinkType::Stream { key: "s".into() };
        let list = RedisSinkType::List { key: "l".into() };
        let with = |t: &RedisSinkType, ttl, len| {
            let mut c = RedisSinkConfig::new("redis://localhost", t.clone());
            c.ttl_secs = ttl;
            c.stream_max_len = len;
            c.validate()
        };
        assert!(with(&kv, Some(60), None).is_ok());
        assert!(with(&stream, None, Some(1000)).is_ok());
        assert!(with(&list, None, None).is_ok());
        assert!(with(&kv, Some(0), None).is_err());
        assert!(with(&stream, None, Some(0)).is_err());
        assert!(with(&kv, None, Some(5)).is_err());
        assert!(with(&list, None, Some(5)).is_err());
        assert!(with(&stream, Some(5), None).is_err());
        assert!(with(&list, Some(5), None).is_err());
    }

    #[test]
    fn batch_atomicity_matches_the_write_path() {
        let c: RedisSinkConfig = serde_json::from_value(serde_json::json!({"url": "redis://localhost", "sink_type": {"type": "List", "key": "items"}})).unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }
}
