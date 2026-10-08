//! Delta Lake source configuration.

use faucet_common_delta::DeltaConnection;
use faucet_core::{DEFAULT_BATCH_SIZE, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Configuration for the Delta Lake source connector.
///
/// The `table_uri` / `credentials` / `storage_options` keys are flattened in
/// from the shared [`DeltaConnection`]. Reads scan the table's active data
/// files at the latest version (or a pinned `version` / `timestamp` for time
/// travel) and yield each row as a JSON object.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DeltaSourceConfig {
    /// Table location + object-store credentials.
    #[serde(flatten)]
    pub connection: DeltaConnection,

    /// Time travel: read the table as of this version. Mutually exclusive with
    /// `timestamp`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,

    /// Time travel: read the table as of this RFC 3339 timestamp. Mutually
    /// exclusive with `version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,

    /// Projection pushdown: only read these columns. Empty = all columns.
    #[serde(default)]
    pub columns: Vec<String>,

    /// Page size hint for [`stream_pages`](faucet_core::Source::stream_pages).
    /// Governs how many rows accumulate before a `StreamPage` is emitted. `0`
    /// is the "no batching" sentinel — one page per data file, however many
    /// row groups it holds.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl DeltaSourceConfig {
    /// Build a source config for `table_uri` with defaults.
    pub fn new(table_uri: impl Into<String>) -> Self {
        Self {
            connection: DeltaConnection {
                table_uri: table_uri.into(),
                credentials: Default::default(),
                storage_options: Default::default(),
            },
            version: None,
            timestamp: None,
            columns: Vec::new(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Validate the config; returns a human-readable error string if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.connection.table_uri.trim().is_empty() {
            return Err("delta source: `table_uri` must not be empty".to_string());
        }
        if self.version.is_some() && self.timestamp.is_some() {
            return Err(
                "delta source: `version` and `timestamp` are mutually exclusive".to_string(),
            );
        }
        validate_batch_size(self.batch_size)
            .map_err(|e| format!("delta source: invalid batch_size: {e}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_has_sane_defaults() {
        let c = DeltaSourceConfig::new("file:///tmp/t");
        assert_eq!(c.connection.table_uri, "file:///tmp/t");
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        assert!(c.columns.is_empty());
        c.validate().unwrap();
    }

    #[test]
    fn rejects_empty_uri() {
        let mut c = DeltaSourceConfig::new("");
        assert!(c.validate().is_err());
        c.connection.table_uri = "   ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_version_and_timestamp_together() {
        let mut c = DeltaSourceConfig::new("file:///tmp/t");
        c.version = Some(3);
        c.timestamp = Some("2026-01-01T00:00:00Z".into());
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_oversized_batch() {
        let mut c = DeltaSourceConfig::new("file:///tmp/t");
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn deserializes_flattened_shape() {
        let v = json!({
            "table_uri": "s3://b/t",
            "credentials": { "type": "aws", "config": { "region": "us-east-1" } },
            "columns": ["id", "name"],
            "version": 5
        });
        let c: DeltaSourceConfig = serde_json::from_value(v).unwrap();
        assert_eq!(c.connection.table_uri, "s3://b/t");
        assert_eq!(c.columns, vec!["id", "name"]);
        assert_eq!(c.version, Some(5));
        c.validate().unwrap();
    }
}
