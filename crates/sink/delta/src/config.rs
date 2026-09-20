//! Delta Lake sink configuration.

use faucet_common_delta::DeltaConnection;
use faucet_core::{DEFAULT_BATCH_SIZE, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default number of records sampled to infer the table schema on first write.
pub const DEFAULT_SAMPLE_SIZE: usize = 100;

fn default_true() -> bool {
    true
}

fn default_sample_size() -> usize {
    DEFAULT_SAMPLE_SIZE
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Configuration for the Delta Lake sink connector (append-only in v1).
///
/// The `table_uri` / `credentials` / `storage_options` keys are flattened in
/// from the shared [`DeltaConnection`]. On first write the sink opens the
/// table (creating it from the inferred schema when `create_if_not_missing`),
/// then appends one Delta commit per `flush()`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
// Serde aliases are invisible to schemars, and the registry's unknown-key gate
// (#654 H9) reads the schema — so a key serde accepts but the schema does not
// list would be rejected before it ever reached serde. Declared here so the
// two agree.
#[schemars(extend("x-faucet-aliases" = ["create_if_not_missing"]))]
pub struct DeltaSinkConfig {
    /// Table location + object-store credentials.
    #[serde(flatten)]
    pub connection: DeltaConnection,

    /// Create the table (and its schema, from the inferred record shape) on the
    /// first write when it does not already exist. When `false`, an absent
    /// table fails the first write with a typed error.
    ///
    /// Spelled `create_table` on the wire since #580, matching every other
    /// table sink; the historical `create_if_not_missing` stays accepted as an
    /// alias. Same default (`true`): a first-ever sync cannot assume the
    /// destination exists.
    #[serde(
        default = "default_true",
        rename = "create_table",
        alias = "create_if_not_missing"
    )]
    pub create_if_not_missing: bool,

    /// Partition columns, applied only when the table is created. Ignored when
    /// appending to an existing table (its stored partitioning is used).
    #[serde(default)]
    pub partition_by: Vec<String>,

    /// Records sampled to infer the Arrow/Delta schema when creating the table.
    #[serde(default = "default_sample_size")]
    pub schema_sample_size: usize,

    /// Re-chunk size for [`Sink::write_batch`](faucet_core::Sink::write_batch).
    /// Incoming pages larger than this are sliced into `batch_size`-row Arrow
    /// batches before writing. `0` writes each page through as one batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Optional target data-file size (bytes) hint for the writer's rollover.
    /// Advisory — delta-rs rolls files at buffer boundaries near this size.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_file_size: Option<usize>,
}

impl DeltaSinkConfig {
    /// Build a sink config for `table_uri` with defaults.
    pub fn new(table_uri: impl Into<String>) -> Self {
        Self {
            connection: DeltaConnection {
                table_uri: table_uri.into(),
                credentials: Default::default(),
                storage_options: Default::default(),
            },
            create_if_not_missing: true,
            partition_by: Vec::new(),
            schema_sample_size: DEFAULT_SAMPLE_SIZE,
            batch_size: DEFAULT_BATCH_SIZE,
            target_file_size: None,
        }
    }

    /// Validate the config; returns a human-readable error string if invalid.
    pub fn validate(&self) -> Result<(), String> {
        if self.connection.table_uri.trim().is_empty() {
            return Err("delta sink: `table_uri` must not be empty".to_string());
        }
        if self.schema_sample_size == 0 {
            return Err("delta sink: `schema_sample_size` must be greater than 0".to_string());
        }
        if matches!(self.target_file_size, Some(0)) {
            return Err(
                "delta sink: `target_file_size` must be greater than 0 when set".to_string(),
            );
        }
        validate_batch_size(self.batch_size)
            .map_err(|e| format!("delta sink: invalid batch_size: {e}"))?;
        Ok(())
    }

    /// Effective schema sample size (never zero after validation).
    pub fn effective_sample_size(&self) -> usize {
        self.schema_sample_size.max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_has_sane_defaults() {
        let c = DeltaSinkConfig::new("file:///tmp/t");
        assert!(c.create_if_not_missing);
        assert_eq!(c.schema_sample_size, DEFAULT_SAMPLE_SIZE);
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        assert!(c.partition_by.is_empty());
        c.validate().unwrap();
    }

    #[test]
    fn rejects_empty_uri_and_zero_sample() {
        assert!(DeltaSinkConfig::new("").validate().is_err());
        let mut c = DeltaSinkConfig::new("file:///tmp/t");
        c.schema_sample_size = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_zero_target_file_size() {
        let mut c = DeltaSinkConfig::new("file:///tmp/t");
        c.target_file_size = Some(0);
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_oversized_batch() {
        let mut c = DeltaSinkConfig::new("file:///tmp/t");
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn deserializes_minimal_applies_defaults() {
        // Only `table_uri` → every `#[serde(default = …)]` fn runs.
        let c: DeltaSinkConfig =
            serde_json::from_value(json!({ "table_uri": "file:///t" })).unwrap();
        assert!(c.create_if_not_missing);
        assert_eq!(c.schema_sample_size, DEFAULT_SAMPLE_SIZE);
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert_eq!(c.effective_sample_size(), DEFAULT_SAMPLE_SIZE);
        assert!(c.target_file_size.is_none());
        c.validate().unwrap();
    }

    #[test]
    fn deserializes_flattened_shape_with_partitions() {
        let v = json!({
            "table_uri": "s3://b/t",
            "partition_by": ["dt"],
            "create_if_not_missing": false,
            "storage_options": { "AWS_ALLOW_HTTP": "true" }
        });
        let c: DeltaSinkConfig = serde_json::from_value(v).unwrap();
        assert_eq!(c.partition_by, vec!["dt"]);
        assert!(!c.create_if_not_missing);
        assert_eq!(
            c.connection.merged_storage_options()["AWS_ALLOW_HTTP"],
            "true"
        );
        c.validate().unwrap();
    }
}
