//! The top-level `verify:` block (#701) — what `faucet verify` compares and
//! how, and whether a run verifies itself after writing.

use crate::config::ConnectorSpec;
use faucet_core::FaucetError;
use faucet_core::diff::Normalizer;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Default number of key ranges in the first digest pass.
pub const DEFAULT_RANGES: usize = 16;
/// Default leaf size: a differing range at or below this many rows is fetched
/// and diffed row by row instead of being bisected further.
pub const DEFAULT_LEAF_ROWS: u64 = 1_000;
/// Default cap on differences reported (the scan keeps counting).
pub const DEFAULT_MAX_DIFFERENCES: usize = 1_000;

fn default_ranges() -> usize {
    DEFAULT_RANGES
}
fn default_leaf_rows() -> u64 {
    DEFAULT_LEAF_ROWS
}
fn default_max_differences() -> usize {
    DEFAULT_MAX_DIFFERENCES
}
fn default_exclude() -> Vec<String> {
    vec!["_faucet_*".to_string()]
}
fn default_true() -> bool {
    true
}

/// Top-level `verify:` block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct VerifySpec {
    /// Key columns rows are matched on. Defaults to the sink's `key`
    /// (`write_mode: upsert`); required otherwise. A single integer key
    /// enables range bisection; any other key compares the whole dataset.
    #[serde(default)]
    pub key: Vec<String>,
    /// Columns to compare. Default: every column present on either side,
    /// minus `exclude`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<String>>,
    /// Columns to ignore — exact names or `prefix*` globs. Default
    /// `["_faucet_*"]` (the metadata columns a run stamps).
    #[serde(default = "default_exclude")]
    pub exclude: Vec<String>,
    /// How to read the destination back. Default: the sink's own read-back
    /// (`postgres` / `mysql` / `sqlite` in column mode); required for sinks
    /// that cannot describe one (files, queues).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<ConnectorSpec>,
    /// Key ranges in the first digest pass (default 16).
    #[serde(default = "default_ranges")]
    pub ranges: usize,
    /// A differing range at or below this many rows is fetched and diffed row
    /// by row (default 1000).
    #[serde(default = "default_leaf_rows")]
    pub leaf_rows: u64,
    /// Stop reporting (but keep counting) after this many differences
    /// (default 1000).
    #[serde(default = "default_max_differences")]
    pub max_differences: usize,
    /// Stop scanning after this many rows have been fetched from either side;
    /// the report is then marked `truncated`. Default: no cap.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows_scanned: Option<u64>,
    /// How values are canonicalised before comparing.
    #[serde(default)]
    pub normalize: Normalizer,
    /// After a successful run (`faucet run` / `schedule` / `serve`), verify
    /// the destination against the source automatically (default `true`
    /// when the block is present).
    #[serde(default = "default_true")]
    pub after_run: bool,
    /// Fail the run when the post-run verification finds differences
    /// (default `true`). With `false` differences are reported and counted
    /// but the run stays green.
    #[serde(default = "default_true")]
    pub fail_on_difference: bool,
    /// Re-sync differing keys through the pipeline's own write path (the
    /// `--repair` default; `faucet verify --repair` forces it on).
    #[serde(default)]
    pub repair: bool,
    /// Let a repair delete destination rows the source no longer has
    /// (`--allow-delete`). Off by default: a delete is not undoable.
    #[serde(default)]
    pub allow_delete: bool,
}

impl Default for VerifySpec {
    fn default() -> Self {
        Self {
            key: Vec::new(),
            columns: None,
            exclude: default_exclude(),
            destination: None,
            ranges: DEFAULT_RANGES,
            leaf_rows: DEFAULT_LEAF_ROWS,
            max_differences: DEFAULT_MAX_DIFFERENCES,
            max_rows_scanned: None,
            normalize: Normalizer::default(),
            after_run: true,
            fail_on_difference: true,
            repair: false,
            allow_delete: false,
        }
    }
}

impl VerifySpec {
    /// Fail-fast validation at config load.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.ranges == 0 {
            return Err(FaucetError::Config(
                "verify: `ranges` must be at least 1".into(),
            ));
        }
        if self.leaf_rows == 0 {
            return Err(FaucetError::Config(
                "verify: `leaf_rows` must be at least 1".into(),
            ));
        }
        if self.max_differences == 0 {
            return Err(FaucetError::Config(
                "verify: `max_differences` must be at least 1".into(),
            ));
        }
        if let Some(cols) = &self.columns
            && cols.is_empty()
        {
            return Err(FaucetError::Config(
                "verify: `columns` must list at least one column when set (omit it to compare every column)".into(),
            ));
        }
        if self.key.iter().any(|k| k.trim().is_empty()) {
            return Err(FaucetError::Config(
                "verify: `key` entries must not be empty".into(),
            ));
        }
        if let Some(dest) = &self.destination
            && dest.kind.trim().is_empty()
        {
            return Err(FaucetError::Config(
                "verify: `destination.type` must name a source connector".into(),
            ));
        }
        self.normalize.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_validation() {
        let spec: VerifySpec = serde_yaml::from_str("{}").unwrap();
        assert_eq!(spec, VerifySpec::default());
        assert_eq!(spec.exclude, vec!["_faucet_*".to_string()]);
        assert!(spec.after_run && spec.fail_on_difference && !spec.repair);
        spec.validate().unwrap();

        let bad = VerifySpec {
            ranges: 0,
            ..VerifySpec::default()
        };
        assert!(bad.validate().unwrap_err().to_string().contains("ranges"));
        let bad = VerifySpec {
            leaf_rows: 0,
            ..VerifySpec::default()
        };
        assert!(
            bad.validate()
                .unwrap_err()
                .to_string()
                .contains("leaf_rows")
        );
        let bad = VerifySpec {
            max_differences: 0,
            ..VerifySpec::default()
        };
        assert!(
            bad.validate()
                .unwrap_err()
                .to_string()
                .contains("max_differences")
        );
        let bad = VerifySpec {
            columns: Some(vec![]),
            ..VerifySpec::default()
        };
        assert!(bad.validate().unwrap_err().to_string().contains("columns"));
        let bad = VerifySpec {
            key: vec![" ".into()],
            ..VerifySpec::default()
        };
        assert!(bad.validate().unwrap_err().to_string().contains("key"));
        let bad: VerifySpec =
            serde_yaml::from_str("destination: { type: '', config: {} }").unwrap();
        assert!(
            bad.validate()
                .unwrap_err()
                .to_string()
                .contains("destination")
        );
    }

    #[test]
    fn unknown_keys_are_rejected() {
        assert!(serde_yaml::from_str::<VerifySpec>("leaf: 5").is_err());
    }
}
