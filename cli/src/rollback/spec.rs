//! The top-level `rollback:` block (#706) — make every run of this config
//! undoable with `faucet rollback`.

use faucet_core::FaucetError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How many undoable runs to keep per row (older journals / markers are
/// pruned as new runs complete).
pub const DEFAULT_RETAIN: usize = 10;

fn default_true() -> bool {
    true
}
fn default_retain() -> usize {
    DEFAULT_RETAIN
}

/// Top-level `rollback:` block.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RollbackSpec {
    /// Master switch (default `true` when the block is present).
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Journal the before-image of every key an upsert / delete run touches,
    /// in the same transaction as the write (default `true`). Required to
    /// undo an upsert; an append run needs only the run-id column.
    #[serde(default = "default_true")]
    pub journal: bool,
    /// Keep the table an overwrite replaces as `<table>__faucet_prev` so the
    /// overwrite can be swapped back (default `true`).
    #[serde(default = "default_true")]
    pub keep_previous: bool,
    /// Undoable runs kept per row (default 10). The oldest run's journal rows
    /// and marker are dropped when a new run completes.
    #[serde(default = "default_retain")]
    pub retain: usize,
}

impl Default for RollbackSpec {
    fn default() -> Self {
        Self {
            enabled: true,
            journal: true,
            keep_previous: true,
            retain: DEFAULT_RETAIN,
        }
    }
}

impl RollbackSpec {
    /// Fail-fast validation at config load.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.retain == 0 {
            return Err(FaucetError::Config(
                "rollback: `retain` must be at least 1 (set `enabled: false` to turn rollback off)"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_validation() {
        let spec: RollbackSpec = serde_yaml::from_str("{}").unwrap();
        assert_eq!(spec, RollbackSpec::default());
        assert!(spec.enabled && spec.journal && spec.keep_previous);
        assert_eq!(spec.retain, 10);
        spec.validate().unwrap();
        let bad = RollbackSpec {
            retain: 0,
            ..RollbackSpec::default()
        };
        assert!(bad.validate().unwrap_err().to_string().contains("retain"));
        assert!(serde_yaml::from_str::<RollbackSpec>("journals: true").is_err());
    }
}
