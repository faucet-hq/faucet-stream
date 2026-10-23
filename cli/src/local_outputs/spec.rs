//! Serde config types for the top-level `local_outputs:` block (#587).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_track() -> bool {
    true
}

/// The top-level `local_outputs:` block: how long the local files this
/// pipeline's sinks write are kept, and whether faucet records them at all.
///
/// ```yaml
/// local_outputs:
///   retention_days: 3   # this pipeline's outputs are collectable after 3 days
/// ```
///
/// The block is optional. Without it a pipeline still records its local outputs
/// (so they can be listed and cleaned) and inherits the runtime's default
/// window — `--local-output-retention-days` /
/// `FAUCET_LOCAL_SINK_OUTPUT_RETENTION_DAYS`, itself 7 days by default.
///
/// Recording needs somewhere to record *to*: under `faucet serve` that is the
/// `--history` backend; for `faucet run` / `schedule` / `replicate` it is the
/// `catalog:` block's store. Without either, tracking is inert and a one-line
/// warning says so rather than failing the run — a pipeline must never break
/// because a housekeeping ledger was unavailable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LocalOutputsSpec {
    /// Record the local files this pipeline's sinks write, so they can be listed
    /// in the console and reclaimed by the GC. Default `true`.
    ///
    /// Setting `false` opts the pipeline out of the ledger entirely: its outputs
    /// are never listed and — because the GC only ever deletes recorded paths —
    /// never automatically deleted either.
    #[serde(default = "default_track")]
    pub track: bool,

    /// Retention window for this pipeline's outputs, in days, overriding the
    /// runtime default. `0` keeps them forever.
    ///
    /// Measured from the last time faucet wrote the file, so an output a local
    /// run keeps refreshing does not expire underneath it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_days: Option<u32>,
}

impl Default for LocalOutputsSpec {
    fn default() -> Self {
        Self {
            track: true,
            retention_days: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_block_tracks_and_inherits_the_default_window() {
        let spec: LocalOutputsSpec = serde_yaml_bw::from_str("{}").unwrap();
        assert!(spec.track);
        assert_eq!(spec.retention_days, None);
        assert_eq!(spec, LocalOutputsSpec::default());
    }

    #[test]
    fn parses_a_retention_override() {
        let spec: LocalOutputsSpec = serde_yaml_bw::from_str("retention_days: 3").unwrap();
        assert_eq!(spec.retention_days, Some(3));
        assert!(spec.track, "tracking stays on unless explicitly disabled");
    }

    #[test]
    fn parses_an_opt_out() {
        let spec: LocalOutputsSpec = serde_yaml_bw::from_str("track: false").unwrap();
        assert!(!spec.track);
    }

    #[test]
    fn zero_days_parses_as_keep_forever() {
        let spec: LocalOutputsSpec = serde_yaml_bw::from_str("retention_days: 0").unwrap();
        assert_eq!(spec.retention_days, Some(0));
    }

    #[test]
    fn rejects_unknown_fields() {
        // A typo'd retention knob must fail loudly, not silently keep files
        // forever.
        let err = serde_yaml_bw::from_str::<LocalOutputsSpec>("retention_dayz: 3").unwrap_err();
        assert!(err.to_string().contains("retention_dayz"), "{err}");
    }

    #[test]
    fn schema_generates_with_both_fields() {
        let schema = schemars::schema_for!(LocalOutputsSpec);
        let v = serde_json::to_value(&schema).unwrap();
        assert!(v["properties"]["track"].is_object());
        assert!(v["properties"]["retention_days"].is_object());
    }

    #[test]
    fn round_trips_through_yaml() {
        let spec = LocalOutputsSpec {
            track: false,
            retention_days: Some(14),
        };
        let yaml = serde_yaml_bw::to_string(&spec).unwrap();
        assert_eq!(
            serde_yaml_bw::from_str::<LocalOutputsSpec>(&yaml).unwrap(),
            spec
        );
    }
}
