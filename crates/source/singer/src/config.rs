//! Configuration types for the Singer tap bridge source.
//!
//! No I/O or protocol logic lives here — just the serde/`JsonSchema` config
//! surface, following the connector-crate convention.

use faucet_core::JsonSchema;
use faucet_core::Value;
use serde::{Deserialize, Serialize};

/// What to do when the tap emits a line that is not valid Singer JSON.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum MalformedPolicy {
    /// Log the offending line at `warn` and continue (default).
    #[default]
    Skip,
    /// Abort the run with a [`faucet_core::FaucetError::Source`].
    Fail,
}

/// Configuration for the Singer tap bridge source.
///
/// Runs an external [Singer](https://www.singer.io/) tap executable and adapts
/// its stdout message stream into faucet records. **v0 is single-stream only**:
/// exactly the stream named by [`stream`](Self::stream) is emitted; RECORD
/// messages for other streams are ignored.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SingerSourceConfig {
    /// The tap executable to run (looked up on `PATH`, or an absolute path),
    /// e.g. `tap-github` or `/opt/taps/tap-csv`.
    pub executable: String,

    /// Extra positional/flag arguments appended after faucet's own
    /// `--config` / `--catalog` / `--state` flags.
    #[serde(default)]
    pub args: Vec<String>,

    /// The tap's configuration object. faucet's loader has already resolved any
    /// `${secret:…}` references before this reaches the source; it is
    /// materialized to a private temp file and passed as `--config`.
    #[serde(default)]
    pub tap_config: Value,

    /// Optional Singer catalog object, passed as `--catalog` when present.
    /// Selects streams/fields; required by most SDK-based taps.
    #[serde(default)]
    pub catalog: Option<Value>,

    /// The single stream to emit records for. RECORD messages for any other
    /// stream are dropped (v0 is single-stream).
    pub stream: String,

    /// State-store key for the resume bookmark. Defaults to
    /// `singer:{executable}:{stream}` when unset.
    #[serde(default)]
    pub state_key: Option<String>,

    /// Flush the current page whenever the tap emits a STATE message (so the
    /// pipeline can checkpoint at the tap's own commit points). Default `true`.
    #[serde(default = "default_true")]
    pub flush_on_state: bool,

    /// Abort the run if no line arrives from the tap within this many seconds.
    /// `None` (default) waits indefinitely.
    #[serde(default)]
    pub idle_timeout_secs: Option<u64>,

    /// How to handle a line that is not valid Singer JSON. Default
    /// [`MalformedPolicy::Skip`].
    #[serde(default)]
    pub on_malformed: MalformedPolicy,
}

fn default_true() -> bool {
    true
}

impl SingerSourceConfig {
    /// Convenience constructor for the two required fields; other fields take
    /// their serde defaults.
    pub fn new(executable: impl Into<String>, stream: impl Into<String>) -> Self {
        Self {
            executable: executable.into(),
            args: Vec::new(),
            tap_config: Value::Object(Default::default()),
            catalog: None,
            stream: stream.into(),
            state_key: None,
            flush_on_state: true,
            idle_timeout_secs: None,
            on_malformed: MalformedPolicy::Skip,
        }
    }

    /// The effective state-store key: the configured `state_key`, or the
    /// derived default `singer:{executable}:{stream}`.
    ///
    /// The derived key is sanitized so a path-style `executable` (e.g.
    /// `/opt/taps/tap-csv`) still yields a valid state key — `validate_state_key`
    /// only permits `[A-Za-z0-9_\-:.]`, so any other character (notably `/`) is
    /// replaced with `_`.
    pub fn effective_state_key(&self) -> String {
        self.state_key
            .clone()
            .unwrap_or_else(|| format!("singer:{}:{}", sanitize(&self.executable), self.stream))
    }
}

/// Replace characters not allowed in a state key with `_`.
fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_apply() {
        let cfg: SingerSourceConfig =
            serde_json::from_value(serde_json::json!({"executable": "tap-x", "stream": "s"}))
                .unwrap();
        assert!(cfg.flush_on_state);
        assert_eq!(cfg.on_malformed, MalformedPolicy::Skip);
        assert!(cfg.args.is_empty());
        assert!(cfg.catalog.is_none());
        assert!(cfg.idle_timeout_secs.is_none());
    }

    #[test]
    fn default_state_key_derivation() {
        let cfg = SingerSourceConfig::new("tap-github", "issues");
        assert_eq!(cfg.effective_state_key(), "singer:tap-github:issues");
        let cfg2 = SingerSourceConfig {
            state_key: Some("custom".into()),
            ..SingerSourceConfig::new("tap-github", "issues")
        };
        assert_eq!(cfg2.effective_state_key(), "custom");
    }

    #[test]
    fn derived_state_key_is_valid_even_for_path_executable() {
        let cfg = SingerSourceConfig::new("/opt/taps/tap-csv", "s");
        let key = cfg.effective_state_key();
        assert_eq!(key, "singer:_opt_taps_tap-csv:s");
        // must satisfy the core state-key validator
        faucet_core::state::validate_state_key(&key).expect("derived key must be valid");
    }

    #[test]
    fn malformed_policy_parses_snake_case() {
        let p: MalformedPolicy = serde_json::from_value(serde_json::json!("fail")).unwrap();
        assert_eq!(p, MalformedPolicy::Fail);
    }
}
