//! Configuration types for the Singer target bridge sink.
//!
//! No I/O or protocol logic lives here — just the serde/`JsonSchema` config
//! surface, following the connector-crate convention.

use std::collections::BTreeMap;

pub use faucet_common_singer::InheritEnv;
use faucet_core::{FaucetError, JsonSchema, Value, WriteMode, WriteSpec};
use serde::{Deserialize, Serialize};

/// When the sink treats written records as durably persisted by the target.
///
/// Most Singer targets (including the Meltano SDK ones) emit `STATE` only when
/// they drain their buffers — usually at end of input — so `exit` is the
/// default: it works with every target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum FlushOn {
    /// Send a `STATE`, close the target's stdin and wait for a successful exit;
    /// the next write starts a fresh target process (default).
    #[default]
    Exit,
    /// Keep one long-lived target: send a `STATE` and wait for the target to
    /// echo it back — the Singer contract for "everything before this state is
    /// persisted". For targets that echo `STATE` as soon as the preceding
    /// records are persisted.
    State,
}

/// Configuration for the Singer target bridge sink.
///
/// Runs an external [Singer](https://www.singer.io/) target executable and
/// feeds it faucet records as one Singer stream (`SCHEMA` / `RECORD` /
/// `STATE`, plus `ACTIVATE_VERSION` for `write_mode: overwrite`).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SingerSinkConfig {
    /// The target executable to run (looked up on `PATH`, or an absolute
    /// path), e.g. `target-jsonl` or `/opt/targets/target-postgres`.
    pub target_command: String,

    /// Extra arguments appended after faucet's own `--config <file>` flag.
    #[serde(default)]
    pub args: Vec<String>,

    /// The target's configuration object. Written to a private (0600) temp
    /// file passed as `--config`; its string values are scrubbed from the
    /// target's echoed stderr.
    #[serde(default = "empty_object")]
    pub target_config: Value,

    /// Extra environment variables for the target process, set on top of
    /// whatever `inherit_env` passes.
    #[serde(default)]
    pub env: BTreeMap<String, String>,

    /// Which of faucet's environment variables the target receives: `true`
    /// (default) the whole environment, `false` only `PATH`, `HOME`, `LANG`,
    /// `LC_ALL` and `TMPDIR`, or a list of variable names passed on top of
    /// that baseline. Set it to keep faucet's own credentials away from the
    /// target.
    #[serde(default)]
    pub inherit_env: InheritEnv,

    /// The Singer stream name records are sent under. The `faucet` CLI fills
    /// it from the matrix row id (or the pipeline name for a single-row
    /// config) when unset; Template Hub sink templates set it to `${stream}`.
    /// Library callers default to `faucet`.
    #[serde(default)]
    pub stream: Option<String>,

    /// The JSON Schema sent in the stream's `SCHEMA` message. When unset the
    /// `faucet` CLI uses the pipeline's `contract:` (as JSON Schema) if one is
    /// declared; otherwise the schema is inferred from the records and a new
    /// `SCHEMA` is sent whenever a page widens it.
    #[serde(default)]
    pub schema: Option<Value>,

    /// `key_properties` for the `SCHEMA` message. Defaults to `key` under
    /// `write_mode: upsert`, else empty.
    #[serde(default)]
    pub key_properties: Option<Vec<String>>,

    /// When written records count as persisted: `exit` (default) closes the
    /// target at every flush and waits for a clean exit; `state` keeps it
    /// running and waits for it to echo a `STATE`.
    #[serde(default)]
    pub flush_on: FlushOn,

    /// How long a flush waits for the target to echo `STATE` (or to exit, with
    /// `flush_on: exit`) before failing, and how long one write may block on a
    /// target that stopped reading its stdin. Default 600.
    #[serde(default = "default_flush_timeout")]
    pub flush_timeout_secs: u64,

    /// Write mode: `append` (default), `upsert` (sets `key_properties` from
    /// `key` so the target can merge), or `overwrite` (versions every record
    /// and sends `ACTIVATE_VERSION` after a successful run, so the target
    /// drops rows from earlier versions). `delete` is not part of the Singer
    /// protocol.
    #[serde(flatten)]
    pub write: WriteSpec,

    /// Internal: the table version for `write_mode: overwrite`, shared by
    /// every writer of a run. Injected by the `faucet` CLI (the run clock in
    /// milliseconds); library callers may leave it unset (a millisecond
    /// timestamp is chosen on first use).
    #[serde(default, rename = "_activate_version")]
    pub activate_version: Option<i64>,
}

fn empty_object() -> Value {
    Value::Object(Default::default())
}

fn default_flush_timeout() -> u64 {
    600
}

impl SingerSinkConfig {
    /// Convenience constructor for the required field; other fields take their
    /// serde defaults.
    pub fn new(target_command: impl Into<String>) -> Self {
        Self {
            target_command: target_command.into(),
            args: Vec::new(),
            target_config: empty_object(),
            env: BTreeMap::new(),
            inherit_env: InheritEnv::default(),
            stream: None,
            schema: None,
            key_properties: None,
            flush_on: FlushOn::Exit,
            flush_timeout_secs: default_flush_timeout(),
            write: WriteSpec::default(),
            activate_version: None,
        }
    }

    /// Validate the config: a non-empty command and stream, a supported write
    /// mode, an object schema, and a positive flush timeout.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.target_command.trim().is_empty() {
            return Err(FaucetError::Config(
                "singer sink: `target_command` must not be empty".into(),
            ));
        }
        if self.stream.as_deref().is_some_and(|s| s.trim().is_empty()) {
            return Err(FaucetError::Config(
                "singer sink: `stream` must not be empty".into(),
            ));
        }
        if self.flush_timeout_secs == 0 {
            return Err(FaucetError::Config(
                "singer sink: `flush_timeout_secs` must be greater than 0".into(),
            ));
        }
        if let Some(schema) = &self.schema
            && !schema.is_object()
        {
            return Err(FaucetError::Config(
                "singer sink: `schema` must be a JSON Schema object".into(),
            ));
        }
        if matches!(self.write.write_mode, WriteMode::Delete) {
            return Err(FaucetError::Config(
                "singer sink: write_mode `delete` is not part of the Singer protocol \
                 (supported: append, upsert, overwrite)"
                    .into(),
            ));
        }
        if self.write.delete_marker.is_some() {
            return Err(FaucetError::Config(
                "singer sink: `delete_marker` is not supported — Singer has no delete message"
                    .into(),
            ));
        }
        self.write
            .validate()
            .map_err(|e| FaucetError::Config(format!("singer sink: {e}")))
    }

    /// The Singer stream name (`stream`, or `faucet`).
    pub fn stream_name(&self) -> &str {
        self.stream.as_deref().unwrap_or("faucet")
    }

    /// The `key_properties` sent with `SCHEMA`.
    pub fn effective_key_properties(&self) -> Vec<String> {
        match &self.key_properties {
            Some(k) => k.clone(),
            None if matches!(self.write.write_mode, WriteMode::Upsert) => self.write.key.clone(),
            None => Vec::new(),
        }
    }

    /// What a failed batch write leaves behind (#737): records stream to the
    /// target one by one, so a failure mid-page may leave some persisted.
    pub fn batch_atomicity(&self) -> faucet_core::BatchAtomicity {
        faucet_core::BatchAtomicity::BestEffort
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(v: Value) -> SingerSinkConfig {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn defaults_apply() {
        let c = parse(json!({"target_command": "target-jsonl"}));
        assert_eq!(c.flush_on, FlushOn::Exit);
        assert_eq!(c.flush_timeout_secs, 600);
        assert_eq!(c.target_config, json!({}));
        assert!(c.env.is_empty() && c.args.is_empty());
        assert_eq!(c.stream_name(), "faucet");
        assert!(c.effective_key_properties().is_empty());
        assert_eq!(c.write.write_mode, WriteMode::Append);
        assert!(c.activate_version.is_none());
        c.validate().unwrap();
        assert_eq!(c.batch_atomicity(), faucet_core::BatchAtomicity::BestEffort);
    }

    #[test]
    fn new_matches_serde_defaults() {
        let a = SingerSinkConfig::new("t");
        let b = parse(json!({"target_command": "t"}));
        assert_eq!(
            serde_json::to_value(a).unwrap(),
            serde_json::to_value(b).unwrap()
        );
    }

    #[test]
    fn upsert_keys_become_key_properties() {
        let c = parse(json!({"target_command": "t", "write_mode": "upsert", "key": ["id"]}));
        c.validate().unwrap();
        assert_eq!(c.effective_key_properties(), vec!["id".to_string()]);
        let c = parse(json!({"target_command": "t", "key_properties": ["a", "b"]}));
        assert_eq!(c.effective_key_properties(), vec!["a", "b"]);
    }

    #[test]
    fn hidden_activate_version_parses() {
        let c = parse(
            json!({"target_command": "t", "write_mode": "overwrite", "_activate_version": 42}),
        );
        assert_eq!(c.activate_version, Some(42));
        c.validate().unwrap();
    }

    #[test]
    fn flush_on_parses_snake_case() {
        let c = parse(json!({"target_command": "t", "flush_on": "state"}));
        assert_eq!(c.flush_on, FlushOn::State);
    }

    #[test]
    fn validate_rejects_bad_configs() {
        let cases = [
            (json!({"target_command": " "}), "target_command"),
            (json!({"target_command": "t", "stream": ""}), "stream"),
            (
                json!({"target_command": "t", "flush_timeout_secs": 0}),
                "flush_timeout_secs",
            ),
            (json!({"target_command": "t", "schema": [1]}), "schema"),
            (
                json!({"target_command": "t", "write_mode": "delete", "key": ["id"]}),
                "delete",
            ),
            (
                json!({"target_command": "t", "write_mode": "upsert"}),
                "key",
            ),
            (
                json!({"target_command": "t", "write_mode": "upsert", "key": ["id"],
                       "delete_marker": {"field": "op", "values": ["d"]}}),
                "delete_marker",
            ),
        ];
        for (cfg, needle) in cases {
            let err = parse(cfg.clone()).validate().unwrap_err().to_string();
            assert!(err.contains(needle), "{cfg}: {err}");
        }
    }
}
