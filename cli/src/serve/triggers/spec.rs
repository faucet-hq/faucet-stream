//! Serde config types for the `--triggers` file. Pure data + `JsonSchema`; no IO.
//! Validation lives in `compiled.rs`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Top-level `--triggers` document.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TriggersFile {
    /// Schema version; must be `1`.
    pub version: u32,
    /// The triggers to run. Each is supervised independently: one watcher
    /// erroring backs off and marks itself unhealthy without stopping the rest.
    pub triggers: Vec<TriggerSpec>,
}

/// One configured trigger.
// Note: `deny_unknown_fields` is intentionally absent here — serde does not
// support it on structs that contain a `#[serde(flatten)]` field. Unknown fields
// are instead rejected at load time by [`unknown_trigger_fields`] (see #232),
// which diffs the raw document against this type's re-serialization.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct TriggerSpec {
    /// Unique name; used in metrics, idempotency keys, and the webhook path.
    pub name: String,
    /// Spawn this trigger? Default `true`.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// The pipeline to enqueue when the trigger fires (path string or inline doc).
    pub config: PipelineRef,
    /// Optional run-shaping.
    #[serde(default)]
    pub run: RunTemplate,
    /// Type-specific settings.
    #[serde(flatten)]
    pub kind: TriggerKind,
}

/// A pipeline reference: a path to a config file, OR an inline config document.
/// Untagged so YAML `config: ./x.yaml` and `config: { pipeline: … }` both parse.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum PipelineRef {
    Path(String),
    Inline(serde_json::Value),
}

/// Trigger type + its settings. Internally-tagged on `type` — variant fields
/// sit flat alongside `type` in the serialized form.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TriggerKind {
    ObjectArrival {
        store: StoreSpec,
        #[serde(default = "default_poll_secs")]
        poll_interval_secs: u64,
        #[serde(default)]
        mode: ArrivalMode,
        #[serde(default)]
        start_at: StartAt,
    },
    Webhook {
        #[serde(default = "default_webhook_methods")]
        methods: Vec<String>,
        /// Header whose value is used as the idempotency key (else per-request UUID).
        #[serde(default)]
        dedupe_header: Option<String>,
        /// Leading-edge debounce window in seconds: coalesce fires that arrive
        /// within this many seconds of the last accepted fire. Default 0 (off).
        #[serde(default)]
        debounce_secs: u64,
    },
    QueueDepth {
        queue: QueueSpec,
        #[serde(default = "default_threshold")]
        threshold: u64,
        #[serde(default = "default_poll_secs")]
        poll_interval_secs: u64,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ArrivalMode {
    #[default]
    PerObject,
    Batch,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StartAt {
    #[default]
    Now,
    Beginning,
}

/// Object-store connection for `object_arrival`. Internally-tagged on `type`;
/// variant fields sit flat alongside `type`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StoreSpec {
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default)]
        region: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
    },
    Gcs {
        bucket: String,
        #[serde(default)]
        prefix: Option<String>,
    },
}

/// Queue connection for `queue_depth`. Internally-tagged on `type`;
/// variant fields sit flat alongside `type`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum QueueSpec {
    Redis {
        url: String,
        key: String,
        #[serde(default)]
        kind: RedisQueueKind,
    },
    Kafka {
        brokers: String,
        topic: String,
        group: String,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RedisQueueKind {
    #[default]
    List,
    Stream,
}

/// Optional run-shaping applied to the enqueued run.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RunTemplate {
    /// Run name template; `{field}` tokens resolve from trigger fields
    /// (`name`, `type`, `object_key`, `bucket`, `queue`, `depth`).
    #[serde(default)]
    pub name: Option<String>,
    /// Static labels merged with the auto-derived trigger labels.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// Per-run timeout in seconds.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

/// Detect fields in the raw triggers document that the typed parse silently
/// dropped. Returns `(trigger_label, dotted_field_path)` pairs — empty when the
/// document has no unknown fields.
///
/// This works around serde's inability to combine `#[serde(flatten)]` (carried
/// by `TriggerSpec.kind`) with `#[serde(deny_unknown_fields)]`: a misspelled
/// top-level trigger field such as `debounce_sec` (for `debounce_secs`) would
/// otherwise deserialize to its default with no error. We round-trip each parsed
/// `TriggerSpec` back to JSON and report any raw key absent from that
/// serialization, so the allow-list is derived from the types themselves and can
/// never drift. Nested objects (`store`, `queue`, `run`) are checked too; the
/// opaque inline pipeline `config:` sub-document is deliberately not descended
/// into (its keys belong to a full pipeline config validated elsewhere).
pub fn unknown_trigger_fields(
    raw: &serde_json::Value,
    file: &TriggersFile,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let Some(raw_triggers) = raw.get("triggers").and_then(|v| v.as_array()) else {
        return out;
    };
    for (i, trigger) in file.triggers.iter().enumerate() {
        let Some(raw_trigger) = raw_triggers.get(i) else {
            continue;
        };
        let Ok(known) = serde_json::to_value(trigger) else {
            continue;
        };
        let label = if trigger.name.trim().is_empty() {
            format!("#{i}")
        } else {
            trigger.name.clone()
        };
        let mut fields = Vec::new();
        collect_unknown_keys(raw_trigger, &known, "", true, &mut fields);
        for f in fields {
            out.push((label.clone(), f));
        }
    }
    out
}

/// Recursively collect keys present in `raw` but absent from `known` (the typed
/// re-serialization). `at_trigger_root` suppresses descent into the opaque
/// `config:` sub-document at the trigger top level.
fn collect_unknown_keys(
    raw: &serde_json::Value,
    known: &serde_json::Value,
    path: &str,
    at_trigger_root: bool,
    out: &mut Vec<String>,
) {
    let (Some(raw_obj), Some(known_obj)) = (raw.as_object(), known.as_object()) else {
        return;
    };
    for (key, raw_val) in raw_obj {
        let child_path = if path.is_empty() {
            key.clone()
        } else {
            format!("{path}.{key}")
        };
        match known_obj.get(key) {
            None => out.push(child_path),
            Some(known_val) => {
                // The inline pipeline document is opaque here — its keys are a
                // full pipeline config, validated by the pipeline loader.
                if at_trigger_root && key == "config" {
                    continue;
                }
                collect_unknown_keys(raw_val, known_val, &child_path, false, out);
            }
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_poll_secs() -> u64 {
    30
}
fn default_threshold() -> u64 {
    1
}
fn default_webhook_methods() -> Vec<String> {
    vec!["POST".to_string()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_object_arrival_with_defaults() {
        let yaml = r#"
version: 1
triggers:
  - name: drop
    type: object_arrival
    config: ./pipelines/load.yaml
    store: { type: s3, bucket: b, prefix: incoming/ }
"#;
        let f: TriggersFile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(f.version, 1);
        assert_eq!(f.triggers.len(), 1);
        let t = &f.triggers[0];
        assert_eq!(t.name, "drop");
        assert!(t.enabled);
        assert!(matches!(t.config, PipelineRef::Path(ref p) if p == "./pipelines/load.yaml"));
        match &t.kind {
            TriggerKind::ObjectArrival {
                poll_interval_secs,
                mode,
                start_at,
                ..
            } => {
                assert_eq!(*poll_interval_secs, 30);
                assert!(matches!(mode, ArrivalMode::PerObject));
                assert!(matches!(start_at, StartAt::Now));
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn parses_inline_pipeline_and_webhook_and_queue() {
        let yaml = r#"
version: 1
triggers:
  - name: hook
    type: webhook
    config: { pipeline: { sources: {}, sinks: {} } }
    dedupe_header: Idempotency-Key
    debounce_secs: 30
  - name: drain
    type: queue_depth
    config: ./drain.yaml
    queue: { type: redis, url: "redis://x", key: jobs, kind: stream }
    threshold: 5
"#;
        let f: TriggersFile = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(f.triggers[0].config, PipelineRef::Inline(_)));
        match &f.triggers[0].kind {
            TriggerKind::Webhook {
                methods,
                dedupe_header,
                debounce_secs,
            } => {
                assert_eq!(methods, &vec!["POST".to_string()]);
                assert_eq!(dedupe_header.as_deref(), Some("Idempotency-Key"));
                assert_eq!(*debounce_secs, 30);
            }
            _ => panic!("wrong kind"),
        }
        match &f.triggers[1].kind {
            TriggerKind::QueueDepth {
                threshold, queue, ..
            } => {
                assert_eq!(*threshold, 5);
                assert!(matches!(
                    queue,
                    QueueSpec::Redis {
                        kind: RedisQueueKind::Stream,
                        ..
                    }
                ));
            }
            _ => panic!("wrong kind"),
        }
    }

    #[test]
    fn rejects_unknown_top_level_field() {
        let yaml = "version: 1\ntriggers: []\nbogus: 1\n";
        assert!(serde_yaml::from_str::<TriggersFile>(yaml).is_err());
    }

    /// Parse the same text both ways: the typed form and a raw JSON `Value`.
    fn parse_both(yaml: &str) -> (TriggersFile, serde_json::Value) {
        let file: TriggersFile = serde_yaml::from_str(yaml).expect("typed parse");
        let raw: serde_json::Value = serde_yaml::from_str(yaml).expect("raw parse");
        (file, raw)
    }

    #[test]
    fn detects_unknown_top_level_trigger_field() {
        // `debounce_sec` is a typo for `debounce_secs` — silently dropped by the
        // flatten-bearing TriggerSpec, so the typed parse alone would accept it.
        let yaml = "\
version: 1
triggers:
  - name: hook
    type: webhook
    config: ./x.yaml
    debounce_sec: 5
";
        let (file, raw) = parse_both(yaml);
        // The bug: the typed parse accepts the typo silently.
        assert_eq!(file.triggers.len(), 1);
        // The fix: the diff surfaces it.
        let unknown = unknown_trigger_fields(&raw, &file);
        assert_eq!(
            unknown,
            vec![("hook".to_string(), "debounce_sec".to_string())]
        );
    }

    #[test]
    fn accepts_all_known_fields_for_every_type() {
        let yaml = "\
version: 1
triggers:
  - name: obj
    type: object_arrival
    enabled: true
    config: ./load.yaml
    run: { name: r, timeout_secs: 10 }
    store: { type: s3, bucket: b, prefix: in/, region: us-east-1, endpoint: http://x }
    poll_interval_secs: 15
    mode: batch
    start_at: beginning
  - name: hook
    type: webhook
    config: { pipeline: { sources: {}, sinks: {} } }
    methods: [POST, PUT]
    dedupe_header: Idempotency-Key
    debounce_secs: 5
  - name: drain
    type: queue_depth
    config: ./drain.yaml
    queue: { type: kafka, brokers: b, topic: t, group: g }
    threshold: 3
    poll_interval_secs: 20
";
        let (file, raw) = parse_both(yaml);
        let unknown = unknown_trigger_fields(&raw, &file);
        assert!(
            unknown.is_empty(),
            "expected no unknown fields, got {unknown:?}"
        );
    }

    #[test]
    fn detects_unknown_nested_store_field() {
        // A typo in an optional nested field (`prefx` for `prefix`) is also
        // silently dropped by the internally-tagged StoreSpec enum.
        let yaml = "\
version: 1
triggers:
  - name: obj
    type: object_arrival
    config: ./load.yaml
    store: { type: s3, bucket: b, prefx: in/ }
";
        let (file, raw) = parse_both(yaml);
        let unknown = unknown_trigger_fields(&raw, &file);
        assert_eq!(
            unknown,
            vec![("obj".to_string(), "store.prefx".to_string())]
        );
    }

    #[test]
    fn ignores_keys_inside_inline_pipeline_config() {
        // The inline `config:` document is a full pipeline config validated
        // elsewhere; its arbitrary keys must not be flagged as unknown.
        let yaml = "\
version: 1
triggers:
  - name: hook
    type: webhook
    config:
      version: 1
      pipeline: { sources: {}, sinks: {} }
      anything_goes_here: true
";
        let (file, raw) = parse_both(yaml);
        let unknown = unknown_trigger_fields(&raw, &file);
        assert!(
            unknown.is_empty(),
            "inline config keys must be ignored, got {unknown:?}"
        );
    }
}
