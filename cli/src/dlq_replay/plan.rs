//! Pure planning for `faucet dlq` — validate a reason filter, derive the
//! fresh "replay-failed" DLQ location, build the replay [`ExpandedNode`], and
//! decide per-line discard actions. Kept free of IO so it is exhaustively
//! unit-testable; the IO shims live in [`mod`](super) and [`reader`](super::reader).

use crate::config::{ConnectorSpec, DlqSpec, OnBatchErrorSpec, PipelineConfig};
use crate::dlq_replay::reader::{DlqDecryptor, DlqReaderSource, SourceOverride};
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole, expand};
use faucet_core::{DeliveryMode, DlqReason, UnwrappedEnvelope};
use serde_json::json;
use std::path::{Path, PathBuf};

/// Validate a user-supplied `--reason` filter against the closed set of DLQ
/// reason values (`partial` / `dlq_all` / `quality` / `schema_drift` /
/// `contract`). Returns the owned string on success.
pub fn validate_reason(reason: Option<&str>) -> CliResult<Option<String>> {
    match reason {
        None => Ok(None),
        Some(r) => {
            if DlqReason::from_serde_str(r).is_some() {
                Ok(Some(r.to_owned()))
            } else {
                let allowed = DlqReason::ALL
                    .iter()
                    .map(|r| r.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                Err(CliError::Config(format!(
                    "unknown --reason '{r}'; expected one of: {allowed}"
                )))
            }
        }
    }
}

/// Derive the default "replay-failed" DLQ path for a set of source files:
/// a `replay-failed.jsonl` sibling next to the first file (or, when the
/// source is a single file `x.jsonl`, `x.replay-failed.jsonl`). Guaranteed
/// distinct from any single-file source so replay can never re-feed itself.
pub fn default_failed_dlq_path(from_files: &[PathBuf]) -> PathBuf {
    match from_files {
        [only] => {
            let stem = only.file_stem().and_then(|s| s.to_str()).unwrap_or("dlq");
            let parent = only.parent().unwrap_or_else(|| Path::new("."));
            parent.join(format!("{stem}.replay-failed.jsonl"))
        }
        _ => {
            let parent = from_files
                .first()
                .and_then(|p| p.parent())
                .unwrap_or_else(|| Path::new("."));
            parent.join("replay-failed.jsonl")
        }
    }
}

/// Build a fresh JSONL DLQ spec pointing at `path`, inheriting the batching
/// policy of the original node's DLQ (if any) so replay behaves like the
/// original run — only the destination changes.
pub fn failed_dlq_spec(path: &Path, original: Option<&DlqSpec>) -> DlqSpec {
    let sink = ConnectorSpec {
        kind: "jsonl".to_string(),
        config: json!({ "path": path.to_string_lossy() }),
        transforms: None,
        inherit_transforms: true,
        status: None,
        tags: Vec::new(),
        complete_for: None,
        attributes: Default::default(),
    };
    match original {
        Some(o) => DlqSpec {
            sink,
            on_batch_error: o.on_batch_error,
            max_failures_per_page: o.max_failures_per_page,
            max_failures_total: o.max_failures_total,
            include_original_payload: o.include_original_payload,
        },
        None => DlqSpec {
            sink,
            on_batch_error: OnBatchErrorSpec::default(),
            max_failures_per_page: None,
            max_failures_total: None,
            include_original_payload: true,
        },
    }
}

/// Select the node to replay from an expanded config: the one whose id equals
/// `row` if given, otherwise the first root. Child nodes cannot be replayed on
/// their own (they need parent records to resolve `${parent.path}`), so only
/// roots are eligible.
pub fn select_replay_node(nodes: Vec<ExpandedNode>, row: Option<&str>) -> CliResult<ExpandedNode> {
    let roots = || nodes.iter().filter(|n| matches!(n.role, NodeRole::Root));
    let chosen = match row {
        Some(id) => nodes.iter().position(|n| n.id == id).ok_or_else(|| {
            CliError::Config(format!(
                "row '{id}' not found in config (roots: {})",
                roots()
                    .map(|n| n.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))
        })?,
        None => nodes
            .iter()
            .position(|n| matches!(n.role, NodeRole::Root))
            .ok_or_else(|| CliError::Config("config has no root pipeline to replay".into()))?,
    };
    let node = &nodes[chosen];
    if !matches!(node.role, NodeRole::Root) {
        return Err(CliError::Config(format!(
            "row '{}' is a child node; only root pipelines can be replayed directly",
            node.id
        )));
    }
    Ok(nodes.into_iter().nth(chosen).expect("index in range"))
}

/// Build the replay node from the referenced config: expand it, pick the
/// target root, swap its source for a [`DlqReaderSource`] over `from_files`,
/// force at-least-once delivery, drop the state block (a replay is a fresh
/// whole-location read), and point failures at a *fresh* DLQ so a replayed
/// row that fails again can never re-feed the source location.
pub fn build_replay_node(
    cfg: &PipelineConfig,
    from_files: Vec<PathBuf>,
    reason: Option<String>,
    failed_dlq: &Path,
    row: Option<&str>,
    decryptor: DlqDecryptor,
) -> CliResult<ExpandedNode> {
    // Loop guard: the fresh DLQ must not be one of the source files.
    if from_files.iter().any(|f| same_file(f, failed_dlq)) {
        return Err(CliError::Config(format!(
            "replay-failed DLQ '{}' is one of the source files — replayed failures would re-feed \
             the source; pass a different --failed-dlq",
            failed_dlq.display()
        )));
    }

    let nodes = expand(cfg)?;
    let original_dlq = nodes
        .iter()
        .find(|n| matches!(n.role, NodeRole::Root))
        .and_then(|n| n.dlq.clone());
    let mut node = select_replay_node(nodes, row)?;

    let reader = DlqReaderSource::new(from_files, reason, decryptor);
    node.source_override = Some(SourceOverride::new(Box::new(reader)));

    // A DLQ envelope's `payload` is the record as it entered the *write* path —
    // i.e. already transformed and already masked (the masking pass runs first per
    // page, so quarantine and write-failure envelopes both hold masked values).
    // Re-running those two passes on replay would apply them a second time, and
    // neither is idempotent: the `hash` transform and masking's `hash`/`tokenize`
    // actions would produce H(H(x)), breaking the joinability masking exists to
    // guarantee; `set` would re-stamp `${now.*}` with the replay time; `cast` /
    // `json_parse` / `split` would re-run against already-converted values. So a
    // replay skips both (#456 H3).
    //
    // The quality and contract passes are deliberately kept: they are pure
    // predicates over the record, so re-checking is idempotent — and a replay is
    // exactly when you want them re-enforced.
    node.transforms.clear();
    #[cfg(feature = "masking")]
    {
        node.masking = None;
    }

    // A replay reads the whole DLQ location once — no bookmarking, and never
    // exactly-once (the reader is not a deterministic-replay source).
    node.state = None;
    node.delivery = DeliveryMode::AtLeastOnce;
    node.dlq = Some(failed_dlq_spec(failed_dlq, original_dlq.as_ref()));
    Ok(node)
}

/// The raw `encryption` value of a config's dlq **jsonl** sink, if any —
/// the key that sealed the DLQ file a replay reads back.
pub fn dlq_encryption_value(dlq: Option<&DlqSpec>) -> Option<&serde_json::Value> {
    let dlq = dlq?;
    if dlq.sink.kind != "jsonl" {
        return None;
    }
    dlq.sink.config.get("encryption")
}

/// Two paths refer to the same file. Uses canonicalization when both exist,
/// falling back to a lexical compare (the fresh DLQ usually does not exist yet).
fn same_file(a: &Path, b: &Path) -> bool {
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(ca), Ok(cb)) => ca == cb,
        _ => a == b,
    }
}

/// Whether a discard filter selects a given envelope: its reason must match
/// (when a filter is set) and, when `before_ms` is set, its `ts_ms` must be
/// strictly older. An envelope missing `ts_ms` is never discarded by an
/// age filter (we cannot prove it is old).
pub fn envelope_selected(
    env: &UnwrappedEnvelope,
    reason: Option<&str>,
    before_ms: Option<i64>,
) -> bool {
    let reason_ok = match reason {
        None => true,
        Some(want) => env.reason.as_deref() == Some(want),
    };
    let age_ok = match before_ms {
        None => true,
        Some(cutoff) => env.ts_ms.is_some_and(|ts| ts < cutoff),
    };
    reason_ok && age_ok
}

/// Decide whether a raw discard-candidate line should be kept or discarded.
/// Only DLQ envelopes matching the filter are discarded; blank, malformed,
/// and non-envelope lines are always kept (we never mangle non-faucet content).
pub fn discard_keep_line(
    line: &str,
    dec: &DlqDecryptor,
    reason: Option<&str>,
    before_ms: Option<i64>,
) -> bool {
    use crate::dlq_replay::reader::{LineOutcome, classify_line_with};
    match classify_line_with(line, dec) {
        LineOutcome::Envelope(env) => !envelope_selected(&env, reason, before_ms),
        // Blank / malformed / non-envelope / undecryptable lines are always
        // preserved verbatim — we never mangle content we cannot prove is a
        // matching envelope.
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(reason: Option<&str>, ts_ms: Option<i64>) -> UnwrappedEnvelope {
        UnwrappedEnvelope {
            payload: json!({}),
            reason: reason.map(str::to_owned),
            error_kind: None,
            error_message: None,
            record_index: None,
            pipeline: None,
            row: None,
            sink: None,
            ts_ms,
        }
    }

    #[test]
    fn validate_reason_accepts_known_and_rejects_unknown() {
        assert_eq!(validate_reason(None).unwrap(), None);
        assert_eq!(
            validate_reason(Some("quality")).unwrap().as_deref(),
            Some("quality")
        );
        assert_eq!(
            validate_reason(Some("schema_drift")).unwrap().as_deref(),
            Some("schema_drift")
        );
        let err = validate_reason(Some("sink_error")).unwrap_err();
        assert!(format!("{err}").contains("unknown --reason"));
    }

    #[test]
    fn default_failed_dlq_path_single_file() {
        let p = default_failed_dlq_path(&[PathBuf::from("/data/dlq.jsonl")]);
        assert_eq!(p, PathBuf::from("/data/dlq.replay-failed.jsonl"));
    }

    #[test]
    fn default_failed_dlq_path_multi_file() {
        let p = default_failed_dlq_path(&[
            PathBuf::from("/data/a.jsonl"),
            PathBuf::from("/data/b.jsonl"),
        ]);
        assert_eq!(p, PathBuf::from("/data/replay-failed.jsonl"));
    }

    #[test]
    fn failed_dlq_spec_inherits_budgets() {
        let orig = DlqSpec {
            sink: ConnectorSpec {
                kind: "jsonl".into(),
                config: json!({"path": "orig.jsonl"}),
                transforms: None,
                inherit_transforms: true,
                status: None,
                tags: Vec::new(),
                complete_for: None,
                attributes: Default::default(),
            },
            on_batch_error: OnBatchErrorSpec::DlqAll,
            max_failures_per_page: Some(5),
            max_failures_total: Some(50),
            include_original_payload: true,
        };
        let spec = failed_dlq_spec(Path::new("failed.jsonl"), Some(&orig));
        assert_eq!(spec.sink.kind, "jsonl");
        assert_eq!(spec.sink.config["path"], "failed.jsonl");
        assert_eq!(spec.on_batch_error, OnBatchErrorSpec::DlqAll);
        assert_eq!(spec.max_failures_per_page, Some(5));
    }

    #[test]
    fn failed_dlq_spec_defaults_without_original() {
        let spec = failed_dlq_spec(Path::new("f.jsonl"), None);
        assert_eq!(spec.on_batch_error, OnBatchErrorSpec::default());
        assert_eq!(spec.max_failures_per_page, None);
        assert!(spec.include_original_payload);
    }

    #[test]
    fn envelope_selected_reason_and_age() {
        let e = env(Some("quality"), Some(1000));
        assert!(envelope_selected(&e, None, None));
        assert!(envelope_selected(&e, Some("quality"), None));
        assert!(!envelope_selected(&e, Some("contract"), None));
        assert!(envelope_selected(&e, Some("quality"), Some(2000))); // older than cutoff
        assert!(!envelope_selected(&e, Some("quality"), Some(500))); // newer than cutoff
        // No ts_ms → never selected by an age filter.
        let no_ts = env(Some("quality"), None);
        assert!(!envelope_selected(&no_ts, None, Some(2000)));
    }

    #[test]
    fn discard_keep_line_only_touches_matching_envelopes() {
        let matching = json!({
            "payload": {"id": 1}, "reason": "quality", "ts_ms": 100,
            "error": {"kind": "QualityFailure", "message": "x"}
        })
        .to_string();
        // Matching envelope with reason filter → discarded (not kept).
        assert!(!discard_keep_line(
            &matching,
            &DlqDecryptor::default(),
            Some("quality"),
            None
        ));
        // Non-matching reason → kept.
        assert!(discard_keep_line(
            &matching,
            &DlqDecryptor::default(),
            Some("contract"),
            None
        ));
        // Non-envelope / blank / malformed → always kept.
        assert!(discard_keep_line(
            r#"{"a":1}"#,
            &DlqDecryptor::default(),
            None,
            None
        ));
        assert!(discard_keep_line("", &DlqDecryptor::default(), None, None));
        assert!(discard_keep_line(
            "not json",
            &DlqDecryptor::default(),
            Some("quality"),
            None
        ));
    }
}

#[cfg(test)]
mod replay_shaping_tests {
    use super::*;
    use crate::config::PipelineConfig;

    /// Build the replay node for `cfg` with fixed paths.
    fn replay_node(cfg: &PipelineConfig) -> ExpandedNode {
        build_replay_node(
            cfg,
            vec![PathBuf::from("./dlq.jsonl")],
            None,
            Path::new("./failed.jsonl"),
            None,
            DlqDecryptor::default(),
        )
        .expect("builds")
    }

    fn cfg(extra: &str) -> PipelineConfig {
        let yaml = format!(
            r#"version: 1
name: replay-shape
pipeline:
{extra}  source: {{ type: csv, config: {{ path: ./in.csv }} }}
  sink: {{ type: jsonl, config: {{ path: ./out.jsonl }} }}
  dlq:
    sink: {{ type: jsonl, config: {{ path: ./dlq.jsonl }} }}
"#
        );
        PipelineConfig::from_text(&yaml, Path::new("test.yaml")).expect("parses")
    }

    /// #456 H3: a DLQ payload is already transformed, so a replay must not run the
    /// chain again — `hash`, `set ${now.*}`, `cast`, and `json_parse` are not
    /// idempotent, and re-applying them writes values the original path would
    /// never have produced.
    #[test]
    fn replay_drops_the_transform_chain() {
        let cfg = cfg("  transforms:\n    - { type: keys_case, config: { mode: snake } }\n");
        // Sanity: the config really does declare a chain, so the assertion below is
        // about the replay shaping and not about an empty config.
        let expanded = crate::expand::expand(&cfg).unwrap();
        assert_eq!(expanded[0].transforms.len(), 1);

        let node = replay_node(&cfg);
        assert!(
            node.transforms.is_empty(),
            "the chain already ran before the payload was captured"
        );
        assert!(node.source_override.is_some());
        assert_eq!(node.delivery, DeliveryMode::AtLeastOnce);
        assert!(node.state.is_none());
    }

    /// …and the payload is already masked, so the masking pass is dropped too:
    /// masking's `hash`/`tokenize` would produce H(H(x)), breaking the
    /// joinability masking exists to guarantee.
    #[cfg(feature = "masking")]
    #[test]
    fn replay_drops_masking() {
        let cfg = cfg(
            "  masking:\n    rules:\n      - name: h\n        match: { fields: [email] }\n        action: { type: hash }\n",
        );
        assert!(crate::expand::expand(&cfg).unwrap()[0].masking.is_some());
        assert!(
            replay_node(&cfg).masking.is_none(),
            "the payload in the envelope is already masked"
        );
    }

    /// Quality is a pure check, so a replay keeps enforcing it.
    #[cfg(feature = "quality")]
    #[test]
    fn replay_keeps_quality() {
        let cfg = cfg(
            "  quality:\n    record:\n      - { type: not_null, field: id, on_failure: abort }\n",
        );
        let node = build_replay_node(
            &cfg,
            vec![PathBuf::from("./dlq.jsonl")],
            None,
            Path::new("./failed.jsonl"),
            None,
            DlqDecryptor::default(),
        )
        .expect("builds");
        assert!(node.quality.is_some(), "checks are idempotent; keep them");
    }
}
