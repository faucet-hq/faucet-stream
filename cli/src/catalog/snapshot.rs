//! Config-change preview (#374): build a redacted snapshot of the resolved +
//! expanded config, and diff the current config against the last recorded one.
//!
//! `faucet run` / `replicate` / `schedule` record a [`ConfigSnapshot`] on every
//! successful invocation (best-effort — see [`super::record_config_snapshot`]).
//! `faucet plan --diff` re-expands the current config, loads the last snapshot,
//! and renders a **semantic, per-row** diff (rows created / changed / removed,
//! and within each changed row the exact fields that differ).
//!
//! Two properties make this trustworthy where a raw YAML text-diff is not:
//!
//! - **Resolved, not textual.** The snapshot is built from the *expanded* nodes
//!   (post `extends`/`vars`/`${env:}` interpolation and matrix fan-out), so a
//!   one-line `${vars.x}` edit that fans out across many rows shows up as the
//!   real per-row effect, and two textually-different files that resolve to the
//!   same movement show no diff.
//! - **Secret-safe.** Every secret-sourced value is replaced with a stable
//!   `<secret:sha256:…>` token before storage (see [`redact_value`]). No secret
//!   material is ever persisted, and a rotated secret surfaces as a changed hash
//!   ("secret rotated") rather than printing either value.

use crate::config::{ExecutionSpec, OnError, PipelineConfig};
use crate::expand::ExpandedNode;
use crate::serve::history::catalog::{
    ConfigSnapshot, ConnectorSnapshot, RowSnapshot, TransformSnapshot,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// The canonical pipeline name used to key snapshots — identical logic to the
/// `run` / `replicate` / `schedule` observability label, so the record side and
/// the `plan --diff` side always agree on the key: the explicit `name:`, else
/// the config file stem, else `"pipeline"`.
pub fn resolve_name(cfg: &PipelineConfig, config_path: Option<&Path>) -> String {
    cfg.name.clone().unwrap_or_else(|| {
        config_path
            .and_then(|p| p.file_stem())
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    })
}

/// The `execution.on_error` policy as a stable string (`"stop"` / `"continue"`).
pub fn on_error_str(execution: &Option<ExecutionSpec>) -> &'static str {
    match execution.as_ref().map(|e| e.on_error).unwrap_or_default() {
        OnError::Stop => "stop",
        OnError::Continue => "continue",
    }
}

/// Build a redacted snapshot from the resolved + expanded nodes. `pipeline` and
/// `on_error` are passed in (via [`resolve_name`] / [`on_error_str`]) so every
/// call site keys snapshots identically; `clock` is the record time (passed in
/// so callers stay deterministic in tests).
pub fn build_snapshot(
    pipeline: String,
    on_error: &str,
    nodes: &[ExpandedNode],
    clock: DateTime<Utc>,
) -> ConfigSnapshot {
    let mut rows = BTreeMap::new();
    for node in nodes {
        let state_key = node
            .state
            .as_ref()
            .map(|_| format!("{pipeline}::{}", node.id));
        rows.insert(
            node.id.clone(),
            RowSnapshot {
                source: connector_snapshot(&node.source.kind, &node.source.config),
                sink: connector_snapshot(&node.sink.kind, &node.sink.config),
                transforms: node
                    .transforms
                    .iter()
                    .map(|t| TransformSnapshot {
                        kind: t.kind.clone(),
                        config: redact_value(&t.config),
                    })
                    .collect(),
                state_key,
                delivery_guarantee: format!("{:?}", node.delivery_guarantee),
                on_error: on_error.to_owned(),
                dlq: node.dlq.is_some(),
                contract: row_contract(node),
            },
        );
    }
    ConfigSnapshot {
        pipeline,
        recorded_at: clock,
        faucet_version: env!("CARGO_PKG_VERSION").to_owned(),
        rows,
    }
}

/// The row's contract as a JSON value, when the build has contracts and the
/// row declares one.
#[cfg(feature = "contract")]
fn row_contract(node: &ExpandedNode) -> Option<Value> {
    node.contract
        .as_ref()
        .and_then(|c| serde_json::to_value(c).ok())
}
#[cfg(not(feature = "contract"))]
fn row_contract(_node: &ExpandedNode) -> Option<Value> {
    None
}

fn connector_snapshot(kind: &str, config: &Value) -> ConnectorSnapshot {
    ConnectorSnapshot {
        kind: kind.to_owned(),
        config: redact_value(config),
    }
}

/// Build + record the config snapshot for a run that finished cleanly, when a
/// catalog is configured. Best-effort — never fails the run (mirrors
/// [`super::record`]). This is the single place `run` / `replicate` /
/// `schedule` funnel through, so the record logic is exercised by one test
/// rather than three untested call sites.
pub async fn record_if_ok(
    catalog: Option<&super::CatalogHandle>,
    pipeline: &str,
    on_error: &str,
    nodes: &[ExpandedNode],
    succeeded: bool,
    clock: DateTime<Utc>,
) {
    if !succeeded {
        return;
    }
    let Some(handle) = catalog else {
        return;
    };
    let snapshot = build_snapshot(pipeline.to_owned(), on_error, nodes, clock);
    super::record_config_snapshot(handle, &snapshot).await;
}

/// Recursively replace every secret-sourced string in `value` with a stable
/// `<secret:sha256:…>` token. Non-secret strings pass through verbatim, so real
/// config changes (paths, table names, page sizes) stay visible in the diff.
pub fn redact_value(value: &Value) -> Value {
    match value {
        Value::String(s) => {
            Value::String(crate::secrets::registry::redact_with(s, secret_token).into_owned())
        }
        Value::Array(items) => Value::Array(items.iter().map(redact_value).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), redact_value(v)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// The stable, non-reversible token a secret value is replaced with. The 12-hex
/// prefix of sha256 is enough to detect rotation without bloating the snapshot.
fn secret_token(secret: &str) -> String {
    let digest = Sha256::digest(secret.as_bytes());
    let hex: String = digest.iter().take(6).map(|b| format!("{b:02x}")).collect();
    format!("<secret:sha256:{hex}>")
}

// ── Diff ─────────────────────────────────────────────────────────────────────

/// Per-row status in a config diff, `terraform plan`-style.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RowStatus {
    /// Present now, absent in the last snapshot — will be created.
    New,
    /// Present in both, with field-level differences.
    Changed,
    /// Present in the last snapshot, absent now — no longer part of the run set.
    Removed,
    /// Present in both, identical.
    Unchanged,
}

impl RowStatus {
    fn glyph(self) -> char {
        match self {
            Self::New => '+',
            Self::Changed => '~',
            Self::Removed => '-',
            Self::Unchanged => '=',
        }
    }
    fn label(self) -> &'static str {
        match self {
            Self::New => "NEW ROW — will be created",
            Self::Changed => "CHANGED",
            Self::Removed => "REMOVED — no longer in the run set",
            Self::Unchanged => "unchanged",
        }
    }
}

/// One field-level change within a changed row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldChange {
    /// Dotted path within the row snapshot (e.g. `source.config.page_size`).
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// True when both sides are secret tokens that differ (a rotation), so the
    /// renderer can say "secret rotated" instead of printing hashes.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub secret_rotated: bool,
}

/// One row's entry in the diff.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RowDiff {
    pub id: String,
    pub status: RowStatus,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub changes: Vec<FieldChange>,
}

/// Roll-up counts, `terraform plan`-style.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
pub struct DiffSummary {
    pub create: usize,
    pub change: usize,
    pub remove: usize,
    pub unchanged: usize,
}

/// The full config diff, serialized verbatim by `--json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SnapshotDiff {
    pub pipeline: String,
    /// When the compared snapshot was recorded (`None` on a first-ever diff).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_recorded_at: Option<DateTime<Utc>>,
    /// True when no prior snapshot existed — every row is `New`.
    pub first_run: bool,
    pub rows: Vec<RowDiff>,
    pub summary: DiffSummary,
}

/// Diff `current` against the last recorded snapshot (`previous`). With no
/// previous snapshot every row is `New` (first-run, like `terraform plan` on
/// fresh state).
pub fn diff(previous: Option<&ConfigSnapshot>, current: &ConfigSnapshot) -> SnapshotDiff {
    let first_run = previous.is_none();
    let empty = BTreeMap::new();
    let prev_rows = previous.map(|p| &p.rows).unwrap_or(&empty);

    let ids: BTreeSet<&String> = prev_rows.keys().chain(current.rows.keys()).collect();
    let mut rows = Vec::new();
    let mut summary = DiffSummary::default();

    for id in ids {
        let diff = match (prev_rows.get(id), current.rows.get(id)) {
            (None, Some(_)) => {
                summary.create += 1;
                RowDiff {
                    id: id.clone(),
                    status: RowStatus::New,
                    changes: Vec::new(),
                }
            }
            (Some(_), None) => {
                summary.remove += 1;
                RowDiff {
                    id: id.clone(),
                    status: RowStatus::Removed,
                    changes: Vec::new(),
                }
            }
            (Some(prev), Some(curr)) => {
                let changes = field_changes(prev, curr);
                if changes.is_empty() {
                    summary.unchanged += 1;
                    RowDiff {
                        id: id.clone(),
                        status: RowStatus::Unchanged,
                        changes,
                    }
                } else {
                    summary.change += 1;
                    RowDiff {
                        id: id.clone(),
                        status: RowStatus::Changed,
                        changes,
                    }
                }
            }
            (None, None) => unreachable!("id came from the union of both maps"),
        };
        rows.push(diff);
    }

    SnapshotDiff {
        pipeline: current.pipeline.clone(),
        previous_recorded_at: previous.map(|p| p.recorded_at),
        first_run,
        rows,
        summary,
    }
}

/// Flatten a row to `dotted.path -> scalar` and compare, producing one
/// [`FieldChange`] per differing leaf.
fn field_changes(prev: &RowSnapshot, curr: &RowSnapshot) -> Vec<FieldChange> {
    let a = flatten_row(prev);
    let b = flatten_row(curr);
    let paths: BTreeSet<&String> = a.keys().chain(b.keys()).collect();
    let mut out = Vec::new();
    for path in paths {
        let before = a.get(path);
        let after = b.get(path);
        if before != after {
            let secret_rotated = matches!((before, after), (Some(x), Some(y))
                if is_secret_token(x) && is_secret_token(y));
            out.push(FieldChange {
                path: path.clone(),
                before: before.cloned(),
                after: after.cloned(),
                secret_rotated,
            });
        }
    }
    out
}

fn is_secret_token(s: &str) -> bool {
    s.starts_with("<secret:sha256:")
}

/// Serialize a row and flatten every leaf to a `dotted.path -> String` map.
fn flatten_row(row: &RowSnapshot) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let value = serde_json::to_value(row).unwrap_or(Value::Null);
    flatten_value("", &value, &mut out);
    out
}

fn flatten_value(prefix: &str, value: &Value, out: &mut BTreeMap<String, String>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let path = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten_value(&path, v, out);
            }
        }
        Value::Array(items) => {
            // Empty arrays are a meaningful leaf (e.g. `transforms: []`).
            if items.is_empty() {
                out.insert(prefix.to_owned(), "[]".to_owned());
            } else {
                for (i, v) in items.iter().enumerate() {
                    flatten_value(&format!("{prefix}[{i}]"), v, out);
                }
            }
        }
        Value::String(s) => {
            out.insert(prefix.to_owned(), s.clone());
        }
        other => {
            out.insert(prefix.to_owned(), other.to_string());
        }
    }
}

// ── Render ───────────────────────────────────────────────────────────────────

/// Render the diff as human-readable text (the default `plan --diff` output).
pub fn render_human(d: &SnapshotDiff) -> String {
    let mut s = String::new();
    let when = match d.previous_recorded_at {
        Some(ts) => format!("last run {}", ts.format("%Y-%m-%d %H:%M UTC")),
        None => "nothing recorded yet — first diff".to_owned(),
    };
    s.push_str(&format!("Pipeline: {}   ({when})\n\n", d.pipeline));

    if d.first_run {
        s.push_str(
            "  No prior snapshot. The next `faucet run` will record one; every row below is new.\n\n",
        );
    }

    for row in &d.rows {
        s.push_str(&format!(
            "  {} {:<18} {}\n",
            row.status.glyph(),
            row.id,
            row.status.label()
        ));
        for c in &row.changes {
            if c.secret_rotated {
                s.push_str(&format!("      {:<28} (secret rotated)\n", c.path));
            } else {
                let before = c.before.as_deref().unwrap_or("(absent)");
                let after = c.after.as_deref().unwrap_or("(absent)");
                s.push_str(&format!("      {:<28} {before}  ->  {after}\n", c.path));
            }
        }
    }

    let sm = &d.summary;
    s.push_str(&format!(
        "\nSummary: {} to create, {} to change, {} removed, {} unchanged.\n",
        sm.create, sm.change, sm.remove, sm.unchanged
    ));
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::history::catalog::ConnectorSnapshot;
    use serde_json::json;

    fn conn(kind: &str, cfg: Value) -> ConnectorSnapshot {
        ConnectorSnapshot {
            kind: kind.into(),
            config: cfg,
        }
    }

    fn row(source_cfg: Value) -> RowSnapshot {
        RowSnapshot {
            source: conn("rest", source_cfg),
            sink: conn("jsonl", json!({"path": "out.jsonl"})),
            transforms: vec![],
            state_key: None,
            delivery_guarantee: "AtLeastOnce".into(),
            on_error: "stop".into(),
            dlq: false,
            contract: None,
        }
    }

    fn snap(rows: Vec<(&str, RowSnapshot)>) -> ConfigSnapshot {
        ConfigSnapshot {
            pipeline: "p".into(),
            recorded_at: DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z")
                .unwrap()
                .to_utc(),
            faucet_version: "0.0.0".into(),
            rows: rows.into_iter().map(|(k, v)| (k.to_owned(), v)).collect(),
        }
    }

    #[test]
    fn first_run_marks_every_row_new() {
        let curr = snap(vec![("a", row(json!({"path": "/v1"})))]);
        let d = diff(None, &curr);
        assert!(d.first_run);
        assert_eq!(d.summary.create, 1);
        assert_eq!(d.rows[0].status, RowStatus::New);
    }

    #[test]
    fn detects_added_removed_changed_and_unchanged() {
        let prev = snap(vec![
            ("payroll", row(json!({"path": "/v1/pay", "page_size": 100}))),
            ("benefits", row(json!({"path": "/v1/benefits"}))),
            ("employees", row(json!({"path": "/v1/emp"}))),
        ]);
        let curr = snap(vec![
            ("people", row(json!({"path": "/v1/people"}))), // NEW
            (
                "payroll",
                row(json!({"path": "/v1/payroll", "page_size": 500})),
            ), // CHANGED
            ("employees", row(json!({"path": "/v1/emp"}))), // unchanged
                                                            // benefits REMOVED
        ]);
        let d = diff(Some(&prev), &curr);
        assert_eq!(d.summary.create, 1);
        assert_eq!(d.summary.change, 1);
        assert_eq!(d.summary.remove, 1);
        assert_eq!(d.summary.unchanged, 1);

        let payroll = d.rows.iter().find(|r| r.id == "payroll").unwrap();
        assert_eq!(payroll.status, RowStatus::Changed);
        let paths: Vec<&str> = payroll.changes.iter().map(|c| c.path.as_str()).collect();
        assert!(paths.contains(&"source.config.path"));
        assert!(paths.contains(&"source.config.page_size"));
        let ps = payroll
            .changes
            .iter()
            .find(|c| c.path == "source.config.page_size")
            .unwrap();
        assert_eq!(ps.before.as_deref(), Some("100"));
        assert_eq!(ps.after.as_deref(), Some("500"));
    }

    #[test]
    fn secret_rotation_is_surfaced_not_printed() {
        let prev = snap(vec![(
            "r",
            row(json!({"token": "<secret:sha256:aaaaaaaaaaaa>"})),
        )]);
        let curr = snap(vec![(
            "r",
            row(json!({"token": "<secret:sha256:bbbbbbbbbbbbb>"})),
        )]);
        let d = diff(Some(&prev), &curr);
        let r = &d.rows[0];
        assert_eq!(r.status, RowStatus::Changed);
        assert!(r.changes[0].secret_rotated);
        let text = render_human(&d);
        assert!(text.contains("secret rotated"), "{text}");
        assert!(!text.contains("bbbbbbbbbbbb"), "hash should not be printed");
    }

    #[test]
    fn redact_value_replaces_registered_secret_with_stable_token() {
        crate::secrets::registry::register("supersecrettoken");
        let redacted = redact_value(&json!({"auth": "supersecrettoken", "path": "/v1"}));
        let token = redacted["auth"].as_str().unwrap();
        assert!(token.starts_with("<secret:sha256:"), "{token}");
        assert_eq!(redacted["path"], json!("/v1"));
        // Stable: same value → same token.
        let again = redact_value(&json!("supersecrettoken"));
        assert_eq!(again.as_str().unwrap(), token);
    }

    /// Write a config to a temp file and return (config, expanded nodes, path).
    fn expand_config(
        yaml: &str,
    ) -> (
        crate::config::PipelineConfig,
        Vec<ExpandedNode>,
        std::path::PathBuf,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("p.yaml");
        std::fs::write(&path, yaml).unwrap();
        let cfg = crate::config::PipelineConfig::from_path_tolerating_secrets(&path, None).unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        std::mem::forget(dir); // keep the temp file alive for the returned path
        (cfg, nodes, path)
    }

    const REST_TO_JSONL: &str = r#"
version: 1
name: mypipe
pipeline:
  source:
    type: rest
    config:
      url: https://api.example.com/v1
      auth: { type: bearer, config: { token: topsecretvalue12345 } }
  sink:
    type: jsonl
    config:
      path: out.jsonl
  transforms:
    - type: flatten
      config: {}
"#;

    #[test]
    fn build_snapshot_shapes_rows_and_redacts_secrets() {
        crate::secrets::registry::register("topsecretvalue12345");
        let (cfg, nodes, path) = expand_config(REST_TO_JSONL);
        assert_eq!(resolve_name(&cfg, Some(&path)), "mypipe");
        assert_eq!(on_error_str(&cfg.execution), "continue"); // default

        let snap = build_snapshot(
            resolve_name(&cfg, Some(&path)),
            on_error_str(&cfg.execution),
            &nodes,
            Utc::now(),
        );
        assert_eq!(snap.pipeline, "mypipe");
        let r = snap.rows.values().next().unwrap();
        assert_eq!(r.source.kind, "rest");
        assert_eq!(r.sink.kind, "jsonl");
        assert_eq!(r.transforms.len(), 1);
        let src = serde_json::to_string(&r.source.config).unwrap();
        assert!(!src.contains("topsecretvalue12345"), "secret leaked: {src}");
        assert!(src.contains("api.example.com"), "non-secret url must show");
    }

    #[test]
    fn resolve_name_falls_back_to_file_stem_then_default() {
        // No `name:` → file stem.
        let (cfg, _n, path) = expand_config(
            "version: 1\npipeline:\n  source: { type: rest, config: { url: https://x/y } }\n  sink: { type: jsonl, config: { path: o.jsonl } }\n",
        );
        assert_eq!(resolve_name(&cfg, Some(&path)), "p"); // p.yaml
        assert_eq!(resolve_name(&cfg, None), "pipeline"); // nothing → default
    }

    #[tokio::test]
    async fn record_if_ok_records_only_on_success_with_a_catalog() {
        let (_cfg, nodes, _p) = expand_config(REST_TO_JSONL);
        let handle = crate::catalog::connect_from_spec(&crate::catalog::CatalogSpec {
            url: "memory".into(),
            sample_records: 10,
            datasets: Vec::new(),
        })
        .await
        .unwrap();

        // Failure → nothing recorded.
        record_if_ok(Some(&handle), "mypipe", "stop", &nodes, false, Utc::now()).await;
        assert!(
            handle
                .store
                .catalog_last_config_snapshot("mypipe")
                .await
                .unwrap()
                .is_none()
        );
        // No catalog → no-op (must not panic).
        record_if_ok(None, "mypipe", "stop", &nodes, true, Utc::now()).await;
        // Success + catalog → recorded.
        record_if_ok(Some(&handle), "mypipe", "stop", &nodes, true, Utc::now()).await;
        let got = handle
            .store
            .catalog_last_config_snapshot("mypipe")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.pipeline, "mypipe");
        assert!(!got.rows.is_empty());
    }
}
