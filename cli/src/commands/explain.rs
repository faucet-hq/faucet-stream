//! `faucet explain` — plain-English narration of what a pipeline does (#389).
//!
//! Where `faucet plan` is structured and machine-oriented, `explain` reads like
//! prose: "Reads `orders` from S3 → applies `flatten`, `rename_keys` → writes to
//! BigQuery with upsert on `id`. Expands to 4 matrix rows; delivery:
//! effectively-once." It is built entirely from the already-resolved
//! [`ExpandedNode`]s — **fully offline, zero I/O, no source is touched.**
//!
//! Secrets are never printed: connectors are described by their kind plus a
//! curated allowlist of structural, non-secret fields (table, path, topic, …).
//! `url` / `connection_url` / `auth` and friends are deliberately excluded, and
//! the rendered output is run through the secret scrubber as a backstop.

use crate::cli::ExplainArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole, expand};
use serde::Serialize;
use serde_json::Value;

/// Structural connector fields that are safe to surface in a narration — never
/// credentials or endpoints that can embed them. `url` / `base_url` /
/// `connection_url` are intentionally absent (they routinely carry `user:pass@`).
const SAFE_DESCRIPTOR_KEYS: &[&str] = &[
    "table_name",
    "table",
    "path",
    "topic",
    "topics",
    "index",
    "bucket",
    "prefix",
    "database",
    "collection",
    "dataset",
    "stream",
    "key_pattern",
    "pattern",
    "query",
];

/// Config keys that mark a source as doing incremental (bookmark-based) reads.
const INCREMENTAL_KEYS: &[&str] = &[
    "replication",
    "incremental",
    "cursor_field",
    "replication_key",
    "start_replication_value",
    "bookmark_key",
];

/// Above this many matrix rows, prose output summarizes with counts instead of
/// narrating every row (override with `--rows`).
const SUMMARIZE_THRESHOLD: usize = 8;

/// Execute the `explain` subcommand.
pub async fn run(args: ExplainArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };

    // Offline: tolerate (do not fetch) secret-manager directives — `explain`
    // never touches the network. `${env:…}` is resolved at load time, so the
    // narration only ever surfaces the safe allowlist below, never raw config.
    let cfg = PipelineConfig::from_path_tolerating_secrets(&path, args.profile.as_deref())?;
    let nodes = expand(&crate::partition::offline(&cfg))?;
    let report = build_report(&cfg, &nodes);

    if args.json {
        let json = serde_json::to_string_pretty(&report)
            .map_err(|e| CliError::Config(format!("cannot serialize explanation: {e}")))?;
        println!("{}", crate::secrets::registry::redact(&json));
    } else {
        let prose = render_prose(&report, args.rows);
        print!("{}", crate::secrets::registry::redact(&prose));
    }
    Ok(())
}

/// A machine-readable explanation (`--json`) and the source for the prose.
#[derive(Debug, Serialize)]
pub(crate) struct Explanation {
    pub pipeline: String,
    pub rows_total: usize,
    pub roots: usize,
    pub children: usize,
    pub incremental_rows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication: Option<String>,
    pub rows: Vec<RowExplanation>,
}

#[derive(Debug, Serialize)]
pub(crate) struct RowExplanation {
    pub id: String,
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub source: String,
    pub transforms: Vec<String>,
    pub sink: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub delivery_guarantee: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    pub incremental: bool,
}

/// Build the full explanation from the resolved config + expanded nodes.
pub(crate) fn build_report(cfg: &PipelineConfig, nodes: &[ExpandedNode]) -> Explanation {
    let roots = nodes
        .iter()
        .filter(|n| matches!(n.role, NodeRole::Root))
        .count();
    let rows: Vec<RowExplanation> = nodes.iter().map(row_explanation).collect();
    let incremental_rows = rows.iter().filter(|r| r.incremental).count();
    Explanation {
        pipeline: cfg.name.clone().unwrap_or_else(|| "(unnamed)".to_string()),
        rows_total: nodes.len(),
        roots,
        children: nodes.len() - roots,
        incremental_rows,
        replication: cfg
            .replication
            .as_ref()
            .map(|r| format!("{:?}", r.mode).to_lowercase()),
        rows,
    }
}

fn row_explanation(node: &ExpandedNode) -> RowExplanation {
    let (role, parent) = match &node.role {
        NodeRole::Root => ("root".to_string(), None),
        NodeRole::Child { parent_id, .. } => ("child".to_string(), Some(parent_id.clone())),
        NodeRole::Discovery { .. } => ("discovery".to_string(), None),
        NodeRole::Product { dims, .. } => (format!("product[{}]", dims.join(",")), None),
    };
    RowExplanation {
        id: node.id.clone(),
        role,
        parent,
        source: describe_connector(&node.source.kind, &node.source.config),
        transforms: node.transforms.iter().map(|t| t.kind.clone()).collect(),
        sink: describe_connector(&node.sink.kind, &node.sink.config),
        write_mode: string_field(&node.sink.config, "write_mode"),
        key: key_field(&node.sink.config),
        delivery_guarantee: node.delivery_guarantee.to_string(),
        state: node.state.as_ref().map(|s| s.kind.clone()),
        incremental: INCREMENTAL_KEYS
            .iter()
            .any(|k| node.source.config.get(*k).is_some()),
    }
}

/// `kind (field=value, …)` using only the safe descriptor allowlist. Falls back
/// to bare `kind` when no allowlisted field is present.
fn describe_connector(kind: &str, config: &Value) -> String {
    let Some(obj) = config.as_object() else {
        return kind.to_string();
    };
    let mut parts = Vec::new();
    for k in SAFE_DESCRIPTOR_KEYS {
        if let Some(v) = obj.get(*k) {
            parts.push(format!("{k}={}", scalar_str(v)));
            if parts.len() == 2 {
                break; // two identifying fields is plenty for a narration
            }
        }
    }
    if parts.is_empty() {
        kind.to_string()
    } else {
        format!("{kind} ({})", parts.join(", "))
    }
}

/// A compact, non-secret rendering of a scalar (or a shape hint for containers).
fn scalar_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Array(a) => format!("[{} item(s)]", a.len()),
        Value::Object(_) => "{…}".to_string(),
        Value::Null => "null".to_string(),
    }
}

fn string_field(config: &Value, key: &str) -> Option<String> {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.to_string())
}

/// Render a `key` field that may be a string or an array of column names.
fn key_field(config: &Value) -> Option<String> {
    match config.get("key") {
        Some(Value::String(s)) => Some(s.clone()),
        Some(Value::Array(a)) => {
            let cols: Vec<String> = a
                .iter()
                .filter_map(Value::as_str)
                .map(|s| s.to_string())
                .collect();
            (!cols.is_empty()).then(|| cols.join(", "))
        }
        _ => None,
    }
}

/// Render the explanation as prose. Large matrices are summarized unless
/// `show_all` (`--rows`) is set.
pub(crate) fn render_prose(r: &Explanation, show_all: bool) -> String {
    let mut out = String::new();
    if r.rows_total == 0 {
        out.push_str(&format!(
            "Pipeline '{}' has no runnable rows.\n",
            r.pipeline
        ));
        return out;
    }

    // Intro line: expansion shape.
    if r.rows_total == 1 {
        out.push_str(&format!(
            "Pipeline '{}' is a single pipeline.\n",
            r.pipeline
        ));
    } else {
        out.push_str(&format!(
            "Pipeline '{}' expands to {} rows ({} root{}, {} child{}).",
            r.pipeline,
            r.rows_total,
            r.roots,
            if r.roots == 1 { "" } else { "s" },
            r.children,
            if r.children == 1 { "" } else { "ren" },
        ));
        if r.incremental_rows > 0 {
            out.push_str(&format!(
                " {} row{} incremental.",
                r.incremental_rows,
                if r.incremental_rows == 1 {
                    " is"
                } else {
                    "s are"
                }
            ));
        }
        out.push('\n');
    }
    if let Some(mode) = &r.replication {
        out.push_str(&format!("Replication mode: {mode}.\n"));
    }
    out.push('\n');

    let summarize = !show_all && r.rows_total > SUMMARIZE_THRESHOLD;
    let shown = if summarize {
        SUMMARIZE_THRESHOLD
    } else {
        r.rows.len()
    };
    for row in r.rows.iter().take(shown) {
        out.push_str(&narrate_row(row));
    }
    if summarize {
        out.push_str(&format!(
            "… and {} more row(s). Pass --rows to narrate every row.\n",
            r.rows_total - shown
        ));
    }
    out
}

fn narrate_row(row: &RowExplanation) -> String {
    let lineage = if row.transforms.is_empty() {
        " → ".to_string()
    } else {
        format!(" → applies {} → ", row.transforms.join(", "))
    };
    let parent = match &row.parent {
        Some(p) => format!(" (per record from '{p}')"),
        None => String::new(),
    };
    let write = match (&row.write_mode, &row.key) {
        (Some(mode), Some(key)) => format!(" [{mode} on {key}]"),
        (Some(mode), None) => format!(" [{mode}]"),
        _ => String::new(),
    };
    let state = match &row.state {
        Some(s) => format!(", state: {s}"),
        None => String::new(),
    };
    format!(
        "• {}{}: reads from {}{}writes to {}{}. delivery: {}{}.\n",
        row.id, parent, row.source, lineage, row.sink, write, row.delivery_guarantee, state,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_with_extension;

    fn explain_yaml(yaml: &str) -> Explanation {
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        build_report(&cfg, &nodes)
    }

    #[test]
    fn describe_connector_uses_only_safe_fields() {
        let cfg = serde_json::json!({
            "table_name": "orders",
            "connection_url": "postgres://user:secret@host/db",
            "auth": { "token": "hunter2" }
        });
        let d = describe_connector("postgres", &cfg);
        assert!(d.contains("table_name=orders"), "{d}");
        assert!(!d.contains("secret"), "must not leak connection_url: {d}");
        assert!(!d.contains("hunter2"), "must not leak auth: {d}");
    }

    #[test]
    fn single_pipeline_prose_names_source_and_sink() {
        let r = explain_yaml(
            r#"
version: 1
name: demo
pipeline:
  source: { type: rest, config: { path: /events } }
  sink: { type: jsonl, config: { path: out.jsonl, append: true } }
  transforms:
    - { type: flatten }
"#,
        );
        assert_eq!(r.rows_total, 1);
        let prose = render_prose(&r, false);
        assert!(prose.contains("reads from rest"), "{prose}");
        assert!(prose.contains("applies flatten"), "{prose}");
        assert!(prose.contains("writes to jsonl"), "{prose}");
        assert!(prose.contains("delivery:"), "{prose}");
    }

    #[test]
    fn matrix_expansion_and_upsert_are_reported() {
        let r = explain_yaml(
            r#"
version: 1
name: fan
pipeline:
  source: { type: rest, config: {} }
  sink:
    type: postgres
    config:
      connection_url: "postgres://localhost/db"
      table_name: t
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
matrix:
  - id: us
  - id: eu
"#,
        );
        assert_eq!(r.rows_total, 2);
        assert_eq!(r.roots, 2);
        let row = &r.rows[0];
        assert_eq!(row.write_mode.as_deref(), Some("upsert"));
        assert_eq!(row.key.as_deref(), Some("id"));
        assert!(row.delivery_guarantee.contains("effectively-once"));
        let prose = render_prose(&r, false);
        assert!(prose.contains("expands to 2 rows"), "{prose}");
        assert!(prose.contains("[upsert on id]"), "{prose}");
    }

    #[test]
    fn parent_child_matrix_describes_fan_out() {
        let r = explain_yaml(
            r#"
version: 1
name: pc
pipeline:
  source: { type: rest, config: {} }
  sink: { type: jsonl, config: { path: o, append: true } }
matrix:
  - id: dims
  - id: facts
    parent: dims
    parent_key: id
"#,
        );
        assert_eq!(r.roots, 1);
        assert_eq!(r.children, 1);
        let child = r.rows.iter().find(|x| x.id == "facts").unwrap();
        assert_eq!(child.parent.as_deref(), Some("dims"));
        let prose = render_prose(&r, true);
        assert!(prose.contains("per record from 'dims'"), "{prose}");
    }

    #[test]
    fn large_matrix_summarizes_without_rows_flag() {
        let mut yaml = String::from(
            "version: 1\nname: big\npipeline:\n  source: { type: rest, config: {} }\n  sink: { type: jsonl, config: { path: o, append: true } }\nmatrix:\n",
        );
        for i in 0..20 {
            yaml.push_str(&format!("  - id: r{i}\n"));
        }
        let r = explain_yaml(&yaml);
        let summarized = render_prose(&r, false);
        assert!(summarized.contains("and 12 more row(s)"), "{summarized}");
        let full = render_prose(&r, true);
        assert!(!full.contains("more row(s)"), "--rows narrates all");
    }

    #[test]
    fn json_output_is_serializable_and_deterministic() {
        let r = explain_yaml(
            r#"
version: 1
name: j
pipeline:
  source: { type: rest, config: { path: /x } }
  sink: { type: jsonl, config: { path: o, append: true } }
"#,
        );
        let a = serde_json::to_string(&r).unwrap();
        let b = serde_json::to_string(&explain_yaml(
            "version: 1\nname: j\npipeline:\n  source: { type: rest, config: { path: /x } }\n  sink: { type: jsonl, config: { path: o, append: true } }\n",
        ))
        .unwrap();
        assert_eq!(a, b);
    }

    // ── `run` command flow (offline; tempfile) ───────────────────────────────

    fn write_cfg(body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("faucet.yaml");
        std::fs::write(&path, body).expect("write");
        (dir, path)
    }

    fn args(path: std::path::PathBuf, json: bool, rows: bool) -> ExplainArgs {
        ExplainArgs {
            config: Some(path),
            env_file: None,
            no_env_file: true,
            profile: None,
            json,
            rows,
        }
    }

    const CFG: &str = "version: 1\nname: demo\npipeline:\n  source: { type: rest, config: { path: /x } }\n  sink: { type: jsonl, config: { path: o, append: true } }\n";

    #[tokio::test]
    async fn run_prose_succeeds() {
        let (_d, path) = write_cfg(CFG);
        run(args(path, false, false)).await.expect("prose ok");
    }

    #[tokio::test]
    async fn run_json_succeeds() {
        let (_d, path) = write_cfg(CFG);
        run(args(path, true, false)).await.expect("json ok");
    }

    #[tokio::test]
    async fn run_prose_all_rows_on_a_matrix() {
        // A matrix config exercises the `--rows` (narrate every row) path.
        let cfg = "version: 1\nname: m\nmatrix:\n  - { id: a }\n  - { id: b }\npipeline:\n  source: { type: rest, config: { path: /x } }\n  sink: { type: jsonl, config: { path: o, append: true } }\n";
        let (_d, path) = write_cfg(cfg);
        run(args(path, false, true)).await.expect("matrix prose ok");
    }
}
