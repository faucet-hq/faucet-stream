//! How the executor reports invocations that did not run to completion:
//! rows a cancel kept from starting, and `--limit` previews.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, InvocationErrorKind, run_expanded};
use faucet_cli::expand::expand;
use faucet_core::CancellationToken;
use std::path::Path;

fn opts(name: &str) -> ExecuteOptions {
    ExecuteOptions {
        legacy_state_writes: false,
        force_lease: false,
        pipeline_name: name.into(),
        run_id: None,
        execution: None,
        concurrency: None,
        dry_run: false,
        limit: None,
        state_path_override: None,
        state_scope: Default::default(),
        shard: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
        cancel: None,
        resilience: None,
        sla: None,
        reconcile: None,
        verify: None,
        rollback: None,
        #[cfg(feature = "lineage")]
        lineage: None,
        #[cfg(feature = "lineage")]
        lineage_cfg: None,
        #[cfg(feature = "notify")]
        notifier: None,
        #[cfg(feature = "catalog")]
        catalog: None,
        usage: Default::default(),
        budget: None,
    }
}

fn load(yaml: &str) -> PipelineConfig {
    PipelineConfig::from_text(yaml, Path::new("outcomes.yaml")).unwrap()
}

/// An external cancel (serve cancel / timeout / drain) on an `on_error: stop`
/// config skips the rows it kept from starting, and says the run was
/// cancelled rather than blaming `on_error: stop`.
#[tokio::test]
async fn rows_an_external_cancel_kept_from_starting_say_so() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "id\n1\n2\n").unwrap();
    let cfg = load(&format!(
        r#"version: 1
name: cancelled
execution: {{ on_error: stop }}
pipeline:
  source: {{ type: csv, config: {{ path: "{input}" }} }}
  sink: {{ type: jsonl, config: {{ path: "{dir}/out-a.jsonl" }} }}
matrix:
  - id: a
  - id: b
    sink: {{ config: {{ path: "{dir}/out-b.jsonl" }} }}
"#,
        input = input.display(),
        dir = dir.path().display(),
    ));
    let token = CancellationToken::new();
    token.cancel();
    let mut o = opts("cancelled");
    o.cancel = Some(token);
    o.execution = cfg.execution.clone();
    let summary = run_expanded(expand(&cfg).unwrap(), o).await.unwrap();
    assert_eq!(summary.invocations.len(), 2);
    for inv in &summary.invocations {
        assert_eq!(
            inv.error_kind,
            Some(InvocationErrorKind::Skipped),
            "{inv:?}"
        );
        let err = inv.error.as_deref().unwrap();
        assert!(err.contains("the run was cancelled"), "{err}");
        assert!(!err.contains("on_error"), "{err}");
    }
    assert!(!summary.had_failures());
}

fn csv_rows(path: &Path, n: usize) {
    let body: String = std::iter::once("id\n".to_string())
        .chain((0..n).map(|i| format!("{i}\n")))
        .collect();
    std::fs::write(path, body).unwrap();
}

/// `--limit` on a config with a `dlq:` block (the per-row write path): rows
/// past the cap are not written, so neither the written count nor the row
/// accounts may count them as accepted.
#[tokio::test]
async fn limit_on_the_per_row_path_reports_rows_past_the_cap_as_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    csv_rows(&input, 100);
    let out = dir.path().join("out.jsonl");
    let cfg = load(&format!(
        r#"version: 1
name: capped
pipeline:
  source: {{ type: csv, config: {{ path: "{input}" }} }}
  sink: {{ type: jsonl, config: {{ path: "{out}" }} }}
  dlq: {{ sink: {{ type: jsonl, config: {{ path: "{dir}/dlq.jsonl" }} }} }}
"#,
        input = input.display(),
        out = out.display(),
        dir = dir.path().display(),
    ));
    let mut o = opts("capped");
    o.limit = Some(10);
    let summary = run_expanded(expand(&cfg).unwrap(), o).await.unwrap();
    let inv = &summary.invocations[0];
    assert!(inv.error.is_none(), "{inv:?}");
    assert_eq!(inv.records_written, 10);
    let rows = inv.metrics.as_ref().unwrap().rows.unwrap();
    assert_eq!(rows.offered, 100, "{rows:?}");
    assert_eq!(rows.accepted, 10, "{rows:?}");
    assert_eq!(rows.skipped, 90, "{rows:?}");
    assert_eq!(std::fs::read_to_string(&out).unwrap().lines().count(), 10);
}

/// The same cap in topology mode: each sink node reports the rows it wrote.
#[tokio::test]
async fn limit_on_a_topology_sink_node_reports_only_the_rows_it_wrote() {
    use faucet_cli::topology::{TopologyRunOptions, run_topology};
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    csv_rows(&input, 100);
    let out = dir.path().join("out.jsonl");
    let cfg = load(&format!(
        r#"version: 1
name: graph
pipeline:
  sources:
    orders: {{ type: csv, config: {{ path: "{input}" }} }}
  sinks:
    a: {{ type: jsonl, config: {{ path: "{out}" }} }}
  dlq: {{ sink: {{ type: jsonl, config: {{ path: "{dir}/dlq.jsonl" }} }} }}
  nodes:
    src: {{ kind: source, ref: orders }}
    wa: {{ kind: sink, ref: a }}
  edges:
    - {{ from: src, to: wa }}
"#,
        input = input.display(),
        out = out.display(),
        dir = dir.path().display(),
    ));
    let auth = faucet_cli::auth_catalog::build_auth_catalog(None).unwrap();
    let summary = run_topology(
        &cfg,
        &auth,
        TopologyRunOptions {
            limit: Some(10),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let node = summary
        .invocations
        .iter()
        .find(|i| i.row_id == "wa")
        .unwrap();
    assert!(node.error.is_none(), "{node:?}");
    assert_eq!(node.records_written, 10);
    assert_eq!(std::fs::read_to_string(&out).unwrap().lines().count(), 10);
}

/// Children fan out only over parent records that reached the destination: a
/// parent sink that declines rows by config (`stdout` `max_records`) does not
/// spawn children for the rows it dropped.
#[tokio::test]
async fn children_fan_out_only_over_parent_rows_the_sink_kept() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    csv_rows(&input, 5);
    let cfg = load(&format!(
        r#"version: 1
name: fanout
pipeline:
  sources:
    rows: {{ type: csv, config: {{ path: "{input}" }} }}
  sinks:
    shown: {{ type: stdout, config: {{ max_records: 2 }} }}
    files: {{ type: jsonl, config: {{ path: "{dir}/kid-${{parent.id}}.jsonl" }} }}
matrix:
  - id: parent
    source: {{ ref: rows }}
    sink: {{ ref: shown }}
  - id: kid
    parent: parent
    parent_key: id
    source: {{ ref: rows }}
    sink: {{ ref: files }}
"#,
        input = input.display(),
        dir = dir.path().display(),
    ));
    let summary = run_expanded(expand(&cfg).unwrap(), opts("fanout"))
        .await
        .unwrap();
    assert!(!summary.had_failures(), "{summary:?}");
    let kids = summary
        .invocations
        .iter()
        .filter(|i| i.row_id == "kid")
        .count();
    assert_eq!(kids, 2, "{summary:?}");
}
