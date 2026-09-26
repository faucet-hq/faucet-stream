//! End-to-end wiring for completeness reconciliation (#502): a run whose rows
//! written fall short of an authoritative count probe must **fail**, and a
//! complete run must pass.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use faucet_cli::reconcile::{CountProbe, ReconcileSpec};
use serde_json::json;
use std::path::Path;

fn pipeline_yaml(input: &Path, output: &Path) -> String {
    format!(
        r#"version: 1
name: recon_test
pipeline:
  source:
    type: csv
    config: {{ path: "{}" }}
  sink:
    type: jsonl
    config: {{ path: "{}" }}
"#,
        input.display(),
        output.display()
    )
}

fn opts_with_reconcile(reconcile: Option<ReconcileSpec>) -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: "recon_test".into(),
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
        reconcile,
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

fn count_probe(dir: &Path, n: u64) -> ReconcileSpec {
    let count_csv = dir.join(format!("count_{n}.csv"));
    std::fs::write(&count_csv, format!("n\n{n}\n")).unwrap();
    ReconcileSpec {
        count: CountProbe {
            kind: "csv".into(),
            config: json!({ "path": count_csv.to_str().unwrap() }),
            count_field: Some("n".into()),
        },
        tolerance_pct: 0.0,
    }
}

#[tokio::test]
async fn complete_run_passes_reconciliation() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id\n1\n2\n").unwrap(); // 2 rows written

    let cfg: PipelineConfig =
        PipelineConfig::from_text(&pipeline_yaml(&input, &output), Path::new("recon.yaml"))
            .unwrap();
    let nodes = expand(&cfg).unwrap();
    // Authoritative count = 2, matches rows written → passes.
    let summary = run_expanded(nodes, opts_with_reconcile(Some(count_probe(dir.path(), 2))))
        .await
        .unwrap();
    assert!(
        !summary.had_failures(),
        "a complete run must pass reconciliation"
    );
}

#[tokio::test]
async fn shortfall_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    std::fs::write(&input, "id\n1\n2\n").unwrap(); // only 2 rows written

    let cfg: PipelineConfig =
        PipelineConfig::from_text(&pipeline_yaml(&input, &output), Path::new("recon.yaml"))
            .unwrap();
    let nodes = expand(&cfg).unwrap();
    // Authoritative count = 5 but only 2 written → shortfall → run fails.
    let summary = run_expanded(nodes, opts_with_reconcile(Some(count_probe(dir.path(), 5))))
        .await
        .unwrap();
    assert!(
        summary.had_failures(),
        "a shortfall vs the authoritative count must fail the run"
    );
}
