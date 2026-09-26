//! End-to-end cost & usage accounting (#704) and run budgets (#703) through
//! the real CLI path: config → `expand` → `run_expanded`.
//!
//! The meter and the budget decorator are unit-tested in `faucet-core`; what
//! these cover is the wiring — that every invocation outcome carries a priced
//! usage record (failed ones too), that a records ceiling refuses the page
//! before anything lands, and that `allowed_sinks` refuses the run before it
//! starts.
#![cfg(all(feature = "source-csv", feature = "sink-jsonl"))]

use faucet_cli::config::PipelineConfig;
use faucet_cli::error::CliError;
use faucet_cli::executor::{ExecuteOptions, InvocationErrorKind, run_expanded};
use faucet_cli::expand::expand;
use faucet_core::BudgetSpec;
use std::path::Path;

fn pipeline_yaml(input: &Path, output: &Path, extra: &str) -> String {
    format!(
        r#"version: 1
name: usage_test
pipeline:
  source:
    type: csv
    config: {{ path: "{}" }}
  sink:
    type: jsonl
    config: {{ path: "{}" }}
{extra}
"#,
        input.display(),
        output.display()
    )
}

fn opts(budget: Option<BudgetSpec>) -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: "usage_test".into(),
        run_id: None,
        execution: None,
        concurrency: None,
        dry_run: false,
        limit: None,
        state_path_override: None,
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
        budget,
    }
}

fn write_input(dir: &Path) -> std::path::PathBuf {
    let input = dir.join("in.csv");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n3,carol\n4,dave\n5,erin\n").unwrap();
    input
}

#[tokio::test]
async fn every_invocation_outcome_carries_a_priced_usage_record() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_input(dir.path());
    let output = dir.path().join("out.jsonl");
    let cfg =
        PipelineConfig::from_text(&pipeline_yaml(&input, &output, ""), Path::new("usage.yaml"))
            .unwrap();
    let nodes = expand(&cfg).unwrap();
    let summary = run_expanded(nodes, opts(None)).await.unwrap();
    let inv = &summary.invocations[0];
    assert!(inv.error.is_none(), "{inv:?}");
    let usage = inv.usage.as_ref().expect("usage record on the outcome");
    assert_eq!(usage.pipeline, "usage_test");
    assert_eq!(usage.source_kind, "csv");
    assert_eq!(usage.sink_kind, "jsonl");
    assert!(!usage.failed);
    assert_eq!(usage.usage.records_read, 5);
    assert_eq!(usage.usage.records_written, 5);
    assert!(usage.usage.bytes_written > 0);
    assert_eq!(usage.usage.bytes_read, usage.usage.bytes_written);
    assert_eq!(usage.cost.currency, "USD");
    assert!(usage.cost.hosted_equivalent > 0.0);
    // Two local files: no egress, no requests, no warehouse — nothing to price.
    assert_eq!(usage.cost.total, 0.0);
    assert!(usage.cost.not_reported.is_empty());
    assert_eq!(inv.run_id.as_deref(), Some(usage.run_id.as_str()));
}

#[tokio::test]
async fn a_records_ceiling_refuses_the_page_and_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_input(dir.path());
    let output = dir.path().join("out.jsonl");
    let cfg = PipelineConfig::from_text(
        &pipeline_yaml(&input, &output, "budget:\n  max_records: 3\n"),
        Path::new("usage.yaml"),
    )
    .unwrap();
    assert_eq!(cfg.budget.as_ref().unwrap().max_records, Some(3));
    let nodes = expand(&cfg).unwrap();
    // The caller's ceiling merges with the config's (stricter wins): this
    // run enforces max_records = 3 either way.
    let budget = faucet_cli::budget::effective_budget(
        cfg.budget.as_ref(),
        Some(BudgetSpec {
            max_records: Some(100),
            ..Default::default()
        }),
    )
    .unwrap();
    let summary = run_expanded(nodes, opts(budget)).await.unwrap();
    let inv = &summary.invocations[0];
    let err = inv.error.as_deref().expect("the run fails");
    assert!(err.contains("max_records"), "{err}");
    assert!(err.contains("limit 3"), "{err}");
    assert_eq!(inv.error_kind, Some(InvocationErrorKind::BudgetExceeded));
    assert_eq!(inv.records_written, 0, "the crossing page never landed");
    // The output holds nothing: the page was refused before the sink saw it.
    let written = std::fs::read_to_string(&output).unwrap_or_default();
    assert!(written.trim().is_empty(), "{written}");
    // A failed invocation still reports what it consumed.
    let usage = inv.usage.as_ref().expect("usage on a failed invocation");
    assert!(usage.failed);
    assert_eq!(usage.usage.records_read, 5);
    assert_eq!(usage.usage.records_written, 0);
}

#[tokio::test]
async fn allowed_sinks_refuses_the_run_before_it_starts() {
    let dir = tempfile::tempdir().unwrap();
    let input = write_input(dir.path());
    let output = dir.path().join("out.jsonl");
    let cfg = PipelineConfig::from_text(
        &pipeline_yaml(
            &input,
            &output,
            "budget:\n  allowed_sinks: [warehouse, postgres]\n",
        ),
        Path::new("usage.yaml"),
    )
    .unwrap();
    let nodes = expand(&cfg).unwrap();
    let err = run_expanded(nodes, opts(cfg.budget.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, CliError::BudgetSinkNotAllowed { sink, allowed, .. } if sink.contains("jsonl") && allowed == "warehouse, postgres"),
        "{err}"
    );
    assert!(err.to_string().contains("budget.allowed_sinks"), "{err}");
    assert!(!output.exists(), "nothing ran");

    // Naming the sink by template name or kind lets the run through.
    let cfg = PipelineConfig::from_text(
        &pipeline_yaml(&input, &output, "budget:\n  allowed_sinks: [jsonl]\n"),
        Path::new("usage.yaml"),
    )
    .unwrap();
    let nodes = expand(&cfg).unwrap();
    let summary = run_expanded(nodes, opts(cfg.budget.clone())).await.unwrap();
    assert!(summary.invocations[0].error.is_none());
    assert_eq!(summary.invocations[0].records_written, 5);
}
