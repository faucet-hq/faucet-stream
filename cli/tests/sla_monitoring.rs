//! End-to-end wiring for the top-level `sla:` block (#202): the expand-time
//! gates, the executor's post-run evaluation + baseline persistence, the
//! never-fails-the-run guarantee, and the `faucet doctor` SLA probes.

use faucet_cli::config::PipelineConfig;
use faucet_cli::error::CliError;
use faucet_cli::expand::expand;
use faucet_core::StateStore;
use std::path::Path;

fn csv_to_jsonl_yaml(input: &Path, output: &Path, state_dir: &Path, sla: &str) -> String {
    format!(
        r#"version: 1
name: slatest
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink: {{ type: jsonl, config: {{ path: {output} }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
{sla}
"#,
        input = input.display(),
        output = output.display(),
        state = state_dir.display(),
    )
}

fn parse(yaml: &str) -> PipelineConfig {
    PipelineConfig::from_text(yaml, Path::new("sla.yaml")).expect("config parses")
}

// ── expand-time gates ────────────────────────────────────────────────────────

#[test]
fn expand_rejects_stateful_sla_without_state_block() {
    let cfg = parse(
        r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
sla:
  max_staleness_secs: 3600
"#,
    );
    let err = expand(&cfg).unwrap_err();
    assert!(
        matches!(&err, CliError::Config(m) if m.contains("state") && m.contains("sla")),
        "expected the sla-needs-state gate, got: {err}"
    );
}

#[test]
fn expand_rejects_invalid_sla_spec() {
    // An empty block declares no checks — caught by SlaSpec::validate.
    let cfg = parse(
        r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
sla: {}
"#,
    );
    let err = expand(&cfg).unwrap_err();
    assert!(
        matches!(&err, CliError::Config(m) if m.contains("declares no checks")),
        "expected the empty-spec rejection, got: {err}"
    );

    // A zero sensitivity is rejected by the same gate.
    let cfg = parse(
        r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
  state: { type: memory, config: {} }
sla:
  volume_anomaly: { method: zscore, sensitivity: 0 }
"#,
    );
    let err = expand(&cfg).unwrap_err();
    assert!(
        matches!(&err, CliError::Config(m) if m.contains("sensitivity")),
        "expected the sensitivity rejection, got: {err}"
    );
}

#[test]
fn expand_allows_stateless_min_rows_without_state() {
    let cfg = parse(
        r#"version: 1
pipeline:
  source: { type: csv, config: { path: in.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
sla:
  min_rows_per_run: 1
"#,
    );
    let nodes = expand(&cfg).expect("stateless min_rows needs no state block");
    assert_eq!(nodes.len(), 1);
}

// ── executor post-run evaluation ─────────────────────────────────────────────

#[tokio::test]
async fn successful_run_persists_sla_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let state_dir = dir.path().join("state");
    std::fs::write(&input, "id\n1\n2\n3\n").unwrap();

    let yaml = csv_to_jsonl_yaml(
        &input,
        &output,
        &state_dir,
        "sla:\n  max_staleness_secs: 3600\n  min_rows_per_run: 1\n",
    );
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.expect("run ok");
    assert!(!summary.had_failures());
    assert_eq!(summary.invocations[0].records_written, 3);

    // The baseline landed under `{name}::{row}::__sla__` in the file store.
    let store = faucet_core::FileStateStore::new(&state_dir);
    let v = store
        .get("slatest::row-0::__sla__")
        .await
        .unwrap()
        .expect("SLA state persisted");
    assert_eq!(v["volumes"], serde_json::json!([3]));
    assert!(v["last_success_unix"].as_i64().unwrap() > 0);

    // A second run appends to the rolling baseline.
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.expect("run ok");
    assert!(!summary.had_failures());
    let v = store.get("slatest::row-0::__sla__").await.unwrap().unwrap();
    assert_eq!(v["volumes"], serde_json::json!([3, 3]));
}

#[tokio::test]
async fn sla_violation_never_fails_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let state_dir = dir.path().join("state");
    std::fs::write(&input, "id\n1\n").unwrap();

    // The floor is far above the actual volume — the violation fires, but the
    // run must still succeed and write its records.
    let yaml = csv_to_jsonl_yaml(
        &input,
        &output,
        &state_dir,
        "sla:\n  min_rows_per_run: 100\n",
    );
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.expect("run ok");
    assert!(!summary.had_failures(), "SLA violations must not fail runs");
    assert_eq!(summary.invocations[0].records_written, 1);
}

#[tokio::test]
async fn failed_run_evaluates_staleness_without_breaking_error_reporting() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope.csv");
    let output = dir.path().join("out.jsonl");
    let state_dir = dir.path().join("state");

    // Seed a last-success far in the past so the failure path walks the
    // staleness branch (violation fires; the run error is preserved).
    let store = faucet_core::FileStateStore::new(&state_dir);
    store
        .put(
            "slatest::row-0::__sla__",
            &serde_json::json!({"last_success_unix": 1, "volumes": [5, 5, 5]}),
        )
        .await
        .unwrap();

    let yaml = csv_to_jsonl_yaml(
        &missing,
        &output,
        &state_dir,
        "sla:\n  max_staleness_secs: 60\n",
    );
    let summary = faucet_cli::run_from_yaml_str(&yaml).await.expect("summary");
    assert!(summary.had_failures(), "the source error is still reported");
    // The failed run must not have touched the success history.
    let v = store.get("slatest::row-0::__sla__").await.unwrap().unwrap();
    assert_eq!(v["last_success_unix"], serde_json::json!(1));
    assert_eq!(v["volumes"], serde_json::json!([5, 5, 5]));
}

#[tokio::test]
async fn dry_run_skips_sla_evaluation() {
    use faucet_cli::executor::{ExecuteOptions, run_expanded};

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let state_dir = dir.path().join("state");
    std::fs::write(&input, "id\n1\n").unwrap();

    let yaml = csv_to_jsonl_yaml(&input, &output, &state_dir, "sla:\n  min_rows_per_run: 1\n");
    let cfg = parse(&yaml);
    let nodes = expand(&cfg).unwrap();
    let summary = run_expanded(
        nodes,
        ExecuteOptions {
            pipeline_name: "slatest".into(),
            run_id: None,
            execution: None,
            concurrency: None,
            dry_run: true,
            limit: None,
            state_path_override: None,
            shard: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
            cancel: None,
            resilience: None,
            sla: cfg.sla.clone(),
            reconcile: cfg.reconcile.clone(),
            verify: cfg.verify.clone(),
            rollback: cfg.rollback.clone(),
            #[cfg(feature = "lineage")]
            lineage: None,
            #[cfg(feature = "lineage")]
            lineage_cfg: None,
            #[cfg(feature = "notify")]
            notifier: None,
            #[cfg(feature = "catalog")]
            catalog: None,
        },
    )
    .await
    .unwrap();
    assert!(!summary.had_failures());

    // No baseline recorded: a dry-run's synthetic volume must not poison it.
    let store = faucet_core::FileStateStore::new(&state_dir);
    assert!(
        store
            .get("slatest::row-0::__sla__")
            .await
            .unwrap()
            .is_none()
    );
}

// ── doctor probes ────────────────────────────────────────────────────────────

#[tokio::test]
async fn doctor_reports_sla_probes_cold_and_warm() {
    use faucet_cli::commands::doctor::probe_roots;
    use faucet_core::check::{CheckContext, ProbeStatus};

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let state_dir = dir.path().join("state");
    std::fs::write(&input, "id\n1\n2\n").unwrap();

    let yaml = csv_to_jsonl_yaml(
        &input,
        &output,
        &state_dir,
        "sla:\n  max_staleness_secs: 86400\n  volume_anomaly: { min_history: 3 }\n",
    );
    let cfg = parse(&yaml);
    let nodes = expand(&cfg).unwrap();
    let auth = Default::default();
    let ctx = CheckContext::default();

    // Cold start: staleness (no success yet) and baseline (0/3) both skip.
    let invs = probe_roots(&nodes, &auth, &ctx, cfg.sla.as_ref(), "slatest").await;
    let sla_probes: Vec<_> = invs[0].probes.iter().filter(|p| p.role == "sla").collect();
    assert_eq!(sla_probes.len(), 2, "{:?}", invs[0].probes);
    assert_eq!(sla_probes[0].name, "staleness");
    assert!(matches!(sla_probes[0].status, ProbeStatus::Skip { .. }));
    assert_eq!(sla_probes[1].name, "baseline");
    assert!(matches!(sla_probes[1].status, ProbeStatus::Skip { .. }));

    // After a successful run the staleness probe passes (fresh); the baseline
    // is still warming (1/3).
    faucet_cli::run_from_yaml_str(&yaml).await.expect("run ok");
    let invs = probe_roots(&nodes, &auth, &ctx, cfg.sla.as_ref(), "slatest").await;
    let sla_probes: Vec<_> = invs[0].probes.iter().filter(|p| p.role == "sla").collect();
    assert!(
        matches!(sla_probes[0].status, ProbeStatus::Pass),
        "{sla_probes:?}"
    );
    assert!(matches!(sla_probes[1].status, ProbeStatus::Skip { .. }));

    // A stale history turns the staleness probe red.
    let store = faucet_core::FileStateStore::new(&state_dir);
    store
        .put(
            "slatest::row-0::__sla__",
            &serde_json::json!({"last_success_unix": 1, "volumes": [2, 2, 2]}),
        )
        .await
        .unwrap();
    let invs = probe_roots(&nodes, &auth, &ctx, cfg.sla.as_ref(), "slatest").await;
    let sla_probes: Vec<_> = invs[0].probes.iter().filter(|p| p.role == "sla").collect();
    assert!(
        matches!(sla_probes[0].status, ProbeStatus::Fail { .. }),
        "{sla_probes:?}"
    );
    // Warm baseline (3/3) now passes.
    assert!(matches!(sla_probes[1].status, ProbeStatus::Pass));

    // Without an `sla:` block no probes are added.
    let invs = probe_roots(&nodes, &auth, &ctx, None, "slatest").await;
    assert!(invs[0].probes.iter().all(|p| p.role != "sla"));
}
