#![cfg(all(feature = "catalog", feature = "serve-history-sqlite"))]
//! End-to-end for the config-change preview (#374): a successful `faucet run`
//! with a `catalog:` block records a config snapshot, and `faucet plan --diff`
//! then diffs the current config against it. Exercises the real command wiring
//! (`run`'s record hook + `plan --diff`) over a sqlite catalog on local disk.

use std::path::PathBuf;

fn run_args(config: PathBuf) -> faucet_cli::cli::RunArgs {
    faucet_cli::cli::RunArgs {
        config: Some(config),
        no_env_file: true,
        ..Default::default()
    }
}

fn plan_diff_args(config: PathBuf) -> faucet_cli::cli::PlanArgs {
    faucet_cli::cli::PlanArgs {
        config: Some(config),
        row: None,
        sample: None,
        live: false,
        limit: 10,
        json: false,
        diff: true,
        impact: false,
        depth: 5,
        resolve_secrets: false,
        profile: None,
        policy: None,
    }
}

#[tokio::test]
async fn run_records_snapshot_then_plan_diff_reads_it() {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    let output = dir.path().join("out.jsonl");
    let db = dir.path().join("cat.db");
    std::fs::write(&input, "id,name\n1,alice\n2,bob\n").unwrap();

    let cfg_path = dir.path().join("pipeline.yaml");
    std::fs::write(
        &cfg_path,
        format!(
            "version: 1\nname: e2ediff\ncatalog:\n  url: \"sqlite:{db}\"\npipeline:\n  source: {{ type: csv, config: {{ path: {input} }} }}\n  sink: {{ type: jsonl, config: {{ path: {output} }} }}\n",
            db = db.display(),
            input = input.display(),
            output = output.display(),
        ),
    )
    .unwrap();

    // A successful run must record a config snapshot (best-effort hook).
    faucet_cli::commands::run::run(run_args(cfg_path.clone()))
        .await
        .expect("run");

    let handle = faucet_cli::catalog::connect_from_spec(&faucet_cli::catalog::CatalogSpec {
        url: format!("sqlite:{}", db.display()),
        sample_records: 10,
        datasets: Vec::new(),
    })
    .await
    .unwrap();
    let snap = handle
        .store
        .catalog_last_config_snapshot("e2ediff")
        .await
        .unwrap();
    assert!(snap.is_some(), "run should have recorded a config snapshot");
    let snap = snap.unwrap();
    assert_eq!(snap.pipeline, "e2ediff");
    assert!(!snap.rows.is_empty());

    // `plan --diff` now compares the current config against the recorded run.
    faucet_cli::commands::plan::run(plan_diff_args(cfg_path))
        .await
        .expect("plan --diff against recorded snapshot");
}
