use super::*;

/// The table output redacts error text like `--json` does (#789 CLI-162).
#[test]
fn the_table_redacts_run_errors() {
    crate::secrets::registry::register("s3cr3t-value-for-history");
    let mut r: RunRecord = serde_json::from_value(serde_json::json!({
        "run_id": "r1", "status": "failed", "submitted_at": "2026-01-01T00:00:00Z",
        "labels": {}, "invocations": [], "records_written": 0
    }))
    .unwrap();
    r.error = Some("auth failed with s3cr3t-value-for-history".into());
    let out = render_table(&[r]);
    assert!(!out.contains("s3cr3t-value-for-history"), "{out}");
}
use crate::serve::history::{InvocationRecord, RunStatus};
use chrono::{TimeZone, Utc};

/// A caller-chosen run id with a multibyte character at the truncation
/// point renders instead of panicking on a char boundary.
#[test]
fn a_multibyte_run_id_is_truncated_by_characters() {
    let r = record(
        "replay-2026-10-üüü-nightly-orders",
        RunStatus::Completed,
        1,
        Some(1.0),
    );
    let out = render_table(&[r]);
    assert!(out.contains("replay-2026-10-üüü-…"), "{out}");
}

fn record(id: &str, status: RunStatus, rows: u64, elapsed: Option<f64>) -> RunRecord {
    let t = Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap();
    RunRecord {
        run_id: id.into(),
        name: Some("demo".into()),
        labels: Default::default(),
        status,
        submitted_at: t,
        started_at: Some(t),
        finished_at: Some(t),
        elapsed_secs: elapsed,
        records_written: rows,
        invocations: vec![InvocationRecord {
            row_id: "us".into(),
            parent_record_key: None,
            run_id: None,
            records_written: rows as usize,
            duration_ms: 0,
            error: None,
            usage: None,
            batches: None,
            source_lag: None,
            rows: None,
            skipped: false,
            status: None,
        }],
        error: (status == RunStatus::Failed).then(|| "boom".to_string()),
        idempotency_key: None,
        doctor_report: None,
        config_body: None,
        config_format: None,
        timeout_secs: None,
        clock: None,
        concurrency: None,
        attempt: 0,
        replay_of: None,
        callback: None,
        tenant: None,
        selection: None,
        depends_on: None,
        host_access: false,
        caller_config: false,
        caller_params: Default::default(),
        pool: "default".into(),
    }
}

#[test]
fn table_lists_runs_with_status_and_throughput() {
    let runs = vec![
        record("run-1", RunStatus::Completed, 1000, Some(2.0)),
        record("run-2", RunStatus::Failed, 0, Some(0.5)),
    ];
    let t = render_table(&runs);
    assert!(t.contains("RUN ID"), "{t}");
    assert!(t.contains("run-1"), "{t}");
    assert!(t.contains("completed"), "{t}");
    assert!(t.contains("500"), "rows/s = 1000/2 = 500: {t}");
    assert!(t.contains("failed"), "{t}");
    assert!(t.contains("error: boom"), "failed run shows its error: {t}");
}

#[test]
fn missing_elapsed_renders_dashes_not_a_panic() {
    let runs = vec![record("r", RunStatus::Running, 5, None)];
    let t = render_table(&runs);
    assert!(t.contains("running"), "{t}");
    // No division by zero / no NaN in the rate column.
    assert!(!t.contains("NaN") && !t.contains("inf"), "{t}");
}

#[tokio::test]
async fn reads_seeded_in_memory_catalog() {
    use crate::serve::history::RunHistory;
    // Seed a memory backend with two runs, then read them back via list().
    let store =
        crate::serve::history::memory::MemoryHistory::new(std::time::Duration::from_secs(3600));
    store
        .upsert(&record("a", RunStatus::Completed, 10, Some(1.0)))
        .await
        .unwrap();
    store
        .upsert(&record("b", RunStatus::Completed, 20, Some(1.0)))
        .await
        .unwrap();
    let page = store
        .list(&ListFilter {
            name: Some("demo".into()),
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.runs.len(), 2);
    let t = render_table(&page.runs);
    assert!(t.contains("a") && t.contains("b"), "{t}");
}

#[test]
fn select_runs_filters_by_row_and_truncates() {
    let mut a = record("a", RunStatus::Completed, 1, Some(1.0));
    a.invocations[0].row_id = "us".into();
    let mut b = record("b", RunStatus::Completed, 1, Some(1.0));
    b.invocations[0].row_id = "eu".into();
    let all = vec![a.clone(), b.clone()];

    // --row keeps only matching invocations.
    let only_eu = select_runs(all.clone(), Some("eu"), 20);
    assert_eq!(only_eu.len(), 1);
    assert_eq!(only_eu[0].run_id, "b");
    // No filter, but --limit truncates.
    let capped = select_runs(all.clone(), None, 1);
    assert_eq!(capped.len(), 1);
    // A row nobody has → empty.
    assert!(select_runs(all, Some("apac"), 20).is_empty());
}

#[tokio::test]
async fn run_errors_without_a_catalog_block() {
    use crate::cli::HistoryArgs;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("faucet.yaml");
    std::fs::write(
            &path,
            "version: 1\nname: demo\npipeline:\n  source: { type: rest, config: { path: /x } }\n  sink: { type: jsonl, config: { path: o } }\n",
        )
        .unwrap();
    let err = super::run(HistoryArgs {
        config: Some(path),
        env_file: None,
        no_env_file: true,
        profile: None,
        limit: 20,
        row: None,
        label: Vec::new(),
        json: false,
    })
    .await;
    match err {
        Err(CliError::Config(m)) => assert!(m.contains("catalog"), "got: {m}"),
        other => panic!("expected a no-catalog Config error, got {other:?}"),
    }
    let err = super::load(HistoryArgs {
        config: None,
        env_file: None,
        no_env_file: true,
        profile: None,
        limit: 20,
        row: None,
        label: vec![":x".into()],
        json: false,
    })
    .await
    .unwrap_err();
    assert!(err.to_string().contains("empty key"), "{err}");
}
