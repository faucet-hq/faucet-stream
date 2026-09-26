//! `faucet status` (#732) end to end: real runs against a file state store, a
//! jsonl DLQ and a SQLite sink produce the health screen and its exit codes;
//! a topology config reports per sink node; `--probe` catches a state-store
//! vs sink-watermark disagreement and says which side the next run trusts.
#![cfg(all(
    feature = "source-csv",
    feature = "sink-sqlite",
    feature = "sink-jsonl"
))]

use clap::Parser;
use faucet_cli::cli::{Cli, StateLoadArgs, StatusArgs};
use faucet_cli::error::CliError;
use faucet_cli::status::{Agreement, Health};
use faucet_core::idempotency::{format_token_with_bookmark, wrap_state};
use faucet_core::{FileStateStore, StateStore};
use serde_json::json;
use std::path::{Path, PathBuf};

async fn run(args: &[&str]) -> Result<(), CliError> {
    let mut argv = vec!["faucet"];
    argv.extend_from_slice(args);
    let cli = Cli::try_parse_from(argv).expect("argv parses");
    Box::pin(faucet_cli::run_command(cli)).await
}

fn s(p: &Path) -> String {
    p.display().to_string()
}

fn args(cfg: &Path) -> StatusArgs {
    StatusArgs {
        config: Some(cfg.to_path_buf()),
        row: None,
        probe: false,
        load: StateLoadArgs {
            json: false,
            env_file: None,
            no_env_file: true,
            profile: None,
        },
    }
}

/// csv → sqlite (row `good`) and csv → an unwritable sqlite path (row `bad`),
/// with a file state store and a jsonl DLQ.
fn write_config(dir: &Path) -> PathBuf {
    std::fs::write(dir.join("in.csv"), "id,name\n1,x\n2,y\n").unwrap();
    let text = format!(
        r#"version: 1
name: shop
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink: {{ type: sqlite, config: {{ database_url: "sqlite://{db}?mode=rwc", table_name: t, column_mapping: auto_map, create_table: true }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
  dlq:
    sink: {{ type: jsonl, config: {{ path: {dlq} }} }}
sla:
  max_staleness_secs: 86400
matrix:
  - id: good
  - id: bad
    sink: {{ config: {{ database_url: "sqlite://{missing}/nope.db" }} }}
"#,
        input = s(&dir.join("in.csv")),
        db = s(&dir.join("out.db")),
        state = s(&dir.join("state")),
        dlq = s(&dir.join("dlq.jsonl")),
        missing = s(&dir.join("no-such-dir")),
    );
    let path = dir.join("shop.yaml");
    std::fs::write(&path, text).unwrap();
    path
}

#[tokio::test]
async fn status_reads_real_runs_dlq_and_exit_codes() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path());
    let cfgs = s(&cfg);

    // Before any run: nothing has succeeded yet.
    let r = faucet_cli::commands::status::build(&args(&cfg))
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 2);
    assert!(r.rows.iter().all(|x| x.health == Health::Warming), "{r:?}");
    run(&["status", &cfgs]).await.unwrap();

    let text = std::fs::read_to_string(&cfg).unwrap();
    let summary = faucet_cli::run_from_yaml_str(&text).await.unwrap();
    assert!(summary.had_failures(), "row `bad` cannot open its database");

    let r = faucet_cli::commands::status::build(&args(&cfg))
        .await
        .unwrap();
    let good = r.rows.iter().find(|x| x.row == "good").unwrap();
    assert_eq!(good.health, Health::Ok, "{:?}", good.reasons);
    assert_eq!(good.last_success.as_ref().unwrap().records, Some(2));
    assert_eq!(good.last_success.as_ref().unwrap().source, "state");
    assert!(good.running.is_none(), "the run released its lease");
    let bad = r.rows.iter().find(|x| x.row == "bad").unwrap();
    assert_eq!(bad.health, Health::Failed);
    assert!(bad.last_failure.as_ref().unwrap().error.is_some());
    assert_eq!(r.exit_code, 2);
    let err = run(&["status", &cfgs]).await.unwrap_err();
    assert!(
        matches!(err, CliError::StatusUnhealthy { code: 2, .. }),
        "{err}"
    );
    let err = run(&["status", &cfgs, "--json"]).await.unwrap_err();
    assert!(
        matches!(err, CliError::StatusUnhealthy { code: 2, .. }),
        "{err}"
    );

    // Only the healthy row, plus a DLQ backlog → degraded (exit 1).
    std::fs::write(
        dir.path().join("dlq.jsonl"),
        json!({"payload": {"id": 1}, "pipeline": "shop", "row": "good", "ts_ms": 1_700_000_000_000i64,
               "error": {"kind": "Sink", "message": "rejected"}})
        .to_string(),
    )
    .unwrap();
    let mut one = args(&cfg);
    one.row = Some("good".into());
    let r = faucet_cli::commands::status::build(&one).await.unwrap();
    assert_eq!(r.rows[0].dlq.count, 1);
    assert_eq!(r.rows[0].health, Health::Degraded);
    let err = run(&["status", &cfgs, "--row", "good"]).await.unwrap_err();
    assert!(
        matches!(err, CliError::StatusUnhealthy { code: 1, .. }),
        "{err}"
    );
    std::fs::remove_file(dir.path().join("dlq.jsonl")).unwrap();
    run(&["status", &cfgs, "--row", "good"]).await.unwrap();
}

#[tokio::test]
async fn topology_configs_report_per_sink_node() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("in.csv"), "id\n1\n2\n3\n").unwrap();
    let text = format!(
        r#"version: 1
name: fan
pipeline:
  sources: {{ src: {{ type: csv, config: {{ path: {input} }} }} }}
  sinks:
    a: {{ type: jsonl, config: {{ path: {a} }} }}
    b: {{ type: jsonl, config: {{ path: {b} }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
  nodes:
    read: {{ kind: source, ref: src }}
    fan: {{ kind: tee, fanout: 2 }}
    left: {{ kind: sink, ref: a }}
    right: {{ kind: sink, ref: b }}
  edges:
    - {{ from: read, to: fan }}
    - {{ from: fan, to: left }}
    - {{ from: fan, to: right }}
"#,
        input = s(&dir.path().join("in.csv")),
        a = s(&dir.path().join("a.jsonl")),
        b = s(&dir.path().join("b.jsonl")),
        state = s(&dir.path().join("state")),
    );
    let cfg = dir.path().join("fan.yaml");
    std::fs::write(&cfg, &text).unwrap();
    run(&["run", &s(&cfg), "--quiet"]).await.unwrap();
    let r = faucet_cli::commands::status::build(&args(&cfg))
        .await
        .unwrap();
    assert!(r.topology);
    let ids: Vec<_> = r.rows.iter().map(|x| x.row.as_str()).collect();
    assert_eq!(ids, vec!["left", "right"]);
    assert!(r.rows.iter().all(|x| x.health == Health::Ok), "{r:?}");
    assert_eq!(r.rows[0].last_success.as_ref().unwrap().records, Some(3));
    run(&["status", &s(&cfg)]).await.unwrap();
    run(&["state", "show", &s(&cfg)]).await.unwrap();
}

#[tokio::test]
async fn probe_detects_a_watermark_disagreement() {
    let dir = tempfile::tempdir().unwrap();
    let sink_cfg = json!({
        "database_url": format!("sqlite://{}?mode=rwc", s(&dir.path().join("eo.db"))),
        "table_name": "t",
        "column_mapping": "auto_map",
        "create_table": true,
    });
    let text = format!(
        r#"version: 1
name: cdc
delivery: exactly_once
pipeline:
  source: {{ type: postgres-cdc, config: {{ connection_url: "postgres://u:p@localhost/db", slot_name: s, publication: p }} }}
  sink: {{ type: sqlite, config: {sink_cfg} }}
  state: {{ type: file, config: {{ path: {state} }} }}
"#,
        state = s(&dir.path().join("state")),
    );
    let cfg = dir.path().join("cdc.yaml");
    std::fs::write(&cfg, &text).unwrap();
    let store = FileStateStore::new(dir.path().join("state"));
    store
        .put("cdc::row-0", &wrap_state(Some(&json!({"lsn": "0/3"})), 3))
        .await
        .unwrap();
    let sink = faucet_cli::registry::build_sink("sqlite", sink_cfg.clone(), &Default::default())
        .await
        .unwrap();
    sink.write_batch_idempotent(
        &[json!({"id": 1})],
        "cdc::row-0",
        &format_token_with_bookmark(7, Some(&json!({"lsn": "0/7"}))),
    )
    .await
    .unwrap();

    let mut a = args(&cfg);
    let r = faucet_cli::commands::status::build(&a).await.unwrap();
    let eo = r.rows[0].exactly_once.as_ref().unwrap();
    assert_eq!(eo.agreement, Agreement::NotProbed);
    assert_eq!(r.rows[0].resume, "lsn=0/3");

    a.probe = true;
    let r = faucet_cli::commands::status::build(&a).await.unwrap();
    let eo = r.rows[0].exactly_once.as_ref().unwrap();
    assert_eq!(eo.agreement, Agreement::SinkAhead);
    assert_eq!(eo.trusted, "sink");
    assert_eq!(eo.sink.as_ref().unwrap().seq, Some(7));
    assert_eq!(r.rows[0].resume_bookmark, Some(json!({"lsn": "0/7"})));
    assert!(r.rows[0].resume.contains("wins"), "{}", r.rows[0].resume);
    run(&["status", &s(&cfg), "--probe"]).await.unwrap();

    // The state store ahead of the sink means committed pages went missing.
    store
        .put("cdc::row-0", &wrap_state(Some(&json!({"lsn": "0/9"})), 9))
        .await
        .unwrap();
    let r = faucet_cli::commands::status::build(&a).await.unwrap();
    let row = &r.rows[0];
    assert_eq!(
        row.exactly_once.as_ref().unwrap().agreement,
        Agreement::StateAhead
    );
    assert_eq!(row.health, Health::Degraded, "{:?}", row.reasons);
    // …and agreeing ones say so.
    store
        .put("cdc::row-0", &wrap_state(Some(&json!({"lsn": "0/7"})), 7))
        .await
        .unwrap();
    let r = faucet_cli::commands::status::build(&a).await.unwrap();
    assert_eq!(
        r.rows[0].exactly_once.as_ref().unwrap().agreement,
        Agreement::Agree
    );
    assert!(r.rows[0].resume.contains("agrees"));
}

#[cfg(feature = "catalog")]
#[tokio::test]
async fn status_folds_in_the_catalog_run_history() {
    use faucet_cli::serve::history::{InvocationRecord, RunRecord, RunStatus};
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("in.csv"), "id\n1\n").unwrap();
    let catalog_url = format!("sqlite:{}", s(&dir.path().join("cat.db")));
    let text = format!(
        r#"version: 1
name: hist
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink: {{ type: jsonl, config: {{ path: {out} }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
catalog: {{ url: "{catalog_url}" }}
"#,
        input = s(&dir.path().join("in.csv")),
        out = s(&dir.path().join("out.jsonl")),
        state = s(&dir.path().join("state")),
    );
    let cfg = dir.path().join("hist.yaml");
    std::fs::write(&cfg, &text).unwrap();
    let spec = faucet_cli::config::PipelineConfig::from_path(&cfg, None)
        .unwrap()
        .catalog
        .unwrap();
    let handle = faucet_cli::catalog::connect_from_spec(&spec).await.unwrap();
    let mut rec = RunRecord::queued(
        "srv-run".into(),
        Some("hist".into()),
        Default::default(),
        None,
        chrono::Utc::now(),
    );
    rec.status = RunStatus::Failed;
    rec.finished_at = Some(chrono::Utc::now());
    rec.invocations = vec![InvocationRecord {
        row_id: "row-0".into(),
        parent_record_key: None,
        run_id: Some("inv-1".into()),
        records_written: 0,
        duration_ms: 3,
        error: Some("server-side failure".into()),
        usage: None,
    }];
    handle.store.upsert(&rec).await.unwrap();
    let r = faucet_cli::commands::status::build(&args(&cfg))
        .await
        .unwrap();
    let row = &r.rows[0];
    assert_eq!(row.health, Health::Failed);
    assert_eq!(row.last_failure.as_ref().unwrap().source, "history");
    assert_eq!(
        row.last_failure.as_ref().unwrap().run_id.as_deref(),
        Some("inv-1")
    );

    // An unreachable catalog is a note, never a failure of the command.
    let broken = text.replace(&catalog_url, "bogus-scheme:x");
    std::fs::write(&cfg, broken).unwrap();
    let r = faucet_cli::commands::status::build(&args(&cfg))
        .await
        .unwrap();
    assert!(
        r.notes.iter().any(|n| n.contains("run history")),
        "{:?}",
        r.notes
    );
}
