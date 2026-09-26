//! #736 end to end through the CLI: `faucet migrate --state` rewrites stored
//! bookmarks into the versioned envelope (and migrates an older mongodb-cdc
//! shape), refuses state another source or a newer release wrote, and
//! `faucet state show`, `faucet status` and the `faucet doctor` state probe
//! report the stored format.
#![cfg(all(
    feature = "source-csv",
    feature = "sink-sqlite",
    feature = "source-mongodb-cdc"
))]

use clap::Parser;
use faucet_cli::cli::{Cli, StateLoadArgs, StatusArgs};
use faucet_cli::commands::migrate::{StateKeyMigration, StateMigrationReport, render_state_report};
use faucet_cli::config::{ConnectorSpec, StateStoreSpec};
use faucet_cli::error::CliError;
use faucet_cli::status::Health;
use faucet_core::check::ProbeStatus;
use faucet_core::state_version::{StoredState, wrap_versioned};
use faucet_core::{FileStateStore, StateStore};
use serde_json::{Value, json};
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

/// Row `a` reads csv (bookmark schema 0); row `cdc` reads mongodb-cdc
/// (bookmark schema 1). File state store.
fn write_config(dir: &Path) -> PathBuf {
    std::fs::write(dir.join("in.csv"), "id,name\n1,x\n").unwrap();
    let text = format!(
        r#"version: 1
name: orders
pipeline:
  sources:
    csv: {{ type: csv, config: {{ path: {input} }} }}
    cdc:
      type: mongodb-cdc
      config:
        connection_uri: "mongodb://127.0.0.1:1/?directConnection=true"
        scope: {{ type: collection, database: app, collection: users }}
  sink: {{ type: sqlite, config: {{ database_url: "sqlite://{db}?mode=rwc", table_name: t, column_mapping: auto_map, create_table: true }} }}
  state: {{ type: file, config: {{ path: {state} }} }}
matrix:
  - id: a
    source: {{ ref: csv }}
  - id: cdc
    source: {{ ref: cdc }}
"#,
        input = s(&dir.join("in.csv")),
        db = s(&dir.join("out.db")),
        state = s(&dir.join("state")),
    );
    let path = dir.join("orders.yaml");
    std::fs::write(&path, text).unwrap();
    path
}

fn status_args(cfg: &Path) -> StatusArgs {
    StatusArgs {
        config: Some(cfg.to_path_buf()),
        row: None,
        probe: false,
        load: StateLoadArgs {
            json: true,
            env_file: None,
            no_env_file: true,
            profile: None,
        },
    }
}

#[tokio::test]
async fn migrate_state_envelopes_migrates_and_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = write_config(dir.path());
    let cfgs = s(&cfg);
    let store = FileStateStore::new(dir.path().join("state"));
    let legacy_cdc = json!({ "resume_token": { "_data": "AA" } });
    store.put("orders::a", &json!({ "id": 2 })).await.unwrap();
    store.put("orders::cdc", &legacy_cdc).await.unwrap();

    // `state show` reports what each row holds.
    let load = StateLoadArgs {
        json: true,
        env_file: None,
        no_env_file: true,
        profile: None,
    };
    let (_, target, _) = faucet_cli::commands::state::load(Some(&cfg), &load)
        .await
        .unwrap();
    let stores = faucet_cli::pipeline_state::ops::Stores::build(&target, None)
        .await
        .unwrap();
    let show = faucet_cli::pipeline_state::ops::show(&target, &stores, None, chrono::Utc::now())
        .await
        .unwrap();
    let fmt = |row: &str| {
        show.rows
            .iter()
            .find(|r| r.row == row)
            .and_then(|r| r.state_format.clone())
            .expect("a state format")
    };
    assert_eq!(fmt("a").status, "legacy");
    assert_eq!(fmt("cdc").status, "migrate");
    assert_eq!(fmt("cdc").expected_schema, 1);
    run(&["state", "show", &cfgs]).await.unwrap();

    // --check reports and fails; nothing is written.
    let err = run(&["migrate", "--state", &cfgs, "--check"])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not current"), "{err}");
    assert_eq!(store.get("orders::cdc").await.unwrap(), Some(legacy_cdc));
    let report = faucet_cli::commands::migrate::migrate_state(&target, &stores, None, true)
        .await
        .unwrap();
    assert_eq!(report.pending(), 2);

    // Migrate for real, one row at a time and then everything.
    run(&["migrate", "--state", &cfgs, "--row", "cdc", "--json"])
        .await
        .unwrap();
    let cdc = StoredState::parse(&store.get("orders::cdc").await.unwrap().unwrap());
    assert_eq!(
        (cdc.format, cdc.owner.as_deref(), cdc.schema),
        (1, Some("mongodb-cdc"), 1)
    );
    assert_eq!(cdc.data["invalidate"], json!(false));
    assert_eq!(
        store.get("orders::a").await.unwrap(),
        Some(json!({ "id": 2 })),
        "--row leaves the other row alone"
    );
    run(&["migrate", "--state", &cfgs]).await.unwrap();
    assert_eq!(
        store.get("orders::a").await.unwrap(),
        Some(wrap_versioned("csv", 0, &json!({ "id": 2 })))
    );
    run(&["migrate", "--state", &cfgs, "--check"])
        .await
        .expect("everything is current now");
    let show = faucet_cli::pipeline_state::ops::show(&target, &stores, None, chrono::Utc::now())
        .await
        .unwrap();
    assert!(
        show.rows.iter().all(|r| r
            .state_format
            .as_ref()
            .is_some_and(|f| f.status == "current")),
        "{:?}",
        show.rows
    );

    // State another source wrote is refused and left untouched.
    let foreign = wrap_versioned("mysql-cdc", 0, &json!({ "file": "b.1", "pos": 4 }));
    store.put("orders::a", &foreign).await.unwrap();
    let err = run(&["migrate", "--state", &cfgs]).await.unwrap_err();
    assert!(err.to_string().contains("cannot be read"), "{err}");
    assert_eq!(store.get("orders::a").await.unwrap(), Some(foreign));

    // status marks the row degraded and says why.
    let status = faucet_cli::commands::status::build(&status_args(&cfg))
        .await
        .unwrap();
    let row = status.rows.iter().find(|r| r.row == "a").unwrap();
    assert_eq!(row.health, Health::Degraded, "{:?}", row.reasons);
    assert!(
        row.reasons.iter().any(|r| r.contains("cannot be read")),
        "{:?}",
        row.reasons
    );
    let text = faucet_cli::status::render::render(&status);
    assert!(text.contains("state: incompatible"), "{text}");
    assert!(
        run(&["status", &cfgs]).await.is_err(),
        "a degraded row makes `faucet status` exit non-zero"
    );

    // `state set` writes the envelope for the row's source.
    run(&[
        "state",
        "set",
        &cfgs,
        "--row",
        "a",
        "--bookmark",
        r#"{"id":5}"#,
        "--yes",
    ])
    .await
    .unwrap();
    assert_eq!(
        store.get("orders::a").await.unwrap(),
        Some(wrap_versioned("csv", 0, &json!({ "id": 5 })))
    );
}

#[test]
fn the_state_report_renders_every_action() {
    let key = |action: &'static str, detail: Option<&str>| StateKeyMigration {
        row: "r".into(),
        key: "p::r".into(),
        owner: "mongodb-cdc".into(),
        action,
        from_schema: 0,
        to_schema: 1,
        detail: detail.map(str::to_owned),
    };
    let keys = vec![
        key("current", None),
        key("enveloped", None),
        key("migrated", None),
        key("refused", Some("found 'x' state")),
    ];
    let applied = StateMigrationReport {
        pipeline: "p".into(),
        check: false,
        keys: keys.clone(),
    };
    let text = render_state_report(&applied);
    assert!(text.contains("state migration"));
    assert!(text.contains("current (mongodb-cdc schema 1)"));
    assert!(text.contains("rewritten in the versioned envelope"));
    assert!(text.contains("migrated: mongodb-cdc schema 0 → 1"));
    assert!(text.contains("REFUSED — found 'x' state"));
    assert_eq!(applied.pending(), 3);

    let check = StateMigrationReport {
        pipeline: "p".into(),
        check: true,
        keys,
    };
    let text = render_state_report(&check);
    assert!(text.contains("state check"));
    assert!(text.contains("would be rewritten in the envelope"));
    assert!(text.contains("needs migration: mongodb-cdc schema 0 → 1"));

    let empty = StateMigrationReport {
        pipeline: "p".into(),
        check: false,
        keys: vec![],
    };
    assert!(render_state_report(&empty).contains("no stored bookmarks"));
}

fn spec<T: serde::de::DeserializeOwned>(v: Value) -> T {
    serde_json::from_value(v).unwrap()
}

#[tokio::test]
async fn the_doctor_state_probe_reports_each_format() {
    use faucet_cli::commands::doctor::state_format_probe;
    let dir = tempfile::tempdir().unwrap();
    let state: StateStoreSpec =
        spec(json!({ "type": "file", "config": { "path": s(&dir.path().join("st")) } }));
    let store = FileStateStore::new(dir.path().join("st"));
    let csv: ConnectorSpec = spec(json!({ "type": "csv", "config": { "path": "in.csv" } }));
    let cdc: ConnectorSpec = spec(json!({
        "type": "mongodb-cdc",
        "config": { "connection_uri": "mongodb://127.0.0.1:1", "scope": { "type": "collection", "database": "a", "collection": "b" } }
    }));

    // Nothing stored, or a memory store: no probe.
    assert!(
        state_format_probe(&csv, Some(&state), "p::r")
            .await
            .is_none()
    );
    let memory: StateStoreSpec = spec(json!({ "type": "memory" }));
    store.put("p::r", &json!({ "id": 1 })).await.unwrap();
    assert!(
        state_format_probe(&csv, Some(&memory), "p::r")
            .await
            .is_none()
    );
    assert!(state_format_probe(&csv, None, "p::r").await.is_none());

    let probe = state_format_probe(&csv, Some(&state), "p::r")
        .await
        .unwrap();
    assert_eq!((probe.role, probe.name), ("state", "format"));
    assert!(matches!(probe.status, ProbeStatus::Pass), "{probe:?}");
    assert!(probe.hint.unwrap().contains("before versioning"));

    store
        .put("p::r", &wrap_versioned("csv", 0, &json!({ "id": 1 })))
        .await
        .unwrap();
    let probe = state_format_probe(&csv, Some(&state), "p::r")
        .await
        .unwrap();
    assert!(matches!(probe.status, ProbeStatus::Pass));
    assert!(probe.hint.is_none());

    store
        .put("p::r", &json!({ "resume_token": { "_data": "AA" } }))
        .await
        .unwrap();
    let probe = state_format_probe(&cdc, Some(&state), "p::r")
        .await
        .unwrap();
    assert!(
        matches!(&probe.status, ProbeStatus::Skip { reason } if reason.contains("0 → 1")),
        "{probe:?}"
    );

    store
        .put("p::r", &wrap_versioned("kafka", 0, &json!({})))
        .await
        .unwrap();
    let probe = state_format_probe(&csv, Some(&state), "p::r")
        .await
        .unwrap();
    assert!(
        matches!(&probe.status, ProbeStatus::Fail { reason } if reason.contains("kafka")),
        "{probe:?}"
    );

    // An unreadable store is a failed probe, not a panic.
    std::fs::write(dir.path().join("not-a-dir"), "x").unwrap();
    let broken: StateStoreSpec =
        spec(json!({ "type": "file", "config": { "path": s(&dir.path().join("not-a-dir")) } }));
    let probe = state_format_probe(&csv, Some(&broken), "p::r")
        .await
        .unwrap();
    assert!(
        matches!(probe.status, ProbeStatus::Fail { .. }),
        "{probe:?}"
    );
}
