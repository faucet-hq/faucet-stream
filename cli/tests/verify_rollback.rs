//! End-to-end content verification (#701) and run rollback (#706) through the
//! real CLI path: config → `expand` → `run_expanded` → `verify` / `rollback`.
//!
//! SQLite on both sides, so no Docker. The sink-level SQL is covered by
//! `faucet-sink-sqlite`'s own tests and the pure diff/journal logic by
//! `faucet-core`'s; what these prove is the wiring in between — the executor
//! injects the rollback spec and marker, a run's rows come back out again, the
//! verifier finds an injected drift and repairs exactly it, and the load-time
//! gates fire.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, RunSummary, run_expanded};
use faucet_cli::expand::expand;
use faucet_cli::rollback::{RollbackInputs, RollbackSpec};
use faucet_cli::verify::{VerifyInputs, VerifySpec};
use sqlx::Row;
use std::path::Path;

fn opts(name: &str, cfg: &PipelineConfig) -> ExecuteOptions {
    ExecuteOptions {
        pipeline_name: name.into(),
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
    }
}

async fn exec(db: &str, sql: &str) {
    let pool = sqlx::SqlitePool::connect(db).await.unwrap();
    sqlx::query(sql).execute(&pool).await.unwrap();
    pool.close().await;
}

/// `(id, name)` rows of `dst`, by id.
async fn rows(db: &str) -> Vec<(i64, String)> {
    let pool = sqlx::SqlitePool::connect(db).await.unwrap();
    let out = sqlx::query("SELECT id, name FROM dst ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<i64, _>("id"), r.get::<String, _>("name")))
        .collect();
    pool.close().await;
    out
}

async fn count(db: &str, sql: &str) -> i64 {
    let pool = sqlx::SqlitePool::connect(db).await.unwrap();
    let n: i64 = sqlx::query_scalar(sql).fetch_one(&pool).await.unwrap();
    pool.close().await;
    n
}

/// A sqlite → sqlite upsert pipeline with a `file` state store, plus the
/// given extra top-level blocks.
fn config_yaml(src: &str, dst: &str, state_dir: &Path, write_mode: &str, extra: &str) -> String {
    let key = if write_mode == "append" {
        ""
    } else {
        "\n      key: [id]"
    };
    format!(
        r#"
version: 1
name: mirror
pipeline:
  source:
    type: sqlite
    config:
      database_url: "{src}"
      query: "SELECT id, name FROM src ORDER BY id"
  sink:
    type: sqlite
    config:
      database_url: "{dst}"
      table_name: dst
      column_mapping: auto_map
      write_mode: {write_mode}{key}
  state:
    type: file
    config:
      path: "{}"
{extra}
"#,
        state_dir.display()
    )
}

fn load(yaml: &str) -> PipelineConfig {
    PipelineConfig::from_text(yaml, Path::new("mirror.yaml")).unwrap()
}

async fn run(cfg: &PipelineConfig) -> RunSummary {
    run_expanded(expand(cfg).unwrap(), opts("mirror", cfg))
        .await
        .unwrap()
}

struct Dbs {
    _dir: tempfile::TempDir,
    src: String,
    dst: String,
    state: std::path::PathBuf,
}

async fn fresh() -> Dbs {
    let dir = tempfile::tempdir().unwrap();
    let src = format!("sqlite://{}?mode=rwc", dir.path().join("src.db").display());
    let dst = format!("sqlite://{}?mode=rwc", dir.path().join("dst.db").display());
    exec(&src, "CREATE TABLE src (id INTEGER PRIMARY KEY, name TEXT)").await;
    exec(
        &src,
        "INSERT INTO src VALUES (1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')",
    )
    .await;
    exec(
        &dst,
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, name TEXT, _faucet_run_id TEXT)",
    )
    .await;
    let state = dir.path().join("state");
    Dbs {
        _dir: dir,
        src,
        dst,
        state,
    }
}

// ───────────────────────────── verify ─────────────────────────────

#[tokio::test]
async fn verify_finds_and_repairs_injected_drift() {
    let d = fresh().await;
    let cfg = load(&config_yaml(&d.src, &d.dst, &d.state, "upsert", ""));
    let summary = run(&cfg).await;
    assert!(!summary.had_failures(), "{summary:?}");
    assert_eq!(rows(&d.dst).await.len(), 4);

    let inputs = || VerifyInputs {
        row: None,
        repair: false,
        allow_delete: false,
        dry_run: false,
        pipeline_name: "mirror".into(),
        execution: None,
        auth: Default::default(),
        clock: chrono::Utc::now().fixed_offset(),
    };
    // A faithful mirror verifies equal, in range mode, matching only digests.
    let ok = faucet_cli::verify::verify(&cfg, &VerifySpec::default(), inputs())
        .await
        .unwrap();
    assert!(ok.report.equal(), "{ok:?}");
    assert_eq!(ok.strategy, "range");
    assert_eq!(ok.key, vec!["id".to_string()]);

    // Inject drift downstream: a changed value, a deleted row, an extra row.
    exec(&d.dst, "UPDATE dst SET name = 'TWO' WHERE id = 2").await;
    exec(&d.dst, "DELETE FROM dst WHERE id = 3").await;
    exec(&d.dst, "INSERT INTO dst (id, name) VALUES (9, 'ghost')").await;

    let bad = faucet_cli::verify::verify(&cfg, &VerifySpec::default(), inputs())
        .await
        .unwrap();
    let (missing, extra, changed, dup) = bad.report.tally();
    assert_eq!((missing, extra, changed, dup), (1, 1, 1, 0), "{bad:?}");
    let changed_key = bad
        .report
        .differences
        .iter()
        .find(|df| matches!(df.kind, faucet_core::diff::DifferenceKind::Changed { .. }))
        .unwrap();
    assert_eq!(changed_key.key["id"], 2);
    assert!(bad.report.ranges_differing >= 1);

    // Repair without delete: the changed and missing keys are re-synced; the
    // ghost row stays.
    let repaired = faucet_cli::verify::verify(
        &cfg,
        &VerifySpec::default(),
        VerifyInputs {
            repair: true,
            ..inputs()
        },
    )
    .await
    .unwrap();
    assert_eq!(repaired.report.repaired_upserts, Some(2), "{repaired:?}");
    assert_eq!(repaired.report.repaired_deletes, Some(0));
    assert_eq!(
        rows(&d.dst).await,
        vec![
            (1, "one".into()),
            (2, "two".into()),
            (3, "three".into()),
            (4, "four".into()),
            (9, "ghost".into())
        ]
    );
    // Now with --allow-delete the ghost goes too, and a re-verify is clean.
    let repaired = faucet_cli::verify::verify(
        &cfg,
        &VerifySpec::default(),
        VerifyInputs {
            repair: true,
            allow_delete: true,
            ..inputs()
        },
    )
    .await
    .unwrap();
    assert_eq!(repaired.report.repaired_deletes, Some(1), "{repaired:?}");
    let clean = faucet_cli::verify::verify(&cfg, &VerifySpec::default(), inputs())
        .await
        .unwrap();
    assert!(clean.report.equal(), "{clean:?}");
}

#[tokio::test]
async fn verify_post_run_fails_a_drifted_run_and_can_be_downgraded() {
    let d = fresh().await;
    // First a clean run so the destination exists, then drift it.
    let base = load(&config_yaml(&d.src, &d.dst, &d.state, "upsert", ""));
    assert!(!run(&base).await.had_failures());
    exec(&d.dst, "INSERT INTO dst (id, name) VALUES (9, 'ghost')").await;

    let strict = load(&config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "upsert",
        "verify: {}",
    ));
    let summary = run(&strict).await;
    let err = summary.invocations[0].error.clone().expect("run must fail");
    assert!(
        err.contains("content verification found 1 differing key"),
        "{err}"
    );
    assert!(err.contains("1 extra in destination"), "{err}");

    let lenient = load(&config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "upsert",
        "verify:\n  fail_on_difference: false",
    ));
    assert!(!run(&lenient).await.had_failures());

    // `repair: true` + `allow_delete: true` heals it inside the run itself —
    // and a fully repaired drift keeps the run green even when differences
    // would otherwise fail it.
    let healing = load(&config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "upsert",
        "verify:\n  repair: true\n  allow_delete: true",
    ));
    let summary = run(&healing).await;
    assert!(!summary.had_failures(), "{summary:?}");
    assert_eq!(rows(&d.dst).await.len(), 4, "the ghost row was deleted");
    // Without `allow_delete` the ghost stays and the run fails: the repair
    // could not cover every difference.
    exec(&d.dst, "INSERT INTO dst (id, name) VALUES (9, 'ghost')").await;
    let partial = load(&config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "upsert",
        "verify:\n  repair: true",
    ));
    let err = run(&partial).await.invocations[0]
        .error
        .clone()
        .expect("must fail");
    assert!(err.contains("repaired: 0 upsert(s), 0 delete(s)"), "{err}");
}

#[tokio::test]
async fn verify_refuses_a_keyless_row_and_validates_its_block() {
    let d = fresh().await;
    let cfg = load(&config_yaml(&d.src, &d.dst, &d.state, "append", ""));
    let err = faucet_cli::verify::verify(
        &cfg,
        &VerifySpec::default(),
        VerifyInputs {
            row: None,
            repair: false,
            allow_delete: false,
            dry_run: false,
            pipeline_name: "mirror".into(),
            execution: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
        },
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("no key"), "{err}");

    // An explicit key over an append sink works (the key is only for matching).
    let keyed = VerifySpec {
        key: vec!["id".into()],
        ..VerifySpec::default()
    };
    exec(&d.dst, "INSERT INTO dst (id, name) VALUES (1, 'one')").await;
    let out = faucet_cli::verify::verify(
        &cfg,
        &keyed,
        VerifyInputs {
            row: None,
            repair: false,
            allow_delete: false,
            dry_run: false,
            pipeline_name: "mirror".into(),
            execution: None,
            auth: Default::default(),
            clock: chrono::Utc::now().fixed_offset(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        out.report.tally().0,
        3,
        "three source rows missing downstream"
    );

    let bad = config_yaml(&d.src, &d.dst, &d.state, "append", "verify:\n  ranges: 0");
    let err = expand(&load(&bad)).unwrap_err();
    assert!(err.to_string().contains("ranges"), "{err}");
}

// ───────────────────────────── rollback ─────────────────────────────

#[tokio::test]
async fn upsert_run_is_undone_and_the_bookmark_rewound() {
    let d = fresh().await;
    // Seed a prior state of the destination the run will change.
    exec(
        &d.dst,
        "INSERT INTO dst VALUES (1, 'old-one', 'r0'), (2, 'old-two', 'r0')",
    )
    .await;
    let cfg = load(&config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "upsert",
        "rollback: {}",
    ));
    let summary = run(&cfg).await;
    assert!(!summary.had_failures(), "{summary:?}");
    let run_id = summary.invocations[0].run_id.clone().expect("run id");
    assert_eq!(rows(&d.dst).await.len(), 4);
    assert_eq!(
        count(
            &d.dst,
            "SELECT count(*) FROM dst WHERE _faucet_run_id IS NOT NULL"
        )
        .await,
        4,
        "the run-id column was stamped without a metadata_columns block"
    );
    assert_eq!(
        count(&d.dst, "SELECT count(*) FROM _faucet_run_journal").await,
        4
    );

    let listed = faucet_cli::rollback::list(&cfg, "mirror", None)
        .await
        .unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].run_id, run_id);
    assert_eq!(listed[0].mode, faucet_core::rollback::RollbackMode::Upsert);

    let inputs = |dry_run: bool, force: bool| RollbackInputs {
        run_id: run_id.clone(),
        row: None,
        dry_run,
        force,
        pipeline_name: "mirror".into(),
        auth: Default::default(),
    };
    let dry = faucet_cli::rollback::rollback(&cfg, inputs(true, false))
        .await
        .unwrap();
    assert!(!dry.outcome.applied && dry.dry_run);
    assert_eq!((dry.outcome.deleted, dry.outcome.restored), (2, 2));
    assert_eq!(rows(&d.dst).await.len(), 4, "dry run changed nothing");

    let report = faucet_cli::rollback::rollback(&cfg, inputs(false, false))
        .await
        .unwrap();
    assert!(report.outcome.applied, "{report:?}");
    assert!(report.bookmark_rewound && !report.token_rewound);
    assert_eq!(
        rows(&d.dst).await,
        vec![(1, "old-one".into()), (2, "old-two".into())]
    );
    assert_eq!(
        count(&d.dst, "SELECT count(*) FROM _faucet_run_journal").await,
        0
    );
    assert!(
        faucet_cli::rollback::list(&cfg, "mirror", None)
            .await
            .unwrap()
            .is_empty()
    );
    let again = faucet_cli::rollback::rollback(&cfg, inputs(false, false))
        .await
        .unwrap_err();
    assert!(again.to_string().contains("no undoable run"), "{again}");
}

#[tokio::test]
async fn rollback_is_blocked_by_a_later_run_unless_forced() {
    let d = fresh().await;
    let cfg = load(&config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "upsert",
        "rollback: {}",
    ));
    let first = run(&cfg).await;
    let first_id = first.invocations[0].run_id.clone().unwrap();
    // A second run changes key 2 (and re-stamps every key it touches).
    exec(&d.src, "UPDATE src SET name = 'deux' WHERE id = 2").await;
    let second = run(&cfg).await;
    assert!(!second.had_failures());

    let report = faucet_cli::rollback::rollback(
        &cfg,
        RollbackInputs {
            run_id: first_id.clone(),
            row: Some("row-0".into()),
            dry_run: false,
            force: false,
            pipeline_name: "mirror".into(),
            auth: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(report.blocked(), "{report:?}");
    assert_eq!(
        report.outcome.conflicts, 4,
        "every key was re-upserted by run 2"
    );
    assert_eq!(rows(&d.dst).await.len(), 4, "blocked: untouched");

    let forced = faucet_cli::rollback::rollback(
        &cfg,
        RollbackInputs {
            run_id: first_id,
            row: None,
            dry_run: false,
            force: true,
            pipeline_name: "mirror".into(),
            auth: Default::default(),
        },
    )
    .await
    .unwrap();
    assert!(forced.outcome.applied);
    // The first run created every key (the table was empty) → all deleted.
    assert_eq!(forced.outcome.deleted, 4);
    assert!(rows(&d.dst).await.is_empty());
}

#[tokio::test]
async fn append_and_overwrite_runs_are_undone() {
    let d = fresh().await;
    // An append destination has no primary key: the same ids land twice.
    exec(&d.dst, "DROP TABLE dst").await;
    exec(
        &d.dst,
        "CREATE TABLE dst (id INTEGER, name TEXT, _faucet_run_id TEXT)",
    )
    .await;
    let append = load(&config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "append",
        "rollback: {}",
    ));
    let s1 = run(&append).await;
    let id1 = s1.invocations[0].run_id.clone().unwrap();
    let s2 = run(&append).await;
    assert!(!s2.had_failures());
    assert_eq!(rows(&d.dst).await.len(), 8, "append twice");
    let report = faucet_cli::rollback::rollback(
        &append,
        RollbackInputs {
            run_id: id1,
            row: None,
            dry_run: false,
            force: false,
            pipeline_name: "mirror".into(),
            auth: Default::default(),
        },
    )
    .await
    .unwrap();
    assert_eq!((report.outcome.deleted, report.outcome.applied), (4, true));
    assert_eq!(
        rows(&d.dst).await.len(),
        4,
        "only the first run's rows went"
    );

    // Overwrite: the replaced table is kept and swapped back.
    let overwrite = load(&config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "overwrite",
        "rollback: {}",
    ));
    exec(&d.src, "DELETE FROM src WHERE id > 1").await;
    let s3 = run(&overwrite).await;
    assert!(!s3.had_failures(), "{s3:?}");
    let id3 = s3.invocations[0].run_id.clone().unwrap();
    assert_eq!(rows(&d.dst).await.len(), 1);
    let report = faucet_cli::rollback::rollback(
        &overwrite,
        RollbackInputs {
            run_id: id3,
            row: None,
            dry_run: false,
            force: false,
            pipeline_name: "mirror".into(),
            auth: Default::default(),
        },
    )
    .await
    .unwrap();
    assert_eq!(report.mode, faucet_core::rollback::RollbackMode::Overwrite);
    assert!(report.outcome.applied, "{report:?}");
    assert_eq!(
        rows(&d.dst).await.len(),
        4,
        "the four appended rows are back"
    );
    assert_eq!(
        count(
            &d.dst,
            "SELECT count(*) FROM sqlite_master WHERE name = 'dst__faucet_prev'"
        )
        .await,
        0
    );
}

#[tokio::test]
async fn rollback_gates_fire_at_load_time() {
    let d = fresh().await;
    // No state block.
    let yaml = config_yaml(&d.src, &d.dst, &d.state, "upsert", "rollback: {}").replace(
        "  state:\n    type: file\n",
        "  state_disabled:\n    type: file\n",
    );
    let no_state = format!(
        r#"
version: 1
name: mirror
pipeline:
  source: {{ type: sqlite, config: {{ database_url: "{}", query: "SELECT 1 AS id" }} }}
  sink: {{ type: sqlite, config: {{ database_url: "{}", table_name: dst, column_mapping: auto_map }} }}
rollback: {{}}
"#,
        d.src, d.dst
    );
    let _ = yaml;
    let err = expand(&load(&no_state)).unwrap_err();
    assert!(err.to_string().contains("needs a `state:` block"), "{err}");

    let memory = no_state.replace("rollback: {}", "  state: { type: memory }\nrollback: {}");
    let err = expand(&load(&memory)).unwrap_err();
    assert!(err.to_string().contains("`memory` state store"), "{err}");

    let jsonl = format!(
        r#"
version: 1
name: mirror
pipeline:
  source: {{ type: sqlite, config: {{ database_url: "{}", query: "SELECT 1 AS id" }} }}
  sink: {{ type: jsonl, config: {{ path: "{}" }} }}
  state: {{ type: file, config: {{ path: "{}" }} }}
rollback: {{}}
"#,
        d.src,
        d.state.join("out.jsonl").display(),
        d.state.display()
    );
    let err = expand(&load(&jsonl)).unwrap_err();
    assert!(err.to_string().contains("cannot undo a run"), "{err}");

    let disabled_meta = config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "upsert",
        "rollback: {}\nmetadata_columns:\n  enabled: false",
    );
    let err = expand(&load(&disabled_meta)).unwrap_err();
    assert!(err.to_string().contains("run_id"), "{err}");

    let retain0 = config_yaml(&d.src, &d.dst, &d.state, "upsert", "rollback:\n  retain: 0");
    let err = expand(&load(&retain0)).unwrap_err();
    assert!(err.to_string().contains("retain"), "{err}");

    // `enabled: false` switches everything off, gates included.
    let off = config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "upsert",
        "rollback:\n  enabled: false",
    );
    let nodes = expand(&load(&off)).unwrap();
    assert!(nodes[0].metadata_columns.is_none());
    let on = expand(&load(&config_yaml(
        &d.src,
        &d.dst,
        &d.state,
        "upsert",
        "rollback: {}",
    )))
    .unwrap();
    assert_eq!(
        on[0].metadata_columns.as_ref().unwrap().columns,
        vec![faucet_core::MetadataColumn::RunId]
    );
    let _ = RollbackSpec::default();
}
