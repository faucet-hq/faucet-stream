//! End-to-end scoped cleanup (#478) through the real CLI path:
//! config → `expand` → `run_expanded` → sink `cleanup_scope`.
//!
//! Uses SQLite so it needs no Docker. The sink-level SQL is covered by
//! `faucet-sink-sqlite`'s own tests and the timing by `faucet-core`'s; what these
//! cover is the wiring in between — that the claim reaches the sink at all, that
//! the guards suppress it, and that the load-time gates fire.

use faucet_cli::config::PipelineConfig;
use faucet_cli::executor::{ExecuteOptions, run_expanded};
use faucet_cli::expand::expand;
use sqlx::Row;

/// Seed a table with three rows for contact 7 and two for contact 8, so a test
/// can prove the cleanup is *scoped* — contact 8's rows must survive untouched.
async fn seed(db: &str) {
    let pool = sqlx::SqlitePool::connect(db).await.unwrap();
    sqlx::query("CREATE TABLE assoc (id INTEGER PRIMARY KEY, contact_id INTEGER, label TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    for (id, contact, label) in [
        (1, 7, "stale-a"),
        (2, 7, "stale-b"),
        (3, 7, "keep-me"),
        (10, 8, "other-contact"),
        (11, 8, "other-contact-2"),
    ] {
        sqlx::query("INSERT INTO assoc (id, contact_id, label) VALUES (?, ?, ?)")
            .bind(id)
            .bind(contact)
            .bind(label)
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;
}

async fn rows(db: &str) -> Vec<(i64, i64)> {
    let pool = sqlx::SqlitePool::connect(db).await.unwrap();
    let out = sqlx::query("SELECT id, contact_id FROM assoc ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap()
        .iter()
        .map(|r| (r.get::<i64, _>("id"), r.get::<i64, _>("contact_id")))
        .collect();
    pool.close().await;
    out
}

/// A pipeline that re-fetches contact 7's associations from `csv` and mirrors
/// them into the seeded table, claiming completeness for `contact_id = 7`.
fn config_yaml(csv: &std::path::Path, db: &str, cleanup: bool) -> String {
    format!(
        r#"
version: 1
name: assoc_mirror
pipeline:
  source:
    type: csv
    config:
      path: "{csv}"
    complete_for:
      scope:
        contact_id: 7
      on_missing: {on_missing}
  sink:
    type: sqlite
    config:
      database_url: "{db}"
      table_name: assoc
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
"#,
        csv = csv.display(),
        db = db,
        on_missing = if cleanup { "delete" } else { "ignore" }
    )
}

fn opts(name: &str) -> ExecuteOptions {
    ExecuteOptions {
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

async fn run(cfg_yaml: &str, dir: &std::path::Path, o: ExecuteOptions) -> usize {
    let path = dir.join("p.yaml");
    std::fs::write(&path, cfg_yaml).unwrap();
    let cfg = PipelineConfig::from_text(cfg_yaml, &path).expect("config parses");
    let nodes = expand(&cfg).expect("expand");
    let summary = run_expanded(nodes, o).await.expect("run");
    let errs: Vec<String> = summary
        .invocations
        .iter()
        .filter_map(|i| i.error.clone())
        .collect();
    assert!(!summary.had_failures(), "pipeline should succeed: {errs:?}");
    summary.invocations.len()
}

#[tokio::test]
async fn deletes_only_stale_rows_inside_the_claimed_scope() {
    let dir = tempfile::tempdir().unwrap();
    let db = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("d.sqlite").display()
    );
    seed(&db).await;

    // The source now reports only id=3 for contact 7 — ids 1 and 2 were deleted
    // upstream, which an incremental upsert alone could never notice.
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "id,contact_id,label\n3,7,keep-me\n").unwrap();

    run(
        &config_yaml(&csv, &db, true),
        dir.path(),
        opts("cleanup_on"),
    )
    .await;

    assert_eq!(
        rows(&db).await,
        vec![(3, 7), (10, 8), (11, 8)],
        "stale rows 1 and 2 removed; the written row and contact 8's rows untouched"
    );
}

#[tokio::test]
async fn an_empty_fetch_clears_the_whole_scope() {
    // THE motivating case: every association for contact 7 was removed upstream,
    // so the fetch returns nothing. An upsert alone writes nothing and the stale
    // rows live forever; cleanup must delete the scope.
    let dir = tempfile::tempdir().unwrap();
    let db = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("d.sqlite").display()
    );
    seed(&db).await;

    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "id,contact_id,label\n").unwrap(); // header only

    run(
        &config_yaml(&csv, &db, true),
        dir.path(),
        opts("cleanup_empty"),
    )
    .await;

    assert_eq!(
        rows(&db).await,
        vec![(10, 8), (11, 8)],
        "contact 7's scope emptied; contact 8 untouched"
    );
}

#[tokio::test]
async fn without_the_sink_opt_in_stale_rows_survive() {
    // The status quo this feature exists to fix — proves the test above is
    // actually measuring cleanup and not some other effect.
    let dir = tempfile::tempdir().unwrap();
    let db = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("d.sqlite").display()
    );
    seed(&db).await;

    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "id,contact_id,label\n3,7,keep-me\n").unwrap();

    run(
        &config_yaml(&csv, &db, false),
        dir.path(),
        opts("cleanup_off"),
    )
    .await;

    assert_eq!(
        rows(&db).await,
        vec![(1, 7), (2, 7), (3, 7), (10, 8), (11, 8)],
        "no cleanup opt-in → stale rows remain"
    );
}

#[tokio::test]
async fn limit_run_does_not_delete() {
    // Under --limit the sink is wrapped to drop records, so "what this run
    // wrote" is a fiction and a delete computed from it would remove live rows.
    let dir = tempfile::tempdir().unwrap();
    let db = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("d.sqlite").display()
    );
    seed(&db).await;

    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "id,contact_id,label\n3,7,keep-me\n").unwrap();

    let mut o = opts("cleanup_limit");
    o.limit = Some(0);
    run(&config_yaml(&csv, &db, true), dir.path(), o).await;

    assert_eq!(
        rows(&db).await.len(),
        5,
        "a --limit run must never delete: its written set is synthetic"
    );
}

#[tokio::test]
async fn dry_run_does_not_delete() {
    let dir = tempfile::tempdir().unwrap();
    let db = format!(
        "sqlite://{}?mode=rwc",
        dir.path().join("d.sqlite").display()
    );
    seed(&db).await;

    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "id,contact_id,label\n3,7,keep-me\n").unwrap();

    let mut o = opts("cleanup_dry");
    o.dry_run = true;
    run(&config_yaml(&csv, &db, true), dir.path(), o).await;

    assert_eq!(
        rows(&db).await.len(),
        5,
        "--dry-run must never delete: the sink is a counter, not the real sink"
    );
}

// ── Load-time gates ──────────────────────────────────────────────────────────

fn expand_err(yaml: &str) -> String {
    let path = std::path::PathBuf::from("p.yaml");
    let cfg = PipelineConfig::from_text(yaml, &path).expect("config parses");
    expand(&cfg).expect_err("expand must reject").to_string()
}

#[test]
fn an_empty_scope_is_rejected() {
    // An empty scope matches every row in the destination — a truncate.
    let err = expand_err(
        r#"
version: 1
pipeline:
  source:
    type: csv
    config: { path: "./x.csv" }
    complete_for:
      scope: {}
      on_missing: delete
  sink:
    type: sqlite
    config:
      database_url: "sqlite://x.db"
      table_name: t
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
"#,
    );
    assert!(err.contains("empty"), "{err}");
}

#[test]
fn a_claim_without_on_missing_delete_is_inert() {
    // Adding a claim must never start deleting on its own.
    let path = std::path::PathBuf::from("p.yaml");
    let cfg = PipelineConfig::from_text(
        r#"
version: 1
pipeline:
  source:
    type: csv
    config: { path: "./x.csv" }
    complete_for:
      scope: { contact_id: 7 }
  sink:
    type: jsonl
    config: { path: "./out.jsonl" }
"#,
        &path,
    )
    .expect("config parses");
    let nodes = expand(&cfg).expect("an inert claim must not be gated at all");
    assert!(
        nodes[0].cleanup_scope.is_none(),
        "on_missing defaults to ignore, so no cleanup is scheduled"
    );
}

#[test]
fn cleanup_on_an_append_sink_is_rejected() {
    let err = expand_err(
        r#"
version: 1
pipeline:
  source:
    type: csv
    config: { path: "./x.csv" }
    complete_for:
      scope: { contact_id: 7 }
      on_missing: delete
  sink:
    type: jsonl
    config:
      path: "./out.jsonl"
"#,
    );
    assert!(
        err.contains("jsonl"),
        "error should name the unsupported sink: {err}"
    );
}

#[test]
fn complete_for_on_a_sink_is_rejected() {
    // Only a source can claim a fetch returned every record for a scope.
    let err = expand_err(
        r#"
version: 1
pipeline:
  source: { type: csv, config: { path: "./x.csv" } }
  sink:
    type: sqlite
    config:
      database_url: "sqlite://x.db"
      table_name: t
      column_mapping: auto_map
    complete_for:
      scope: { contact_id: 7 }
"#,
    );
    assert!(err.contains("complete_for"), "{err}");
}

#[test]
fn quarantining_quality_policy_is_rejected() {
    // A quarantined record never reaches the sink, so the cleanup tracker never
    // records its key — and the delete would then remove its destination row,
    // losing data the source still has.
    let err = expand_err(
        r#"
version: 1
pipeline:
  source:
    type: csv
    config: { path: "./x.csv" }
    complete_for:
      scope: { contact_id: 7 }
      on_missing: delete
  sink:
    type: sqlite
    config:
      database_url: "sqlite://x.db"
      table_name: t
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
  quality:
    record:
      - type: not_null
        field: id
        on_failure: quarantine
  dlq:
    sink: { type: jsonl, config: { path: "./dlq.jsonl" } }
"#,
    );
    assert!(
        err.contains("quarantin"),
        "error should explain the quarantine incompatibility: {err}"
    );
}
