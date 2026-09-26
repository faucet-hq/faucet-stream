//! End-to-end `faucet backfill` tests (#282) — fully offline (SQLite source →
//! SQLite upsert sink, file state store). Covers the acceptance criteria:
//! a real windowed replay writes rows and records a durable marker, `--resume`
//! completes only remaining units, overlapping replays into an upsert sink
//! produce no duplicates, and the forward-sync bookmark is provably untouched.

#![cfg(all(feature = "source-sqlite", feature = "sink-sqlite"))]

use faucet_cli::backfill::state::{BackfillState, marker_key};
use faucet_cli::backfill::{BackfillOptions, BackfillRange, run_backfill};
use faucet_cli::config::PipelineConfig;
use serde_json::json;
use std::path::Path;

fn utc() -> chrono_tz::Tz {
    "UTC".parse().unwrap()
}

fn time_range(from: &str, to: &str, window_days: i64) -> BackfillRange {
    BackfillRange::Time {
        from: faucet_cli::backfill::plan::parse_boundary(from, utc()).unwrap(),
        to: faucet_cli::backfill::plan::parse_boundary(to, utc()).unwrap(),
        // `Days` (calendar) rather than an absolute 24h step — these ranges are
        // UTC, where the two coincide, but the calendar form is what `--window 1d`
        // now parses to (#461).
        window: Some(faucet_cli::backfill::plan::WindowStep::Days(window_days)),
        tz: utc(),
    }
}

fn opts(range: BackfillRange) -> BackfillOptions {
    BackfillOptions {
        pipeline_name: "bf".into(),
        execution: None,
        auth: faucet_cli::auth_catalog::AuthCatalog::default(),
        resilience: None,
        usage: Default::default(),
        range,
        // SQLite serializes writers at the file level, so parallel units
        // against one destination file would contend — run sequentially.
        concurrency: 1,
        row: None,
        into_sink: None,
        dry_run: false,
        resume: false,
        restart: false,
        cancel: None,
    }
}

/// Seed a source DB with two rows per day for 2026-06-01..03 and create the
/// (upsert-keyed) destination table.
async fn seed(dir: &Path) -> (String, String) {
    let src = dir.join("src.db").display().to_string();
    let dst = dir.join("dst.db").display().to_string();
    for (path, ddl) in [
        (
            &src,
            "CREATE TABLE events (id INTEGER PRIMARY KEY, day TEXT NOT NULL, amount INTEGER)",
        ),
        (
            &dst,
            "CREATE TABLE events_out (id INTEGER PRIMARY KEY, day TEXT NOT NULL, amount INTEGER)",
        ),
    ] {
        let pool = sqlx::SqlitePool::connect(&format!("sqlite://{path}?mode=rwc"))
            .await
            .expect("create db");
        sqlx::query(ddl).execute(&pool).await.expect("ddl");
        pool.close().await;
    }
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{src}"))
        .await
        .expect("open src");
    let mut id = 0;
    for day in ["2026-06-01", "2026-06-02", "2026-06-03"] {
        for _ in 0..2 {
            id += 1;
            sqlx::query("INSERT INTO events (id, day, amount) VALUES (?, ?, 10)")
                .bind(id)
                .bind(day)
                .execute(&pool)
                .await
                .expect("seed row");
        }
    }
    pool.close().await;
    (src, dst)
}

async fn dst_row_count(dst: &str) -> i64 {
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{dst}"))
        .await
        .expect("open dst");
    let n: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events_out")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    n
}

fn config(src: &str, dst: &str, state_dir: &Path) -> PipelineConfig {
    let yaml = format!(
        r#"
version: 1
name: bf
pipeline:
  source:
    type: sqlite
    config:
      database_url: "sqlite://{src}"
      query: >-
        SELECT id, day, amount FROM events
        WHERE day >= '${{backfill.start_date}}' AND day < '${{backfill.end_date}}'
  sink:
    type: sqlite
    config:
      database_url: "sqlite://{dst}?mode=rwc"
      table_name: events_out
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
  state:
    type: file
    config: {{ path: "{state}" }}
"#,
        src = src,
        dst = dst,
        state = state_dir.display(),
    );
    faucet_cli::config::parse_with_extension(&yaml, "yaml").expect("config parses")
}

#[tokio::test(flavor = "multi_thread")]
async fn backfill_runs_units_resumes_and_never_touches_the_live_bookmark() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (src, dst) = seed(dir.path()).await;
    let state_dir = dir.path().join("state");
    let cfg = config(&src, &dst, &state_dir);

    let store = faucet_cli::state::build_state_store(cfg.pipeline.state.as_ref().unwrap())
        .await
        .expect("state store");

    // Plant a forward-sync bookmark at the live key — the single most
    // important invariant is that a backfill never clobbers it.
    let live_key = "bf::default";
    store.put(live_key, &json!("2026-07-04")).await.unwrap();

    // ── Full 3-day windowed run ───────────────────────────────────────────────
    let out = run_backfill(&cfg, opts(time_range("2026-06-01", "2026-06-04", 1)))
        .await
        .expect("backfill runs");
    assert_eq!(out.planned, 3);
    assert_eq!(out.succeeded, 3, "unit outcomes: {:?}", out.units);
    assert_eq!(out.failed, 0);
    assert_eq!(dst_row_count(&dst).await, 6, "two rows per day written");

    // Durable marker records all three units as done.
    let hash = faucet_cli::backfill::plan::range_hash(&out.descriptor);
    let marker_k = marker_key("bf", &hash);
    let marker = BackfillState::from_value(store.get(&marker_k).await.unwrap().expect("marker"))
        .expect("marker parses");
    assert_eq!(marker.done_count(), 3);
    assert_eq!(marker.failed_count(), 0);

    // The live bookmark is provably unchanged.
    assert_eq!(
        store.get(live_key).await.unwrap(),
        Some(json!("2026-07-04")),
        "forward-sync bookmark untouched by the backfill"
    );

    // ── Same range again without --resume/--restart → actionable error ──────
    let err = run_backfill(&cfg, opts(time_range("2026-06-01", "2026-06-04", 1)))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("--resume"), "{err}");

    // ── --resume with everything done is a no-op ────────────────────────────
    let mut o = opts(time_range("2026-06-01", "2026-06-04", 1));
    o.resume = true;
    let out = run_backfill(&cfg, o).await.expect("resume runs");
    assert_eq!(out.skipped, 3, "all units skipped");
    assert_eq!(out.succeeded, 0);
    assert_eq!(dst_row_count(&dst).await, 6, "no rewrites");

    // ── Interrupted-backfill resume: only remaining units run ───────────────
    // Simulate a crash after day 1 by rewriting the marker with only the
    // first unit done, then wiping the destination so writes are observable.
    let mut partial = BackfillState::new(marker.descriptor.clone());
    partial.mark_done("20260601T000000Z");
    partial.mark_failed("20260602T000000Z", "simulated crash");
    store
        .put(&marker_k, &partial.to_value().unwrap())
        .await
        .unwrap();
    let pool = sqlx::SqlitePool::connect(&format!("sqlite://{dst}"))
        .await
        .unwrap();
    sqlx::query("DELETE FROM events_out")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let mut o = opts(time_range("2026-06-01", "2026-06-04", 1));
    o.resume = true;
    let out = run_backfill(&cfg, o).await.expect("resume runs");
    assert_eq!(out.skipped, 1, "done unit skipped");
    assert_eq!(out.succeeded, 2, "failed + pending units re-run");
    assert_eq!(
        dst_row_count(&dst).await,
        4,
        "only days 2 and 3 replayed (day 1 was already done)"
    );

    // ── Overlapping replay into an upsert sink produces no duplicates ───────
    let mut o = opts(time_range("2026-06-01", "2026-06-04", 1));
    o.restart = true;
    let out = run_backfill(&cfg, o).await.expect("restart runs");
    assert_eq!(out.succeeded, 3);
    assert_eq!(
        dst_row_count(&dst).await,
        6,
        "full re-replay converges under write_mode: upsert — no duplicates"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dry_run_reports_plan_without_writing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (src, dst) = seed(dir.path()).await;
    let cfg = config(&src, &dst, &dir.path().join("state"));

    let mut o = opts(time_range("2026-06-01", "2026-07-02", 1));
    o.dry_run = true;
    let out = run_backfill(&cfg, o).await.expect("dry run");
    assert!(out.dry_run);
    assert_eq!(out.planned, 31, "31 one-day units planned");
    assert!(out.units.iter().all(|u| u.outcome == "pending"));
    assert_eq!(dst_row_count(&dst).await, 0, "nothing executed");
}
