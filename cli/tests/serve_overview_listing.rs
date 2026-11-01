//! Integration test for the `RunHistory::overview_aggregates` trait default
//! (`aggregate_by_listing`), the path a third-party history backend takes:
//! against a real SQLite history it must report what SQLite's own aggregation
//! reports, with a pipeline filter and the freshness section.
#![cfg(feature = "serve-history-sqlite")]

use chrono::{DateTime, Utc};
use faucet_cli::serve::history::catalog::{CatalogUpdate, DatasetObservation, DatasetRole};
use faucet_cli::serve::history::overview::{OverviewQuery, aggregate_by_listing};
use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{RunHistory as _, RunRecord, RunStatus};
use std::time::Duration;

fn ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

fn run(id: &str, name: Option<&str>, pipeline: Option<&str>, status: RunStatus) -> RunRecord {
    let mut r = RunRecord::queued(
        id.into(),
        name.map(Into::into),
        Default::default(),
        None,
        ts("2026-10-30T05:00:00Z"),
    );
    if let Some(p) = pipeline {
        r.labels.insert("pipeline".into(), p.into());
    }
    r.status = status;
    r.records_written = 10;
    r
}

fn observe(at: &str, schema: serde_json::Value) -> CatalogUpdate {
    CatalogUpdate {
        run_id: at.into(),
        pipeline: "orders".into(),
        row: "default".into(),
        recorded_at: ts(at),
        sources: vec![DatasetObservation {
            uri: "file://in".into(),
            kind: "csv".into(),
            role: DatasetRole::Source,
            schema: None,
            records: 1,
        }],
        sink: DatasetObservation {
            uri: "file://out".into(),
            kind: "jsonl".into(),
            role: DatasetRole::Sink,
            schema: Some(schema),
            records: 1,
        },
        column_lineage: None,
    }
}

#[tokio::test]
async fn the_listing_default_matches_sqlite_with_a_pipeline_filter_and_freshness() {
    let dir = tempfile::tempdir().unwrap();
    let h = SqliteHistory::connect(
        &format!("sqlite:{}", dir.path().join("h.db").display()),
        Duration::from_secs(60),
        Duration::from_secs(30),
        "i".into(),
    )
    .await
    .unwrap();
    for r in [
        run("p1", None, Some("orders"), RunStatus::Completed),
        run("p2", Some("nightly"), Some("orders"), RunStatus::Failed),
        run("p3", None, Some("users"), RunStatus::Failed),
        run("p4", Some("orders"), None, RunStatus::Completed),
        run("p5", Some("orders"), None, RunStatus::Running),
    ] {
        h.upsert(&r).await.unwrap();
    }
    let a = serde_json::json!({"type": "object", "properties": {"id": {"type": "integer"}}});
    let b = serde_json::json!({"type": "object", "properties": {"id": {"type": "integer"}, "v": {"type": "string"}}});
    h.catalog_record(&observe("2026-10-30T01:00:00Z", a))
        .await
        .unwrap();
    h.catalog_record(&observe("2026-10-30T02:00:00Z", b))
        .await
        .unwrap();

    let mut q = OverviewQuery::window(ts("2026-10-30T00:00:00Z"), ts("2026-10-31T00:00:00Z"));
    q.stale_before = Some(ts("2026-10-29T12:00:00Z"));
    let mut filtered = q.clone();
    filtered.pipeline = Some("orders".into());
    for q in [q, filtered] {
        let listed = aggregate_by_listing(&h, &q).await.unwrap();
        assert_eq!(listed, h.overview_aggregates(&q).await.unwrap(), "{q:?}");
        assert_eq!(listed.freshness.unwrap().schema_changed_count, 1, "{q:?}");
    }
    let mut only = OverviewQuery::window(ts("2026-10-30T00:00:00Z"), ts("2026-10-31T00:00:00Z"));
    only.pipeline = Some("orders".into());
    assert_eq!(aggregate_by_listing(&h, &only).await.unwrap().runs.total, 4);
}
