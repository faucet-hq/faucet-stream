//! Emulator integration tests for change streams on a **PostgreSQL-dialect**
//! database (`spanner.read_json_<stream>`).

mod support;

use faucet_core::Source;
use faucet_source_spanner_cdc::SpannerCdcSource;
use serde_json::{Value, json};
use std::time::Duration;

const TABLE_DDL: &str = "CREATE TABLE t (id bigint primary key, name varchar, amt numeric, \
                         doc jsonb, tags varchar[])";
const STREAM_DDL: &str = "CREATE CHANGE STREAM cs FOR t";
const WAIT: Duration = Duration::from_secs(60);

async fn setup(db: &str) -> Option<(support::Emulator, gcloud_spanner::client::Client)> {
    let emu = support::start_emulator().await?;
    support::create_pg_database(&emu.host, db, &[TABLE_DDL, STREAM_DDL]).await;
    let client = support::raw_client(&emu.host, db).await;
    Some((emu, client))
}

async fn source_from(emu: &support::Emulator, db: &str, start: Value) -> SpannerCdcSource {
    SpannerCdcSource::new(support::pg_cdc_config(&emu.host, db, "cs", start))
        .await
        .expect("source")
}

#[tokio::test(flavor = "multi_thread")]
async fn captures_insert_update_delete_with_typed_values() {
    let Some((emu, client)) = setup("pgcrud").await else {
        return;
    };
    let ts0 = support::emulator_now(&client, true).await;
    support::execute_dml(
        &client,
        "INSERT INTO t (id, name, amt, doc, tags) \
         VALUES (1, 'a', 1.5, '{\"k\": 1}'::jsonb, ARRAY['x', 'y'])",
    )
    .await;
    support::execute_dml(&client, "UPDATE t SET name = 'b' WHERE id = 1").await;
    support::execute_dml(&client, "DELETE FROM t WHERE id = 1").await;

    let source = source_from(
        &emu,
        "pgcrud",
        json!({"type": "timestamp", "timestamp": ts0}),
    )
    .await;
    let records = support::collect(&source, 3, WAIT).await;

    assert_eq!(
        support::ops_and_ids(&records),
        vec![("c".into(), 1), ("u".into(), 1), ("d".into(), 1)],
        "{records:#?}"
    );
    let insert = &records[0];
    assert_eq!(insert["table"], "t");
    assert_eq!(insert["before"], Value::Null);
    assert_eq!(insert["after"]["id"], 1);
    assert_eq!(insert["after"]["name"], "a");
    assert_eq!(insert["after"]["amt"], "1.5");
    assert_eq!(insert["after"]["doc"], json!({"k": 1}));
    assert_eq!(insert["after"]["tags"], json!(["x", "y"]));
    assert_eq!(insert["document_key"], json!({"id": 1}));

    let update = &records[1];
    assert_eq!(update["before"]["name"], "a");
    assert_eq!(update["after"]["name"], "b");

    let delete = &records[2];
    assert_eq!(delete["after"], Value::Null);
    assert_eq!(delete["before"]["name"], "b");
}

#[tokio::test(flavor = "multi_thread")]
async fn start_from_now_anchors_then_reads_only_new_changes() {
    let Some((emu, client)) = setup("pganchor").await else {
        return;
    };
    support::execute_dml(&client, "INSERT INTO t (id, name) VALUES (100, 'old')").await;

    let source = source_from(&emu, "pganchor", json!({"type": "now"})).await;
    let first = support::run_once(&source).await.expect("anchor run");
    assert!(first.records.is_empty());
    let anchor = first.bookmark.expect("anchor bookmark");

    tokio::time::sleep(Duration::from_secs(2)).await;
    support::execute_dml(&client, "INSERT INTO t (id, name) VALUES (200, 'new')").await;
    source.apply_start_bookmark(anchor).await.unwrap();
    let records = support::collect(&source, 1, WAIT).await;
    assert_eq!(support::ops_and_ids(&records), vec![("c".into(), 200)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn resuming_from_a_bookmark_does_not_replay_delivered_changes() {
    let Some((emu, client)) = setup("pgresume").await else {
        return;
    };
    let ts0 = support::emulator_now(&client, true).await;
    support::execute_dml(&client, "INSERT INTO t (id, name) VALUES (1, 'a')").await;
    support::execute_dml(&client, "UPDATE t SET name = 'b' WHERE id = 1").await;

    let source = source_from(
        &emu,
        "pgresume",
        json!({"type": "timestamp", "timestamp": ts0}),
    )
    .await;
    assert_eq!(support::collect(&source, 2, WAIT).await.len(), 2);

    support::execute_dml(&client, "INSERT INTO t (id, name) VALUES (2, 'c')").await;
    let seen = support::ops_and_ids(&support::collect(&source, 1, WAIT).await);
    assert!(seen.contains(&("c".into(), 2)), "{seen:?}");
    assert!(
        !seen.contains(&("c".into(), 1)) && !seen.contains(&("u".into(), 1)),
        "already-delivered changes were replayed: {seen:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_transaction_is_never_split_across_pages() {
    let Some((emu, client)) = setup("pgtxn").await else {
        return;
    };
    let ts0 = support::emulator_now(&client, true).await;
    support::execute_dml_txn(
        &client,
        &[
            "INSERT INTO t (id, name) VALUES (1, 'a')",
            "INSERT INTO t (id, name) VALUES (2, 'b')",
            "INSERT INTO t (id, name) VALUES (3, 'c')",
        ],
    )
    .await;
    let mut cfg = support::pg_cdc_config(
        &emu.host,
        "pgtxn",
        "cs",
        json!({"type": "timestamp", "timestamp": ts0}),
    );
    cfg.batch_size = 2;
    let source = SpannerCdcSource::new(cfg).await.expect("source");
    let mut got = 0;
    let deadline = std::time::Instant::now() + WAIT;
    while got < 3 && std::time::Instant::now() < deadline {
        let run = support::run_once(&source).await.expect("run");
        for page in run.pages.iter().filter(|p| !p.records.is_empty()) {
            // All three rows share one transaction, so they arrive together
            // even though `batch_size` is 2.
            assert_eq!(page.records.len(), 3, "transaction was split");
            got += page.records.len();
        }
        if let Some(bm) = run.bookmark {
            source.apply_start_bookmark(bm).await.unwrap();
        }
    }
    assert_eq!(got, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_stream_is_a_clear_config_error() {
    let Some((emu, _client)) = setup("pgmissing").await else {
        return;
    };
    let err = SpannerCdcSource::new(support::pg_cdc_config(
        &emu.host,
        "pgmissing",
        "nope",
        json!({"type": "now"}),
    ))
    .await
    .err()
    .expect("must fail");
    assert!(err.to_string().contains("does not exist"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_start_before_the_stream_existed_is_an_actionable_error() {
    let Some((emu, _client)) = setup("pgold").await else {
        return;
    };
    let source = source_from(
        &emu,
        "pgold",
        json!({"type": "timestamp", "timestamp": "2020-01-01T00:00:00Z"}),
    )
    .await;
    let err = support::run_once(&source).await.err().expect("must fail");
    assert!(err.to_string().contains("retention"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn check_reports_connection_and_stream_probes() {
    let Some((emu, _client)) = setup("pgcheck").await else {
        return;
    };
    let source = source_from(&emu, "pgcheck", json!({"type": "now"})).await;
    let report = source
        .check(&faucet_core::check::CheckContext::default())
        .await
        .expect("check");
    assert_eq!(report.failed_count(), 0, "{report:?}");
}
