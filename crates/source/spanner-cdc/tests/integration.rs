//! Emulator integration tests for the Spanner change-streams source.
//!
//! Boots the Cloud Spanner emulator via testcontainers and skips cleanly when
//! Docker is unavailable (`FAUCET_REQUIRE_BACKENDS` turns a skip into a failure
//! in CI). Run a single test with `cargo test -p faucet-source-spanner-cdc
//! --test integration <name>`.

mod support;

use faucet_core::{FaucetError, Source};
use faucet_source_spanner_cdc::SpannerCdcSource;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::time::Duration;

const TABLE_DDL: &str = "CREATE TABLE t (id INT64 NOT NULL, name STRING(MAX), amt NUMERIC, \
                         doc JSON, tags ARRAY<STRING(MAX)>) PRIMARY KEY (id)";
const STREAM_DDL: &str = "CREATE CHANGE STREAM cs FOR t";
const WAIT: Duration = Duration::from_secs(60);

async fn setup(db: &str) -> Option<(support::Emulator, gcloud_spanner::client::Client)> {
    let emu = support::start_emulator().await?;
    support::create_database(&emu.host, db, &[TABLE_DDL, STREAM_DDL]).await;
    let client = support::raw_client(&emu.host, db).await;
    Some((emu, client))
}

async fn source_from(emu: &support::Emulator, db: &str, start: Value) -> SpannerCdcSource {
    SpannerCdcSource::new(support::cdc_config(&emu.host, db, "cs", start))
        .await
        .expect("source")
}

#[tokio::test(flavor = "multi_thread")]
async fn captures_insert_update_delete_with_typed_values() {
    let Some((emu, client)) = setup("crud").await else {
        return;
    };
    let ts0 = support::emulator_now(&client, false).await;
    support::execute_dml(
        &client,
        "INSERT INTO t (id, name, amt, doc, tags) \
         VALUES (1, 'a', NUMERIC '1.5', JSON '{\"k\": 1}', ['x', 'y'])",
    )
    .await;
    support::execute_dml(&client, "UPDATE t SET name = 'b' WHERE id = 1").await;
    support::execute_dml(&client, "DELETE FROM t WHERE id = 1").await;

    let source = source_from(&emu, "crud", json!({"type": "timestamp", "timestamp": ts0})).await;
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
    assert_eq!(delete["document_key"], json!({"id": 1}));
    // commit timestamps are non-decreasing in delivery order
    let ts: Vec<&str> = records
        .iter()
        .map(|r| r["commit_timestamp"].as_str().unwrap())
        .collect();
    assert!(ts.windows(2).all(|w| w[0] <= w[1]), "{ts:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn start_from_now_anchors_then_reads_only_new_changes() {
    let Some((emu, client)) = setup("anchor").await else {
        return;
    };
    // History that must NOT be replayed.
    support::execute_dml(&client, "INSERT INTO t (id, name) VALUES (100, 'old')").await;

    let source = source_from(&emu, "anchor", json!({"type": "now"})).await;
    let first = support::run_once(&source).await.expect("anchor run");
    assert!(first.records.is_empty());
    assert_eq!(first.pages.len(), 1, "an anchor run is one empty page");
    let anchor = first.bookmark.expect("anchor bookmark");
    assert!(anchor["partitions"][0]["token"].is_null(), "root partition");

    // Leave headroom for host/VM clock skew, then write a new row.
    tokio::time::sleep(Duration::from_secs(2)).await;
    support::execute_dml(&client, "INSERT INTO t (id, name) VALUES (200, 'new')").await;
    source.apply_start_bookmark(anchor).await.unwrap();
    let records = support::collect(&source, 1, WAIT).await;
    assert_eq!(support::ops_and_ids(&records), vec![("c".into(), 200)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn resuming_from_a_bookmark_does_not_replay_delivered_changes() {
    let Some((emu, client)) = setup("resume").await else {
        return;
    };
    let ts0 = support::emulator_now(&client, false).await;
    support::execute_dml(&client, "INSERT INTO t (id, name) VALUES (1, 'a')").await;
    support::execute_dml(&client, "UPDATE t SET name = 'b' WHERE id = 1").await;

    let source = source_from(
        &emu,
        "resume",
        json!({"type": "timestamp", "timestamp": ts0}),
    )
    .await;
    let first = support::collect(&source, 2, WAIT).await;
    assert_eq!(first.len(), 2);

    support::execute_dml(&client, "INSERT INTO t (id, name) VALUES (2, 'c')").await;
    let next = support::collect(&source, 1, WAIT).await;
    let seen = support::ops_and_ids(&next);
    assert!(seen.contains(&("c".into(), 2)), "{seen:?}");
    assert!(
        !seen.contains(&("c".into(), 1)) && !seen.contains(&("u".into(), 1)),
        "already-delivered changes were replayed: {seen:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_quiet_run_still_persists_a_bookmark_with_the_frontier() {
    let Some((emu, client)) = setup("quiet").await else {
        return;
    };
    let ts0 = support::emulator_now(&client, false).await;
    let source = source_from(
        &emu,
        "quiet",
        json!({"type": "timestamp", "timestamp": ts0}),
    )
    .await;
    // Let the run window open past the start (host/VM clock headroom).
    tokio::time::sleep(Duration::from_secs(2)).await;
    let run = support::run_once(&source).await.expect("run");
    assert!(run.records.is_empty());
    let bm = run.bookmark.expect("bookmark on a quiet run");
    let parts = bm["partitions"].as_array().unwrap();
    assert!(
        !parts.is_empty(),
        "frontier must hold the stream's partitions"
    );
    assert!(
        parts.iter().all(|p| p["token"].is_string()),
        "root is consumed"
    );
    assert!(bm["watermark"].is_string());
    // The persisted bookmark is accepted back.
    source.apply_start_bookmark(bm).await.unwrap();
    let lag = source
        .lag()
        .await
        .unwrap()
        .expect("lag known after a bookmark");
    assert!(lag.seconds.unwrap() >= 0.0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_source_transaction_is_never_split_across_pages() {
    let Some((emu, client)) = setup("pages").await else {
        return;
    };
    let ts0 = support::emulator_now(&client, false).await;
    support::execute_dml_txn(
        &client,
        &[
            "INSERT INTO t (id, name) VALUES (1, 'a')",
            "INSERT INTO t (id, name) VALUES (2, 'b')",
            "INSERT INTO t (id, name) VALUES (3, 'c')",
            "INSERT INTO t (id, name) VALUES (4, 'd')",
            "INSERT INTO t (id, name) VALUES (5, 'e')",
        ],
    )
    .await;
    support::execute_dml(&client, "INSERT INTO t (id, name) VALUES (6, 'f')").await;

    // batch_size 2 is far below the 5-row transaction.
    let mut cfg = support::cdc_config(
        &emu.host,
        "pages",
        "cs",
        json!({"type": "timestamp", "timestamp": ts0}),
    );
    cfg.batch_size = 2;
    let source = SpannerCdcSource::new(cfg).await.expect("source");

    let mut pages: Vec<Vec<Value>> = Vec::new();
    let deadline = std::time::Instant::now() + WAIT;
    while pages.iter().map(Vec::len).sum::<usize>() < 6 && std::time::Instant::now() < deadline {
        let run = support::run_once(&source).await.expect("run");
        pages.extend(run.pages.iter().map(|p| p.records.clone()));
        if let Some(bm) = run.bookmark {
            source.apply_start_bookmark(bm).await.unwrap();
        }
    }
    assert_eq!(pages.iter().map(Vec::len).sum::<usize>(), 6);
    for txn in pages
        .iter()
        .flatten()
        .map(|r| r["server_transaction_id"].as_str().unwrap().to_string())
        .collect::<BTreeSet<_>>()
    {
        let holders = pages
            .iter()
            .filter(|p| p.iter().any(|r| r["server_transaction_id"] == txn.as_str()))
            .count();
        assert_eq!(holders, 1, "transaction {txn} straddles pages");
    }
}

/// The emulator splits partitions on its own after ~20s. A run that starts
/// before a split must walk the partition tree (parent before child) and
/// deliver every change exactly once, in per-key order.
#[tokio::test(flavor = "multi_thread")]
async fn reads_across_partition_splits_in_per_key_order() {
    let Some((emu, client)) = setup("splits").await else {
        return;
    };
    let ts0 = support::emulator_now(&client, false).await;
    support::execute_dml(
        &client,
        "INSERT INTO t (id, name) VALUES (1, 'before-split')",
    )
    .await;
    support::execute_dml(
        &client,
        "INSERT INTO t (id, name) VALUES (2, 'before-split')",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(45)).await;
    support::execute_dml(&client, "UPDATE t SET name = 'after-split' WHERE id = 1").await;
    support::execute_dml(
        &client,
        "INSERT INTO t (id, name) VALUES (3, 'after-split')",
    )
    .await;

    let source = source_from(
        &emu,
        "splits",
        json!({"type": "timestamp", "timestamp": ts0}),
    )
    .await;
    let records = support::collect(&source, 4, Duration::from_secs(120)).await;
    let seen = support::ops_and_ids(&records);
    assert_eq!(seen.len(), 4, "every change exactly once: {seen:?}");
    let pos = |op: &str, id: i64| seen.iter().position(|x| x == &(op.to_string(), id));
    assert!(
        pos("c", 1).unwrap() < pos("u", 1).unwrap(),
        "per-key order broken across the split: {seen:?}"
    );
    assert!(pos("c", 2).is_some() && pos("c", 3).is_some(), "{seen:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_stream_is_a_clear_config_error() {
    let Some((emu, _client)) = setup("missing").await else {
        return;
    };
    let err = SpannerCdcSource::new(support::cdc_config(
        &emu.host,
        "missing",
        "no_such_stream",
        json!({"type": "now"}),
    ))
    .await
    .err()
    .expect("must fail");
    assert!(matches!(err, FaucetError::Config(_)), "{err:?}");
    assert!(err.to_string().contains("no_such_stream"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_start_before_the_stream_existed_is_an_actionable_error() {
    let Some((emu, _client)) = setup("toold").await else {
        return;
    };
    let source = source_from(
        &emu,
        "toold",
        json!({"type": "timestamp", "timestamp": "2020-01-01T00:00:00Z"}),
    )
    .await;
    let err = support::run_once(&source).await.err().expect("must fail");
    assert!(matches!(err, FaucetError::Source(_)), "{err:?}");
    assert!(err.to_string().contains("retention"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mutable_key_range_stream_is_refused() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    let created = support::try_create_database(
        &emu.host,
        "mkr",
        &[
            TABLE_DDL,
            "CREATE CHANGE STREAM mkr_cs FOR t OPTIONS (partition_mode = 'MUTABLE_KEY_RANGE')",
        ],
    )
    .await;
    if let Err(e) = created {
        // The emulator does not model every stream option; the refusal itself
        // is unit-tested (`check_stream_options`).
        eprintln!("skipping: emulator rejects partition_mode option ({e})");
        return;
    }
    let err = SpannerCdcSource::new(support::cdc_config(
        &emu.host,
        "mkr",
        "mkr_cs",
        json!({"type": "now"}),
    ))
    .await
    .err()
    .expect("must refuse");
    assert!(matches!(err, FaucetError::Config(_)), "{err:?}");
    assert!(err.to_string().contains("MUTABLE_KEY_RANGE"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_postgresql_database_without_the_dialect_setting_is_a_clear_error() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_pg_database(
        &emu.host,
        "pgdb",
        &[
            "CREATE TABLE t (id bigint primary key, name varchar)",
            "CREATE CHANGE STREAM cs FOR t",
        ],
    )
    .await;
    let err = SpannerCdcSource::new(support::cdc_config(
        &emu.host,
        "pgdb",
        "cs",
        json!({"type": "now"}),
    ))
    .await
    .err()
    .expect("must refuse");
    assert!(matches!(err, FaucetError::Config(_)), "{err:?}");
    assert!(err.to_string().contains("dialect: postgresql"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_postgresql_setting_on_a_googlesql_database_is_a_clear_error() {
    let Some((emu, _client)) = setup("wrongdialect").await else {
        return;
    };
    let err = SpannerCdcSource::new(support::pg_cdc_config(
        &emu.host,
        "wrongdialect",
        "cs",
        json!({"type": "now"}),
    ))
    .await
    .err()
    .expect("must refuse");
    assert!(matches!(err, FaucetError::Config(_)), "{err:?}");
    assert!(err.to_string().contains("GoogleSQL"), "{err}");
}
