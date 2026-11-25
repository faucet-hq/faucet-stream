//! A `>=` cursor on a FLOAT32 column keeps the rows at the bookmark value
//! (#1114): `0.7` as a FLOAT32 is `0.699999988…`, below its decimal.
#![allow(clippy::disallowed_methods)] // tests drive raw clients directly

mod support;

use std::collections::HashMap;

use faucet_core::Source;
use faucet_source_spanner::{SpannerReplication, SpannerSource, SpannerSourceConfig};
use futures::TryStreamExt;
use serde_json::{Value, json};

async fn read(host: &str, db: &str, query: &str, bookmark: Option<&Value>) -> (Vec<i64>, Value) {
    let mut config = SpannerSourceConfig::new(support::PROJECT, support::INSTANCE, db, query);
    config.connection = support::connection(host, db);
    config.replication = SpannerReplication::Incremental {
        column: "c".into(),
        initial_value: json!(0.0),
    };
    let source = SpannerSource::new(config).await.expect("source");
    if let Some(b) = bookmark {
        source
            .apply_start_bookmark(b.clone())
            .await
            .expect("bookmark");
    }
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut ids = Vec::new();
    let mut last = Value::Null;
    while let Some(page) = stream.try_next().await.expect("page") {
        ids.extend(page.records.iter().map(|r| r["id"].as_i64().unwrap()));
        if let Some(b) = page.bookmark {
            last = b;
        }
    }
    ids.sort_unstable();
    (ids, last)
}

async fn rows_at_the_bookmark_are_kept(db: &str, column_type: &str, query: &str) {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    let ddl = format!("CREATE TABLE t (id INT64 NOT NULL, c {column_type}) PRIMARY KEY (id)");
    support::create_database(&emu.host, db, &[&ddl]).await;
    let client = support::raw_client(&emu.host, db).await;
    support::execute_dml(&client, "INSERT INTO t (id, c) VALUES (1, 0.5), (2, 0.7)").await;

    let (first, bookmark) = read(&emu.host, db, query, None).await;
    assert_eq!(first, vec![1, 2]);
    assert_eq!(bookmark["value"], json!(0.7), "{bookmark}");

    support::execute_dml(&client, "INSERT INTO t (id, c) VALUES (3, 0.7), (4, 0.9)").await;
    let (second, _) = read(&emu.host, db, query, Some(&bookmark)).await;
    assert_eq!(
        second,
        vec![3, 4],
        "{column_type}: rows committed at the bookmark value"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn float32_cursor_keeps_rows_at_the_bookmark() {
    rows_at_the_bookmark_are_kept("f32", "FLOAT32", "SELECT id, c FROM t").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn float32_cursor_keeps_rows_at_a_user_bookmark() {
    rows_at_the_bookmark_are_kept(
        "f32user",
        "FLOAT32",
        "SELECT id, c FROM t WHERE c >= @bookmark",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn float64_cursor_keeps_rows_at_a_user_bookmark() {
    rows_at_the_bookmark_are_kept(
        "f64user",
        "FLOAT64",
        "SELECT id, c FROM t WHERE c >= @bookmark",
    )
    .await;
}
