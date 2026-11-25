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

/// Values whose single precision lies above or below their decimal, at the
/// range ends, a subnormal and an integer beyond 2^24.
const VALUES: &[&str] = &[
    "0.1",
    "0.3",
    "-0.1",
    "-0.7",
    "7.038531e-26",
    "-7.038531e-26",
    "3.4e38",
    "16777217",
    "1.4e-45",
];

async fn read_from(
    host: &str,
    db: &str,
    query: &str,
    initial: Value,
    bookmark: Option<&Value>,
) -> (Vec<i64>, Value) {
    let mut config = SpannerSourceConfig::new(support::PROJECT, support::INSTANCE, db, query);
    config.connection = support::connection(host, db);
    config.replication = SpannerReplication::Incremental {
        column: "c".into(),
        initial_value: initial,
    };
    let source = SpannerSource::new(config).await.expect("source");
    if let Some(b) = bookmark {
        source
            .apply_start_bookmark(b.clone())
            .await
            .expect("bookmark");
    }
    let (rows, last) = source.fetch_all_incremental().await.expect("read");
    let mut ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    ids.sort_unstable();
    (ids, last.unwrap_or(Value::Null))
}

#[tokio::test(flavor = "multi_thread")]
async fn float32_cursor_keeps_tied_rows_for_values_on_both_sides_of_their_decimal() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    let ddl: Vec<String> = (0..VALUES.len())
        .map(|i| format!("CREATE TABLE v{i} (id INT64 NOT NULL, c FLOAT32) PRIMARY KEY (id)"))
        .collect();
    let ddl: Vec<&str> = ddl.iter().map(String::as_str).collect();
    support::create_database(&emu.host, "vals", &ddl).await;
    let client = support::raw_client(&emu.host, "vals").await;
    for (i, v) in VALUES.iter().enumerate() {
        let t = format!("v{i}");
        support::execute_dml(
            &client,
            &format!("INSERT INTO {t} (id, c) VALUES (0, -3e38), (1, {v}), (2, {v})"),
        )
        .await;
        for query in [
            format!("SELECT id, c FROM {t}"),
            format!("SELECT id, c FROM {t} WHERE c >= @bookmark"),
        ] {
            let (first, bookmark) =
                read_from(&emu.host, "vals", &query, json!(-3.3e38), None).await;
            assert_eq!(first, vec![0, 1, 2], "{v}: {query}");
            let (again, _) =
                read_from(&emu.host, "vals", &query, json!(-3.3e38), Some(&bookmark)).await;
            assert!(again.is_empty(), "{v}: {query}: {again:?}");
        }
        support::execute_dml(
            &client,
            &format!("INSERT INTO {t} (id, c) VALUES (3, {v}), (4, {v})"),
        )
        .await;
        let one = format!("SELECT id, c FROM {t} WHERE id = 1");
        let value = read_from(&emu.host, "vals", &one, json!(-3.3e38), None)
            .await
            .1["value"]
            .clone();
        let tied = json!({"value": value, "boundary": []});
        for query in [
            format!("SELECT id, c FROM {t}"),
            format!("SELECT id, c FROM {t} WHERE c >= @bookmark"),
        ] {
            let (second, _) =
                read_from(&emu.host, "vals", &query, json!(-3.3e38), Some(&tied)).await;
            assert_eq!(second, vec![1, 2, 3, 4], "{v}: {query}");
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_exact_spelling_and_a_double_token_keep_their_rows() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "spell",
        &[
            "CREATE TABLE e (id INT64 NOT NULL, c FLOAT32) PRIMARY KEY (id)",
            "CREATE TABLE dd (id INT64 NOT NULL, d FLOAT64) PRIMARY KEY (id)",
        ],
    )
    .await;
    let client = support::raw_client(&emu.host, "spell").await;
    support::execute_dml(
        &client,
        "INSERT INTO e (id, c) VALUES (1, 0.5), (2, 0.7), (3, 0.7)",
    )
    .await;
    let spelled = json!(0.699_999_988_079_071);
    let q = "SELECT id, c FROM e";
    assert_eq!(
        read_from(&emu.host, "spell", q, spelled.clone(), None)
            .await
            .0,
        vec![2, 3]
    );
    let stored = json!({"value": spelled, "boundary": []});
    assert_eq!(
        read_from(&emu.host, "spell", q, json!(0.0), Some(&stored))
            .await
            .0,
        vec![2, 3]
    );

    support::execute_dml(&client, "INSERT INTO dd (id, d) VALUES (1, 0.5), (2, 0.7)").await;
    let q = "SELECT id, CAST(d AS FLOAT32) AS c FROM dd WHERE d >= @bookmark";
    let (first, bookmark) = read_from(&emu.host, "spell", q, json!(0.0), None).await;
    assert_eq!(first, vec![1, 2]);
    support::execute_dml(&client, "INSERT INTO dd (id, d) VALUES (3, 0.7), (4, 0.9)").await;
    assert_eq!(
        read_from(&emu.host, "spell", q, json!(0.0), Some(&bookmark))
            .await
            .0,
        vec![3, 4]
    );
}
