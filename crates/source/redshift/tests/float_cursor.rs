//! A `>=` cursor on a REAL column keeps the rows at the bookmark value
//! (#1114), against a Postgres container speaking the Redshift wire path.

use std::collections::HashMap;

use faucet_core::Source;
use faucet_source_redshift::{
    RedshiftConnection, RedshiftReplication, RedshiftSource, RedshiftSourceConfig,
};
use futures::StreamExt;
use serde_json::{Value, json};
use testcontainers::ImageExt;
use testcontainers_modules::postgres::Postgres;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn config(port: u16, query: &str) -> RedshiftSourceConfig {
    let mut conn = RedshiftConnection::new("127.0.0.1", "postgres", "postgres", "postgres");
    conn.port = port;
    conn.tls = false;
    RedshiftSourceConfig {
        connection: conn,
        query: query.into(),
        params: Vec::new(),
        max_connections: 2,
        batch_size: 0,
        replication: RedshiftReplication::Incremental {
            column: "c".into(),
            initial_value: json!(0.0),
        },
        state_key: None,
    }
}

async fn read(cfg: RedshiftSourceConfig, bookmark: Option<&Value>) -> (Vec<i64>, Value) {
    let source = RedshiftSource::new(cfg).expect("source");
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
    while let Some(page) = stream.next().await {
        let page = page.expect("page");
        ids.extend(page.records.iter().map(|r| r["id"].as_i64().unwrap()));
        if let Some(b) = page.bookmark {
            last = b;
        }
    }
    ids.sort_unstable();
    (ids, last)
}

async fn rows_at_the_bookmark_are_kept(query: &str) {
    let _serial = SERIAL.lock().await;
    let container =
        faucet_conformance::containers::start(|| Postgres::default().with_tag("16-alpine")).await;
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    let pool = sqlx::PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    ))
    .await
    .expect("connect");
    sqlx::raw_sql(
        "CREATE TABLE t (id BIGINT PRIMARY KEY, c REAL NOT NULL); \
         INSERT INTO t VALUES (1, 0.5), (2, 0.7);",
    )
    .execute(&pool)
    .await
    .expect("seed");

    let (first, bookmark) = read(config(port, query), None).await;
    assert_eq!(first, vec![1, 2]);
    assert_eq!(bookmark["value"], json!(0.7), "{bookmark}");

    sqlx::raw_sql("INSERT INTO t VALUES (3, 0.7), (4, 0.9)")
        .execute(&pool)
        .await
        .expect("late rows");
    let (second, _) = read(config(port, query), Some(&bookmark)).await;
    assert_eq!(second, vec![3, 4], "rows committed at the bookmark value");
}

#[tokio::test(flavor = "multi_thread")]
async fn real_cursor_keeps_rows_at_the_bookmark() {
    rows_at_the_bookmark_are_kept("SELECT id, c FROM t").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn real_cursor_keeps_rows_at_a_user_bookmark() {
    rows_at_the_bookmark_are_kept("SELECT id, c FROM t WHERE c >= ${bookmark}").await;
}
