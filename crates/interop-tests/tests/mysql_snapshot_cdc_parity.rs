//! A `faucet mirror` from MySQL reads a table once with the `mysql` query
//! source and then follows it with `mysql-cdc`; both must render the same
//! value identically, or a text destination holds two spellings of one row.
#![allow(clippy::disallowed_methods)] // tests drive raw clients directly

use faucet_core::Source;
use faucet_source_mysql::MysqlSource;
use faucet_source_mysql_cdc::MysqlCdcSource;
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use testcontainers::ImageExt;
use testcontainers_modules::mysql::Mysql;

const COLUMNS: [&str; 4] = ["d", "dt", "dt6", "ts"];

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_and_cdc_render_temporal_columns_identically() {
    let Some(container) = faucet_conformance::containers::start_or_skip(
        || {
            Mysql::default().with_cmd([
                "--server-id=1",
                "--log-bin=mysql-bin",
                "--binlog-format=ROW",
                "--binlog-row-image=FULL",
                "--binlog-row-metadata=FULL",
            ])
        },
        &Default::default(),
    )
    .await
    else {
        return;
    };
    let port = container.get_host_port_ipv4(3306).await.expect("port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    let pool = sqlx::MySqlPool::connect(&url).await.expect("connect");
    sqlx::query(
        "CREATE TABLE t (id INT PRIMARY KEY, d DATE, dt DATETIME, dt6 DATETIME(6), \
         ts TIMESTAMP(3) NULL)",
    )
    .execute(&pool)
    .await
    .unwrap();

    let cdc = MysqlCdcSource::new(
        serde_json::from_value(json!({
            "connection_url": url,
            "server_id": 1002,
            "idle_timeout": 3,
            "batch_size": 0,
            "include_tables": ["test.t"],
        }))
        .unwrap(),
    )
    .await
    .expect("cdc source");
    let start = cdc
        .capture_resume_position()
        .await
        .unwrap()
        .expect("position");
    sqlx::query(
        "INSERT INTO t VALUES (1, '2024-01-03', '2024-01-03 04:05:06', \
         '2024-01-03 04:05:06.123456', '2024-01-03 04:05:06.500')",
    )
    .execute(&pool)
    .await
    .unwrap();

    let snapshot = MysqlSource::new(
        serde_json::from_value(json!({
            "connection_url": url,
            "query": "SELECT * FROM t",
        }))
        .unwrap(),
    )
    .await
    .expect("source")
    .fetch_all()
    .await
    .expect("snapshot");

    cdc.apply_start_bookmark(start).await.unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = cdc.stream_pages(&ctx, 0);
    let mut changes = Vec::new();
    while let Some(page) = pages.next().await {
        changes.extend(page.expect("page").records);
    }
    let after = changes
        .iter()
        .find_map(|c| c.get("after").filter(|a| !a.is_null()))
        .expect("an insert event");
    for col in COLUMNS {
        assert_eq!(after[col], snapshot[0][col], "column {col}");
    }
}
