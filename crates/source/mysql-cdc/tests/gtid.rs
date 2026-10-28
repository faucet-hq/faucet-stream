//! GTID mode (#967): `start_position: gtid_set`, a `{gtid_set}` bookmark, and
//! GTID-set containment, against `mysql:8.1` with `--gtid-mode=ON
//! --enforce-gtid-consistency=ON` (the default on RDS, Aurora and Cloud SQL).
#![allow(clippy::disallowed_methods)] // tests drive raw clients directly

use faucet_core::Source;
use faucet_source_mysql_cdc::{MysqlCdcSource, MysqlCdcSourceConfig};
use futures::StreamExt;
use mysql_async::{Conn, Opts, prelude::Queryable};
use serde_json::{Value, json};
use std::collections::HashMap;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::mysql::Mysql;

async fn start_gtid_mysql() -> (ContainerAsync<Mysql>, String) {
    let container = faucet_conformance::containers::start(|| {
        Mysql::default().with_tag("8.1").with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-row-metadata=FULL",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
        ])
    })
    .await;
    let port = container.get_host_port_ipv4(3306).await.expect("port");
    (container, format!("mysql://root@127.0.0.1:{port}/test"))
}

async fn connect(url: &str) -> Conn {
    Conn::new(Opts::from_url(url).unwrap()).await.unwrap()
}

async fn executed(conn: &mut Conn) -> String {
    let set: String = conn
        .query_first("SELECT @@GLOBAL.gtid_executed")
        .await
        .unwrap()
        .unwrap();
    set.replace('\n', "")
}

fn config(url: &str, start: Value) -> MysqlCdcSourceConfig {
    serde_json::from_value(json!({
        "connection_url": url,
        "server_id": 1002,
        "start_position": start,
        "idle_timeout": 3,
        "batch_size": 0
    }))
    .unwrap()
}

async fn drain(source: &MysqlCdcSource) -> (Vec<i64>, Option<Value>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let (mut ids, mut bookmark) = (Vec::new(), None);
    while let Some(page) = pages.next().await {
        let page = page.expect("page");
        for r in &page.records {
            assert_eq!(r["op"], "c", "{r}");
            ids.push(r["after"]["id"].as_i64().unwrap());
        }
        if page.bookmark.is_some() {
            bookmark = page.bookmark;
        }
    }
    (ids, bookmark)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gtid_start_bookmark_and_containment_follow_the_executed_set() {
    let (_c, url) = start_gtid_mysql().await;
    let mut conn = connect(&url).await;
    conn.query_drop("CREATE TABLE test.t (id INT PRIMARY KEY)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test.t VALUES (1), (2)")
        .await
        .unwrap();
    let before = executed(&mut conn).await;
    conn.query_drop("INSERT INTO test.t VALUES (3)")
        .await
        .unwrap();
    conn.query_drop("INSERT INTO test.t VALUES (4)")
        .await
        .unwrap();
    let after = executed(&mut conn).await;
    assert_ne!(before, after);

    // Only transactions outside the start set are streamed.
    let source = MysqlCdcSource::new(config(&url, json!({"type": "gtid_set", "value": before})))
        .await
        .expect("gtid source");
    let (ids, bookmark) = drain(&source).await;
    assert_eq!(ids, vec![3, 4]);
    let bookmark = bookmark.expect("bookmark");
    assert!(
        bookmark.get("file").is_some(),
        "sessions resume by file/pos: {bookmark}"
    );

    // A persisted `{gtid_set}` bookmark wins over the configured start.
    conn.query_drop("INSERT INTO test.t VALUES (5)")
        .await
        .unwrap();
    let resumed = MysqlCdcSource::new(config(&url, json!({"type": "current"})))
        .await
        .unwrap();
    resumed
        .apply_start_bookmark(json!({ "gtid_set": after }))
        .await
        .unwrap();
    assert_eq!(drain(&resumed).await.0, vec![5]);

    // Containment over the server's own GTID sets.
    let (b, a) = (json!({"gtid_set": before}), json!({"gtid_set": after}));
    assert_eq!(source.position_le(&b, &a), Some(true));
    assert_eq!(source.position_le(&a, &b), Some(false));
    assert_eq!(source.position_le(&a, &a), Some(true));
    assert_eq!(
        source.position_min(&[a.clone(), b.clone()]),
        Some(b.clone())
    );
    // A row's position is binlog coordinates, which a GTID set does not order.
    let row = json!({"op": "c", "lsn": bookmark});
    assert_eq!(source.record_covered(&row, &a), None);
    assert_eq!(source.record_covered(&row, &bookmark), Some(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_gtid_start_is_refused_when_gtid_mode_is_off() {
    let container = faucet_conformance::containers::start(|| {
        Mysql::default().with_tag("8.1").with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-row-metadata=FULL",
        ])
    })
    .await;
    let port = container.get_host_port_ipv4(3306).await.unwrap();
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    let err = MysqlCdcSource::new(config(
        &url,
        json!({"type": "gtid_set", "value": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"}),
    ))
    .await
    .err()
    .expect("refused")
    .to_string();
    assert!(err.contains("gtid_mode is 'OFF'"), "{err}");
}
