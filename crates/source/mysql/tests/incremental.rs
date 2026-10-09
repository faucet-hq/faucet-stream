//! Incremental replication (`replication: incremental`) against a real MySQL
//! via testcontainers (#825): first run from `initial_value`, resume from a
//! stored bookmark, rows sharing a cursor value across page boundaries, and a
//! crash that replays at most one page.

use faucet_core::{Source, StreamPage};
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;

async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let container = Mysql::default().start().await.expect("mysql start");
    let port = container.get_host_port_ipv4(3306).await.expect("port");
    (container, format!("mysql://root@127.0.0.1:{port}/test"))
}

async fn exec(url: &str, statements: &[&str]) {
    use sqlx::Connection;
    let mut conn = sqlx::MySqlConnection::connect(url).await.expect("connect");
    for sql in statements {
        sqlx::raw_sql(sql).execute(&mut conn).await.expect(sql);
    }
}

async fn pages(source: &MysqlSource) -> Vec<StreamPage> {
    let ctx = HashMap::new();
    let mut stream = source.stream_pages(&ctx, 0);
    let mut out = Vec::new();
    while let Some(page) = stream.next().await {
        out.push(page.expect("page"));
    }
    out
}

fn ids(pages: &[StreamPage]) -> Vec<i64> {
    pages
        .iter()
        .flat_map(|p| p.records.iter().map(|r| r["id"].as_i64().unwrap()))
        .collect()
}

fn sorted(mut v: Vec<i64>) -> Vec<i64> {
    v.sort_unstable();
    v
}

fn config(url: &str, query: &str, batch: usize) -> MysqlSourceConfig {
    MysqlSourceConfig::new(url, query)
        .with_batch_size(batch)
        .incremental("updated_at", json!("2024-01-01 00:00:00"))
}

async fn resumed(cfg: MysqlSourceConfig, bookmark: &Value) -> Vec<StreamPage> {
    let source = MysqlSource::new(cfg).await.expect("source");
    source
        .apply_start_bookmark(bookmark.clone())
        .await
        .expect("bookmark");
    pages(&source).await
}

const SEED: &[&str] = &[
    "CREATE TABLE items (id BIGINT PRIMARY KEY, updated_at DATETIME(6) NOT NULL, \
     changed TIMESTAMP NOT NULL, v VARCHAR(20), amount DECIMAL(12,2))",
    "INSERT INTO items VALUES \
      (1, '2023-12-31 00:00:00', '2023-12-31 00:00:00', 'old', 1.10), \
      (2, '2024-01-02 00:00:00', '2024-01-02 00:00:00', 'a', 2.20), \
      (3, '2024-01-03 00:00:00', '2024-01-03 00:00:00', 'b', 3.30), \
      (4, '2024-01-03 00:00:00', '2024-01-03 00:00:00', 'c', 4.40), \
      (5, '2024-01-03 00:00:00', '2024-01-03 00:00:00', 'd', 5.50), \
      (6, '2024-01-03 00:00:00', '2024-01-03 00:00:00', 'e', 6.60)",
];

#[tokio::test(flavor = "multi_thread")]
async fn incremental_reads_resume_without_skipping_boundary_rows() {
    let (_c, url) = start_mysql().await;
    exec(&url, SEED).await;
    let query = "SELECT id, updated_at, v, amount FROM items";

    let source = MysqlSource::new(config(&url, query, 2)).await.unwrap();
    let first = pages(&source).await;
    let got = ids(&first);
    assert_eq!(got[0], 2, "cursor order, initial_value excludes id 1");
    assert_eq!(sorted(got), vec![2, 3, 4, 5, 6]);
    assert!(first.iter().all(|p| p.bookmark.is_some()));
    let done = first.last().unwrap().bookmark.clone().unwrap();
    assert_eq!(done["value"], json!("2024-01-03T00:00:00+00:00"));
    assert_eq!(done["boundary"].as_array().unwrap().len(), 4);

    let again = resumed(config(&url, query, 2), &done).await;
    assert!(ids(&again).is_empty());
    assert_eq!(again.last().unwrap().bookmark.as_ref(), Some(&done));

    exec(
        &url,
        &[
            "INSERT INTO items VALUES (7, '2024-01-03 00:00:00', '2024-01-03 00:00:00', 'late', 7.70), \
             (8, '2024-01-04 00:00:00', '2024-01-04 00:00:00', 'new', 8.80)",
            "UPDATE items SET v = 'b2' WHERE id = 3",
        ],
    )
    .await;
    let third = resumed(config(&url, query, 2), &done).await;
    assert_eq!(sorted(ids(&third)), vec![3, 7, 8]);

    let one_row_pages = pages(&MysqlSource::new(config(&url, query, 1)).await.unwrap()).await;
    for (i, page) in one_row_pages.iter().enumerate() {
        let bookmark = page.bookmark.clone().unwrap();
        let replay = ids(&resumed(config(&url, query, 1), &bookmark).await);
        assert_eq!(
            sorted(replay),
            sorted(ids(&one_row_pages[i + 1..])),
            "resume after page {i}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bookmark_token_context_shards_and_digests() {
    let (_c, url) = start_mysql().await;
    exec(&url, SEED).await;

    // `@bookmark` inside the query, a TIMESTAMP cursor and a decimal value.
    let query = "SELECT id, changed, v FROM items WHERE changed >= @bookmark AND id <> 4";
    let mut cfg = MysqlSourceConfig::new(&url, query)
        .with_batch_size(0)
        .incremental("changed", json!("2024-01-01T00:00:00+00:00"));
    let source = MysqlSource::new(cfg.clone()).await.unwrap();
    let got = pages(&source).await;
    assert_eq!(got.len(), 1);
    assert_eq!(sorted(ids(&got)), vec![2, 3, 5, 6]);
    let bookmark = got[0].bookmark.clone().unwrap();
    let again = resumed(cfg.clone(), &bookmark).await;
    assert!(ids(&again).is_empty(), "an RFC 3339 bookmark binds back");

    let (records, last) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(records.len(), 4);
    assert!(last.is_some());
    assert_eq!(source.fetch_all().await.unwrap().len(), 4);

    cfg.shard = Some(faucet_source_mysql::ShardConfig { key: "id".into() });
    let sharded = MysqlSource::new(cfg).await.unwrap();
    let shards = sharded.enumerate_shards(2).await.unwrap();
    let mut all = Vec::new();
    for shard in &shards {
        sharded.apply_shard(shard).await.unwrap();
        all.extend(ids(&pages(&sharded).await));
    }
    assert_eq!(sorted(all), vec![2, 3, 5, 6]);
    let range = faucet_core::diff::KeyRange {
        lo: Some(0),
        hi: Some(100),
    };
    let digest = sharded
        .range_digest(&range, "id", &["v".to_string()])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(digest.rows, 4);

    let by_amount = MysqlSource::new(
        MysqlSourceConfig::new(&url, "SELECT id, amount FROM items")
            .incremental("amount", json!("5.50")),
    )
    .await
    .unwrap();
    assert_eq!(ids(&pages(&by_amount).await), vec![5, 6]);
}
