//! Incremental replication (`replication: incremental`) against a real
//! Postgres via testcontainers (#825): first run from `initial_value`, resume
//! from a stored bookmark, rows sharing a cursor value across page
//! boundaries, and a crash that replays at most one page.

use faucet_core::{Source, StreamPage};
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("postgres container start");
    let port = container.get_host_port_ipv4(5432).await.expect("port");
    (
        container,
        format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
    )
}

async fn exec(url: &str, sql: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("connect");
    sqlx::raw_sql(sql).execute(&pool).await.expect(sql);
    pool.close().await;
}

async fn pages(source: &PostgresSource) -> Vec<StreamPage> {
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

fn config(url: &str, query: &str, batch: usize) -> PostgresSourceConfig {
    PostgresSourceConfig::new(url, query)
        .with_batch_size(batch)
        .incremental("updated_at", json!("2024-01-01T00:00:00Z"))
}

async fn resumed(cfg: PostgresSourceConfig, bookmark: &Value) -> Vec<StreamPage> {
    let source = PostgresSource::new(cfg).await.expect("source");
    source
        .apply_start_bookmark(bookmark.clone())
        .await
        .expect("bookmark");
    pages(&source).await
}

const SEED: &str = "
CREATE TABLE items (id BIGINT PRIMARY KEY, updated_at TIMESTAMPTZ NOT NULL, v TEXT, amount NUMERIC(12,2));
INSERT INTO items VALUES
  (1, '2023-12-31T00:00:00Z', 'old', 1.10),
  (2, '2024-01-02T00:00:00Z', 'a', 2.20),
  (3, '2024-01-03T00:00:00Z', 'b', 3.30),
  (4, '2024-01-03T00:00:00Z', 'c', 4.40),
  (5, '2024-01-03T00:00:00Z', 'd', 5.50),
  (6, '2024-01-03T00:00:00Z', 'e', 6.60);
";

#[tokio::test(flavor = "multi_thread")]
async fn incremental_reads_resume_without_skipping_boundary_rows() {
    let (_c, url) = start_postgres().await;
    exec(&url, SEED).await;
    let query = "SELECT id, updated_at, v, amount FROM items";

    // First run: from initial_value, in cursor order, a bookmark on every page.
    let source = PostgresSource::new(config(&url, query, 2)).await.unwrap();
    let first = pages(&source).await;
    let mut got = ids(&first);
    assert_eq!(
        got.remove(0),
        2,
        "cursor order, initial_value excludes id 1"
    );
    got.sort_unstable();
    assert_eq!(got, vec![3, 4, 5, 6]);
    assert!(first.iter().all(|p| p.bookmark.is_some()));
    assert_eq!(
        first[0].records[0]["amount"],
        json!("2.20"),
        "numeric read as text"
    );
    let done = first.last().unwrap().bookmark.clone().unwrap();
    assert_eq!(done["value"], json!("2024-01-03T00:00:00+00:00"));
    assert_eq!(done["boundary"].as_array().unwrap().len(), 4);

    // Nothing changed: a resumed run emits nothing and keeps its position.
    let again = resumed(config(&url, query, 2), &done).await;
    assert!(ids(&again).is_empty());
    assert_eq!(again.last().unwrap().bookmark.as_ref(), Some(&done));

    // A row committed late at the boundary value, a boundary row updated in
    // place without its cursor moving, and a newer row are all read.
    exec(
        &url,
        "INSERT INTO items VALUES (7, '2024-01-03T00:00:00Z', 'late', 7.70), \
                                  (8, '2024-01-04T00:00:00Z', 'new', 8.80); \
         UPDATE items SET v = 'b2' WHERE id = 3;",
    )
    .await;
    let third = resumed(config(&url, query, 2), &done).await;
    let mut got = ids(&third);
    got.sort_unstable();
    assert_eq!(got, vec![3, 7, 8]);
    let last = third.last().unwrap().bookmark.clone().unwrap();
    assert_eq!(last["value"], json!("2024-01-04T00:00:00+00:00"));

    // A crash after the first page: resuming from that page's bookmark
    // re-reads only what the lost page held.
    let one_row_pages = pages(&PostgresSource::new(config(&url, query, 1)).await.unwrap()).await;
    for (i, page) in one_row_pages.iter().enumerate() {
        let bookmark = page.bookmark.clone().unwrap();
        let replay = ids(&resumed(config(&url, query, 1), &bookmark).await);
        let expected: Vec<i64> = ids(&one_row_pages[i + 1..]);
        let mut replay_sorted = replay.clone();
        replay_sorted.sort_unstable();
        let mut expected_sorted = expected.clone();
        expected_sorted.sort_unstable();
        assert_eq!(replay_sorted, expected_sorted, "resume after page {i}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn bookmark_token_context_shards_and_digests() {
    let (_c, url) = start_postgres().await;
    exec(&url, SEED).await;

    // `${bookmark}` inside the query, next to a configured param.
    let query = "SELECT id, updated_at, v FROM items \
                 WHERE updated_at >= ${bookmark} AND id <> $1";
    let mut cfg = config(&url, query, 0);
    cfg.params = vec![json!(4)];
    let source = PostgresSource::new(cfg.clone()).await.unwrap();
    let got = pages(&source).await;
    assert_eq!(got.len(), 1);
    let mut got = ids(&got);
    got.sort_unstable();
    assert_eq!(got, vec![2, 3, 5, 6]);

    let (records, bookmark) = source.fetch_all_incremental().await.unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(
        bookmark.unwrap()["value"],
        json!("2024-01-03T00:00:00+00:00")
    );
    assert_eq!(source.fetch_all().await.unwrap().len(), 4);

    // Sharded incremental reads bind the bookmark in the bounds query.
    cfg.shard = Some(faucet_source_postgres::ShardConfig { key: "id".into() });
    let sharded = PostgresSource::new(cfg).await.unwrap();
    let shards = sharded.enumerate_shards(2).await.unwrap();
    assert!(!shards.is_empty());
    let mut all = Vec::new();
    for shard in &shards {
        sharded.apply_shard(shard).await.unwrap();
        all.extend(ids(&pages(&sharded).await));
    }
    all.sort_unstable();
    assert_eq!(all, vec![2, 3, 5, 6]);

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

    // An integer cursor binds as a number; a missing column is a config error.
    let by_id = PostgresSource::new(
        PostgresSourceConfig::new(&url, "SELECT id, v FROM items").incremental("id", json!(5)),
    )
    .await
    .unwrap();
    assert_eq!(ids(&pages(&by_id).await), vec![5, 6]);

    let missing = PostgresSource::new(
        PostgresSourceConfig::new(&url, "SELECT id FROM items").incremental("nope", json!(0)),
    )
    .await
    .unwrap();
    let err = missing.fetch_all().await.unwrap_err();
    assert!(
        matches!(err, faucet_core::FaucetError::Config(_)) && err.to_string().contains("nope"),
        "{err}"
    );
}
