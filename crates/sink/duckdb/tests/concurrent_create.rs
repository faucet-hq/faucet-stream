//! #990: sinks sharing one database instance that create (or widen) the same
//! table at the same moment both land their rows, instead of one failing with
//! DuckDB's catalog write-write conflict.

use faucet_core::Sink;
use faucet_sink_duckdb::{DuckdbColumnMapping, DuckdbSink, DuckdbSinkConfig};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::Barrier;

const ROUNDS: usize = 40;

fn cfg(path: &str, table: &str) -> DuckdbSinkConfig {
    DuckdbSinkConfig::new(path, table).column_mapping(DuckdbColumnMapping::AutoMap)
}

fn count(path: &str, table: &str) -> i64 {
    let conn = duckdb::Connection::open(path).expect("reopen");
    conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .expect("count")
}

fn columns(path: &str, table: &str) -> Vec<String> {
    let conn = duckdb::Connection::open(path).expect("reopen");
    let mut stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_name = ? ORDER BY column_name",
        )
        .expect("prepare");
    stmt.query_map([table], |r| r.get::<_, String>(0))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("decode")
}

/// Build `writers` sinks on one file and have them all write their page at the
/// same moment.
async fn race(path: &str, pages: Vec<Vec<Value>>) {
    let barrier = Arc::new(Barrier::new(pages.len()));
    let mut sinks = Vec::new();
    for _ in &pages {
        sinks.push(Arc::new(
            DuckdbSink::new(cfg(path, "prices")).await.expect("sink"),
        ));
    }
    let tasks: Vec<_> = sinks
        .iter()
        .cloned()
        .zip(pages)
        .map(|(sink, page)| {
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                sink.write_batch(&page).await
            })
        })
        .collect();
    for (i, t) in tasks.into_iter().enumerate() {
        let n = t
            .await
            .expect("join")
            .unwrap_or_else(|e| panic!("writer {i}: {e}"));
        assert_eq!(n, 1, "writer {i} wrote its row");
    }
    for sink in &sinks {
        sink.flush().await.expect("flush");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sinks_creating_one_table_both_land_their_rows() {
    for round in 0..ROUNDS {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.duckdb");
        let path = path.to_str().expect("utf-8 path").to_string();
        race(
            &path,
            vec![
                vec![json!({"id": 1, "amount": 12.5})],
                vec![json!({"id": 2, "amount": 7.25})],
            ],
        )
        .await;
        assert_eq!(count(&path, "prices"), 2, "round {round}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sinks_widening_one_table_both_land_their_rows() {
    for round in 0..ROUNDS {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.duckdb");
        let path = path.to_str().expect("utf-8 path").to_string();
        // Different shapes: whichever creates the table second adds its column.
        race(
            &path,
            vec![
                vec![json!({"id": 1, "a": "x"})],
                vec![json!({"id": 2, "b": "y"})],
            ],
        )
        .await;
        assert_eq!(count(&path, "prices"), 2, "round {round}");
        assert_eq!(columns(&path, "prices"), ["a", "b", "id"], "round {round}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_existing_table_with_a_new_column_raced_by_two_sinks() {
    for round in 0..ROUNDS {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.duckdb");
        let path = path.to_str().expect("utf-8 path").to_string();
        duckdb::Connection::open(&path)
            .expect("open")
            .execute_batch("CREATE TABLE prices (id BIGINT)")
            .expect("create");
        // Both add the same column at once.
        race(
            &path,
            vec![
                vec![json!({"id": 1, "note": "x"})],
                vec![json!({"id": 2, "note": "y"})],
            ],
        )
        .await;
        assert_eq!(count(&path, "prices"), 2, "round {round}");
        assert_eq!(columns(&path, "prices"), ["id", "note"], "round {round}");
    }
}
