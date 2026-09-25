//! Integration tests for [`PostgresSink`]'s scoped-cleanup path (#478) against a
//! real Postgres instance via testcontainers.
//!
//! The SQL generation is unit-tested in the crate, but the parts that only a real
//! server exercises are the ones most likely to be wrong: whether the temp table
//! is actually created and dropped, whether the `NOT EXISTS` join matches across
//! real column types, whether the transaction is genuinely all-or-nothing, and
//! whether the scope predicate really confines the delete. Those are what these
//! cover.
//!
//! Require Docker. Each test boots its own container, so they are isolated and
//! parallel-safe.

use faucet_core::{CleanupPolicy, Sink, WriteMode, WriteSpec};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default().with_tag("16-alpine");
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// `assoc(id, contact_id, label)` — the shape the feature was built for: a child
/// table whose rows belong to a parent scope.
async fn create_assoc_table(url: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE assoc (id INT PRIMARY KEY, contact_id INT, label TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    // Contact 7 has three rows; contact 8 has two. Contact 8 must never be
    // touched — that is what makes the delete *scoped* rather than a truncate.
    for (id, contact, label) in [
        (1, 7, "stale-a"),
        (2, 7, "stale-b"),
        (3, 7, "keep-me"),
        (10, 8, "other"),
        (11, 8, "other-2"),
    ] {
        sqlx::query("INSERT INTO assoc (id, contact_id, label) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(contact)
            .bind(label)
            .execute(&pool)
            .await
            .expect("seed");
    }
    pool.close().await;
}

async fn ids(url: &str) -> Vec<(i32, i32)> {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let rows: Vec<(i32, i32)> = sqlx::query_as("SELECT id, contact_id FROM assoc ORDER BY id")
        .fetch_all(&pool)
        .await
        .expect("read back");
    pool.close().await;
    rows
}

/// Whether a table exists in any schema — used to prove the temp table does not
/// leak past its transaction.
async fn table_exists(url: &str, name: &str) -> bool {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let found: Option<i32> = sqlx::query_scalar("SELECT 1 FROM pg_class WHERE relname = $1")
        .bind(name)
        .fetch_optional(&pool)
        .await
        .expect("lookup");
    pool.close().await;
    found.is_some()
}

fn sink_config(url: &str) -> PostgresSinkConfig {
    let mut config =
        PostgresSinkConfig::new(url, "assoc").column_mapping(PostgresColumnMapping::AutoMap);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
        rollback: None,
    };
    config
}

fn scope(contact_id: i64) -> BTreeMap<String, Value> {
    BTreeMap::from([("contact_id".to_string(), json!(contact_id))])
}

fn policy(contact_id: i64, max_keys: usize) -> CleanupPolicy {
    CleanupPolicy::new(scope(contact_id), vec!["id".to_string()], max_keys).expect("policy")
}

/// Drive a page through the sink, then run the cleanup with exactly that page's
/// keys — the sequence `run_stream` performs.
async fn write_then_cleanup(sink: &PostgresSink, page: &[Value], contact_id: i64) -> u64 {
    let p = policy(contact_id, 1000);
    if !page.is_empty() {
        sink.write_batch(page).await.expect("write");
    }
    let mut seen = faucet_core::SeenKeys::new();
    seen.record_page(page, &p.key, p.max_keys);
    sink.cleanup_scope(&p.scope, &seen).await.expect("cleanup")
}

#[tokio::test]
async fn deletes_only_unwritten_rows_inside_the_scope() {
    let (_c, url) = start_postgres().await;
    create_assoc_table(&url).await;
    let sink = PostgresSink::new(sink_config(&url)).await.expect("sink");

    // The source now reports only id=3 for contact 7 — ids 1 and 2 were deleted
    // upstream, which an incremental upsert alone could never notice.
    let deleted = write_then_cleanup(
        &sink,
        &[json!({"id": 3, "contact_id": 7, "label": "keep-me"})],
        7,
    )
    .await;

    assert_eq!(deleted, 2, "exactly the two stale rows");
    assert_eq!(
        ids(&url).await,
        vec![(3, 7), (10, 8), (11, 8)],
        "the written row survives and contact 8 is untouched"
    );
}

#[tokio::test]
async fn an_empty_page_clears_the_whole_scope_but_nothing_else() {
    // The motivating case: every association for contact 7 was removed upstream,
    // so the fetch returns nothing. Upsert alone writes nothing and the stale
    // rows live forever.
    let (_c, url) = start_postgres().await;
    create_assoc_table(&url).await;
    let sink = PostgresSink::new(sink_config(&url)).await.expect("sink");

    let deleted = write_then_cleanup(&sink, &[], 7).await;

    assert_eq!(deleted, 3, "all three of contact 7's rows");
    assert_eq!(
        ids(&url).await,
        vec![(10, 8), (11, 8)],
        "contact 8's rows must survive an empty page for contact 7"
    );
}

#[tokio::test]
async fn a_scope_with_no_stale_rows_deletes_nothing() {
    // The steady state a healthy mirror shows: everything in the scope was
    // written this run, so there is nothing to remove.
    let (_c, url) = start_postgres().await;
    create_assoc_table(&url).await;
    let sink = PostgresSink::new(sink_config(&url)).await.expect("sink");

    let deleted = write_then_cleanup(
        &sink,
        &[
            json!({"id": 1, "contact_id": 7, "label": "a"}),
            json!({"id": 2, "contact_id": 7, "label": "b"}),
            json!({"id": 3, "contact_id": 7, "label": "c"}),
        ],
        7,
    )
    .await;

    assert_eq!(deleted, 0);
    assert_eq!(ids(&url).await.len(), 5, "nothing removed");
}

#[tokio::test]
async fn the_temp_table_does_not_outlive_its_transaction() {
    // `ON COMMIT DROP` is what lets concurrent cleanups on other pooled
    // connections use the same table name without colliding. If it leaked, the
    // second cleanup on a reused connection would fail on "already exists".
    let (_c, url) = start_postgres().await;
    create_assoc_table(&url).await;
    let sink = PostgresSink::new(sink_config(&url)).await.expect("sink");

    write_then_cleanup(&sink, &[json!({"id": 3, "contact_id": 7, "label": "x"})], 7).await;
    assert!(
        !table_exists(&url, "faucet_cleanup_keys").await,
        "the temp table must be gone after the transaction commits"
    );

    // Running a second cleanup on the same pool proves it in the way that
    // matters: a leaked table would make this fail.
    let deleted = write_then_cleanup(&sink, &[], 8).await;
    assert_eq!(deleted, 2, "contact 8's rows, on a reused connection");
}

#[tokio::test]
async fn a_scope_column_that_does_not_exist_is_a_clear_error() {
    let (_c, url) = start_postgres().await;
    create_assoc_table(&url).await;
    let sink = PostgresSink::new(sink_config(&url)).await.expect("sink");

    let bad = CleanupPolicy::new(
        BTreeMap::from([("contactid".to_string(), json!(7))]), // typo
        vec!["id".to_string()],
        1000,
    )
    .unwrap();
    let err = sink
        .cleanup_scope(&bad.scope, &faucet_core::SeenKeys::new())
        .await
        .expect_err("a non-existent column must be refused");
    let msg = err.to_string();
    assert!(msg.contains("contactid"), "names the column: {msg}");
    assert!(msg.contains("assoc"), "names the table: {msg}");
    assert_eq!(
        ids(&url).await.len(),
        5,
        "nothing deleted on the error path"
    );
}

#[tokio::test]
async fn a_large_key_set_is_loaded_in_chunks_and_still_matches() {
    // The written-key set is loaded into the temp table in parameter-bounded
    // chunks. This crosses that boundary in miniature: many keys, only some of
    // which exist, with a stale row that must still be found.
    let (_c, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    sqlx::query("CREATE TABLE assoc (id INT PRIMARY KEY, contact_id INT, label TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    for id in 0..500 {
        sqlx::query("INSERT INTO assoc (id, contact_id, label) VALUES ($1, 7, 'x')")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
    }
    pool.close().await;

    let sink = PostgresSink::new(sink_config(&url)).await.expect("sink");
    // Re-write every even id; the odd ones become stale.
    let page: Vec<Value> = (0..500)
        .filter(|i| i % 2 == 0)
        .map(|i| json!({"id": i, "contact_id": 7, "label": "kept"}))
        .collect();
    let deleted = write_then_cleanup(&sink, &page, 7).await;

    assert_eq!(deleted, 250, "every odd id was stale");
    let remaining = ids(&url).await;
    assert_eq!(remaining.len(), 250);
    assert!(
        remaining.iter().all(|(id, _)| id % 2 == 0),
        "only the written (even) ids survive"
    );
}
