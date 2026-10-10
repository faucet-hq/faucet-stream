//! Integration tests for the MongoDB sink's `batch_size` re-chunking
//! behaviour. Each test boots a fresh MongoDB container via testcontainers,
//! drives a single `write_batch` call, and reads back the inserted
//! documents to assert on the chunking outcome.
//!
//! `insert_many` is a server-side operation, so we can't directly count the
//! number of outbound network calls from the driver without a proxy. We
//! instead assert on the observable post-condition (every document landed
//! in the collection) plus, where useful, on per-chunk effects: the
//! mongodb driver allocates a single `_id` per document and inserts each
//! chunk atomically (without bulk write ordering) — so total document
//! count plus contents preservation is the relevant invariant.

use faucet_core::Sink;
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use serde_json::{Value, json};
use testcontainers::ContainerAsync;
use testcontainers_modules::mongo::Mongo;

/// Start a MongoDB container and return both the container handle and a
/// connection URI. The container is kept alive by the returned handle.
async fn start_mongo() -> (ContainerAsync<Mongo>, String) {
    let container: ContainerAsync<Mongo> =
        faucet_conformance::containers::start(Mongo::default).await;
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    let uri = format!("mongodb://127.0.0.1:{port}");
    (container, uri)
}

fn make_records(n: usize) -> Vec<Value> {
    (0..n).map(|i| json!({"id": i, "name": "row"})).collect()
}

async fn count_docs(uri: &str, db: &str, collection: &str) -> u64 {
    let client = Client::with_uri_str(uri).await.expect("client");
    client
        .database(db)
        .collection::<Document>(collection)
        .count_documents(doc! {})
        .await
        .expect("count_documents")
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_rechunks_into_batch_size_inserts() {
    // 2500 records with batch_size = 1000 → 3 insert_many calls
    // (1000, 1000, 500), all 2500 docs land in the collection.
    let (_container, uri) = start_mongo().await;
    let config = MongoSinkConfig::new(&uri, "testdb", "events").with_batch_size(1000);
    let sink = MongoSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&make_records(2_500)).await.expect("write");
    assert_eq!(written, 2_500);
    assert_eq!(count_docs(&uri, "testdb", "events").await, 2_500);
}

#[tokio::test(flavor = "multi_thread")]
async fn unordered_insert_lands_valid_docs_despite_a_duplicate() {
    // Regression for #78/#20: the default is now unordered, so a single bad
    // document (here a duplicate `_id`) does not drop the rest of the batch.
    // Ordered would have committed `_id` 1 and 2, aborted on the duplicate,
    // and dropped `_id` 3 → 2 docs; unordered lands all 3 distinct ids.
    let (_container, uri) = start_mongo().await;
    let config = MongoSinkConfig::new(&uri, "testdb", "events"); // ordered = false (default)
    let sink = MongoSink::new(config).await.expect("sink new");

    let records = vec![
        json!({"_id": 1, "v": "a"}),
        json!({"_id": 2, "v": "b"}),
        json!({"_id": 2, "v": "c"}), // duplicate _id — only this one should fail
        json!({"_id": 3, "v": "d"}),
    ];
    // The batch surfaces the duplicate-key error, but the valid docs land.
    let _ = sink.write_batch(&records).await;
    assert_eq!(
        count_docs(&uri, "testdb", "events").await,
        3,
        "unordered insert must land the 3 distinct docs despite the duplicate _id"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_exact_multiple_of_batch_size() {
    // 1000 records with batch_size = 1000 → 1 insert_many call.
    let (_container, uri) = start_mongo().await;
    let config = MongoSinkConfig::new(&uri, "testdb", "events").with_batch_size(1000);
    let sink = MongoSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&make_records(1_000)).await.expect("write");
    assert_eq!(written, 1_000);
    assert_eq!(count_docs(&uri, "testdb", "events").await, 1_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_with_sentinel_zero_sends_single_insert() {
    // batch_size = 0 → pass-through: a single insert_many call regardless
    // of record count. We assert that all docs landed (sentinel doesn't
    // drop anything) and that re-chunking is disabled (the call did not
    // panic on a zero-sized chunk).
    let (_container, uri) = start_mongo().await;
    let config = MongoSinkConfig::new(&uri, "testdb", "events").with_batch_size(0);
    let sink = MongoSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&make_records(5_000)).await.expect("write");
    assert_eq!(written, 5_000);
    assert_eq!(count_docs(&uri, "testdb", "events").await, 5_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_empty_records_makes_no_inserts() {
    let (_container, uri) = start_mongo().await;
    let config = MongoSinkConfig::new(&uri, "testdb", "events").with_batch_size(1000);
    let sink = MongoSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&[]).await.expect("write");
    assert_eq!(written, 0);
    assert_eq!(count_docs(&uri, "testdb", "events").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_smaller_than_batch_size_inserts_once() {
    let (_container, uri) = start_mongo().await;
    let config = MongoSinkConfig::new(&uri, "testdb", "events").with_batch_size(1000);
    let sink = MongoSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&make_records(42)).await.expect("write");
    assert_eq!(written, 42);
    assert_eq!(count_docs(&uri, "testdb", "events").await, 42);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_preserves_document_contents_across_chunks() {
    // 250 docs with batch_size = 100 → 3 chunks (100, 100, 50). After the
    // write, every original `id` must be present in the collection exactly
    // once, regardless of which chunk it landed in.
    let (_container, uri) = start_mongo().await;
    let config = MongoSinkConfig::new(&uri, "testdb", "items").with_batch_size(100);
    let sink = MongoSink::new(config).await.expect("sink new");

    sink.write_batch(&make_records(250)).await.expect("write");

    let client = Client::with_uri_str(&uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>("items");

    assert_eq!(coll.count_documents(doc! {}).await.unwrap(), 250);

    // Spot-check the first, middle, and last id.
    for id in [0_i64, 99, 100, 199, 249] {
        let found = coll
            .find_one(doc! { "id": id })
            .await
            .expect("find_one")
            .unwrap_or_else(|| panic!("doc with id={id} not found"));
        assert_eq!(found.get_str("name").unwrap(), "row");
    }
}
