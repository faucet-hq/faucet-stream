//! Integration tests for the MongoDB sink's `upsert` / `delete` write modes.
//!
//! Each test boots a fresh MongoDB container via testcontainers, drives one or
//! more `write_batch` calls in upsert/delete mode (with `key: ["_id"]`), and
//! reads the collection back to assert on the post-condition. Upserts are
//! committed via per-document `replace_one(upsert = true)` and deletes via
//! `delete_one`, so the observable invariants are the document count and the
//! contents of the surviving documents.

use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mongo::Mongo;

/// Start a MongoDB container and return both the container handle and a
/// connection URI. The container is kept alive by the returned handle.
async fn start_mongo() -> (ContainerAsync<Mongo>, String) {
    let container: ContainerAsync<Mongo> = Mongo::default()
        .start()
        .await
        .expect("mongo container start");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    let uri = format!("mongodb://127.0.0.1:{port}");
    (container, uri)
}

/// Build an upsert-mode config keyed on `_id`, with an optional delete marker.
fn upsert_config(uri: &str, delete_marker: Option<DeleteMarker>) -> MongoSinkConfig {
    let mut config = MongoSinkConfig::new(uri, "testdb", "docs");
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["_id".to_string()],
        delete_marker,
        rollback: None,
    };
    config
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

async fn find_one_by_id(uri: &str, db: &str, collection: &str, id: i64) -> Option<Document> {
    let client = Client::with_uri_str(uri).await.expect("client");
    client
        .database(db)
        .collection::<Document>(collection)
        .find_one(doc! { "_id": id })
        .await
        .expect("find_one")
}

// ---------------------------------------------------------------------------
// Test 1: upsert replaces an existing document in place (last-write-wins).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn upsert_replaces_existing_document() {
    let (_container, uri) = start_mongo().await;
    let sink = MongoSink::new(upsert_config(&uri, None))
        .await
        .expect("sink new");

    // Insert the initial document.
    sink.write_batch(&[serde_json::json!({"_id": 1, "name": "a"})])
        .await
        .expect("first write");
    assert_eq!(count_docs(&uri, "testdb", "docs").await, 1);
    let doc = find_one_by_id(&uri, "testdb", "docs", 1)
        .await
        .expect("doc present");
    assert_eq!(doc.get_str("name").unwrap(), "a");

    // Upsert the same _id with a new name — must replace in place, not duplicate.
    sink.write_batch(&[serde_json::json!({"_id": 1, "name": "b"})])
        .await
        .expect("second write");
    assert_eq!(
        count_docs(&uri, "testdb", "docs").await,
        1,
        "upsert must not create a second document for the same _id"
    );
    let doc = find_one_by_id(&uri, "testdb", "docs", 1)
        .await
        .expect("doc still present");
    assert_eq!(
        doc.get_str("name").unwrap(),
        "b",
        "the replacement document's name must win"
    );
}

// ---------------------------------------------------------------------------
// Test 2: delete_marker routes a delete-flagged row to delete_one; the upsert
// replacement that precedes it does NOT carry the marker field (plan_writes
// strips it).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn delete_marker_removes_document_and_strips_marker_from_upsert() {
    let (_container, uri) = start_mongo().await;
    let marker = DeleteMarker {
        field: "__op".to_string(),
        values: vec!["d".to_string()],
    };
    let sink = MongoSink::new(upsert_config(&uri, Some(marker)))
        .await
        .expect("sink new");

    // Upsert a document carrying the marker field with a non-delete value.
    sink.write_batch(&[serde_json::json!({"_id": 1, "name": "x", "__op": "u"})])
        .await
        .expect("upsert write");
    assert_eq!(count_docs(&uri, "testdb", "docs").await, 1);
    let doc = find_one_by_id(&uri, "testdb", "docs", 1)
        .await
        .expect("doc present");
    assert_eq!(doc.get_str("name").unwrap(), "x");
    assert!(
        !doc.contains_key("__op"),
        "the upsert replacement must not contain the stripped delete-marker field"
    );

    // Now write a delete-flagged row for the same _id — must remove it.
    sink.write_batch(&[serde_json::json!({"_id": 1, "__op": "d"})])
        .await
        .expect("delete write");
    assert_eq!(
        count_docs(&uri, "testdb", "docs").await,
        0,
        "the delete-marked row must remove the document"
    );
}

// ---------------------------------------------------------------------------
// Test 3: within a single batch, last-write-wins dedup collapses repeated keys
// to one document carrying the final value.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn intra_batch_last_write_wins() {
    let (_container, uri) = start_mongo().await;
    let sink = MongoSink::new(upsert_config(&uri, None))
        .await
        .expect("sink new");

    sink.write_batch(&[
        serde_json::json!({"_id": 1, "name": "old"}),
        serde_json::json!({"_id": 1, "name": "new"}),
    ])
    .await
    .expect("write");

    assert_eq!(
        count_docs(&uri, "testdb", "docs").await,
        1,
        "two rows with the same _id collapse to one document"
    );
    let doc = find_one_by_id(&uri, "testdb", "docs", 1)
        .await
        .expect("doc present");
    assert_eq!(
        doc.get_str("name").unwrap(),
        "new",
        "the last write in the batch must win"
    );
}

// ---------------------------------------------------------------------------
// Test 4: write_batch_partial routes missing-key rows to the DLQ per-row.
// The good row is upserted; only the missing-`_id` row comes back as Err.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn write_batch_partial_routes_missing_key_per_row() {
    let (_container, uri) = start_mongo().await;
    let sink = MongoSink::new(upsert_config(&uri, None))
        .await
        .expect("sink new");

    let records = [
        serde_json::json!({"_id": 1, "name": "ok"}),
        serde_json::json!({"name": "missing-id"}),
    ];
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("partial write");

    assert_eq!(outcomes.len(), 2, "one outcome per input row");
    assert!(outcomes[0].is_ok(), "the good row must be Ok");
    assert!(
        outcomes[1].is_err(),
        "the missing-key row must be Err (routed to the DLQ)"
    );

    assert_eq!(
        count_docs(&uri, "testdb", "docs").await,
        1,
        "only the good row should be written"
    );
    let doc = find_one_by_id(&uri, "testdb", "docs", 1)
        .await
        .expect("doc present");
    assert_eq!(
        doc.get_str("name").unwrap(),
        "ok",
        "_id=1 must be present with name 'ok'"
    );
}
