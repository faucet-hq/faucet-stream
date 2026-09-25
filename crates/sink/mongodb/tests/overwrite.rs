//! Integration tests for the MongoDB sink's `write_mode: overwrite` (#492).
//!
//! Each test boots a fresh MongoDB container. The lifecycle the pipeline drives
//! is `begin_overwrite` → N × `write_batch` (inserts into a staging collection)
//! → `commit_overwrite` (atomic `renameCollection … dropTarget:true`) on
//! success, or `abort_overwrite` on failure/cancel. The guarantees under test:
//! writes stage until the swap, a successful commit fully replaces the target,
//! and an abort leaves the previous target completely intact.

use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mongo::Mongo;

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

fn overwrite_config(uri: &str) -> MongoSinkConfig {
    let mut config = MongoSinkConfig::new(uri, "testdb", "docs");
    config.write = WriteSpec {
        write_mode: WriteMode::Overwrite,
        key: vec![],
        delete_marker: None,
        rollback: None,
    };
    config
}

async fn insert_docs(uri: &str, coll: &str, docs: Vec<Document>) {
    let client = Client::with_uri_str(uri).await.expect("client");
    client
        .database("testdb")
        .collection::<Document>(coll)
        .insert_many(docs)
        .await
        .expect("seed insert");
}

async fn names(uri: &str, coll: &str) -> Vec<String> {
    use futures::TryStreamExt;
    let client = Client::with_uri_str(uri).await.expect("client");
    let cursor = client
        .database("testdb")
        .collection::<Document>(coll)
        .find(doc! {})
        .sort(doc! { "_id": 1 })
        .await
        .expect("find");
    let docs: Vec<Document> = cursor.try_collect().await.expect("collect");
    docs.iter()
        .map(|d| d.get_str("name").unwrap().to_string())
        .collect()
}

async fn collection_exists(uri: &str, coll: &str) -> bool {
    let client = Client::with_uri_str(uri).await.expect("client");
    let names = client
        .database("testdb")
        .list_collection_names()
        .await
        .expect("list collections");
    names.iter().any(|n| n == coll)
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_replaces_collection_on_commit() {
    let (_c, uri) = start_mongo().await;
    insert_docs(
        &uri,
        "docs",
        vec![
            doc! {"_id": 1, "name": "old_a"},
            doc! {"_id": 2, "name": "old_b"},
        ],
    )
    .await;

    let sink = MongoSink::new(overwrite_config(&uri)).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[serde_json::json!({"_id": 10, "name": "new_x"})])
        .await
        .unwrap();
    sink.write_batch(&[serde_json::json!({"_id": 11, "name": "new_y"})])
        .await
        .unwrap();

    // Staged: the destination still shows the old docs.
    assert_eq!(names(&uri, "docs").await, vec!["old_a", "old_b"]);

    sink.commit_overwrite().await.unwrap();

    assert_eq!(names(&uri, "docs").await, vec!["new_x", "new_y"]);
    assert!(
        !collection_exists(&uri, "docs__faucet_ovw").await,
        "staging collection must be gone after the rename swap"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_abort_leaves_collection_intact() {
    let (_c, uri) = start_mongo().await;
    insert_docs(
        &uri,
        "docs",
        vec![
            doc! {"_id": 1, "name": "old_a"},
            doc! {"_id": 2, "name": "old_b"},
        ],
    )
    .await;

    let sink = MongoSink::new(overwrite_config(&uri)).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[serde_json::json!({"_id": 99, "name": "doomed"})])
        .await
        .unwrap();
    sink.abort_overwrite().await.unwrap();

    assert_eq!(names(&uri, "docs").await, vec!["old_a", "old_b"]);
    assert!(!collection_exists(&uri, "docs__faucet_ovw").await);
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_in_supported_write_modes() {
    let (_c, uri) = start_mongo().await;
    let sink = MongoSink::new(overwrite_config(&uri)).await.unwrap();
    assert!(sink.supported_write_modes().contains(&WriteMode::Overwrite));
    assert!(sink.is_overwrite());
}
