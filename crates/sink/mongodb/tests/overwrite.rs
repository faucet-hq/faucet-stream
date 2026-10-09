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
use testcontainers::ContainerAsync;
use testcontainers_modules::mongo::Mongo;

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
    assert_eq!(
        sink.overwrite_staging_exists().await.unwrap(),
        Some(true),
        "the staging probe sees the in-flight staging"
    );
    sink.abort_overwrite().await.unwrap();
    assert_eq!(
        sink.overwrite_staging_exists().await.unwrap(),
        Some(false),
        "the staging probe sees it gone after abort"
    );

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

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_keeps_the_destination_indexes_and_options() {
    use futures::TryStreamExt;
    let (_c, uri) = start_mongo().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let db = client.database("testdb");
    db.run_command(doc! {
        "create": "docs",
        "validator": { "name": { "$type": "string" } },
        "collation": { "locale": "en", "strength": 2 },
    })
    .await
    .expect("create with options");
    db.run_command(doc! {
        "createIndexes": "docs",
        "indexes": [
            { "key": { "name": 1 }, "name": "name_unique", "unique": true },
            { "key": { "seen": 1 }, "name": "seen_ttl", "expireAfterSeconds": 3600 },
        ],
    })
    .await
    .expect("create indexes");
    insert_docs(&uri, "docs", vec![doc! {"_id": 1, "name": "old"}]).await;

    let sink = MongoSink::new(overwrite_config(&uri)).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[serde_json::json!({"_id": 10, "name": "new"})])
        .await
        .unwrap();
    sink.commit_overwrite().await.unwrap();
    assert_eq!(names(&uri, "docs").await, vec!["new"]);

    let indexes: Vec<Document> = db
        .collection::<Document>("docs")
        .list_indexes()
        .await
        .expect("list indexes")
        .try_collect::<Vec<_>>()
        .await
        .expect("collect")
        .into_iter()
        .map(|ix| mongodb::bson::to_document(&ix).unwrap())
        .collect();
    let by_name = |n: &str| indexes.iter().find(|ix| ix.get_str("name") == Ok(n));
    assert_eq!(
        by_name("name_unique").and_then(|ix| ix.get_bool("unique").ok()),
        Some(true)
    );
    assert!(by_name("seen_ttl").is_some(), "{indexes:?}");

    let spec = db
        .run_command(doc! { "listCollections": 1, "filter": { "name": "docs" } })
        .await
        .expect("listCollections");
    let options = spec
        .get_document("cursor")
        .unwrap()
        .get_array("firstBatch")
        .unwrap()[0]
        .as_document()
        .unwrap()
        .get_document("options")
        .unwrap()
        .clone();
    assert!(options.get_document("validator").is_ok(), "{options:?}");
    assert_eq!(
        options.get_document("collation").unwrap().get_str("locale"),
        Ok("en")
    );

    let dup = MongoSink::new(overwrite_config(&uri)).await.unwrap();
    dup.begin_overwrite().await.unwrap();
    assert!(
        dup.write_batch(&[
            serde_json::json!({"_id": 1, "name": "same"}),
            serde_json::json!({"_id": 2, "name": "SAME"}),
        ])
        .await
        .is_err(),
        "the unique index (with its collation) is enforced on staging"
    );
    dup.abort_overwrite().await.unwrap();
    assert_eq!(names(&uri, "docs").await, vec!["new"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn overwrite_first_run_creates_the_collection() {
    let (_c, uri) = start_mongo().await;
    let sink = MongoSink::new(overwrite_config(&uri)).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.write_batch(&[serde_json::json!({"_id": 1, "name": "first"})])
        .await
        .unwrap();
    sink.commit_overwrite().await.unwrap();
    assert_eq!(names(&uri, "docs").await, vec!["first"]);
    assert!(!collection_exists(&uri, "docs__faucet_ovw").await);
}

/// A first overwrite run whose source returns no rows still commits — an
/// empty collection — instead of failing `renameCollection` with
/// NamespaceNotFound every run (#789 MSG-52).
#[tokio::test(flavor = "multi_thread")]
async fn overwrite_first_run_with_no_rows_commits_an_empty_collection() {
    let (_c, uri) = start_mongo().await;
    let sink = MongoSink::new(overwrite_config(&uri)).await.unwrap();
    sink.begin_overwrite().await.unwrap();
    sink.commit_overwrite()
        .await
        .expect("an empty first run commits");
    assert!(collection_exists(&uri, "docs").await);
    assert!(names(&uri, "docs").await.is_empty());
}

/// In append mode a document the server rejects (a duplicate `_id`) fails
/// alone: the other documents are inserted, including those after it under
/// `ordered: true`, and re-running does not re-insert committed ones
/// (#789 MSG-44).
#[tokio::test(flavor = "multi_thread")]
async fn append_isolates_a_rejected_document_per_row() {
    let (_c, uri) = start_mongo().await;
    insert_docs(&uri, "docs", vec![doc! {"_id": 2, "name": "existing"}]).await;
    for ordered in [true, false] {
        let mut cfg = MongoSinkConfig::new(&uri, "testdb", "docs");
        cfg.ordered = ordered;
        cfg.batch_size = 2;
        let sink = MongoSink::new(cfg).await.unwrap();
        let base = if ordered { 10 } else { 20 };
        let records = vec![
            serde_json::json!({"_id": base + 1, "name": "a"}),
            serde_json::json!({"_id": 2, "name": "dup"}),
            serde_json::json!({"_id": base + 3, "name": "c"}),
            serde_json::json!("not an object"),
        ];
        let outcomes = sink.write_batch_partial(&records).await.unwrap();
        assert!(outcomes[0].is_ok() && outcomes[2].is_ok(), "{outcomes:?}");
        assert!(outcomes[1].is_err() && outcomes[3].is_err());
    }
    let mut got = names(&uri, "docs").await;
    got.sort();
    assert_eq!(got, vec!["a", "a", "c", "c", "existing"]);
}

/// The sink's catalog identity drops the URI's path and options, matching
/// the source's identity for the same collection (#789 MSG-92).
#[tokio::test(flavor = "multi_thread")]
async fn dataset_uri_is_host_database_collection() {
    let (_c, uri) = start_mongo().await;
    let sink = MongoSink::new(MongoSinkConfig::new(
        format!("{uri}/admin?appName=t"),
        "testdb",
        "docs",
    ))
    .await
    .unwrap();
    assert_eq!(sink.dataset_uri(), format!("{uri}/testdb/docs"));
}
