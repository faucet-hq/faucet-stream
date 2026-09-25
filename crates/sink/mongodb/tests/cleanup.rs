//! Integration tests for the MongoDB sink's scoped-cleanup path (#478).
//!
//! The filter construction is unit-tested in the crate; what only a real server
//! shows is whether that filter actually selects the right documents. The
//! important one is the `$and` wrapping: a scope field can also be a key field,
//! and as sibling keys the duplicate entry would drop the scope predicate and
//! widen the delete from one parent's documents to **the whole collection**.
//!
//! Requires Docker. Each test boots its own container. Note the cleanup path
//! uses a single `delete_many` with no session, so unlike the exactly-once tests
//! these need no replica set.

use faucet_core::{CleanupPolicy, Sink, WriteMode, WriteSpec};
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use serde_json::{Value, json};
use std::collections::BTreeMap;
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
    (container, format!("mongodb://127.0.0.1:{port}"))
}

fn config(uri: &str, key: Vec<String>) -> MongoSinkConfig {
    let mut config = MongoSinkConfig::new(uri, "testdb", "assoc");
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key,
        delete_marker: None,
        rollback: None,
    };
    config
}

/// Contact 7 owns three documents, contact 8 owns two. Contact 8 must never be
/// touched — that is what makes the delete scoped rather than a collection wipe.
async fn seed(uri: &str) {
    let client = Client::with_uri_str(uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>("assoc");
    for (id, contact) in [(1, 7), (2, 7), (3, 7), (10, 8), (11, 8)] {
        coll.insert_one(doc! { "_id": id, "contact_id": contact, "label": "seed" })
            .await
            .expect("seed");
    }
}

async fn remaining(uri: &str) -> Vec<i64> {
    use futures::TryStreamExt;
    let client = Client::with_uri_str(uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>("assoc");
    let docs: Vec<Document> = coll
        .find(doc! {})
        .await
        .expect("find")
        .try_collect()
        .await
        .expect("collect");
    // Read `_id` type-agnostically: seeded docs are Int32, but a doc written
    // through the sink round-trips from JSON as Int64. A helper that only read
    // one of the two would silently drop exactly the rows under test.
    let mut ids: Vec<i64> = docs
        .iter()
        .map(|d| match d.get("_id") {
            Some(mongodb::bson::Bson::Int32(v)) => i64::from(*v),
            Some(mongodb::bson::Bson::Int64(v)) => *v,
            other => panic!("unexpected _id type: {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    ids
}

async fn write_then_cleanup(
    sink: &MongoSink,
    page: &[Value],
    scope: BTreeMap<String, Value>,
    key: Vec<String>,
) -> u64 {
    let p = CleanupPolicy::new(scope, key, 1000).expect("policy");
    if !page.is_empty() {
        sink.write_batch(page).await.expect("write");
    }
    let mut seen = faucet_core::SeenKeys::new();
    seen.record_page(page, &p.key, p.max_keys);
    sink.cleanup_scope(&p.scope, &seen).await.expect("cleanup")
}

fn scope7() -> BTreeMap<String, Value> {
    BTreeMap::from([("contact_id".to_string(), json!(7))])
}

#[tokio::test]
async fn deletes_only_unwritten_documents_inside_the_scope() {
    let (_c, uri) = start_mongo().await;
    seed(&uri).await;
    let sink = MongoSink::new(config(&uri, vec!["_id".into()]))
        .await
        .expect("sink");

    let deleted = write_then_cleanup(
        &sink,
        &[json!({"_id": 3, "contact_id": 7, "label": "kept"})],
        scope7(),
        vec!["_id".into()],
    )
    .await;

    assert_eq!(deleted, 2, "the two stale documents");
    assert_eq!(
        remaining(&uri).await,
        vec![3, 10, 11],
        "the written doc survives and contact 8 is untouched"
    );
}

#[tokio::test]
async fn an_empty_page_clears_the_scope_and_only_the_scope() {
    // The motivating case: contact 7's documents were all removed upstream, so
    // the fetch returns nothing.
    let (_c, uri) = start_mongo().await;
    seed(&uri).await;
    let sink = MongoSink::new(config(&uri, vec!["_id".into()]))
        .await
        .expect("sink");

    let deleted = write_then_cleanup(&sink, &[], scope7(), vec!["_id".into()]).await;

    assert_eq!(deleted, 3);
    assert_eq!(
        remaining(&uri).await,
        vec![10, 11],
        "contact 8 must survive an empty page for contact 7"
    );
}

#[tokio::test]
async fn a_scope_field_that_is_also_a_key_field_does_not_widen_the_delete() {
    // THE regression. With sibling filter fields rather than `$and`, the
    // duplicate `contact_id` entry would overwrite the scope predicate and the
    // delete would take out the entire collection, contact 8 included.
    let (_c, uri) = start_mongo().await;
    seed(&uri).await;
    let key = vec!["contact_id".to_string(), "_id".to_string()];
    let sink = MongoSink::new(config(&uri, key.clone()))
        .await
        .expect("sink");

    let deleted = write_then_cleanup(
        &sink,
        &[json!({"_id": 3, "contact_id": 7, "label": "kept"})],
        scope7(),
        key,
    )
    .await;

    assert_eq!(deleted, 2, "only contact 7's stale documents");
    assert_eq!(
        remaining(&uri).await,
        vec![3, 10, 11],
        "contact 8's documents must still be present — a widened delete would have \
         removed them"
    );
}

#[tokio::test]
async fn a_scope_with_no_stale_documents_deletes_nothing() {
    let (_c, uri) = start_mongo().await;
    seed(&uri).await;
    let sink = MongoSink::new(config(&uri, vec!["_id".into()]))
        .await
        .expect("sink");

    let deleted = write_then_cleanup(
        &sink,
        &[
            json!({"_id": 1, "contact_id": 7, "label": "a"}),
            json!({"_id": 2, "contact_id": 7, "label": "b"}),
            json!({"_id": 3, "contact_id": 7, "label": "c"}),
        ],
        scope7(),
        vec!["_id".into()],
    )
    .await;

    assert_eq!(deleted, 0);
    assert_eq!(remaining(&uri).await.len(), 5);
}
