//! A batch re-sent after an `insert_many` failure: a duplicate key on a
//! document whose `_id` the sink generated is read back, so a document that
//! collided on another unique index fails instead of being counted written.

use faucet_core::Sink;
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::mongo::Mongo;

async fn start_mongo() -> (ContainerAsync<Mongo>, String) {
    let container: ContainerAsync<Mongo> = faucet_conformance::containers::start(|| {
        Mongo::default().with_cmd(["--setParameter", "enableTestCommands=1"])
    })
    .await;
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    (container, format!("mongodb://127.0.0.1:{port}"))
}

/// A unique `email` index holding `a`, and the next `insert` failing once.
async fn prepare(uri: &str) -> mongodb::Collection<Document> {
    let client = Client::with_uri_str(uri).await.expect("client");
    let users = client.database("testdb").collection::<Document>("users");
    users
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! { "email": 1 })
                .options(
                    mongodb::options::IndexOptions::builder()
                        .unique(true)
                        .build(),
                )
                .build(),
        )
        .await
        .expect("index");
    users.insert_one(doc! { "email": "a" }).await.expect("seed");
    client
        .database("admin")
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": { "times": 1 },
            "data": { "failCommands": ["insert"], "errorCode": 2 },
        })
        .await
        .expect("fail point");
    users
}

async fn resend_fails_the_colliding_row(ordered: bool) {
    let (_container, uri) = start_mongo().await;
    let users = prepare(&uri).await;
    let config = MongoSinkConfig::new(&uri, "testdb", "users").with_ordered(ordered);
    let sink = MongoSink::new(config).await.expect("sink");

    let outcomes = sink
        .write_batch_partial(&[json!({"email": "a"}), json!({"email": "b"})])
        .await
        .expect("outcomes");
    let err = outcomes[0].as_ref().expect_err("collides on email");
    assert!(err.to_string().contains("E11000"), "{err}");
    assert!(outcomes[1].is_ok(), "{outcomes:?}");
    assert_eq!(
        users.count_documents(doc! { "email": "b" }).await.unwrap(),
        1
    );
    assert_eq!(users.count_documents(doc! {}).await.unwrap(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_resent_document_that_collides_on_another_index_fails() {
    resend_fails_the_colliding_row(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_ordered_resent_document_that_collides_on_another_index_fails() {
    resend_fails_the_colliding_row(true).await;
}
