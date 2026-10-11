//! #862 M-060: an `insert_many` the server applied but whose write concern it
//! could not confirm is reported per row as applied, never as an outer error
//! (which would claim nothing committed and let a DLQ replay duplicate it).
//! Requires Docker (a single-node replica set).

use faucet_core::Sink;
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use mongodb::Client;
use mongodb::bson::Document;
use serde_json::json;
use testcontainers::ContainerAsync;
use testcontainers_modules::mongo::Mongo;

#[tokio::test(flavor = "multi_thread")]
async fn a_write_concern_error_reports_the_applied_rows_as_written() {
    let container: ContainerAsync<Mongo> =
        faucet_conformance::containers::start(Mongo::repl_set).await;
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    let base = format!("mongodb://127.0.0.1:{port}/?directConnection=true");
    // `w: 2` on a one-member replica set: the server applies the inserts and
    // then reports a write-concern error.
    let uri = format!("{base}&w=2&wtimeoutMS=500");
    let sink = MongoSink::new(MongoSinkConfig::new(&uri, "testdb", "wc"))
        .await
        .expect("sink");
    let out = sink
        .write_batch_partial(&[json!({"a": 1}), json!({"a": 2})])
        .await
        .expect("a write-concern error is not an outer error");
    assert_eq!(out.len(), 2);
    assert!(out.iter().all(Result::is_ok), "{out:?}");

    let client = Client::with_uri_str(&base).await.expect("client");
    let n = client
        .database("testdb")
        .collection::<Document>("wc")
        .count_documents(Document::new())
        .await
        .expect("count");
    assert_eq!(n, 2, "both documents landed");
}
