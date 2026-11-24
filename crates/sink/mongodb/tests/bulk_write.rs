//! Keyed writes on MongoDB 8.0 (one `bulkWrite` per page) land the same
//! documents, and report the same row errors, as on a server without the
//! command (one `replace_one` / `delete_one` per row).

#![allow(deprecated)] // the tests drive the 1.x lifecycle hooks directly (RFC 0014)

use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use mongodb::Client;
use mongodb::bson::{Bson, Document, doc};
use serde_json::{Value, json};
use testcontainers::core::ExecCommand;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::mongo::Mongo;

const DB: &str = "testdb";
const COLLECTION: &str = "docs";

/// A single-node replica set: the module's own one for the default image,
/// or `tag` started with `--replSet` and initiated here (the module waits for
/// a log line newer servers no longer print).
async fn start(tag: Option<&'static str>) -> (ContainerAsync<Mongo>, String) {
    start_with(tag, &[]).await
}

/// [`start`], with extra server arguments for a tagged image.
#[allow(clippy::disallowed_methods)] // a test's readiness poll
async fn start_with(
    tag: Option<&'static str>,
    args: &'static [&'static str],
) -> (ContainerAsync<Mongo>, String) {
    let container: ContainerAsync<Mongo> = match tag {
        Some(tag) => {
            faucet_conformance::containers::start(|| {
                let mut cmd = vec!["--replSet", "rs"];
                cmd.extend_from_slice(args);
                Mongo::default().with_tag(tag).with_cmd(cmd)
            })
            .await
        }
        None => faucet_conformance::containers::start(Mongo::repl_set).await,
    };
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");
    if tag.is_some() {
        container
            .exec(ExecCommand::new([
                "mongosh",
                "--quiet",
                "--eval",
                "rs.initiate()",
            ]))
            .await
            .expect("rs.initiate");
        let admin = Client::with_uri_str(&uri)
            .await
            .expect("client")
            .database("admin");
        let mut primary = false;
        for _ in 0..120 {
            if let Ok(hello) = admin.run_command(doc! {"hello": 1}).await
                && hello.get_bool("isWritablePrimary").unwrap_or(false)
            {
                primary = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        assert!(primary, "the replica set never elected a primary");
    }
    (container, uri)
}

fn config(uri: &str) -> MongoSinkConfig {
    let mut config = MongoSinkConfig::new(uri, DB, COLLECTION);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["_id".to_string()],
        delete_marker: Some(DeleteMarker {
            field: "__op".to_string(),
            values: vec!["d".to_string()],
        }),
        rollback: None,
    };
    config
}

/// Three pages over 1,200 keys: inserts, then replacements with repeated
/// keys, then deletes mixed with new rows.
fn pages() -> Vec<Vec<Value>> {
    let first: Vec<Value> = (0..1200)
        .map(|i| json!({"_id": i, "name": format!("n{i}"), "v": 1, "__op": "u"}))
        .collect();
    let mut second: Vec<Value> = (0..1200)
        .step_by(2)
        .map(|i| json!({"_id": i, "name": format!("m{i}"), "v": 2, "__op": "u"}))
        .collect();
    second.extend(
        (0..300).map(|i| json!({"_id": i, "v": 3, "nested": {"k": [i, "x"]}, "__op": "u"})),
    );
    let mut third: Vec<Value> = (0..1200)
        .step_by(3)
        .map(|i| json!({"_id": i, "__op": "d"}))
        .collect();
    third.extend((1200..1300).map(|i| json!({"_id": i, "v": 4, "__op": "u"})));
    vec![first, second, third]
}

async fn collection(uri: &str) -> Vec<Document> {
    use futures::TryStreamExt;
    Client::with_uri_str(uri)
        .await
        .expect("client")
        .database(DB)
        .collection::<Document>(COLLECTION)
        .find(doc! {})
        .sort(doc! {"_id": 1})
        .await
        .expect("find")
        .try_collect()
        .await
        .expect("documents")
}

async fn bulk_write_commands(uri: &str) -> i64 {
    let status = Client::with_uri_str(uri)
        .await
        .expect("client")
        .database("admin")
        .run_command(doc! {"serverStatus": 1})
        .await
        .expect("serverStatus");
    let total = status
        .get_document("metrics")
        .and_then(|m| m.get_document("commands"))
        .ok()
        .and_then(|c| c.get_document("bulkWrite").ok())
        .and_then(|b| b.get("total").cloned());
    match total {
        Some(Bson::Int64(n)) => n,
        Some(Bson::Int32(n)) => i64::from(n),
        _ => 0,
    }
}

async fn unique_name_index(uri: &str) {
    Client::with_uri_str(uri)
        .await
        .expect("client")
        .database(DB)
        .collection::<Document>(COLLECTION)
        .create_index(
            mongodb::IndexModel::builder()
                .keys(doc! {"name": 1})
                .options(
                    mongodb::options::IndexOptions::builder()
                        .unique(true)
                        .build(),
                )
                .build(),
        )
        .await
        .expect("index");
}

/// The error up to the end of its write error, with the server's own message
/// removed: the two server versions word it differently, and a single write
/// appends the raw server reply.
fn frame(error: &str) -> (String, String) {
    let (head, rest) = error.split_once("message: \"").expect("a write error");
    let tail = rest.split_once("E11000").expect("a duplicate key error").1;
    let tail = tail
        .split_once("}), labels")
        .expect("a rendered write error")
        .0;
    (head.to_string(), tail.to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_bulk_page_lands_the_documents_the_row_by_row_path_lands() {
    let (_new, bulk_uri) = start(Some("8.0")).await;
    let (_old, row_uri) = start(None).await;

    let mut written = Vec::new();
    for uri in [&bulk_uri, &row_uri] {
        let sink = MongoSink::new(config(uri)).await.expect("sink");
        let mut counts = Vec::new();
        for page in pages() {
            counts.push(sink.write_batch(&page).await.expect("write"));
        }
        let outcomes = sink
            .write_batch_partial(&[
                json!({"_id": 5000, "v": 9, "__op": "u"}),
                json!({"v": 9, "__op": "u"}),
                json!({"_id": 1, "__op": "d"}),
            ])
            .await
            .expect("partial write");
        counts.push(outcomes.iter().filter(|o| o.is_ok()).count());
        assert!(outcomes[1].is_err(), "a row without its key fails alone");
        written.push(counts);
    }
    assert_eq!(written[0], written[1]);

    let bulk = collection(&bulk_uri).await;
    assert_eq!(bulk, collection(&row_uri).await);
    assert_eq!(bulk.len(), 900);
    assert!(bulk_write_commands(&bulk_uri).await >= 4);
    assert_eq!(bulk_write_commands(&row_uri).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_row_fails_the_page_with_the_single_write_error() {
    let (_new, bulk_uri) = start(Some("8.0")).await;
    let (_old, row_uri) = start(None).await;

    let mut errors = Vec::new();
    for uri in [&bulk_uri, &row_uri] {
        let sink = MongoSink::new(config(uri)).await.expect("sink");
        sink.write_batch(&[json!({"_id": 1, "name": "taken", "__op": "u"})])
            .await
            .expect("seed");
        unique_name_index(uri).await;
        let err = sink
            .write_batch(&[json!({"_id": 2, "name": "taken", "__op": "u"})])
            .await
            .expect_err("duplicate name")
            .to_string();
        assert!(
            err.starts_with("Sink error: MongoDB replace_one (upsert) failed: "),
            "{err}"
        );
        assert!(err.contains("11000"), "{err}");
        errors.push(err);
    }
    assert_eq!(frame(&errors[0]), frame(&errors[1]));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exactly_once_page_is_one_transaction_on_a_bulk_server() {
    let (_new, uri) = start(Some("8.0")).await;
    let sink = MongoSink::new(config(&uri)).await.expect("sink");
    let page: Vec<Value> = (0..500)
        .map(|i| json!({"_id": i, "name": format!("n{i}"), "__op": "u"}))
        .collect();
    assert_eq!(
        sink.write_batch_idempotent(&page, "pipe::row", "00000000000000000001")
            .await
            .expect("first page"),
        500
    );
    assert_eq!(
        sink.last_committed_token("pipe::row").await.expect("token"),
        Some("00000000000000000001".to_string())
    );
    assert!(bulk_write_commands(&uri).await >= 1);

    unique_name_index(&uri).await;
    let refused = [
        json!({"_id": 0, "__op": "d"}),
        json!({"_id": 900, "name": "fresh", "__op": "u"}),
        json!({"_id": 901, "name": "n7", "__op": "u"}),
    ];
    let err = sink
        .write_batch_idempotent(&refused, "pipe::row", "00000000000000000002")
        .await
        .expect_err("duplicate name")
        .to_string();
    assert!(
        err.contains("MongoDB replace_one (exactly-once upsert) failed"),
        "{err}"
    );
    let after = collection(&uri).await;
    assert_eq!(after.len(), 500, "the refused page left nothing behind");
    assert_eq!(
        sink.last_committed_token("pipe::row").await.expect("token"),
        Some("00000000000000000001".to_string())
    );

    let mixed = [
        json!({"_id": 0, "__op": "d"}),
        json!({"_id": 1, "name": "renamed", "__op": "u"}),
        json!({"_id": 900, "name": "fresh", "__op": "u"}),
    ];
    assert_eq!(
        sink.write_batch_idempotent(&mixed, "pipe::row", "00000000000000000003")
            .await
            .expect("third page"),
        3
    );
    let after = collection(&uri).await;
    assert_eq!(after.len(), 500);
    assert!(
        after
            .iter()
            .all(|d| d.get_i64("_id").ok() != Some(0) && d.get_i32("_id").ok() != Some(0))
    );
    assert!(
        after
            .iter()
            .any(|d| d.get_str("name").ok() == Some("renamed"))
    );
}

/// Make the server answer `bulkWrite` with `CommandNotFound`, as a server
/// whose wire version advertises the command but does not run it would.
async fn refuse_bulk_write(uri: &str) {
    Client::with_uri_str(uri)
        .await
        .expect("client")
        .database("admin")
        .run_command(doc! {
            "configureFailPoint": "failCommand",
            "mode": "alwaysOn",
            "data": {"failCommands": ["bulkWrite"], "errorCode": 59},
        })
        .await
        .expect("failpoint");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_refusing_the_command_gets_the_page_row_by_row() {
    let (_new, uri) = start_with(Some("8.0"), &["--setParameter", "enableTestCommands=1"]).await;
    refuse_bulk_write(&uri).await;
    let sink = MongoSink::new(config(&uri)).await.expect("sink");
    let mut counts = Vec::new();
    for page in pages() {
        counts.push(sink.write_batch(&page).await.expect("write"));
    }
    let (_old, row_uri) = start(None).await;
    let row_sink = MongoSink::new(config(&row_uri)).await.expect("sink");
    let mut row_counts = Vec::new();
    for page in pages() {
        row_counts.push(row_sink.write_batch(&page).await.expect("write"));
    }
    assert_eq!(counts, row_counts);
    assert_eq!(collection(&uri).await, collection(&row_uri).await);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_exactly_once_page_refused_as_a_command_fails_once_then_goes_row_by_row() {
    let (_new, uri) = start_with(Some("8.0"), &["--setParameter", "enableTestCommands=1"]).await;
    refuse_bulk_write(&uri).await;
    let sink = MongoSink::new(config(&uri)).await.expect("sink");
    let page: Vec<Value> = (0..50)
        .map(|i| json!({"_id": i, "name": format!("n{i}"), "__op": "u"}))
        .collect();
    let err = sink
        .write_batch_idempotent(&page, "pipe::row", "00000000000000000001")
        .await
        .expect_err("the transaction's bulkWrite is refused")
        .to_string();
    assert!(
        err.contains("MongoDB bulk_write (exactly-once) failed"),
        "{err}"
    );
    assert!(collection(&uri).await.is_empty());
    assert_eq!(
        sink.last_committed_token("pipe::row").await.expect("token"),
        None
    );
    assert_eq!(
        sink.write_batch_idempotent(&page, "pipe::row", "00000000000000000001")
            .await
            .expect("retried page"),
        50
    );
    assert_eq!(collection(&uri).await.len(), 50);
    assert_eq!(
        sink.last_committed_token("pipe::row").await.expect("token"),
        Some("00000000000000000001".to_string())
    );
}
