//! Integration tests for the MongoDB sink's exactly-once (effectively-once)
//! delivery path (#291).
//!
//! MongoDB multi-document transactions require a replica set, so most tests
//! boot a **single-node replica-set** container (`Mongo::repl_set()`, connected
//! with `directConnection=true`). The final test boots the plain standalone
//! container to assert the typed "requires a replica set" error.

use faucet_core::pipeline::{StreamPage, run_stream};
use faucet_core::state::{MemoryStateStore, StateStore};
use faucet_core::{DeliveryMode, FaucetError, RunStreamOptions, Sink, Value, WriteMode, WriteSpec};
use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use serde_json::json;
use std::sync::Arc;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mongo::Mongo;

const DB: &str = "testdb";
const COLLECTION: &str = "events";
const TOKEN_COLLECTION: &str = "_faucet_commit_token";

/// Start a single-node MongoDB **replica set** container (transactions
/// available) and return the handle + a direct-connection URI.
async fn start_mongo_repl_set() -> (ContainerAsync<Mongo>, String) {
    let container: ContainerAsync<Mongo> = Mongo::repl_set()
        .start()
        .await
        .expect("mongo repl-set container start");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    // directConnection=true: skip topology discovery of the container-internal
    // hostname the replica-set config advertises.
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");
    (container, uri)
}

/// Start a plain **standalone** MongoDB container (no transactions).
async fn start_mongo_standalone() -> (ContainerAsync<Mongo>, String) {
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

fn append_config(uri: &str) -> MongoSinkConfig {
    MongoSinkConfig::new(uri, DB, COLLECTION)
}

fn upsert_config(uri: &str) -> MongoSinkConfig {
    let mut config = MongoSinkConfig::new(uri, DB, COLLECTION);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["_id".to_string()],
        delete_marker: None,
        rollback: None,
    };
    config
}

async fn count_docs(uri: &str, collection: &str, filter: Document) -> u64 {
    let client = Client::with_uri_str(uri).await.expect("client");
    client
        .database(DB)
        .collection::<Document>(collection)
        .count_documents(filter)
        .await
        .expect("count_documents")
}

/// Read the raw watermark document for a scope straight from the collection.
async fn read_token_doc(uri: &str, scope: &str) -> Option<Document> {
    let client = Client::with_uri_str(uri).await.expect("client");
    client
        .database(DB)
        .collection::<Document>(TOKEN_COLLECTION)
        .find_one(doc! { "_id": scope })
        .await
        .expect("find_one watermark")
}

// ---------------------------------------------------------------------------
// (a) write_batch_idempotent commits the page's rows AND the watermark
//     atomically — both are readable afterwards, with the exact token value.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn write_batch_idempotent_commits_rows_and_token_atomically() {
    let (_container, uri) = start_mongo_repl_set().await;
    let sink = MongoSink::new(append_config(&uri)).await.expect("sink new");

    let records = [json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})];
    let written = sink
        .write_batch_idempotent(&records, "pipe::row", "00000000000000000001")
        .await
        .expect("idempotent write");
    assert_eq!(written, 2);

    assert_eq!(
        count_docs(&uri, COLLECTION, doc! {}).await,
        2,
        "both page rows must be committed"
    );
    let token_doc = read_token_doc(&uri, "pipe::row")
        .await
        .expect("watermark doc must exist");
    assert_eq!(
        token_doc.get_str("token").unwrap(),
        "00000000000000000001",
        "the watermark must carry the page token"
    );
}

// ---------------------------------------------------------------------------
// (b) last_committed_token round-trip; None for an unknown scope. The token is
//     opaque (may carry '#'+JSON) and must round-trip verbatim.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn last_committed_token_round_trip_and_none_for_unknown_scope() {
    let (_container, uri) = start_mongo_repl_set().await;
    let sink = MongoSink::new(append_config(&uri)).await.expect("sink new");

    assert_eq!(
        sink.last_committed_token("no-such-scope")
            .await
            .expect("token read"),
        None,
        "an unknown scope must read back as None"
    );

    let token = r##"00000000000000000007#{"lsn":"0/16B3748"}"##;
    sink.write_batch_idempotent(&[json!({"id": 1})], "pipe::row", token)
        .await
        .expect("idempotent write");

    assert_eq!(
        sink.last_committed_token("pipe::row")
            .await
            .expect("token read")
            .as_deref(),
        Some(token),
        "the opaque token must round-trip verbatim"
    );
    // A later page for the same scope advances the watermark in place.
    sink.write_batch_idempotent(&[json!({"id": 2})], "pipe::row", "00000000000000000008")
        .await
        .expect("second idempotent write");
    assert_eq!(
        sink.last_committed_token("pipe::row")
            .await
            .expect("token read")
            .as_deref(),
        Some("00000000000000000008"),
    );
    assert_eq!(
        count_docs(&uri, TOKEN_COLLECTION, doc! {}).await,
        1,
        "one watermark document per scope"
    );
}

// ---------------------------------------------------------------------------
// (c) Resume-skip end-to-end via faucet_core::run_stream: a crash between
//     sink-write and bookmark-persist must produce ZERO duplicates on resume.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn crash_between_write_and_bookmark_yields_no_duplicates() {
    let (_container, uri) = start_mongo_repl_set().await;
    let sink = MongoSink::new(append_config(&uri)).await.expect("sink new");

    // Run 1: commit only page 1, with a state store that never persists —
    // simulating a crash after the sink committed the page + watermark but
    // before the pipeline bookmark landed.
    struct DroppingStore;
    #[faucet_core::async_trait]
    impl StateStore for DroppingStore {
        async fn get(&self, _k: &str) -> Result<Option<Value>, FaucetError> {
            Ok(None)
        }
        async fn put(&self, _k: &str, _v: &Value) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn delete(&self, _k: &str) -> Result<(), FaucetError> {
            Ok(())
        }
    }
    let opts1 = RunStreamOptions::new()
        .with_state(Arc::new(DroppingStore), "events::r1")
        .with_delivery(DeliveryMode::ExactlyOnce);
    let first_page: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
        records: vec![json!({"id": 1})],
        bookmark: Some(json!("b1")),
    })];
    run_stream(futures::stream::iter(first_page), &sink, opts1)
        .await
        .expect("run 1");
    assert_eq!(count_docs(&uri, COLLECTION, doc! {"id": 1}).await, 1);

    // Run 2 (resume): fresh state, full replay of pages 1+2. The sink's
    // watermark (page 1's token) must make the pipeline skip page 1.
    let both_pages: Vec<Result<StreamPage, FaucetError>> = vec![
        Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: Some(json!("b1")),
        }),
        Ok(StreamPage {
            records: vec![json!({"id": 2})],
            bookmark: Some(json!("b2")),
        }),
    ];
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let opts2 = RunStreamOptions::new()
        .with_state(store, "events::r1")
        .with_delivery(DeliveryMode::ExactlyOnce);
    run_stream(futures::stream::iter(both_pages), &sink, opts2)
        .await
        .expect("run 2");

    assert_eq!(
        count_docs(&uri, COLLECTION, doc! {"id": 1}).await,
        1,
        "id=1 must NOT be duplicated on resume"
    );
    assert_eq!(
        count_docs(&uri, COLLECTION, doc! {"id": 2}).await,
        1,
        "id=2 written exactly once"
    );
}

// ---------------------------------------------------------------------------
// (d) Exactly-once composed with write_mode: upsert — the same _id upserted
//     across two idempotent pages converges to one document, and the
//     watermark advances with each page.
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn exactly_once_composes_with_upsert() {
    let (_container, uri) = start_mongo_repl_set().await;
    let sink = MongoSink::new(upsert_config(&uri)).await.expect("sink new");

    sink.write_batch_idempotent(
        &[json!({"_id": 1, "name": "old"})],
        "pipe::row",
        "00000000000000000001",
    )
    .await
    .expect("page 1");
    sink.write_batch_idempotent(
        &[json!({"_id": 1, "name": "new"})],
        "pipe::row",
        "00000000000000000002",
    )
    .await
    .expect("page 2");

    assert_eq!(
        count_docs(&uri, COLLECTION, doc! {}).await,
        1,
        "the same _id upserted twice must converge to one document"
    );
    assert_eq!(
        count_docs(&uri, COLLECTION, doc! {"_id": 1, "name": "new"}).await,
        1,
        "the second upsert's value must win"
    );
    assert_eq!(
        sink.last_committed_token("pipe::row")
            .await
            .expect("token read")
            .as_deref(),
        Some("00000000000000000002"),
        "the watermark must advance to the latest page token"
    );

    // A missing-key row fails fast, BEFORE any transaction — nothing written,
    // watermark unchanged (mirrors the postgres fail-fast on plan errors).
    let err = sink
        .write_batch_idempotent(
            &[json!({"name": "no-id"})],
            "pipe::row",
            "00000000000000000003",
        )
        .await
        .expect_err("missing-key row must fail");
    assert!(
        matches!(err, FaucetError::Sink(ref m) if m.contains("mongodb upsert")),
        "got: {err:?}"
    );
    assert_eq!(
        sink.last_committed_token("pipe::row")
            .await
            .expect("token read")
            .as_deref(),
        Some("00000000000000000002"),
        "a failed page must not advance the watermark"
    );
}

// ---------------------------------------------------------------------------
// (e) Against a STANDALONE server, write_batch_idempotent surfaces the typed
//     "requires a replica set" error (transactions unavailable).
// ---------------------------------------------------------------------------
#[tokio::test(flavor = "multi_thread")]
async fn standalone_server_yields_typed_replica_set_error() {
    let (_container, uri) = start_mongo_standalone().await;
    let sink = MongoSink::new(append_config(&uri)).await.expect("sink new");

    let err = sink
        .write_batch_idempotent(&[json!({"id": 1})], "pipe::row", "00000000000000000001")
        .await
        .expect_err("transactions must be unavailable on a standalone server");

    match err {
        FaucetError::Sink(m) => {
            assert!(
                m.contains("requires a replica set or sharded cluster"),
                "got: {m}"
            );
            assert!(m.contains("write_batch_idempotent"), "got: {m}");
        }
        other => panic!("expected a typed Sink error, got {other:?}"),
    }
}
