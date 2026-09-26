//! Integration tests for `MongoCdcSource` against a real MongoDB single-node
//! replica set via testcontainers.
//!
//! These tests require Docker (matching the kafka / postgres-cdc convention).
//! Change Streams require a replica set, so the container is started with
//! `Mongo::repl_set()`, which boots `mongod --replSet rs`, runs
//! `rs.initiate()`, and waits for the primary to be elected. We connect with
//! `directConnection=true`.
//!
//! The test opens a change stream, then a concurrent writer performs
//! insert / update / delete after a warm-up delay (so the stream is open and
//! `start_from = now` captures the writes), and asserts the emitted CDC
//! envelopes. It then resumes from the captured bookmark and asserts that a
//! subsequent write — and only that write — is delivered (no replay).

use faucet_core::Source;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_source_mongodb_cdc::{MongoCdcSource, MongoCdcSourceConfig};
use futures::StreamExt;
use mongodb::Client;
use mongodb::bson::{Document, doc};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::mongo::Mongo;

const DB: &str = "app";
const COLL: &str = "users";

/// Start a single-node replica set and return the container handle + URI.
async fn start_repl_set() -> (ContainerAsync<Mongo>, String) {
    let container = Mongo::repl_set()
        .start()
        .await
        .expect("mongo replica-set container start");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");
    (container, uri)
}

fn config(uri: &str) -> MongoCdcSourceConfig {
    serde_json::from_value(json!({
        "connection_uri": uri,
        "scope": { "type": "collection", "database": DB, "collection": COLL },
        "full_document": "update_lookup",
        "start_from": { "type": "now" },
        "idle_timeout": 5,
        "max_await_time_ms": 500,
        "batch_size": 0
    }))
    .expect("config")
}

/// Drain a single fetch cycle into a flat `Vec` of records plus the bookmark of
/// the last page that carried one. The cycle ends after `idle_timeout` of quiet.
async fn drain(source: &MongoCdcSource) -> (Vec<Value>, Option<Value>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut records = Vec::new();
    let mut bookmark = None;
    while let Some(page) = pages.next().await {
        let page = page.expect("page");
        records.extend(page.records);
        if page.bookmark.is_some() {
            bookmark = page.bookmark;
        }
    }
    (records, bookmark)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_captures_crud_then_resumes_without_replay() {
    let (_container, uri) = start_repl_set().await;

    // Create the collection up front (before the change stream opens) so the
    // single-collection watch has an existing namespace. This seed insert
    // happens before `start_from = now`, so it is not captured.
    {
        let client = Client::with_uri_str(&uri).await.expect("seed client");
        client
            .database(DB)
            .collection::<Document>(COLL)
            .insert_one(doc! { "_id": 0, "seed": true })
            .await
            .expect("seed insert");
    }

    let source = MongoCdcSource::new(config(&uri)).await.expect("source");

    // Concurrent writer: wait for the stream to open, then c / u / d on _id=1.
    let writer_uri = uri.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let client = Client::with_uri_str(&writer_uri)
            .await
            .expect("writer client");
        let coll = client.database(DB).collection::<Document>(COLL);
        coll.insert_one(doc! { "_id": 1, "name": "alice" })
            .await
            .expect("insert");
        coll.update_one(doc! { "_id": 1 }, doc! { "$set": { "name": "bob" } })
            .await
            .expect("update");
        coll.delete_one(doc! { "_id": 1 }).await.expect("delete");
    });

    let (records, bookmark) = drain(&source).await;
    writer.await.expect("writer task");

    // We must have observed the create, update, and delete for _id=1.
    let ops: Vec<&str> = records
        .iter()
        .map(|r| r["op"].as_str().unwrap_or(""))
        .collect();
    assert!(ops.contains(&"c"), "expected a create op, got {ops:?}");
    assert!(ops.contains(&"u"), "expected an update op, got {ops:?}");
    assert!(ops.contains(&"d"), "expected a delete op, got {ops:?}");

    // The create envelope should carry the full document (update_lookup) and
    // the correct namespace.
    let create = records
        .iter()
        .find(|r| r["op"] == "c")
        .expect("create record");
    assert_eq!(create["namespace"]["db"], DB);
    assert_eq!(create["namespace"]["coll"], COLL);
    assert_eq!(create["after"]["name"], "alice");
    assert!(create["resume_token"]["_data"].is_string());

    let bookmark = bookmark.expect("cycle 1 produced a bookmark");

    // Resume after the last event of cycle 1, then write a NEW document.
    source
        .apply_start_bookmark(bookmark)
        .await
        .expect("apply bookmark");

    let writer_uri = uri.clone();
    let writer2 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let client = Client::with_uri_str(&writer_uri)
            .await
            .expect("writer2 client");
        client
            .database(DB)
            .collection::<Document>(COLL)
            .insert_one(doc! { "_id": 2, "name": "carol" })
            .await
            .expect("insert2");
    });

    let (records2, _bm2) = drain(&source).await;
    writer2.await.expect("writer2 task");

    // Resume must not replay _id=1's events; only the _id=2 insert appears.
    assert!(
        !records2.is_empty(),
        "expected the post-bookmark insert to be delivered"
    );
    for r in &records2 {
        let key = &r["document_key"]["_id"];
        assert_eq!(
            key,
            &json!(2),
            "resume replayed a pre-bookmark event: {r:?}"
        );
    }
    assert!(records2.iter().any(|r| r["op"] == "c"));
}

// --- instance trait methods (require a live replica set via new()) ---

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn instance_metadata_methods() {
    let (_container, uri) = start_repl_set().await;
    let source = MongoCdcSource::new(config(&uri)).await.expect("source");

    // state_key is derived from the (collection) scope.
    assert_eq!(
        source.state_key().as_deref(),
        Some("mongodb-cdc:coll:app.users"),
        "collection-scope state key"
    );

    // CDC supports exactly-once delivery (durable resume token).
    assert!(source.supports_exactly_once());

    // Stable connector label used for metrics/logging.
    assert_eq!(source.connector_name(), "mongodb-cdc");

    // dataset_uri for a collection scope appends db/coll after the redacted URI.
    let ds = source.dataset_uri();
    assert!(
        ds.ends_with(&format!("/{DB}/{COLL}")),
        "collection dataset_uri: {ds}"
    );

    // config_schema is the JSON Schema for the config struct.
    let schema = source.config_schema();
    let props = schema
        .get("properties")
        .and_then(|p| p.as_object())
        .expect("config_schema must be a JSON object schema");
    assert!(props.contains_key("connection_uri"), "schema: {schema}");
    assert!(props.contains_key("scope"));
    assert!(props.contains_key("start_from"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dataset_uri_database_and_cluster_scopes() {
    let (_container, uri) = start_repl_set().await;
    let base = faucet_core::redact_uri_credentials(&uri);

    // Database scope → base/<db>; state key is mongodb-cdc:db:<db>.
    let db_cfg: MongoCdcSourceConfig = serde_json::from_value(json!({
        "connection_uri": &uri,
        "scope": { "type": "database", "database": DB },
        "idle_timeout": 5,
        "max_await_time_ms": 500,
        "batch_size": 0
    }))
    .expect("db config");
    let db_source = MongoCdcSource::new(db_cfg).await.expect("db source");
    assert_eq!(db_source.dataset_uri(), format!("{base}/{DB}"));
    assert_eq!(
        db_source.state_key().as_deref(),
        Some(format!("mongodb-cdc:db:{DB}").as_str())
    );

    // Cluster scope → just the redacted base; state key is mongodb-cdc:cluster.
    let cluster_cfg: MongoCdcSourceConfig = serde_json::from_value(json!({
        "connection_uri": &uri,
        "scope": { "type": "cluster" },
        "idle_timeout": 5,
        "max_await_time_ms": 500,
        "batch_size": 0
    }))
    .expect("cluster config");
    let cluster_source = MongoCdcSource::new(cluster_cfg)
        .await
        .expect("cluster source");
    assert_eq!(cluster_source.dataset_uri(), base);
    assert_eq!(
        cluster_source.state_key().as_deref(),
        Some("mongodb-cdc:cluster")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn check_passes_on_replica_set() {
    let (_container, uri) = start_repl_set().await;
    let source = MongoCdcSource::new(config(&uri)).await.expect("source");

    let report = source
        .check(&CheckContext::default())
        .await
        .expect("check report");
    // Replica set → both connection and topology probes pass.
    let names: Vec<&str> = report.probes.iter().map(|p| p.name).collect();
    assert!(names.contains(&"connection"), "probes: {names:?}");
    assert!(names.contains(&"topology"), "probes: {names:?}");
    for p in &report.probes {
        assert!(
            matches!(p.status, ProbeStatus::Pass),
            "probe {} must pass on a replica set, got {:?}",
            p.name,
            p.status
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_with_context_drains_into_flat_vec() {
    let (_container, uri) = start_repl_set().await;
    {
        let client = Client::with_uri_str(&uri).await.expect("seed client");
        client
            .database(DB)
            .collection::<Document>(COLL)
            .insert_one(doc! { "_id": 0, "seed": true })
            .await
            .expect("seed insert");
    }
    let source = MongoCdcSource::new(config(&uri)).await.expect("source");

    let writer_uri = uri.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let client = Client::with_uri_str(&writer_uri)
            .await
            .expect("writer client");
        client
            .database(DB)
            .collection::<Document>(COLL)
            .insert_one(doc! { "_id": 7, "name": "eve" })
            .await
            .expect("insert");
    });

    // fetch_with_context drains every page of one cycle into a flat Vec.
    let ctx: HashMap<String, Value> = HashMap::new();
    let records = source.fetch_with_context(&ctx).await.expect("fetch");
    writer.await.expect("writer task");

    assert!(
        records
            .iter()
            .any(|r| r["op"] == "c" && r["after"]["name"] == "eve"),
        "expected the eve insert in the flattened result, got {records:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_warns_but_succeeds_with_pre_image_request() {
    // full_document_before_change != Off triggers the warn branch in new();
    // construction must still succeed against a replica set (the server only
    // errors at stream open if pre-images are unavailable, not at connect).
    let (_container, uri) = start_repl_set().await;
    let cfg: MongoCdcSourceConfig = serde_json::from_value(json!({
        "connection_uri": uri,
        "scope": { "type": "collection", "database": DB, "collection": COLL },
        "full_document_before_change": "when_available",
        "idle_timeout": 5,
        "max_await_time_ms": 500,
        "batch_size": 0
    }))
    .expect("config");
    let source = MongoCdcSource::new(cfg).await.expect("source new");
    assert_eq!(source.connector_name(), "mongodb-cdc");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_rejects_standalone_topology() {
    // A standalone mongod (not a replica set) does not support change streams;
    // new() must fail via ensure_changestream_capable with a Source error.
    let container = Mongo::default()
        .start()
        .await
        .expect("standalone mongo start");
    let port = container.get_host_port_ipv4(27017).await.expect("port");
    let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");

    let cfg: MongoCdcSourceConfig = serde_json::from_value(json!({
        "connection_uri": uri,
        "scope": { "type": "collection", "database": DB, "collection": COLL },
        "idle_timeout": 5,
        "max_await_time_ms": 500,
        "batch_size": 0
    }))
    .expect("config");

    match MongoCdcSource::new(cfg).await {
        Err(faucet_core::FaucetError::Source(m)) => {
            assert!(
                m.contains("replica set") || m.contains("standalone"),
                "expected a topology rejection, got: {m}"
            );
        }
        Err(other) => panic!("expected a Source topology error, got {other:?}"),
        Ok(_) => panic!("standalone mongod must be rejected for change streams"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_resume_position_returns_resume_token() {
    // `config(&uri)` uses Collection scope, so this exercises the
    // `Scope::Collection` arm of `capture_now_token`.
    let (_container, uri) = start_repl_set().await;
    let source = MongoCdcSource::new(config(&uri)).await.expect("source");
    let pos = source
        .capture_resume_position()
        .await
        .expect("capture")
        .expect("mongodb-cdc must support capture");
    // Shape: { "resume_token": { "_data": "..." } }
    assert!(
        pos.get("resume_token").is_some(),
        "resume_token present: {pos}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_resume_position_database_scope() {
    // Exercises the `Scope::Database` arm of `capture_now_token` —
    // `client.database(db).watch()`.
    let (_container, uri) = start_repl_set().await;
    let cfg: MongoCdcSourceConfig = serde_json::from_value(json!({
        "connection_uri": &uri,
        "scope": { "type": "database", "database": DB },
        "start_from": { "type": "now" },
        "idle_timeout": 5,
        "max_await_time_ms": 500,
        "batch_size": 0
    }))
    .expect("database-scope config");
    let source = MongoCdcSource::new(cfg).await.expect("source");
    let pos = source
        .capture_resume_position()
        .await
        .expect("capture")
        .expect("mongodb-cdc must support capture");
    assert!(
        pos.get("resume_token").is_some(),
        "resume_token present: {pos}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn capture_resume_position_cluster_scope() {
    // Exercises the `Scope::Cluster` arm of `capture_now_token` —
    // the deployment-wide `client.watch()`.
    let (_container, uri) = start_repl_set().await;
    let cfg: MongoCdcSourceConfig = serde_json::from_value(json!({
        "connection_uri": &uri,
        "scope": { "type": "cluster" },
        "start_from": { "type": "now" },
        "idle_timeout": 5,
        "max_await_time_ms": 500,
        "batch_size": 0
    }))
    .expect("cluster-scope config");
    let source = MongoCdcSource::new(cfg).await.expect("source");
    let pos = source
        .capture_resume_position()
        .await
        .expect("capture")
        .expect("mongodb-cdc must support capture");
    assert!(
        pos.get("resume_token").is_some(),
        "resume_token present: {pos}"
    );
}

/// #733 — `lag()` reports how old the oldest undelivered change is, against the
/// cluster's current time: none before a position is known, near zero once a
/// cycle has caught up, and the age of a change sitting unread behind a stored
/// bookmark (what `faucet status --probe` asks on a fresh source).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lag_reports_the_age_of_the_oldest_unread_change() {
    let (_container, uri) = start_repl_set().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let coll = client.database(DB).collection::<Document>(COLL);
    coll.insert_one(doc! { "_id": 0, "seed": true })
        .await
        .expect("seed insert");

    let source = MongoCdcSource::new(config(&uri)).await.expect("source");
    assert_eq!(source.lag().await.expect("lag"), None);

    let writer_uri = uri.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let client = Client::with_uri_str(&writer_uri).await.expect("writer");
        client
            .database(DB)
            .collection::<Document>(COLL)
            .insert_one(doc! { "_id": 1 })
            .await
            .expect("insert");
    });
    let (records, bookmark) = drain(&source).await;
    writer.await.expect("writer task");
    assert_eq!(records.len(), 1);
    let caught_up = source.lag().await.expect("lag").expect("a reading");
    assert!(
        caught_up.seconds.unwrap() < 30.0,
        "an idle, caught-up stream is not behind: {caught_up:?}"
    );

    coll.insert_one(doc! { "_id": 2 }).await.expect("insert");
    tokio::time::sleep(Duration::from_secs(3)).await;
    client
        .database(DB)
        .collection::<Document>("other")
        .insert_one(doc! { "tick": 1 })
        .await
        .expect("advance the cluster time");
    let probe = MongoCdcSource::new(config(&uri)).await.expect("source");
    probe
        .apply_start_bookmark(bookmark.expect("bookmark"))
        .await
        .expect("apply");
    let behind = probe.lag().await.expect("lag").expect("a reading");
    assert!(
        behind.seconds.unwrap() >= 2.0,
        "the unread insert is at least as old as the pause: {behind:?}"
    );
}

/// Records every row a pipeline writes.
#[derive(Default)]
struct Capture(std::sync::Mutex<Vec<Value>>);

#[faucet_core::async_trait]
impl faucet_core::Sink for Capture {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, faucet_core::FaucetError> {
        self.0.lock().unwrap().extend_from_slice(records);
        Ok(records.len())
    }
}

/// #736: a pipeline resumes from a bookmark a release before versioned state
/// stored — bare, in the schema-0 shape (no `invalidate` flag) — migrates it,
/// delivers only what came after it, and stores the next bookmark in the
/// envelope at schema 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pipeline_resumes_from_a_released_schema_zero_bookmark() {
    use faucet_core::state_version::{STATE_FORMAT, StoredState};
    use faucet_core::{MemoryStateStore, Pipeline, StateStore};
    use std::sync::Arc;

    let (_container, uri) = start_repl_set().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let coll = client.database(DB).collection::<Document>(COLL);
    coll.insert_one(doc! { "_id": 0, "seed": true })
        .await
        .expect("seed");

    let first = MongoCdcSource::new(config(&uri)).await.expect("source");
    let writer_uri = uri.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let client = Client::with_uri_str(&writer_uri).await.expect("writer");
        client
            .database(DB)
            .collection::<Document>(COLL)
            .insert_one(doc! { "_id": 1, "name": "alice" })
            .await
            .expect("insert");
    });
    let (_records, bookmark) = drain(&first).await;
    writer.await.expect("writer");
    let mut legacy = bookmark.expect("a bookmark");
    legacy.as_object_mut().unwrap().remove("invalidate");
    assert!(legacy.get("invalidate").is_none(), "the schema-0 shape");

    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let key = first.state_key().expect("state key");
    store.put(&key, &legacy).await.unwrap();

    coll.insert_one(doc! { "_id": 2, "name": "carol" })
        .await
        .expect("insert after the bookmark");

    let source = MongoCdcSource::new(config(&uri)).await.expect("source");
    let sink = Capture::default();
    Pipeline::new(&source, &sink)
        .with_state_store(store.clone())
        .run()
        .await
        .expect("the legacy bookmark resumes");

    let written = sink.0.lock().unwrap().clone();
    assert!(!written.is_empty(), "the post-bookmark insert is delivered");
    for r in &written {
        assert_eq!(r["document_key"]["_id"], json!(2), "replayed: {r}");
    }
    let stored = StoredState::parse(&store.get(&key).await.unwrap().expect("stored"));
    assert_eq!(stored.format, STATE_FORMAT);
    assert_eq!(stored.owner.as_deref(), Some("mongodb-cdc"));
    assert_eq!(stored.schema, 1);
    assert_eq!(stored.data["invalidate"], json!(false));
}
