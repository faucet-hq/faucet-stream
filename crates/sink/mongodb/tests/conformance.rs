//! `faucet-conformance` battery for the MongoDB sink.
//! Passing this battery in CI is the Tier-1 (supported) criterion.
//!
//! - check 1 `assert_config_schema_valid_value`
//! - check 4 `assert_idempotent_replay` — the atomic-watermark path
//!   (`write_batch_idempotent` + `last_committed_token`, one multi-document
//!   transaction plus a `_faucet_commit_token` collection).
//! - check 5 `assert_capabilities_truthful` — Append plus the advertised
//!   idempotency mechanism actually hold.
//!
//! Checks 4 and 5 boot a single-node **replica-set** MongoDB container
//! (transactions require a replica set — reusing `exactly_once.rs`'s setup),
//! so they require Docker.
use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_mongodb::MongoSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "mongodb");
}

// ── Check 10: connector_name is non-empty (offline) ──────────────────────────
//
// `MongoSink::new` only parses the URI (it spawns the driver's background
// topology monitor, no blocking connect), so a sink builds offline and its
// pure `connector_name()` can be asserted without Docker.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    use faucet_core::Sink;
    let sink = faucet_sink_mongodb::MongoSink::new(faucet_sink_mongodb::MongoSinkConfig::new(
        "mongodb://127.0.0.1:27017",
        "testdb",
        "events",
    ))
    .await
    .expect("sink builds lazily");
    faucet_conformance::assert_batch_atomicity_declared(&sink);
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    assert_eq!(sink.connector_name(), "mongodb");
}

mod idempotent {
    use faucet_sink_mongodb::{MongoSink, MongoSinkConfig};
    use mongodb::Client;
    use mongodb::bson::{Document, doc};
    use testcontainers::{ContainerAsync, runners::AsyncRunner};
    use testcontainers_modules::mongo::Mongo;

    const DB: &str = "testdb";
    const COLLECTION: &str = "events";

    /// Start a single-node MongoDB **replica set** container (transactions
    /// available) — mirrors `exactly_once.rs::start_mongo_repl_set`.
    async fn start_mongo_repl_set() -> (ContainerAsync<Mongo>, String) {
        let container: ContainerAsync<Mongo> = Mongo::repl_set()
            .start()
            .await
            .expect("mongo repl-set container start");
        let port = container
            .get_host_port_ipv4(27017)
            .await
            .expect("mongo port");
        let uri = format!("mongodb://127.0.0.1:{port}/?directConnection=true");
        (container, uri)
    }

    /// A fresh replica-set container + an append-mode sink pointed at the
    /// `events` collection. The battery's atomic-watermark path writes `{id, v}`
    /// rows through `write_batch_idempotent`; the distinct-row count is the
    /// number of documents in `events` (the `_faucet_commit_token` watermark
    /// lives in a separate collection and is not counted).
    async fn fresh_sink() -> (ContainerAsync<Mongo>, String, MongoSink) {
        let (container, uri) = start_mongo_repl_set().await;
        let sink = MongoSink::new(MongoSinkConfig::new(&uri, DB, COLLECTION))
            .await
            .expect("sink new");
        (container, uri, sink)
    }

    /// A fresh replica-set container + a **keyed upsert-mode** sink, so the
    /// advertised `Upsert`/`Delete` write modes can be exercised through the
    /// trait. Keyed on `["id"]` (the field the battery writes), with the
    /// conformance delete marker (`__op` = `"d"`) so a delete-marked record
    /// removes its row.
    async fn keyed_sink() -> (ContainerAsync<Mongo>, String, MongoSink) {
        let (container, uri) = start_mongo_repl_set().await;
        let mut config = MongoSinkConfig::new(&uri, DB, COLLECTION);
        config.write.write_mode = faucet_core::WriteMode::Upsert;
        config.write.key = vec!["id".to_string()];
        config.write.delete_marker = Some(faucet_core::DeleteMarker {
            field: faucet_conformance::doubles::DELETE_MARKER_FIELD.to_string(),
            values: vec![faucet_conformance::doubles::DELETE_MARKER_VALUE.to_string()],
        });
        let sink = MongoSink::new(config).await.expect("keyed sink new");
        (container, uri, sink)
    }

    async fn count_docs(uri: &str) -> usize {
        let client = Client::with_uri_str(uri).await.expect("client");
        client
            .database(DB)
            .collection::<Document>(COLLECTION)
            .count_documents(doc! {})
            .await
            .expect("count_documents") as usize
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_idempotent_replay() {
        let (_container, uri, sink) = fresh_sink().await;
        faucet_conformance::assert_idempotent_replay(&sink, || {
            let uri = uri.clone();
            async move { count_docs(&uri).await }
        })
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_capabilities_truthful() {
        let (_container, uri, sink) = fresh_sink().await;
        faucet_conformance::assert_capabilities_truthful(&sink, || {
            let uri = uri.clone();
            async move { count_docs(&uri).await }
        })
        .await;
    }

    /// Check 7: the advertised `Upsert`/`Delete` write modes are truthful — a
    /// re-written key converges (no duplicate), a delete-marked record removes
    /// its row, and missing/null-key rows are reported as failed. Uses a keyed
    /// sink so `dedups_by_key()` is true and the modes can be exercised.
    #[tokio::test(flavor = "multi_thread")]
    async fn conformance_write_modes_truthful() {
        let (_container, uri, sink) = keyed_sink().await;
        faucet_conformance::assert_write_modes_truthful(&sink, || {
            let uri = uri.clone();
            async move { count_docs(&uri).await }
        })
        .await;
    }
}
