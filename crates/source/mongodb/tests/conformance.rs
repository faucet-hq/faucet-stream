//! `faucet-conformance` Tier-1 battery for the MongoDB source.
//!
//! Check 1 (config-schema validity) is pure and offline. Check 2
//! (bounded-memory streaming) boots a real MongoDB via testcontainers and so
//! requires Docker — it runs in CI alongside the other integration tests. It
//! matches the existing streaming integration test's container boot + seeding
//! path verbatim.

use faucet_conformance::{assert_config_schema_valid_value, assert_connector_name_nonempty};
use faucet_core::Source;
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use testcontainers::ContainerAsync;
use testcontainers_modules::mongo::Mongo;

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(MongoSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-mongodb");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// Start a MongoDB container and return both the container handle and a
/// connection URI. The container is kept alive by the returned handle.
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

/// Insert `n` documents of the form `{ id: i }` for `i = 1..=n` into
/// `db.collection`.
async fn seed_docs(uri: &str, db: &str, collection: &str, n: i64) {
    let client = Client::with_uri_str(uri).await.expect("client");
    let coll = client.database(db).collection::<Document>(collection);
    let docs: Vec<Document> = (1..=n).map(|i| doc! { "id": i }).collect();
    coll.insert_many(docs).await.expect("insert_many");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 5_000).await;

    let config = MongoSourceConfig::new(uri.as_str(), "testdb", "events").with_batch_size(250);
    let source = MongoSource::new(config).await.expect("source new");

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;

    // Check 10: connector_name is non-empty.
    assert_connector_name_nonempty(&source);

    // Check 11: preflight check() is well-formed against the live server (the
    // MongoDB source uses the default real-read-path probe — a non-mutating
    // find of the first page).
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    // Check 9: batch_size = 0 (the "no batching" sentinel) yields the entire
    // result set as a single page. Reuses the same container + seeded data.
    let zero_config = MongoSourceConfig::new(uri.as_str(), "testdb", "events").with_batch_size(0);
    let zero_source = MongoSource::new(zero_config)
        .await
        .expect("zero-batch source new");
    faucet_conformance::assert_batch_size_zero_single_page(&zero_source).await;
    // _container stays alive to here
}

// ── Check 12: discovery round-trips (Docker) ────────────────────────────────

/// Every collection `discover()` reports must be genuinely selectable:
/// deep-merge its config_patch (`{"collection": …}`) onto the base config,
/// rebuild the source, and read it.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_discover_roundtrips() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 3).await;

    let source = MongoSource::new(MongoSourceConfig::new(uri.as_str(), "testdb", "events"))
        .await
        .expect("source new");
    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let uri = uri.clone();
        async move {
            let base = serde_json::to_value(MongoSourceConfig::new(&uri, "testdb", "events"))
                .expect("serialize base config");
            let merged = faucet_conformance::merge_config_patch(base, &patch);
            let cfg: MongoSourceConfig =
                serde_json::from_value(merged).expect("deserialize merged config");
            Box::new(MongoSource::new(cfg).await.expect("rebuilt source"))
                as Box<dyn faucet_core::Source>
        }
    })
    .await;
    // _container stays alive to here
}

// ── Check 6: errors, not panics (no Docker) ─────────────────────────────────

/// `MongoSource::new` only parses the URI and spawns the driver's background
/// topology monitor — it does not perform a blocking connect — so a bad
/// endpoint builds successfully and the failure surfaces at read time. The URI
/// carries a short `serverSelectionTimeoutMS` so the driver gives up quickly
/// (instead of the 30s default) and returns a typed `FaucetError`. The battery
/// asserts both `fetch_all` and `stream_pages` return that error rather than
/// panicking.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    // Port 1 is unroutable; the short selection timeout keeps the test fast.
    let uri = "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=2000";
    let config = MongoSourceConfig::new(uri, "testdb", "events");
    let source = MongoSource::new(config)
        .await
        .expect("builds lazily; read fails");

    // Check 10: connector_name is non-empty (pure — runs offline, no Docker).
    assert_connector_name_nonempty(&source);
    assert_eq!(source.connector_name(), "mongodb");

    faucet_conformance::assert_errors_not_panics(&source).await;
}
