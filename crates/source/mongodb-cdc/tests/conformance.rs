//! `faucet-conformance` battery for the MongoDB CDC source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value
//!          (pure, offline — always runs).
//! Check 2: `stream_pages` bounds peak memory. Unlike the per-transaction SQL
//!          CDC sources, mongodb-cdc buffers per-document change events and
//!          flushes a page every time the buffer reaches `batch_size` (see
//!          `stream_pages_impl`). So a change stream of TOTAL events with
//!          `batch_size = 250` yields pages of ≤ 250 records: the peak page
//!          (250) is ≤ the batch cap and strictly < the total, proving genuine
//!          bounded paging. Because the source starts from "now" when the
//!          stream opens, the writes happen in a concurrent writer task while
//!          `assert_bounded_memory` drives the stream; the source's
//!          `idle_timeout` (5 s of quiet) terminates the drain (and flushes the
//!          trailing partial page) once the writer finishes.

use faucet_conformance::{
    assert_bookmark_roundtrip, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_source_mongodb_cdc::{MongoCdcSource, MongoCdcSourceConfig};
use mongodb::Client;
use mongodb::bson::{Document, doc};
use serde_json::json;
use std::time::Duration;
use testcontainers::ContainerAsync;
use testcontainers_modules::mongo::Mongo;

const DB: &str = "app";
const COLL: &str = "events";
const BATCH: usize = 250;
// Comfortably exceeds BATCH so the source flushes many full pages plus a
// trailing partial one.
const TOTAL: usize = 5000;

// ── Check 1: config schema validity (offline) ────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(MongoCdcSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-mongodb-cdc");
}

// ── Check 2: bounded-memory streaming (Docker) ───────────────────────────────

async fn start_repl_set() -> (ContainerAsync<Mongo>, String) {
    let container = faucet_conformance::containers::start(Mongo::repl_set).await;
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
        "start_from": { "type": "now" },
        "idle_timeout": 20,
        "max_await_time_ms": 500,
        // Flush a page every BATCH buffered change events (config field is
        // authoritative; the trait-level batch_size argument is ignored).
        "batch_size": BATCH,
    }))
    .expect("config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_bounded_memory() {
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

    // Check 10: connector_name is non-empty (pure — no I/O).
    assert_connector_name_nonempty(&source);
    // Check 11: preflight check() is well-formed against the live replica set.
    // The CDC check() is overridden to run a read-only `hello` + topology probe
    // (never a page pull), so it is safe to call in the live test.
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;

    // Concurrent writer: wait ~2 s for the change stream to open, then insert
    // TOTAL documents (each a distinct change event). After the writer goes
    // quiet the source's idle_timeout ends the drain and flushes the trailing
    // partial page.
    let writer_uri = uri.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let client = Client::with_uri_str(&writer_uri)
            .await
            .expect("writer client");
        let coll = client.database(DB).collection::<Document>(COLL);
        for i in 1..=TOTAL {
            coll.insert_one(doc! { "_id": i as i64, "name": "n" })
                .await
                .expect("insert");
        }
    });

    // The buffer flushes at BATCH, so the peak page is 250 ≤ BATCH (250) and
    // < TOTAL (5000).
    assert_bounded_memory(&source, BATCH, TOTAL).await;

    writer.await.expect("writer task");
}

// ── Check 3: bookmark round-trip (Docker) ────────────────────────────────────

/// The mongodb-cdc source is resumable via a Change Streams `resumeToken`: each
/// flushed page carries `bookmark = Some(resumeToken)` (the token of its last
/// event), and a persisted bookmark applied via `apply_start_bookmark` overrides
/// the config's `start_from` (`resume_after(token)`).
///
/// `assert_bookmark_roundtrip` drives the SAME source twice. Because
/// `start_from = now`, the source captures from the moment the change stream
/// opens — so, exactly as in the bounded-memory check, a concurrent writer
/// inserts the N documents ~2 s after the source is built (i.e. after the first
/// drain's change stream is live). The first drain captures those N change
/// events and emits a final `resumeToken`; the battery applies it and re-drains.
/// No new writes occur between the two drains, so the resumed run streams zero
/// records — strictly fewer than the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_bookmark_roundtrip() {
    // Modest count: enough to flush at least one full page plus a trailing
    // partial one, without making the test slow.
    const N: usize = 300;

    let (_container, uri) = start_repl_set().await;

    // Create the collection up front (before the change stream opens). This seed
    // insert happens before `start_from = now`, so it is not captured.
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

    // Concurrent writer: wait ~2 s for the change stream to open, then insert N
    // documents (each a distinct change event). All N land inside the FIRST
    // drain's window; the source's idle_timeout ends that drain (flushing the
    // trailing partial page with the final resumeToken) once the writer goes
    // quiet. No further writes occur, so the battery's second (resumed) drain
    // yields 0 records.
    let writer_uri = uri.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let client = Client::with_uri_str(&writer_uri)
            .await
            .expect("writer client");
        let coll = client.database(DB).collection::<Document>(COLL);
        for i in 1..=N {
            coll.insert_one(doc! { "_id": i as i64, "name": "n" })
                .await
                .expect("insert");
        }
    });

    assert_bookmark_roundtrip(&source).await;

    writer.await.expect("writer task");
}

// ── Check 6: errors are typed, not panics (Docker) ───────────────────────────

/// `MongoCdcSource::new` is **not** lazy — it opens a client and runs a `hello`
/// to verify the deployment supports Change Streams, so an unreachable address
/// would fail in `new()` rather than at read time. Following the battery's
/// contract (the source must *build* successfully, then *fail at read*), we boot
/// a real replica set so `new()`/`hello` passes, but supply an **invalid
/// aggregation-pipeline stage** (`$notARealStage`). It is well-formed BSON (so
/// `build_pipeline` accepts it), but the server rejects it deterministically
/// when the change stream is opened — at `watch().await`, before any event is
/// needed — so `fetch_all` and the first `stream_pages` poll each surface a
/// typed `FaucetError` (`mongodb-cdc watch failed: …`) without unwinding, which
/// the battery verifies. This is independent of writes/timing, unlike an
/// unreachable broker (which for a change-stream idle-times-out to an empty
/// success rather than an error).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let (_container, uri) = start_repl_set().await;

    // Create the collection so the watch has an existing namespace.
    {
        let client = Client::with_uri_str(&uri).await.expect("seed client");
        client
            .database(DB)
            .collection::<Document>(COLL)
            .insert_one(doc! { "_id": 0, "seed": true })
            .await
            .expect("seed insert");
    }

    // Valid connection (hello passes) but an aggregation stage the server
    // rejects at change-stream open, so opening the change stream fails at read.
    let config: MongoCdcSourceConfig = serde_json::from_value(json!({
        "connection_uri": uri,
        "scope": { "type": "collection", "database": DB, "collection": COLL },
        "start_from": { "type": "now" },
        "idle_timeout": 20,
        "max_await_time_ms": 500,
        "batch_size": 250,
        // Well-formed BSON, but not a real aggregation stage — MongoDB errors
        // when the change stream is opened.
        "aggregation_pipeline": [ { "$notARealStage": {} } ],
    }))
    .expect("config");

    let source = MongoCdcSource::new(config)
        .await
        .expect("new + hello succeed against a live replica set");
    assert_errors_not_panics(&source).await;
}
