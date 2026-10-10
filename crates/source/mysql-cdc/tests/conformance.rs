//! `faucet-conformance` battery for the MySQL CDC source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value
//!          (pure, offline — always runs).
//! Check 2: `stream_pages` bounds peak memory. Like postgres-cdc, the mysql-cdc
//!          source emits **one page per committed transaction** in
//!          per-transaction mode (`batch_size > 0`); a transaction is never
//!          split across pages. We therefore seed many *small, single-row*
//!          autocommitted `INSERT`s — each is its own transaction and hence its
//!          own one-record page — so the peak page (1) is ≤ the batch cap (250)
//!          and strictly < the total, while exercising the real binlog +
//!          decode + paging path. Because the source starts from the *current*
//!          binlog position when the stream opens, the writes must happen
//!          *after* the stream is live, so a concurrent writer task performs
//!          them while `assert_bounded_memory` drives the stream; the source's
//!          `idle_timeout` (5 s of quiet) terminates the drain once the writer
//!          finishes.

use faucet_conformance::{
    assert_bookmark_roundtrip, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_source_mysql_cdc::{MysqlCdcSource, MysqlCdcSourceConfig};
use mysql_async::{Conn, Opts, prelude::Queryable};
use serde_json::json;
use std::time::Duration;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::mysql::Mysql;

const BATCH: usize = 250;
// Comfortably exceeds BATCH (task guidance: batch + a few hundred). Each of
// these is a separate single-row transaction → a separate one-record page.
const TOTAL: usize = 600;

// ── Check 1: config schema validity (offline) ────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(MysqlCdcSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-mysql-cdc");
}

// ── Check 2: bounded-memory streaming (Docker) ───────────────────────────────

async fn start_mysql_cdc() -> (ContainerAsync<Mysql>, String) {
    let container = faucet_conformance::containers::start(|| {
        Mysql::default().with_tag("8.1").with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-row-metadata=FULL",
        ])
    })
    .await;
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

async fn connect(url: &str) -> Conn {
    Conn::new(Opts::from_url(url).expect("parse URL"))
        .await
        .expect("connect")
}

fn build_config(url: &str) -> MysqlCdcSourceConfig {
    serde_json::from_value(json!({
        "connection_url": url,
        "server_id": 5005,
        "start_position": { "type": "current" },
        "idle_timeout": 20,
        // Per-transaction paging: batch_size > 0 turns on per-commit page
        // emission (the config field is authoritative; the trait-level
        // batch_size argument is ignored by this source).
        "batch_size": BATCH,
    }))
    .expect("config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_bounded_memory() {
    let (_container, url) = start_mysql_cdc().await;

    // Pre-create the table BEFORE building the source so `start_position =
    // current` sits after the DDL (the CREATE is not captured).
    {
        let mut conn = connect(&url).await;
        conn.query_drop("CREATE TABLE test.events (id INT PRIMARY KEY)")
            .await
            .expect("create table");
    }

    let source = MysqlCdcSource::new(build_config(&url))
        .await
        .expect("source new");

    // Check 10: connector_name is non-empty (pure — no I/O).
    assert_connector_name_nonempty(&source);
    // Check 11: preflight check() is well-formed against the live server. The
    // CDC check() is overridden to run a read-only connect + binlog-config probe
    // (never a page pull), so it is safe to call in the live test.
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;

    // Concurrent writer: wait ~2 s for the binlog stream to open, then perform
    // TOTAL single-row autocommitted INSERTs — each its own transaction, hence
    // its own one-record page. After the writer goes quiet the source's
    // idle_timeout ends the drain.
    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let mut conn = connect(&writer_url).await;
        for i in 0..TOTAL {
            conn.query_drop(format!("INSERT INTO test.events (id) VALUES ({i})"))
                .await
                .expect("insert");
        }
    });

    // Each committed transaction is its own page (1 record) → peak == 1, which
    // is ≤ BATCH (250) and < TOTAL (600).
    assert_bounded_memory(&source, BATCH, TOTAL).await;

    writer.await.expect("writer task");
}

// ── Check 3: bookmark round-trip (Docker) ────────────────────────────────────

/// The mysql-cdc source is resumable via a `{file, pos}` binlog bookmark: each
/// committed transaction is streamed as its own page carrying
/// `bookmark = Some(file_pos)`, and a persisted bookmark applied via
/// `apply_start_bookmark` always overrides the config's `start_position`.
///
/// `assert_bookmark_roundtrip` drives the SAME source twice. Because
/// `start_position = current`, the source captures from the binlog position at
/// stream-open — so, exactly as in the bounded-memory check, a concurrent
/// writer performs the inserts ~2 s after the source is built (i.e. after the
/// first drain's binlog stream is live). The first drain captures those N
/// committed transactions and emits a final `{file, pos}` bookmark; the battery
/// applies it and re-drains. No new writes occur between the two drains, so the
/// resumed run streams zero records — strictly fewer than the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conformance_bookmark_roundtrip() {
    // Modest count: enough to prove multiple committed transactions round-trip
    // through the bookmark without making the test slow.
    const N: usize = 300;

    let (_container, url) = start_mysql_cdc().await;

    // Pre-create the table BEFORE building the source so `start_position =
    // current` sits after the DDL (the CREATE is not captured).
    {
        let mut conn = connect(&url).await;
        conn.query_drop("CREATE TABLE test.events (id INT PRIMARY KEY)")
            .await
            .expect("create table");
    }

    let source = MysqlCdcSource::new(build_config(&url))
        .await
        .expect("source new");

    // Concurrent writer: wait ~2 s for the binlog stream to open, then perform N
    // single-row autocommitted INSERTs — each its own transaction, hence its own
    // one-record page with a per-commit {file, pos} bookmark. All N land inside
    // the FIRST drain's window; the source's idle_timeout ends that drain once
    // the writer goes quiet. No further writes occur, so the battery's second
    // (resumed) drain yields 0 records.
    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let mut conn = connect(&writer_url).await;
        for i in 0..N {
            conn.query_drop(format!("INSERT INTO test.events (id) VALUES ({i})"))
                .await
                .expect("insert");
        }
    });

    assert_bookmark_roundtrip(&source).await;

    writer.await.expect("writer task");
}

// ── Check 6: errors are typed, not panics (Docker) ───────────────────────────

/// `MysqlCdcSource::new` is **not** lazy — it opens a connection and runs a
/// server-variable preflight, so an unreachable address would fail in `new()`
/// rather than at read time. Following the battery's contract (the source must
/// *build* successfully, then *fail at read*), we boot a real server so
/// `new()`/preflight passes, but point `start_position` at a **non-existent
/// binlog file**. Preflight does not validate the start file/pos, so the source
/// builds; opening the binlog stream (the first `stream_pages` poll and
/// `fetch_all`) then fails with a typed `FaucetError` — MySQL rejects the
/// COM_BINLOG_DUMP for a log file it cannot find. The battery verifies this
/// surfaces without unwinding.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let (_container, url) = start_mysql_cdc().await;

    // Pre-create a table so the server is fully initialised; not strictly
    // required, but keeps the setup identical to the other checks.
    {
        let mut conn = connect(&url).await;
        conn.query_drop("CREATE TABLE test.events (id INT PRIMARY KEY)")
            .await
            .expect("create table");
    }

    // Valid connection (preflight passes) but a binlog file that does not exist,
    // so START_REPLICATION / get_binlog_stream fails at read.
    let config: MysqlCdcSourceConfig = serde_json::from_value(json!({
        "connection_url": url,
        "server_id": 6006,
        "start_position": { "type": "file_pos", "file": "faucet-nonexistent-bin.999999", "pos": 4 },
        "idle_timeout": 20,
        "batch_size": 250,
    }))
    .expect("config");

    let source = MysqlCdcSource::new(config)
        .await
        .expect("new + preflight succeed against a live server");
    assert_errors_not_panics(&source).await;
}
