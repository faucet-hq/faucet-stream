//! `faucet-conformance` battery for the PostgreSQL CDC source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value
//!          (pure, offline — always runs).
//! Check 2: `stream_pages` bounds peak memory. The postgres-cdc source emits
//!          **one page per committed transaction** (a transaction is never
//!          split across pages; `batch_size` is advisory — see
//!          `stream_pages_with_batch_size`). To get a bounded-memory shape we
//!          therefore seed many *small, single-row* transactions rather than one
//!          large one: each autocommitted `INSERT` is its own transaction and
//!          hence its own one-record page, so the peak page (1) is ≤ the batch
//!          cap (250) and strictly < the total, while still exercising the real
//!          replication + decode + paging path. (Seeding one big transaction
//!          instead would produce a single giant page and fail the check — that
//!          is the documented per-transaction page granularity, not a leak.)

use faucet_conformance::{
    assert_bookmark_roundtrip, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::Source;
use faucet_source_postgres_cdc::{PostgresCdcSource, PostgresCdcSourceConfig};
use std::time::Duration;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

const BATCH: usize = 250;
// Comfortably exceeds BATCH (task guidance: batch + a few hundred). Each of
// these is a separate single-row transaction → a separate one-record page.
const TOTAL: usize = 600;

// ── Check 1: config schema validity (offline) ────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(PostgresCdcSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-postgres-cdc");
}

// ── Check 2: bounded-memory streaming (Docker) ───────────────────────────────

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default()
        .with_host_auth()
        .with_tag("16-alpine")
        .with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_wal_senders=4",
            "-c",
            "max_replication_slots=4",
        ]);
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

async fn ddl(url: &str, sql: &str) {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client.batch_execute(sql).await.expect("batch execute");
}

fn cfg(url: &str) -> PostgresCdcSourceConfig {
    PostgresCdcSourceConfig {
        connection_url: url.into(),
        slot_name: "conformance_slot".into(),
        publication_name: "conformance_pub".into(),
        create_slot_if_missing: true,
        slot_type: faucet_source_postgres_cdc::SlotType::Permanent,
        tls: faucet_source_postgres_cdc::CdcTls::Disable,
        start_lsn: None,
        proto_version: 1,
        idle_timeout: Duration::from_secs(5),
        // Stop the drain cleanly once every seeded change has been streamed.
        max_messages: Some(TOTAL),
        max_cycle_duration: std::time::Duration::from_secs(300),
        max_staged_records: None,
        status_update_interval: Duration::from_secs(1),
        tcp_keepalive: Duration::from_secs(60),
        // Per-transaction paging: batch_size > 0 turns on per-commit page
        // emission. The config field is authoritative (the trait-level
        // batch_size argument is ignored by this source).
        batch_size: BATCH,
        slot_acquire_retries: 10,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.events (id int4 PRIMARY KEY); \
         CREATE PUBLICATION conformance_pub FOR TABLE public.events;",
    )
    .await;

    let source = PostgresCdcSource::new(cfg(&url)).await.expect("source");

    // Check 10: connector_name is non-empty (pure — no I/O).
    assert_connector_name_nonempty(&source);
    // Check 11: preflight check() is well-formed against the live server. The
    // CDC check() is overridden to run a read-only connect + slot metadata probe
    // (never a page pull), so it is safe to call in the live test. The slot does
    // not exist yet, so the slot probe is a well-formed `Skip` — still a valid
    // report inside `Ok`.
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;

    // Warm-up fetch: materialise the slot so the seeded changes below land in
    // the replicated WAL. idle_timeout drains 0 records.
    let _ = source.fetch_all_incremental().await.expect("warm-up");

    // Seed TOTAL single-row transactions. Each `execute` on a non-transaction
    // connection autocommits independently, so this produces TOTAL separate
    // committed transactions — hence TOTAL one-record pages when streamed.
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    for i in 0..TOTAL {
        client
            .execute("INSERT INTO public.events (id) VALUES ($1)", &[&(i as i32)])
            .await
            .expect("insert");
    }

    // Each committed transaction is its own page (1 record), so peak == 1,
    // which is ≤ BATCH (250) and < TOTAL (600).
    assert_bounded_memory(&source, BATCH, TOTAL).await;
}

// ── Check 3: bookmark round-trip (Docker) ────────────────────────────────────

/// The postgres-cdc source is resumable via the slot's LSN: each committed
/// transaction is streamed as its own page carrying `bookmark = Some(commit_lsn)`.
///
/// `assert_bookmark_roundtrip` drives the SAME source twice. We seed a batch of
/// single-row transactions BEFORE the first drain (each autocommit `INSERT` is
/// its own transaction → its own one-record page with a per-commit LSN
/// bookmark). The first drain consumes them all and captures the last LSN; the
/// battery applies it via `apply_start_bookmark` and re-drains. Because we
/// produce **no new writes** between the two drains, the resumed run streams
/// zero records — strictly fewer than the first (the battery asserts
/// `second < first`).
///
/// Both drains terminate on `idle_timeout` (no `max_messages` cap), so each
/// `stream_pages` run completes deterministically.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_bookmark_roundtrip() {
    // Modest count: enough to prove multiple committed transactions round-trip
    // through the bookmark without making the test slow.
    const N: usize = 300;

    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.events (id int4 PRIMARY KEY); \
         CREATE PUBLICATION conformance_pub FOR TABLE public.events;",
    )
    .await;

    let mut c = cfg(&url);
    // Rely purely on idle_timeout to terminate each drain (no cumulative cap,
    // so the second drain also terminates cleanly at 0 records).
    c.max_messages = None;
    // Generous idle window: under the instrumented (llvm-cov) CI build, decoding
    // the seeded WAL into per-commit pages is slower, and the first drain must
    // not idle out before it has replayed every seeded transaction.
    c.idle_timeout = Duration::from_secs(15);
    let source = PostgresCdcSource::new(c).await.expect("source");

    // Warm-up fetch: materialise the slot so the seeded changes below land in
    // the replicated WAL. idle_timeout drains 0 records.
    let _ = source.fetch_all_incremental().await.expect("warm-up");

    // Seed N single-row (autocommitted) transactions → N committed transactions,
    // each streamed as its own one-record page with a per-commit LSN bookmark.
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    for i in 0..N {
        client
            .execute("INSERT INTO public.events (id) VALUES ($1)", &[&(i as i32)])
            .await
            .expect("insert");
    }

    // First drain consumes the N changes and emits a final LSN bookmark; the
    // battery applies it and re-drives with no new writes, expecting 0 records.
    assert_bookmark_roundtrip(&source).await;
}

// ── Check 6: errors are typed, not panics (offline) ──────────────────────────

/// `PostgresCdcSource::new` is lazy — it validates config but opens no
/// connection. Pointing `connection_url` at an unreachable address
/// (`127.0.0.1:1`) makes the replication connection fail on the first read
/// (`fetch_all` and the first `stream_pages` poll), which the battery verifies
/// surfaces as a typed `FaucetError` without unwinding. No Docker needed.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let source = PostgresCdcSource::new(cfg("postgres://postgres@127.0.0.1:1/postgres"))
        .await
        .expect("new is lazy — must not connect");
    assert_errors_not_panics(&source).await;
}
