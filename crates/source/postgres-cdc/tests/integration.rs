//! Integration tests for `PostgresCdcSource` against a real Postgres instance
//! via testcontainers.
//!
//! These tests require Docker. Set `RUST_LOG=info,pgwire_replication=warn`
//! while debugging. Postgres is started with `wal_level=logical`, two
//! replication slots, and two WAL senders — the minimum to actually
//! replicate.
//!
//! Each test boots its own container and creates schema/publication/slot
//! independently, so the tests are fully isolated.

use faucet_core::Source;
use faucet_source_postgres_cdc::{PostgresCdcSource, PostgresCdcSourceConfig};
use std::time::Duration;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::NoTls;

/// Start a Postgres container with `wal_level=logical` and return both the
/// container handle and a connection URL.
///
/// We use PostgreSQL 16 because pgwire-replication 0.3.2 emits
/// `messages 'true'` in its START_REPLICATION command, which requires PG 14+.
///
/// We use `with_host_auth()` (POSTGRES_HOST_AUTH_METHOD=trust) so that
/// `pgwire-replication` (which does not enable its optional `md5` feature in
/// this workspace) can connect without password auth. Trust mode is safe for
/// ephemeral, fully-isolated test containers.
async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    // `with_host_auth()` must be called before `with_tag()` because
    // `with_host_auth` is defined on `Postgres` and `with_tag` converts it
    // into a `ContainerRequest<Postgres>` (testcontainers extension trait).
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
    // Trust auth — no password required. The URL still includes the user so
    // pgwire-replication and tokio-postgres can identify the role.
    let url = format!("postgres://postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// Open a non-replication connection and run one or more SQL statements.
async fn ddl(url: &str, sql: &str) {
    let (client, conn) = tokio_postgres::connect(url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client.batch_execute(sql).await.expect("batch execute");
}

fn cfg(url: &str, slot: &str, publication: &str) -> PostgresCdcSourceConfig {
    PostgresCdcSourceConfig {
        connection_url: url.into(),
        slot_name: slot.into(),
        publication_name: publication.into(),
        create_slot_if_missing: true,
        slot_type: faucet_source_postgres_cdc::SlotType::Permanent,
        tls: faucet_source_postgres_cdc::CdcTls::Disable,
        start_lsn: None,
        proto_version: 1,
        idle_timeout: Duration::from_secs(5),
        max_messages: None,
        max_cycle_duration: std::time::Duration::from_secs(300),
        max_staged_records: None,
        max_buffered_events: 100_000,
        status_update_interval: Duration::from_secs(1),
        tcp_keepalive: Duration::from_secs(60),
        batch_size: faucet_core::DEFAULT_BATCH_SIZE,
        slot_acquire_retries: 10,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn insert_round_trip() {
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.users (id int4 PRIMARY KEY, name text); \
         CREATE PUBLICATION faucet_pub FOR TABLE public.users;",
    )
    .await;

    let source = PostgresCdcSource::new(cfg(&url, "faucet_slot", "faucet_pub"))
        .await
        .expect("source");

    // Warm-up fetch: creates the slot, idle_timeout drains 0 records.
    let _ = source.fetch_all_incremental().await.expect("warm-up");

    // Apply changes AFTER the slot exists so they end up in the replicated WAL.
    ddl(
        &url,
        "INSERT INTO public.users VALUES (1, 'alice'), (2, 'bob');",
    )
    .await;

    let (records, bookmark) = source.fetch_all_incremental().await.expect("fetch");
    let inserts: Vec<_> = records.iter().filter(|r| r["op"] == "insert").collect();
    assert_eq!(
        inserts.len(),
        2,
        "expected 2 inserts, got records: {records:?}"
    );
    assert_eq!(inserts[0]["schema"], "public");
    assert_eq!(inserts[0]["table"], "users");
    assert_eq!(inserts[0]["after"]["id"], 1);
    assert_eq!(inserts[0]["after"]["name"], "alice");
    assert_eq!(inserts[1]["after"]["id"], 2);
    assert!(
        bookmark.is_some(),
        "bookmark must be Some after committed txns"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn update_and_delete_emit_before_after() {
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.users (id int4 PRIMARY KEY, name text); \
         ALTER TABLE public.users REPLICA IDENTITY FULL; \
         CREATE PUBLICATION faucet_pub FOR TABLE public.users; \
         INSERT INTO public.users VALUES (1, 'alice');",
    )
    .await;

    let source = PostgresCdcSource::new(cfg(&url, "faucet_slot", "faucet_pub"))
        .await
        .expect("source");

    // Warm-up: materialise the slot. The INSERT above predates the slot and
    // will NOT appear in the stream — that's intentional; the test only
    // asserts UPDATE+DELETE behaviour.
    let _ = source.fetch_all_incremental().await.expect("warm-up");

    ddl(
        &url,
        "UPDATE public.users SET name = 'alice2' WHERE id = 1; \
         DELETE FROM public.users WHERE id = 1;",
    )
    .await;

    let (records, _bm) = source.fetch_all_incremental().await.expect("fetch");
    let update = records
        .iter()
        .find(|r| r["op"] == "update")
        .expect("update record");
    let delete = records
        .iter()
        .find(|r| r["op"] == "delete")
        .expect("delete record");

    assert_eq!(update["before"]["id"], 1);
    assert_eq!(update["before"]["name"], "alice");
    assert_eq!(update["after"]["name"], "alice2");
    assert_eq!(delete["before"]["id"], 1);
    assert_eq!(delete["after"], serde_json::Value::Null);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_slot_with_create_if_missing_creates_it() {
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.t (id int PRIMARY KEY); \
         CREATE PUBLICATION faucet_pub FOR TABLE public.t;",
    )
    .await;

    let mut c = cfg(&url, "fresh_slot", "faucet_pub");
    c.create_slot_if_missing = true;
    let source = PostgresCdcSource::new(c).await.expect("source");
    // First fetch should succeed even though no slot exists yet.
    let _ = source.fetch_all_incremental().await.expect("fetch");

    // Verify the slot now exists by querying pg_replication_slots directly.
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = client
        .query_one(
            "SELECT 1::int4 FROM pg_replication_slots WHERE slot_name = $1",
            &[&"fresh_slot"],
        )
        .await
        .expect("slot exists");
    assert_eq!(row.get::<_, i32>(0), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_slot_removes_the_slot() {
    // #78/#12: drop_slot must remove a permanent slot (freeing its WAL).
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.t (id int PRIMARY KEY); \
         CREATE PUBLICATION faucet_pub FOR TABLE public.t;",
    )
    .await;

    let source = PostgresCdcSource::new(cfg(&url, "drop_me", "faucet_pub"))
        .await
        .expect("source");
    let _ = source
        .fetch_all_incremental()
        .await
        .expect("warm-up creates slot");

    // Slot exists now.
    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let before: i64 = client
        .query_one(
            "SELECT count(*)::int8 FROM pg_replication_slots WHERE slot_name = $1",
            &[&"drop_me"],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, 1, "slot should exist after warm-up");

    source.drop_slot().await.expect("drop_slot");

    let after: i64 = client
        .query_one(
            "SELECT count(*)::int8 FROM pg_replication_slots WHERE slot_name = $1",
            &[&"drop_me"],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(after, 0, "slot must be gone after drop_slot");

    // Dropping again is a no-op (not an error).
    source.drop_slot().await.expect("drop_slot is idempotent");
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_slot_without_create_if_missing_errors() {
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.t (id int PRIMARY KEY); \
         CREATE PUBLICATION faucet_pub FOR TABLE public.t;",
    )
    .await;

    let mut c = cfg(&url, "no_slot_here", "faucet_pub");
    c.create_slot_if_missing = false;
    let source = PostgresCdcSource::new(c).await.expect("source");
    let err = source.fetch_all_incremental().await.unwrap_err();
    assert!(
        format!("{err}").contains("no_slot_here"),
        "error must mention the slot name: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn lsn_not_advanced_without_durable_bookmark_redelivers() {
    // #78/#1: the slot's `confirmed_flush_lsn` must not advance past changes
    // the consumer has not durably persisted. We drain a transaction in run 1
    // but never carry its bookmark into run 2 — simulating a crash before the
    // sink flush + bookmark persist. Run 2 (a fresh source with no
    // `apply_start_bookmark`) must REDELIVER the change rather than skip it
    // (at-least-once: no data loss).
    //
    // Pre-fix, the keepalive handler advanced the applied-LSN to the server's
    // `wal_end`, so `confirmed_flush_lsn` moved past the change during run 1
    // and run 2 started after it — losing the row. The fix advances the
    // advertised LSN only from a durable bookmark, so run 2 redelivers.
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.users (id int4 PRIMARY KEY, name text); \
         CREATE PUBLICATION faucet_pub FOR TABLE public.users;",
    )
    .await;

    // Run 1: create the slot, then drain one inserted transaction.
    let s1 = PostgresCdcSource::new(cfg(&url, "faucet_slot", "faucet_pub"))
        .await
        .expect("source 1");
    let _ = s1.fetch_all_incremental().await.expect("warm-up"); // materialise slot
    ddl(&url, "INSERT INTO public.users VALUES (1, 'alice');").await;
    let (first, bookmark) = s1.fetch_all_incremental().await.expect("first fetch");
    let first_ids: Vec<_> = first
        .iter()
        .filter(|r| r["op"] == "insert")
        .map(|r| r["after"]["id"].as_i64().unwrap())
        .collect();
    assert_eq!(first_ids, vec![1], "run 1 must drain the insert");
    assert!(bookmark.is_some(), "run 1 must produce a bookmark");
    // Drop s1 WITHOUT applying/persisting its bookmark — simulates a crash
    // before durability was confirmed.
    drop(s1);

    // Run 2: fresh source, NO apply_start_bookmark → resumes from the slot's
    // confirmed_flush_lsn. It must still see row 1.
    let s2 = PostgresCdcSource::new(cfg(&url, "faucet_slot", "faucet_pub"))
        .await
        .expect("source 2");
    let (second, _bm) = s2.fetch_all_incremental().await.expect("second fetch");
    let second_ids: Vec<_> = second
        .iter()
        .filter(|r| r["op"] == "insert")
        .map(|r| r["after"]["id"].as_i64().unwrap())
        .collect();
    assert!(
        second_ids.contains(&1),
        "run 2 must redeliver row 1 (no data loss without a persisted bookmark); \
         got {second_ids:?} from records {second:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_from_bookmark_skips_already_consumed() {
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.users (id int4 PRIMARY KEY, name text); \
         CREATE PUBLICATION faucet_pub FOR TABLE public.users;",
    )
    .await;

    // First pass: drain rows 1+2 and capture the bookmark.
    let s1 = PostgresCdcSource::new(cfg(&url, "faucet_slot", "faucet_pub"))
        .await
        .expect("source 1");
    // Warm-up creates the slot before our INSERTs.
    let _ = s1.fetch_all_incremental().await.expect("warm-up");
    ddl(&url, "INSERT INTO public.users VALUES (1, 'a'), (2, 'b');").await;
    let (first, bookmark) = s1.fetch_all_incremental().await.expect("first fetch");
    let first_ids: Vec<_> = first
        .iter()
        .filter(|r| r["op"] == "insert")
        .map(|r| r["after"]["id"].as_i64().unwrap())
        .collect();
    assert_eq!(first_ids, vec![1, 2], "first drain must return 1 and 2");
    let bm = bookmark.expect("bookmark must be set after first fetch");

    // Now apply rows 3+4. We construct a fresh source and feed it the
    // bookmark before the second fetch.
    ddl(&url, "INSERT INTO public.users VALUES (3, 'c'), (4, 'd');").await;
    let s2 = PostgresCdcSource::new(cfg(&url, "faucet_slot", "faucet_pub"))
        .await
        .expect("source 2");
    s2.apply_start_bookmark(bm).await.expect("apply bookmark");
    let (second, _bm2) = s2.fetch_all_incremental().await.expect("second fetch");
    let second_ids: Vec<_> = second
        .iter()
        .filter(|r| r["op"] == "insert")
        .map(|r| r["after"]["id"].as_i64().unwrap())
        .collect();
    assert_eq!(
        second_ids,
        vec![3, 4],
        "resume must skip 1 and 2; got {second_ids:?} from records {second:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn capture_resume_position_returns_current_lsn_and_creates_slot() {
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.users (id int4 PRIMARY KEY, name text); \
         CREATE PUBLICATION faucet_pub FOR TABLE public.users;",
    )
    .await;

    let source = PostgresCdcSource::new(cfg(&url, "cap_slot", "faucet_pub"))
        .await
        .expect("source");

    let pos = source
        .capture_resume_position()
        .await
        .expect("capture")
        .expect("postgres-cdc must support capture");
    // Shape: { "last_lsn": "X/Y" }
    let lsn = pos
        .get("last_lsn")
        .and_then(|v| v.as_str())
        .expect("last_lsn");
    assert!(lsn.contains('/'), "expected an LSN like X/Y, got {lsn}");

    // The slot was created as a side effect (so WAL from `pos` is retained).
    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = client
        .query_one(
            "SELECT count(*)::int4 FROM pg_replication_slots WHERE slot_name = $1",
            &[&"cap_slot"],
        )
        .await
        .expect("slot query");
    let n: i32 = row.get(0);
    assert_eq!(n, 1, "capture must have created the slot");
}

#[tokio::test(flavor = "multi_thread")]
async fn capture_over_existing_slot_anchors_on_slot_consistent_point() {
    // F8 regression: capturing a resume position over a slot that ALREADY EXISTS
    // with unconsumed WAL must anchor on the slot's retained consistent point
    // (confirmed_flush_lsn), NOT the server's current WAL position — otherwise
    // the retained change window is silently skipped (lost CDC events).
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.users (id int4 PRIMARY KEY, name text); \
         CREATE PUBLICATION faucet_pub FOR TABLE public.users;",
    )
    .await;

    // First capture creates the slot and anchors it at its consistent point.
    let s1 = PostgresCdcSource::new(cfg(&url, "exist_slot", "faucet_pub"))
        .await
        .expect("source");
    s1.capture_resume_position()
        .await
        .expect("capture1")
        .expect("supported");
    drop(s1);

    // Generate WAL the slot retains but nobody consumed (no feedback is sent, so
    // confirmed_flush_lsn stays put while pg_current_wal_lsn advances).
    for i in 0..50 {
        ddl(
            &url,
            &format!("INSERT INTO public.users(id, name) VALUES ({i}, 'n{i}')"),
        )
        .await;
    }
    ddl(&url, "SELECT pg_switch_wal()").await;

    // Capture again over the SAME pre-existing slot.
    let s2 = PostgresCdcSource::new(cfg(&url, "exist_slot", "faucet_pub"))
        .await
        .expect("source");
    let pos = s2
        .capture_resume_position()
        .await
        .expect("capture2")
        .expect("supported");
    let captured = pos
        .get("last_lsn")
        .and_then(|v| v.as_str())
        .expect("last_lsn")
        .to_string();

    let (client, conn) = tokio_postgres::connect(&url, NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = client
        .query_one(
            "SELECT confirmed_flush_lsn::text, pg_current_wal_lsn()::text \
             FROM pg_replication_slots WHERE slot_name = $1",
            &[&"exist_slot"],
        )
        .await
        .expect("slot lsn query");
    let confirmed: String = row.get(0);
    let current: String = row.get(1);
    assert_eq!(
        captured, confirmed,
        "capture must anchor on the slot's retained confirmed_flush_lsn"
    );
    assert_ne!(
        confirmed, current,
        "test setup invariant: WAL must have advanced past the slot's confirmed position"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn temporary_slots_are_refused_at_config_load() {
    let (_pg, url) = start_postgres().await;
    let mut c = cfg(&url, "temp_slot", "faucet_pub");
    c.slot_type = faucet_source_postgres_cdc::SlotType::Temporary;
    let err = PostgresCdcSource::new(c)
        .await
        .err()
        .expect("temporary is refused");
    assert!(
        err.to_string()
            .contains("slot_type: temporary is not supported"),
        "expected the temporary-slot refusal, got: {err}"
    );
}

/// #733 — a stuck consumer's slot lag grows as WAL piles up behind it, and
/// shrinks once the changes are read; a slot that does not exist has no lag.
#[tokio::test(flavor = "multi_thread")]
async fn slot_lag_grows_while_unconsumed_and_shrinks_once_read() {
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.lagged (id int4 PRIMARY KEY, pad text); \
         CREATE PUBLICATION lag_pub FOR TABLE public.lagged;",
    )
    .await;

    let missing = PostgresCdcSource::new(PostgresCdcSourceConfig {
        create_slot_if_missing: false,
        ..cfg(&url, "no_such_slot", "lag_pub")
    })
    .await
    .expect("source");
    assert_eq!(missing.lag().await.expect("lag"), None);

    let source = PostgresCdcSource::new(cfg(&url, "lag_slot", "lag_pub"))
        .await
        .expect("source");
    let _ = source.fetch_all_incremental().await.expect("warm-up");
    let idle = source.lag().await.expect("lag").expect("slot exists");

    ddl(
        &url,
        "INSERT INTO public.lagged SELECT g, repeat('x', 512) FROM generate_series(1, 500) g;",
    )
    .await;
    let stuck = source.lag().await.expect("lag").expect("slot exists");
    assert!(
        stuck.bytes.unwrap() > idle.bytes.unwrap() + 500 * 512,
        "the unread insert shows up as slot lag: {stuck:?} vs {idle:?}"
    );
    ddl(
        &url,
        "INSERT INTO public.lagged SELECT g, repeat('y', 512) FROM generate_series(501, 1000) g;",
    )
    .await;
    let stuck_more = source.lag().await.expect("lag").expect("slot exists");
    assert!(
        stuck_more.bytes > stuck.bytes,
        "lag keeps growing while nothing consumes: {stuck_more:?} vs {stuck:?}"
    );

    let (records, _) = source.fetch_all_incremental().await.expect("fetch");
    assert_eq!(records.iter().filter(|r| r["op"] == "insert").count(), 1000);
    let caught_up = source.lag().await.expect("lag").expect("slot exists");
    assert!(
        caught_up.bytes < stuck_more.bytes,
        "reading the changes shrinks the lag: {caught_up:?} vs {stuck_more:?}"
    );
}

/// #789 SQL-08: under writes arriving faster than `idle_timeout`, a fetch
/// cycle still ends once it has run for `max_cycle_duration`, so the next
/// cycle can advance the slot. Before, the cycle never returned and the slot
/// pinned WAL without bound.
#[tokio::test(flavor = "multi_thread")]
async fn a_cycle_under_steady_writes_ends_at_its_bound() {
    let (_pg, url) = start_postgres().await;
    ddl(
        &url,
        "CREATE TABLE public.ticks (id serial PRIMARY KEY, at timestamptz DEFAULT now()); \
         CREATE PUBLICATION tick_pub FOR TABLE public.ticks;",
    )
    .await;
    let mut config = cfg(&url, "tick_slot", "tick_pub");
    config.idle_timeout = Duration::from_secs(30);
    config.max_cycle_duration = Duration::from_secs(3);
    let source = PostgresCdcSource::new(config).await.expect("source");
    let _ = source.fetch_all_incremental().await.expect("warm-up");

    let writer_url = url.clone();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer_stop = stop.clone();
    let writer = tokio::spawn(async move {
        let (client, conn) = tokio_postgres::connect(&writer_url, NoTls)
            .await
            .expect("connect");
        tokio::spawn(async move {
            let _ = conn.await;
        });
        while !writer_stop.load(std::sync::atomic::Ordering::Relaxed) {
            client
                .batch_execute("INSERT INTO public.ticks DEFAULT VALUES")
                .await
                .expect("tick");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    let started = std::time::Instant::now();
    let (records, bookmark) =
        tokio::time::timeout(Duration::from_secs(20), source.fetch_all_incremental())
            .await
            .expect("the cycle must end well before idle_timeout under steady writes")
            .expect("fetch");
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.await.expect("writer");
    assert!(started.elapsed() < Duration::from_secs(15));
    assert!(!records.is_empty());
    assert!(bookmark.is_some());
}
