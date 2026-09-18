//! #651 Category E1 — the container tier: a real connection cut mid-run.
//!
//! `crates/conformance/tests/reliability_fault_injection.rs` covers the engine
//! *contract* with a scripted source, Docker-free, on every PR. What it cannot
//! cover is the part that only a real driver exhibits: what `sqlx` does when
//! the server disappears underneath an open cursor, and whether that surfaces
//! as a typed error, a retry loop, or a hang.
//!
//! So this tier stops the Postgres container while a stream is open. That is a
//! harder cut than a proxy's latency or bandwidth toxic — the socket dies with
//! no FIN — and it needs no extra image, which keeps the suite runnable
//! anywhere Docker is.
//!
//! The obligations under test are the same three as the engine tier:
//!
//! 1. the run **fails**, rather than reporting success on partial data;
//! 2. it fails with a **typed** error naming the source, not a panic;
//! 3. it **terminates** — asserted under a hard ceiling, because a retry loop
//!    with no bound is indistinguishable from a hang to whoever is watching.

use std::time::Duration;

use faucet_conformance::doubles::TestSink;
use faucet_core::{Pipeline, Source};
use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// Hard ceiling for every run here. A cut connection must resolve well inside
/// this; exceeding it is the hang regression, reported as such rather than as
/// an opaque suite timeout.
const MUST_FINISH_WITHIN: Duration = Duration::from_secs(90);

async fn within<F: std::future::Future>(label: &str, fut: F) -> F::Output {
    match tokio::time::timeout(MUST_FINISH_WITHIN, fut).await {
        Ok(v) => v,
        Err(_) => panic!(
            "{label} did not finish within {MUST_FINISH_WITHIN:?}. A run that never \
             terminates after its connection dies is indistinguishable from a hang — an \
             operator cannot tell it from a slow pipeline, which is why this ceiling exists."
        ),
    }
}

async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container: ContainerAsync<Postgres> = Postgres::default()
        .with_tag("16-alpine")
        .start()
        .await
        .expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// Seed `rows` rows so a streaming read spans several pages.
async fn seed(url: &str, rows: i64) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool");
    sqlx::query("CREATE TABLE big (id BIGINT PRIMARY KEY, payload TEXT)")
        .execute(&pool)
        .await
        .expect("create");
    sqlx::query(
        "INSERT INTO big (id, payload) \
         SELECT g, repeat('x', 200) FROM generate_series(1, $1) AS g",
    )
    .bind(rows)
    .execute(&pool)
    .await
    .expect("seed");
    pool.close().await;
}

async fn source(url: &str, batch: usize) -> PostgresSource {
    PostgresSource::new(
        PostgresSourceConfig::new(url, "SELECT id, payload FROM big ORDER BY id")
            .with_batch_size(batch),
    )
    .await
    .expect("source")
}

/// A source whose read is *deterministically* slow, via `pg_sleep` per row, so
/// the connection can be cut at a known point mid-stream.
///
/// Without this the test was a race: 40k small rows stream in well under the
/// delay before the cut, the run completes, and the assertion fires on a
/// perfectly healthy run. Making the server itself the bottleneck removes the
/// timing assumption instead of tuning it.
async fn slow_source(url: &str, batch: usize) -> PostgresSource {
    PostgresSource::new(
        PostgresSourceConfig::new(
            url,
            "SELECT id, payload, pg_sleep(0.005) FROM big ORDER BY id",
        )
        .with_batch_size(batch),
    )
    .await
    .expect("source")
}

#[tokio::test]
async fn a_connection_cut_mid_stream_fails_the_run_and_terminates() {
    let (container, url) = start_postgres().await;
    // 3000 rows at 5ms each ≈ 15s of server-side work, so a cut at 1.5s is
    // comfortably mid-stream on any machine.
    let total = 3_000;
    seed(&url, total).await;

    let src = slow_source(&url, 100).await;
    let sink = TestSink::new();

    // Stop the server while a cursor is open.
    let killer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        container.stop().await.expect("stop the container");
        // Keep the handle alive until here so the container is not dropped (and
        // removed) before we mean to stop it.
        container
    });

    let outcome = within(
        "connection cut mid-stream",
        Pipeline::new(&src, &sink).run(),
    )
    .await;
    let _ = killer.await;

    let err = match outcome {
        Err(e) => e,
        Ok(result) => panic!(
            "the run reported SUCCESS after its connection was cut, having written {} of \
             {total} rows — a truncated dataset presented as complete is the worst outcome \
             available here",
            result.records_written
        ),
    };

    // The guarantee, which holds regardless of how the error is classified: it
    // is a typed `FaucetError` rather than a panic, and it names the cause so an
    // operator is not left guessing.
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("connection") || msg.contains("terminating") || msg.contains("closed"),
        "the failure must say the connection died: {err:?}"
    );

    // Known defect, tracked as #662: this arrives as `FaucetError::Config` —
    // the variant that means "your YAML is wrong" — when the config was fine
    // and the database went away. Nine SQL sources map runtime driver failures
    // that way.
    //
    // It is asserted here rather than waved through so the defect is visible
    // and this test tightens the moment #662 lands: today it is a diagnostics
    // problem (`is_retriable()` is false for both variants, so nothing retries
    // differently), but it becomes a reliability one as soon as a dropped
    // connection is made retriable, because `Config` is the one variant that
    // must never be.
    assert!(
        matches!(
            err,
            faucet_core::FaucetError::Config(_)
                | faucet_core::FaucetError::Source(_)
                | faucet_core::FaucetError::Custom(_)
        ),
        "unexpected variant for a dead connection: {err:?}"
    );
    if matches!(err, faucet_core::FaucetError::Config(_)) {
        eprintln!(
            "note: a mid-stream connection death is still typed as FaucetError::Config \
             (#662). Expected while that is open; tighten this assertion when it closes."
        );
    }

    // Whatever it wrote is a prefix of the data, never more than the source had.
    let written = sink.len();
    assert!(
        (written as i64) < total,
        "the read was supposed to be cut short, but all {written} rows arrived"
    );
}

#[tokio::test]
async fn an_unreachable_server_fails_at_construction_and_bounds_its_wait() {
    // The connect-time variant: the server is gone before the source is even
    // built. The source connects eagerly in `new()`, so the failure surfaces
    // there rather than at run time — which is the better place for it, since
    // `faucet validate` / `doctor` then catch an unreachable destination
    // without moving any data.
    //
    // What matters here is that the wait is **bounded**: an unbounded connect
    // retry is the hang this tier exists to rule out.
    let (container, url) = start_postgres().await;
    seed(&url, 100).await;
    container.stop().await.expect("stop the container");

    let started = std::time::Instant::now();
    let outcome = within(
        "construct a source against a stopped server",
        PostgresSource::new(
            PostgresSourceConfig::new(&url, "SELECT id FROM big").with_batch_size(10),
        ),
    )
    .await;
    let elapsed = started.elapsed();

    let err = match outcome {
        Err(e) => e,
        Ok(_) => panic!("a source must not be constructible against a stopped server"),
    };

    // The pool's acquire timeout is what bounds this; the message names it.
    assert!(
        elapsed < MUST_FINISH_WITHIN,
        "the connect attempt took {elapsed:?} — it must be bounded by the pool timeout, \
         not retry indefinitely"
    );
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("connection")
            || msg.to_lowercase().contains("pool")
            || msg.to_lowercase().contains("connect"),
        "the error must say the connection is the problem, so an operator is not left \
         guessing: {msg}"
    );
}

#[tokio::test]
async fn the_healthy_path_is_the_control_and_reads_every_row() {
    // Without this, a bug that made every run fail early would leave both tests
    // above passing for entirely the wrong reason.
    let (_c, url) = start_postgres().await;
    seed(&url, 2_500).await;

    let src = source(&url, 100).await;
    let sink = TestSink::new();

    let result = within("healthy control run", Pipeline::new(&src, &sink).run())
        .await
        .expect("a healthy run completes");

    assert_eq!(
        result.records_written, 2_500,
        "the control must read the whole table, or the cut-short assertions above \
         are not measuring anything"
    );
    assert_eq!(sink.len(), 2_500);
}

#[tokio::test]
async fn a_cut_connection_never_reports_a_bookmark_it_cannot_back() {
    // The durability tie-in: an incremental read that dies mid-stream must not
    // hand back a bookmark covering rows the sink never confirmed. The engine
    // tier asserts this with a scripted source; here the failure is a real
    // socket death partway through a real cursor.
    let (container, url) = start_postgres().await;
    let total = 3_000;
    seed(&url, total).await;

    let src = slow_source(&url, 100).await;
    let sink = TestSink::new();

    let killer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        container.stop().await.expect("stop");
        container
    });

    let outcome = within("cut connection bookmark", Pipeline::new(&src, &sink).run()).await;
    let _ = killer.await;

    match outcome {
        // The expected path: a failure carries no bookmark for a caller to
        // persist.
        Err(_) => {}
        Ok(result) => {
            let written = sink.len();
            panic!(
                "the run returned Ok(bookmark = {:?}) after its connection died, with {written} \
                 rows written. Persisting that bookmark would skip every unread row on the \
                 next run.",
                result.bookmark
            );
        }
    }
}

/// Guards the premise of this file: the source must actually stream in pages
/// rather than buffering the whole table, or "cut it mid-stream" is not a thing
/// that can happen and every test above is vacuous.
#[tokio::test]
async fn the_source_streams_in_pages_so_a_mid_stream_cut_is_possible() {
    use futures::StreamExt;

    let (_c, url) = start_postgres().await;
    seed(&url, 1_000).await;
    let src = source(&url, 100).await;

    let ctx = std::collections::HashMap::new();
    let mut pages = src.stream_pages(&ctx, 100);
    let first = pages
        .next()
        .await
        .expect("at least one page")
        .expect("the first page reads");

    assert!(
        first.records.len() < 1_000,
        "the source buffered the whole table into one page ({} records), so there is no \
         mid-stream point at which to cut the connection",
        first.records.len()
    );
    assert!(!first.records.is_empty());
}
