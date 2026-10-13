//! `faucet-conformance` Tier-1 battery for the Spanner source.
//!
//! Check 1 (config-schema validity) is pure and offline. Checks 2 (bounded
//! memory), 3 (bookmark round-trip), and 6 (errors, not panics) run against
//! the Cloud Spanner emulator via testcontainers and skip cleanly when Docker
//! is unavailable.

mod support;

use faucet_conformance::{
    assert_bookmark_roundtrip, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics,
};
use faucet_source_spanner::{SpannerReplication, SpannerSource, SpannerSourceConfig};

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SpannerSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-spanner");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// Seed `n` rows into `nums(id INT64, v STRING)` via batched DML.
async fn seed_nums(client: &gcloud_spanner::client::Client, n: usize) {
    for chunk_start in (0..n).step_by(500) {
        let values: Vec<String> = (chunk_start..(chunk_start + 500).min(n))
            .map(|i| format!("({i}, 'v{i}')"))
            .collect();
        support::execute_dml(
            client,
            &format!("INSERT INTO nums (id, v) VALUES {}", values.join(", ")),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "conformance",
        &["CREATE TABLE nums (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "conformance").await;
    seed_nums(&client, 5_000).await;

    let mut cfg = SpannerSourceConfig::new(
        support::PROJECT,
        support::INSTANCE,
        "conformance",
        "SELECT * FROM nums",
    );
    cfg.connection = support::connection(&emu.host, "conformance");
    cfg.batch_size = 500;
    let source = SpannerSource::new(cfg).await.expect("source");

    // Check 10: connector_name() is non-empty (reuses this live instance).
    assert_connector_name_nonempty(&source);

    assert_bounded_memory(&source, 500, 5_000).await;
}

// ── Check 3: bookmark round-trip (Docker) ───────────────────────────────────

/// Drive an incremental source to completion once (capturing the max-`id`
/// bookmark), feed it back via `apply_start_bookmark`, then re-drive with no
/// new rows. The resumed run must replay strictly fewer records (zero — the
/// `@bookmark` pushdown filters them all), proving the bookmark is honoured.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_bookmark_roundtrip() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "resume",
        &["CREATE TABLE nums (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "resume").await;
    seed_nums(&client, 500).await;

    let mut cfg = SpannerSourceConfig::new(
        support::PROJECT,
        support::INSTANCE,
        "resume",
        "SELECT * FROM nums WHERE id > @bookmark",
    );
    cfg.connection = support::connection(&emu.host, "resume");
    cfg.replication = SpannerReplication::Incremental {
        column: "id".into(),
        initial_value: serde_json::json!(-1),
    };
    let source = SpannerSource::new(cfg).await.expect("source");

    assert_bookmark_roundtrip(&source).await;
}

// ── Check 12: discovery round-trips (Docker) ────────────────────────────────

/// Every dataset `discover()` reports must be genuinely selectable: take its
/// config_patch (`{"query": …}`), rebuild the source pointed at that query, and
/// read it against the emulator.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_discover_roundtrips() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "discover",
        &["CREATE TABLE nums (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "discover").await;
    seed_nums(&client, 3).await;

    let mut base =
        SpannerSourceConfig::new(support::PROJECT, support::INSTANCE, "discover", "SELECT 1");
    base.connection = support::connection(&emu.host, "discover");
    let source = SpannerSource::new(base).await.expect("source");

    let host = emu.host.clone();
    faucet_conformance::assert_discover_roundtrips(&source, |patch| {
        let host = host.clone();
        async move {
            let query = patch["query"].as_str().expect("query patch").to_string();
            let mut cfg =
                SpannerSourceConfig::new(support::PROJECT, support::INSTANCE, "discover", query);
            cfg.connection = support::connection(&host, "discover");
            Box::new(SpannerSource::new(cfg).await.expect("rebuilt source"))
                as Box<dyn faucet_core::Source>
        }
    })
    .await;
}

// ── Check 6: errors, not panics (Docker) ────────────────────────────────────

/// Query a table that does not exist — both `fetch_all` and `stream_pages`
/// must surface a typed `FaucetError`, never a panic.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(&emu.host, "errors", &[]).await;

    let mut cfg = SpannerSourceConfig::new(
        support::PROJECT,
        support::INSTANCE,
        "errors",
        "SELECT * FROM does_not_exist",
    );
    cfg.connection = support::connection(&emu.host, "errors");
    let source = SpannerSource::new(cfg).await.expect("source builds");

    assert_errors_not_panics(&source).await;
}

// ── Descriptor agreement (RFC 0013, Docker) ─────────────────────────────────

/// `new()` opens a session pool, so the built source needs the emulator.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_source() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(&emu.host, "descriptor", &[]).await;
    let base = serde_json::json!({
        "project_id": support::PROJECT,
        "instance": support::INSTANCE,
        "database": "descriptor",
        "emulator_host": emu.host,
        "query": "SELECT 1",
    });
    let mut sharded = base.clone();
    sharded["shard"] = serde_json::json!({"key": "id"});
    for v in [base, sharded] {
        let cfg: SpannerSourceConfig = serde_json::from_value(v.clone()).unwrap();
        let built = SpannerSource::new(cfg).await.expect("source");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_spanner::DESCRIPTOR,
            "spanner",
            &v,
            &built,
        );
    }
}

// ── Check: backend calls honour the call deadline and cancel (#880) ──────────

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

/// The client opens its session pool while it is built, so the backend hangs
/// only once the source exists: a proxy forwards to the emulator during
/// `new()` and then stops answering.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "deadline",
        &["CREATE TABLE t (id INT64 NOT NULL) PRIMARY KEY (id)"],
    )
    .await;
    let proxy = faucet_conformance::hung::HangingProxy::start(&emu.host)
        .await
        .expect("proxy");
    let host = proxy.addr().to_string();
    faucet_conformance::calls::assert_source_call_deadline(|| {
        proxy.resume();
        let mut cfg = SpannerSourceConfig::new(
            support::PROJECT,
            support::INSTANCE,
            "deadline",
            "SELECT id FROM t",
        );
        cfg.connection = support::connection(&host, "deadline");
        let source = block_on(SpannerSource::new(cfg)).expect("source builds");
        proxy.hang();
        source
    })
    .await;
}
