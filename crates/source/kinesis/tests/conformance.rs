//! `faucet-conformance` Tier-1 battery for the Kinesis source.
//!
//! Check 1 (config-schema validity) is pure and offline. Check 2
//! (bounded-memory streaming) boots LocalStack via testcontainers and so
//! requires Docker — it runs in CI alongside the other integration tests. It
//! matches the existing streaming integration test's LocalStack setup +
//! seeding path verbatim.
//!
//! Termination: `idle_termination_secs` stops the per-shard workers once every
//! shard has been fully drained, so `stream_pages` completes after emitting
//! exactly the seeded records — letting the bounded-memory check assert
//! `seen == total`.

use faucet_conformance::{
    assert_bookmark_roundtrip, assert_config_schema_valid_value, assert_errors_not_panics,
};
use faucet_source_kinesis::{KinesisCredentials, KinesisSource, KinesisSourceConfig};
use testcontainers::ContainerAsync;
use testcontainers_modules::localstack::LocalStack;

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(KinesisSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-kinesis");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// Start LocalStack with the kinesis service and return (container, endpoint).
async fn start_localstack() -> (ContainerAsync<LocalStack>, String) {
    use testcontainers::ImageExt;
    let container = faucet_conformance::containers::start(|| {
        LocalStack::default().with_env_var("SERVICES", "kinesis")
    })
    .await;
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("localstack port");
    (container, format!("http://127.0.0.1:{port}"))
}

fn test_credentials() -> KinesisCredentials {
    KinesisCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    }
}

async fn raw_client(endpoint: &str) -> aws_sdk_kinesis::Client {
    faucet_source_kinesis::build_client(Some("us-east-1"), Some(endpoint), &test_credentials())
        .await
        .expect("client")
}

/// Wait until the mapped endpoint actually accepts Kinesis API calls —
/// the container's port maps before the service is ready.
async fn await_ready(client: &aws_sdk_kinesis::Client) {
    for _ in 0..120 {
        if client.list_streams().send().await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("localstack kinesis never became ready");
}

/// Create a stream and wait until it is ACTIVE.
async fn create_stream(client: &aws_sdk_kinesis::Client, name: &str, shards: i32) {
    await_ready(client).await;
    client
        .create_stream()
        .stream_name(name)
        .shard_count(shards)
        .send()
        .await
        .expect("create stream");
    for _ in 0..60 {
        let out = client
            .describe_stream_summary()
            .stream_name(name)
            .send()
            .await
            .expect("describe");
        if out
            .stream_description_summary()
            .map(|d| d.stream_status() == &aws_sdk_kinesis::types::StreamStatus::Active)
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("stream {name} never became ACTIVE");
}

/// Put `n` JSON records with rotating partition keys.
async fn put_records(client: &aws_sdk_kinesis::Client, stream: &str, start: usize, n: usize) {
    use aws_sdk_kinesis::primitives::Blob;
    use aws_sdk_kinesis::types::PutRecordsRequestEntry;
    for chunk_start in (start..start + n).step_by(100) {
        let entries: Vec<PutRecordsRequestEntry> = (chunk_start
            ..(chunk_start + 100).min(start + n))
            .map(|i| {
                PutRecordsRequestEntry::builder()
                    .partition_key(format!("user-{}", i % 7))
                    .data(Blob::new(format!("{{\"i\":{i}}}").into_bytes()))
                    .build()
                    .expect("entry")
            })
            .collect();
        let out = client
            .put_records()
            .stream_name(stream)
            .set_records(Some(entries))
            .send()
            .await
            .expect("put_records");
        assert_eq!(
            out.failed_record_count().unwrap_or(0),
            0,
            "seeding must not throttle"
        );
    }
}

fn source_config(endpoint: &str, stream: &str) -> KinesisSourceConfig {
    let mut cfg = KinesisSourceConfig::new(stream);
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(endpoint.into());
    cfg.credentials = test_credentials();
    cfg.poll_interval_ms = 200;
    // Generous idle window so the first drain reliably reads every seeded record
    // across all shards before terminating — 3 s is too tight under the
    // instrumented (llvm-cov) CI build.
    cfg.idle_termination_secs = Some(15);
    cfg.batch_size = 250;
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client, "conformance", 3).await;
    put_records(&client, "conformance", 0, 5_000).await;

    let source = KinesisSource::new(source_config(&endpoint, "conformance"))
        .await
        .expect("source");

    // Check 11: preflight check() is well-formed against the live stream
    // (`DescribeStreamSummary` → a Pass probe inside Ok(report); no records
    // consumed).
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;
    // _container stays alive to here
}

// ── Check 3: bookmark round-trip (Docker) ───────────────────────────────────

/// Drive the source to completion once (capturing the cumulative per-shard
/// sequence bookmark), feed it back via `apply_start_bookmark`, then re-drive
/// against the *same* stream with **no new records**. The resume must consume
/// strictly fewer records than the first run (zero — the stream is fully
/// drained past the bookmarked sequence), proving the sequence-number bookmark
/// is honoured. `idle_termination_secs` guarantees the resumed run terminates
/// even though it produces no records.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_bookmark_roundtrip() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client, "resume", 3).await;
    // ≥300 records so the first run consumes a real volume.
    put_records(&client, "resume", 0, 500).await;

    let source = KinesisSource::new(source_config(&endpoint, "resume"))
        .await
        .expect("source");

    assert_bookmark_roundtrip(&source).await;
    // _container stays alive to here
}

// ── Check 6: errors, not panics ─────────────────────────────────────────────

/// Point the source at an unreachable endpoint (`http://127.0.0.1:1`, which
/// refuses connections immediately). `new()` stays lazy — no container needed —
/// and the first `ListShards`/`GetRecords` call fails with a typed
/// `FaucetError` on both the `fetch_all` and `stream_pages` paths, never a
/// panic.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let mut cfg = KinesisSourceConfig::new("does-not-exist");
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some("http://127.0.0.1:1".into());
    cfg.credentials = test_credentials();
    cfg.poll_interval_ms = 200;
    // A terminating run is required by config validation; keep it short so the
    // failure path is exercised promptly.
    cfg.idle_termination_secs = Some(1);
    cfg.max_messages = Some(10);

    let source = KinesisSource::new(cfg).await.expect("source builds lazily");
    // Check 10: connector_name is non-empty (metric-cardinality contract).
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert_errors_not_panics(&source).await;
}
