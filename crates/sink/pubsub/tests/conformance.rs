//! `faucet-conformance` battery for the Google Cloud Pub/Sub sink.
//!
//! Check 1 (config schema) is pure/offline and always runs. Check 5
//! (capabilities truthful) **auto-starts** the official Pub/Sub **emulator** in
//! Docker via `testcontainers`; it skips cleanly when Docker is unavailable and
//! runs for real in CI. The Pub/Sub sink is an append-only publisher (no
//! idempotent-watermark / keyed-upsert mechanism), so check 5 takes the
//! honest-`false` branch: Append works and no phantom commit token is recorded.
//! The destination count is read back by draining a subscription created on the
//! target topic before the sink publishes. Passing this battery in CI is the
//! Tier-1 (supported) criterion.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use faucet_core::Sink as _;
use faucet_sink_pubsub::{
    PubsubConnection, PubsubCredentials, PubsubSink, PubsubSinkConfig, ValueFormat,
};
use gcloud_pubsub::client::{Client, ClientConfig};
use gcloud_pubsub::subscription::Subscription;
use testcontainers_modules::google_cloud_sdk_emulators::{CloudSdk, PUBSUB_PORT};
use testcontainers_modules::testcontainers::ContainerAsync;

const PROJECT: &str = "faucet-test";

async fn start_emulator() -> Option<(ContainerAsync<CloudSdk>, String)> {
    let container =
        faucet_conformance::containers::start_or_skip(CloudSdk::pubsub, &Default::default())
            .await?;
    let port = container.get_host_port_ipv4(PUBSUB_PORT).await.ok()?;
    Some((container, format!("127.0.0.1:{port}")))
}

async fn setup_client() -> Client {
    #[allow(clippy::field_reassign_with_default)]
    let config = {
        let mut config = ClientConfig::default();
        config.project_id = Some(PROJECT.to_string());
        config
    };
    Client::new(config).await.expect("emulator setup client")
}

fn conn(host: &str) -> PubsubConnection {
    PubsubConnection {
        project_id: Some(PROJECT.into()),
        emulator_host: Some(host.to_string()),
        credentials: PubsubCredentials::Anonymous,
        ..Default::default()
    }
}

/// Pull and ack every message currently available on `sub`, adding the count to
/// `seen`; returns the running total. A short idle deadline bounds the wait so a
/// "nothing published yet" call returns promptly.
async fn count_available(sub: &Subscription, seen: &AtomicUsize) -> usize {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let msgs = sub.pull(100, None).await.unwrap_or_default();
        if msgs.is_empty() {
            tokio::time::sleep(Duration::from_millis(150)).await;
            continue;
        }
        for m in &msgs {
            let _ = m.ack().await;
        }
        seen.fetch_add(msgs.len(), Ordering::SeqCst);
    }
    seen.load(Ordering::SeqCst)
}

// ── Check 1: config schema validity (pure, offline) ──────────────────────────
#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(PubsubSinkConfig)).unwrap();
    faucet_conformance::assert_config_schema_valid_value(&schema, "pubsub");
}

// ── Check 5: capabilities are truthful (emulator, skip if no Docker) ─────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let Some((_c, host)) = start_emulator().await else {
        eprintln!("skipping pubsub conformance_capabilities_truthful: Docker unavailable");
        return;
    };
    // SAFETY: the only writer of the var in this test; clients are built after.
    unsafe {
        std::env::set_var("PUBSUB_EMULATOR_HOST", &host);
    }

    let client = setup_client().await;
    let topic = client
        .create_topic("conf-sink-t", None, None)
        .await
        .expect("create topic");
    client
        .create_subscription(
            "conf-sink-s",
            topic.fully_qualified_name(),
            Default::default(),
            None,
        )
        .await
        .expect("create subscription");
    let sub = client.subscription("conf-sink-s");

    let mut cfg = PubsubSinkConfig::new("conf-sink-t");
    cfg.connection = conn(&host);
    cfg.value_format = ValueFormat::Json;
    let sink = PubsubSink::new(cfg).await.expect("sink builds");
    faucet_conformance::assert_batch_atomicity_declared(&sink);

    // Check 10: connector_name is non-empty (metric-cardinality contract).
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    // Check 11: preflight check() is well-formed against the emulator (the
    // topic created above exists → a Pass probe inside Ok(report)).
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    let seen = Arc::new(AtomicUsize::new(0));
    let sub = Arc::new(sub);
    assert_capabilities_truthful_count(&sink, seen, sub).await;

    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}

/// Helper to bind the `distinct_count` closure's captures cleanly.
async fn assert_capabilities_truthful_count(
    sink: &PubsubSink,
    seen: Arc<AtomicUsize>,
    sub: Arc<Subscription>,
) {
    faucet_conformance::assert_capabilities_truthful(sink, move || {
        let seen = seen.clone();
        let sub = sub.clone();
        async move { count_available(&sub, &seen).await }
    })
    .await;
}
