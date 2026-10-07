//! `faucet-conformance` battery for the Google Cloud Pub/Sub source.
//!
//! Check 1 (config schema) is pure/offline and always runs. Check 2
//! (bounded-memory streaming) **auto-starts** the official Pub/Sub **emulator**
//! (gRPC) in Docker via `testcontainers`; it skips cleanly when Docker is
//! unavailable and runs for real in CI. `max_messages` gives the drain a hard
//! upper bound, so the streamed count is deterministic despite Pub/Sub's
//! at-least-once delivery. Passing this battery in CI is the Tier-1 (supported)
//! criterion.

use faucet_common_pubsub::PubsubMessage;
use faucet_core::Source as _;
use faucet_source_pubsub::{
    PubsubConnection, PubsubCredentials, PubsubSource, PubsubSourceConfig, ValueFormat,
};
use gcloud_pubsub::client::{Client, ClientConfig};
use testcontainers_modules::google_cloud_sdk_emulators::{CloudSdk, PUBSUB_PORT};
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const PROJECT: &str = "faucet-test";

/// Start the Pub/Sub emulator, or `None` when Docker is unavailable.
async fn start_emulator() -> Option<(ContainerAsync<CloudSdk>, String)> {
    let container = CloudSdk::pubsub().start().await.ok()?;
    let port = container.get_host_port_ipv4(PUBSUB_PORT).await.ok()?;
    Some((container, format!("127.0.0.1:{port}")))
}

/// A setup/admin client scoped to `PROJECT`, pointed at the emulator.
async fn setup_client() -> Client {
    #[allow(clippy::field_reassign_with_default)]
    let config = {
        let mut config = ClientConfig::default(); // reads PUBSUB_EMULATOR_HOST
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

// ── Check 1: config schema validity (pure, offline) ──────────────────────────
#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(PubsubSourceConfig)).unwrap();
    faucet_conformance::assert_config_schema_valid_value(&schema, "pubsub");
}

// ── Check 2: bounded-memory streaming (emulator, skip if no Docker) ──────────
#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let Some((_c, host)) = start_emulator().await else {
        eprintln!("skipping pubsub conformance_bounded_memory: Docker unavailable");
        return;
    };
    // SAFETY: this test is the only writer of the var; the setup client below is
    // built after this line, and the connector reads `emulator_host` from its
    // explicit config rather than the env var.
    unsafe {
        std::env::set_var("PUBSUB_EMULATOR_HOST", &host);
    }

    let client = setup_client().await;
    let topic = client
        .create_topic("conf-src-t", None, None)
        .await
        .expect("create topic");
    client
        .create_subscription(
            "conf-src-s",
            topic.fully_qualified_name(),
            Default::default(),
            None,
        )
        .await
        .expect("create subscription");

    // Publish 150 messages.
    let publisher = client.topic("conf-src-t").new_publisher(None);
    let mut awaiters = Vec::with_capacity(150);
    for n in 0..150i64 {
        let m = PubsubMessage {
            data: format!("{{\"n\":{n}}}").into_bytes(),
            ..Default::default()
        };
        awaiters.push(publisher.publish(m).await);
    }
    for a in awaiters {
        a.get().await.expect("publish");
    }

    // Drain via the source at batch_size 30 with a hard cap of 150 → pages of
    // ≤30, exactly 150 records, bounded.
    let mut cfg = PubsubSourceConfig::new("conf-src-s");
    cfg.connection = conn(&host);
    cfg.value_format = ValueFormat::Json;
    cfg.idle_termination_secs = Some(5);
    cfg.max_messages = Some(150);
    cfg.batch_size = 30;
    let source = PubsubSource::new(cfg).await.expect("source builds");
    assert!(source.consumes_destructively());

    // Check 10: connector_name is non-empty (metric-cardinality contract).
    faucet_conformance::assert_connector_name_nonempty(&source);
    // Check 11: preflight check() is well-formed against the emulator (the
    // subscription created above exists → a Pass probe inside Ok(report)).
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    faucet_conformance::assert_bounded_memory(&source, 30, 150).await;
}
