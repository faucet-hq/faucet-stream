//! `faucet-conformance` Tier-1 battery for the RabbitMQ sink.
//!
//! - **Check 1** (`conformance_config_schema_valid`) — pure/offline.
//! - **Check 5** (`conformance_capabilities_truthful`) — a real
//!   `rabbitmq:3-management` broker (Docker) verifies the append-only
//!   capability surface against real behaviour (queue depth after writes).
//!
//! Idempotency checks (3/4) do not apply — the sink is append-only.

mod common;

use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink as _;
use faucet_sink_rabbitmq::{RabbitMqConnectionConfig, RabbitMqSink, RabbitMqSinkConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(RabbitMqSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-sink-rabbitmq");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let broker = common::start_broker().await;
    common::declare_queue(&broker.url, "conformance").await;
    let mut cfg = RabbitMqSinkConfig::to_queue("conformance");
    cfg.connection = RabbitMqConnectionConfig::from_url(&broker.url);
    let sink = RabbitMqSink::new(cfg).await.expect("sink");

    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    let url = broker.url.clone();
    faucet_conformance::assert_capabilities_truthful(&sink, move || {
        let url = url.clone();
        async move { common::ready_count(&url, "conformance").await as usize }
    })
    .await;
}
