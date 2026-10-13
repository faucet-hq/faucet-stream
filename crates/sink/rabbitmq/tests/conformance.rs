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
    faucet_conformance::assert_batch_atomicity_declared(&sink);

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
    let count = move || {
        let url = url.clone();
        async move { common::ready_count(&url, "conformance").await as usize }
    };
    faucet_conformance::assert_capabilities_truthful(&sink, count.clone()).await;
    // #878: per-message confirm outcomes are truthful.
    faucet_conformance::assert_partial_outcomes_truthful(&sink, count).await;
}

/// The static descriptor answers exactly what the built sink declares (RFC 0013).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_sink() {
    #[allow(unused_mut)]
    let configs =
        vec![r#"{"url": "amqp://127.0.0.1:1/%2f", "exchange": "x", "routing_key": "rk"}"#];
    for raw in configs {
        let v: faucet_core::Value = faucet_core::serde_json::from_str(raw).expect("json");
        let cfg: faucet_sink_rabbitmq::RabbitMqSinkConfig =
            faucet_core::serde_json::from_value(v.clone()).expect("config");
        let built = faucet_sink_rabbitmq::RabbitMqSink::new(cfg)
            .await
            .expect("offline build");
        faucet_conformance::assert_descriptor_matches_sink(
            &faucet_sink_rabbitmq::DESCRIPTOR,
            "rabbitmq",
            &v,
            &built,
        );
    }
}

/// A server that accepts TCP connections and never answers stands in for a
/// hung backend (#880): the lazy connect waits on it.
async fn silent_server() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((stream, _)) = listener.accept().await {
            held.push(stream);
        }
    });
    format!("amqp://{addr}/%2f")
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_call_deadline() {
    let url = silent_server().await;
    let mut built = Vec::new();
    for _ in 0..2 {
        let mut cfg = RabbitMqSinkConfig::to_queue("events");
        cfg.connection = RabbitMqConnectionConfig::from_url(&url);
        cfg.connection.connect_timeout_secs = 60;
        built.push(RabbitMqSink::new(cfg).await.expect("lazy construction"));
    }
    let built = std::sync::Mutex::new(built);
    faucet_conformance::calls::assert_sink_call_deadline(
        || built.lock().unwrap().pop().unwrap(),
        &[serde_json::json!({"id": 1})],
    )
    .await;
}
