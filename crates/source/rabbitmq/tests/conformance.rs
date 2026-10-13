//! `faucet-conformance` Tier-1 battery for the RabbitMQ source.
//!
//! - **Check 1** (`conformance_config_schema_valid`) — pure/offline.
//! - **Check 6** (`conformance_errors_not_panics`) — unreachable broker; the
//!   connect fails on the first poll, so it runs offline.
//! - **Check 2** (`conformance_bounded_memory`) — a real `rabbitmq:3-management`
//!   broker (Docker): publish N messages, drain with `max_messages = N`.
//!
//! Bookmark checks (3) do not apply: the broker, not faucet, owns the queue
//! position, so the per-page bookmark is informational only.

mod common;

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_source_rabbitmq::{RabbitMqConnectionConfig, RabbitMqSource, RabbitMqSourceConfig};

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(RabbitMqSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-rabbitmq");
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    let mut cfg = RabbitMqSourceConfig::new("events");
    cfg.connection = RabbitMqConnectionConfig::from_url("amqp://127.0.0.1:1/%2f");
    cfg.connection.connect_timeout_secs = 5;
    cfg.idle_timeout_secs = Some(1);
    let source = RabbitMqSource::new(cfg).await.expect("lazy construction");
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert!(faucet_core::Source::consumes_destructively(&source));
    assert_errors_not_panics(&source).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let broker = common::start_broker().await;
    const N: usize = 2_000;
    common::declare_queue(&broker.url, "bounded").await;
    common::publish_json(&broker.url, "bounded", N).await;

    let mut cfg = RabbitMqSourceConfig::new("bounded");
    cfg.connection = RabbitMqConnectionConfig::from_url(&broker.url);
    cfg.max_messages = Some(N);
    cfg.idle_timeout_secs = Some(30);
    cfg.batch_size = 250;
    let source = RabbitMqSource::new(cfg).await.expect("source");
    faucet_conformance::assert_bounded_memory(&source, 250, N).await;
}

/// The static descriptor answers exactly what the built source declares (RFC 0013).
#[tokio::test(flavor = "multi_thread")]
async fn conformance_descriptor_matches_built_source() {
    #[allow(unused_mut)]
    let configs =
        vec![r#"{"queue": "q", "url": "amqp://127.0.0.1:1/%2f", "idle_timeout_secs": 1}"#];
    for raw in configs {
        let v: faucet_core::Value = faucet_core::serde_json::from_str(raw).expect("json");
        let cfg: faucet_source_rabbitmq::RabbitMqSourceConfig =
            faucet_core::serde_json::from_value(v.clone()).expect("config");
        let built = faucet_source_rabbitmq::RabbitMqSource::new(cfg)
            .await
            .expect("offline build");
        faucet_conformance::assert_descriptor_matches_source(
            &faucet_source_rabbitmq::DESCRIPTOR,
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
        let mut cfg = RabbitMqSourceConfig::new("events");
        cfg.connection = RabbitMqConnectionConfig::from_url(&url);
        cfg.connection.connect_timeout_secs = 60;
        cfg.idle_timeout_secs = Some(60);
        built.push(RabbitMqSource::new(cfg).await.expect("lazy construction"));
    }
    let built = std::sync::Mutex::new(built);
    faucet_conformance::calls::assert_source_call_deadline(|| built.lock().unwrap().pop().unwrap())
        .await;
}
