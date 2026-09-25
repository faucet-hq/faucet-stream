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
