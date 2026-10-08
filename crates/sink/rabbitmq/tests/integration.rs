//! Integration tests against a real `rabbitmq:3-management` broker (Docker).
//! Published messages are read back through the RabbitMQ source.

mod common;

use common::*;
use faucet_core::Sink;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_sink_rabbitmq::{
    RabbitMqConnectionConfig, RabbitMqExchangeKind, RabbitMqSink, RabbitMqSinkConfig,
};
use serde_json::json;

fn sink_cfg(url: &str, queue: &str) -> RabbitMqSinkConfig {
    let mut c = RabbitMqSinkConfig::to_queue(queue);
    c.connection = RabbitMqConnectionConfig::from_url(url);
    c
}

#[tokio::test(flavor = "multi_thread")]
async fn publishes_without_confirms() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "nc").await;
    let mut cfg = sink_cfg(&broker.url, "nc");
    cfg.confirm = false;
    let sink = RabbitMqSink::new(cfg).await.unwrap();
    sink.write_batch(&[json!({"a": 1})]).await.unwrap();
    assert_eq!(wait_ready(&broker.url, "nc", 1).await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_exchange_fails_and_the_next_write_reconnects() {
    let broker = start_broker().await;
    let mut cfg = sink_cfg(&broker.url, "k");
    cfg.exchange = "ghost".into();
    let sink = RabbitMqSink::new(cfg).await.unwrap();
    let first = sink.write_batch(&[json!({"a": 1})]).await.unwrap_err();
    assert!(
        first.to_string().contains("ghost") || first.to_string().contains("NOT_FOUND"),
        "{first}"
    );
    let second = sink.write_batch(&[json!({"a": 1})]).await.unwrap_err();
    assert!(second.to_string().contains("rabbitmq"), "{second}");
}

#[tokio::test(flavor = "multi_thread")]
async fn exchange_kind_mismatch_is_a_config_error() {
    let broker = start_broker().await;
    let mut cfg = sink_cfg(&broker.url, "k");
    cfg.exchange = "typed".into();
    cfg.exchange_kind = Some(RabbitMqExchangeKind::Fanout);
    // Nothing is bound to the exchange; `mandatory` (default true since #789
    // MSG-75) would turn the publish into a row error this test is not about.
    cfg.mandatory = false;
    RabbitMqSink::new(cfg.clone())
        .await
        .unwrap()
        .write_batch(&[json!({"a": 1})])
        .await
        .unwrap();
    cfg.exchange_kind = Some(RabbitMqExchangeKind::Topic);
    let err = RabbitMqSink::new(cfg)
        .await
        .unwrap()
        .write_batch(&[json!({"a": 1})])
        .await
        .unwrap_err();
    assert!(matches!(err, faucet_core::FaucetError::Config(_)), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn check_probes_connect_and_exchange() {
    let broker = start_broker().await;
    let ctx = CheckContext::default();

    let report = RabbitMqSink::new(sink_cfg(&broker.url, "q"))
        .await
        .unwrap()
        .check(&ctx)
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0);
    assert!(matches!(report.probes[1].status, ProbeStatus::Skip { .. }));

    let mut cfg = sink_cfg(&broker.url, "q");
    cfg.exchange = "amq.topic".into();
    let report = RabbitMqSink::new(cfg.clone())
        .await
        .unwrap()
        .check(&ctx)
        .await
        .unwrap();
    assert!(matches!(report.probes[1].status, ProbeStatus::Pass));

    cfg.exchange = "nope".into();
    let report = RabbitMqSink::new(cfg.clone())
        .await
        .unwrap()
        .check(&ctx)
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 1);

    cfg.exchange_kind = Some(RabbitMqExchangeKind::Direct);
    let report = RabbitMqSink::new(cfg)
        .await
        .unwrap()
        .check(&ctx)
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0);
    assert!(matches!(report.probes[1].status, ProbeStatus::Skip { .. }));
}

/// With the default settings a message no queue receives is a row error, not
/// a confirmed-and-dropped publish (#789 MSG-75).
#[tokio::test(flavor = "multi_thread")]
async fn unroutable_messages_fail_by_default() {
    let broker = start_broker().await;
    let sink = RabbitMqSink::new(sink_cfg(&broker.url, "no-such-queue"))
        .await
        .unwrap();
    let outcomes = sink.write_batch_partial(&[json!({"a": 1})]).await.unwrap();
    assert!(
        outcomes[0]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("unroutable"),
        "{outcomes:?}"
    );
}
