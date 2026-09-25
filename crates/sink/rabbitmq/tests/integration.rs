//! Integration tests against a real `rabbitmq:3-management` broker (Docker).
//! Published messages are read back through the RabbitMQ source.

mod common;

use common::*;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::{Sink, Source};
use faucet_sink_rabbitmq::{
    RabbitMqConnectionConfig, RabbitMqExchangeKind, RabbitMqSink, RabbitMqSinkConfig,
    RabbitMqValueFormat,
};
use faucet_source_rabbitmq::{RabbitMqBinding, RabbitMqSource, RabbitMqSourceConfig};
use serde_json::{Value, json};

fn sink_cfg(url: &str, queue: &str) -> RabbitMqSinkConfig {
    let mut c = RabbitMqSinkConfig::to_queue(queue);
    c.connection = RabbitMqConnectionConfig::from_url(url);
    c
}

fn source_cfg(url: &str, queue: &str) -> RabbitMqSourceConfig {
    let mut c = RabbitMqSourceConfig::new(queue);
    c.connection = RabbitMqConnectionConfig::from_url(url);
    c.idle_timeout_secs = Some(1);
    c
}

async fn read_back(cfg: RabbitMqSourceConfig) -> Vec<Value> {
    RabbitMqSource::new(cfg)
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn publishes_to_the_default_exchange() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "orders").await;
    let mut cfg = sink_cfg(&broker.url, "orders");
    cfg.batch_size = 3;
    let sink = RabbitMqSink::new(cfg).await.unwrap();
    let records: Vec<Value> = (1..=7).map(|i| json!({"id": i})).collect();
    assert_eq!(sink.write_batch(&records).await.unwrap(), 7);
    assert_eq!(sink.write_batch(&records[..2]).await.unwrap(), 2);
    sink.flush().await.unwrap();

    let mut got = read_back(source_cfg(&broker.url, "orders")).await;
    assert_eq!(got.len(), 9);
    got.truncate(7);
    assert_eq!(got, records);
}

#[tokio::test(flavor = "multi_thread")]
async fn routes_per_record_through_a_named_exchange() {
    let broker = start_broker().await;
    let bind = |queue: &str, key: &str| {
        let mut c = source_cfg(&broker.url, queue);
        c.bindings = vec![RabbitMqBinding {
            exchange: "events".into(),
            routing_key: key.into(),
            exchange_kind: Some(RabbitMqExchangeKind::Topic),
        }];
        c
    };
    let orders = bind("orders-q", "orders.*");
    let users = bind("users-q", "users.*");
    assert!(read_back(orders.clone()).await.is_empty());
    assert!(read_back(users.clone()).await.is_empty());

    let mut cfg = sink_cfg(&broker.url, "unused");
    cfg.routing_key = None;
    cfg.routing_key_field = Some("kind".into());
    cfg.exchange = "events".into();
    cfg.exchange_kind = Some(RabbitMqExchangeKind::Topic);
    let sink = RabbitMqSink::new(cfg).await.unwrap();
    let records = vec![
        json!({"kind": "orders.created", "id": 1}),
        json!({"kind": "users.created", "id": 2}),
        json!({"kind": "orders.paid", "id": 3}),
    ];
    assert_eq!(sink.write_batch(&records).await.unwrap(), 3);

    let got_orders = read_back(orders).await;
    let got_users = read_back(users).await;
    assert_eq!(got_orders, vec![records[0].clone(), records[2].clone()]);
    assert_eq!(got_users, vec![records[1].clone()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn mandatory_unroutable_rows_fail_individually() {
    let broker = start_broker().await;
    let mut routed = source_cfg(&broker.url, "routed");
    routed.bindings = vec![RabbitMqBinding {
        exchange: "direct-ex".into(),
        routing_key: "known".into(),
        exchange_kind: Some(RabbitMqExchangeKind::Direct),
    }];
    assert!(read_back(routed.clone()).await.is_empty());

    let mut cfg = sink_cfg(&broker.url, "unused");
    cfg.routing_key = None;
    cfg.routing_key_jsonpath = Some("$.route".into());
    cfg.exchange = "direct-ex".into();
    cfg.mandatory = true;
    let sink = RabbitMqSink::new(cfg).await.unwrap();
    let records = vec![
        json!({"route": "known", "id": 1}),
        json!({"route": "nowhere", "id": 2}),
        json!({"id": 3}),
    ];
    let outcomes = sink.write_batch_partial(&records).await.unwrap();
    assert!(outcomes[0].is_ok());
    let unroutable = outcomes[1].as_ref().unwrap_err().to_string();
    assert!(unroutable.contains("unroutable"), "{unroutable}");
    assert!(
        outcomes[2]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("matched nothing")
    );

    let err = sink.write_batch(&records[1..2]).await.unwrap_err();
    assert!(err.to_string().contains("1 of 1"), "{err}");

    assert_eq!(read_back(routed).await, vec![records[0].clone()]);
}

#[tokio::test(flavor = "multi_thread")]
async fn string_and_bytes_formats_round_trip() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "text").await;
    let mut cfg = sink_cfg(&broker.url, "text");
    cfg.value_format = RabbitMqValueFormat::String;
    cfg.persistent = false;
    cfg.batch_size = 0;
    let sink = RabbitMqSink::new(cfg).await.unwrap();
    sink.write_batch(&[json!("hello"), json!("world")])
        .await
        .unwrap();
    let outcomes = sink.write_batch_partial(&[json!(42)]).await.unwrap();
    assert!(outcomes[0].is_err());
    let mut src = source_cfg(&broker.url, "text");
    src.value_format = RabbitMqValueFormat::String;
    src.include_metadata = true;
    let got = read_back(src).await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0]["data"], "hello");
    assert_eq!(got[0]["content_type"], "text/plain");

    declare_queue(&broker.url, "blobs").await;
    let mut cfg = sink_cfg(&broker.url, "blobs");
    cfg.value_format = RabbitMqValueFormat::Bytes;
    let sink = RabbitMqSink::new(cfg).await.unwrap();
    sink.write_batch(&[json!("/wA=")]).await.unwrap();
    let mut src = source_cfg(&broker.url, "blobs");
    src.value_format = RabbitMqValueFormat::Bytes;
    assert_eq!(read_back(src).await, vec![json!("/wA=")]);
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
