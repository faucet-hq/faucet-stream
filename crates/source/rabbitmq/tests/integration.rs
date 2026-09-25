//! Integration tests against a real `rabbitmq:3-management` broker (Docker).

mod common;

use async_trait::async_trait;
use common::*;
use faucet_core::{FaucetError, Pipeline, Sink, Source};
use faucet_source_rabbitmq::{
    AckMode, OnDecodeError, RabbitMqBinding, RabbitMqConnectionConfig, RabbitMqExchangeKind,
    RabbitMqSource, RabbitMqSourceConfig, RabbitMqValueFormat,
};
use futures::StreamExt;
use lapin::BasicProperties;
use lapin::types::{AMQPValue, FieldTable};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn source_cfg(url: &str, queue: &str) -> RabbitMqSourceConfig {
    let mut c = RabbitMqSourceConfig::new(queue);
    c.connection = RabbitMqConnectionConfig::from_url(url);
    c.idle_timeout_secs = Some(2);
    c
}

async fn drain(cfg: RabbitMqSourceConfig) -> Vec<Vec<Value>> {
    let source = RabbitMqSource::new(cfg).await.unwrap();
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut out = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page");
        out.push(page.records);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn drains_by_max_messages_in_pages_and_acks_everything() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "orders").await;
    publish_json(&broker.url, "orders", 25).await;

    let mut cfg = source_cfg(&broker.url, "orders");
    cfg.max_messages = Some(25);
    cfg.idle_timeout_secs = None;
    cfg.batch_size = 10;
    let source = RabbitMqSource::new(cfg).await.unwrap();
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut sizes = Vec::new();
    let mut ids = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert_eq!(page.bookmark.as_ref().unwrap()["queue"], "orders");
        sizes.push(page.records.len());
        ids.extend(page.records.iter().map(|r| r["id"].as_u64().unwrap()));
    }
    drop(pages);
    assert_eq!(sizes, vec![10, 10, 5]);
    assert_eq!(ids, (1..=25).collect::<Vec<_>>());
    assert_eq!(wait_ready(&broker.url, "orders", 0).await, 0);

    publish_json(&broker.url, "orders", 3).await;
    let mut cfg = source_cfg(&broker.url, "orders");
    cfg.max_messages = Some(25);
    cfg.idle_timeout_secs = Some(1);
    let source = RabbitMqSource::new(cfg).await.unwrap();
    let all = source.fetch_all().await.unwrap();
    assert_eq!(all.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_timeout_terminates_a_quiet_queue() {
    let broker = start_broker().await;
    let mut cfg = source_cfg(&broker.url, "quiet");
    cfg.idle_timeout_secs = Some(1);
    let started = std::time::Instant::now();
    let pages = drain(cfg).await;
    assert!(pages.is_empty());
    assert!(started.elapsed() < std::time::Duration::from_secs(10));

    publish_json(&broker.url, "quiet", 4).await;
    let mut cfg = source_cfg(&broker.url, "quiet");
    cfg.idle_timeout_secs = Some(1);
    cfg.batch_size = 0;
    cfg.prefetch = Some(0);
    let pages = drain(cfg).await;
    assert_eq!(pages.len(), 1, "batch_size 0 drains into one page");
    assert_eq!(pages[0].len(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn unconfirmed_page_is_redelivered() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "work").await;
    publish_json(&broker.url, "work", 10).await;

    let mut cfg = source_cfg(&broker.url, "work");
    cfg.batch_size = 5;
    cfg.max_messages = Some(10);
    {
        let source = RabbitMqSource::new(cfg.clone()).await.unwrap();
        let ctx = HashMap::new();
        let mut pages = source.stream_pages(&ctx, 0);
        let first = pages.next().await.unwrap().unwrap();
        assert_eq!(first.records.len(), 5);
    }
    assert_eq!(wait_ready(&broker.url, "work", 10).await, 10);

    {
        let source = RabbitMqSource::new(cfg.clone()).await.unwrap();
        let ctx = HashMap::new();
        let mut pages = source.stream_pages(&ctx, 0);
        pages.next().await.unwrap().unwrap();
        pages.next().await.unwrap().unwrap();
    }
    assert_eq!(
        wait_ready(&broker.url, "work", 5).await,
        5,
        "resuming after page 1 acks exactly page 1"
    );

    cfg.include_metadata = true;
    let pages = drain(cfg).await;
    let records: Vec<Value> = pages.into_iter().flatten().collect();
    assert_eq!(records.len(), 5);
    assert!(records.iter().all(|r| r["redelivered"] == true));
}

struct FailingSink;

#[async_trait]
impl Sink for FailingSink {
    async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
        Err(FaucetError::Sink("boom".into()))
    }
}

struct CountingSink(Arc<AtomicUsize>);

#[async_trait]
impl Sink for CountingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.0.fetch_add(records.len(), Ordering::SeqCst);
        Ok(records.len())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_sink_write_leaves_messages_for_the_next_run() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "pipe").await;
    publish_json(&broker.url, "pipe", 6).await;
    let mut cfg = source_cfg(&broker.url, "pipe");
    cfg.batch_size = 3;

    let source = RabbitMqSource::new(cfg.clone()).await.unwrap();
    let err = Pipeline::new(&source, &FailingSink).run().await;
    assert!(err.is_err());
    assert_eq!(wait_ready(&broker.url, "pipe", 6).await, 6);

    let written = Arc::new(AtomicUsize::new(0));
    let source = RabbitMqSource::new(cfg).await.unwrap();
    let sink = CountingSink(written.clone());
    Pipeline::new(&source, &sink).run().await.unwrap();
    assert_eq!(written.load(Ordering::SeqCst), 6);
    assert_eq!(wait_ready(&broker.url, "pipe", 0).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn max_messages_requeues_prefetched_extras() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "cap").await;
    publish_json(&broker.url, "cap", 20).await;
    let mut cfg = source_cfg(&broker.url, "cap");
    cfg.max_messages = Some(5);
    cfg.batch_size = 5;
    cfg.prefetch = Some(50);
    let pages = drain(cfg).await;
    assert_eq!(pages.concat().len(), 5);
    assert_eq!(wait_ready(&broker.url, "cap", 15).await, 15);
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_ack_mode_drains_without_bookmarks() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "fast").await;
    publish_json(&broker.url, "fast", 7).await;
    let mut cfg = source_cfg(&broker.url, "fast");
    cfg.ack_mode = AckMode::Auto;
    cfg.batch_size = 4;
    let source = RabbitMqSource::new(cfg).await.unwrap();
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut n = 0;
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert!(page.bookmark.is_none());
        n += page.records.len();
    }
    assert_eq!(n, 7);
    drop(pages);
    assert_eq!(wait_ready(&broker.url, "fast", 0).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn value_formats_metadata_and_decode_errors() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "raw").await;
    let mut headers = FieldTable::default();
    headers.insert("tenant".into(), AMQPValue::LongString("acme".into()));
    let props = BasicProperties::default()
        .with_headers(headers)
        .with_message_id("m-1".into());
    publish_with(
        &broker.url,
        "",
        "raw",
        &[b"plain text".to_vec(), vec![0xff, 0x00]],
        props,
    )
    .await;

    let mut cfg = source_cfg(&broker.url, "raw");
    cfg.value_format = RabbitMqValueFormat::Bytes;
    cfg.include_metadata = true;
    let records = drain(cfg).await.concat();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["data"], json!("cGxhaW4gdGV4dA=="));
    assert_eq!(records[1]["data"], json!("/wA="));
    assert_eq!(records[0]["headers"]["tenant"], "acme");
    assert_eq!(records[0]["message_id"], "m-1");
    assert_eq!(records[0]["routing_key"], "raw");

    publish_raw(
        &broker.url,
        "",
        "raw",
        &[b"ok".to_vec(), vec![0xff], b"{\"a\":1}".to_vec()],
    )
    .await;
    let mut cfg = source_cfg(&broker.url, "raw");
    cfg.value_format = RabbitMqValueFormat::String;
    cfg.on_decode_error = OnDecodeError::Skip;
    let records = drain(cfg).await.concat();
    assert_eq!(records, vec![json!("ok"), json!("{\"a\":1}")]);
    assert_eq!(
        wait_ready(&broker.url, "raw", 0).await,
        0,
        "skipped message is rejected"
    );

    publish_raw(&broker.url, "", "raw", &[b"not json".to_vec()]).await;
    let source = RabbitMqSource::new(source_cfg(&broker.url, "raw"))
        .await
        .unwrap();
    let err = source.fetch_all().await.unwrap_err();
    assert!(err.to_string().contains("not valid JSON"), "{err}");
    assert_eq!(
        wait_ready(&broker.url, "raw", 1).await,
        1,
        "a failed decode leaves the message for redelivery"
    );

    let mut cfg = source_cfg(&broker.url, "raw");
    cfg.ack_mode = AckMode::Auto;
    cfg.on_decode_error = OnDecodeError::Skip;
    assert!(drain(cfg).await.is_empty());
    assert_eq!(wait_ready(&broker.url, "raw", 0).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn bindings_declare_and_route_from_an_exchange() {
    let broker = start_broker().await;
    let mut cfg = source_cfg(&broker.url, "orders-created");
    cfg.bindings = vec![RabbitMqBinding {
        exchange: "events".into(),
        routing_key: "orders.*".into(),
        exchange_kind: Some(RabbitMqExchangeKind::Topic),
    }];
    cfg.idle_timeout_secs = Some(1);
    assert!(drain(cfg.clone()).await.is_empty());

    publish_raw(
        &broker.url,
        "events",
        "orders.created",
        &[b"{\"id\":1}".to_vec()],
    )
    .await;
    publish_raw(
        &broker.url,
        "events",
        "users.created",
        &[b"{\"id\":2}".to_vec()],
    )
    .await;
    cfg.bindings[0].exchange_kind = None;
    let records = drain(cfg).await.concat();
    assert_eq!(records, vec![json!({"id": 1})]);
}

#[tokio::test(flavor = "multi_thread")]
async fn mismatched_queue_properties_are_a_config_error() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "durable-q").await;
    let mut cfg = source_cfg(&broker.url, "durable-q");
    cfg.queue_durable = false;
    let source = RabbitMqSource::new(cfg).await.unwrap();
    let err = source.fetch_all().await.unwrap_err();
    assert!(
        matches!(err, FaucetError::Config(ref m) if m.contains("PRECONDITION_FAILED")),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_queue_without_declare_fails() {
    let broker = start_broker().await;
    let mut cfg = source_cfg(&broker.url, "absent");
    cfg.declare_queue = false;
    let source = RabbitMqSource::new(cfg).await.unwrap();
    let err = source.fetch_all().await.unwrap_err();
    assert!(err.to_string().contains("absent"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn binding_to_a_missing_exchange_fails() {
    let broker = start_broker().await;
    let mut cfg = source_cfg(&broker.url, "orphan");
    cfg.bindings = vec![RabbitMqBinding {
        exchange: "no-such-exchange".into(),
        routing_key: "".into(),
        exchange_kind: None,
    }];
    let source = RabbitMqSource::new(cfg).await.unwrap();
    let err = source.fetch_all().await.unwrap_err();
    assert!(err.to_string().contains("no-such-exchange"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_the_queue_mid_run_fails_loudly() {
    let broker = start_broker().await;
    declare_queue(&broker.url, "doomed").await;
    let mut cfg = source_cfg(&broker.url, "doomed");
    cfg.idle_timeout_secs = Some(30);
    let source = RabbitMqSource::new(cfg).await.unwrap();
    let url = broker.url.clone();
    let deleter = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        let conn = connect(&url).await;
        let ch = conn.create_channel().await.unwrap();
        ch.queue_delete("doomed".into(), Default::default())
            .await
            .unwrap();
    });
    let err = source.fetch_all().await.unwrap_err();
    deleter.await.unwrap();
    assert!(err.to_string().contains("doomed"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn check_probes_connect_and_queue() {
    let broker = start_broker().await;
    let ctx = faucet_core::check::CheckContext::default();

    let source = RabbitMqSource::new(source_cfg(&broker.url, "not-yet"))
        .await
        .unwrap();
    let report = source.check(&ctx).await.unwrap();
    assert_eq!(report.failed_count(), 0);
    assert!(matches!(
        report.probes[1].status,
        faucet_core::check::ProbeStatus::Skip { .. }
    ));

    let mut cfg = source_cfg(&broker.url, "not-yet");
    cfg.declare_queue = false;
    let report = RabbitMqSource::new(cfg)
        .await
        .unwrap()
        .check(&ctx)
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 1);

    declare_queue(&broker.url, "exists").await;
    let report = RabbitMqSource::new(source_cfg(&broker.url, "exists"))
        .await
        .unwrap()
        .check(&ctx)
        .await
        .unwrap();
    assert_eq!(report.failed_count(), 0);
    assert!(matches!(
        report.probes[1].status,
        faucet_core::check::ProbeStatus::Pass
    ));
    assert_eq!(wait_ready(&broker.url, "exists", 0).await, 0);
}
