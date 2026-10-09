//! `faucet-conformance` battery against the real SQS sink (via LocalStack).
//!
//! The SQS sink is append-only — it advertises no idempotency mechanism, so the
//! battery exercises the **honest branch**:
//! - check 1 `assert_config_schema_valid_value` (value form, for sinks) — pure /
//!   offline, always runs;
//! - check 5 `assert_capabilities_truthful` — Append works, and the sink does
//!   *not* claim idempotent / keyed dedup (so the pipeline correctly refuses
//!   `delivery: exactly_once`).
//!
//! Check 5 requires Docker (LocalStack via testcontainers), mirroring `sink.rs`.

use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink;
use faucet_sink_sqs::{SqsCredentials, SqsSink, SqsSinkConfig};
use testcontainers::ContainerAsync;
use testcontainers_modules::localstack::LocalStack;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SqsSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "sqs");
}

// ── Check 5: capabilities truthful (Docker) ─────────────────────────────────

async fn start_localstack() -> (ContainerAsync<LocalStack>, String) {
    use testcontainers::ImageExt;
    let container = faucet_conformance::containers::start(|| {
        LocalStack::default().with_env_var("SERVICES", "sqs")
    })
    .await;
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("localstack port");
    (container, format!("http://127.0.0.1:{port}"))
}

fn test_credentials() -> SqsCredentials {
    SqsCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    }
}

async fn raw_client(endpoint: &str) -> aws_sdk_sqs::Client {
    faucet_sink_sqs::build_client(Some("us-east-1"), Some(endpoint), &test_credentials())
        .await
        .expect("client")
}

async fn create_queue(client: &aws_sdk_sqs::Client, name: &str) -> String {
    for _ in 0..120 {
        match client.create_queue().queue_name(name).send().await {
            Ok(out) => return out.queue_url().expect("queue url").to_string(),
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    panic!("localstack sqs never became ready");
}

/// Count durable messages currently visible in the queue via the
/// `ApproximateNumberOfMessages` attribute. LocalStack updates this promptly
/// after a `SendMessageBatch`.
async fn count_messages(client: &aws_sdk_sqs::Client, queue_url: &str) -> usize {
    use aws_sdk_sqs::types::QueueAttributeName;
    for _ in 0..40 {
        let out = client
            .get_queue_attributes()
            .queue_url(queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
            .expect("get_queue_attributes");
        if let Some(v) = out
            .attributes()
            .and_then(|a| a.get(&QueueAttributeName::ApproximateNumberOfMessages))
            && let Ok(n) = v.parse::<usize>()
        {
            return n;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    0
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    let queue_url = create_queue(&client, "conformance").await;

    let mut cfg = SqsSinkConfig::new(&queue_url);
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(endpoint.clone());
    cfg.credentials = test_credentials();
    let sink = SqsSink::new(cfg).await.expect("sink");
    faucet_conformance::assert_batch_atomicity_declared(&sink);

    // Check 10: connector_name is non-empty (metric-cardinality contract).
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    // Check 11: preflight check() is well-formed against the live queue
    // (`GetQueueAttributes` → a Pass probe inside Ok(report); nothing written).
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    let client_ref = &client;
    let url = queue_url.clone();
    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let url = url.clone();
        async move { count_messages(client_ref, &url).await }
    })
    .await;

    // The honest branch must have left the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}

/// Records up to the 1 MiB SQS limit are sent (the old 256 KiB cap refused
/// them, #789 MSG-77), and a FIFO queue receives a page in order with one
/// request in flight at a time (#789 MSG-35).
#[tokio::test(flavor = "multi_thread")]
async fn large_records_and_fifo_order() {
    use aws_sdk_sqs::types::QueueAttributeName;
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    let std_url = create_queue(&client, "big").await;
    let fifo_url = client
        .create_queue()
        .queue_name("ordered.fifo")
        .attributes(QueueAttributeName::FifoQueue, "true")
        .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
        .send()
        .await
        .expect("fifo queue")
        .queue_url()
        .unwrap()
        .to_string();
    let config = |url: &str| {
        let mut c = SqsSinkConfig::new(url);
        c.region = Some("us-east-1".into());
        c.endpoint_url = Some(endpoint.clone());
        c.credentials = test_credentials();
        c
    };

    // Two 200 KiB records fit one 1 MiB request; a service still enforcing
    // the old 256 KiB request limit (LocalStack) is answered by re-splitting.
    let big = serde_json::json!({ "blob": "x".repeat(200 * 1024) });
    let sink = SqsSink::new(config(&std_url)).await.unwrap();
    let outcomes = sink.write_batch_partial(&[big.clone(), big]).await.unwrap();
    assert!(outcomes.iter().all(Result::is_ok), "{outcomes:?}");

    let mut fifo = config(&fifo_url);
    assert!(
        SqsSink::new(fifo.clone()).await.is_err(),
        "FIFO needs a group id"
    );
    fifo.message_group_id = Some("g".into());
    let sink = SqsSink::new(fifo).await.unwrap();
    let rows: Vec<_> = (0..25).map(|i| serde_json::json!({ "i": i })).collect();
    assert_eq!(sink.write_batch(&rows).await.unwrap(), 25);
    let mut seen = Vec::new();
    while seen.len() < 25 {
        let out = client
            .receive_message()
            .queue_url(&fifo_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await
            .unwrap();
        for m in out.messages() {
            let v: serde_json::Value = serde_json::from_str(m.body().unwrap()).unwrap();
            seen.push(v["i"].as_i64().unwrap());
            client
                .delete_message()
                .queue_url(&fifo_url)
                .receipt_handle(m.receipt_handle().unwrap())
                .send()
                .await
                .unwrap();
        }
    }
    assert_eq!(seen, (0..25).collect::<Vec<i64>>());
}
