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
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::localstack::LocalStack;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SqsSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "sqs");
}

// ── Check 5: capabilities truthful (Docker) ─────────────────────────────────

async fn start_localstack() -> (ContainerAsync<LocalStack>, String) {
    use testcontainers::ImageExt;
    let image = LocalStack::default().with_env_var("SERVICES", "sqs");
    let container = image.start().await.expect("localstack start");
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
