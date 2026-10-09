//! `faucet-conformance` battery against the real Kinesis sink (via LocalStack).
//!
//! The Kinesis sink is append-only — it advertises no idempotency mechanism,
//! so the battery exercises the **honest branch**:
//! - check 1 `assert_config_schema_valid_value` (value form, for sinks);
//! - check 5 `assert_capabilities_truthful` — Append works, and the sink does
//!   *not* claim idempotent/keyed dedup (so the pipeline correctly refuses
//!   `delivery: exactly_once` for it).
//!
//! Check 5 requires Docker (LocalStack via testcontainers), mirroring `sink.rs`.
use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::Sink;
use faucet_sink_kinesis::{KinesisCredentials, KinesisSink, KinesisSinkConfig};
use testcontainers::ContainerAsync;
use testcontainers_modules::localstack::LocalStack;

const STREAM: &str = "conformance";

async fn start_localstack() -> (ContainerAsync<LocalStack>, String) {
    use testcontainers::ImageExt;
    let container = faucet_conformance::containers::start(|| {
        LocalStack::default().with_env_var("SERVICES", "kinesis")
    })
    .await;
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("localstack port");
    (container, format!("http://127.0.0.1:{port}"))
}

fn test_credentials() -> KinesisCredentials {
    KinesisCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    }
}

async fn raw_client(endpoint: &str) -> aws_sdk_kinesis::Client {
    faucet_sink_kinesis::build_client(Some("us-east-1"), Some(endpoint), &test_credentials())
        .await
        .expect("client")
}

async fn await_ready(client: &aws_sdk_kinesis::Client) {
    for _ in 0..120 {
        if client.list_streams().send().await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("localstack kinesis never became ready");
}

async fn create_stream(client: &aws_sdk_kinesis::Client) {
    await_ready(client).await;
    client
        .create_stream()
        .stream_name(STREAM)
        .shard_count(1)
        .send()
        .await
        .expect("create stream");
    for _ in 0..60 {
        let out = client
            .describe_stream_summary()
            .stream_name(STREAM)
            .send()
            .await
            .expect("describe");
        if out
            .stream_description_summary()
            .map(|d| d.stream_status() == &aws_sdk_kinesis::types::StreamStatus::Active)
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("stream never became ACTIVE");
}

/// Count durable records: drain every shard from TrimHorizon and total the
/// records read back. Zero before any write.
async fn count_records(client: &aws_sdk_kinesis::Client) -> usize {
    let shards = client
        .list_shards()
        .stream_name(STREAM)
        .send()
        .await
        .expect("list shards");
    let mut total = 0usize;
    for shard in shards.shards() {
        let mut iterator = client
            .get_shard_iterator()
            .stream_name(STREAM)
            .shard_id(shard.shard_id())
            .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
            .send()
            .await
            .expect("iterator")
            .shard_iterator()
            .map(str::to_string);
        let mut empty_polls = 0;
        while let Some(it) = iterator {
            let resp = client
                .get_records()
                .shard_iterator(&it)
                .send()
                .await
                .expect("get records");
            total += resp.records().len();
            if resp.records().is_empty() {
                empty_polls += 1;
                if empty_polls >= 5 {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            } else {
                empty_polls = 0;
            }
            iterator = resp.next_shard_iterator().map(str::to_string);
        }
    }
    total
}

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_kinesis::KinesisSinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "kinesis");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client).await;

    // Default partition_key is Random, so the battery's `{id, v}` record needs
    // no partition-key field.
    let mut cfg = KinesisSinkConfig::new(STREAM);
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(endpoint.clone());
    cfg.credentials = test_credentials();
    let sink = KinesisSink::new(cfg).await.expect("sink");
    faucet_conformance::assert_batch_atomicity_declared(&sink);

    // Check 10: connector_name is non-empty (metric-cardinality contract).
    faucet_conformance::assert_connector_name_nonempty_value(
        sink.connector_name(),
        sink.connector_name(),
    );
    // Check 11: preflight check() is well-formed against the live stream
    // (`DescribeStreamSummary` → a Pass probe inside Ok(report); nothing
    // written).
    faucet_conformance::assert_sink_preflight_check_wellformed(
        &sink,
        &faucet_core::check::CheckContext::default(),
    )
    .await;

    let client_ref = &client;
    faucet_conformance::assert_capabilities_truthful(&sink, || async move {
        // PutRecords completes synchronously within write_batch; the records
        // are readable from TrimHorizon immediately after.
        count_records(client_ref).await
    })
    .await;

    // The honest branch must have left the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}
