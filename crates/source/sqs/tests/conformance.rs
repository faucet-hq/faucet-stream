//! `faucet-conformance` battery for the SQS source.
//!
//! Check 1 (config-schema validity) and check 6 (errors, not panics) are pure /
//! offline and always run. Check 2 (bounded-memory streaming) boots LocalStack
//! via testcontainers and so requires Docker — it runs in CI alongside the
//! other integration tests.
//!
//! Check 3 (bookmark round-trip) does not apply: the SQS source drains the
//! queue with no resumable bookmark (every page carries an informational
//! `{queue, consumed}` bookmark only so the pipeline flushes before deleting).

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_source_sqs::{SqsCredentials, SqsSource, SqsSourceConfig};
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::localstack::LocalStack;

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SqsSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-sqs");
}

// ── Check 6: errors, not panics (offline) ───────────────────────────────────

/// Point the source at an unreachable endpoint (`http://127.0.0.1:1`, which
/// refuses connections immediately). `new()` stays lazy — no container needed —
/// and the first `ReceiveMessage` fails with a typed `FaucetError` on both the
/// `fetch_all` and `stream_pages` paths, never a panic.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let mut cfg = SqsSourceConfig::new("https://sqs.us-east-1.amazonaws.com/1/does-not-exist");
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some("http://127.0.0.1:1".into());
    cfg.credentials = test_credentials();
    cfg.wait_time_seconds = 0;
    // A terminating run is required by config validation.
    cfg.idle_timeout_secs = Some(1);
    cfg.max_messages = Some(10);

    let source = SqsSource::new(cfg).await.expect("source builds lazily");
    // Check 10: connector_name is non-empty (metric-cardinality contract).
    faucet_conformance::assert_connector_name_nonempty(&source);
    assert_errors_not_panics(&source).await;
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

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
    faucet_source_sqs::build_client(Some("us-east-1"), Some(endpoint), &test_credentials())
        .await
        .expect("client")
}

/// Create a queue (large visibility timeout so nothing is redelivered mid-run)
/// and return its URL. Retries until LocalStack's SQS endpoint is ready.
async fn create_queue(client: &aws_sdk_sqs::Client, name: &str) -> String {
    use aws_sdk_sqs::types::QueueAttributeName;
    for _ in 0..120 {
        match client
            .create_queue()
            .queue_name(name)
            .attributes(QueueAttributeName::VisibilityTimeout, "300")
            .send()
            .await
        {
            Ok(out) => return out.queue_url().expect("queue url").to_string(),
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    panic!("localstack sqs never became ready");
}

/// Send `n` JSON messages in batches of 10.
async fn seed(client: &aws_sdk_sqs::Client, queue_url: &str, n: usize) {
    use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
    let mut i = 0usize;
    while i < n {
        let end = (i + 10).min(n);
        let entries: Vec<SendMessageBatchRequestEntry> = (i..end)
            .map(|j| {
                SendMessageBatchRequestEntry::builder()
                    .id(format!("m{j}"))
                    .message_body(format!("{{\"i\":{j}}}"))
                    .build()
                    .expect("entry")
            })
            .collect();
        let out = client
            .send_message_batch()
            .queue_url(queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .expect("send_message_batch");
        assert!(out.failed().is_empty(), "seeding must not fail");
        i = end;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    let queue_url = create_queue(&client, "conformance").await;
    seed(&client, &queue_url, 5_000).await;

    let mut cfg = SqsSourceConfig::new(&queue_url);
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(endpoint.clone());
    cfg.credentials = test_credentials();
    cfg.wait_time_seconds = 1;
    cfg.idle_timeout_secs = Some(15);
    cfg.batch_size = 250;

    let source = SqsSource::new(cfg).await.expect("source");
    // Check 11: preflight check() is well-formed against the live queue
    // (`GetQueueAttributes` → a Pass probe inside Ok(report); no messages
    // consumed).
    faucet_conformance::assert_preflight_check_wellformed(
        &source,
        &faucet_core::check::CheckContext::default(),
    )
    .await;
    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;
    // _container stays alive to here
}

// ── #456 C1: deletion must not precede the downstream write ─────────────────

/// Create a queue whose messages become visible again immediately, so a
/// non-deleted message can be observed without waiting out a visibility timeout.
async fn create_queue_visible_immediately(client: &aws_sdk_sqs::Client, name: &str) -> String {
    use aws_sdk_sqs::types::QueueAttributeName;
    for _ in 0..120 {
        match client
            .create_queue()
            .queue_name(name)
            .attributes(QueueAttributeName::VisibilityTimeout, "0")
            .send()
            .await
        {
            Ok(out) => return out.queue_url().expect("queue url").to_string(),
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    panic!("localstack sqs never became ready");
}

/// Count the distinct message bodies still retrievable from the queue.
async fn count_remaining(client: &aws_sdk_sqs::Client, queue_url: &str) -> usize {
    let mut seen = std::collections::HashSet::new();
    // Several passes: SQS returns an arbitrary subset per call.
    for _ in 0..10 {
        let out = client
            .receive_message()
            .queue_url(queue_url)
            .max_number_of_messages(10)
            .wait_time_seconds(1)
            .send()
            .await
            .expect("receive_message");
        for m in out.messages() {
            if let Some(b) = m.body() {
                seen.insert(b.to_string());
            }
        }
    }
    seen.len()
}

/// A page's messages must still be in the queue after the page has been yielded
/// but before the consumer comes back for the next one — that is the window in
/// which the sink write happens, and deleting inside it turns SQS's at-least-once
/// contract into at-most-once (#456 C1).
///
/// The test abandons the stream after one page, which is what a sink error or a
/// crash looks like from the source's point of view.
#[tokio::test(flavor = "multi_thread")]
async fn messages_survive_a_downstream_failure_after_the_page_is_yielded() {
    use faucet_core::Source as _;
    use futures::StreamExt;

    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    let queue_url = create_queue_visible_immediately(&client, "ack-ordering").await;
    seed(&client, &queue_url, 4).await;

    let mut cfg = SqsSourceConfig::new(&queue_url);
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(endpoint.clone());
    cfg.credentials = test_credentials();
    cfg.wait_time_seconds = 1;
    cfg.idle_timeout_secs = Some(5);
    cfg.batch_size = 2;

    let source = SqsSource::new(cfg).await.expect("source");
    {
        let ctx = std::collections::HashMap::new();
        let mut pages = source.stream_pages(&ctx, 2);
        let first = pages
            .next()
            .await
            .expect("one page")
            .expect("page is not an error");
        assert_eq!(first.records.len(), 2, "batch_size pages the queue");
        // Abandon the stream: the consumer never resumed us, so nothing this page
        // carried was ever written. Its messages must NOT have been deleted.
        drop(pages);
    }

    assert_eq!(
        count_remaining(&client, &queue_url).await,
        4,
        "no message may be deleted before the page it belongs to is written \
         downstream — every one must still be redeliverable"
    );
}

/// Create a queue with a short visibility timeout.
async fn create_queue_with_visibility(
    client: &aws_sdk_sqs::Client,
    name: &str,
    secs: &str,
) -> String {
    use aws_sdk_sqs::types::QueueAttributeName;
    for _ in 0..120 {
        match client
            .create_queue()
            .queue_name(name)
            .attributes(QueueAttributeName::VisibilityTimeout, secs)
            .send()
            .await
        {
            Ok(out) => return out.queue_url().expect("queue url").to_string(),
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    panic!("localstack sqs never became ready");
}

/// Drain the queue, collecting every record.
async fn drain(source: &SqsSource) -> Vec<serde_json::Value> {
    use faucet_core::Source as _;
    use futures::StreamExt;
    let ctx = std::collections::HashMap::new();
    let mut pages = source.stream_pages(&ctx, 4);
    let mut out = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page");
        assert!(
            page.bookmark.is_some(),
            "every page carries a bookmark (MSG-07)"
        );
        out.extend(page.records);
    }
    out
}

/// MSG-09: a page still being assembled when the queue's visibility timeout
/// runs out must not receive its own messages again — the source keeps
/// renewing the visibility of everything it holds. Two messages, a page of
/// four, a 3 s visibility timeout and an 8 s idle window: without renewal the
/// two come back and fill the page with duplicates.
#[tokio::test(flavor = "multi_thread")]
async fn held_messages_are_not_redelivered_while_a_page_is_assembled() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;

    let config = |queue_url: &str, renew: u32| {
        let mut cfg = SqsSourceConfig::new(queue_url);
        cfg.region = Some("us-east-1".into());
        cfg.endpoint_url = Some(endpoint.clone());
        cfg.credentials = test_credentials();
        cfg.wait_time_seconds = 1;
        cfg.idle_timeout_secs = Some(8);
        cfg.batch_size = 4;
        cfg.visibility_extension_secs = renew;
        cfg
    };

    let renewed = create_queue_with_visibility(&client, "lease-renewed", "3").await;
    seed(&client, &renewed, 2).await;
    let source = SqsSource::new(config(&renewed, 6)).await.expect("source");
    assert_eq!(drain(&source).await.len(), 2, "no redelivery while renewed");

    let unrenewed = create_queue_with_visibility(&client, "lease-unrenewed", "3").await;
    seed(&client, &unrenewed, 2).await;
    let source = SqsSource::new(config(&unrenewed, 0)).await.expect("source");
    assert!(
        drain(&source).await.len() > 2,
        "without renewal the held messages come back (proves the test can fail)"
    );
}

/// A single-group FIFO queue drains completely: SQS hands out nothing more
/// from a group while earlier messages are in flight, so the source emits and
/// deletes after every receive instead of waiting for a full page (#789
/// MSG-48). Records carry the SQS message id with `include_metadata` (MSG-91).
#[tokio::test(flavor = "multi_thread")]
async fn a_single_group_fifo_queue_drains_in_order() {
    use aws_sdk_sqs::types::QueueAttributeName;
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    let mut queue_url = None;
    for _ in 0..120 {
        match client
            .create_queue()
            .queue_name("orders.fifo")
            .attributes(QueueAttributeName::FifoQueue, "true")
            .attributes(QueueAttributeName::ContentBasedDeduplication, "true")
            .attributes(QueueAttributeName::VisibilityTimeout, "300")
            .send()
            .await
        {
            Ok(out) => {
                queue_url = Some(out.queue_url().unwrap().to_string());
                break;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    let queue_url = queue_url.expect("fifo queue");
    for i in 0..15 {
        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(format!("{{\"i\":{i}}}"))
            .message_group_id("g")
            .send()
            .await
            .expect("send");
    }
    let mut cfg = SqsSourceConfig::new(&queue_url);
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(endpoint.clone());
    cfg.credentials = test_credentials();
    cfg.idle_timeout_secs = Some(4);
    cfg.wait_time_seconds = 1;
    cfg.include_metadata = true;
    let source = SqsSource::new(cfg).await.unwrap();
    let got = drain(&source).await;
    let order: Vec<i64> = got
        .iter()
        .map(|r| r["payload"]["i"].as_i64().unwrap())
        .collect();
    assert_eq!(order, (0..15).collect::<Vec<i64>>());
    assert!(got.iter().all(|r| r["message_id"].is_string()));
}
