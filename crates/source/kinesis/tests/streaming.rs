//! Integration tests for `KinesisSource` against LocalStack (Docker).
//!
//! Each test boots its own LocalStack container and creates its own stream,
//! so they are isolated and safe to run in parallel.

use faucet_core::Source;
use faucet_source_kinesis::{KinesisCredentials, KinesisSource, KinesisSourceConfig};
use futures::StreamExt;
use std::collections::HashMap;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::localstack::LocalStack;

/// Start LocalStack with the kinesis service and return (container, endpoint).
async fn start_localstack() -> (ContainerAsync<LocalStack>, String) {
    use testcontainers::ImageExt;
    let image = LocalStack::default().with_env_var("SERVICES", "kinesis");
    let container = image.start().await.expect("localstack start");
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
    faucet_source_kinesis::build_client(Some("us-east-1"), Some(endpoint), &test_credentials())
        .await
        .expect("client")
}

/// Wait until the mapped endpoint actually accepts Kinesis API calls —
/// the container's port maps before the service is ready.
async fn await_ready(client: &aws_sdk_kinesis::Client) {
    for _ in 0..120 {
        if client.list_streams().send().await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("localstack kinesis never became ready");
}

/// Create a stream and wait until it is ACTIVE.
async fn create_stream(client: &aws_sdk_kinesis::Client, name: &str, shards: i32) {
    await_ready(client).await;
    client
        .create_stream()
        .stream_name(name)
        .shard_count(shards)
        .send()
        .await
        .expect("create stream");
    for _ in 0..60 {
        let out = client
            .describe_stream_summary()
            .stream_name(name)
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
    panic!("stream {name} never became ACTIVE");
}

/// Put `n` JSON records with rotating partition keys.
async fn put_records(client: &aws_sdk_kinesis::Client, stream: &str, start: usize, n: usize) {
    use aws_sdk_kinesis::primitives::Blob;
    use aws_sdk_kinesis::types::PutRecordsRequestEntry;
    for chunk_start in (start..start + n).step_by(100) {
        let entries: Vec<PutRecordsRequestEntry> = (chunk_start
            ..(chunk_start + 100).min(start + n))
            .map(|i| {
                PutRecordsRequestEntry::builder()
                    .partition_key(format!("user-{}", i % 7))
                    .data(Blob::new(format!("{{\"i\":{i}}}").into_bytes()))
                    .build()
                    .expect("entry")
            })
            .collect();
        let out = client
            .put_records()
            .stream_name(stream)
            .set_records(Some(entries))
            .send()
            .await
            .expect("put_records");
        assert_eq!(
            out.failed_record_count().unwrap_or(0),
            0,
            "seeding must not throttle"
        );
    }
}

fn source_config(endpoint: &str, stream: &str) -> KinesisSourceConfig {
    let mut cfg = KinesisSourceConfig::new(stream);
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(endpoint.into());
    cfg.credentials = test_credentials();
    cfg.poll_interval_ms = 200;
    cfg.idle_termination_secs = Some(3);
    cfg.batch_size = 25;
    cfg
}

#[tokio::test(flavor = "multi_thread")]
async fn consumes_across_shards_with_metadata_and_bookmarks() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client, "events", 3).await;
    put_records(&client, "events", 0, 90).await;

    let source = KinesisSource::new(source_config(&endpoint, "events"))
        .await
        .expect("source");
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 25);

    let mut records = Vec::new();
    let mut last_bookmark = None;
    while let Some(page) = pages.next().await {
        let page = page.expect("page");
        assert!(page.bookmark.is_some(), "every page carries the shard map");
        last_bookmark = page.bookmark;
        records.extend(page.records);
    }
    assert_eq!(records.len(), 90, "all seeded records consumed");

    // Record shape + per-shard sequence monotonicity.
    let mut by_shard: HashMap<String, Vec<String>> = HashMap::new();
    let mut seen_is: Vec<i64> = Vec::new();
    for r in &records {
        assert!(r["partition_key"].as_str().unwrap().starts_with("user-"));
        assert!(r["approximate_arrival_timestamp_ms"].as_i64().unwrap() > 0);
        seen_is.push(r["data"]["i"].as_i64().expect("json payload"));
        by_shard
            .entry(r["shard_id"].as_str().unwrap().to_string())
            .or_default()
            .push(r["sequence_number"].as_str().unwrap().to_string());
    }
    seen_is.sort_unstable();
    assert_eq!(seen_is, (0..90).collect::<Vec<i64>>(), "no loss, no dupes");
    assert!(by_shard.len() > 1, "records spread across shards");
    for (shard, seqs) in &by_shard {
        let mut sorted = seqs.clone();
        sorted.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
        assert_eq!(&sorted, seqs, "sequences monotonic within {shard}");
    }

    // The final bookmark maps every shard that produced records.
    let bookmark = last_bookmark.expect("bookmark");
    let shards = bookmark["shards"].as_object().expect("shard map");
    assert_eq!(shards.len(), by_shard.len());

    // ── Resume: a fresh source with the bookmark sees only new records ──────
    put_records(&client, "events", 100, 20).await;
    let resumed = KinesisSource::new(source_config(&endpoint, "events"))
        .await
        .expect("resumed source");
    resumed
        .apply_start_bookmark(bookmark)
        .await
        .expect("apply bookmark");
    let records = resumed.fetch_all().await.expect("resume fetch");
    let mut is: Vec<i64> = records
        .iter()
        .map(|r| r["data"]["i"].as_i64().unwrap())
        .collect();
    is.sort_unstable();
    assert_eq!(
        is,
        (100..120).collect::<Vec<i64>>(),
        "resume replays nothing already bookmarked"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mid_batch_page_bookmark_is_a_valid_resume_point() {
    // #321 C2: a single GetRecords batch returns all 30 records
    // (records_per_request high), but batch_size = 10 forces the generator to
    // emit 3 pages. Each page's bookmark must equal its own last record's
    // sequence — NOT the batch-final sequence. Resuming from the FIRST page's
    // bookmark must therefore see records 10..30 (nothing before is skipped).
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client, "midbatch", 1).await;
    put_records(&client, "midbatch", 0, 30).await;

    let mut cfg = source_config(&endpoint, "midbatch");
    cfg.batch_size = 10;
    cfg.records_per_request = 10_000; // one GetRecords returns the whole shard
    let source = KinesisSource::new(cfg).await.expect("source");
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 10);

    // Capture the FIRST full page (10 records) and its bookmark.
    let first = pages.next().await.expect("a page").expect("ok");
    assert_eq!(
        first.records.len(),
        10,
        "first page holds one batch_size chunk"
    );
    let first_bookmark = first.bookmark.expect("first page carries a bookmark");
    // Drain the rest so the source terminates cleanly.
    while let Some(p) = pages.next().await {
        let _ = p.expect("ok");
    }

    // Resume a fresh source from the FIRST page's bookmark: it must replay
    // exactly records 10..30 — proving the bookmark did not run ahead of the
    // page's own contents.
    let mut rcfg = source_config(&endpoint, "midbatch");
    rcfg.records_per_request = 10_000;
    let resumed = KinesisSource::new(rcfg).await.expect("resumed source");
    resumed
        .apply_start_bookmark(first_bookmark)
        .await
        .expect("apply bookmark");
    let records = resumed.fetch_all().await.expect("resume fetch");
    let mut is: Vec<i64> = records
        .iter()
        .map(|r| r["data"]["i"].as_i64().unwrap())
        .collect();
    is.sort_unstable();
    assert_eq!(
        is,
        (10..30).collect::<Vec<i64>>(),
        "resume from the first page's bookmark must lose none of records 10..30"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn max_messages_and_check_probe() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client, "capped", 1).await;
    put_records(&client, "capped", 0, 50).await;

    let mut cfg = source_config(&endpoint, "capped");
    cfg.idle_termination_secs = None;
    cfg.max_messages = Some(10);
    let source = KinesisSource::new(cfg).await.expect("source");
    let records = source.fetch_all().await.expect("fetch");
    assert_eq!(records.len(), 10, "stops at max_messages");

    // check(): a healthy stream passes, a missing stream fails.
    let report = source
        .check(&faucet_core::CheckContext::default())
        .await
        .expect("check");
    assert_eq!(report.failed_count(), 0);

    let mut missing = source_config(&endpoint, "no-such-stream");
    missing.max_messages = Some(1);
    let missing = KinesisSource::new(missing).await.expect("source");
    let report = missing
        .check(&faucet_core::CheckContext::default())
        .await
        .expect("check");
    assert_eq!(report.failed_count(), 1, "missing stream fails the probe");
}

/// #733 — `lag()` reports `MillisBehindLatest` in seconds: probed per shard
/// from the resume position before any read, and the value the reads of a
/// run reported afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn lag_reports_millis_behind_latest() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    create_stream(&client, "lagging", 2).await;
    put_records(&client, "lagging", 0, 20).await;

    let source = KinesisSource::new(source_config(&endpoint, "lagging"))
        .await
        .expect("source");
    let probed = source.lag().await.expect("lag").expect("a reading");
    assert!(probed.seconds.unwrap() >= 0.0, "{probed:?}");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 25);
    let mut n = 0;
    while let Some(page) = pages.next().await {
        n += page.expect("page").records.len();
    }
    drop(pages);
    assert_eq!(n, 20);
    let after = source.lag().await.expect("lag").expect("a reading");
    assert!(after.seconds.unwrap() >= 0.0, "{after:?}");
}
