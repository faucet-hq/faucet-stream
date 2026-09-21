//! #629 — route small objects away from the async job's fixed latency floor.
//!
//! A bulk API pays job-queue + processing time whatever the row count:
//! measured at ~14s of a 22s, 701-row Salesforce `User` run. A synchronous
//! query answers the same request immediately. Bulk is still right for the
//! large objects it exists for, so the choice is made per run from a cheap
//! count probe rather than from a guess baked into the config.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use faucet_source_rest::{AsyncJobConfig, PaginationStyle, RestStream, RestStreamConfig};
use futures::StreamExt as _;
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A config carrying **both** shapes: the async job for large objects, and the
/// ordinary paginated read for small ones.
fn cfg(server: &MockServer, threshold: u64) -> RestStreamConfig {
    let mut c = RestStreamConfig::new(&server.uri(), "/sync");
    c.pagination = PaginationStyle::None;
    c.records_path = Some("$.records[*]".into());
    let job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs", "json": { "query": "SELECT Id FROM T" } },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 1, "timeout_secs": 30 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": { "url": "/jobs/${job_id}/result", "records_path": "$.records[*]" },
        "sync_below_rows": threshold,
        "count": { "url": "/count", "count_path": "$.total" }
    }))
    .expect("async_job config");
    c.async_job = Some(job);
    c
}

/// Mount the count probe, the synchronous read, and the whole job lifecycle,
/// counting which of the two data paths was actually used.
async fn mount(server: &MockServer, count: Value) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
    Mock::given(method("GET"))
        .and(path("/count"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "total": count })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/sync"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "records": [{ "src": "sync" }] })),
        )
        .mount(server)
        .await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "j1" })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "Complete" })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1/result"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "records": [{ "src": "bulk" }] })),
        )
        .mount(server)
        .await;
    (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)))
}

async fn read(cfg: RestStreamConfig) -> Vec<Value> {
    let stream = RestStream::new(cfg).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as faucet_core::Source>::stream_pages(&stream, &ctx, 1000);
    let mut out = Vec::new();
    while let Some(p) = pages.next().await {
        out.extend(p.unwrap().records);
    }
    out
}

#[tokio::test]
async fn a_small_object_skips_the_job_entirely() {
    let server = MockServer::start().await;
    mount(&server, json!(10)).await;
    let records = read(cfg(&server, 1000)).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0]["src"], "sync",
        "10 rows under a 1000-row threshold must take the synchronous path"
    );
    // And the job was never submitted — the whole point is not paying its
    // async floor, which a submitted-then-ignored job would still cost.
    let submitted = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path() == "/jobs")
        .count();
    assert_eq!(submitted, 0, "no job may be submitted for a small object");
}

#[tokio::test]
async fn a_large_object_still_uses_the_job() {
    let server = MockServer::start().await;
    mount(&server, json!(50_000)).await;
    let records = read(cfg(&server, 1000)).await;
    assert_eq!(records[0]["src"], "bulk", "above the threshold, Bulk wins");
}

#[tokio::test]
async fn a_count_that_arrives_as_a_string_is_accepted() {
    // Common enough, and unambiguous — a JSON API returning "42" for a count
    // should not silently fall back to the slow path.
    let server = MockServer::start().await;
    mount(&server, json!("10")).await;
    assert_eq!(read(cfg(&server, 1000)).await[0]["src"], "sync");
}

#[tokio::test]
async fn a_failed_probe_falls_back_to_the_job_rather_than_failing_the_run() {
    // The probe is an optimisation. Failing a run because an optimisation's
    // probe 404'd would be the worse trade — the job path is always correct.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/count"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "j1" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "Complete" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j1/result"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "records": [{ "src": "bulk" }] })),
        )
        .mount(&server)
        .await;
    assert_eq!(read(cfg(&server, 1000)).await[0]["src"], "bulk");
}

#[tokio::test]
async fn half_a_routing_pair_is_a_config_error() {
    // A threshold with no probe can never fire; a probe with no threshold is
    // never read. Either alone is a config that reads as doing something and
    // does nothing.
    let mut c = RestStreamConfig::new("https://api.example.com", "/x");
    c.pagination = PaginationStyle::None;
    let job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs", "json": {} },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 1, "timeout_secs": 30 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": { "url": "/jobs/${job_id}/result" },
        "sync_below_rows": 100
    }))
    .unwrap();
    c.async_job = Some(job);
    let err = c
        .validate()
        .expect_err("a threshold with no probe must be rejected");
    assert!(err.to_string().contains("count"), "{err}");
}
