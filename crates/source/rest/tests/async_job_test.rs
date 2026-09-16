//! Integration test for the async-job source pattern (#514):
//! submit → poll (not-ready → ready) → fetch → decode.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::Source;
use faucet_source_rest::{
    AsyncJobConfig, DecodeStep, ParseFormat, ParseSpec, RestStream, RestStreamConfig,
};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// Poll responds "InProgress" once, then "Complete".
struct PollThenComplete(Arc<AtomicUsize>);
impl Respond for PollThenComplete {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        let state = if n == 0 { "InProgress" } else { "Complete" };
        ResponseTemplate::new(200).set_body_json(json!({ "state": state }))
    }
}

#[tokio::test]
async fn async_job_submit_poll_fetch_decode() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "job-1" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1"))
        .respond_with(PollThenComplete(Arc::new(AtomicUsize::new(0))))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1/result"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,name\n1,alice\n2,bob\n"))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs", "json": { "query": "SELECT 1" } },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": { "method": "GET", "url": "/jobs/${job_id}/result" }
    }))
    .unwrap();

    let mut cfg = RestStreamConfig::new(&server.uri(), "").decode(vec![DecodeStep::Parse {
        parse: ParseSpec {
            format: ParseFormat::Csv,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    }]);
    cfg.async_job = Some(async_job);

    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], "1");
    assert_eq!(records[1]["name"], "bob");
}

/// Poll responds "pending" once, then "succeeded" carrying the download URL in
/// the body (the Stripe report-run shape).
struct StripeReportPoll {
    calls: Arc<AtomicUsize>,
    download_url: String,
}
impl Respond for StripeReportPoll {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        let body = if n == 0 {
            json!({ "status": "pending" })
        } else {
            json!({ "status": "succeeded", "result": { "url": self.download_url } })
        };
        ResponseTemplate::new(200).set_body_json(body)
    }
}

#[tokio::test]
async fn async_job_fetch_url_from_poll_body() {
    let server = MockServer::start().await;
    // Absolute one-time download URL returned in the poll body.
    let download_url = format!("{}/files/report-abc.csv", server.uri());

    Mock::given(method("POST"))
        .and(path("/v1/reporting/report_runs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "frr_1" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/reporting/report_runs/frr_1"))
        .respond_with(StripeReportPoll {
            calls: Arc::new(AtomicUsize::new(0)),
            download_url: download_url.clone(),
        })
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/files/report-abc.csv"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,name\n1,alice\n2,bob\n"))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/v1/reporting/report_runs" },
        "job_id": "$.id",
        "poll": {
            "url": "/v1/reporting/report_runs/${job_id}",
            "interval_secs": 0,
            "timeout_secs": 30
        },
        "status": { "path": "$.status", "success": ["succeeded"], "failure": ["failed"] },
        "fetch": { "url_from": "$.result.url" }
    }))
    .unwrap();

    let mut cfg = RestStreamConfig::new(&server.uri(), "").decode(vec![DecodeStep::Parse {
        parse: ParseSpec {
            format: ParseFormat::Csv,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    }]);
    cfg.async_job = Some(async_job);

    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], "1");
    assert_eq!(records[1]["name"], "bob");
}

#[tokio::test]
async fn async_job_url_from_no_match_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "j" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j"))
        // Terminal, but no `result.url` in the body.
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "status": "succeeded" })))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs" },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 5 },
        "status": { "path": "$.status", "success": ["succeeded"] },
        "fetch": { "url_from": "$.result.url" }
    }))
    .unwrap();
    let mut cfg = RestStreamConfig::new(&server.uri(), "");
    cfg.async_job = Some(async_job);
    let err = RestStream::new(cfg).unwrap().fetch_all().await.unwrap_err();
    assert!(err.to_string().contains("url_from"), "{err}");
}

#[tokio::test]
async fn async_job_failure_status_errors() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "j" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/j"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "Failed" })))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs" },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 5 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": { "url": "/jobs/${job_id}/result" }
    }))
    .unwrap();
    let mut cfg = RestStreamConfig::new(&server.uri(), "");
    cfg.async_job = Some(async_job);
    let err = RestStream::new(cfg).unwrap().fetch_all().await.unwrap_err();
    assert!(err.to_string().contains("Failed"), "{err}");
}

/// Locator-paged fetch (#557): page 1 carries a `Sforce-Locator` header pointing
/// to page 2; page 2 is terminal (no locator). Deterministic by call count.
struct FetchTwoLocatorPages(Arc<AtomicUsize>);
impl Respond for FetchTwoLocatorPages {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            ResponseTemplate::new(200)
                .insert_header("Sforce-Locator", "loc2")
                .set_body_string("id,name\n1,alice\n")
        } else {
            ResponseTemplate::new(200).set_body_string("id,name\n2,bob\n")
        }
    }
}

fn bulk_job() -> AsyncJobConfig {
    serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs" },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": {
            "method": "GET",
            "url": "/jobs/${job_id}/result",
            "locator_header": "Sforce-Locator",
            "locator_param": "locator"
        }
    }))
    .unwrap()
}

async fn mount_bulk_two_pages(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "job-1" })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "Complete" })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1/result"))
        .respond_with(FetchTwoLocatorPages(Arc::new(AtomicUsize::new(0))))
        .mount(server)
        .await;
}

fn bulk_csv_config(server: &MockServer) -> RestStreamConfig {
    let mut cfg = RestStreamConfig::new(&server.uri(), "").decode(vec![DecodeStep::Parse {
        parse: ParseSpec {
            format: ParseFormat::Csv,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    }]);
    cfg.async_job = Some(bulk_job());
    cfg
}

/// #623: a locator-paged Bulk job must stream ONE `StreamPage` per locator page
/// (bounded memory), not concatenate the whole extract into a single page.
#[tokio::test]
async fn async_job_bulk_streams_one_page_per_locator() {
    let server = MockServer::start().await;
    mount_bulk_two_pages(&server).await;

    let stream = RestStream::new(bulk_csv_config(&server)).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);

    let mut collected = Vec::new();
    while let Some(page) = pages.next().await {
        collected.push(page.unwrap());
    }

    // Two distinct pages, one per locator — NOT one concatenated page.
    assert_eq!(
        collected.len(),
        2,
        "expected one StreamPage per locator page"
    );
    assert_eq!(
        collected[0].records.len(),
        1,
        "page 1 holds only its own rows"
    );
    assert_eq!(collected[0].records[0]["name"], "alice");
    assert_eq!(
        collected[1].records.len(),
        1,
        "page 2 holds only its own rows"
    );
    assert_eq!(collected[1].records[0]["name"], "bob");
    // Async-job sources carry no incremental bookmark.
    assert!(collected.iter().all(|p| p.bookmark.is_none()));
}

/// The buffering convenience path (`fetch_all`) is unchanged: it still drains the
/// streamed locator pages into one concatenated `Vec`.
#[tokio::test]
async fn async_job_bulk_fetch_all_still_concatenates() {
    let server = MockServer::start().await;
    mount_bulk_two_pages(&server).await;

    let records = RestStream::new(bulk_csv_config(&server))
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["name"], "alice");
    assert_eq!(records[1]["name"], "bob");
}

/// Zero-row job still yields exactly one (empty) page, preserving prior behavior.
#[tokio::test]
async fn async_job_empty_result_yields_one_empty_page() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "job-1" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "Complete" })))
        .mount(&server)
        .await;
    // Header row only → zero data rows, no locator → terminal after one fetch.
    Mock::given(method("GET"))
        .and(path("/jobs/job-1/result"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,name\n"))
        .mount(&server)
        .await;

    let stream = RestStream::new(bulk_csv_config(&server)).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);
    let mut collected = Vec::new();
    while let Some(page) = pages.next().await {
        collected.push(page.unwrap());
    }
    assert_eq!(collected.len(), 1, "one empty page, as before");
    assert!(collected[0].records.is_empty());
}

/// Backoff: `interval_secs` is a CAP, not a fixed wait. With one pending poll
/// and a 60s cap, exponential backoff starts at ~1s, so the run finishes in a
/// couple of seconds — the old fixed-interval loop would have waited ~60s.
#[tokio::test]
async fn async_job_poll_backs_off_far_below_interval_cap() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "job-1" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1"))
        .respond_with(PollThenComplete(Arc::new(AtomicUsize::new(0))))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1/result"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,name\n1,alice\n"))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs", "json": { "query": "SELECT 1" } },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 60, "timeout_secs": 300 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": { "method": "GET", "url": "/jobs/${job_id}/result" }
    }))
    .unwrap();

    let mut cfg = RestStreamConfig::new(&server.uri(), "").decode(vec![DecodeStep::Parse {
        parse: ParseSpec {
            format: ParseFormat::Csv,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    }]);
    cfg.async_job = Some(async_job);

    let started = std::time::Instant::now();
    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(records.len(), 1);
    assert!(
        elapsed < std::time::Duration::from_secs(15),
        "poll backoff should finish far under the 60s interval cap, took {elapsed:?}"
    );
}

// ── incremental replication over the wire (#630) ─────────────────────────────

/// Build a bulk-style job whose submit body carries a `query` (the amendable
/// surface the incremental predicate is injected into).
fn incremental_job() -> AsyncJobConfig {
    serde_json::from_value(json!({
        "submit": {
            "method": "POST",
            "url": "/jobs",
            "json": { "operation": "query", "query": "SELECT Id FROM Lead" }
        },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": { "method": "GET", "url": "/jobs/${job_id}/result" }
    }))
    .unwrap()
}

fn incremental_csv_config(server: &MockServer) -> RestStreamConfig {
    let mut cfg = RestStreamConfig::new(&server.uri(), "").decode(vec![DecodeStep::Parse {
        parse: ParseSpec {
            format: ParseFormat::Csv,
            records_path: None,
            delimiter: None,
            has_headers: true,
            sheet: None,
            header_row: 0,
        },
    }]);
    cfg.async_job = Some(incremental_job());
    cfg.replication_method = faucet_core::ReplicationMethod::Incremental;
    cfg.replication_key = Some("SystemModstamp".into());
    cfg
}

/// The resumed bookmark must reach the server as a `WHERE` predicate in the
/// submit body — the whole point of #630. The submit mock matches only when
/// the predicate is present, so a silent full-export submit fails this test.
#[tokio::test]
async fn async_job_incremental_injects_predicate_into_submit() {
    use wiremock::matchers::body_string_contains;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .and(body_string_contains(
            "SELECT Id FROM Lead WHERE SystemModstamp > 2026-01-01T00:00:00Z",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "id": "job-1" })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "state": "Complete" })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1/result"))
        .respond_with(ResponseTemplate::new(200).set_body_string("id,name\n1,alice\n"))
        .mount(&server)
        .await;

    let stream = RestStream::new(incremental_csv_config(&server)).unwrap();
    stream
        .apply_start_bookmark(json!("2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);
    let mut records = 0usize;
    while let Some(page) = pages.next().await {
        records += page.unwrap().records.len();
    }
    assert_eq!(records, 1);
    server.verify().await;
}

/// The final page of an incremental async-job run must carry the new bookmark:
/// run start minus the lookback margin (default 5m), so a client clock ahead of
/// the server can never permanently skip records stamped in the skew gap.
#[tokio::test]
async fn async_job_incremental_final_page_bookmarks_start_minus_lookback() {
    let server = MockServer::start().await;
    mount_bulk_two_pages(&server).await;
    let mut cfg = incremental_csv_config(&server);
    // Reuse the locator-paged fixture's job shape but keep the amendable query.
    cfg.async_job = Some({
        let mut job = bulk_job();
        job.submit.json = Some(json!({ "operation": "query", "query": "SELECT Id FROM Lead" }));
        job
    });
    let stream = RestStream::new(cfg)
        .unwrap()
        .with_now_override_rfc3339("2026-06-01T12:00:00Z");
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);
    let mut collected = Vec::new();
    while let Some(page) = pages.next().await {
        collected.push(page.unwrap());
    }
    let last = collected.last().expect("at least one page");
    assert_eq!(
        last.bookmark,
        Some(json!("2026-06-01T11:55:00Z")),
        "bookmark = run start minus the default 5m lookback"
    );
    // Only the final page carries it.
    assert!(
        collected[..collected.len() - 1]
            .iter()
            .all(|p| p.bookmark.is_none())
    );
}

/// A configurable lookback overrides the default margin.
#[tokio::test]
async fn async_job_incremental_lookback_is_configurable() {
    let server = MockServer::start().await;
    mount_bulk_two_pages(&server).await;
    let mut cfg = incremental_csv_config(&server);
    cfg.async_job = Some({
        let mut job = bulk_job();
        job.submit.json = Some(json!({ "operation": "query", "query": "SELECT Id FROM Lead" }));
        job.lookback = Some("1h".into());
        job
    });
    let stream = RestStream::new(cfg)
        .unwrap()
        .with_now_override_rfc3339("2026-06-01T12:00:00Z");
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);
    let mut last_bookmark = None;
    while let Some(page) = pages.next().await {
        if let Some(bm) = page.unwrap().bookmark {
            last_bookmark = Some(bm);
        }
    }
    assert_eq!(last_bookmark, Some(json!("2026-06-01T11:00:00Z")));
}

/// `replication_method: incremental` + `async_job` without an amendable submit
/// `query` must fail at validate time — otherwise every run silently stays a
/// full export while the bookmark advances (a lying high-water mark).
#[test]
fn incremental_async_job_without_query_is_rejected_at_validate() {
    let server_uri = "https://api.example.com";
    let mut cfg = RestStreamConfig::new(server_uri, "");
    cfg.async_job = Some(bulk_job()); // submit has no json body at all
    cfg.replication_method = faucet_core::ReplicationMethod::Incremental;
    cfg.replication_key = Some("SystemModstamp".into());
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("top-level string `query`"), "{err}");
    // …and the runtime backstop: no bookmark is emitted for that shape either.
    // (Full-table stays valid.)
    cfg.replication_method = faucet_core::ReplicationMethod::FullTable;
    cfg.replication_key = None;
    assert!(cfg.validate().is_ok());
}

/// `replication_bind` and `async_job` are mutually exclusive — the async-job
/// path pushes the bookmark down via the submit query, not a request bind, and
/// a silently-ignored bind would look configured while doing nothing.
#[test]
fn replication_bind_with_async_job_is_rejected_at_validate() {
    let mut cfg = RestStreamConfig::new("https://api.example.com", "");
    let mut job = bulk_job();
    job.submit.json = Some(json!({ "query": "SELECT Id FROM Lead" }));
    cfg.async_job = Some(job);
    cfg.replication_method = faucet_core::ReplicationMethod::Incremental;
    cfg.replication_key = Some("SystemModstamp".into());
    cfg.replication_bind = Some(
        serde_json::from_value(json!({
            "into": "query",
            "name": "modified_since",
            "template": "${bookmark}"
        }))
        .unwrap(),
    );
    let err = cfg.validate().unwrap_err().to_string();
    assert!(err.contains("mutually exclusive"), "{err}");
}
