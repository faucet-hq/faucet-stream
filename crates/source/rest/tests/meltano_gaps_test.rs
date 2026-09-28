//! Integration tests for the Meltano-migration REST source features:
//! record_ancestors (#549), records_multi (#548), OffsetInBody (#553),
//! RecordFieldCursor / keyset (#554), resumable cursor (#547), and async_job
//! result-set locator paging (#557).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::Source;
use faucet_source_rest::{
    AsyncJobConfig, PaginationStyle, RecordsMultiSpec, RestStream, RestStreamConfig,
};
use futures::StreamExt;
use reqwest::Method;
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

// ── #549: record_ancestors ──────────────────────────────────────────────────

#[tokio::test]
async fn record_ancestors_lifts_envelope_fields_onto_unwrapped_records() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": "evt_1", "created": 111, "data": {"object": {"sub": "a"}}},
                {"id": "evt_2", "created": 222, "data": {"object": {"sub": "b"}}}
            ]
        })))
        .mount(&server)
        .await;

    let mut ancestors = HashMap::new();
    ancestors.insert("event_id".to_string(), "id".to_string());
    ancestors.insert("event_created".to_string(), "created".to_string());

    let records = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/events")
            .records_path("$.data[*].data.object")
            .record_ancestors(ancestors),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["sub"], "a");
    assert_eq!(records[0]["event_id"], "evt_1");
    assert_eq!(records[0]["event_created"], 111);
    assert_eq!(records[1]["event_id"], "evt_2");
    assert_eq!(records[1]["event_created"], 222);
}

// ── #548: records_multi (op-stamped multi-array fan-out) ─────────────────────

#[tokio::test]
async fn records_multi_stamps_op_and_emits_all_arrays_in_one_pass() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/sync"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "added": [{"k": 1}],
            "modified": [{"k": 2}],
            "removed": [{"k": 3}]
        })))
        .mount(&server)
        .await;

    let records = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/sync")
            .records_multi(vec![
                RecordsMultiSpec {
                    path: "$.added[*]".into(),
                    op: "upsert".into(),
                },
                RecordsMultiSpec {
                    path: "$.modified[*]".into(),
                    op: "upsert".into(),
                },
                RecordsMultiSpec {
                    path: "$.removed[*]".into(),
                    op: "delete".into(),
                },
            ])
            .op_field("_op"),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();

    assert_eq!(records.len(), 3, "all three arrays emitted in one pass");
    let by_k = |k: i64| records.iter().find(|r| r["k"] == json!(k)).unwrap();
    assert_eq!(by_k(1)["_op"], "upsert");
    assert_eq!(by_k(2)["_op"], "upsert");
    assert_eq!(by_k(3)["_op"], "delete");
}

// ── #553: OffsetInBody pagination ────────────────────────────────────────────

struct BodyOffsetPages(Arc<AtomicUsize>);
impl Respond for BodyOffsetPages {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            ResponseTemplate::new(200).set_body_json(json!({"rows": [{"id": 1}, {"id": 2}]}))
        } else {
            ResponseTemplate::new(200).set_body_json(json!({"rows": [{"id": 3}]}))
        }
    }
}

#[tokio::test]
async fn offset_in_body_paginates_and_advances_offset_in_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/query"))
        .respond_with(BodyOffsetPages(Arc::new(AtomicUsize::new(0))))
        .mount(&server)
        .await;

    let records = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/query")
            .method(Method::POST)
            .body(json!({"filter": "x"}))
            .records_path("$.rows[*]")
            .pagination(PaginationStyle::OffsetInBody {
                offset_field: "offset".into(),
                limit_field: "limit".into(),
                limit: 2,
                stop_when_short: true,
                rows_path: None,
                total_path: None,
            }),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();

    assert_eq!(records.len(), 3, "page 1 (2) + page 2 (1, short → stop)");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    let b0: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(b0["offset"], 0, "first request starts at offset 0");
    assert_eq!(b0["limit"], 2);
    assert_eq!(b0["filter"], "x", "base body preserved");
    let b1: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(
        b1["offset"], 2,
        "second request offset advanced by the page size"
    );
}

// ── #770: OffsetInBody rows_path / total_path ────────────────────────────────

/// A report API: every response is one record whose `rows` page by offset.
struct ReportPages;
impl Respond for ReportPages {
    fn respond(&self, req: &wiremock::Request) -> ResponseTemplate {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let offset = body["offset"].as_u64().unwrap();
        let rows: Vec<Value> = (offset..(offset + 2).min(5)).map(|i| json!([i])).collect();
        ResponseTemplate::new(200).set_body_json(json!({
            "dimensionHeaders": [{"name": "date"}],
            "rows": rows,
            "rowCount": 5
        }))
    }
}

async fn report(rows_path: Option<&str>, total_path: Option<&str>) -> (usize, Vec<u64>) {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/report"))
        .respond_with(ReportPages)
        .mount(&server)
        .await;
    let records = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/report")
            .method(Method::POST)
            .body(json!({}))
            .records_path("$")
            .pagination(PaginationStyle::OffsetInBody {
                offset_field: "offset".into(),
                limit_field: "limit".into(),
                limit: 2,
                stop_when_short: true,
                rows_path: rows_path.map(Into::into),
                total_path: total_path.map(Into::into),
            }),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();
    let offsets = server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| {
            serde_json::from_slice::<Value>(&r.body).unwrap()["offset"]
                .as_u64()
                .unwrap()
        })
        .collect();
    (records.len(), offsets)
}

#[tokio::test]
async fn rows_path_pages_a_one_record_report() {
    assert_eq!(report(Some("$.rows"), None).await, (3, vec![0, 2, 4]));
    // Without rows_path the single record reads as a short page: truncated.
    assert_eq!(report(None, None).await, (1, vec![0]));
}

#[tokio::test]
async fn total_path_stops_exactly_at_the_total() {
    // Rows 4..5 fill the total, so no fourth request is made.
    assert_eq!(
        report(Some("$.rows"), Some("$.rowCount")).await,
        (3, vec![0, 2, 4])
    );
}

// ── #554: RecordFieldCursor (keyset) ─────────────────────────────────────────

struct KeysetPages(Arc<AtomicUsize>);
impl Respond for KeysetPages {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            ResponseTemplate::new(200).set_body_json(json!([{"jn": 5}, {"jn": 8}]))
        } else {
            ResponseTemplate::new(200).set_body_json(json!([{"jn": 9}]))
        }
    }
}

#[tokio::test]
async fn record_field_cursor_injects_max_of_field_and_stops_on_short_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/journals"))
        .respond_with(KeysetPages(Arc::new(AtomicUsize::new(0))))
        .mount(&server)
        .await;

    let records = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/journals")
            .records_path("$[*]")
            .pagination(PaginationStyle::RecordFieldCursor {
                field: "jn".into(),
                into: Default::default(),
                param: "offset".into(),
                agg: Default::default(),
                stop_when_short: true,
                page_size: 2,
            }),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();

    assert_eq!(records.len(), 3, "page 1 (2) + page 2 (1, short → stop)");
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    // The second request's offset is max(jn) over page 1 = 8.
    let q1 = requests[1].url.query().unwrap_or_default();
    assert!(
        q1.contains("offset=8"),
        "keyset offset = max of page 1: {q1}"
    );
    let q0 = requests[0].url.query().unwrap_or_default();
    assert!(
        !q0.contains("offset="),
        "first request has no keyset offset: {q0}"
    );
}

// ── #547: resumable cursor (persist + seed) ──────────────────────────────────

struct SyncCursorPages(Arc<AtomicUsize>);
impl Respond for SyncCursorPages {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            ResponseTemplate::new(200).set_body_json(json!({
                "results": [{"id": 1}], "paging": {"next": {"after": "cNEXT"}}
            }))
        } else {
            ResponseTemplate::new(200).set_body_json(json!({"results": [], "paging": {}}))
        }
    }
}

fn sync_cfg(uri: &str) -> RestStreamConfig {
    RestStreamConfig::new(uri, "/sync")
        .method(Method::POST)
        .body(json!({"limit": 100}))
        .records_path("$.results[*]")
        .pagination(PaginationStyle::CursorInBody {
            next_token_path: "$.paging.next.after".into(),
            body_cursor_field: "after".into(),
        })
        .persist_cursor(true)
}

#[tokio::test]
async fn persist_cursor_emits_bookmark_and_seeds_next_run() {
    // Run 1: drain both pages, capturing the terminal cursor bookmark.
    let s1 = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/sync"))
        .respond_with(SyncCursorPages(Arc::new(AtomicUsize::new(0))))
        .mount(&s1)
        .await;

    let src1 = RestStream::new(sync_cfg(&s1.uri())).unwrap();
    let mut bookmark: Option<Value> = None;
    let ctx1: HashMap<String, Value> = HashMap::new();
    let mut pages1 = Source::stream_pages(&src1, &ctx1, 1000);
    while let Some(page) = pages1.next().await {
        if let Some(bm) = page.unwrap().bookmark {
            bookmark = Some(bm);
        }
    }
    let bookmark = bookmark.expect("persist_cursor emits a terminal-cursor bookmark");

    // Run 2: a fresh source resumed from the saved bookmark must seed the
    // cursor into its FIRST request body.
    let s2 = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/sync"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"results": [], "paging": {}})),
        )
        .mount(&s2)
        .await;

    // Drive the streaming path (what the pipeline uses on resume), which seeds
    // the saved cursor into the first request.
    let src2 = RestStream::new(sync_cfg(&s2.uri())).unwrap();
    src2.apply_start_bookmark(bookmark).await.unwrap();
    let ctx2: HashMap<String, Value> = HashMap::new();
    let mut pages = Source::stream_pages(&src2, &ctx2, 1000);
    while let Some(p) = pages.next().await {
        p.unwrap();
    }

    let reqs = s2.received_requests().await.unwrap();
    assert!(!reqs.is_empty(), "resumed run issues a request");
    let b0: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(
        b0["after"], "cNEXT",
        "the saved cursor is seeded into the first request body: {b0}"
    );
}

// ── #557: async_job result-set locator paging ────────────────────────────────

struct LocatorFetch(Arc<AtomicUsize>);
impl Respond for LocatorFetch {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            // First page carries a continuation locator header.
            ResponseTemplate::new(200)
                .insert_header("Sforce-Locator", "loc1")
                .set_body_json(json!({"records": [{"id": 1}, {"id": 2}]}))
        } else {
            // Last page: no (empty) locator → stop.
            ResponseTemplate::new(200)
                .insert_header("Sforce-Locator", "null")
                .set_body_json(json!({"records": [{"id": 3}]}))
        }
    }
}

#[tokio::test]
async fn async_job_fetch_follows_result_locator_across_pages() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/jobs"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "job-1"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"state": "Complete"})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/jobs/job-1/result"))
        .respond_with(LocatorFetch(Arc::new(AtomicUsize::new(0))))
        .mount(&server)
        .await;

    let async_job: AsyncJobConfig = serde_json::from_value(json!({
        "submit": { "method": "POST", "url": "/jobs", "json": { "q": "SELECT 1" } },
        "job_id": "$.id",
        "poll": { "url": "/jobs/${job_id}", "interval_secs": 0, "timeout_secs": 30 },
        "status": { "path": "$.state", "success": ["Complete"], "failure": ["Failed"] },
        "fetch": {
            "method": "GET",
            "url": "/jobs/${job_id}/result",
            "locator_header": "Sforce-Locator",
            "locator_param": "locator",
            "records_path": "$.records[*]"
        }
    }))
    .unwrap();

    let mut cfg = RestStreamConfig::new(&server.uri(), "");
    cfg.async_job = Some(async_job);
    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();

    assert_eq!(
        records.len(),
        3,
        "records appended across both result pages"
    );
    // The second fetch carried the locator from the first page's header.
    let reqs = server.received_requests().await.unwrap();
    let fetch_with_locator = reqs
        .iter()
        .filter(|r| r.url.path() == "/jobs/job-1/result")
        .any(|r| r.url.query().unwrap_or_default().contains("locator=loc1"));
    assert!(
        fetch_with_locator,
        "the continuation fetch sends locator=loc1"
    );
}
