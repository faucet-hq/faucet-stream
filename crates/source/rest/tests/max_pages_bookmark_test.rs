//! `max_pages` and bookmarks (#789 API-01).
//!
//! A pass that `max_pages` cuts short has not read the rest of the feed, so it
//! must not persist a bookmark that claims those pages: a record-derived
//! (`running_max`) or window bookmark would skip them for good. Only a
//! persisted cursor names the next unread page and is kept. With no explicit
//! cap a feed is read to its natural end.

use faucet_core::{ReplicationMethod, Source};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig, WindowSpec};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

async fn drain(stream: &RestStream) -> (Vec<Value>, Option<Value>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(stream, &ctx, 1000);
    let mut records = Vec::new();
    let mut bookmark = None;
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        records.extend(page.records);
        if let Some(bm) = page.bookmark {
            bookmark = Some(bm);
        }
    }
    (records, bookmark)
}

fn cursor() -> PaginationStyle {
    PaginationStyle::Cursor {
        next_token_path: "$.next_page_token".into(),
        param_name: "page_token".into(),
    }
}

#[tokio::test]
async fn a_truncated_incremental_pass_persists_no_bookmark() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "updated_at": "2026-01-01T00:00:00Z"},
                {"id": 2, "updated_at": "2026-01-02T00:00:00Z"},
            ],
            "next_page_token": "page-2"
        })))
        .mount(&server)
        .await;

    let config = RestStreamConfig::new(&server.uri(), "/items")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .pagination(cursor())
        .max_pages(1);
    let (records, bookmark) = drain(&RestStream::new(config).unwrap()).await;

    assert_eq!(records.len(), 2, "the fetched page is still delivered");
    assert_eq!(
        bookmark, None,
        "page 2 was never read, so no bookmark may claim it"
    );
}

#[tokio::test]
async fn a_truncated_pass_keeps_a_persisted_cursor() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}],
            "next_page_token": "page-2"
        })))
        .mount(&server)
        .await;

    let config = RestStreamConfig::new(&server.uri(), "/items")
        .records_path("$.data[*]")
        .pagination(cursor())
        .persist_cursor(true)
        .max_pages(1);
    let (records, bookmark) = drain(&RestStream::new(config).unwrap()).await;

    assert_eq!(records.len(), 1);
    assert_eq!(
        bookmark,
        Some(json!("page-2")),
        "a cursor bookmark names the next unread page, so resuming from it skips nothing"
    );
}

#[tokio::test]
async fn a_truncated_window_stops_the_sweep_without_a_bookmark() {
    let server = MockServer::start().await;
    let later_windows = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/report"))
        .and(query_param("start_date", "2024-01-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}],
            "next_page_token": "more"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/report"))
        .respond_with(Counting(Arc::clone(&later_windows)))
        .mount(&server)
        .await;

    let window: WindowSpec = serde_json::from_value(json!({
        "step": "1d",
        "lower": {"into": "query", "name": "start_date", "format": "date"},
        "upper": {"into": "query", "name": "end_date", "format": "date"},
    }))
    .unwrap();
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/report")
            .records_path("$.data[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-01-01"))
            .pagination(cursor())
            .window(window)
            .max_pages(1),
    )
    .unwrap()
    .with_now_override_rfc3339("2024-01-04T00:00:00Z");

    let (records, bookmark) = drain(&stream).await;
    assert_eq!(records.len(), 1);
    assert_eq!(
        bookmark, None,
        "the first window is unfinished, so neither its end nor a later window's may persist"
    );
    assert_eq!(
        later_windows.load(Ordering::SeqCst),
        0,
        "the sweep stops at the truncated window"
    );
}

struct Counting(Arc<AtomicUsize>);

impl Respond for Counting {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        self.0.fetch_add(1, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 9}]}))
    }
}

/// Without an explicit cap the feed is read to its own end, well past the
/// old implicit 100-page limit.
#[tokio::test]
async fn an_uncapped_feed_reads_past_one_hundred_pages() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(Paged(Arc::new(AtomicUsize::new(0)), 150))
        .mount(&server)
        .await;

    let config = RestStreamConfig::new(&server.uri(), "/items")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .pagination(cursor());
    let (records, bookmark) = drain(&RestStream::new(config).unwrap()).await;

    assert_eq!(records.len(), 150);
    assert_eq!(bookmark, Some(json!(150)));
}

struct Paged(Arc<AtomicUsize>, usize);

impl Respond for Paged {
    fn respond(&self, _req: &Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
        let next = if n < self.1 {
            json!(format!("p{}", n + 1))
        } else {
            Value::Null
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": n, "updated_at": n}],
            "next_page_token": next
        }))
    }
}

/// Guard: when pagination ends naturally (no truncation), the bookmark is still
/// emitted on the final page.
#[tokio::test]
async fn natural_pagination_end_still_emits_bookmark() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "updated_at": "2026-01-01T00:00:00Z"},
                {"id": 2, "updated_at": "2026-01-02T00:00:00Z"},
            ],
            "next_page_token": null
        })))
        .mount(&server)
        .await;

    let config = RestStreamConfig::new(&server.uri(), "/items")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .pagination(cursor());
    let (records, bookmark) = drain(&RestStream::new(config).unwrap()).await;

    assert_eq!(records.len(), 2);
    assert_eq!(bookmark, Some(json!("2026-01-02T00:00:00Z")));
}
