//! `window:` with concurrent partitions (#789 API-03).
//!
//! Each partition walks its own windows, so the bounds a request carries must
//! be the bounds of *its* window: a shared slot let concurrently polled
//! partitions send each other's bounds and skip windows while the bookmark
//! advanced past them. A partition that `max_pages` cuts short must also stop
//! the consolidated bookmark from claiming its unread pages.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use chrono::NaiveDate;
use faucet_core::{ReplicationMethod, Source};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig, WindowSpec};
use futures::StreamExt as _;
use serde_json::{Value, json};
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

struct EchoWindow;

impl Respond for EchoWindow {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let q: HashMap<String, String> = req.url.query_pairs().into_owned().collect();
        ResponseTemplate::new(200)
            .set_delay(Duration::from_millis(20))
            .set_body_json(json!({
                "data": [{"path": req.url.path(), "start": q.get("start_date")}]
            }))
    }
}

fn window() -> WindowSpec {
    serde_json::from_value(json!({
        "step": "1d",
        "lower": {"into": "query", "name": "start_date", "format": "date"},
        "upper": {"into": "query", "name": "end_date", "format": "date"},
    }))
    .unwrap()
}

async fn drain(stream: &RestStream) -> (Vec<Value>, Option<Value>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = Source::stream_pages(stream, &ctx, 1000);
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

#[tokio::test]
async fn concurrent_partitions_each_send_their_own_window_bounds() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/p/\d+$"))
        .respond_with(EchoWindow)
        .mount(&server)
        .await;

    let mut cfg = RestStreamConfig::new(&server.uri(), "/p/{n}")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .start_replication_value(json!("2024-01-01"))
        .window(window());
    cfg.partition_concurrency = Some(4);
    cfg.partitions = (0..4)
        .map(|n| HashMap::from([("n".to_string(), json!(n))]))
        .collect();
    let stream = RestStream::new(cfg)
        .unwrap()
        .with_now_override_rfc3339("2024-01-04T00:00:00Z");

    let (records, bookmark) = drain(&stream).await;

    let requests = server.received_requests().await.unwrap();
    let mut seen = BTreeSet::new();
    for req in &requests {
        let q: HashMap<String, String> = req.url.query_pairs().into_owned().collect();
        let start = NaiveDate::parse_from_str(&q["start_date"], "%Y-%m-%d").unwrap();
        let end = NaiveDate::parse_from_str(&q["end_date"], "%Y-%m-%d").unwrap();
        assert_eq!(
            end - start,
            chrono::Duration::days(1),
            "a request carried another window's bound: {}",
            req.url
        );
        assert!(
            seen.insert((req.url.path().to_string(), start)),
            "a (partition, window) pair was requested twice: {}",
            req.url
        );
    }
    assert_eq!(seen.len(), 12, "4 partitions × 3 windows, each once");
    assert_eq!(records.len(), 12);
    let bm = bookmark.expect("every partition finished its windows");
    assert!(
        bm.as_str().unwrap().starts_with("2024-01-04T00:00:00"),
        "{bm}"
    );
}

struct PagedFirst;

impl Respond for PagedFirst {
    fn respond(&self, req: &Request) -> ResponseTemplate {
        let next = if req.url.path() == "/p/0" {
            json!("more")
        } else {
            Value::Null
        };
        ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"updated_at": "2026-01-02T00:00:00Z"}],
            "next": next
        }))
    }
}

#[tokio::test]
async fn a_partition_cut_short_by_max_pages_withholds_the_consolidated_bookmark() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/p/\d+$"))
        .respond_with(PagedFirst)
        .mount(&server)
        .await;

    let mut cfg = RestStreamConfig::new(&server.uri(), "/p/{n}")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .pagination(PaginationStyle::Cursor {
            next_token_path: "$.next".into(),
            param_name: "cursor".into(),
        })
        .max_pages(1);
    cfg.partitions = (0..2)
        .map(|n| HashMap::from([("n".to_string(), json!(n))]))
        .collect();
    let stream = RestStream::new(cfg).unwrap();

    let (records, bookmark) = drain(&stream).await;
    assert_eq!(records.len(), 2);
    assert_eq!(
        bookmark, None,
        "partition 0 has unread pages, so the other partition's high-water mark must not persist"
    );
}
