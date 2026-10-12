//! Regression test for #503: an HTTP `204 No Content` response terminates
//! offset/skip pagination cleanly — no error, no extra page.
//!
//! A 204 has already been treated as an empty page since #146 M10; because a
//! zero-record page stops every pagination style, an offset paginator that pages
//! with `$top`/`$skip` and receives a 204 after the last data page (a common
//! convention) stops without erroring on the empty body. This test locks that
//! behavior in as a contract.

use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// One full data page, then a 204 (as a `$top`/`$skip` feed signals "done").
struct RowsThen204(Arc<AtomicUsize>);
impl Respond for RowsThen204 {
    fn respond(&self, _req: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}, {"id": 2}]}))
        } else {
            // 204 No Content: empty body, terminal.
            ResponseTemplate::new(204)
        }
    }
}

#[tokio::test]
async fn offset_pagination_stops_cleanly_on_204() {
    let server = MockServer::start().await;
    let hits = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/hr/workers"))
        .respond_with(RowsThen204(hits.clone()))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/hr/workers")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Offset {
                offset_param: "$skip".into(),
                limit_param: "$top".into(),
                limit: 2,
                total_path: None,
                rows_path: None,
                stop_when_short: false,
            }),
    )
    .unwrap();

    // The full first page (== limit) makes the paginator fetch again; the second
    // request returns 204, which ends pagination without a JSON-parse error.
    let records = stream
        .fetch_all()
        .await
        .expect("a 204 must end pagination cleanly, not error");
    assert_eq!(records.len(), 2, "only the real data page is emitted");
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "one data request + the terminal 204 request"
    );
}

#[tokio::test]
async fn offset_stop_when_short_never_requests_past_a_short_page() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/rows"))
        .and(wiremock::matchers::query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"id": 1}, {"id": 2}, {"id": 3}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/rows"))
        .and(wiremock::matchers::query_param("offset", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "data": [{"id": 4}]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/rows"))
        .and(wiremock::matchers::query_param("offset", "4"))
        .respond_with(ResponseTemplate::new(416))
        .expect(0)
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/rows")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::Offset {
                offset_param: "offset".into(),
                limit_param: "limit".into(),
                limit: 3,
                total_path: None,
                rows_path: None,
                stop_when_short: true,
            }),
    )
    .unwrap();
    let records = stream.fetch_all().await.expect("a short page ends paging");
    assert_eq!(records.len(), 4);
}
