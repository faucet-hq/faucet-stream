//! Integration tests for `PaginationStyle::CursorInBody` (#500): POST-search
//! pagination where the next-page cursor is read from the response body and
//! injected back into the request JSON body (e.g. a CRM object search).

use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use reqwest::Method;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// Two-page POST-search server: page 1 returns a cursor, page 2 has none.
struct SearchPages(Arc<AtomicUsize>);
impl Respond for SearchPages {
    fn respond(&self, _req: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        if n == 0 {
            ResponseTemplate::new(200).set_body_json(json!({
                "results": [{"id": 1}, {"id": 2}],
                "paging": {"next": {"after": "c1"}}
            }))
        } else {
            ResponseTemplate::new(200).set_body_json(json!({
                "results": [{"id": 3}],
                "paging": {}
            }))
        }
    }
}

#[tokio::test]
async fn cursor_in_body_paginates_and_injects_cursor_into_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/search"))
        .respond_with(SearchPages(Arc::new(AtomicUsize::new(0))))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/search")
            .method(Method::POST)
            .body(json!({"limit": 100}))
            .records_path("$.results[*]")
            .pagination(PaginationStyle::CursorInBody {
                next_token_path: "$.paging.next.after".into(),
                body_cursor_field: "after".into(),
            }),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3, "both pages drained");

    // Inspect the recorded request bodies: page 1 carries no cursor; page 2
    // carries the extracted `after`, alongside the original `limit`.
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2, "exactly two requests (page 1 + page 2)");

    let b0: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(b0["limit"], 100);
    assert!(
        b0.get("after").is_none(),
        "first request must not carry a cursor"
    );

    let b1: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(
        b1["limit"], 100,
        "the base body is preserved on later pages"
    );
    assert_eq!(
        b1["after"], "c1",
        "the extracted cursor is injected into the body"
    );
}

#[tokio::test]
async fn cursor_in_body_starts_from_empty_object_when_no_base_body() {
    // With no configured body, the first request sends nothing and later pages
    // send just the cursor object.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/search"))
        .respond_with(SearchPages(Arc::new(AtomicUsize::new(0))))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/search")
            .method(Method::POST)
            .records_path("$.results[*]")
            .pagination(PaginationStyle::CursorInBody {
                next_token_path: "$.paging.next.after".into(),
                body_cursor_field: "after".into(),
            }),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);

    let requests = server.received_requests().await.unwrap();
    let b1: Value = serde_json::from_slice(&requests[1].body).unwrap();
    assert_eq!(b1["after"], "c1");
}

#[tokio::test]
async fn cursor_in_body_errors_on_non_object_body() {
    // The cursor is injected into a JSON *object*; a non-object base body has
    // nowhere to put it, so a page that needs injection fails loudly rather than
    // silently sending an un-paginated request.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/search"))
        .respond_with(SearchPages(Arc::new(AtomicUsize::new(0))))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/search")
            .method(Method::POST)
            .body(json!([1, 2, 3])) // an array, not an object
            .records_path("$.results[*]")
            .pagination(PaginationStyle::CursorInBody {
                next_token_path: "$.paging.next.after".into(),
                body_cursor_field: "after".into(),
            }),
    )
    .unwrap();

    // Page 1 sends the array fine; page 2 must inject the cursor and errors.
    let err = stream
        .fetch_all()
        .await
        .expect_err("a non-object body must error when a cursor needs injecting");
    assert!(
        err.to_string().contains("JSON object request body"),
        "unexpected error: {err}"
    );
}
