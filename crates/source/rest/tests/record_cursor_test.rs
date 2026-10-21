//! `RecordFieldCursor` keyset paging (#789 API-04, API-09).
//!
//! The cursor must come from every record a page returned (not the ones the
//! incremental filter kept), compare string IDs by value, and fail rather than
//! stop silently when a full page leaves it where it was.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::ReplicationMethod;
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

struct Pages(Arc<AtomicUsize>, Vec<Value>);

impl Respond for Pages {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        let body = self.1.get(n).cloned().unwrap_or_else(|| json!([]));
        ResponseTemplate::new(200).set_body_json(body)
    }
}

fn keyset(page_size: usize) -> PaginationStyle {
    PaginationStyle::RecordFieldCursor {
        field: "id".into(),
        into: Default::default(),
        param: "after".into(),
        agg: Default::default(),
        stop_when_short: true,
        page_size,
        inclusive: false,
    }
}

async fn serve(pages: Vec<Value>) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/rows"))
        .respond_with(Pages(Arc::new(AtomicUsize::new(0)), pages))
        .mount(&server)
        .await;
    server
}

async fn after_params(server: &MockServer) -> Vec<Option<String>> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .map(|r| {
            r.url
                .query_pairs()
                .find(|(k, _)| k == "after")
                .map(|(_, v)| v.into_owned())
        })
        .collect()
}

#[tokio::test]
async fn a_page_of_only_old_rows_still_advances_the_cursor() {
    let server = serve(vec![
        json!([
            {"id": 1, "updated_at": "2026-01-01T00:00:00Z"},
            {"id": 2, "updated_at": "2026-01-01T00:00:00Z"}
        ]),
        json!([
            {"id": 3, "updated_at": "2026-01-03T00:00:00Z"},
            {"id": 4, "updated_at": "2026-01-03T00:00:00Z"}
        ]),
        json!([{"id": 5, "updated_at": "2026-01-04T00:00:00Z"}]),
    ])
    .await;

    let records = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/rows")
            .records_path("$[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2026-01-02T00:00:00Z"))
            .pagination(keyset(2)),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();

    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(
        ids,
        vec![3, 4, 5],
        "the newer rows behind the old page are read"
    );
    assert_eq!(
        after_params(&server).await,
        vec![None, Some("2".into()), Some("4".into())]
    );
}

#[tokio::test]
async fn string_ids_are_compared_by_value_across_a_digit_boundary() {
    let server = serve(vec![
        json!([{"id": "8"}, {"id": "9"}]),
        json!([{"id": "10"}, {"id": "11"}]),
        json!([{"id": "12"}]),
    ])
    .await;

    let records = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/rows")
            .records_path("$[*]")
            .pagination(keyset(2)),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();

    assert_eq!(records.len(), 5);
    assert_eq!(
        after_params(&server).await,
        vec![None, Some("9".into()), Some("11".into())],
        "\"11\" follows \"9\"; a text comparison would keep \"9\" and stop"
    );
}

#[tokio::test]
async fn a_full_page_that_leaves_the_cursor_in_place_fails_the_run() {
    let server = serve(vec![
        json!([{"id": 7}, {"id": 7}]),
        json!([{"id": 7}, {"id": 7}]),
    ])
    .await;

    let err = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/rows")
            .records_path("$[*]")
            .pagination(keyset(2)),
    )
    .unwrap()
    .fetch_all()
    .await
    .expect_err("a stalled cursor on a full page must not end the run green");
    assert!(err.to_string().contains("did not advance"), "{err}");
}

#[tokio::test]
async fn an_inclusive_keyset_on_a_non_unique_field_reads_every_row_once() {
    // The server returns rows `>= after`: page 2 repeats the two rows tied at
    // t=2 from the end of page 1, page 3 repeats the row at t=3.
    let server = serve(vec![
        json!([{"id": 1, "t": 1}, {"id": 2, "t": 2}, {"id": 3, "t": 2}]),
        json!([{"id": 2, "t": 2}, {"id": 3, "t": 2}, {"id": 4, "t": 3}]),
        json!([{"id": 4, "t": 3}, {"id": 5, "t": 3}]),
    ])
    .await;

    let records = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/rows")
            .records_path("$[*]")
            .pagination(PaginationStyle::RecordFieldCursor {
                field: "t".into(),
                into: Default::default(),
                param: "after".into(),
                agg: Default::default(),
                stop_when_short: true,
                page_size: 3,
                inclusive: true,
            }),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();

    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    assert_eq!(
        after_params(&server).await,
        vec![None, Some("2".into()), Some("3".into())]
    );
}

#[tokio::test]
async fn inclusive_keeps_a_new_row_that_shares_the_boundary_value() {
    // Page 2 returns the boundary row again plus a row with the same value
    // that was not on page 1: only the repeat is dropped.
    let server = serve(vec![
        json!([{"id": 1, "t": 1}, {"id": 2, "t": 2}]),
        json!([{"id": 2, "t": 2}, {"id": 9, "t": 2}, {"id": 3, "t": 4}]),
        json!([{"id": 3, "t": 4}]),
    ])
    .await;

    let records = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/rows")
            .records_path("$[*]")
            .pagination(PaginationStyle::RecordFieldCursor {
                field: "t".into(),
                into: Default::default(),
                param: "after".into(),
                agg: Default::default(),
                stop_when_short: false,
                page_size: 2,
                inclusive: true,
            }),
    )
    .unwrap()
    .fetch_all()
    .await
    .unwrap();

    let ids: Vec<i64> = records.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![1, 2, 9, 3]);
}
