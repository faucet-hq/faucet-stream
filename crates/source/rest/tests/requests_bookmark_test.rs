//! An incremental REST source with several `requests:` entries keeps one
//! bookmark per entry, so an entry with its own sequence never resumes from
//! another entry's position. Stored state is versioned (schema 1); a bookmark
//! from an earlier release is the start every entry resumes from.

use faucet_core::state_version::resolve_for_source;
use faucet_core::{ReplicationMethod, Source};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const EU: &str = "e77abed24194121d";
const US: &str = "96366955c414205b";

fn since_bind() -> faucet_core::ReplicationBind {
    serde_json::from_value(json!({"into": "query", "name": "since"})).unwrap()
}

/// `/items/eu` serves ids up to 100, `/items/us` up to 50; with a `since`
/// bound each entry serves only what lies above it.
async fn two_entry_feed() -> MockServer {
    let server = MockServer::start().await;
    for (region, max) in [("eu", 100), ("us", 50)] {
        for (since, rows) in [
            (Some("100"), json!([])),
            (Some("50"), json!([])),
            (Some("75"), json!([])),
            (None, json!([{"id": max - 1}, {"id": max}])),
        ] {
            let mut mock = Mock::given(method("GET")).and(path(format!("/items/{region}")));
            mock = match since {
                Some(s) => mock.and(query_param("since", s)),
                None => mock,
            };
            mock.respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": rows})))
                .with_priority(if since.is_some() { 1 } else { 5 })
                .mount(&server)
                .await;
        }
    }
    server
}

fn two_entries(server: &MockServer) -> RestStreamConfig {
    RestStreamConfig::new(&server.uri(), "/items/{region}")
        .records_path("$.data[*]")
        .pagination(PaginationStyle::None)
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("id")
        .replication_bind(since_bind())
        .add_partition(HashMap::from([("region".to_string(), json!("eu"))]))
        .add_partition(HashMap::from([("region".to_string(), json!("us"))]))
}

async fn drain(stream: &RestStream) -> (usize, Option<Value>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = Source::stream_pages(stream, &ctx, 1000);
    let (mut records, mut bookmark) = (0, None);
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        records += page.records.len();
        if page.bookmark.is_some() {
            assert!(bookmark.is_none(), "one bookmark, after every entry");
            bookmark = page.bookmark;
        }
    }
    (records, bookmark)
}

async fn since_of(server: &MockServer, region: &str) -> Vec<String> {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.url.path() == format!("/items/{region}"))
        .map(|r| {
            r.url
                .query_pairs()
                .find(|(k, _)| k == "since")
                .map(|(_, v)| v.into_owned())
                .unwrap_or_default()
        })
        .collect()
}

#[tokio::test]
async fn each_entry_keeps_and_resumes_from_its_own_bookmark() {
    let server = two_entry_feed().await;
    let (records, bookmark) = drain(&RestStream::new(two_entries(&server)).unwrap()).await;
    assert_eq!(records, 4);
    assert_eq!(bookmark, Some(json!({"requests": {EU: 100, US: 50}})));

    let next = RestStream::new(two_entries(&server)).unwrap();
    next.apply_start_bookmark(bookmark.unwrap()).await.unwrap();
    let (records, _) = drain(&next).await;
    assert_eq!(records, 0);
    assert_eq!(since_of(&server, "eu").await.last().unwrap(), "100");
    assert_eq!(since_of(&server, "us").await.last().unwrap(), "50");
}

#[tokio::test]
async fn a_bookmark_from_an_earlier_release_is_every_entry_s_start() {
    let server = two_entry_feed().await;
    let stored: Value =
        serde_json::from_str(include_str!("fixtures/requests-bookmark-legacy-v0.json")).unwrap();
    let source = RestStream::new(two_entries(&server)).unwrap();
    assert_eq!(source.state_schema(), 1);
    let resolved = resolve_for_source("k", &stored, &source).unwrap();
    assert_eq!(resolved.migrated_from, Some(0));
    source.apply_start_bookmark(resolved.data).await.unwrap();
    let (_, bookmark) = drain(&source).await;
    assert_eq!(since_of(&server, "eu").await, vec!["75"]);
    assert_eq!(since_of(&server, "us").await, vec!["75"]);
    assert_eq!(bookmark, Some(json!({"requests": {EU: 75, US: 75}})));

    let bare = RestStream::new(two_entries(&server)).unwrap();
    let resolved = resolve_for_source("k", &json!(75), &bare).unwrap();
    assert_eq!(resolved.data, json!(75));
}

#[tokio::test]
async fn the_frozen_per_entry_bookmark_resumes() {
    let server = two_entry_feed().await;
    let stored: Value =
        serde_json::from_str(include_str!("fixtures/requests-bookmark-v1.json")).unwrap();
    let source = RestStream::new(two_entries(&server)).unwrap();
    let resolved = resolve_for_source("k", &stored, &source).unwrap();
    assert_eq!(resolved.migrated_from, None);
    source.apply_start_bookmark(resolved.data).await.unwrap();
    drain(&source).await;
    assert_eq!(since_of(&server, "eu").await, vec!["100"]);
    assert_eq!(since_of(&server, "us").await, vec!["50"]);
}

#[tokio::test]
async fn an_entry_removed_from_the_config_drops_out_of_the_bookmark() {
    let server = two_entry_feed().await;
    let source = RestStream::new(two_entries(&server)).unwrap();
    source
        .apply_start_bookmark(json!({"requests": {EU: 100, US: 50, "0000000000000000": 7}}))
        .await
        .unwrap();
    let (_, bookmark) = drain(&source).await;
    assert_eq!(bookmark, Some(json!({"requests": {EU: 100, US: 50}})));
}

#[tokio::test]
async fn a_single_entry_resumes_from_its_position_and_stores_a_plain_bookmark() {
    let server = two_entry_feed().await;
    let one = RestStreamConfig::new(&server.uri(), "/items/{region}")
        .records_path("$.data[*]")
        .pagination(PaginationStyle::None)
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("id")
        .replication_bind(since_bind())
        .add_partition(HashMap::from([("region".to_string(), json!("us"))]));
    let source = RestStream::new(one).unwrap();
    source
        .apply_start_bookmark(json!({"requests": {EU: 100, US: 50}}))
        .await
        .unwrap();
    let (_, bookmark) = drain(&source).await;
    assert_eq!(since_of(&server, "us").await, vec!["50"]);
    assert_eq!(bookmark, Some(json!(50)));
}

#[tokio::test]
async fn the_buffered_read_keeps_a_bookmark_per_entry() {
    let server = two_entry_feed().await;
    let source = RestStream::new(two_entries(&server)).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let (records, bookmark) = source.fetch_with_context_incremental(&ctx).await.unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(bookmark, Some(json!({"requests": {EU: 100, US: 50}})));
}
