//! Nested `replication_key` paths and the no-silent-drop rule (#747).

use faucet_core::observability::{RoundtripRecorder, RoundtripSide};
use faucet_core::{OnMissingKey, ReplicationMethod, Source, Value};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn jira_server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/rest/api/3/search"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "issues": [
                {"key": "A-1", "fields": {"updated": "2024-01-01T00:00:00Z"}},
                {"key": "A-2", "fields": {"updated": "2024-07-01T00:00:00Z"}},
                {"key": "A-3", "fields": {}},
                {"key": "A-4", "fields": {"updated": null}},
                {"key": "A-5", "fields": {"updated": "2024-09-01T00:00:00Z"}}
            ]
        })))
        .mount(&server)
        .await;
    server
}

fn config(uri: &str, key: &str) -> RestStreamConfig {
    let mut c = RestStreamConfig::new(uri, "/rest/api/3/search")
        .records_path("$.issues[*]")
        .pagination(PaginationStyle::None)
        .replication_method(ReplicationMethod::Incremental)
        .replication_key(key);
    c.start_replication_value = Some(json!("2024-06-01T00:00:00Z"));
    c
}

async fn run(stream: &RestStream) -> Result<(Vec<Value>, Option<Value>), faucet_core::FaucetError> {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(stream, &ctx, 1000);
    let mut records = Vec::new();
    let mut bookmark = None;
    while let Some(page) = pages.next().await {
        let page = page?;
        records.extend(page.records);
        if page.bookmark.is_some() {
            bookmark = page.bookmark;
        }
    }
    Ok((records, bookmark))
}

fn keys(records: &[Value]) -> Vec<&str> {
    records.iter().map(|r| r["key"].as_str().unwrap()).collect()
}

#[tokio::test]
async fn nested_key_filters_advances_and_counts_missing() {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    let recorder = DebuggingRecorder::new();
    let snap = recorder.snapshotter();
    metrics::set_global_recorder(recorder).expect("only recorder in this test binary");

    let server = jira_server().await;
    let stream = RestStream::new(config(&server.uri(), "fields.updated")).unwrap();
    stream.set_roundtrip_recorder(Arc::new(RoundtripRecorder::new(
        RoundtripSide::Source,
        "p",
        "jira",
        "rest",
    )));
    let (records, bookmark) = run(&stream).await.unwrap();
    assert_eq!(keys(&records), vec!["A-2", "A-3", "A-4", "A-5"]);
    assert_eq!(bookmark, Some(json!("2024-09-01T00:00:00Z")));

    let missing: u64 = snap
        .snapshot()
        .into_vec()
        .into_iter()
        .filter(|(k, _, _, _)| {
            k.key().name() == "faucet_source_replication_key_missing_total"
                && k.key().labels().any(|l| l.value() == "jira")
        })
        .map(|(_, _, _, v)| match v {
            DebugValue::Counter(c) => c,
            _ => 0,
        })
        .sum();
    assert_eq!(missing, 2, "A-3 (absent) and A-4 (null) are counted");
}

#[tokio::test]
async fn json_pointer_key_resolves_the_same_value() {
    let server = jira_server().await;
    let stream = RestStream::new(config(&server.uri(), "/fields/updated")).unwrap();
    let (records, bookmark) = run(&stream).await.unwrap();
    assert_eq!(records.len(), 4);
    assert_eq!(bookmark, Some(json!("2024-09-01T00:00:00Z")));
}

#[tokio::test]
async fn on_missing_key_drop_and_fail() {
    let server = jira_server().await;
    let mut c = config(&server.uri(), "fields.updated");
    c.on_missing_key = OnMissingKey::Drop;
    let (records, _) = run(&RestStream::new(c).unwrap()).await.unwrap();
    assert_eq!(keys(&records), vec!["A-2", "A-5"]);

    let mut c = config(&server.uri(), "fields.updated");
    c.on_missing_key = OnMissingKey::Fail;
    let err = run(&RestStream::new(c).unwrap()).await.unwrap_err();
    assert!(err.to_string().contains("fields.updated"), "{err}");
}

#[tokio::test]
async fn first_run_without_bookmark_keeps_everything_and_advances() {
    let server = jira_server().await;
    let mut c = config(&server.uri(), "fields.updated");
    c.start_replication_value = None;
    let (records, bookmark) = run(&RestStream::new(c).unwrap()).await.unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(bookmark, Some(json!("2024-09-01T00:00:00Z")));
}

#[tokio::test]
async fn fetch_all_incremental_uses_the_nested_key() {
    let server = jira_server().await;
    let mut c = config(&server.uri(), "fields.updated");
    c.start_replication_value = None;
    let (records, bookmark) = RestStream::new(c)
        .unwrap()
        .fetch_all_incremental()
        .await
        .unwrap();
    assert_eq!(records.len(), 5);
    assert_eq!(bookmark, Some(json!("2024-09-01T00:00:00Z")));
}

#[test]
fn malformed_keys_fail_at_load() {
    assert!(RestStream::new(config("http://x", "fields..updated")).is_err());
    assert!(RestStream::new(config("http://x", "")).is_err());
}
