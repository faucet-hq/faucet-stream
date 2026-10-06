//! Accumulated commit groups (#789 SQL-04, SQL-21, SQL-22): a failed group
//! keeps its earlier pages' rows for the retry, and the DLQ path commits and
//! reports per page.

use faucet_core::Sink;
use faucet_sink_snowflake::{SnowflakeAuth, SnowflakeSink, SnowflakeSinkConfig};
use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config() -> SnowflakeSinkConfig {
    let mut c = SnowflakeSinkConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        "events",
        SnowflakeAuth::OAuth {
            token: "tok".into(),
        },
    )
    .with_create_table(false)
    .with_batch_size(1);
    c.commit_rows = Some(2);
    c
}

async fn mount(server: &MockServer, failing: &str, times: Option<u64>) {
    let fail = Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains(failing))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad row"))
        .with_priority(1);
    match times {
        Some(n) => fail.up_to_n_times(n).mount(server).await,
        None => fail.mount(server).await,
    }
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001", "message": "ok"
        })))
        .mount(server)
        .await;
}

async fn bodies_with(server: &MockServer, needle: &str) -> usize {
    server
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| String::from_utf8_lossy(&r.body).contains(needle))
        .count()
}

/// SQL-04: an earlier page whose group commit fails is restored, so the next
/// `flush` (a resilience retry) commits it instead of finding nothing.
#[tokio::test]
async fn a_failed_group_keeps_earlier_pages_for_the_retry() {
    let server = MockServer::start().await;
    mount(&server, "flaky-row", Some(1)).await;
    let sink = SnowflakeSink::new(config())
        .unwrap()
        .with_endpoint(format!("{}/api/v2/statements", server.uri()));

    sink.write_batch(&[json!({"id": "flaky-row"})])
        .await
        .expect("buffered");
    sink.write_batch(&[json!({"id": "current-row"})])
        .await
        .expect_err("the group commit fails on its first chunk");
    sink.flush()
        .await
        .expect("the retry commits the restored row");

    assert_eq!(bodies_with(&server, "flaky-row").await, 2);
    assert_eq!(
        bodies_with(&server, "current-row").await,
        0,
        "the failing call's own page stays the caller's to retry or route"
    );
}

/// SQL-21: the DLQ path flushes buffered pages, commits this page alone and
/// names exactly which of its rows did not land.
#[tokio::test]
async fn the_dlq_path_commits_per_page_and_reports_each_row() {
    let server = MockServer::start().await;
    mount(&server, "poison-row", None).await;
    let sink = SnowflakeSink::new(config())
        .unwrap()
        .with_endpoint(format!("{}/api/v2/statements", server.uri()));

    sink.write_batch(&[json!({"id": "earlier-row"})])
        .await
        .expect("buffered");
    let outcomes = sink
        .write_batch_partial(&[json!({"id": "good-row"}), json!({"id": "poison-row"})])
        .await
        .expect("per-row outcomes");
    assert!(outcomes[0].is_ok());
    assert!(
        outcomes[1]
            .as_ref()
            .is_err_and(|e| e.to_string().contains("snowflake"))
    );
    assert_eq!(
        bodies_with(&server, "earlier-row").await,
        1,
        "flushed first"
    );

    let err = sink
        .write_batch_partial(&[json!({"id": "poison-row"})])
        .await
        .expect_err("nothing of the page landed");
    assert!(err.to_string().contains("400"), "{err}");
    assert!(sink.write_batch_partial(&[]).await.unwrap().is_empty());
}
