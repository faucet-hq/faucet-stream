//! A stalled endpoint fails the write instead of hanging the run (#789 API-05).

use std::time::Duration;

use faucet_core::Sink;
use faucet_sink_http::{HttpSink, HttpSinkConfig};
use serde_json::json;
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn a_stalled_endpoint_times_out_the_write() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
        .mount(&server)
        .await;
    let sink = HttpSink::new(
        HttpSinkConfig::new(format!("{}/ingest", server.uri()))
            .timeout(Some(Duration::from_millis(100))),
    );

    let outcome = tokio::time::timeout(
        Duration::from_secs(25),
        sink.write_batch(&[json!({"id": 1})]),
    )
    .await
    .expect("the request timeout must end the write, not the test guard");
    assert!(outcome.is_err(), "a timed-out request is an error");
}

#[test]
fn timeouts_default_to_thirty_and_ten_seconds() {
    let mut v = serde_json::to_value(HttpSinkConfig::new("https://api/ingest")).unwrap();
    let obj = v.as_object_mut().unwrap();
    obj.remove("timeout");
    obj.remove("connect_timeout");
    let cfg: HttpSinkConfig = serde_json::from_value(v.clone()).unwrap();
    assert_eq!(cfg.timeout, Some(Duration::from_secs(30)));
    assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(10)));

    let obj = v.as_object_mut().unwrap();
    obj.insert("timeout".into(), serde_json::Value::Null);
    obj.insert("connect_timeout".into(), json!(3));
    let cfg: HttpSinkConfig = serde_json::from_value(v).unwrap();
    assert_eq!(cfg.timeout, None);
    assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(3)));
}

#[test]
fn the_connect_timeout_builder_and_debug_show_both_timeouts() {
    let cfg = HttpSinkConfig::new("https://api/ingest")
        .timeout(None)
        .connect_timeout(Some(Duration::from_secs(4)));
    assert_eq!(cfg.connect_timeout, Some(Duration::from_secs(4)));
    let shown = format!("{cfg:?}");
    assert!(
        shown.contains("connect_timeout") && shown.contains("timeout: None"),
        "{shown}"
    );
}

async fn timed_out_attempts(http_method: reqwest::Method) -> usize {
    let server = MockServer::start().await;
    Mock::given(method(http_method.as_str()))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(5)))
        .mount(&server)
        .await;
    let sink = HttpSink::new(
        HttpSinkConfig::new(format!("{}/ingest", server.uri()))
            .method(http_method)
            .max_retries(2)
            .timeout(Some(Duration::from_millis(100))),
    );
    let outcome = sink.write_batch(&[json!({"id": 1})]).await;
    assert!(outcome.is_err(), "every attempt times out");
    server.received_requests().await.unwrap().len()
}

#[tokio::test]
async fn a_timed_out_post_is_not_resent() {
    assert_eq!(timed_out_attempts(reqwest::Method::POST).await, 1);
}

#[tokio::test]
async fn a_timed_out_put_is_retried() {
    assert_eq!(timed_out_attempts(reqwest::Method::PUT).await, 3);
}

async fn dropped_connection_attempts(
    http_method: reqwest::Method,
) -> (usize, faucet_core::FaucetError) {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let accepted = Arc::new(AtomicUsize::new(0));
    let counter = accepted.clone();
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            let _ = socket.read(&mut buf).await;
            drop(socket);
        }
    });
    let sink = HttpSink::new(
        HttpSinkConfig::new(format!("http://{addr}/ingest"))
            .method(http_method)
            .max_retries(2),
    );
    let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    (accepted.load(Ordering::SeqCst), err)
}

#[tokio::test]
async fn a_post_whose_connection_drops_after_sending_is_not_resent() {
    let (attempts, err) = dropped_connection_attempts(reqwest::Method::POST).await;
    assert_eq!(attempts, 1);
    assert!(
        matches!(&err, faucet_core::FaucetError::Sink(m) if m.contains("may have been delivered")),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_put_whose_connection_drops_is_retried() {
    let (attempts, err) = dropped_connection_attempts(reqwest::Method::PUT).await;
    assert_eq!(attempts, 3);
    assert!(matches!(err, faucet_core::FaucetError::Http(_)), "{err:?}");
}
