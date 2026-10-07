//! End-to-end receive loop: signature verification, path validation and the
//! window-close refusal, driven over real HTTP.

use faucet_core::Source;
use faucet_source_webhook::{WebhookSignature, WebhookSource, WebhookSourceConfig};
use hmac::{Mac, digest::KeyInit};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::time::Duration;

fn free_addr() -> String {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().to_string()
}

fn sign(secret: &str, msg: &[u8]) -> String {
    let mut mac = <hmac::Hmac<sha2::Sha256> as KeyInit>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(msg);
    mac.finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

async fn post_until_up(client: &reqwest::Client, url: &str, req: impl Fn() -> reqwest::RequestBuilder) -> reqwest::Response {
    for _ in 0..100 {
        if let Ok(r) = req().send().await {
            return r;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("server at {url} never came up ({client:?})");
}

/// API-50: an HMAC-signed sender is accepted; a missing or wrong signature is 401.
#[tokio::test]
async fn signed_requests_are_verified() {
    let addr = free_addr();
    let cfg = WebhookSourceConfig::new()
        .listen_addr(addr.clone())
        .max_payloads(1)
        .timeout_secs(10)
        .signature(serde_json::from_value(json!({
            "header": "X-Signature",
            "secret": "s3cret",
            "prefix": "sha256=",
            "timestamp_header": "X-Timestamp"
        })).unwrap());
    let source = WebhookSource::new(cfg);
    let run = tokio::spawn(async move { source.fetch_all().await });
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let body = r#"{"id":1}"#;
    let unsigned = post_until_up(&client, &url, || client.post(&url).body(body)).await;
    assert_eq!(unsigned.status(), 401);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let stale = client
        .post(&url)
        .header("X-Timestamp", (now - 3600).to_string())
        .header("X-Signature", format!("sha256={}", sign("s3cret", format!("{}.{body}", now - 3600).as_bytes())))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), 401, "a replayed old signature is refused");
    let good = client
        .post(&url)
        .header("X-Timestamp", now.to_string())
        .header("X-Signature", format!("sha256={}", sign("s3cret", format!("{now}.{body}").as_bytes()).to_uppercase()))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(good.status(), 200);
    let records = run.await.unwrap().unwrap();
    assert_eq!(records, vec![json!({"id": 1})]);
}

/// API-14: a payload arriving after the window closed is refused with a
/// retryable status, never acknowledged and dropped.
#[tokio::test]
async fn payloads_over_the_cap_are_refused_not_acknowledged() {
    let addr = free_addr();
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr(addr.clone())
            .max_payloads(1)
            .timeout_secs(10),
    );
    let run = tokio::spawn(async move { source.fetch_all().await });
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let first = post_until_up(&client, &url, || client.post(&url).json(&json!({"n": 1}))).await;
    assert_eq!(first.status(), 200);
    // The server is now shutting down: a further POST is either refused with
    // 503 or cannot connect — never a 200 for a payload that is dropped.
    if let Ok(resp) = client.post(&url).json(&json!({"n": 2})).send().await {
        assert_ne!(resp.status(), 200);
    }
    let records = run.await.unwrap().unwrap();
    assert_eq!(records, vec![json!({"n": 1})]);
}

/// API-49: an invalid path is a config error, not a router panic, and a
/// substituted parent value is percent-encoded into one segment.
#[tokio::test]
async fn paths_are_validated_and_context_values_encoded() {
    let err = WebhookSource::new(WebhookSourceConfig::new().path("webhook"))
        .fetch_all()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("must start with `/`"), "{err}");
    for bad in ["/a/:id", "/a/*rest", "/a/{x", "/a/x{y}", "/a/{}", "/a b", "/a?b"] {
        let cfg = WebhookSourceConfig::new().path(bad);
        assert!(cfg.validate().is_err(), "{bad}");
    }
    assert!(WebhookSourceConfig::new().path("/a/{x}/b").validate().is_ok());

    let addr = free_addr();
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr(addr.clone())
            .path("/hooks/{tenant}")
            .max_payloads(1)
            .timeout_secs(10),
    );
    let ctx: HashMap<String, Value> = [("tenant".to_string(), json!(":a/b"))].into_iter().collect();
    let run = tokio::spawn(async move { source.fetch_with_context(&ctx).await });
    let url = format!("http://{addr}/hooks/%3Aa%2Fb");
    let client = reqwest::Client::new();
    let resp = post_until_up(&client, &url, || client.post(&url).json(&json!({"ok": true}))).await;
    assert_eq!(resp.status(), 200);
    assert_eq!(run.await.unwrap().unwrap().len(), 1);
}
