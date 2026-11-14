//! End-to-end receive loop: signature verification, path validation and the
//! window-close refusal, driven over real HTTP.
#![allow(clippy::disallowed_methods)] // tests drive time and raw clients directly

use faucet_core::Source;
use faucet_source_webhook::{AckMode, WebhookSource, WebhookSourceConfig};
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

async fn post_until_up(
    client: &reqwest::Client,
    url: &str,
    req: impl Fn() -> reqwest::RequestBuilder,
) -> reqwest::Response {
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
        .signature(
            serde_json::from_value(json!({
                "header": "X-Signature",
                "secret": "s3cret",
                "prefix": "sha256=",
                "timestamp_header": "X-Timestamp"
            }))
            .unwrap(),
        );
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
        .header(
            "X-Signature",
            format!(
                "sha256={}",
                sign("s3cret", format!("{}.{body}", now - 3600).as_bytes())
            ),
        )
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), 401, "a replayed old signature is refused");
    let good = client
        .post(&url)
        .header("X-Timestamp", now.to_string())
        .header(
            "X-Signature",
            format!(
                "sha256={}",
                sign("s3cret", format!("{now}.{body}").as_bytes()).to_uppercase()
            ),
        )
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(good.status(), 200);
    let records = run.await.unwrap().unwrap();
    assert_eq!(records, vec![json!({"id": 1})]);
}

/// A signed request repeated inside the tolerance window is acknowledged but
/// stored once.
#[tokio::test]
async fn a_repeated_signed_request_is_stored_once() {
    let addr = free_addr();
    let cfg = WebhookSourceConfig::new()
        .listen_addr(addr.clone())
        .max_payloads(2)
        .timeout_secs(3)
        .signature(
            serde_json::from_value(json!({
                "header": "X-Signature",
                "secret": "s3cret",
                "timestamp_header": "X-Timestamp"
            }))
            .unwrap(),
        );
    let source = WebhookSource::new(cfg);
    let run = tokio::spawn(async move { source.fetch_all().await });
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let body = r#"{"id":7}"#;
    let signature = sign("s3cret", format!("{now}.{body}").as_bytes());
    let send = || {
        client
            .post(&url)
            .header("X-Timestamp", now.to_string())
            .header("X-Signature", signature.clone())
            .body(body)
    };
    assert_eq!(post_until_up(&client, &url, send).await.status(), 200);
    assert_eq!(send().send().await.unwrap().status(), 200);
    let records = run.await.unwrap().unwrap();
    assert_eq!(records, vec![json!({"id": 7})]);
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

/// The window's buffered bytes are bounded: a POST that would cross
/// `max_buffered_bytes` closes the window and is refused with a retryable
/// status, and the payloads already stored are emitted.
#[tokio::test]
async fn buffered_bytes_are_bounded_per_window() {
    let addr = free_addr();
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr(addr.clone())
            .max_buffered_bytes(30)
            .timeout_secs(30),
    );
    let started = std::time::Instant::now();
    let run = tokio::spawn(async move { source.fetch_all().await });
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let body = r#"{"n":1,"pad":"xxxxxxxx"}"#;
    let first = post_until_up(&client, &url, || client.post(&url).body(body)).await;
    assert_eq!(first.status(), 200);
    if let Ok(resp) = client.post(&url).body(body).send().await {
        assert_eq!(resp.status(), 503);
        assert!(resp.headers().contains_key("retry-after"));
    }
    let records = run.await.unwrap().unwrap();
    assert_eq!(records, vec![json!({"n": 1, "pad": "xxxxxxxx"})]);
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "the bound ends the window"
    );
}

/// A body larger than `max_buffered_bytes` (but within `max_body_bytes`) is
/// still taken into an empty window, which it fills and closes, rather than
/// being refused in every window.
#[tokio::test]
async fn a_body_over_the_byte_cap_is_taken_into_an_empty_window() {
    let addr = free_addr();
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr(addr.clone())
            .max_buffered_bytes(8)
            .timeout_secs(30),
    );
    let started = std::time::Instant::now();
    let run = tokio::spawn(async move { source.fetch_all().await });
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let body = r#"{"n":1,"pad":"xxxxxxxx"}"#;
    let first = post_until_up(&client, &url, || client.post(&url).body(body)).await;
    assert_eq!(first.status(), 200);
    let records = run.await.unwrap().unwrap();
    assert_eq!(records, vec![json!({"n": 1, "pad": "xxxxxxxx"})]);
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "a full window closes without waiting for the timeout"
    );
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
    for bad in [
        "/a/:id", "/a/*rest", "/a/{x", "/a/x{y}", "/a/{}", "/a b", "/a?b",
    ] {
        let cfg = WebhookSourceConfig::new().path(bad);
        assert!(cfg.validate().is_err(), "{bad}");
    }
    assert!(
        WebhookSourceConfig::new()
            .path("/a/{x}/b")
            .validate()
            .is_ok()
    );

    let addr = free_addr();
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr(addr.clone())
            .path("/hooks/{tenant}")
            .max_payloads(1)
            .timeout_secs(10),
    );
    let ctx: HashMap<String, Value> = [("tenant".to_string(), json!(":a/b"))]
        .into_iter()
        .collect();
    let run = tokio::spawn(async move { source.fetch_with_context(&ctx).await });
    let url = format!("http://{addr}/hooks/%3Aa%2Fb");
    let client = reqwest::Client::new();
    let resp = post_until_up(&client, &url, || {
        client.post(&url).json(&json!({"ok": true}))
    })
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(run.await.unwrap().unwrap().len(), 1);
}

/// A wrong bearer token is 401, a non-JSON text body is kept as a string, a
/// body that is neither JSON nor UTF-8 is 400, and a non-string context value
/// is rendered into the path as its JSON text.
#[tokio::test]
async fn bearer_tokens_text_bodies_and_numeric_path_values() {
    let addr = free_addr();
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr(addr.clone())
            .path("/hooks/{shard}")
            .auth_token("s3cret")
            .max_payloads(1)
            .timeout_secs(10),
    );
    let ctx: HashMap<String, Value> = [("shard".to_string(), json!(7))].into_iter().collect();
    let run = tokio::spawn(async move { source.fetch_with_context(&ctx).await });
    let url = format!("http://{addr}/hooks/7");
    let client = reqwest::Client::new();
    let wrong = post_until_up(&client, &url, || {
        client.post(&url).bearer_auth("nope").body("{}")
    })
    .await;
    assert_eq!(wrong.status(), 401);
    let binary = client
        .post(&url)
        .bearer_auth("s3cret")
        .body(vec![0xff, 0xfe, 0x00])
        .send()
        .await
        .unwrap();
    assert_eq!(binary.status(), 400);
    let text = client
        .post(&url)
        .bearer_auth("s3cret")
        .body("plain text")
        .send()
        .await
        .unwrap();
    assert_eq!(text.status(), 200);
    assert_eq!(run.await.unwrap().unwrap(), vec![json!("plain text")]);
}

/// A sender that stalls mid-body cannot hold the run open: once the window
/// closes the server gets a bounded grace period, then is stopped.
#[tokio::test]
async fn a_stalled_sender_cannot_hold_the_run_open() {
    use tokio::io::AsyncWriteExt;
    let addr = free_addr();
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr(addr.clone())
            .max_payloads(1)
            .timeout_secs(60),
    );
    let run = tokio::spawn(async move { source.fetch_all().await });
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let mut stalled = loop {
        if let Ok(s) = tokio::net::TcpStream::connect(&addr).await {
            break s;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    stalled
        .write_all(
            b"POST /webhook HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"n\":",
        )
        .await
        .unwrap();
    let resp = post_until_up(&client, &url, || client.post(&url).json(&json!({"n": 1}))).await;
    assert_eq!(resp.status(), 200);
    let started = std::time::Instant::now();
    let records = tokio::time::timeout(Duration::from_secs(30), run)
        .await
        .expect("the run ends despite the stalled sender")
        .unwrap()
        .unwrap();
    assert_eq!(records, vec![json!({"n": 1})]);
    assert!(started.elapsed() < Duration::from_secs(30));
    drop(stalled);
}

/// #862 L-130: a `{key}` inside a segment is filled from the context, and one
/// the context does not fill is refused instead of becoming a route capture.
#[tokio::test]
async fn partial_segment_placeholders_resolve_and_unfilled_ones_fail() {
    let missing = WebhookSource::new(WebhookSourceConfig::new().path("/hooks/{id}"));
    let ctx: HashMap<String, Value> = [("other".to_string(), json!(1))].into_iter().collect();
    let err = missing.fetch_with_context(&ctx).await.unwrap_err();
    assert!(err.to_string().contains("did not fill"), "{err}");

    let addr = free_addr();
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr(addr.clone())
            .path("/hooks/user-{id}")
            .max_payloads(1)
            .timeout_secs(10),
    );
    let ctx: HashMap<String, Value> = [("id".to_string(), json!(7))].into_iter().collect();
    let run = tokio::spawn(async move { source.fetch_with_context(&ctx).await });
    let url = format!("http://{addr}/hooks/user-7");
    let client = reqwest::Client::new();
    let resp = post_until_up(&client, &url, || {
        client.post(&url).json(&json!({"ok": true}))
    })
    .await;
    assert_eq!(resp.status(), 200);
    assert_eq!(run.await.unwrap().unwrap().len(), 1);
}

/// RFC 0014: with `ack: on_receive` the page stream serves what the window
/// collected, `batch_size` records per page, with no bookmark.
#[tokio::test]
async fn the_page_stream_chunks_the_collected_payloads() {
    let addr = free_addr();
    let cfg = WebhookSourceConfig::new()
        .listen_addr(addr.clone())
        .ack(AckMode::OnReceive)
        .with_batch_size(2)
        .max_payloads(3)
        .timeout_secs(10);
    let source = WebhookSource::new(cfg);
    let run = tokio::spawn(async move {
        let ctx = HashMap::new();
        let mut stream = source.stream_pages(&ctx, 2);
        let mut pages = Vec::new();
        while let Some(page) = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
            let page = page?;
            pages.push((page.records.len(), page.bookmark));
        }
        Ok::<_, faucet_core::FaucetError>(pages)
    });
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    for i in 0..3 {
        let body = format!(r#"{{"id":{i}}}"#);
        let r = post_until_up(&client, &url, || client.post(&url).body(body.clone())).await;
        assert!(r.status().is_success());
    }
    let pages = run.await.unwrap().unwrap();
    assert_eq!(pages, vec![(2, None), (1, None)]);
}

/// Poll the next page of a source's stream.
async fn next_page(
    stream: &mut faucet_core::driver::PageStream<'_>,
) -> Option<Result<faucet_core::StreamPage, faucet_core::FaucetError>> {
    std::future::poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

fn on_commit(addr: &str) -> WebhookSourceConfig {
    WebhookSourceConfig::new()
        .listen_addr(addr.to_string())
        .timeout_secs(30)
        .max_hold_secs(1)
}

/// #1032: with `ack: on_commit` a POST is answered only when the pipeline
/// polls again after its page, and every page carries a bookmark so the
/// pipeline flushes before polling.
#[tokio::test]
async fn a_post_is_answered_only_after_its_page_is_written() {
    let addr = free_addr();
    let source = WebhookSource::new(on_commit(&addr).with_batch_size(2).max_payloads(3));
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let posts: Vec<_> = (0..2)
        .map(|i| {
            let (client, url) = (client.clone(), url.clone());
            tokio::spawn(async move {
                post_until_up(&client, &url, || client.post(&url).json(&json!({"id": i}))).await
            })
        })
        .collect();
    let first = next_page(&mut pages).await.unwrap().unwrap();
    assert_eq!(first.records.len(), 2, "batch_size triggers the page");
    assert_eq!(first.bookmark, Some(json!({"received": 2})));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        posts.iter().all(|p| !p.is_finished()),
        "held until the page commits"
    );

    let third = {
        let (client, url) = (client.clone(), url.clone());
        tokio::spawn(async move { client.post(&url).json(&json!({"id": 2})).send().await })
    };
    // Polling again acknowledges the first page.
    let second = next_page(&mut pages).await.unwrap().unwrap();
    for p in posts {
        assert_eq!(p.await.unwrap().status(), 200);
    }
    assert_eq!(second.records, vec![json!({"id": 2})]);
    assert_eq!(second.bookmark, Some(json!({"received": 3})));
    assert!(
        next_page(&mut pages).await.is_none(),
        "max_payloads closed the window"
    );
    assert_eq!(third.await.unwrap().unwrap().status(), 200);
}

/// #1032: when the run fails (the stream is dropped) every held request is
/// answered `503` + `Retry-After`, including one never taken into a page.
#[tokio::test]
async fn a_failed_run_answers_held_requests_retry_later() {
    let addr = free_addr();
    let source = WebhookSource::new(on_commit(&addr).with_batch_size(1));
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);
    let in_page = {
        let (client, url) = (client.clone(), url.clone());
        tokio::spawn(async move {
            post_until_up(&client, &url, || client.post(&url).json(&json!({"n": 1}))).await
        })
    };
    let page = next_page(&mut pages).await.unwrap().unwrap();
    assert_eq!(page.records, vec![json!({"n": 1})]);
    let waiting = {
        let (client, url) = (client.clone(), url.clone());
        tokio::spawn(async move { client.post(&url).json(&json!({"n": 2})).send().await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(pages);
    for resp in [in_page.await.unwrap(), waiting.await.unwrap().unwrap()] {
        assert_eq!(resp.status(), 503);
        assert_eq!(resp.headers()["retry-after"], "60");
    }
}

/// #1032: `max_hold_secs` emits a partial page, and a cancel closes the
/// window, emits what is held and ends the stream once it is acknowledged.
#[tokio::test]
async fn max_hold_emits_a_partial_page_and_cancel_drains() {
    let addr = free_addr();
    let source = WebhookSource::new(on_commit(&addr));
    let cancel = faucet_core::CancellationToken::new();
    assert!(
        source.set_cancel(cancel.clone()),
        "on_commit drains on cancel"
    );
    assert!(source.consumes_destructively());
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);
    let post = {
        let (client, url) = (client.clone(), url.clone());
        tokio::spawn(async move {
            post_until_up(&client, &url, || client.post(&url).json(&json!({"n": 1}))).await
        })
    };
    let started = std::time::Instant::now();
    let page = next_page(&mut pages).await.unwrap().unwrap();
    assert_eq!(page.records.len(), 1);
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "emitted by max_hold_secs"
    );
    let late = {
        let (client, url) = (client.clone(), url.clone());
        tokio::spawn(async move { client.post(&url).json(&json!({"n": 2})).send().await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    let drained = next_page(&mut pages).await.unwrap().unwrap();
    assert_eq!(drained.records, vec![json!({"n": 2})]);
    assert_eq!(post.await.unwrap().status(), 200);
    assert!(next_page(&mut pages).await.is_none());
    assert_eq!(late.await.unwrap().unwrap().status(), 200);

    let receive = WebhookSource::new(on_commit(&addr).ack(AckMode::OnReceive));
    assert!(!receive.set_cancel(faucet_core::CancellationToken::new()));
    assert!(!receive.consumes_destructively());
}

/// #1032: a byte-identical resend of a signed request whose page is still
/// being written is asked to retry; once committed it is acknowledged without
/// being stored again.
#[tokio::test]
async fn a_resend_of_a_pending_signed_request_retries_then_is_acknowledged() {
    let addr = free_addr();
    let cfg = on_commit(&addr).with_batch_size(1).signature(
        serde_json::from_value(json!({
            "header": "X-Signature",
            "secret": "s3cret",
            "timestamp_header": "X-Timestamp"
        }))
        .unwrap(),
    );
    let source = WebhookSource::new(cfg);
    let url = format!("http://{addr}/webhook");
    let client = reqwest::Client::new();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let body = r#"{"id":7}"#;
    let signature = sign("s3cret", format!("{now}.{body}").as_bytes());
    let send = {
        let (client, url) = (client.clone(), url.clone());
        move || {
            client
                .post(&url)
                .header("X-Timestamp", now.to_string())
                .header("X-Signature", signature.clone())
                .body(body)
        }
    };
    let cancel = faucet_core::CancellationToken::new();
    assert!(source.set_cancel(cancel.clone()));
    let ctx = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);
    let first = {
        let (client, url, send) = (client.clone(), url.clone(), send.clone());
        tokio::spawn(async move { post_until_up(&client, &url, send).await })
    };
    let page = next_page(&mut pages).await.unwrap().unwrap();
    assert_eq!(page.records, vec![json!({"id": 7})]);
    assert_eq!(
        send().send().await.unwrap().status(),
        503,
        "first copy still pending"
    );
    // Polling again acknowledges the page. Once the first copy has its 200, a
    // resend is acknowledged without being stored; the cancel then ends the
    // window with nothing held.
    let (rest, resend) = tokio::join!(next_page(&mut pages), async {
        assert_eq!(first.await.unwrap().status(), 200);
        let resend = send().send().await.unwrap().status();
        cancel.cancel();
        resend
    });
    assert_eq!(resend, 200, "a committed resend is acknowledged");
    assert!(rest.is_none(), "the resend was not stored");
}
