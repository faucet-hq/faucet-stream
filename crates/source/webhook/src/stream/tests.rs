fn state(max_payloads: Option<usize>, closed: bool) -> std::sync::Arc<super::AppState> {
    held_state(max_payloads, closed, false, 0)
}

fn held_state(
    max_payloads: Option<usize>,
    closed: bool,
    hold: bool,
    max_held: usize,
) -> std::sync::Arc<super::AppState> {
    std::sync::Arc::new(super::AppState {
        inbox: tokio::sync::Mutex::new(super::Inbox {
            closed,
            ..Default::default()
        }),
        max_payloads,
        max_buffered_bytes: 0,
        done: tokio::sync::Notify::new(),
        auth_token: None,
        signature: None,
        hold,
        max_held,
        arrived: tokio::sync::Notify::new(),
        abort: faucet_core::CancellationToken::new(),
    })
}

#[test]
fn take_count_emits_on_count_age_or_close() {
    use std::time::Duration;
    let hold = Duration::from_secs(5);
    let young = Some(Duration::from_secs(1));
    let old = Some(Duration::from_secs(5));
    assert_eq!(super::take_count(0, None, true, 10, hold), None);
    assert_eq!(super::take_count(3, young, false, 10, hold), None);
    assert_eq!(super::take_count(10, young, false, 10, hold), Some(10));
    assert_eq!(super::take_count(12, young, false, 10, hold), Some(10));
    assert_eq!(super::take_count(3, old, false, 10, hold), Some(3));
    assert_eq!(super::take_count(30, old, false, 10, hold), Some(10));
    assert_eq!(super::take_count(3, young, true, 10, hold), Some(3));
    // `batch_size: 0` drops the count trigger and takes everything.
    assert_eq!(super::take_count(5000, young, false, 0, hold), None);
    assert_eq!(super::take_count(5000, old, false, 0, hold), Some(5000));
}

#[tokio::test]
async fn a_held_request_answers_after_its_page_is_acknowledged() {
    let s = held_state(None, false, true, 0);
    let post = tokio::spawn(send_payload(s.clone()));
    // Wait until the request is held, then answer it as the stream would.
    let held = loop {
        if let Some(h) = s.inbox.lock().await.held.pop_front() {
            break h;
        }
        tokio::task::yield_now().await;
    };
    assert_eq!(held.record, serde_json::json!({"a": 1}));
    assert!(!post.is_finished());
    held.tx.send(()).unwrap();
    assert_eq!(post.await.unwrap().status(), axum::http::StatusCode::OK);
}

#[tokio::test]
async fn a_dropped_page_or_an_abort_answers_retry_later() {
    let s = held_state(None, false, true, 0);
    let post = tokio::spawn(send_payload(s.clone()));
    let held = loop {
        if let Some(h) = s.inbox.lock().await.held.pop_front() {
            break h;
        }
        tokio::task::yield_now().await;
    };
    drop(held);
    let resp = post.await.unwrap();
    assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers()[axum::http::header::RETRY_AFTER], "60");

    // A request still in the inbox when the stream goes away is answered too.
    let post = tokio::spawn(send_payload(s.clone()));
    while s.inbox.lock().await.held.is_empty() {
        tokio::task::yield_now().await;
    }
    s.abort.cancel();
    assert_eq!(
        post.await.unwrap().status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn requests_over_max_held_are_refused_and_not_stored() {
    let s = held_state(None, false, true, 1);
    let _first = tokio::spawn(send_payload(s.clone()));
    while s.inbox.lock().await.held.is_empty() {
        tokio::task::yield_now().await;
    }
    let resp = send_payload(s.clone()).await;
    assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    let inbox = s.inbox.lock().await;
    assert_eq!((inbox.held.len(), inbox.stored), (1, 1));
    assert!(!inbox.closed);
}

async fn send_payload(state: std::sync::Arc<super::AppState>) -> axum::response::Response {
    super::webhook_handler(
        axum::extract::State(state),
        axum::http::HeaderMap::new(),
        axum::body::Bytes::from_static(b"{\"a\":1}"),
    )
    .await
}

#[tokio::test]
async fn a_closed_window_refuses_with_retry_after() {
    let closed = state(None, true);
    let resp = send_payload(closed.clone()).await;
    assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers()[axum::http::header::RETRY_AFTER], "60");
    assert!(closed.inbox.lock().await.records.is_empty());
}

#[tokio::test]
async fn a_full_inbox_refuses_while_the_window_is_open() {
    let open = state(Some(1), false);
    assert_eq!(
        send_payload(open.clone()).await.status(),
        axum::http::StatusCode::OK
    );
    let resp = send_payload(open.clone()).await;
    assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(resp.headers()[axum::http::header::RETRY_AFTER], "60");
    assert_eq!(open.inbox.lock().await.records.len(), 1);
}

#[tokio::test]
async fn server_results_map_io_and_task_failures() {
    assert!(super::server_result(Ok(Ok(()))).is_ok());
    let io = super::server_result(Ok(Err(std::io::Error::other("boom"))))
        .unwrap_err()
        .to_string();
    assert!(io.contains("webhook server error: boom"), "{io}");
    let join = tokio::spawn(async { panic!("task died") })
        .await
        .unwrap_err();
    let task = super::server_result(Err(join)).unwrap_err().to_string();
    assert!(task.contains("webhook server task failed"), "{task}");
}

use super::*;
use serde_json::json;

#[test]
fn token_matches_accepts_raw_and_bearer() {
    assert!(token_matches(
        Some("sekret-token-value"),
        "sekret-token-value"
    ));
    assert!(token_matches(
        Some("Bearer sekret-token-value"),
        "sekret-token-value"
    ));
}

#[test]
fn token_matches_rejects_wrong_and_missing() {
    assert!(!token_matches(
        Some("wrong-token-value"),
        "sekret-token-value"
    ));
    assert!(!token_matches(
        Some("Bearer wrong-token-value"),
        "sekret-token-value"
    ));
    assert!(!token_matches(None, "sekret-token-value"));
    // Differing length must reject (not panic).
    assert!(!token_matches(
        Some("sekret-token-valu"),
        "sekret-token-value"
    ));
}

#[test]
fn decide_payload_no_cap_always_accepts() {
    for len in [0usize, 1, 100, 10_000] {
        assert_eq!(
            decide_payload(len, None),
            PayloadDecision {
                accept: true,
                done: false
            }
        );
    }
}

#[test]
fn decide_payload_accepts_until_cap_then_drops() {
    let max = Some(2);
    // 0 stored: accept, not yet done.
    assert_eq!(
        decide_payload(0, max),
        PayloadDecision {
            accept: true,
            done: false
        }
    );
    // 1 stored: accept the last slot, now done.
    assert_eq!(
        decide_payload(1, max),
        PayloadDecision {
            accept: true,
            done: true
        }
    );
    // 2 stored (at cap): drop, signal done.
    assert_eq!(
        decide_payload(2, max),
        PayloadDecision {
            accept: false,
            done: true
        }
    );
    // 3 stored (somehow past cap): still drop, still done.
    assert_eq!(
        decide_payload(3, max),
        PayloadDecision {
            accept: false,
            done: true
        }
    );
}

#[test]
fn cap_invariant_never_exceeded_under_concurrent_arrivals() {
    // Simulate many in-flight POSTs all racing the cap: the same record
    // count can be observed by several requests before any of them push.
    // Applying `decide_payload` per request must still bound the Vec at
    // `max`, dropping the surplus rather than overflowing (#146 LOW).
    let max = 2usize;
    let mut records: Vec<Value> = Vec::new();
    // 5 concurrent arrivals; each makes its decision against the current
    // length and only pushes if accepted (mirroring the handler under the
    // records lock).
    for i in 0..5 {
        let decision = decide_payload(records.len(), Some(max));
        if decision.accept {
            records.push(json!({ "id": i }));
        }
    }
    assert_eq!(
        records.len(),
        max,
        "Vec must never exceed max_payloads, got {}",
        records.len()
    );
}

#[tokio::test]
async fn handler_never_exceeds_cap_under_concurrent_posts() {
    // Drive the real handler with many concurrent requests against one
    // shared AppState. The records Vec must never exceed `max_payloads`,
    // even though several requests are accepted in-flight before any of
    // them observe the cap being hit (#146 LOW).
    let max = 3usize;
    let state = Arc::new(AppState {
        inbox: Mutex::new(Inbox::default()),
        max_payloads: Some(max),
        max_buffered_bytes: 0,
        done: Notify::new(),
        auth_token: None,
        signature: None,
        hold: false,
        max_held: 0,
        arrived: Notify::new(),
        abort: faucet_core::CancellationToken::new(),
    });

    let mut handles = Vec::new();
    for i in 0..50 {
        let st = Arc::clone(&state);
        handles.push(tokio::spawn(async move {
            let body = axum::body::Bytes::from(format!("{{\"id\":{i}}}"));
            webhook_handler(State(st), axum::http::HeaderMap::new(), body).await
        }));
    }
    // API-14: exactly `max` are stored and answered 200; every surplus
    // payload is refused with a retryable 503 + Retry-After, never a 200.
    let mut ok = 0;
    for h in handles {
        let resp = h.await.unwrap();
        if resp.status() == StatusCode::OK {
            ok += 1;
        } else {
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(resp.headers()[axum::http::header::RETRY_AFTER], "60");
        }
    }
    assert_eq!(ok, max);

    let records = &state.inbox.lock().await.records;
    assert_eq!(
        records.len(),
        max,
        "Vec must never exceed max_payloads, got {}",
        records.len()
    );
}

#[tokio::test]
async fn webhook_collects_payloads() {
    // Use port 0 to get a random available port.
    let config = WebhookSourceConfig::new()
        .listen_addr("127.0.0.1:0")
        .max_payloads(2)
        .timeout_secs(5);

    let state = Arc::new(AppState {
        inbox: Mutex::new(Inbox::default()),
        max_payloads: config.max_payloads,
        max_buffered_bytes: config.max_buffered_bytes,
        done: Notify::new(),
        auth_token: config.auth_token.clone(),
        signature: None,
        hold: false,
        max_held: 0,
        arrived: Notify::new(),
        abort: faucet_core::CancellationToken::new(),
    });

    let server_state = Arc::clone(&state);
    let app = Router::new()
        .route(&config.path, post(webhook_handler))
        .with_state(Arc::clone(&state));

    let listener = tokio::net::TcpListener::bind(&config.listen_addr)
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let done_notified = server_state.done.notified();
        tokio::select! {
            result = axum::serve(listener, app).into_future() => {
                if let Err(e) = result {
                    panic!("server error: {e}");
                }
            }
            () = done_notified => {}
        }
    });

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/webhook");

    // Send two payloads.
    let resp1 = client
        .post(&url)
        .json(&json!({"event": "created", "id": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), 200);

    let resp2 = client
        .post(&url)
        .json(&json!({"event": "updated", "id": 2}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);

    // Wait for the server to notice max_payloads.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    server_handle.abort();

    let records = &state.inbox.lock().await.records;
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["event"], "created");
    assert_eq!(records[1]["event"], "updated");
}

#[tokio::test]
async fn check_passes_when_port_is_bindable() {
    use faucet_core::Source;
    use faucet_core::check::{CheckContext, ProbeStatus};

    // Port 0 = let the OS pick a free port, so the bind always succeeds.
    let source = WebhookSource::new(WebhookSourceConfig::new().listen_addr("127.0.0.1:0"));
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "io");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "expected Pass, got {:?}",
        report.probes[0].status
    );
    assert_eq!(report.failed_count(), 0);
}

#[tokio::test]
async fn check_fails_when_port_is_already_bound() {
    use faucet_core::Source;
    use faucet_core::check::{CheckContext, ProbeStatus};

    // Hold a real listener, then point the source at the same address so
    // the probe's bind collides.
    let held = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = held.local_addr().unwrap();

    let source = WebhookSource::new(WebhookSourceConfig::new().listen_addr(addr.to_string()));
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert_eq!(report.probes.len(), 1);
    assert_eq!(report.probes[0].name, "io");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Fail { .. }),
        "expected Fail, got {:?}",
        report.probes[0].status
    );
    assert_eq!(report.failed_count(), 1);
    assert!(
        report.probes[0]
            .hint
            .as_deref()
            .unwrap()
            .contains("not bindable")
    );
}

#[tokio::test]
async fn webhook_handles_non_json_body() {
    let state = Arc::new(AppState {
        inbox: Mutex::new(Inbox::default()),
        max_payloads: Some(1),
        max_buffered_bytes: 0,
        done: Notify::new(),
        auth_token: None,
        signature: None,
        hold: false,
        max_held: 0,
        arrived: Notify::new(),
        abort: faucet_core::CancellationToken::new(),
    });

    let server_state = Arc::clone(&state);
    let app = Router::new()
        .route("/webhook", post(webhook_handler))
        .with_state(Arc::clone(&state));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_handle = tokio::spawn(async move {
        let done_notified = server_state.done.notified();
        tokio::select! {
            result = axum::serve(listener, app).into_future() => {
                if let Err(e) = result {
                    panic!("server error: {e}");
                }
            }
            () = done_notified => {}
        }
    });

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://{addr}/webhook"))
        .body("plain text body")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    server_handle.abort();

    let records = &state.inbox.lock().await.records;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], Value::String("plain text body".into()));
}

#[test]
fn dataset_uri_combines_addr_and_path() {
    use faucet_core::Source;
    let source = WebhookSource::new(
        WebhookSourceConfig::new()
            .listen_addr("127.0.0.1:8080")
            .path("/hooks/incoming"),
    );
    assert_eq!(
        source.dataset_uri(),
        "webhook://127.0.0.1:8080/hooks/incoming"
    );
}

fn sig(algorithm: SignatureAlgorithm, encoding: SignatureEncoding) -> WebhookSignature {
    WebhookSignature {
        header: "x-sig".into(),
        secret: "k".into(),
        algorithm,
        encoding,
        prefix: None,
        timestamp_header: None,
        tolerance_secs: 300,
    }
}

#[test]
fn signature_variants_verify_and_reject() {
    use base64::Engine;
    use hmac::{Mac, digest::KeyInit};
    let body = b"payload";
    let mut mac = <hmac::Hmac<sha2::Sha512> as KeyInit>::new_from_slice(b"k").unwrap();
    mac.update(body);
    let b64 = base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("x-sig", b64.parse().unwrap());
    let s512 = sig(SignatureAlgorithm::Sha512, SignatureEncoding::Base64);
    assert!(signature_ok(&s512, &headers, body, 0));
    assert!(!signature_ok(&s512, &headers, b"tampered", 0));
    // Wrong prefix, missing header, unparsable timestamp.
    let mut prefixed = s512.clone();
    prefixed.prefix = Some("v1=".into());
    assert!(!signature_ok(&prefixed, &headers, body, 0));
    assert!(!signature_ok(&s512, &axum::http::HeaderMap::new(), body, 0));
    let mut stamped = sig(SignatureAlgorithm::Sha256, SignatureEncoding::Hex);
    stamped.timestamp_header = Some("x-ts".into());
    headers.insert("x-ts", "not-a-number".parse().unwrap());
    assert!(!signature_ok(&stamped, &headers, body, 0));
}

#[test]
fn signature_config_requires_header_and_secret() {
    let mut s = sig(SignatureAlgorithm::Sha256, SignatureEncoding::Hex);
    s.secret.clear();
    let cfg = WebhookSourceConfig::new().signature(s);
    assert!(cfg.validate().is_err());
}

async fn held_request(s: &std::sync::Arc<super::AppState>) -> super::Held {
    loop {
        if let Some(h) = s.inbox.lock().await.held.pop_front() {
            return h;
        }
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn requests_in_the_page_being_written_count_against_max_held() {
    let s = held_state(None, false, true, 1);
    let _first = tokio::spawn(send_payload(s.clone()));
    // Take it into a page, as `next_batch` does.
    let page = held_request(&s).await;
    s.inbox.lock().await.in_page += 1;
    let resp = send_payload(s.clone()).await;
    assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    drop(page);
}

#[tokio::test]
async fn a_post_refused_by_max_held_does_not_close_the_window() {
    let s = held_state(Some(2), false, true, 1);
    let _first = tokio::spawn(send_payload(s.clone()));
    while s.inbox.lock().await.held.is_empty() {
        tokio::task::yield_now().await;
    }
    // This one would fill `max_payloads`, but it is refused first.
    let resp = send_payload(s.clone()).await;
    assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    let closed =
        tokio::time::timeout(std::time::Duration::from_millis(50), s.done.notified()).await;
    assert!(closed.is_err(), "the window must stay open");
    assert_eq!(s.inbox.lock().await.stored, 1);
}

#[tokio::test]
async fn on_commit_bounds_unacknowledged_bytes_and_keeps_the_window_open() {
    let s = std::sync::Arc::new(super::AppState {
        inbox: tokio::sync::Mutex::new(super::Inbox::default()),
        max_payloads: None,
        max_buffered_bytes: 10,
        done: tokio::sync::Notify::new(),
        auth_token: None,
        signature: None,
        hold: true,
        max_held: 0,
        arrived: tokio::sync::Notify::new(),
        abort: faucet_core::CancellationToken::new(),
    });
    let _first = tokio::spawn(send_payload(s.clone()));
    while s.inbox.lock().await.held.is_empty() {
        tokio::task::yield_now().await;
    }
    // 7 + 7 bytes would cross 10: refused, window still open.
    let resp = send_payload(s.clone()).await;
    assert_eq!(resp.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(!s.inbox.lock().await.closed);
    let closed =
        tokio::time::timeout(std::time::Duration::from_millis(50), s.done.notified()).await;
    assert!(closed.is_err());
    // Acknowledging the page releases its bytes, so the next POST fits.
    let held = held_request(&s).await;
    let session = super::Session {
        state: s.clone(),
        stop: None,
        server: None,
        window_end: tokio::time::Instant::now(),
        cancel: None,
    };
    s.inbox.lock().await.in_page += 1;
    session.acknowledge(vec![(held.tx, None)], held.bytes).await;
    {
        let inbox = s.inbox.lock().await;
        assert_eq!((inbox.bytes, inbox.in_page), (0, 0));
    }
    let _next = tokio::spawn(send_payload(s.clone()));
    let next = held_request(&s).await;
    assert_eq!(next.bytes, 7);
    drop(session);
}

#[tokio::test]
async fn each_run_takes_its_own_cancel_token() {
    let driver = super::WebhookDriver {
        config: crate::config::WebhookSourceConfig::new().listen_addr("127.0.0.1:0"),
        cancel: std::sync::Mutex::new(None),
    };
    let token = faucet_core::CancellationToken::new();
    token.cancel();
    assert!(faucet_core::driver::SourceDriver::set_cancel(
        &driver, token
    ));
    let ctx = HashMap::new();
    // Creating the run's page stream takes the token, before any poll.
    let stream = driver.held_pages(&ctx);
    assert!(driver.cancel.lock().unwrap().is_none());
    drop(stream);
    let next = super::Session::open(&driver, "/webhook", None)
        .await
        .unwrap();
    assert!(
        next.cancel.is_none(),
        "a later run must not inherit the token"
    );
}

#[test]
fn a_page_never_takes_more_than_half_of_max_held() {
    assert_eq!(super::page_cap(1000, 2000), 1000);
    assert_eq!(super::page_cap(5000, 2000), 1000);
    assert_eq!(super::page_cap(0, 2000), 1000);
    assert_eq!(super::page_cap(0, 1), 1);
    assert_eq!(super::page_cap(0, 0), 0);
    assert_eq!(super::page_cap(7, 0), 7);
}
