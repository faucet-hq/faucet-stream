//! Integration tests against an in-process tokio-tungstenite server.

use faucet_core::{
    AuthProvider, AuthReference, AuthSpec, Credential, FaucetError, SharedAuthProvider, Source,
};
use faucet_source_websocket::{
    OnParseError, WebsocketAuth, WebsocketSource, WebsocketSourceConfig, WsMessageFormat,
};
use futures::{SinkExt, StreamExt};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

fn base_config(url: &str) -> WebsocketSourceConfig {
    WebsocketSourceConfig {
        url: url.to_string(),
        auth: AuthSpec::Inline(WebsocketAuth::None),
        subscribe_messages: vec![],
        message_format: WsMessageFormat::Json,
        on_parse_error: OnParseError::Fail,
        envelope: false,
        ping_interval: None,
        max_messages: None,
        idle_timeout: None,
        reconnect: false,
        reconnect_backoff: Duration::from_millis(50),
        max_reconnect_attempts: None,
        max_message_bytes: None,
        batch_size: 1000,
    }
}

/// Spawn a server that pushes `messages` then holds the connection open.
async fn spawn_pushing_server(messages: Vec<String>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            for m in messages {
                if ws.send(Message::Text(m.into())).await.is_err() {
                    return;
                }
            }
            // Keep the connection open so the client terminates via its own
            // limit, not a server close.
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });
    format!("ws://{addr}")
}

#[tokio::test]
async fn collects_up_to_max_messages() {
    let url = spawn_pushing_server(vec![
        r#"{"id":1}"#.into(),
        r#"{"id":2}"#.into(),
        r#"{"id":3}"#.into(),
    ])
    .await;
    let mut cfg = base_config(&url);
    cfg.max_messages = Some(3);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[2]["id"], 3);
}

#[tokio::test]
async fn idle_timeout_terminates_quiet_stream() {
    // Server pushes 2 then goes silent; idle_timeout ends the run.
    let url = spawn_pushing_server(vec![r#"{"id":1}"#.into(), r#"{"id":2}"#.into()]).await;
    let mut cfg = base_config(&url);
    cfg.idle_timeout = Some(Duration::from_millis(300));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
}

#[tokio::test]
async fn binary_frames_base64_encoded() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let _ = ws.send(Message::Binary(b"hello".to_vec().into())).await;
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.message_format = WsMessageFormat::Binary;
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0], serde_json::json!("aGVsbG8="));
}

#[tokio::test]
async fn envelope_mode_wraps_record() {
    let url = spawn_pushing_server(vec![r#"{"id":1}"#.into()]).await;
    let mut cfg = base_config(&url);
    cfg.envelope = true;
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records[0]["data"], serde_json::json!({"id": 1}));
    assert!(records[0]["received_at"].is_number());
    assert_eq!(records[0]["url"], url);
}

#[tokio::test]
async fn skip_drops_malformed_json() {
    let url = spawn_pushing_server(vec!["not json".into(), r#"{"id":2}"#.into()]).await;
    let mut cfg = base_config(&url);
    cfg.on_parse_error = OnParseError::Skip;
    cfg.max_messages = Some(1); // the one valid record
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 2);
}

#[tokio::test]
async fn skipped_frames_reset_idle_timeout() {
    // M9 (#146): with on_parse_error=Skip, an unparseable frame yields no
    // record but the connection is demonstrably alive — receiving any frame
    // must reset the idle timer. Here the server sends one skippable frame at
    // ~300ms (within the 400ms idle window), then a valid record at ~600ms.
    // Before the fix the skip frame didn't reset `last_message_at`, so the run
    // idle-timed-out at ~400ms and the valid record was never received (0
    // records). After the fix the skip frame resets the timer, so the run stays
    // alive long enough to receive the record.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = ws.send(Message::Text("not json".into())).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            let _ = ws.send(Message::Text(r#"{"id":99}"#.into())).await;
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.on_parse_error = OnParseError::Skip;
    cfg.idle_timeout = Some(Duration::from_millis(400));
    cfg.max_messages = Some(1);
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(
        records.len(),
        1,
        "a skipped frame must reset idle_timeout so the later valid record is still received"
    );
    assert_eq!(records[0]["id"], 99);
}

#[tokio::test]
async fn auth_header_is_sent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let seen2 = Arc::clone(&seen);
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            // The `accept_hdr_async` callback's return type is fixed by the
            // tungstenite `Callback` trait (`Response<Option<String>>` is large
            // by value); we cannot box it here, so allow the lint test-side.
            #[allow(clippy::result_large_err)]
            let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                            resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                if req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    == Some("Bearer secret")
                {
                    seen2.store(1, Ordering::SeqCst);
                }
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(stream, callback)
                .await
                .unwrap();
            let _ = ws.send(Message::Text(r#"{"id":1}"#.into())).await;
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.auth = AuthSpec::Inline(WebsocketAuth::Bearer {
        token: "secret".into(),
    });
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "server did not see the Authorization header"
    );
}

#[tokio::test]
async fn reconnect_resumes_after_drop() {
    // Server closes after the first message on the first connection, then
    // serves a second message on the next connection. Count connections.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut conn = 0u32;
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            conn += 1;
            let n = conn;
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                // Each connection sends one message tagged with the conn number,
                // then closes (drops) to force the client to reconnect.
                let _ = ws
                    .send(Message::Text(format!(r#"{{"conn":{n}}}"#).into()))
                    .await;
                let _ = ws.close(None).await;
            });
        }
    });
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.reconnect = true;
    cfg.reconnect_backoff = Duration::from_millis(20);
    cfg.max_messages = Some(2);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["conn"], 1);
    assert_eq!(records[1]["conn"], 2);
}

#[tokio::test]
async fn custom_headers_auth_is_sent() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let seen2 = Arc::clone(&seen);
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            // The `accept_hdr_async` callback's return type is fixed by the
            // tungstenite `Callback` trait (`Response<Option<String>>` is large
            // by value); we cannot box it here, so allow the lint test-side.
            #[allow(clippy::result_large_err)]
            let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                            resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                if req.headers().get("x-api-key").and_then(|v| v.to_str().ok()) == Some("k123") {
                    seen2.store(1, Ordering::SeqCst);
                }
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(stream, callback)
                .await
                .unwrap();
            let _ = ws.send(Message::Text(r#"{"id":1}"#.into())).await;
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });
    let mut headers = BTreeMap::new();
    headers.insert("x-api-key".to_string(), "k123".to_string());
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.auth = AuthSpec::Inline(WebsocketAuth::Custom { headers });
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg).unwrap();
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "server did not see the custom x-api-key header"
    );
}

#[tokio::test]
async fn ping_keepalive_is_sent() {
    // Server reads incoming control frames and records whether it sees a Ping.
    // It sends no data, so the client never hits max_messages; it terminates
    // via idle_timeout. The ping_interval (~100ms) fires before idle_timeout,
    // so the server must observe a Ping.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let saw_ping = Arc::new(AtomicUsize::new(0));
    let saw_ping2 = Arc::clone(&saw_ping);
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            // Drain incoming frames; flag the first Ping we observe.
            while let Some(msg) = ws.next().await {
                match msg {
                    Ok(Message::Ping(_)) => {
                        saw_ping2.store(1, Ordering::SeqCst);
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        }
    });
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.ping_interval = Some(Duration::from_millis(100));
    cfg.idle_timeout = Some(Duration::from_secs(1));
    cfg.max_messages = Some(1); // server sends nothing, so this never fires
    let src = WebsocketSource::new(cfg).unwrap();
    let _ = src.fetch_all().await.unwrap();
    assert_eq!(
        saw_ping.load(Ordering::SeqCst),
        1,
        "server did not observe a keepalive Ping within the idle window"
    );
}

// ── Shared AuthProvider tests ─────────────────────────────────────────────────

#[derive(Debug)]
struct FixedBearer(&'static str);

#[async_trait::async_trait]
impl AuthProvider for FixedBearer {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Bearer(self.0.to_string()))
    }
    fn provider_name(&self) -> &'static str {
        "fixed-bearer"
    }
}

/// Injecting a `FixedBearer` provider causes the handshake to carry the token.
#[tokio::test]
async fn injected_provider_supplies_bearer_token() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(AtomicUsize::new(0));
    let seen2 = Arc::clone(&seen);
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            #[allow(clippy::result_large_err)]
            let callback = |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                            resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                if req
                    .headers()
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    == Some("Bearer INJECTED")
                {
                    seen2.store(1, Ordering::SeqCst);
                }
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(stream, callback)
                .await
                .unwrap();
            let _ = ws
                .send(tokio_tungstenite::tungstenite::Message::Text(
                    r#"{"id":1}"#.into(),
                ))
                .await;
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });

    let provider: SharedAuthProvider = Arc::new(FixedBearer("INJECTED"));
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg)
        .unwrap()
        .with_auth_provider(provider);
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(
        seen.load(Ordering::SeqCst),
        1,
        "server did not see the Authorization: Bearer INJECTED header"
    );
}

/// A `{ ref: name }` auth with no provider supplied must error at connect time.
#[tokio::test]
async fn unresolved_auth_reference_errors() {
    // We need a valid WS server for the connect attempt, but the error should
    // fire before the network round-trip, so the listener never has to accept.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.auth = AuthSpec::Reference(AuthReference {
        name: "missing".into(),
    });
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_millis(500));
    let src = WebsocketSource::new(cfg).unwrap();
    let err = src.fetch_all().await.unwrap_err();
    assert!(
        matches!(err, FaucetError::Auth(_)),
        "expected Auth error, got {err:?}"
    );
}

#[tokio::test]
async fn pages_flush_at_batch_size() {
    // Server pushes 5 JSON messages; batch_size=2 should yield pages of 2,2,1.
    let url = spawn_pushing_server(vec![
        r#"{"id":1}"#.into(),
        r#"{"id":2}"#.into(),
        r#"{"id":3}"#.into(),
        r#"{"id":4}"#.into(),
        r#"{"id":5}"#.into(),
    ])
    .await;
    let mut cfg = base_config(&url);
    cfg.batch_size = 2;
    cfg.max_messages = Some(5);
    cfg.idle_timeout = Some(Duration::from_secs(1));
    let src = WebsocketSource::new(cfg).unwrap();
    let ctx = std::collections::HashMap::new();
    let mut pages = src.stream_pages(&ctx, 2);
    let mut page_sizes = Vec::new();
    let mut total = 0usize;
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert!(
            page.bookmark.is_none(),
            "websocket pages must carry no bookmark"
        );
        total += page.records.len();
        page_sizes.push(page.records.len());
    }
    assert_eq!(total, 5, "all 5 records must arrive");
    assert!(
        page_sizes.len() >= 2,
        "batch_size=2 over 5 records must produce multiple pages, got {page_sizes:?}"
    );
    assert_eq!(page_sizes, vec![2, 2, 1]);
}

/// A shared provider whose current token the server has revoked: `invalidate`
/// refreshes it unless a newer one was already handed out (#789 API-06).
#[derive(Debug, Default)]
struct RevokedThenRefreshed {
    token: std::sync::Mutex<u32>,
}

#[async_trait::async_trait]
impl AuthProvider for RevokedThenRefreshed {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Bearer(format!(
            "t{}",
            self.token.lock().unwrap()
        )))
    }
    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        let mut t = self.token.lock().unwrap();
        let current = format!("t{t}");
        if !matches!(stale, Credential::Bearer(s) if *s != current) {
            *t += 1;
        }
        Ok(Credential::Bearer(format!("t{t}")))
    }
    fn provider_name(&self) -> &'static str {
        "revoked-then-refreshed"
    }
}

/// A handshake rejected with `401` re-authenticates the shared provider and
/// connects again with the fresh token.
#[tokio::test]
#[allow(clippy::result_large_err)]
async fn a_rejected_handshake_refreshes_the_shared_token() {
    use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let accepted = tokio_tungstenite::accept_hdr_async(
                stream,
                |req: &Request, resp: Response| -> Result<Response, ErrorResponse> {
                    let auth = req
                        .headers()
                        .get("authorization")
                        .and_then(|v| v.to_str().ok());
                    if auth == Some("Bearer t1") {
                        Ok(resp)
                    } else {
                        let mut reject = ErrorResponse::new(None);
                        *reject.status_mut() =
                            tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
                        Err(reject)
                    }
                },
            )
            .await;
            if let Ok(mut ws) = accepted {
                let _ = ws.send(Message::Text(r#"{"id":1}"#.into())).await;
                while ws.next().await.is_some() {}
            }
        }
    });

    let mut cfg = base_config(&format!("ws://{addr}"));
    cfg.max_messages = Some(1);
    cfg.idle_timeout = Some(Duration::from_secs(5));
    let src = WebsocketSource::new(cfg)
        .unwrap()
        .with_auth_provider(Arc::new(RevokedThenRefreshed::default()));
    let records = src.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
}
