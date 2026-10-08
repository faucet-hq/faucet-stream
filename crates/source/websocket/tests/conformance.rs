//! `faucet-conformance` battery for the WebSocket source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value
//!          (pure, offline — always runs).
//! Check 2: `stream_pages` bounds peak memory. The websocket source buffers
//!          incoming frames and flushes a page every `batch_size` messages (see
//!          `stream.rs`), so an in-process server that pushes TOTAL frames with
//!          `batch_size = 250` streams in pages of ≤ 250: the peak page (250)
//!          is ≤ the batch cap and strictly < the total. The source terminates
//!          the drain via `max_messages` (with `idle_timeout` as a backstop).

use faucet_conformance::{
    assert_bounded_memory, assert_config_schema_valid_value, assert_connector_name_nonempty,
    assert_errors_not_panics, assert_preflight_check_wellformed,
};
use faucet_core::AuthSpec;
use faucet_source_websocket::{
    OnParseError, WebsocketAuth, WebsocketSource, WebsocketSourceConfig, WsMessageFormat,
};
use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

const BATCH: usize = 250;
const TOTAL: usize = 5000;

// ── Check 1: config schema validity (offline) ────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(WebsocketSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-websocket");
}

// ── Check 2: bounded-memory streaming (in-process WS server) ─────────────────

/// Spawn a server that pushes `count` JSON frames, then holds the connection
/// open so the client terminates via its own `max_messages` limit rather than a
/// server close.
async fn spawn_pushing_server(count: usize) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((stream, _)) = listener.accept().await {
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            for i in 0..count {
                let frame = format!("{{\"id\":{i}}}");
                if ws.send(Message::Text(frame.into())).await.is_err() {
                    return;
                }
            }
            loop {
                if ws.next().await.is_none() {
                    break;
                }
            }
        }
    });
    format!("ws://{addr}")
}

fn base_config(url: &str) -> WebsocketSourceConfig {
    WebsocketSourceConfig {
        url: url.to_string(),
        auth: AuthSpec::Inline(WebsocketAuth::None),
        subscribe_messages: vec![],
        message_format: WsMessageFormat::Json,
        on_parse_error: OnParseError::Fail,
        envelope: false,
        ping_interval: None,
        max_messages: Some(TOTAL),
        idle_timeout: Some(Duration::from_secs(30)),
        reconnect: false,
        reconnect_backoff: Duration::from_millis(50),
        max_reconnect_attempts: None,
        max_message_bytes: None,
        batch_size: BATCH,
        connect_timeout: std::time::Duration::from_secs(30),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let url = spawn_pushing_server(TOTAL).await;
    let source = WebsocketSource::new(base_config(&url)).unwrap();
    // The source flushes a page every BATCH frames, so the peak page is 250 ≤
    // BATCH (250) and < TOTAL (5000).
    assert_bounded_memory(&source, BATCH, TOTAL).await;
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

/// Point the source at an unreachable WebSocket endpoint (`ws://127.0.0.1:1`,
/// which refuses connections immediately on all platforms). `new()` is lazy —
/// it validates config and stores it; the connection is opened on first read.
/// With `reconnect: false` the failed connect surfaces immediately as a typed
/// `FaucetError` on both the `fetch_all` and `stream_pages` paths, never a
/// panic.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let source = WebsocketSource::new(base_config("ws://127.0.0.1:1")).unwrap();
    assert_errors_not_panics(&source).await;
}

// ── Check 10: connector_name non-empty (offline) ──────────────────────────────

#[test]
fn conformance_connector_name_nonempty() {
    let source = WebsocketSource::new(base_config("ws://127.0.0.1:1")).unwrap();
    assert_connector_name_nonempty(&source);
}

// ── Check 11: preflight check() is well-formed ────────────────────────────────

/// The websocket source overrides `check()` with a TCP-connect probe. Pointed
/// at an unreachable endpoint it surfaces the failure as a `Fail` probe inside
/// `Ok(report)`, never an `Err` — exactly what the contract requires.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_preflight_check_wellformed() {
    let source = WebsocketSource::new(base_config("ws://127.0.0.1:1")).unwrap();
    assert_preflight_check_wellformed(&source, &faucet_core::check::CheckContext::default()).await;
}
